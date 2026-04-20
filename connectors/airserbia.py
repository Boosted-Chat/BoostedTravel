"""
Air Serbia (JU) — CDP Chrome connector — form fill + API intercept.

Air Serbia's website at www.airserbia.com uses a search widget with autocomplete
airport fields and calendar date picker. Direct API calls are blocked;
headed CDP Chrome with form fill + API interception is required.

Strategy (CDP Chrome + API interception):
1. Launch headed Chrome via CDP (off-screen, stealth).
2. Navigate to airserbia.com → SPA loads with search widget.
3. Accept cookies → set one-way → fill origin/dest → select date → search.
4. Intercept the search API response (flight availability JSON).
5. If API not captured, fall back to DOM scraping on results page.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, date, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9497
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".airserbia_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("AirSerbia: connected to existing Chrome on port %d", _DEBUG_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass
            chrome = find_chrome()
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            args = [
                chrome,
                f"--remote-debugging-port={_DEBUG_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                *proxy_chrome_args(),
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1400,900",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.0)
            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            logger.info("AirSerbia: Chrome launched on CDP port %d", _DEBUG_PORT)

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    global _browser, _context, _pw_instance, _chrome_proc
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = _context = _pw_instance = _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
        except Exception:
            pass


async def _dismiss_overlays(page) -> None:
    try:
        await page.evaluate("""() => {
            // Air Serbia uses CookieFirst — try that first
            const cf = document.querySelector('[data-cookiefirst-action="accept"]');
            if (cf && cf.offsetHeight > 0) { cf.click(); return; }
            const cfRoot = document.querySelector('#cookiefirst-root');
            if (cfRoot) {
                const cfBtns = cfRoot.querySelectorAll('button');
                for (const b of cfBtns) {
                    const t = (b.textContent || '').trim().toLowerCase();
                    if ((t.includes('accept') || t.includes('agree')) && b.offsetHeight > 0) {
                        b.click(); return;
                    }
                }
            }
            // OneTrust fallback
            const accept = document.querySelector('#onetrust-accept-btn-handler');
            if (accept && accept.offsetHeight > 0) { accept.click(); return; }
            // Generic fallback
            const ariaBtn = document.querySelector('button[aria-label*="Accept all"]');
            if (ariaBtn && ariaBtn.offsetHeight > 0) { ariaBtn.click(); return; }
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = (b.textContent || '').trim().toLowerCase();
                if ((t.includes('accept all') || t.includes('agree') || t.includes('got it'))
                    && b.offsetHeight > 0) { b.click(); return; }
            }
        }""")
        await asyncio.sleep(1.0)
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter, ' +
                '#cookiefirst-root, [class*="cookiefirst"], ' +
                '[class*="cookie-banner"], [class*="consent-banner"]'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class AirSerbiaConnectorClient:
    """Air Serbia (JU) CDP Chrome connector."""

    IATA = "JU"
    AIRLINE_NAME = "Air Serbia"
    SOURCE = "airserbia_direct"
    HOMEPAGE = "https://www.airserbia.com/en"
    DEFAULT_CURRENCY = "EUR"

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            nonlocal search_data
            url = response.url.lower()
            if response.status not in (200, 201):
                return
            try:
                # Match GraphQL and flight-related URLs
                if any(k in url for k in ["/search", "/availability", "/flight",
                                           "/offer", "/fare", "/lowprice", "/schedule",
                                           "/graphql", "/api/"]):
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct and "text" not in ct:
                        return
                    body = await response.text()
                    if len(body) < 50:
                        return
                    data = json.loads(body)
                    if not isinstance(data, dict):
                        return
                    # Prefer the GraphQL response with bookingAirSearch (the flight data)
                    bas = (data.get("data") or {}).get("bookingAirSearch")
                    if bas:
                        search_data = data  # replace with flight data response
                        api_event.set()  # ONLY set event for GraphQL flight data
                        logger.info("AirSerbia: captured GraphQL flight data (%d bytes)", len(body))
                        return
                    # Fallback: check generic keywords (non-GraphQL endpoints)
                    # Store silently but DON'T set api_event — keep waiting for GraphQL
                    keys_str = " ".join(str(k).lower() for k in data.keys())
                    if any(k in keys_str for k in ["flight", "itiner", "offer", "fare",
                                                     "bound", "trip", "result", "segment",
                                                     "avail", "journey", "price"]):
                        if not search_data:  # only if no better response yet
                            search_data = data
                            # DON'T set api_event here — keep waiting for GraphQL
                            logger.info("AirSerbia: captured API fallback → %s (%d keys)", url[:80], len(data))
            except Exception:
                pass

        page.on("response", _on_response)

        async def _on_new_page(new_page):
            logger.info("AirSerbia: new page opened → %s", new_page.url[:80])
            new_page.on("response", _on_response)

        try:
            logger.info("AirSerbia: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(self.HOMEPAGE, wait_until="domcontentloaded", timeout=30000)
            # Wait for selectize.js search widget to render — Cloudflare challenge
            # may fire first (403 then auto-solve), so poll for form visibility.
            for _wait in range(20):  # up to 20s total
                await asyncio.sleep(1.0)
                vis = await page.locator('input[placeholder="From"]:visible').count()
                if vis > 0:
                    break
            else:
                logger.warning("AirSerbia: From input never became visible")
            await _dismiss_overlays(page)

            # One-way toggle — Air Serbia uses pill tab buttons
            await page.evaluate("""() => {
                const btn = document.getElementById('one-way-tab-hero_banner_0');
                if (btn && btn.offsetHeight > 0) { btn.click(); return; }
                const els = document.querySelectorAll('button, li, a');
                for (const el of els) {
                    const t = (el.textContent || '').trim().toLowerCase();
                    if (t === 'one way' && el.offsetHeight > 0) { el.click(); return; }
                }
            }""")
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, 'input[placeholder="From"]', req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, 'input[placeholder="To"]', req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Attach listener to new pages BEFORE clicking search
            # (booking.airserbia.com may open in a new tab)
            context.on("page", _on_new_page)

            # Click search — use Playwright click (JS click doesn't trigger the handler)
            try:
                search_btn = page.locator('button.btn-search:visible, button:has-text("Show flights"):visible').first
                await search_btn.click(timeout=5000)
            except Exception:
                # Fallback to JS click on any visible show/search button
                await page.evaluate("""() => {
                    const btns = document.querySelectorAll('button.btn-search, button[type="submit"]');
                    for (const b of btns) {
                        const t = (b.textContent || '').trim().toLowerCase();
                        if ((t.includes('show') || t.includes('search') || t.includes('find'))
                            && b.offsetHeight > 0) { b.click(); return; }
                    }
                }""")
            logger.info("AirSerbia: search clicked")

            # Wait for navigation/API response — may stay on same page or open new tab
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            booking_redirect_time = None
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                cur_url = page.url
                if "booking.airserbia.com" in cur_url or "flight-selection" in cur_url:
                    # Booking page detected — wait up to 12s for GraphQL response
                    if booking_redirect_time is None:
                        booking_redirect_time = time.monotonic()
                        logger.info("AirSerbia: redirected to booking page, waiting for GraphQL...")
                    elapsed_on_booking = time.monotonic() - booking_redirect_time
                    if elapsed_on_booking > 12:
                        break
                    await asyncio.sleep(1.0)
                    continue
                if any(k in cur_url.lower() for k in ["result", "search", "availability"]):
                    await asyncio.sleep(2.0)
                    break
                await asyncio.sleep(1.0)

            # Final wait for late API responses (new-tab scenario covered by context.on("page"))
            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=8.0)
                except asyncio.TimeoutError:
                    pass

            offers = []
            if search_data:
                logger.info("AirSerbia: captured API data (%d keys)", len(search_data))
                offers = self._parse_api_response(search_data, req)
            if not offers:
                offers = await self._scrape_dom(page, req)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("AirSerbia %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"airserbia{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("AirSerbia error: %s", e)
            return self._empty(req)
        finally:
            try:
                context.remove_listener("page", _on_new_page)
            except Exception:
                pass
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_airport(self, page, selector: str, iata: str) -> bool:
        """Fill Air Serbia autocomplete airport field — uses selectize.js widget."""
        try:
            # Multiple From/To inputs exist (round-trip, one-way, multi-city tabs);
            # only the active tab's input is visible.
            field = page.locator(f'{selector}:visible').first
            await field.click(timeout=5000)
            await asyncio.sleep(0.5)
            await page.keyboard.press("Control+a")
            await page.keyboard.press("Backspace")
            await field.type(iata, delay=120)
            await asyncio.sleep(2.0)

            selected = await page.evaluate("""(iata) => {
                // selectize.js uses .selectize-dropdown .option with city and code inside
                const opts = document.querySelectorAll(
                    '.selectize-dropdown .option, [role="option"], [class*="suggest"] li, ' +
                    '[class*="autocomplete"] li, .search-result-item'
                );
                for (const o of opts) {
                    if (o.textContent.includes(iata) && o.offsetHeight > 0) {
                        o.click(); return true;
                    }
                }
                return false;
            }""", iata)
            if not selected:
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.2)
                await page.keyboard.press("Enter")

            await asyncio.sleep(0.5)
            logger.info("AirSerbia: airport %s → %s", selector[-30:], iata)
            return True
        except Exception as e:
            logger.warning("AirSerbia: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill Air Serbia flatpickr date picker."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        iso = dt.strftime("%Y-%m-%d")
        month_idx = dt.month - 1  # 0-indexed for flatpickr month dropdown
        year = str(dt.year)
        day = str(dt.day)
        try:
            # Click the visible departure date input to open flatpickr
            date_input = page.locator('input[placeholder="Departure date"]:visible, input[placeholder="Date range"]:visible').first
            await date_input.click(timeout=5000)
            await asyncio.sleep(1.0)

            # Set month and year on the open calendar's dropdowns
            await page.evaluate(f"""() => {{
                const cal = document.querySelector('.flatpickr-calendar.open');
                if (!cal) return;
                const sel = cal.querySelector('.flatpickr-monthDropdown-months');
                if (sel) {{ sel.value = '{month_idx}'; sel.dispatchEvent(new Event('change', {{bubbles: true}})); }}
                const yr = cal.querySelector('.numInput.cur-year');
                if (yr) {{ yr.value = '{year}'; yr.dispatchEvent(new Event('input', {{bubbles: true}})); yr.dispatchEvent(new Event('change', {{bubbles: true}})); }}
            }}""")
            await asyncio.sleep(0.5)

            # Click the target day cell
            clicked = await page.evaluate("""(day) => {
                const cells = document.querySelectorAll('.flatpickr-day:not(.flatpickr-disabled):not(.prevMonthDay):not(.nextMonthDay)');
                for (const c of cells) {
                    if (c.textContent.trim() === day && c.offsetHeight > 0) { c.click(); return true; }
                }
                return false;
            }""", day)
            if clicked:
                logger.info("AirSerbia: date set %s", iso)
            await asyncio.sleep(1.0)
            return True
        except Exception as e:
            logger.warning("AirSerbia: date error: %s", e)
            return False

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        # Check for GraphQL response format (Air Serbia DX platform)
        bas = (data.get("data") or {}).get("bookingAirSearch")
        if bas:
            return self._parse_graphql(bas, req)

        offers = []
        flights = (
            data.get("flights") or data.get("results") or data.get("itineraries") or
            data.get("flightInfos") or data.get("offers") or data.get("journeys") or
            data.get("routeList") or data.get("flightList") or []
        )
        if isinstance(flights, dict):
            for key in ("flights", "results", "itineraries", "options", "list"):
                if key in flights:
                    flights = flights[key]
                    break
            else:
                flights = [flights]
        if not isinstance(flights, list):
            flights = self._find_flights(data)
        for flight in flights:
            offer = self._build_offer(flight, req)
            if offer:
                offers.append(offer)
        return offers

    def _parse_graphql(self, bas: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Air Serbia GraphQL bookingAirSearch response."""
        orig_resp = bas.get("originalResponse", {})
        currency = orig_resp.get("currency", self.DEFAULT_CURRENCY)
        offers = []

        # unbundledOffers[0] contains the outbound offer list
        ub = orig_resp.get("unbundledOffers") or []
        offer_list = ub[0] if ub and isinstance(ub[0], list) else ub

        for raw in offer_list:
            if not isinstance(raw, dict) or raw.get("soldout"):
                continue
            try:
                # Price: total.alternatives[0][0].amount
                total_block = raw.get("total", {})
                alts = total_block.get("alternatives", [[]])
                price = float(alts[0][0].get("amount", 0)) if alts and alts[0] else 0
                if price <= 0:
                    continue
                cur = alts[0][0].get("currency", currency) if alts and alts[0] else currency

                cabin = raw.get("cabinClass", "economy").lower()
                brand = raw.get("brandId", "")

                # Segments from itineraryPart[0].segments
                itin_parts = raw.get("itineraryPart", [])
                if not itin_parts or not isinstance(itin_parts, list):
                    continue
                itin = itin_parts[0] if isinstance(itin_parts[0], dict) else {}
                seg_list = itin.get("segments", [])
                if not seg_list:
                    continue

                segments = []
                for seg in seg_list:
                    flt = seg.get("flight", {})
                    airline_code = flt.get("airlineCode", self.IATA)
                    op_code = flt.get("operatingAirlineCode", airline_code)
                    flt_num = str(flt.get("flightNumber", ""))
                    flight_no = f"{airline_code}{flt_num}" if flt_num else self.IATA

                    dep_dt = self._parse_dt(seg.get("departure", ""), req.date_from)
                    arr_dt = self._parse_dt(seg.get("arrival", ""), req.date_from)

                    segments.append(FlightSegment(
                        airline=airline_code[:2],
                        airline_name=self.AIRLINE_NAME if airline_code == self.IATA else op_code,
                        flight_no=flight_no,
                        origin=seg.get("origin", req.origin),
                        destination=seg.get("destination", req.destination),
                        departure=dep_dt,
                        arrival=arr_dt,
                        cabin_class=cabin,
                    ))

                if not segments:
                    continue

                total_dur = itin.get("totalDuration", 0) * 60  # minutes → seconds
                stops = itin.get("stops", max(0, len(segments) - 1))
                route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=stops)

                offer_id = hashlib.md5(
                    f"{self.IATA.lower()}_{req.origin}_{req.destination}_{req.date_from}_{price}_{segments[0].flight_no}_{brand}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"{self.IATA.lower()}_{offer_id}",
                    price=round(price, 2),
                    currency=cur,
                    price_formatted=f"{cur} {price:,.2f}",
                    outbound=route,
                    inbound=None,
                    airlines=list({s.airline for s in segments}),
                    owner_airline=self.IATA,
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source=self.SOURCE,
                    source_tier="free",
                ))
            except Exception as e:
                logger.debug("AirSerbia: GraphQL offer parse error: %s", e)
                continue

        logger.info("AirSerbia: parsed %d offers from GraphQL", len(offers))
        return offers

    def _find_flights(self, data, depth=0) -> list:
        if depth > 4 or not isinstance(data, dict):
            return []
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                sample_keys = {str(k).lower() for k in val[0].keys()}
                if sample_keys & {"price", "fare", "flight", "departure", "segment", "leg"}:
                    return val
            elif isinstance(val, dict):
                result = self._find_flights(val, depth + 1)
                if result:
                    return result
        return []

    def _build_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        try:
            price = (
                flight.get("price") or flight.get("totalPrice") or
                flight.get("fare") or flight.get("amount") or
                flight.get("adultPrice") or 0
            )
            if isinstance(price, dict):
                price = price.get("amount") or price.get("total") or price.get("value") or 0
            price = float(price) if price else 0
            if price <= 0:
                return None

            currency = self._extract_currency(flight)

            segments_data = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
            if not isinstance(segments_data, list):
                segments_data = [flight]

            segments = []
            for seg in segments_data:
                dep_str = seg.get("departure") or seg.get("departureTime") or seg.get("depTime") or ""
                arr_str = seg.get("arrival") or seg.get("arrivalTime") or seg.get("arrTime") or ""
                dep_dt = self._parse_dt(dep_str, req.date_from)
                arr_dt = self._parse_dt(arr_str, req.date_from)
                airline_code = seg.get("airline") or seg.get("carrierCode") or seg.get("operatingCarrier") or self.IATA
                flight_no = seg.get("flightNumber") or seg.get("flightNo") or ""
                if flight_no and not flight_no.startswith(airline_code):
                    flight_no = f"{airline_code}{flight_no}"

                segments.append(FlightSegment(
                    airline=airline_code[:2], airline_name=self.AIRLINE_NAME if airline_code == self.IATA else airline_code,
                    flight_no=flight_no or self.IATA, origin=seg.get("origin") or seg.get("departureAirport") or req.origin,
                    destination=seg.get("destination") or seg.get("arrivalAirport") or req.destination,
                    departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                ))

            if not segments:
                return None

            route = FlightRoute(segments=segments, total_duration_seconds=0, stopovers=max(0, len(segments) - 1))
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{req.origin}_{req.destination}_{req.date_from}_{price}_{segments[0].flight_no}".encode()
            ).hexdigest()[:12]

            return FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
                price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
                airlines=list({s.airline for s in segments}), owner_airline=self.IATA,
                booking_url=self._booking_url(req), is_locked=False,
                source=self.SOURCE, source_tier="free",
            )
        except Exception as e:
            logger.debug("AirSerbia: offer parse error: %s", e)
            return None

    def _extract_currency(self, d: dict) -> str:
        for key in ("currency", "currencyCode"):
            val = d.get(key)
            if isinstance(val, str) and len(val) == 3:
                return val.upper()
        if isinstance(d.get("price"), dict):
            return d["price"].get("currency", self.DEFAULT_CURRENCY)
        return self.DEFAULT_CURRENCY

    @staticmethod
    def _parse_dt(s, fallback_date) -> datetime:
        if not s:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                return datetime(dt.year, dt.month, dt.day) if isinstance(dt, date) and not isinstance(dt, datetime) else dt
            except Exception:
                return datetime.now()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(s[:19], fmt)
            except (ValueError, TypeError):
                continue
        m = re.search(r"(\d{1,2}):(\d{2})", str(s))
        if m:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                d = dt if isinstance(dt, date) and not isinstance(dt, datetime) else dt.date() if isinstance(dt, datetime) else dt
                return datetime(d.year, d.month, d.day, int(m.group(1)), int(m.group(2)))
            except Exception:
                pass
        return datetime.now()

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        await asyncio.sleep(1.5)
        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];
            const cards = document.querySelectorAll(
                '[class*="flight-card"], [class*="flight-row"], [class*="itinerary"], ' +
                '[class*="result-card"], [class*="bound"], [class*="flight-item"], ' +
                '[class*="flightInfo"], [class*="flight_item"]'
            );
            for (const card of cards) {
                const text = card.innerText || '';
                if (text.length < 20) continue;
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;
                const priceMatch = text.match(/(EUR|RSD|USD|€|\$)\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(EUR|RSD|USD|€|\$)/i);
                if (!priceMatch) continue;
                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0) continue;
                let currency = 'EUR';
                if (/USD|\$/.test(priceMatch[0])) currency = 'USD';
                else if (/RSD/.test(priceMatch[0])) currency = 'RSD';
                const fnMatch = text.match(/\b(JU\s*\d{2,4})\b/i);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'JU',
                });
            }
            return results;
        }""", [req.origin, req.destination])

        offers = []
        for f in (flights or []):
            offer = self._build_dom_offer(f, req)
            if offer:
                offers.append(offer)
        return offers

    def _build_dom_offer(self, f: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        price = f.get("price", 0)
        if price <= 0:
            return None
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            dep_date = dt.date() if isinstance(dt, datetime) else dt if isinstance(dt, date) else date.today()
        except (ValueError, TypeError):
            dep_date = date.today()

        dep_time = f.get("depTime", "00:00")
        arr_time = f.get("arrTime", "00:00")
        try:
            h, m = dep_time.split(":")
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day, int(h), int(m))
        except (ValueError, IndexError):
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day)
        try:
            h, m = arr_time.split(":")
            arr_dt = datetime(dep_date.year, dep_date.month, dep_date.day, int(h), int(m))
            if arr_dt <= dep_dt:
                arr_dt += timedelta(days=1)
        except (ValueError, IndexError):
            arr_dt = dep_dt

        flight_no = f.get("flightNo", self.IATA)
        currency = f.get("currency", self.DEFAULT_CURRENCY)
        offer_id = hashlib.md5(f"{self.IATA.lower()}_{req.origin}_{req.destination}_{dep_date}_{flight_no}_{price}".encode()).hexdigest()[:12]

        segment = FlightSegment(
            airline=self.IATA, airline_name=self.AIRLINE_NAME, flight_no=flight_no,
            origin=req.origin, destination=req.destination, departure=dep_dt, arrival=arr_dt, cabin_class="economy",
        )
        route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
        return FlightOffer(
            id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
            price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
            airlines=[self.AIRLINE_NAME], owner_airline=self.IATA,
            booking_url=self._booking_url(req), is_locked=False, source=self.SOURCE, source_tier="free",
        )

    def _booking_url(self, req: FlightSearchRequest) -> str:
        try:
            date_str = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
        except Exception:
            date_str = ""
        return f"https://www.airserbia.com/en?from={req.origin}&to={req.destination}&date={date_str}"

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"ju_rt_{o.id}_{i.id}",
                    price=round(o.price + i.price, 2),
                    currency=o.currency,
                    outbound=o.outbound,
                    inbound=i.outbound,
                    owner_airline=o.owner_airline,
                    airlines=list(set(o.airlines + i.airlines)),
                    source=o.source,
                    booking_url=o.booking_url,
                    conditions=o.conditions,
                ))
        combos.sort(key=lambda x: x.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"airserbia{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
