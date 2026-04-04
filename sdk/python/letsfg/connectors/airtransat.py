"""
Air Transat (TS) — CDP Chrome connector — form fill + API intercept.

Air Transat's website at www.airtransat.com uses a search widget with autocomplete
airport fields and calendar date picker. Direct API calls are blocked;
headed CDP Chrome with form fill + API interception is required.

Strategy (CDP Chrome + API interception):
1. Launch headed Chrome via CDP (off-screen, stealth).
2. Navigate to airchina.com → SPA loads with search widget.
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

_DEBUG_PORT = 9496
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".airtransat_chrome_data"
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
            logger.info("AirTransat: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("AirTransat: Chrome launched on CDP port %d", _DEBUG_PORT)

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
        # First try clicking the OneTrust accept button properly
        await page.evaluate("""() => {
            const accept = document.querySelector('#onetrust-accept-btn-handler');
            if (accept && accept.offsetHeight > 0) { accept.click(); return; }
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = b.textContent.trim().toLowerCase();
                if ((t.includes('accept') || t.includes('agree') || t.includes('got it'))
                    && b.offsetHeight > 0) { b.click(); return; }
            }
        }""")
        await asyncio.sleep(1.0)
        # Force remove any remaining overlays/dark filters/popups
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter, ' +
                '#newsletter-popin, [class*="newsletter-popin"], ' +
                '[class*="cookie"], [class*="consent"]'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class AirTransatConnectorClient:
    """Air Transat (TS) CDP Chrome connector."""

    IATA = "TS"
    AIRLINE_NAME = "Air Transat"
    SOURCE = "airtransat_direct"
    HOMEPAGE = "https://www.airtransat.com/en-CA"
    DEFAULT_CURRENCY = "CAD"

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            url = response.url.lower()
            if response.status not in (200, 201):
                return
            try:
                if any(k in url for k in ["/search", "/availability", "/flight",
                                           "/offer", "/fare", "/lowprice", "/schedule",
                                           "/shopping", "/price", "/getfares"]):
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct and "text" not in ct:
                        return
                    body = await response.text()
                    if len(body) < 50:
                        return
                    data = json.loads(body)
                    if not isinstance(data, dict):
                        return
                    keys_str = " ".join(str(k).lower() for k in data.keys())
                    if any(k in keys_str for k in ["flight", "itiner", "offer", "fare",
                                                     "bound", "trip", "result", "segment",
                                                     "avail", "journey", "price",
                                                     "flightleginfo", "fliid"]):
                        search_data.update(data)
                        api_event.set()
                        logger.info("AirTransat: captured API → %s (%d keys)", url[:80], len(data))
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            logger.info("AirTransat: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(self.HOMEPAGE, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5.0)
            await _dismiss_overlays(page)

            # One-way toggle — Air Transat uses li tab with .OW class
            await page.evaluate("""() => {
                const ow = document.querySelector('li.co-tab.OW');
                if (ow && ow.offsetHeight > 0) { ow.click(); return; }
                const els = document.querySelectorAll('li, a, button');
                for (const el of els) {
                    const t = (el.textContent || '').trim().toLowerCase();
                    if (t.includes('one-way') && el.offsetHeight > 0) { el.click(); return; }
                }
            }""")
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, '#departureOriginDropdown-input', req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, '#departureDestinationDropdown-input', req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            # Re-dismiss overlays (newsletter popup may appear after delay)
            await _dismiss_overlays(page)

            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Click search — Air Transat: "Continue" button
            await page.evaluate("""() => {
                const btn = document.querySelector('button.stepSetContinue');
                if (btn && btn.offsetHeight > 0) { btn.click(); return; }
                const btns = document.querySelectorAll('button[type="submit"]');
                for (const b of btns) {
                    const t = (b.textContent || '').trim().toLowerCase();
                    if ((t.includes('continue') || t.includes('search') || t.includes('find'))
                        && b.offsetHeight > 0) { b.click(); return; }
                }
            }""")
            logger.info("AirTransat: search clicked")

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                if any(k in url.lower() for k in ["result", "search", "flight", "availability"]):
                    await asyncio.sleep(4.0)
                    break
                await asyncio.sleep(1.0)

            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=8.0)
                except asyncio.TimeoutError:
                    pass

            offers = []
            if search_data:
                offers = self._parse_api_response(search_data, req)
            if not offers:
                offers = await self._scrape_dom(page, req)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("AirTransat %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"airtransat{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("AirTransat error: %s", e)
            return self._empty(req)
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_airport(self, page, selector: str, iata: str) -> bool:
        """Fill Air Transat co-dropdown airport field."""
        try:
            field = page.locator(selector).first
            await field.click(timeout=5000)
            await asyncio.sleep(0.5)
            await page.keyboard.press("Control+a")
            await page.keyboard.press("Backspace")
            await field.type(iata, delay=120)
            await asyncio.sleep(2.0)

            selected = await page.evaluate("""(iata) => {
                const opts = document.querySelectorAll(
                    '[role="option"], [class*="suggest"] li, [class*="dropdown"] li, ' +
                    '[class*="autocomplete"] li, .search-result-item, ' +
                    '.co-dropdown-list li, .co-dropdown-option'
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
            logger.info("AirTransat: airport %s → %s", selector[-30:], iata)
            return True
        except Exception as e:
            logger.warning("AirTransat: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill Air Transat VDP date picker for departure."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        target_day = str(dt.day)
        target_month = dt.strftime("%B %Y")
        iso = dt.strftime("%Y-%m-%d")
        try:
            # Click departure date field or its calendar button to open picker
            dep_field = page.locator('#datePickerDeparture').first
            await dep_field.click(timeout=5000, force=True)
            await asyncio.sleep(1.5)

            # Try calendar button if picker didn't open
            cal_open = await page.evaluate("""() => {
                return document.querySelector(
                    '.vdp-datepicker__calendar, [class*="calendar"][class*="open"], ' +
                    '[class*="datepicker"][class*="open"], .vdpTable'
                ) !== null;
            }""")
            if not cal_open:
                await page.evaluate("""() => {
                    const btn = document.querySelector('button.vdpBtnCalendar.icon-calendar');
                    if (btn) btn.click();
                }""")
                await asyncio.sleep(1.5)

            # Navigate to target month — VDP v2 uses .vdpPeriodControl for month text
            for _ in range(12):
                month_text = await page.evaluate("""() => {
                    const h = document.querySelectorAll(
                        '.vdpPeriodControl, .vdpPeriodControls, ' +
                        '.vdp-datepicker__calendar header span, ' +
                        '[class*="calendar"] [class*="month"], .month-title, th[colspan]'
                    );
                    return [...h].map(e => e.textContent.trim()).join('|');
                }""")
                if target_month.lower() in month_text.lower():
                    break
                await page.evaluate("""() => {
                    const n = document.querySelector(
                        '.vdpArrowNext, .vdpArrow.vdpArrowNext, ' +
                        '.vdp-datepicker__calendar .next, ' +
                        '[aria-label*="next" i], [aria-label*="Next"]'
                    );
                    if (n && n.offsetHeight > 0) n.click();
                }""")
                await asyncio.sleep(0.5)

            # Click the target day — VDP v2 uses .vdpCell.selectable with button/span children
            clicked = await page.evaluate("""(day) => {
                // VDP v2 cells
                const cells = document.querySelectorAll('.vdpCell.selectable');
                for (const c of cells) {
                    const text = c.textContent.trim();
                    if (text === day && c.offsetHeight > 0) {
                        c.click(); return true;
                    }
                }
                // VDP v1 fallback
                const spans = document.querySelectorAll(
                    '.vdp-datepicker__calendar td span, td[class*="day"], ' +
                    'td[role="gridcell"], [class*="calendar-day"]'
                );
                for (const c of spans) {
                    const text = c.textContent.trim();
                    if (text === day && !c.classList.contains('disabled') &&
                        c.getAttribute('aria-disabled') !== 'true' && c.offsetHeight > 0) {
                        c.click(); return true;
                    }
                }
                return false;
            }""", target_day)
            if clicked:
                logger.info("AirTransat: date selected %s", iso)
            else:
                logger.warning("AirTransat: could not click day %s in %s", target_day, target_month)
            await asyncio.sleep(1.0)
            return clicked
        except Exception as e:
            logger.warning("AirTransat: date error: %s", e)
            return False

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []
        # Transat shopping/Price/Grid or Fares/GetFares structure
        fares = data.get("fares") or data.get("flightLegInfos") or []
        if isinstance(fares, list) and fares:
            for fare in fares:
                if not isinstance(fare, dict):
                    continue
                price = fare.get("price") or fare.get("totalPrice") or fare.get("amount") or 0
                if isinstance(price, dict):
                    price = price.get("amount") or price.get("total") or 0
                price = float(price) if price else 0
                if price <= 0:
                    continue
                currency = fare.get("currency") or fare.get("currencyCode") or self.DEFAULT_CURRENCY
                dep_str = fare.get("scheduledDeparture") or fare.get("departure") or ""
                arr_str = fare.get("scheduledArrival") or fare.get("arrival") or ""
                dep_dt = self._parse_dt(dep_str, req.date_from)
                arr_dt = self._parse_dt(arr_str, req.date_from)
                origin = fare.get("origin") or req.origin
                dest = fare.get("destination") or req.destination
                flight_no = fare.get("flightNo") or fare.get("flightNumber") or ""
                if not flight_no:
                    flight_no = f"TS{fare.get('fliid', '')}"

                segment = FlightSegment(
                    airline=self.IATA, airline_name=self.AIRLINE_NAME,
                    flight_no=flight_no, origin=origin, destination=dest,
                    departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                )
                route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
                offer_id = hashlib.md5(
                    f"{self.IATA.lower()}_{origin}_{dest}_{req.date_from}_{price}_{flight_no}".encode()
                ).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
                    price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
                    airlines=[self.AIRLINE_NAME], owner_airline=self.IATA,
                    booking_url=self._booking_url(req), is_locked=False,
                    source=self.SOURCE, source_tier="free",
                ))
            if offers:
                return offers

        # Generic fallback parser
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
            logger.debug("AirTransat: offer parse error: %s", e)
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
        await asyncio.sleep(3)
        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];
            // Air Transat results page: flight cards with class containing "flight"
            const cards = document.querySelectorAll(
                '[class*="flight-card"], [class*="flight-row"], [class*="itinerary"], ' +
                '[class*="result-card"], [class*="bound"], [class*="flight-item"], ' +
                '[class*="flightInfo"], [class*="flight_item"], [class*="flight-result"]'
            );
            for (const card of cards) {
                const text = card.innerText || '';
                if (text.length < 20) continue;
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;
                const priceMatch = text.match(/\$\s*[\d,]+\.?\d*/i) ||
                                   text.match(/(CAD|USD|EUR|€)\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(CAD|USD|EUR)/i);
                if (!priceMatch) continue;
                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0) continue;
                let currency = 'CAD';
                if (/USD/.test(priceMatch[0])) currency = 'USD';
                else if (/EUR|€/.test(priceMatch[0])) currency = 'EUR';
                const fnMatch = text.match(/\b(TS\s*\d{2,4})\b/i) || text.match(/\b([A-Z]{2}\s*\d{2,4})\b/);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'TS',
                });
            }
            // Fallback: look for "Starting from $XXX" patterns on the page
            if (results.length === 0) {
                const allText = document.body.innerText || '';
                const flights = allText.match(/(\d{1,2}:\d{2})[\s\S]{1,200}?(\d{1,2}:\d{2})[\s\S]{1,300}?\$\s*([\d,]+)/g) || [];
                for (const block of flights) {
                    const times = block.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                    if (times.length < 2) continue;
                    const pm = block.match(/\$\s*([\d,]+)/);
                    if (!pm) continue;
                    const price = parseFloat(pm[1].replace(/,/g, ''));
                    if (!price || price < 10) continue;
                    const fn = block.match(/\b(TS\d{2,4})\b/i);
                    results.push({
                        depTime: times[0], arrTime: times[1], price, currency: 'CAD',
                        flightNo: fn ? fn[1] : 'TS',
                    });
                }
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
        return f"https://www.airtransat.com/en-CA?from={req.origin}&to={req.destination}&date={date_str}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"airtransat{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
