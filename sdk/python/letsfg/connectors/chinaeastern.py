"""
China Eastern Airlines (MU) — CDP Chrome connector — form fill + API intercept.

China Eastern Airlines's website at us.ceair.com uses a search widget with autocomplete
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

_DEBUG_PORT = 9492
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".chinaeastern_chrome_data"
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
            logger.info("ChinaEastern: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("ChinaEastern: Chrome launched on CDP port %d", _DEBUG_PORT)

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
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter, ' +
                '[class*="cookie"], [class*="consent"], [class*="overlay"]'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class ChinaEasternConnectorClient:
    """China Eastern Airlines (MU) CDP Chrome connector."""

    IATA = "MU"
    AIRLINE_NAME = "China Eastern Airlines"
    SOURCE = "chinaeastern_direct"
    HOMEPAGE = "https://us.ceair.com"
    DEFAULT_CURRENCY = "CNY"

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
                                           "/briefinfo", "/shopping/brief", "/price",
                                           "/summaryprice", "/queryflightinfo"]):
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
                                                     "flightitem", "briefinfo", "summaryprice"]):
                        # Only update if this response has actual data (not empty/error)
                        if data.get("data") is not None or not search_data:
                            search_data.update(data)
                        api_event.set()
                        logger.info("ChinaEastern: captured API → %s (%d keys)", url[:80], len(data))
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            logger.info("ChinaEastern: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(self.HOMEPAGE, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5.0)
            await _dismiss_overlays(page)

            # Switch to International tab (defaults to domestic)
            await page.evaluate("""() => {
                const radios = document.querySelectorAll('input.ceair-radio-button__orig-radio');
                for (const r of radios) {
                    if (r.value === 'international') {
                        const label = r.closest('label') || r.parentElement;
                        if (label) { label.click(); return; }
                        r.click(); return;
                    }
                }
            }""")
            await asyncio.sleep(1.0)

            # One-way toggle — China Eastern uses ceair-radio with value="oneway"
            await page.evaluate("""() => {
                const radios = document.querySelectorAll('input.ceair-radio__original');
                for (const r of radios) {
                    if (r.value === 'oneway') {
                        const label = r.closest('label') || r.parentElement;
                        if (label) { label.click(); return; }
                        r.click(); return;
                    }
                }
            }""")
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, 'input[aria-label="From"]', req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, 'input[aria-label="To"]', req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Click search — China Eastern: button.submit-btn
            await page.evaluate("""() => {
                const btn = document.querySelector('button.submit-btn');
                if (btn && btn.offsetHeight > 0) { btn.click(); return; }
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    const t = (b.textContent || '').trim().toLowerCase();
                    if ((t === 'search' || t.includes('search') || t.includes('查询') || t.includes('搜索'))
                        && b.offsetHeight > 0) { b.click(); return; }
                }
            }""")
            logger.info("ChinaEastern: search clicked")

            # Wait briefly then check if URL contains the wrong date — direct-navigate if needed
            await asyncio.sleep(3.0)
            cur_url = page.url
            expected_date = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
            if "/shopping/" in cur_url and expected_date not in cur_url:
                # URL has wrong date — reconstruct with correct date
                m = re.search(r'/shopping/(oneway|roundtrip)/([^/]+)/', cur_url)
                if m:
                    trip_type = m.group(1)
                    route_part = m.group(2)
                    fixed_url = f"https://www.ceair.com/shopping/{trip_type}/{route_part}/{expected_date}"
                    logger.info("ChinaEastern: fixing URL date: %s", fixed_url)
                    # Clear stale data from wrong-date response before re-navigating
                    search_data.clear()
                    api_event.clear()
                    await page.goto(fixed_url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(6.0)  # wait for briefInfo to fire on correct date

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                if any(k in url.lower() for k in ["result", "search", "flight", "availability", "shopping"]):
                    await asyncio.sleep(6.0)
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
            logger.info("ChinaEastern %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"chinaeastern{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("ChinaEastern error: %s", e)
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
        """Fill China Eastern ceair-input autocomplete airport field."""
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
                    '[class*="autocomplete"] li, [class*="city-item"], ' +
                    '[class*="ceair-select"] li, .search-result-item'
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
            logger.info("ChinaEastern: airport %s → %s", selector[-20:], iata)
            return True
        except Exception as e:
            logger.warning("ChinaEastern: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill China Eastern ceair date picker."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        iso = dt.strftime("%Y-%m-%d")
        try:
            # China Eastern uses a ceair date picker — try setting value via JS first
            ok = await page.evaluate("""(args) => {
                const [iso, formatted] = args;
                const el = document.querySelector('input[aria-label="Departure"]');
                if (!el) return 'no-element';
                const nativeSetter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value'
                ).set;
                nativeSetter.call(el, formatted);
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                return 'set';
            }""", [iso, dt.strftime("%Y %b %d")])
            logger.info("ChinaEastern: date value set: %s → %s", ok, iso)

            # Click the departure field to open calendar
            dep_field = page.locator('input[aria-label="Departure"]').first
            await dep_field.click(timeout=5000)
            await asyncio.sleep(1.5)

            target_day = str(dt.day)
            target_ym = dt.strftime("%Y-%m")
            # Navigate calendar to target month
            for _ in range(12):
                month_text = await page.evaluate("""() => {
                    const h = document.querySelectorAll(
                        '[class*="calendar"] [class*="month"], [class*="calendar"] [class*="title"], ' +
                        '.month-title, .calendar-title, th[colspan]'
                    );
                    return [...h].map(e => e.textContent.trim()).join('|');
                }""")
                if target_ym in month_text or dt.strftime("%B %Y").lower() in month_text.lower() or f"{dt.year}年{dt.month}月" in month_text:
                    break
                await page.evaluate("""() => {
                    const n = document.querySelector(
                        '[class*="next"], [aria-label*="next"], [class*="forward"], ' +
                        'button[class*="arrow-right"], .next-month, [class*="right-arrow"]'
                    );
                    if (n && n.offsetHeight > 0) n.click();
                }""")
                await asyncio.sleep(0.5)

            # Click the target day
            clicked = await page.evaluate("""(day) => {
                const cells = document.querySelectorAll(
                    'td[class*="day"], td[role="gridcell"], [class*="calendar-day"], ' +
                    '[class*="date-cell"], .day, td.available'
                );
                for (const c of cells) {
                    const text = c.textContent.trim();
                    if (text === day && !c.classList.contains('disabled') &&
                        c.getAttribute('aria-disabled') !== 'true' && c.offsetHeight > 0) {
                        c.click(); return true;
                    }
                }
                return false;
            }""", target_day)
            if clicked:
                logger.info("ChinaEastern: date clicked %s", iso)
            await asyncio.sleep(1.0)
            return True
        except Exception as e:
            logger.warning("ChinaEastern: date error: %s", e)
            return False

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []

        # China Eastern briefInfo structure: data.flightItems[].flightInfos[].flightSegments[]
        flight_items = None
        d = data.get("data")
        if isinstance(d, dict):
            flight_items = d.get("flightItems")
        if isinstance(flight_items, list) and flight_items:
            return self._parse_brief_info(flight_items, data, req)

        # Generic fallback
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

    def _parse_brief_info(self, flight_items: list, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse China Eastern briefInfo API response structure."""
        offers = []
        # Collect fare prices from segmentLowestPriceList or inline fares
        fare_prices = {}
        for seg_price in (data.get("segmentLowestPriceList") or []):
            if seg_price.get("lowestPrice"):
                fare_prices[seg_price.get("flightDate", "")] = float(seg_price["lowestPrice"])

        for item in flight_items:
            for info in (item.get("flightInfos") or []):
                segments_raw = info.get("flightSegments") or []
                if not segments_raw:
                    continue

                # Skip sold-out flights
                fare_ids = info.get("flightFareIds") or []
                if fare_ids == ["soldOut"] or (len(fare_ids) == 1 and fare_ids[0] == "soldOut"):
                    continue

                # Extract price — China Eastern stores in flightSort or cabinInfoDescs
                price = 0.0
                currency = "CNY"
                flight_sort = info.get("flightSort")
                if isinstance(flight_sort, dict):
                    price = float(flight_sort.get("price") or flight_sort.get("priceWithTax") or 0)

                # Try cabinInfoDescs for more accurate per-cabin pricing
                if not price:
                    for cab in (item.get("cabinInfoDescs") or []):
                        for fare_desc in (cab.get("fareInfoDescList") or []):
                            lp = fare_desc.get("lprice") or fare_desc.get("totalPrice")
                            if lp and float(lp) > 0:
                                price = float(lp)
                                break
                        if price > 0:
                            break

                # Fallback to flightFareInfos lookup
                if not price:
                    for fare_id in fare_ids:
                        if isinstance(fare_id, str) and fare_id != "soldOut":
                            fare_details = (data.get("data", {}).get("flightFareInfos") or {}).get(fare_id)
                            if isinstance(fare_details, dict):
                                price = float(fare_details.get("adultPrice") or fare_details.get("price") or
                                              fare_details.get("totalPrice") or 0)
                                currency = fare_details.get("currencyCode") or currency
                                break

                # Another fallback from segmentLowestPriceList
                if not price:
                    flt_date = segments_raw[0].get("fltDate", "")
                    price = fare_prices.get(flt_date, 0)

                if price <= 0:
                    continue

                segments = []
                total_dur = info.get("duration") or 0
                for seg in segments_raw:
                    dep_date = seg.get("fltDate", str(req.date_from))
                    dep_time = seg.get("orgTime", "00:00")
                    arr_date = seg.get("arriDate", dep_date)
                    arr_time = seg.get("destTime", "00:00")
                    dep_dt = self._parse_dt(f"{dep_date}T{dep_time}", req.date_from)
                    arr_dt = self._parse_dt(f"{arr_date}T{arr_time}", req.date_from)

                    carrier = seg.get("carrierCode") or seg.get("airlineCode") or self.IATA
                    fno = seg.get("flightNo") or seg.get("carrierNo") or ""
                    full_fno = f"{carrier}{fno}" if fno and not fno.startswith(carrier) else fno

                    segments.append(FlightSegment(
                        airline=carrier[:2],
                        airline_name=seg.get("airlineCodeName") or self.AIRLINE_NAME,
                        flight_no=full_fno or f"{self.IATA}???",
                        origin=seg.get("orgCode") or req.origin,
                        destination=seg.get("destCode") or req.destination,
                        departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                    ))

                if not segments:
                    continue

                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=total_dur * 60 if total_dur else 0,
                    stopovers=max(0, len(segments) - 1),
                )
                offer_id = hashlib.md5(
                    f"{self.IATA.lower()}_{segments[0].origin}_{segments[-1].destination}_{segments[0].departure}_{price}_{segments[0].flight_no}".encode()
                ).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
                    price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
                    airlines=list({s.airline for s in segments}), owner_airline=self.IATA,
                    booking_url=self._booking_url(req), is_locked=False,
                    source=self.SOURCE, source_tier="free",
                ))
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
            logger.debug("ChinaEastern: offer parse error: %s", e)
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
                const priceMatch = text.match(/(CNY|USD|EUR|¥|\$|€)\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(CNY|USD|EUR|¥|\$|€)/i);
                if (!priceMatch) continue;
                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0) continue;
                let currency = 'CNY';
                if (/USD|\$/.test(priceMatch[0])) currency = 'USD';
                else if (/EUR|€/.test(priceMatch[0])) currency = 'EUR';
                const fnMatch = text.match(/\b(MU\s*\d{2,4})\b/i) || text.match(/\b([A-Z]{2}\s*\d{2,4})\b/);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'MU',
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
        return f"https://us.ceair.com?from={req.origin}&to={req.destination}&date={date_str}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"chinaeastern{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
