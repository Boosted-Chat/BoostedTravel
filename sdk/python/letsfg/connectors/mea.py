"""
Middle East Airlines (ME) — CDP Chrome connector — form fill + API intercept.

Middle East Airlines's website at www.mea.com.lb uses Select2 jQuery dropdowns
for airports and a Swiper-based popup calendar for dates. Direct API calls
are blocked by Cloudflare; headed CDP Chrome is required.

Strategy (CDP Chrome + form fill + API interception):
1. Launch headed Chrome via CDP (off-screen, stealth).
2. Navigate to www.mea.com.lb (root, NOT /en) → homepage with booking widget.
3. Set one-way → fill origin/dest via jQuery Select2 API → click Continue.
4. In the popup: click "Travel Date" → navigate Swiper calendar → select day.
5. Click "Confirm" then "Search Flights" → navigates to digital.mea.com.lb/booking.
6. Intercept api-des.mea.com.lb/v2/search/air-bounds JSON response.
7. If API not captured, fall back to DOM scraping of refx- web components.
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

_DEBUG_PORT = 9499
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".mea_chrome_data"
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
            logger.info("MEA: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("MEA: Chrome launched on CDP port %d", _DEBUG_PORT)

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
            document.querySelectorAll(
                '#onetrust-consent-sdk, .cookie-banner, [class*="cookie"], [class*="consent"]'
            ).forEach(el => el.remove());
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = b.textContent.trim().toLowerCase();
                if (t.includes('accept') || t.includes('agree') || t.includes('got it') || t.includes('ok')) {
                    if (b.offsetHeight > 0) { b.click(); break; }
                }
            }
        }""")
    except Exception:
        pass


class MEAConnectorClient:
    """Middle East Airlines (ME) CDP Chrome connector."""

    IATA = "ME"
    AIRLINE_NAME = "Middle East Airlines"
    SOURCE = "mea_direct"
    HOMEPAGE = "https://www.mea.com.lb/"
    DEFAULT_CURRENCY = "USD"

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
            url = response.url
            if response.status not in (200, 201):
                return
            try:
                if "air-bounds" in url:
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    body = await response.text()
                    if len(body) < 100:
                        return
                    data = json.loads(body)
                    if isinstance(data, dict) and "data" in data:
                        search_data.update(data)
                        api_event.set()
                        logger.info("MEA: captured air-bounds API (%d bytes)", len(body))
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            logger.info("MEA: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(self.HOMEPAGE, wait_until="load", timeout=30000)
            await asyncio.sleep(6.0)
            await _dismiss_overlays(page)

            # One-way toggle
            await page.evaluate("() => document.querySelector('#oneWay')?.click()")
            await asyncio.sleep(0.5)

            # Fill airports via jQuery Select2 API
            ok = await self._fill_airport(page, "origin", req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport(page, "destination", req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Click Continue button to open popup
            await page.evaluate("""() => {
                const btn = document.querySelector('.bookATripForm button.roundedButton');
                if (btn) btn.click();
            }""")
            await asyncio.sleep(3.0)
            logger.info("MEA: Continue clicked, popup opening")

            # Fill date in the Swiper calendar popup
            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Click "Search Flights" button in popup
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    if (b.textContent.trim() === 'Search Flights' && b.offsetHeight > 0) {
                        b.click(); return;
                    }
                }
            }""")
            logger.info("MEA: Search Flights clicked")

            # Wait for air-bounds API response or booking page navigation
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                if "digital.mea.com.lb" in url:
                    await asyncio.sleep(5.0)
                    break
                await asyncio.sleep(1.0)

            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    pass

            offers = []
            if search_data:
                offers = self._parse_api_response(search_data, req)
            if not offers:
                offers = await self._scrape_dom(page, req)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("MEA %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"mea{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("MEA error: %s", e)
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

    async def _fill_airport(self, page, direction: str, iata: str) -> bool:
        """Set airport via jQuery Select2 dropdown API."""
        try:
            dropdown = "fromDropdown" if direction == "origin" else "toDropdown"
            ok = await page.evaluate(f"""() => {{
                try {{
                    jQuery("select[name='{dropdown}']").val('{iata}').trigger('change');
                    return true;
                }} catch(e) {{
                    return false;
                }}
            }}""")
            if ok:
                logger.info("MEA: airport %s → %s (Select2)", direction, iata)
            else:
                logger.warning("MEA: Select2 set failed for %s → %s", direction, iata)
            return ok
        except Exception as e:
            logger.warning("MEA: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Select date in the Swiper popup calendar."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        target_day = str(dt.day)
        target_month = dt.strftime("%B %Y").lower()  # e.g. "june 2026"
        try:
            # Click "Travel Date" trigger to open the Swiper calendar
            await page.evaluate("""() => {
                const el = document.querySelector('a.selectDate');
                if (el) el.click();
            }""")
            await asyncio.sleep(1.5)

            # Navigate to the target month and click the day
            clicked = await page.evaluate("""(args) => {
                const [targetMonth, targetDay] = args;
                const months = document.querySelectorAll('.month-container.swiper-slide');
                for (const month of months) {
                    const title = month.querySelector('.month-title');
                    if (!title) continue;
                    if (title.textContent.trim().toLowerCase() === targetMonth) {
                        const days = month.querySelectorAll('.day:not(.disabled):not(.old)');
                        for (const day of days) {
                            const content = day.querySelector('.day-content');
                            const text = (content ? content.textContent : day.textContent).trim();
                            if (text === targetDay) {
                                day.click();
                                return true;
                            }
                        }
                    }
                }
                return false;
            }""", [target_month, target_day])

            if not clicked:
                logger.warning("MEA: could not find %s %s in calendar", target_month, target_day)
                return False

            await asyncio.sleep(1.0)

            # Click Confirm button to lock the date
            await page.evaluate("""() => {
                const btn = document.querySelector('.selectDatesButton');
                if (btn && !btn.classList.contains('disabled')) btn.click();
            }""")
            await asyncio.sleep(1.0)
            logger.info("MEA: date selected %s", dt.strftime("%Y-%m-%d"))
            return True
        except Exception as e:
            logger.warning("MEA: date error: %s", e)
            return False

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse api-des.mea.com.lb/v2/search/air-bounds response."""
        offers = []
        inner = data.get("data", {})
        groups = inner.get("airBoundGroups", [])
        dicts = data.get("dictionaries", {})
        flights_dict = dicts.get("flight", {})
        currency_dict = dicts.get("currency", {})

        for grp in groups:
            bd = grp.get("boundDetails", {})
            seg_refs = bd.get("segments", [])
            duration = bd.get("duration", 0)

            # Use cheapest fare family (lowest total price)
            air_bounds = grp.get("airBounds", [])
            if not air_bounds:
                continue
            cheapest = min(air_bounds, key=lambda ab: self._extract_total(ab))
            price_raw = self._extract_total(cheapest)
            if price_raw <= 0:
                continue

            # Determine currency and decimal places
            currency_code = self.DEFAULT_CURRENCY
            decimal_places = 2
            total_prices = cheapest.get("prices", {}).get("totalPrices", [])
            if total_prices:
                currency_code = total_prices[0].get("currencyCode", self.DEFAULT_CURRENCY)
            if currency_code in currency_dict:
                decimal_places = currency_dict[currency_code].get("decimalPlaces", 2)
            price = price_raw / (10 ** decimal_places)

            # Build segments from dictionaries.flight
            segments = []
            for sr in seg_refs:
                fid = sr.get("flightId", "")
                fd = flights_dict.get(fid, {})
                dep_info = fd.get("departure", {})
                arr_info = fd.get("arrival", {})
                dep_dt = self._parse_dt(dep_info.get("dateTime", ""), req.date_from)
                arr_dt = self._parse_dt(arr_info.get("dateTime", ""), req.date_from)
                airline_code = fd.get("marketingAirlineCode", self.IATA)
                flight_num = fd.get("marketingFlightNumber", "")
                flight_no = f"{airline_code}{flight_num}" if flight_num else airline_code

                cab = "economy"
                avail = cheapest.get("availabilityDetails", [])
                for av in avail:
                    if av.get("flightId") == fid:
                        cb = av.get("cabin", "eco")
                        cab = "business" if cb == "business" else "economy"
                        break

                segments.append(FlightSegment(
                    airline=airline_code, airline_name=self.AIRLINE_NAME if airline_code == self.IATA else airline_code,
                    flight_no=flight_no, origin=dep_info.get("locationCode", req.origin),
                    destination=arr_info.get("locationCode", req.destination),
                    departure=dep_dt, arrival=arr_dt, cabin_class=cab,
                ))

            if not segments:
                continue

            route = FlightRoute(segments=segments, total_duration_seconds=duration, stopovers=max(0, len(segments) - 1))
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{req.origin}_{req.destination}_{req.date_from}_{price}_{segments[0].flight_no}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency_code,
                price_formatted=f"{currency_code} {price:,.2f}", outbound=route, inbound=None,
                airlines=list({s.airline for s in segments}), owner_airline=self.IATA,
                booking_url=self._booking_url(req), is_locked=False,
                source=self.SOURCE, source_tier="free",
            ))
        return offers

    @staticmethod
    def _extract_total(air_bound: dict) -> int:
        """Extract total price (in cents) from an airBound."""
        total_prices = air_bound.get("prices", {}).get("totalPrices", [])
        if total_prices:
            return total_prices[0].get("total", 0)
        return 0

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
        """Fallback: scrape flight data from refx web components on the booking page."""
        await asyncio.sleep(3)
        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];
            const cards = document.querySelectorAll(
                'refx-flight-card-pres, refx-upsell-premium-row-pres, ' +
                '[class*="flight-card"], [class*="bound-card"], [class*="flight-row"]'
            );
            for (const card of cards) {
                const text = card.innerText || '';
                if (text.length < 20) continue;
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;
                const priceMatch = text.match(/(USD|EUR|GBP)\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(USD|EUR|GBP)/i);
                if (!priceMatch) continue;
                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0) continue;
                let currency = 'USD';
                if (/EUR|€/.test(priceMatch[0])) currency = 'EUR';
                else if (/GBP|£/.test(priceMatch[0])) currency = 'GBP';
                const fnMatch = text.match(/\b(ME\s*\d{2,4})\b/i);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'ME',
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
        return f"https://www.mea.com.lb/?from={req.origin}&to={req.destination}&date={date_str}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"mea{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
