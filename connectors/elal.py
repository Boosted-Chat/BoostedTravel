"""
El Al Israel Airlines (LY) — CDP Chrome connector — Angular 18 form fill + API intercept.

El Al's website at elal.com is an Angular 18 SPA with a search widget on the homepage.
Direct API calls fail (/en/ paths return 403); the working path is the geo-redirected
homepage (e.g. /eng/poland) which renders the full Angular app with search form.

Strategy (CDP Chrome + API interception):
1. Launch headed Chrome via CDP (stealth, off-screen).
2. Navigate to elal.com → auto-redirects to /eng/<country>.
3. Accept cookies → fill origin/destination/date inputs → click "Search >".
4. Intercept the search API response (JSON flights data).
5. If API not captured, fall back to DOM scraping on the results page.
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

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9479
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".elal_chrome_data"
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
    """Persistent Chrome via CDP (headed — reCAPTCHA blocks headless)."""
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
            logger.info("El Al: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("El Al: Chrome launched on CDP port %d", _DEBUG_PORT)

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    """Wipe Chrome profile when session is broken."""
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
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
        except Exception:
            pass


async def _dismiss_overlays(page) -> None:
    """Accept cookies dialog (OneTrust) and close any geo/agency selector overlays."""
    try:
        btn = page.locator("button:has-text('Accept All')")
        if await btn.count() > 0:
            await btn.first.click(timeout=3000)
            await asyncio.sleep(0.5)
    except Exception:
        pass
    try:
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter, #onetrust-banner-sdk'
            ).forEach(el => el.remove());
            // Close any agency/geo-selector overlays that might be open
            document.querySelectorAll('.cdk-overlay-container .cdk-overlay-pane').forEach(el => el.remove());
            // Also close any agency dropdown that's already open
            document.querySelectorAll('.agency-option, .agency-dropdown, [class*="agency"]').forEach(el => {
                if (el.closest('.cdk-overlay-pane')) el.closest('.cdk-overlay-pane').remove();
                else el.remove();
            });
        }""")
    except Exception:
        pass


class ElAlConnectorClient:
    """El Al (LY) CDP Chrome connector — Angular form fill + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()

        # Interception state
        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            url = response.url.lower()
            if response.status != 200:
                return
            try:
                if any(k in url for k in ["/api/flights", "/api/search", "/api/availability",
                                           "cheapest", "fare-calendar", "lowest"]):
                    data = await response.json()
                    if isinstance(data, dict):
                        # Heuristic: flight search results contain itinerary/flights/offers keys
                        keys_str = " ".join(str(k).lower() for k in data.keys())
                        if any(k in keys_str for k in ["flight", "itiner", "offer", "route",
                                                         "bound", "trip", "result", "fare"]):
                            search_data.update(data)
                            api_event.set()
                            logger.info("El Al: captured search API → %s (%d keys)", url[:100], len(data))
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            # Step 1: Load homepage (redirects to /eng/<country>)
            logger.info("El Al: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto("https://www.elal.com/", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5.0)
            await _dismiss_overlays(page)
            await asyncio.sleep(0.5)

            # Check if blocked
            title = await page.title()
            if "error" in title.lower() or "denied" in title.lower():
                logger.warning("El Al: blocked (%s)", title)
                await _reset_profile()
                return self._empty(req)

            # Step 2: Click "One way" if available, or just proceed (round-trip default)
            # El Al may not have explicit one-way toggle on homepage — we search one-way
            # by only filling departure date, not return

            # Step 3: Fill origin airport
            ok = await self._fill_airport(page, "#outbound-origin-location-input", req.origin)
            if not ok:
                logger.warning("El Al: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(1.0)

            # Step 4: Fill destination airport
            ok = await self._fill_airport(page, "#outbound-destination-location-input", req.destination)
            if not ok:
                logger.warning("El Al: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(1.0)

            # Step 5: Fill departure date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("El Al: date fill failed")
                return self._empty(req)

            # Step 6: Click "Search >"
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    if (b.textContent.trim().includes('Search') && b.offsetHeight > 0) {
                        b.click(); return;
                    }
                }
            }""")
            logger.info("El Al: search clicked")

            # Step 7: Wait for results
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining

            # Wait for either API capture or navigation to results page
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                if "search" in url.lower() and "result" in url.lower():
                    await asyncio.sleep(3.0)
                    break
                if "booking.elal" in url.lower():
                    await asyncio.sleep(5.0)
                    break
                if "flight" in url.lower() and ("select" in url.lower() or "result" in url.lower()):
                    await asyncio.sleep(3.0)
                    break
                await asyncio.sleep(1.0)

            # Give extra time for API to arrive after page load
            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=8.0)
                except asyncio.TimeoutError:
                    pass

            # Step 8: Build offers from captured API data
            offers = []
            if search_data:
                offers = self._parse_api_response(search_data, req)
                logger.info("El Al: parsed %d offers from API", len(offers))

            # Step 9: DOM fallback if no API data
            if not offers:
                offers = await self._scrape_dom(page, req)
                if offers:
                    logger.info("El Al: scraped %d offers from DOM", len(offers))

            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info("El Al %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"elal{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            currency = offers[0].currency if offers else "USD"
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("El Al error: %s", e)
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

    # ------------------------------------------------------------------
    # Form interaction helpers
    # ------------------------------------------------------------------

    async def _fill_airport(self, page, selector: str, iata: str) -> bool:
        """Fill an El Al Angular autocomplete field with IATA code."""
        try:
            field = page.locator(selector)
            if await field.count() == 0:
                return False

            # Dismiss any open agency/country overlay before interacting
            await page.evaluate("""() => {
                document.querySelectorAll('.cdk-overlay-container .cdk-overlay-pane').forEach(el => {
                    if (el.querySelector('.agency-option') || el.querySelector('a.agency-option')) el.remove();
                });
            }""")
            await asyncio.sleep(0.3)

            # Click to activate
            await field.click(timeout=5000)
            await asyncio.sleep(0.5)

            # Remove agency dropdown that appeared on click
            await page.evaluate("""() => {
                document.querySelectorAll('.cdk-overlay-container .cdk-overlay-pane').forEach(el => {
                    if (el.querySelector('.agency-option')) el.remove();
                });
            }""")
            await asyncio.sleep(0.2)

            # Triple-click to select all, then delete
            await field.click(click_count=3, timeout=3000)
            await asyncio.sleep(0.2)
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.3)

            # Remove agency overlay AGAIN after clearing
            await page.evaluate("""() => {
                document.querySelectorAll('.cdk-overlay-container .cdk-overlay-pane').forEach(el => {
                    if (el.querySelector('.agency-option')) el.remove();
                });
            }""")
            await asyncio.sleep(0.2)

            # Type IATA code
            await field.type(iata, delay=150)
            await asyncio.sleep(3.0)

            # Remove agency dropdown one more time (may reappear per keystroke)
            await page.evaluate("""() => {
                document.querySelectorAll('.cdk-overlay-container .cdk-overlay-pane').forEach(el => {
                    if (el.querySelector('.agency-option')) el.remove();
                });
            }""")
            await asyncio.sleep(0.5)

            # Click matching airport option — skip agency/country items
            selected = await page.evaluate("""(iata) => {
                const selectors = [
                    '[role="option"]:not(.agency-option)',
                    'mat-option', '.mat-option',
                    '.cdk-overlay-container li',
                    '[class*="autocomplete"] li', '[class*="location"] li',
                    '[class*="airport"]', '[class*="search-result"]'
                ];
                const all = document.querySelectorAll(selectors.join(','));
                for (const opt of all) {
                    const text = (opt.textContent || '').trim();
                    if (text.includes(iata) && opt.offsetHeight > 0 &&
                        !opt.classList.contains('agency-option') &&
                        !text.includes('(\u05e2\u05d1\u05e8\u05d9\u05ea)') && !text.includes('(English)') &&
                        !text.includes('Austria') && !text.includes('Belgium')) {
                        opt.click();
                        return text.slice(0, 80);
                    }
                }
                return null;
            }""", iata)

            if not selected:
                # Set value via JS and trigger Angular change detection
                await page.evaluate("""(args) => {
                    const [sel, iata] = args;
                    const field = document.querySelector(sel);
                    if (field) {
                        field.value = iata;
                        field.dispatchEvent(new Event('input', {bubbles: true}));
                        field.dispatchEvent(new Event('change', {bubbles: true}));
                    }
                }""", [selector, iata])
                await asyncio.sleep(1.0)
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.3)
                await page.keyboard.press("Enter")

            await asyncio.sleep(0.5)
            value = await field.input_value()
            if iata.upper() in (value or "").upper() or len(value or "") > 2:
                logger.info("El Al: airport %s → '%s'", selector[-30:], value)
                return True

            logger.warning("El Al: airport fill for %s got '%s'", iata, value)
            return bool(value)

        except Exception as e:
            logger.warning("El Al: airport error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open calendar and select departure date."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False

        target_month = dt.strftime("%B %Y")  # e.g. "April 2026"
        target_day = str(dt.day)

        try:
            # Click departure date input — ID contains comma, use attribute selector
            date_input = page.locator('[id="outbound-departure,return-calendar-input"]')
            if await date_input.count() == 0:
                date_input = page.locator("[id*='departure'][id*='calendar']")
            if await date_input.count() == 0:
                date_input = page.locator("[id*='departure'][id*='return']")
            await date_input.first.click(timeout=5000)
            await asyncio.sleep(1.5)

            # Navigate calendar to target month
            for _ in range(12):
                visible_text = await page.evaluate("""() => {
                    const headers = document.querySelectorAll(
                        '[class*="calendar"] [class*="header"], [class*="month-title"], ' +
                        '[class*="calendar-title"], .mat-calendar-period-button, ' +
                        '.mat-calendar-body-label, [class*="month-name"]'
                    );
                    return [...headers].map(h => h.textContent.trim()).filter(Boolean).join('|');
                }""")
                if target_month.lower() in visible_text.lower():
                    break
                # Click next month button
                await page.evaluate("""() => {
                    const next = document.querySelector(
                        '[class*="next"], [aria-label*="next"], [class*="forward"], ' +
                        '.mat-calendar-next-button, button[class*="arrow-right"]'
                    );
                    if (next && next.offsetHeight > 0) next.click();
                }""")
                await asyncio.sleep(0.5)

            # Click the target day
            clicked = await page.evaluate("""(args) => {
                const [targetDay, targetMonth] = args;
                // Find calendar day cells
                const cells = document.querySelectorAll(
                    '[class*="calendar"] [class*="day"], ' +
                    '.mat-calendar-body-cell, ' +
                    'td[role="gridcell"], ' +
                    '[class*="date-cell"]'
                );
                for (const cell of cells) {
                    const text = cell.textContent.trim();
                    const isDisabled = cell.classList.contains('disabled') ||
                                       cell.getAttribute('aria-disabled') === 'true' ||
                                       cell.classList.contains('mat-calendar-body-disabled');
                    if (text === targetDay && !isDisabled && cell.offsetHeight > 0) {
                        cell.click();
                        return true;
                    }
                }
                return false;
            }""", [target_day, target_month])

            if not clicked:
                logger.warning("El Al: could not click day %s in %s", target_day, target_month)
                # Try typing the date directly
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.3)
                date_str = dt.strftime("%d/%m/%Y")
                await date_input.first.click(timeout=3000)
                await page.keyboard.press("Control+a")
                await date_input.first.type(date_str, delay=50)
                await page.keyboard.press("Enter")

            await asyncio.sleep(1.0)
            logger.info("El Al: date selected for %s", dt.strftime("%Y-%m-%d"))
            return True

        except Exception as e:
            logger.warning("El Al: date error: %s", e)
            return False

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the El Al search API response into FlightOffers."""
        offers = []

        # Try multiple possible response shapes
        flights_list = (
            data.get("flights") or data.get("results") or data.get("itineraries") or
            data.get("outbound") or data.get("trips") or data.get("offers") or
            data.get("flightResults") or data.get("bounds") or []
        )

        if isinstance(flights_list, dict):
            # Might be nested: {outbound: {flights: [...]}}
            for key in ("flights", "results", "itineraries", "options"):
                if key in flights_list:
                    flights_list = flights_list[key]
                    break
            else:
                flights_list = [flights_list]

        if not isinstance(flights_list, list):
            # Try to find flight data recursively
            flights_list = self._find_flights_in_data(data)

        for flight in flights_list:
            offer = self._build_offer_from_api(flight, req)
            if offer:
                offers.append(offer)

        return offers

    def _find_flights_in_data(self, data, depth=0) -> list:
        """Recursively search for flight arrays in nested data."""
        if depth > 4 or not isinstance(data, dict):
            return []
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0:
                if isinstance(val[0], dict):
                    sample_keys = set(val[0].keys())
                    flight_keys = {"price", "fare", "flight", "departure", "arrival",
                                   "origin", "destination", "segments", "legs"}
                    if sample_keys & flight_keys:
                        return val
            elif isinstance(val, dict):
                result = self._find_flights_in_data(val, depth + 1)
                if result:
                    return result
        return []

    def _build_offer_from_api(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build FlightOffer from a single API flight result."""
        try:
            # Extract price (try various field names)
            price = (
                flight.get("price") or flight.get("totalPrice") or
                flight.get("fare", {}).get("total") if isinstance(flight.get("fare"), dict) else None or
                flight.get("lowestFare") or flight.get("amount") or 0
            )
            if isinstance(price, dict):
                price = price.get("amount") or price.get("total") or price.get("value") or 0
            price = float(price) if price else 0
            if price <= 0:
                return None

            currency = (
                flight.get("currency") or flight.get("currencyCode") or
                (flight.get("fare", {}).get("currency") if isinstance(flight.get("fare"), dict) else None) or
                (flight.get("price", {}).get("currency") if isinstance(flight.get("price"), dict) else None) or
                "USD"
            )
            if isinstance(currency, dict):
                currency = currency.get("code") or "USD"

            # Extract segments
            segments_data = (
                flight.get("segments") or flight.get("legs") or
                flight.get("slices") or flight.get("flights") or []
            )
            if not isinstance(segments_data, list):
                segments_data = [flight]

            segments = []
            for seg in segments_data:
                dep_str = seg.get("departure") or seg.get("departureTime") or seg.get("depTime") or ""
                arr_str = seg.get("arrival") or seg.get("arrivalTime") or seg.get("arrTime") or ""

                dep_dt = self._parse_dt(dep_str, req.date_from)
                arr_dt = self._parse_dt(arr_str, req.date_from)

                airline_code = seg.get("airline") or seg.get("carrierCode") or seg.get("operatingCarrier") or "LY"
                flight_no = seg.get("flightNumber") or seg.get("flightNo") or seg.get("number") or ""
                if flight_no and not flight_no.startswith(airline_code):
                    flight_no = f"{airline_code}{flight_no}"

                segments.append(FlightSegment(
                    airline=airline_code[:2] if len(airline_code) > 2 else airline_code,
                    airline_name="El Al" if airline_code == "LY" else airline_code,
                    flight_no=flight_no or f"LY",
                    origin=seg.get("origin") or seg.get("departureAirport") or req.origin,
                    destination=seg.get("destination") or seg.get("arrivalAirport") or req.destination,
                    departure=dep_dt,
                    arrival=arr_dt,
                    cabin_class="economy",
                ))

            if not segments:
                return None

            duration = flight.get("duration") or flight.get("totalDuration") or 0
            if isinstance(duration, str):
                # Parse "PT5H30M" or "5h 30m"
                m = re.search(r"(\d+)[hH].*?(\d+)?", duration)
                if m:
                    duration = int(m.group(1)) * 60 + int(m.group(2) or 0)

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=int(duration) * 60 if duration else 0,
                stopovers=max(0, len(segments) - 1),
            )

            offer_id = hashlib.md5(
                f"ly_{req.origin}_{req.destination}_{req.date_from}_{price}_{segments[0].flight_no}".encode()
            ).hexdigest()[:12]

            return FlightOffer(
                id=f"ly_{offer_id}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{currency} {price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=list({s.airline for s in segments}),
                owner_airline="LY",
                booking_url=self._booking_url(req),
                is_locked=False,
                source="elal_direct",
                source_tier="free",
            )
        except Exception as e:
            logger.debug("El Al: offer parse error: %s", e)
            return None

    @staticmethod
    def _parse_dt(s, fallback_date) -> datetime:
        """Parse a datetime string, falling back to date_from + midnight."""
        if not s:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                return datetime(dt.year, dt.month, dt.day) if isinstance(dt, date) and not isinstance(dt, datetime) else dt
            except Exception:
                return datetime.now()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[:19], fmt)
            except (ValueError, TypeError):
                continue
        # Try extracting time from string like "14:30"
        m = re.search(r"(\d{1,2}):(\d{2})", str(s))
        if m:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                d = dt if isinstance(dt, date) and not isinstance(dt, datetime) else dt.date() if isinstance(dt, datetime) else dt
                return datetime(d.year, d.month, d.day, int(m.group(1)), int(m.group(2)))
            except Exception:
                pass
        return datetime.now()

    # ------------------------------------------------------------------
    # DOM scraping fallback
    # ------------------------------------------------------------------

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Scrape flight cards from the results page DOM."""
        await asyncio.sleep(2)

        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];

            // Look for flight cards / rows
            const cards = document.querySelectorAll(
                '[class*="flight-card"], [class*="flight-row"], ' +
                '[class*="itinerary"], [class*="result-card"], ' +
                '[class*="bound-card"], [data-testid*="flight"]'
            );

            for (const card of cards) {
                const text = card.innerText || '';
                if (text.length < 20) continue;

                // Extract times (HH:MM patterns)
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;

                // Extract price
                const priceMatch = text.match(/(USD|EUR|ILS|GBP|\$|€|₪)\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(USD|EUR|ILS|GBP|\$|€|₪)/i);
                if (!priceMatch) continue;

                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0) continue;

                // Determine currency
                let currency = 'USD';
                if (/ILS|₪/.test(priceMatch[0])) currency = 'ILS';
                else if (/EUR|€/.test(priceMatch[0])) currency = 'EUR';
                else if (/GBP|£/.test(priceMatch[0])) currency = 'GBP';

                // Extract flight number
                const fnMatch = text.match(/\b(LY\s*\d{2,4})\b/i);
                const flightNo = fnMatch ? fnMatch[1].replace(/\s/g, '') : 'LY';

                // Extract duration
                let durationMin = 0;
                const durMatch = text.match(/(\d+)\s*h(?:rs?)?\s*(\d+)?\s*m/i);
                if (durMatch) {
                    durationMin = parseInt(durMatch[1]) * 60 + parseInt(durMatch[2] || 0);
                }

                // Stops
                const nonstop = /non.?stop|direct/i.test(text);
                const stopsMatch = text.match(/(\d+)\s*stop/i);
                const stops = nonstop ? 0 : (stopsMatch ? parseInt(stopsMatch[1]) : 0);

                results.push({
                    depTime: times[0],
                    arrTime: times[1],
                    price,
                    currency,
                    flightNo,
                    durationMin,
                    stops,
                    origin: origin,
                    destination: destination,
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

    def _build_dom_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build FlightOffer from DOM-scraped flight data."""
        price = flight.get("price", 0)
        if price <= 0:
            return None

        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            dep_date = dt.date() if isinstance(dt, datetime) else dt if isinstance(dt, date) else date.today()
        except (ValueError, TypeError):
            dep_date = date.today()

        dep_time = flight.get("depTime", "00:00")
        arr_time = flight.get("arrTime", "00:00")

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

        flight_no = flight.get("flightNo", "LY")
        currency = flight.get("currency", "USD")

        offer_id = hashlib.md5(
            f"ly_{req.origin}_{req.destination}_{dep_date}_{flight_no}_{price}".encode()
        ).hexdigest()[:12]

        segment = FlightSegment(
            airline="LY",
            airline_name="El Al",
            flight_no=flight_no,
            origin=flight.get("origin", req.origin),
            destination=flight.get("destination", req.destination),
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=flight.get("durationMin", 0) * 60,
            cabin_class="economy",
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=flight.get("durationMin", 0) * 60,
            stopovers=flight.get("stops", 0),
        )

        return FlightOffer(
            id=f"ly_{offer_id}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{currency} {price:,.0f}",
            outbound=route,
            inbound=None,
            airlines=["El Al"],
            owner_airline="LY",
            booking_url=self._booking_url(req),
            is_locked=False,
            source="elal_direct",
            source_tier="free",
        )

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        """Build El Al booking URL."""
        try:
            dt = req.date_from
            date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)
        except Exception:
            date_str = ""
        return f"https://www.elal.com/eng/flight-search/{req.origin}/{req.destination}/{date_str}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"elal{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
