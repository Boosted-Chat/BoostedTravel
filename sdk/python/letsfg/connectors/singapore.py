"""
Singapore Airlines (SQ) CDP Chrome connector — form fill + DOM results scraping.

SIA's booking at singaporeair.com is a Vue.js SPA behind Akamai WAF.
Direct API calls are blocked; the ONLY reliable path is form-triggered browsing.

Strategy (CDP Chrome + DOM scraping):
1. Launch REAL Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP.
3. Navigate to /en_UK/sg/home (SG locale — required for autocomplete).
4. Accept cookies → fill departure/arrival via vue-simple-suggest typeahead
   (vm.select for programmatic selection) → check one-way → pick date via
   custom Vue calendar (li[date-data]) → force-click Search.
5. Page navigates SPA-style to /flightsearch/searchFlight.form#/booking.
6. Wait for DOM with flight results → scrape (flight no, times, duration,
   stops, airports, prices, aircraft, fare class).

Discovered Mar 2026:
  - SG locale (/en_UK/sg/home) defaults origin to SIN, autocomplete works.
  - vue-simple-suggest: vm.select(airportObject) for programmatic selection.
  - Calendar: li[date-data="YYYY-MM-DD"], month nav via .calendar a.right.
  - Submit button (#submitbtn) overlapped by one-way label — force click.
  - Results page: text-based with SQ flight numbers, SGD prices.
  - Akamai WAF blocks headless; CDP headed Chrome works.
"""

from __future__ import annotations

import asyncio
import hashlib
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

_DEBUG_PORT = 9462
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".singapore_chrome_data"
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
    """Get or create a persistent browser context (headed — Akamai blocks headless)."""
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

        # Try existing Chrome
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("Singapore: connected to existing Chrome on port %d", _DEBUG_PORT)
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
                "--disable-http2",
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
            logger.info(
                "Singapore: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _dismiss_overlays(page) -> None:
    """Remove SIA cookie banner and any overlays."""
    try:
        btn = page.locator('button:text("ACCEPT ALL COOKIES")')
        if await btn.count() > 0:
            await btn.click(timeout=3000)
            await asyncio.sleep(0.5)
            return
    except Exception:
        pass
    # Force-remove cookie elements
    try:
        await page.evaluate("""() => {
            document.querySelectorAll('[class*="SiaCookie"]').forEach(el => el.remove());
            const c = document.querySelector('sia-cookie'); if (c) c.remove();
        }""")
    except Exception:
        pass


async def _reset_profile():
    """Wipe Chrome profile when Akamai flags the session."""
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
            logger.info("Singapore: deleted stale Chrome profile")
        except Exception:
            pass


class SingaporeConnectorClient:
    """Singapore Airlines CDP Chrome connector — form fill + results page scraping."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        akamai_blocked = False

        async def _on_response(response):
            nonlocal akamai_blocked
            url = response.url
            if "accessrestricted" in url or "sec-cp-challenge" in url:
                akamai_blocked = True

        page.on("response", _on_response)

        try:
            # Step 1: Load SG locale homepage (origin defaults to SIN)
            logger.info("Singapore: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.singaporeair.com/en_UK/sg/home",
                wait_until="domcontentloaded",
                timeout=45000,
            )
            await asyncio.sleep(8.0)
            await _dismiss_overlays(page)
            await asyncio.sleep(0.5)

            if akamai_blocked:
                logger.warning("Singapore: Akamai blocked on load, resetting profile")
                await _reset_profile()
                return self._empty(req)

            # Step 2: Fill origin airport
            ok = await self._fill_origin(page, req.origin)
            if not ok:
                logger.warning("Singapore: origin fill failed")
                return self._empty(req)

            # Step 3: Fill destination airport
            ok = await self._fill_destination(page, req.destination)
            if not ok:
                logger.warning("Singapore: destination fill failed")
                return self._empty(req)

            # Step 4: Open calendar & set one-way
            await page.locator("#departDate1").click()
            await asyncio.sleep(2.0)

            await page.evaluate("""() => {
                const ow = document.getElementById('oneway_id');
                if (ow && !ow.checked) ow.click();
            }""")
            await asyncio.sleep(1.0)

            # Step 5: Navigate calendar & pick date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("Singapore: date selection failed")
                return self._empty(req)

            # Close calendar
            await page.evaluate("""() => {
                const done = document.querySelector('.calendar_information_sticky .btn-primary');
                if (done) done.click();
            }""")
            await asyncio.sleep(1.0)

            # Step 6: Submit search
            await page.evaluate("""() => {
                document.querySelectorAll('[class*="SiaCookie"]').forEach(el => el.remove());
                document.querySelectorAll('.calendar').forEach(el => {
                    const s = getComputedStyle(el);
                    if (s.position === 'absolute' || s.position === 'fixed') el.style.display = 'none';
                });
            }""")

            try:
                await page.locator("#submitbtn").click(force=True, timeout=5000)
            except Exception:
                await page.evaluate("document.getElementById('submitbtn').click()")
            logger.info("Singapore: search clicked")

            # Step 7: Wait for results page (SPA navigation)
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            got_results = False
            while time.monotonic() < deadline:
                await asyncio.sleep(1.5)
                url = page.url
                if akamai_blocked:
                    logger.warning("Singapore: Akamai blocked, resetting profile")
                    await _reset_profile()
                    return self._empty(req)
                if "searchFlight" in url or "flightsearch" in url or "select-flights" in url:
                    logger.info("Singapore: reached results page: %s", url[:150])
                    # Wait for flights to actually render (SPA lazy-loads)
                    for wait_i in range(10):
                        await asyncio.sleep(3.0)
                        has_flights = await page.evaluate("""() => {
                            const text = document.body?.innerText || '';
                            return text.includes('SQ ') || text.includes('Economy') ||
                                   text.includes('Business') || text.includes('Premium Economy');
                        }""")
                        if has_flights:
                            logger.info("Singapore: flight content detected after %ds", (wait_i + 1) * 3)
                            await asyncio.sleep(3.0)
                            break
                    got_results = True
                    break
                # Check if page title changed to indicate results
                title = await page.title()
                if "Select Flight" in title:
                    logger.info("Singapore: results detected by title: %s", title)
                    for wait_i in range(10):
                        await asyncio.sleep(3.0)
                        has_flights = await page.evaluate("""() => {
                            const text = document.body?.innerText || '';
                            return text.includes('SQ ') || text.includes('Economy');
                        }""")
                        if has_flights:
                            await asyncio.sleep(3.0)
                            break
                    got_results = True
                    break

            if not got_results:
                logger.warning("Singapore: never reached results page (URL: %s)", page.url[:200])
                return self._empty(req)

            # Step 8: Scrape flight data from DOM
            flights = await self._scrape_results(page, req)

            offers = []
            for f in flights:
                offer = self._build_offer(f, req)
                if offer:
                    offers.append(offer)

            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "Singapore %s→%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"singapore{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            currency = offers[0].currency if offers else "SGD"
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Singapore CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Form fill helpers
    # ------------------------------------------------------------------

    async def _fill_origin(self, page, iata: str) -> bool:
        """Fill origin airport via vue-simple-suggest. SG locale defaults to SIN."""
        try:
            current = await page.evaluate(
                "document.getElementById('flightOrigin1')?.value || ''"
            )
            if iata.upper() in current.upper():
                logger.info("Singapore: origin already set to %s", current)
                return True

            # Need to type and select
            field = page.locator("#flightOrigin1")
            await field.click()
            await asyncio.sleep(0.5)
            await field.click(click_count=3)
            await page.keyboard.press("Backspace")
            await page.keyboard.type(iata, delay=100)
            await asyncio.sleep(2.5)

            selected = await page.evaluate("""(iata) => {
                const el = document.getElementById('flightOrigin1');
                let node = el;
                for (let i = 0; i < 15; i++) {
                    if (node.__vue__?.$data?.suggestions !== undefined) {
                        const vm = node.__vue__;
                        const sug = vm.$data.suggestions || [];
                        const match = sug.find(s => s.airportCode === iata);
                        if (match && typeof vm.select === 'function') {
                            vm.select(match);
                            return match.airportCode;
                        }
                        return null;
                    }
                    node = node.parentElement;
                    if (!node) break;
                }
                return null;
            }""", iata.upper())

            if not selected:
                # Keyboard fallback
                await field.press("ArrowDown")
                await asyncio.sleep(0.2)
                await field.press("Enter")

            await asyncio.sleep(1.0)
            value = await page.evaluate(
                "document.getElementById('flightOrigin1')?.value || ''"
            )
            if iata.upper() in value.upper() or len(value) > 2:
                logger.info("Singapore: origin filled → %s", value)
                return True

            logger.warning("Singapore: origin fill failed for %s (got '%s')", iata, value)
            return False

        except Exception as e:
            logger.warning("Singapore: origin error: %s", e)
            return False

    async def _fill_destination(self, page, iata: str) -> bool:
        """Fill destination airport via vue-simple-suggest."""
        try:
            field = page.locator("#bookFlightDestination")
            await field.click()
            await asyncio.sleep(0.5)
            await field.click(click_count=3)
            await asyncio.sleep(0.2)
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.3)
            await page.keyboard.type(iata, delay=100)
            await asyncio.sleep(3.0)

            selected = await page.evaluate("""(iata) => {
                const el = document.getElementById('bookFlightDestination');
                let node = el;
                for (let i = 0; i < 15; i++) {
                    if (node.__vue__?.$data?.suggestions !== undefined) {
                        const vm = node.__vue__;
                        const sug = vm.$data.suggestions || [];
                        const match = sug.find(s => s.airportCode === iata);
                        if (match && typeof vm.select === 'function') {
                            vm.select(match);
                            return match.airportCode;
                        }
                        return null;
                    }
                    node = node.parentElement;
                    if (!node) break;
                }
                return null;
            }""", iata.upper())

            if not selected:
                await field.press("ArrowDown")
                await asyncio.sleep(0.2)
                await field.press("Enter")

            await asyncio.sleep(1.0)
            value = await page.evaluate(
                "document.getElementById('bookFlightDestination')?.value || ''"
            )
            if iata.upper() in value.upper() or len(value) > 2:
                logger.info("Singapore: destination filled → %s", value)
                return True

            logger.warning("Singapore: destination fill failed for %s (got '%s')", iata, value)
            return False

        except Exception as e:
            logger.warning("Singapore: destination error: %s", e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Navigate custom Vue calendar and pick the target date."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            logger.warning("Singapore: invalid date_from: %s", req.date_from)
            return False

        target_date_str = dt.strftime("%Y-%m-%d")  # e.g. "2026-07-15"

        try:
            # Navigate forward to reach target month (up to 12 months)
            for _ in range(12):
                # Check if target date is visible
                found = await page.evaluate("""(dateStr) => {
                    const day = document.querySelector('li[date-data="' + dateStr + '"]');
                    return day && !day.classList.contains('calendar_days--disabled');
                }""", target_date_str)
                if found:
                    break
                await page.evaluate("""() => {
                    const next = document.querySelector('.calendar a.right');
                    if (next) next.click();
                }""")
                await asyncio.sleep(0.5)

            # Click the target date
            clicked = await page.evaluate("""(dateStr) => {
                const day = document.querySelector('li[date-data="' + dateStr + '"]');
                if (day && !day.classList.contains('calendar_days--disabled')) {
                    day.click();
                    return true;
                }
                return false;
            }""", target_date_str)

            if not clicked:
                logger.warning("Singapore: could not click date %s", target_date_str)
                return False

            await asyncio.sleep(1.0)
            logger.info("Singapore: date selected → %s", target_date_str)
            return True

        except Exception as e:
            logger.warning("Singapore: date selection error: %s", e)
            return False

    # ------------------------------------------------------------------
    # DOM scraping
    # ------------------------------------------------------------------

    async def _scrape_results(self, page, req: FlightSearchRequest) -> list[dict]:
        """Scrape flight results from the SIA results page DOM."""
        flights = await page.evaluate(r"""() => {
            const body = document.body?.innerText || '';
            if (!body || body.includes('no flights available') ||
                body.includes('Sorry, no flights were found')) return [];

            const lines = body.split('\n').map(l => l.trim()).filter(Boolean);
            const results = [];

            // SIA results format (actual DOM):
            //   "Non-stop • 13hrs 45mins"
            //   "SIN 09:00"          ← origin + dep time
            //   "Singapore"
            //   "12 Jul (Sun)"
            //   "LHR 15:45"          ← dest + arr time
            //   "London"
            //   "12 Jul (Sun)"
            //   "Singapore Airlines"
            //   "•"
            //   "SQ 308"             ← flight number
            //   ...
            //   "SGD 727.20"         ← cheapest price (Economy)

            for (let i = 0; i < lines.length; i++) {
                // Look for flight number: "SQ 308" or "SQ308"
                const fnMatch = lines[i].match(/^SQ\s*(\d{2,4})$/);
                if (!fnMatch) continue;

                const flightNo = 'SQ ' + fnMatch[1];
                const flight = { flightNo };

                // Search backwards for duration line "Non-stop • Xhrs Ymins"
                for (let j = i - 1; j >= Math.max(0, i - 20); j--) {
                    const durMatch = lines[j].match(/(\d+)\s*hrs?\s*(\d+)\s*mins?/);
                    if (durMatch) {
                        flight.duration = parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]);
                        flight.durationText = lines[j];
                        const stopsText = lines[j].toLowerCase();
                        flight.nonstop = stopsText.includes('non-stop') || stopsText.includes('nonstop');
                        if (!flight.nonstop) {
                            const sm = lines[j].match(/(\d+)\s*stop/i);
                            flight.stops = sm ? parseInt(sm[1]) : 1;
                        } else {
                            flight.stops = 0;
                        }
                        break;
                    }
                }

                // Search backwards for origin time: "SIN 09:00" pattern
                for (let j = i - 1; j >= Math.max(0, i - 15); j--) {
                    const otm = lines[j].match(/^([A-Z]{3})\s+(\d{2}:\d{2})$/);
                    if (otm) {
                        if (!flight.arrTime) {
                            // First match going backwards is arrival
                            flight.arrTime = otm[2];
                            flight.destination = otm[1];
                        } else if (!flight.depTime) {
                            // Second match is departure
                            flight.depTime = otm[2];
                            flight.origin = otm[1];
                        }
                        if (flight.depTime && flight.arrTime) break;
                    }
                }

                // Search forward for aircraft type (appears in "More details" section)
                for (let j = i + 1; j < Math.min(lines.length, i + 15); j++) {
                    if (/A\d{3}|7[0-9]{2}|Boeing|Airbus/i.test(lines[j])) {
                        flight.aircraft = lines[j];
                        break;
                    }
                }

                // Search forward for price: "SGD 727.20" or "FROM SGD\n727.20"
                let bestPrice = Infinity;
                let currency = 'SGD';
                for (let j = i; j < Math.min(lines.length, i + 40); j++) {
                    // Match "SGD 727.20" or "SGD 2,225.20"
                    const pm = lines[j].match(/(SGD|USD|EUR|GBP|AED|AUD|INR|JPY|KRW|MYR|THB|CNY|HKD)\s*([\d,]+(?:\.\d{2})?)/i);
                    if (pm) {
                        const p = parseFloat(pm[2].replace(/,/g, ''));
                        if (p > 0 && p < bestPrice) {
                            bestPrice = p;
                            currency = pm[1].toUpperCase();
                        }
                        break;  // Take the first (cheapest Economy) price
                    }
                }

                // Also stop searching if we hit the next flight block
                // (next "Non-stop" or "1 stop" line)

                if (bestPrice < Infinity && flight.depTime) {
                    flight.price = bestPrice;
                    flight.currency = currency;
                    results.push(flight);
                }
            }

            // Deduplicate by flight number (keep cheapest)
            const seen = {};
            for (const f of results) {
                if (!seen[f.flightNo] || f.price < seen[f.flightNo].price) {
                    seen[f.flightNo] = f;
                }
            }
            return Object.values(seen);
        }""")
        logger.info("Singapore: scraped %d flights from DOM", len(flights) if flights else 0)
        return flights or []

    # ------------------------------------------------------------------
    # Offer construction
    # ------------------------------------------------------------------

    def _build_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build a FlightOffer from scraped flight data."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            dep_date = dt if isinstance(dt, date) and not isinstance(dt, datetime) else dt.date() if isinstance(dt, datetime) else dt
        except (ValueError, TypeError):
            dep_date = date.today()

        dep_time = flight.get("depTime", "00:00")
        arr_time = flight.get("arrTime", "00:00")

        try:
            hm_dep = dep_time.split(":")
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day,
                              int(hm_dep[0]), int(hm_dep[1]))
        except (ValueError, IndexError):
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day)

        try:
            hm_arr = arr_time.split(":")
            arr_dt = datetime(dep_date.year, dep_date.month, dep_date.day,
                              int(hm_arr[0]), int(hm_arr[1]))
            if arr_dt <= dep_dt:
                arr_dt += timedelta(days=1)
        except (ValueError, IndexError):
            arr_dt = dep_dt

        duration_min = flight.get("duration", 0)
        flight_no = flight.get("flightNo", "SQ")
        origin = flight.get("origin", "") or req.origin
        destination = flight.get("destination", "") or req.destination
        price = flight.get("price", 0)
        currency = flight.get("currency", "SGD")

        if price <= 0:
            return None

        offer_id = hashlib.md5(
            f"sq_{origin}_{destination}_{dep_date}_{flight_no}_{price}".encode()
        ).hexdigest()[:12]

        segment = FlightSegment(
            airline="SQ",
            airline_name="Singapore Airlines",
            flight_no=flight_no,
            origin=origin,
            destination=destination,
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=duration_min * 60,
            cabin_class=flight.get("cabin", "economy"),
            aircraft=flight.get("aircraft", ""),
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=duration_min * 60,
            stopovers=flight.get("stops", 0),
        )

        return FlightOffer(
            id=f"sq_{offer_id}",
            price=price,
            currency=currency,
            price_formatted=f"{currency} {price:,.2f}",
            outbound=route,
            inbound=None,
            airlines=["Singapore Airlines"],
            owner_airline="SQ",
            booking_url=self._booking_url(req),
            is_locked=False,
            source="singapore_direct",
            source_tier="free",
        )

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        """Build SIA booking deep-link."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_str = dt.strftime("%d%m%Y")  # SIA uses DDMMYYYY format
        except (ValueError, TypeError):
            date_str = ""
        return (
            f"https://www.singaporeair.com/en_UK/sg/plan-and-book/official-website-background/"
            f"?selectedOrigin={req.origin}&selectedDestination={req.destination}"
            f"&selectedDate={date_str}&tripType=O&cabinClass=Y"
            f"&numOfAdults={req.adults or 1}&numOfChildren={req.children or 0}"
            f"&numOfInfants={req.infants or 0}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"singapore{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="SGD",
            offers=[],
            total_results=0,
        )
