"""
Porter Airlines Playwright scraper — browser automation to bypass WAF.

Porter (IATA: PD) is a Canadian airline based at Billy Bishop Toronto City Airport.

Uses booking.flyporter.com (no Cloudflare) instead of www.flyporter.com (blocked).
The booking page is a Next.js SPA with:
  - input#autocomplete-destination (Where from combobox)
  - input#autocomplete-arrival (Where to combobox)
  - DD/MM/YYYY date textbox
  - "Find Flights" button

Strategy:
1. Navigate to booking.flyporter.com/en/book-travel/book-flights-online
2. Wait for search form to load (~5s lazy load)
3. Select One-way via combobox dropdown
4. Fill origin/destination via #autocomplete-destination / #autocomplete-arrival
5. Select airport from listbox option containing IATA code
6. Fill departure date as DD/MM/YYYY in text input
7. Click "Find Flights" → intercept API response or scrape DOM
8. Parse JSON → FlightOffer objects
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import time
from datetime import datetime
from typing import Optional

from boostedtravel.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from boostedtravel.connectors.browser import stealth_args, stealth_position_arg, stealth_popen_kwargs

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
]
_LOCALES = ["en-US", "en-CA", "en-GB"]
_TIMEZONES = [
    "America/Toronto", "America/Vancouver", "America/Edmonton",
    "America/Halifax", "America/New_York",
]

_BOOKING_URL = "https://booking.flyporter.com/en/book-travel/book-flights-online"

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None

_USER_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".porter_chrome_data")
_DEBUG_PORT = 9333


def _find_chrome() -> Optional[str]:
    """Find Chrome executable on the system."""
    candidates = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real Chrome via subprocess + connect via CDP.

    This avoids Playwright's automation flags that trigger Cloudflare.
    Falls back to regular Playwright launch if Chrome is not found.
    """
    global _pw_instance, _browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass

        from playwright.async_api import async_playwright
        import subprocess

        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
        _pw_instance = await async_playwright().start()

        chrome_path = _find_chrome()
        if chrome_path:
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            # Check if port is already in use — try connecting first
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Porter: connected to existing Chrome via CDP")
                return _browser
            except Exception:
                pass  # No existing Chrome, launch a new one

            vp = random.choice(_VIEWPORTS)
            _chrome_proc = subprocess.Popen(
                [
                    chrome_path,
                    f"--remote-debugging-port={_DEBUG_PORT}",
                    f"--user-data-dir={_USER_DATA_DIR}",
                    f"--window-size={vp['width']},{vp['height']}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-background-networking",
                    *stealth_position_arg(),
                    "about:blank",
                ],
                **stealth_popen_kwargs(),
            )
            # Give Chrome time to start and open the debug port
            await asyncio.sleep(2.5)
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Porter: connected to real Chrome via CDP (no automation flags)")
                return _browser
            except Exception as e:
                logger.warning("Porter: CDP connect failed: %s, falling back to Playwright launch", e)
                if _chrome_proc:
                    _chrome_proc.terminate()
                    _chrome_proc = None

        # Fallback: regular Playwright
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=True, channel="chrome",
                args=["--disable-blink-features=AutomationControlled", *stealth_args()],
            )
        except Exception:
            _browser = await _pw_instance.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox", *stealth_args()],
            )
        logger.info("Porter: Playwright browser launched (headed Chrome, fallback)")
        return _browser


class PorterConnectorClient:
    """Porter Airlines Playwright scraper.

    Strategy A (preferred): Navigate directly to the results URL at
    www.flyporter.com/en/flight/tickets/Select_BAF?... — this is the same URL
    the booking form would redirect to.  A persistent browser context keeps
    Cloudflare clearance cookies across runs.

    Strategy B (fallback): Fill the booking form at booking.flyporter.com
    (no Cloudflare), let it redirect to www.flyporter.com, and scrape DOM.
    """

    _RESULTS_URL_TPL = (
        "https://www.flyporter.com/en/flight/tickets/Select_BAF"
        "?departStation={origin}&destination={dest}&depDate={date}"
        "&paxADT=1&paxCHD=0&paxINF=0&trpType=OneWay&fareClass=R&bookWithPoints=0"
    )

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = browser.contexts[0] if browser.contexts else await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
        )

        page = await context.new_page()
        try:
            from playwright_stealth import stealth_async
            await stealth_async(page)
        except ImportError:
            pass

        try:
            # --- Strategy A: Direct results URL ---
            results_url = self._RESULTS_URL_TPL.format(
                origin=req.origin, dest=req.destination,
                date=req.date_from.strftime("%Y-%m-%d"),
            )
            logger.info("Porter: navigating directly to results for %s→%s", req.origin, req.destination)
            await page.goto(results_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for Cloudflare challenge to resolve
            cf_cleared = await self._wait_cloudflare(page, timeout=30)
            if not cf_cleared:
                logger.warning("Porter: Cloudflare blocked direct URL, falling back to booking form")
                offers = await self._strategy_booking_form(page, req, t0)
            else:
                # Wait for flight results in DOM
                try:
                    await page.wait_for_selector(
                        "h1:has-text('Select Flights'), h2:has-text('Departing flights')",
                        timeout=20000)
                    logger.info("Porter: flight results loaded via direct URL")
                except Exception:
                    logger.debug("Porter: flight headings not found, extracting DOM anyway")
                # Wait for flight list items to render (lazy JS hydration)
                try:
                    await page.wait_for_selector("h4:has-text('Departs')", timeout=10000)
                except Exception:
                    logger.debug("Porter: flight cards not yet visible, waiting extra")
                    await asyncio.sleep(5.0)
                offers = await self._extract_from_dom(page, req)

            elapsed = time.monotonic() - t0
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Porter Playwright error: %s", e)
            return self._empty(req)
        finally:
            await page.close()

    async def _wait_cloudflare(self, page, timeout: int = 30) -> bool:
        """Wait for Cloudflare challenge page to resolve. Returns True if cleared."""
        for _ in range(timeout):
            try:
                title = await page.title()
            except Exception:
                # Execution context destroyed = page navigated = CF likely cleared
                await asyncio.sleep(1.0)
                return True
            if "moment" not in title.lower() and "security" not in title.lower():
                return True
            await asyncio.sleep(1.0)
        return False

    async def _strategy_booking_form(self, page, req: FlightSearchRequest, t0: float) -> list[FlightOffer]:
        """Fallback: fill booking form at booking.flyporter.com → redirect → scrape DOM."""
        logger.info("Porter: using booking form fallback for %s→%s", req.origin, req.destination)
        await page.goto(_BOOKING_URL, wait_until="domcontentloaded", timeout=30000)

        try:
            await page.wait_for_selector("#autocomplete-destination", timeout=10000)
        except Exception:
            logger.warning("Porter: search form did not load within 10s")
            return []
        await asyncio.sleep(1.0)

        await self._dismiss_cookies(page)
        await self._set_one_way(page)
        await asyncio.sleep(0.5)

        if not await self._fill_airport(page, "#autocomplete-destination", req.origin):
            logger.warning("Porter: origin fill failed for %s", req.origin)
            return []
        await asyncio.sleep(0.5)

        if not await self._fill_airport(page, "#autocomplete-arrival", req.destination):
            logger.warning("Porter: destination fill failed for %s", req.destination)
            return []
        await asyncio.sleep(0.5)

        if not await self._fill_date(page, req):
            logger.warning("Porter: date fill failed")
            return []
        await asyncio.sleep(0.3)

        await self._click_search(page)

        # Wait for redirect to results page
        remaining = max(self.timeout - (time.monotonic() - t0), 10)
        try:
            await page.wait_for_url("https://www.flyporter.com/**", timeout=remaining * 1000)
        except Exception:
            pass

        # Wait for Cloudflare on the redirected page
        await self._wait_cloudflare(page, timeout=20)

        try:
            await page.wait_for_selector(
                "h1:has-text('Select Flights'), h2:has-text('Departing flights')",
                timeout=15000)
        except Exception:
            await asyncio.sleep(3.0)

        return await self._extract_from_dom(page, req)

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept All", "Accept all", "Accept", "I agree",
            "Got it", "OK", "Close", "Dismiss", "Accept Cookies",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], '
                    + '[class*="onetrust"], [id*="onetrust"], [class*="modal-overlay"], '
                    + '[class*="popup"], [id*="popup"], [class*="privacy"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        """Select One-way trip type via the combobox dropdown."""
        try:
            # Porter uses a <button role="combobox"> for trip type with a listbox dropdown.
            # IMPORTANT: filter by text — there are multiple combobox buttons on the page
            # (e.g. language selector shows "en ca"). Trip type shows "Round trip" / "One-way".
            trip_combo = page.locator("button[role='combobox']").filter(
                has_text=re.compile(r"round|one.?way|trip", re.IGNORECASE)
            ).first
            if await trip_combo.count() > 0:
                text = (await trip_combo.inner_text()).strip().lower()
                logger.info("Porter: current trip type: '%s'", text)
                if "one" in text:
                    return  # Already one-way
                await trip_combo.click(timeout=3000)
                await asyncio.sleep(0.5)
                # Select "One-way" from the listbox options
                oneway = page.get_by_role("option", name=re.compile(r"one.?way", re.IGNORECASE))
                if await oneway.count() > 0:
                    await oneway.first.click(timeout=3000)
                    logger.info("Porter: set trip type to One-way")
                    await asyncio.sleep(0.5)
                    return
                else:
                    logger.warning("Porter: One-way option not found in dropdown")
            else:
                logger.warning("Porter: trip type combobox not found (no button with round/trip/one-way text)")
        except Exception as e:
            logger.debug("Porter: trip type combobox approach failed: %s", e)
        # Fallback: try text click
        for label in ["One-way", "One Way", "One way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if el and await el.count() > 0:
                    await el.click(timeout=3000)
                    logger.info("Porter: set trip type via text click '%s'", label)
                    return
            except Exception:
                continue
        logger.warning("Porter: could not set one-way — will fill return date as fallback")

    async def _fill_airport(self, page, selector: str, iata: str) -> bool:
        """Fill an airport combobox by its CSS selector, type IATA, and pick from dropdown."""
        try:
            field = page.locator(selector)
            if await field.count() == 0:
                logger.warning("Porter: selector %s not found", selector)
                return False

            # The <label for="..."> overlays the input and intercepts pointer events.
            # Click the label first to focus the input, then interact.
            label_sel = f"label[for='{selector.lstrip('#')}']"
            label_el = page.locator(label_sel)
            if await label_el.count() > 0:
                await label_el.first.click(timeout=3000)
                await asyncio.sleep(0.5)
            else:
                # Fallback: force-click the input itself
                await field.click(timeout=3000, force=True)
                await asyncio.sleep(0.5)

            # If there's a "Clear" button visible (to clear pre-filled origin), click it
            try:
                clear_btn = page.get_by_role("button", name="Clear")
                if await clear_btn.count() > 0 and await clear_btn.first.is_visible():
                    await clear_btn.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    # Re-click the label/field to re-focus after clearing
                    if await label_el.count() > 0:
                        await label_el.first.click(timeout=2000)
                    else:
                        await field.click(timeout=2000, force=True)
                    await asyncio.sleep(0.3)
            except Exception:
                pass

            # Type the IATA code character by character for reactivity
            await field.fill("")
            await asyncio.sleep(0.2)
            await page.keyboard.type(iata, delay=100)
            await asyncio.sleep(2.5)

            # Porter's dropdown shows IATA codes in <code> elements.
            # The accessible name may have spaced letters like "Y T Z" (from aria).
            # Try both compact and spaced IATA patterns.
            spaced_iata = " ".join(iata)  # "YTZ" -> "Y T Z"
            for pattern in [iata, spaced_iata]:
                option = page.get_by_role("option").filter(has_text=re.compile(rf"{re.escape(pattern)}", re.IGNORECASE)).first
                if await option.count() > 0:
                    await option.click(timeout=3000)
                    logger.info("Porter: selected airport %s from dropdown", iata)
                    return True

            # Fallback: click any listbox option (first match)
            any_option = page.get_by_role("option").first
            if await any_option.count() > 0:
                await any_option.click(timeout=3000)
                logger.info("Porter: selected first available airport option for %s", iata)
                return True

            # Last resort: press Enter to confirm
            await page.keyboard.press("Enter")
            logger.info("Porter: pressed Enter to confirm airport %s", iata)
            return True

        except Exception as e:
            logger.warning("Porter: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill the departure date via calendar or text input. Also fills return date if round trip."""
        from datetime import timedelta
        target = req.date_from
        date_str = target.strftime("%d/%m/%Y")  # DD/MM/YYYY format
        try:
            ok = await self._fill_single_date(page, target, index=0, label="departure")
            if not ok:
                return False

            # Check if the form is still in round-trip mode (return date input visible)
            return_inputs = page.locator("input[placeholder='DD/MM/YYYY']")
            count = await return_inputs.count()
            if count >= 2:
                # Round-trip still active — fill return date = departure + 7 days
                return_date = target + timedelta(days=7)
                logger.info("Porter: form still in round-trip mode, filling return date %s",
                            return_date.strftime("%d/%m/%Y"))
                await self._fill_single_date(page, return_date, index=1, label="return")

            return True
        except Exception as e:
            logger.warning("Porter: date error: %s", e)
        return False

    async def _fill_single_date(self, page, target, index: int = 0, label: str = "departure") -> bool:
        """Fill a single date field by index (0=departure, 1=return)."""
        date_str = target.strftime("%d/%m/%Y")
        try:
            date_input = page.locator("input[placeholder='DD/MM/YYYY']").nth(index)
            if await date_input.count() == 0:
                logger.warning("Porter: %s date input not found (index %d)", label, index)
                return False

            # Focus the input field
            await date_input.click(timeout=5000, force=True)
            await asyncio.sleep(1.0)

            # Check if a calendar popup opened
            calendar = page.locator("[class*='calendar'], [class*='Calendar'], [role='dialog'], [class*='datepicker']").first
            if await calendar.count() > 0 and await calendar.is_visible():
                logger.info("Porter: calendar popup opened for %s date", label)
                picked = await self._pick_date_from_calendar(page, target)
                if picked:
                    await asyncio.sleep(0.5)
                    for btn_name in ["Done", "Apply", "Confirm", "OK", "Select"]:
                        done_btn = page.get_by_role("button", name=re.compile(rf"^{btn_name}$", re.IGNORECASE))
                        if await done_btn.count() > 0:
                            await done_btn.first.click(timeout=2000)
                            logger.info("Porter: clicked '%s' to confirm %s date", btn_name, label)
                            await asyncio.sleep(0.5)
                            break
                    logger.info("Porter: filled %s date %s via calendar", label, date_str)
                    return True
            else:
                logger.info("Porter: no calendar popup for %s date, filling as text", label)

            # Fill the date as text — use fill() which clears and types in one step
            await date_input.fill(date_str)
            await asyncio.sleep(0.3)
            await page.keyboard.press("Tab")
            await asyncio.sleep(0.5)

            logger.info("Porter: filled %s date %s as text", label, date_str)
            return True
        except Exception as e:
            logger.warning("Porter: %s date fill error: %s", label, e)
            return False

    async def _pick_date_from_calendar(self, page, target) -> bool:
        """Navigate a calendar popup and click the target date."""
        try:
            target_my = target.strftime("%B %Y")  # e.g. "April 2026"
            day = target.day

            # Navigate months forward until we find the right month
            for _ in range(12):
                content = await page.content()
                if target_my.lower() in content.lower():
                    break
                fwd = page.locator("[class*='next'], [aria-label*='next'], [aria-label*='Next']").first
                if await fwd.count() > 0:
                    await fwd.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    break

            # Try various aria-label formats for the day button
            for fmt in [
                f"{target.strftime('%B')} {day}, {target.year}",
                f"{day} {target.strftime('%B')} {target.year}",
                f"{target.strftime('%B')} {day}",
                target.strftime("%Y-%m-%d"),
            ]:
                day_btn = page.locator(f"[aria-label*='{fmt}']").first
                if await day_btn.count() > 0:
                    await day_btn.click(timeout=3000)
                    logger.info("Porter: picked date from calendar via aria-label")
                    return True

            # Try matching day number in calendar grid
            day_btn = page.locator(
                "[class*='calendar'] button, [class*='datepicker'] button, table button"
            ).filter(has_text=re.compile(rf"^{day}$")).first
            if await day_btn.count() > 0:
                await day_btn.click(timeout=3000)
                logger.info("Porter: picked date %d from calendar grid", day)
                return True
        except Exception as e:
            logger.debug("Porter: calendar pick failed: %s", e)
        return False

    async def _click_search(self, page) -> None:
        """Click the 'Find Flights' button."""
        try:
            btn = page.get_by_role("button", name=re.compile(r"find flights", re.IGNORECASE))
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
                logger.info("Porter: clicked Find Flights")
                return
        except Exception as e:
            logger.warning("Porter: search click error: %s", e)
        # Fallback labels
        for label in ["Search", "SEARCH", "Search Flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight cards from the www.flyporter.com results page DOM."""
        try:
            # The results page at www.flyporter.com renders flight cards as <li> elements
            # inside a list with aria-label "Direct flights" (or "1-stop flights" etc.).
            # Each flight card contains:
            #   - h4 "Departs {time}" / h4 "Arrives {time}"
            #   - Duration text (e.g. "59min")
            #   - Flight Number "PD 2205"
            #   - Fare buttons: "Fare category: PorterClassic ... From $169"
            flight_data = await page.evaluate("""() => {
                const results = [];
                const debugInfo = {
                    url: document.location.href,
                    title: document.title,
                    bodyLen: document.body.innerText.length,
                    h4Count: document.querySelectorAll('h4').length,
                    liCount: document.querySelectorAll('li').length,
                };
                // Find all flight list items
                const items = document.querySelectorAll('li');
                for (const item of items) {
                    const headings = item.querySelectorAll('h4');
                    let dep = null, arr = null;
                    for (const h of headings) {
                        const t = h.textContent.trim();
                        if (t.startsWith('Departs')) dep = t.replace('Departs', '').trim();
                        if (t.startsWith('Arrives')) arr = t.replace('Arrives', '').trim();
                    }
                    if (!dep || !arr) continue;

                    // Flight number
                    let flightNum = '';
                    const allText = item.innerText;
                    const fnMatch = allText.match(/PD\\s*\\d+/);
                    if (fnMatch) flightNum = fnMatch[0].replace(/\\s+/g, '');

                    // Duration
                    let duration = '';
                    const durMatch = allText.match(/(\\d+)\\s*min/);
                    if (durMatch) duration = durMatch[0];

                    // Stops
                    const isNonstop = /non.?stop/i.test(allText);

                    // Fares — only match buttons that contain "Fare category:" text
                    const fares = [];
                    const fareButtons = item.querySelectorAll('button');
                    for (const btn of fareButtons) {
                        const bt = btn.textContent || '';
                        if (!bt.includes('Fare category')) continue;
                        const priceMatch = bt.match(/\\$(\\d+(?:,\\d{3})*(?:\\.\\d{2})?)/);
                        const catMatch = bt.match(/Fare category:\\s*([\\w]+)/);
                        if (priceMatch && catMatch) {
                            fares.push({
                                category: catMatch[1],
                                price: parseFloat(priceMatch[1].replace(',', '')),
                            });
                        }
                    }

                    if (fares.length > 0) {
                        results.push({ dep, arr, flightNum, duration, nonstop: isNonstop, fares });
                    }
                }
                return { flights: results, debug: debugInfo };
            }""")

            if not flight_data or not flight_data.get("flights"):
                logger.info("Porter: no flight cards found in DOM (debug: %s)",
                            flight_data.get("debug") if flight_data else "null")
                return []

            flights = flight_data["flights"]
            logger.info("Porter: extracted %d flights from DOM", len(flights))

            booking_url = self._build_booking_url(req)
            dep_date = req.date_from.strftime("%Y-%m-%d")
            offers: list[FlightOffer] = []

            for f in flights:
                dep_time = self._parse_time(f.get("dep", ""), dep_date)
                arr_time = self._parse_time(f.get("arr", ""), dep_date)
                dur_min = 0
                dur_match = re.search(r"(\d+)\s*min", f.get("duration", ""))
                if dur_match:
                    dur_min = int(dur_match.group(1))
                # Also check for hours
                hr_match = re.search(r"(\d+)\s*h", f.get("duration", ""))
                if hr_match:
                    dur_min += int(hr_match.group(1)) * 60

                flight_num = f.get("flightNum", "")
                nonstop = f.get("nonstop", True)
                dur_sec = dur_min * 60

                seg = FlightSegment(
                    airline="PD",
                    airline_name="Porter Airlines",
                    flight_no=flight_num,
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_time,
                    arrival=arr_time,
                    duration_seconds=dur_sec,
                )
                route = FlightRoute(
                    segments=[seg],
                    stopovers=0 if nonstop else 1,
                    total_duration_seconds=dur_sec,
                )

                for fare in f.get("fares", []):
                    price = fare.get("price", 0)
                    cat = fare.get("category", "Economy")
                    offer_id = hashlib.md5(
                        f"PD-{flight_num}-{dep_time}-{cat}-{price}".encode()
                    ).hexdigest()[:12]
                    offers.append(FlightOffer(
                        id=offer_id,
                        price=float(price),
                        currency="CAD",
                        outbound=route,
                        airlines=["PD"],
                        owner_airline="PD",
                        source="porter_scraper",
                        source_tier="protocol",
                        is_locked=False,
                        booking_url=booking_url,
                    ))

            return offers
        except Exception as e:
            logger.warning("Porter: DOM extraction error: %s", e)
        return []

    @staticmethod
    def _parse_time(time_str: str, date_str: str) -> datetime:
        """Parse '7:25AM' into a datetime object."""
        time_str = time_str.strip().upper()
        for fmt in ["%I:%M%p", "%I:%M %p"]:
            try:
                t = datetime.strptime(time_str, fmt)
                d = datetime.strptime(date_str, "%Y-%m-%d")
                return d.replace(hour=t.hour, minute=t.minute, second=0)
            except ValueError:
                continue
        return datetime.strptime(date_str, "%Y-%m-%d")

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Porter %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"porter{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.flyporter.com/en/flight-results?from={req.origin}"
            f"&to={req.destination}&departure={dep}&adults={req.adults}&tripType=oneway"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"porter{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
