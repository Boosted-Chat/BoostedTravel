"""
IndiGo direct scraper — uses real Chrome via CDP to scrape flight data.

IndiGo (IATA: 6E) is India's largest airline by market share.
Website: www.goindigo.in — custom React SPA with Module Federation micro-frontends.

Strategy:
1. Launch real Chrome subprocess + connect via CDP (bypasses Akamai)
2. Navigate to goindigo.in homepage
3. Dismiss cookie/session banners
4. Fill search form (From/To/Departure, One Way)
5. Intercept flight search API via CDP Fetch.enable (requestStage: Response)
6. Parse JSON → FlightOffer objects

Key technical details:
- API endpoint: api-prod-flight-skyplus6e.goindigo.in/v1/flight/search (POST)
- Akamai Bot Manager protects the API; Playwright launch_persistent_context is detected
- Real Chrome subprocess with CDP connection avoids Akamai detection
- CDP Fetch.enable intercepts responses while letting browser handle requests naturally
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json as _json
import logging
import os
import platform
import random
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import auto_block_if_proxied, find_chrome, proxy_chrome_args

logger = logging.getLogger(__name__)

# ── Chrome flags matching MCP/Pegasus pattern (Akamai-safe) ────────────
_CHROME_FLAGS = [
    "--disable-field-trial-config",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-back-forward-cache",
    "--disable-breakpad",
    "--disable-client-side-phishing-detection",
    "--disable-component-extensions-with-background-pages",
    "--disable-component-update",
    "--no-default-browser-check",
    "--disable-default-apps",
    "--disable-dev-shm-usage",
    "--disable-features=AvoidUnnecessaryBeforeUnloadCheckSync,"
    "BoundaryEventDispatchTracksNodeRemoval,DestroyProfileOnBrowserClose,"
    "DialMediaRouteProvider,GlobalMediaControls,HttpsUpgrades,LensOverlay,"
    "MediaRouter,PaintHolding,ThirdPartyStoragePartitioning,Translate,"
    "AutoDeElevate,RenderDocument,OptimizationHints,AutomationControlled",
    "--enable-features=CDPScreenshotNewSurface",
    "--allow-pre-commit-input",
    "--disable-hang-monitor",
    "--disable-ipc-flooding-protection",
    "--disable-popup-blocking",
    "--disable-prompt-on-repost",
    "--disable-renderer-backgrounding",
    "--force-color-profile=srgb",
    "--metrics-recording-only",
    "--no-first-run",
    *proxy_chrome_args(),
    "--password-store=basic",
    "--no-service-autorun",
    "--disable-search-engine-choice-screen",
    "--disable-infobars",
    "--disable-sync",
    "--enable-unsafe-swiftshader",
    "--window-position=-2400,-2400",
    "--window-size=1440,900",
]

# ── Shared CDP Chrome state ────────────────────────────────────────────
_CDP_PORT = 9473
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", "/tmp"), "chrome-cdp-indigo"
)
_chrome_proc: Optional[subprocess.Popen] = None
_browser = None
_pw_instance = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real Chrome via CDP (no Playwright automation flags)."""
    global _chrome_proc, _browser, _pw_instance
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass
            _browser = None

        from playwright.async_api import async_playwright

        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass

        _pw_instance = await async_playwright().start()

        # Try connecting to existing Chrome first
        try:
            _browser = await _pw_instance.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            logger.info("IndiGo: connected to existing Chrome on port %d", _CDP_PORT)
            return _browser
        except Exception:
            pass

        # Launch real Chrome subprocess
        from .browser import find_chrome

        chrome = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)

        args = [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            *_CHROME_FLAGS,
            "about:blank",
        ]

        popen_kw: dict = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if platform.system() == "Windows":
            si = subprocess.STARTUPINFO()
            si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            si.wShowWindow = 7  # SW_SHOWMINNOACTIVE
            popen_kw["startupinfo"] = si
            popen_kw["creationflags"] = subprocess.CREATE_NO_WINDOW

        _chrome_proc = subprocess.Popen(args, **popen_kw)
        await asyncio.sleep(2.5)

        _browser = await _pw_instance.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_CDP_PORT}"
        )
        logger.info(
            "IndiGo: Chrome ready via CDP (port %d, pid %d)",
            _CDP_PORT,
            _chrome_proc.pid,
        )
        return _browser


async def _reset_browser():
    """Close browser + Chrome process (called after persistent failures)."""
    global _browser, _chrome_proc, _pw_instance
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                await _browser.close()
            except Exception:
                pass
            _browser = None
        if _chrome_proc:
            try:
                _chrome_proc.terminate()
            except Exception:
                pass
            _chrome_proc = None
        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
            _pw_instance = None


class IndiGoConnectorClient:
    """IndiGo scraper — real Chrome via CDP + Fetch interception."""

    def __init__(self, timeout: float = 45.0):
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
        if ob_result.offers:
            segs = ob_result.offers[0].outbound.segments if ob_result.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(
                        anc_origin, anc_dest,
                        req.date_from.isoformat() if hasattr(req.date_from, 'isoformat') else str(req.date_from),
                        req.adults, ob_result.currency or req.currency,
                    ),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s->%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error: %s", _anc_err)
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Retry up to 2 times (Akamai may block the first attempt)
        for attempt in range(2):
            result = await self._try_search(req)
            if result.total_results > 0:
                return result
            if attempt == 0:
                logger.info("IndiGo: retrying search (attempt %d failed)", attempt + 1)
                await asyncio.sleep(2.0)
        return result

    async def _try_search(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        ctx = browser.contexts[0]

        page = await ctx.new_page()
        await auto_block_if_proxied(page)

        try:
            # Set up CDP Fetch interception for the flight search API response.
            # Unlike page.route() + route.fetch(), this lets the browser send
            # the request naturally (with Akamai cookies) while we read the
            # response body through CDP.
            cdp = await page.context.new_cdp_session(page)

            captured_data: dict = {}
            api_event = asyncio.Event()

            await cdp.send("Fetch.enable", {
                "patterns": [
                    {"urlPattern": "*api-prod-flight*search*", "requestStage": "Response"},
                    {"urlPattern": "*v1/flight/search*", "requestStage": "Response"},
                ],
                "handleAuthRequests": False,
            })

            def _on_fetch_paused(params):
                req_id = params.get("requestId")
                status = params.get("responseStatusCode", 0)
                url = params.get("request", {}).get("url", "")

                async def _handle():
                    try:
                        if status == 200:
                            body_result = await cdp.send("Fetch.getResponseBody", {"requestId": req_id})
                            body = body_result.get("body", "")
                            if body_result.get("base64Encoded"):
                                body = base64.b64decode(body).decode("utf-8", errors="replace")
                            if body.strip():
                                data = _json.loads(body)
                                if isinstance(data, dict) and "data" in data:
                                    captured_data["json"] = data["data"]
                                else:
                                    captured_data["json"] = data
                                logger.info("IndiGo: captured flight/search API (%d bytes)", len(body))
                                api_event.set()
                        elif status == 403:
                            logger.warning("IndiGo: API returned 403 (Akamai blocking)")
                        # Let the browser continue processing the response
                        await cdp.send("Fetch.continueResponse", {"requestId": req_id})
                    except Exception as exc:
                        logger.debug("IndiGo CDP fetch error: %s", exc)
                        try:
                            await cdp.send("Fetch.continueResponse", {"requestId": req_id})
                        except Exception:
                            pass

                asyncio.ensure_future(_handle())

            cdp.on("Fetch.requestPaused", _on_fetch_paused)

            logger.info("IndiGo: loading homepage for %s->%s", req.origin, req.destination)
            await page.goto(
                "https://www.goindigo.in/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            # Mouse movements to build Akamai sensor data
            for _ in range(3):
                x = random.randint(200, 1000)
                y = random.randint(100, 500)
                await page.mouse.move(x, y, steps=random.randint(5, 10))
                await asyncio.sleep(random.uniform(0.2, 0.5))

            # Select trip type (One Way or Round Trip)
            if req.return_from:
                await self._set_round_trip(page)
            else:
                await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Fill origin
            ok = await self._fill_airport_field(page, "From", req.origin, 0)
            if not ok:
                logger.warning("IndiGo: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill destination
            ok = await self._fill_airport_field(page, "To", req.destination, 1)
            if not ok:
                logger.warning("IndiGo: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("IndiGo: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)

            # Fill return date for RT
            if req.return_from:
                ok = await self._fill_return_date(page, req)
                if not ok:
                    logger.warning("IndiGo: return date fill failed")
                    # Continue anyway — API may still return RT data
                await asyncio.sleep(0.3)

            # Click search
            await self._click_search(page)

            # Wait for the flight search API response via CDP Fetch interceptor
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("IndiGo: API event timed out, trying DOM fallback")

            data = captured_data.get("json")
            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_response(data, req)
                return self._build_response(offers, req, elapsed)

            # DOM fallback
            logger.info("IndiGo: trying DOM extraction fallback")
            offers = await self._extract_from_dom(page, req)
            if offers:
                return self._build_response(offers, req, time.monotonic() - t0)
            return self._empty(req)

        except Exception as e:
            logger.error("IndiGo Playwright error: %s", e)
            if "closed" in str(e).lower() or "target" in str(e).lower() or "disconnected" in str(e).lower():
                await _reset_browser()
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _dismiss_cookies(self, page) -> None:
        # IndiGo has a minimal cookie notice — try to close it
        for label in [
            "Accept", "Accept All", "Accept all", "I agree",
            "Got it", "OK", "Close", "Dismiss", "Agree",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        # Close dialog windows if any
        try:
            close_link = page.locator("a:has-text('Close this dialog window')")
            if await close_link.count() > 0:
                await close_link.first.click(timeout=2000)
                await asyncio.sleep(0.3)
        except Exception:
            pass
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="Cookie"], [id*="Cookie"], [class*="onetrust"], [id*="onetrust"], ' +
                    '[class*="modal-overlay"], [class*="popup"], [id*="popup"], ' +
                    '[class*="privacy"], [id*="privacy"], [class*="dialog"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        # IndiGo radio inputs are hidden; click the visible label/wrapper instead.
        # One Way is the default selection, but click it to be safe.
        try:
            radio = page.locator("input#radio-input-triptype-oneWay")
            if await radio.count() > 0 and await radio.is_checked():
                return  # already selected
        except Exception:
            pass
        # Click the wrapper div that contains the radio + label text
        try:
            wrapper = page.locator("label[for='radio-input-triptype-oneWay'], input#radio-input-triptype-oneWay + *")
            if await wrapper.count() > 0:
                await wrapper.first.click(timeout=3000)
                return
        except Exception:
            pass
        try:
            ow = page.get_by_text("One Way", exact=True).first
            if ow and await ow.count() > 0:
                await ow.click(timeout=3000)
        except Exception:
            pass

    async def _set_round_trip(self, page) -> None:
        """Select 'Round Trip' radio button on IndiGo form."""
        try:
            radio = page.locator("input#radio-input-triptype-roundTrip")
            if await radio.count() > 0 and await radio.is_checked():
                return
        except Exception:
            pass
        try:
            wrapper = page.locator("label[for='radio-input-triptype-roundTrip'], input#radio-input-triptype-roundTrip + *")
            if await wrapper.count() > 0:
                await wrapper.first.click(timeout=3000)
                return
        except Exception:
            pass
        try:
            rt = page.get_by_text("Round Trip", exact=True).first
            if rt and await rt.count() > 0:
                await rt.click(timeout=3000)
        except Exception:
            pass

    async def _fill_return_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill the return date calendar (react-date-range) for RT bookings."""
        target = req.return_from
        if not target:
            return False
        try:
            # Click return date button to open calendar
            ret_btn = page.locator("button[class*='returnDate'], .popover__wrapper.search-widget-form-body__return")
            if await ret_btn.count() == 0:
                ret_btn = page.locator("[aria-label*='returnDate']")
            if await ret_btn.count() == 0:
                # Try generic return selector
                ret_btn = page.get_by_text("Return", exact=False).first
            await ret_btn.first.click(timeout=5000)
            await asyncio.sleep(0.8)

            # Navigate to target month
            target_month_year = target.strftime("%B %Y")
            for _ in range(14):
                month_names = page.locator(".rdrMonthName")
                found = False
                for i in range(await month_names.count()):
                    mn_text = await month_names.nth(i).inner_text()
                    if target_month_year.lower() in mn_text.lower():
                        found = True
                        break
                if found:
                    break
                header = page.locator(".rdrMonthAndYearPickers")
                if await header.count() > 0:
                    text = await header.first.inner_text()
                    if target_month_year.lower() in text.lower():
                        break
                nxt = page.locator(".rdrNextButton")
                if await nxt.count() > 0:
                    await nxt.first.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    break

            # Click the target day
            day_num = target.day
            day_name = target.strftime("%A")
            month_name = target.strftime("%B")
            aria_label = f"{day_name}, {day_num} {month_name} {target.year}"

            day_el = page.locator(f"span[aria-label='{aria_label}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            day_el = page.locator(f"span[aria-label*='{day_num} {month_name} {target.year}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            day_btns = page.locator(".rdrDay:not(.rdrDayDisabled) .rdrDayNumber span")
            for i in range(await day_btns.count()):
                btn = day_btns.nth(i)
                txt = (await btn.inner_text()).strip()
                if txt == str(day_num):
                    await btn.click(timeout=3000)
                    return True

            logger.warning("IndiGo: could not find return day %s in calendar", day_num)
            return False
        except Exception as e:
            logger.warning("IndiGo: return date error: %s", e)
            return False

    async def _fill_airport_field(self, page, label: str, iata: str, index: int) -> bool:
        """
        IndiGo DOM: inputs are hidden inside .popover__wrapper divs.
        Click the container (aria-label 'sourceCity'/'destinationCity') to reveal
        a combobox input, type the IATA code, then click the matching suggestion.
        """
        try:
            # Step 1: Click the city selector container to reveal the combobox
            if index == 0:
                container = page.locator("[aria-label*='sourceCity'], .popover__wrapper.search-widget-form-body__from")
            else:
                container = page.locator("[aria-label*='destinationCity'], .popover__wrapper.search-widget-form-body__to")
            await container.first.click(timeout=5000, no_wait_after=True)
            await asyncio.sleep(0.8)

            # Step 2: Find the now-visible combobox input and type the IATA code
            combo = page.locator("input[role='combobox']")
            visible_combos = []
            for i in range(await combo.count()):
                if await combo.nth(i).is_visible():
                    visible_combos.append(combo.nth(i))
            if not visible_combos:
                logger.warning("IndiGo: no visible combobox after clicking %s container", label)
                return False
            inp = visible_combos[0]
            await inp.fill("")
            await asyncio.sleep(0.2)
            await inp.press_sequentially(iata, delay=80)
            await asyncio.sleep(1.5)

            # Step 3: Click the matching suggestion.
            # IndiGo now uses .city-selection__list-item-wrapper for suggestion items.
            # Each item has the IATA code as text in a child element.
            sugg = page.locator(".city-selection__list-item-wrapper")
            if await sugg.count() > 0:
                # Try to find the exact IATA match
                for i in range(min(await sugg.count(), 20)):
                    item = sugg.nth(i)
                    try:
                        if not await item.is_visible():
                            continue
                        text = await item.inner_text()
                        if iata in text:
                            await item.click(timeout=3000)
                            return True
                    except Exception:
                        continue
                # If no exact match, click the first visible item
                await sugg.first.click(timeout=3000)
                return True

            # Fallback: keyboard selection (ArrowDown + Enter)
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.3)
            await page.keyboard.press("Enter")
            return True

        except Exception as e:
            logger.warning("IndiGo: %s airport field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """
        IndiGo uses react-date-range calendar (rdrCalendarWrapper).
        Click departure button → navigate months → click day by aria-label.
        Day aria-labels: "Wednesday, 11 March 2026" etc.
        Navigate via .rdrNextButton / .rdrPprevButton.
        """
        target = req.date_from
        try:
            # Click departure date button to open calendar
            dep_btn = page.locator("button[class*='departureDate'], .popover__wrapper.search-widget-form-body__departure")
            if await dep_btn.count() == 0:
                dep_btn = page.locator("[aria-label*='departureDate']")
            await dep_btn.first.click(timeout=5000)
            await asyncio.sleep(0.8)

            # Navigate to target month using react-date-range next button
            target_month_year = target.strftime("%B %Y")  # e.g. "April 2026"
            for _ in range(14):
                header = page.locator(".rdrMonthAndYearPickers, .rdrMonthName")
                if await header.count() > 0:
                    text = await header.first.inner_text()
                    if target_month_year.lower() in text.lower():
                        break
                # Also check all month name elements (multi-month view)
                month_names = page.locator(".rdrMonthName")
                for i in range(await month_names.count()):
                    mn_text = await month_names.nth(i).inner_text()
                    if target_month_year.lower() in mn_text.lower():
                        break
                else:
                    # Click next month button
                    nxt = page.locator(".rdrNextButton")
                    if await nxt.count() > 0:
                        await nxt.first.click(timeout=2000)
                        await asyncio.sleep(0.4)
                        continue
                    break
                break  # found in month_names loop

            # Click the target day
            # IndiGo day spans have aria-label like "Wednesday, 11 March 2026"
            day_num = target.day
            day_name = target.strftime("%A")  # e.g. "Wednesday"
            month_name = target.strftime("%B")  # e.g. "March"
            aria_label = f"{day_name}, {day_num} {month_name} {target.year}"

            day_el = page.locator(f"span[aria-label='{aria_label}']")
            if await day_el.count() > 0:
                # Wait for calendar animation to settle, then force-click
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            # Fallback: match partial aria-label
            day_el = page.locator(f"span[aria-label*='{day_num} {month_name} {target.year}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            # Last fallback: click rdrDay button matching the day number
            day_btns = page.locator(".rdrDay:not(.rdrDayDisabled) .rdrDayNumber span")
            for i in range(await day_btns.count()):
                btn = day_btns.nth(i)
                txt = (await btn.inner_text()).strip()
                if txt == str(day_num):
                    await btn.click(timeout=3000)
                    return True

            logger.warning("IndiGo: could not find day %s in calendar", day_num)
            return False
        except Exception as e:
            logger.warning("IndiGo: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        # IndiGo search button starts disabled; wait for it to become enabled
        try:
            search_btn = page.locator("button:has-text('Search'):not([disabled])")
            await search_btn.first.wait_for(state="visible", timeout=5000)
            await search_btn.first.click(timeout=5000)
            logger.info("IndiGo: clicked search")
            return
        except Exception:
            pass
        for label in ["Search", "SEARCH", "Search Flights", "Search flights", "Find flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000, force=True)
                    logger.info("IndiGo: clicked search (force)")
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.journeys || d.fares || d.availability)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """
        Parse IndiGo v1/flight/search API response.
        Structure: data.trips[0].journeysAvailable[] → each journey has:
          - designator: {origin, destination, departure, arrival}
          - passengerFares[]: [{productClass, totalFareAmount, totalTax, ...}]
          - segments[]: [{designator: {...}, identifier: {identifier, carrierCode}}]
          - stops, flightType, journeyKey, segKey
        Currency at data.currencyCode (default INR).
        """
        if isinstance(data, list):
            data = {"trips": [{"journeysAvailable": data}]}
        booking_url = self._build_booking_url(req)
        currency = data.get("currencyCode") or req.currency
        offers: list[FlightOffer] = []

        trips = data.get("trips") or []
        if not trips:
            return offers

        # ── Parse IB from trips[1] if RT ──
        ib_route = None
        ib_price = 0.0
        if req.return_from and len(trips) > 1:
            ib_journeys = trips[1].get("journeysAvailable") or []
            best_ib = None
            best_ib_p = float("inf")
            for jrn in ib_journeys:
                if jrn.get("isSold"):
                    continue
                ib_req = FlightSearchRequest(
                    origin=req.destination, destination=req.origin,
                    date_from=req.return_from, adults=req.adults,
                )
                o = self._parse_journey(jrn, ib_req, booking_url, currency)
                if o and o.price < best_ib_p:
                    best_ib_p = o.price
                    best_ib = o
            if best_ib:
                ib_route = best_ib.outbound
                ib_price = best_ib.price

        trip = trips[0]
        journeys = trip.get("journeysAvailable") or []

        for journey in journeys:
            if journey.get("isSold"):
                continue
            offer = self._parse_journey(journey, req, booking_url, currency)
            if offer:
                if ib_route:
                    combined = round(offer.price + ib_price, 2)
                    offer = FlightOffer(
                        id=f"6e_rt_{offer.id[3:]}",
                        price=combined,
                        currency=offer.currency,
                        price_formatted=f"{combined:.2f} {offer.currency}",
                        outbound=offer.outbound,
                        inbound=ib_route,
                        airlines=offer.airlines,
                        owner_airline=offer.owner_airline,
                        booking_url=offer.booking_url,
                        is_locked=False,
                        source="indigo_direct",
                        source_tier="free",
                    )
                offers.append(offer)
        return offers

    def _parse_journey(self, journey: dict, req: FlightSearchRequest, booking_url: str, currency: str) -> Optional[FlightOffer]:
        # Get cheapest fare from passengerFares (productClass R = economy regular)
        passenger_fares = journey.get("passengerFares") or []
        best_price = float("inf")
        best_product_class = ""
        for pf in passenger_fares:
            amt = pf.get("totalFareAmount")
            if amt is not None:
                try:
                    v = float(amt)
                    if 0 < v < best_price:
                        best_price = v
                        best_product_class = str(pf.get("productClass") or "").upper()
                except (TypeError, ValueError):
                    pass
        if best_price == float("inf"):
            return None

        # Map live productClass → bag conditions so _apply_ancillaries sees pre-set values
        _6E_CLASS_BAGS: dict[str, tuple[str, str]] = {
            # Unbundled SAVER/LITE fares — carry-on only
            "SAVER": ("7 kg carry-on included", "no checked bag (SAVER fare — add from ~INR 700/15 kg)"),
            "LITE": ("7 kg carry-on included", "no checked bag (Lite fare — add from ~INR 700/15 kg)"),
            # Standard Economy / Regular — includes 15 kg checked bag
            "R": ("7 kg carry-on included", "15 kg checked bag included"),
            "REGULAR": ("7 kg carry-on included", "15 kg checked bag included"),
            # Flexi / Super6E — includes 30 kg checked bag
            "FLEXI": ("7 kg carry-on included", "30 kg checked bag included (Flexi)"),
            "SUPER6E": ("7 kg carry-on included", "30 kg checked bag included (Super6E)"),
        }
        bag_conds: dict = {}
        if best_product_class in _6E_CLASS_BAGS:
            carry_str, checked_str = _6E_CLASS_BAGS[best_product_class]
            bag_conds = {"carry_on": carry_str, "checked_bag": checked_str}

        # Build segments from journey.segments[]
        _6e_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segments_raw = journey.get("segments") or []
        segments: list[FlightSegment] = []
        for seg in segments_raw:
            desig = seg.get("designator") or {}
            ident = seg.get("identifier") or {}
            segments.append(FlightSegment(
                airline=ident.get("carrierCode") or "6E",
                airline_name="IndiGo",
                flight_no=str(ident.get("identifier") or ""),
                origin=desig.get("origin") or req.origin,
                destination=desig.get("destination") or req.destination,
                departure=self._parse_dt(desig.get("departure") or ""),
                arrival=self._parse_dt(desig.get("arrival") or ""),
                cabin_class=_6e_cabin,
            ))
        if not segments:
            # Fallback: use journey-level designator
            desig = journey.get("designator") or {}
            segments.append(FlightSegment(
                airline="6E", airline_name="IndiGo", flight_no="",
                origin=desig.get("origin") or req.origin,
                destination=desig.get("destination") or req.destination,
                departure=self._parse_dt(desig.get("departure") or ""),
                arrival=self._parse_dt(desig.get("arrival") or ""),
                cabin_class=_6e_cabin,
            ))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = journey.get("journeyKey") or journey.get("segKey") or f"{time.monotonic()}"
        return FlightOffer(
            id=f"6e_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["IndiGo"],
            owner_airline="6E",
            booking_url=booking_url,
            is_locked=False,
            source="indigo_direct",
            source_tier="free",
            conditions=bag_conds,
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("IndiGo %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"indigo{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%d/%m/%Y")
        url = (
            f"https://www.goindigo.in/flight-booking?origin={req.origin}"
            f"&destination={req.destination}&date={dep}&adults={req.adults}&tripType={'R' if req.return_from else 'O'}"
        )
        if req.return_from:
            url += f"&returnDate={req.return_from.strftime('%d/%m/%Y')}"
        return url

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
    ) -> dict | None:
        """Return IndiGo (6E) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("6E")
        if not ref:
            return None
        carry_on = ref.get("carry_on")
        checked = ref.get("checked_bag")
        seat = ref.get("seat")
        ref_cur = ref.get("currency", "USD")

        # Carry-on note
        if note := ref.get("carry_on_note"):
            bags_note = note
        elif carry_on == 0.0:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"1 cabin bag included ({kg} kg)"
        elif carry_on is not None:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"cabin bag from ~{ref_cur} {carry_on:.0f} ({kg} kg)"
        else:
            bags_note = None

        # Checked bag note
        if note := ref.get("checked_bag_note"):
            checked_note = note
        elif checked == 0.0:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"1 checked bag included ({kg} kg)"
        elif checked is not None:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"checked bag from ~{ref_cur} {checked:.0f} ({kg} kg)"
        else:
            checked_note = None

        # Seat note
        if seat == 0.0:
            seat_note = "seat selection included"
        elif seat is not None:
            seat_note = f"seat selection from ~{ref_cur} {seat:.0f}"
        else:
            seat_note = "seat selection available at checkout"

        return {
            "bags_from": carry_on,
            "bags_note": bags_note,
            "checked_bag_from": checked,
            "checked_bag_note": checked_note,
            "seat_from": seat,
            "seat_note": seat_note,
            "currency": ref_cur,
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag_note")
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_from")
        seat_from = ancillary.get("seat_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            # Use setdefault so live fare-class conditions (set in _parse_journey) are not overwritten
            if bags_note:
                offer.conditions.setdefault("carry_on", bags_note)
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions.setdefault("seat", seat_note)
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price.setdefault("carry_on", bags_from)
            if checked_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price.setdefault("checked_bag", checked_from)
            if seat_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price.setdefault("seat", seat_from)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"indigo{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )


    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req,
    ) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_indigo_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
