"""
IndiGo patchright connector — Cloud Run patch.

Replaces CDP Chrome subprocess with Patchright headed browser.
Uses form fill + API response interception to get flight data.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

from letsfg.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 2


async def _launch_browser():
    """Launch a Patchright browser with anti-detection settings."""
    from patchright.async_api import async_playwright
    from .browser import find_chrome, inject_stealth_js, auto_block_if_proxied

    proxy = None
    _BYPASS = ".google.com,.googletagmanager.com,.gstatic.com,.googleapis.com"
    letsfg_proxy = os.environ.get("LETSFG_PROXY", "").strip()
    if letsfg_proxy:
        import socket as _sock
        try:
            _s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
            _s.connect(("127.0.0.1", 8899))
            _s.close()
            proxy = {"server": "http://127.0.0.1:8899", "bypass": _BYPASS}
            logger.info("IndiGo: using proxy relay on port 8899")
        except OSError:
            from urllib.parse import urlparse
            p = urlparse(letsfg_proxy)
            proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}", "bypass": _BYPASS}
            if p.username:
                proxy["username"] = p.username
                proxy["password"] = p.password or ""
            logger.info("IndiGo: using direct proxy %s:%s", p.hostname, p.port)
    else:
        logger.info("IndiGo: no proxy, direct connection")

    try:
        chrome_path = find_chrome()
        logger.info("IndiGo: using system Chrome at %s", chrome_path)
    except RuntimeError:
        chrome_path = None
        logger.info("IndiGo: using bundled Chromium")

    pw = await async_playwright().start()
    launch_args = [
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
        "--disable-blink-features=AutomationControlled",
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
        "--password-store=basic",
        "--no-service-autorun",
        "--disable-search-engine-choice-screen",
        "--disable-infobars",
        "--disable-sync",
        "--enable-unsafe-swiftshader",
        "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
        "--window-size=1440,900",
    ]
    launch_kwargs = dict(
        headless=False,
        args=launch_args,
        proxy=proxy,
    )
    if chrome_path:
        launch_kwargs["executable_path"] = chrome_path
    browser = await pw.chromium.launch(**launch_kwargs)
    
    # More realistic context for anti-detection
    context = await browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        color_scheme="light",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        extra_http_headers={
            "Accept-Language": "en-IN,en;q=0.9",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
    )
    page = await context.new_page()
    # Do NOT use inject_stealth_js(page) — it calls add_init_script() which creates
    # a detectable patchright-init-script-inject.internal/ request that Akamai flags.
    # Stealth patches are applied via page.evaluate() after navigation instead.

    # Do NOT use page.route() — Akamai can detect Playwright route interception.
    # Chrome background noise is minimal and doesn't affect performance.

    return pw, browser, context, page


class IndiGoConnectorClient:
    """IndiGo scraper — Patchright + form fill + API intercept."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob = await self._search_ow(req)
        if req.return_from and ob.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "return_from": None,
            })
            ib = await self._search_ow(ib_req)
            if ib.total_results > 0:
                ob.offers = self._combine_rt(ob.offers, ib.offers, req)
                ob.total_results = len(ob.offers)
        return ob

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._attempt_search(req, t0)
                if result is not None and result.total_results > 0:
                    return result
                if attempt < _MAX_ATTEMPTS:
                    logger.info("IndiGo: attempt %d/%d returned 0, retrying", attempt, _MAX_ATTEMPTS)
                    await asyncio.sleep(2.0)
            except Exception as e:
                logger.warning("IndiGo: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)
        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[FlightSearchResponse]:
        pw = browser = context = page = None
        try:
            pw, browser, context, page = await _launch_browser()

            # Set up CDP Fetch interception for the flight search API response.
            # Unlike page.on("response"), CDP Fetch intercepts the actual API call
            # with Akamai cookies intact, letting us read the response body.
            import base64
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
                                data = json.loads(body)
                                if isinstance(data, dict) and "data" in data:
                                    captured_data["json"] = data["data"]
                                else:
                                    captured_data["json"] = data
                                logger.info("IndiGo: captured flight/search API (%d bytes)", len(body))
                                api_event.set()
                        elif status == 403:
                            logger.warning("IndiGo: API returned 403 (Akamai blocking)")
                            api_event.set()  # unblock wait so DOM fallback runs immediately
                        await cdp.send("Fetch.continueResponse", {"requestId": req_id})
                    except Exception as exc:
                        logger.debug("IndiGo CDP fetch error: %s", exc)
                        try:
                            await cdp.send("Fetch.continueResponse", {"requestId": req_id})
                        except Exception:
                            pass

                asyncio.ensure_future(_handle())

            cdp.on("Fetch.requestPaused", _on_fetch_paused)

            # ── Load homepage and fill the search form ──
            logger.info("IndiGo: loading homepage for %s->%s", req.origin, req.destination)
            await page.goto("https://www.goindigo.in/", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2.0)

            # Apply stealth patches via evaluate (not add_init_script which is detectable)
            try:
                await page.evaluate("""() => {
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    if (!window.chrome) { window.chrome = {runtime: {}, loadTimes: () => ({}), csi: () => ({})}; }
                    delete navigator.__proto__.webdriver;
                }""")
            except Exception:
                pass

            # Wait for SPA to hydrate + Akamai sensor script to run
            try:
                await page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass
            await asyncio.sleep(3.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            # Brief warm-up: mouse movements + scroll to build Akamai sensor data
            for _ in range(4):
                x = random.randint(100, 1300)
                y = random.randint(80, 700)
                await page.mouse.move(x, y, steps=random.randint(8, 15))
                await asyncio.sleep(random.uniform(0.15, 0.4))
            await page.evaluate("window.scrollTo({top: 300, behavior: 'smooth'})")
            await asyncio.sleep(0.8)
            await page.evaluate("window.scrollTo({top: 0, behavior: 'smooth'})")
            await asyncio.sleep(0.5)

            # Set One Way
            if not req.return_from:
                await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Fill origin
            ok = await self._fill_airport(page, "source", req.origin)
            if not ok:
                logger.warning("IndiGo: origin fill failed")
                return None
            logger.info("IndiGo: origin %s filled OK", req.origin)
            await asyncio.sleep(random.uniform(0.5, 1.0))

            # Fill destination
            ok = await self._fill_airport(page, "destination", req.destination)
            if not ok:
                logger.warning("IndiGo: destination fill failed")
                return None
            logger.info("IndiGo: destination %s filled OK", req.destination)
            await asyncio.sleep(random.uniform(0.5, 1.0))

            # Fill date
            ok = await self._fill_date(page, req.date_from)
            if not ok:
                logger.warning("IndiGo: date fill failed")
                return None
            logger.info("IndiGo: date %s filled OK", req.date_from)
            await asyncio.sleep(random.uniform(0.3, 0.8))

            # Quick mouse movement before triggering search
            await page.mouse.move(random.randint(400, 800), random.randint(300, 500), steps=8)
            await asyncio.sleep(0.5)

            # Click search
            logger.info("IndiGo: clicking search button")
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
                logger.info("IndiGo: %s->%s returned %d offers in %.1fs", 
                           req.origin, req.destination, len(offers), elapsed)
                return self._build_response(offers, req, elapsed)

            # DOM fallback - wait for page to settle, then extract
            logger.info("IndiGo: trying DOM extraction, url=%s", page.url)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await asyncio.sleep(3.0)
            try:
                # Check for error messages or loading states
                page_state = await page.evaluate("""() => {
                    const body = document.body.innerText || '';
                    const hasError = body.includes('error') || body.includes('Error') || body.includes('sorry');
                    const hasLoading = body.includes('loading') || body.includes('Loading') || body.includes('please wait');
                    const hasNoFlights = body.includes('no flights') || body.includes('No flights') || body.includes('not available');
                    const priceCount = (body.match(/₹\\s*[\\d,]+/g) || []).length;
                    return { hasError, hasLoading, hasNoFlights, priceCount, bodyLen: body.length };
                }""")
                logger.info("IndiGo: page state: %s", page_state)
            except Exception as e:
                logger.warning("IndiGo: page state check error: %s", e)
            
            offers = await self._extract_from_dom(page, req)
            if offers:
                elapsed = time.monotonic() - t0
                return self._build_response(offers, req, elapsed)

            return None

        except Exception as e:
            logger.error("IndiGo error: %s", e)
            return None
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass
            if context:
                try:
                    await context.close()
                except:
                    pass
            if browser:
                try:
                    await browser.close()
                except:
                    pass
            if pw:
                try:
                    await pw.stop()
                except:
                    pass

    async def _dismiss_cookies(self, page) -> None:
        for label in ["Accept", "Accept All", "Got it", "OK", "Close", "Agree"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0 and await btn.first.is_visible():
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll('[class*="cookie"], [class*="consent"], [class*="modal-overlay"]')
                    .forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except:
            pass

    async def _set_one_way(self, page) -> None:
        try:
            ow = page.get_by_text("One Way", exact=True).first
            if await ow.count() > 0 and await ow.is_visible():
                await ow.click(timeout=3000)
        except:
            pass

    async def _fill_airport(self, page, field_type: str, iata: str) -> bool:
        """Fill origin or destination airport."""
        try:
            # Step 1: Click the city selector container to reveal the combobox
            if field_type == "source":
                container = page.locator("[aria-label*='sourceCity'], .popover__wrapper.search-widget-form-body__from, [data-testid*='origin'], .search-widget-form-body__from")
            else:
                container = page.locator("[aria-label*='destinationCity'], .popover__wrapper.search-widget-form-body__to, [data-testid*='destination'], .search-widget-form-body__to")

            # Wait for container to appear (SPA may take time)
            try:
                await container.first.wait_for(state="visible", timeout=15000)
            except Exception:
                logger.warning("IndiGo: %s container not visible after 15s", field_type)
                # Try clicking anywhere on body to dismiss overlays
                await page.locator("body").click(position={"x": 100, "y": 100})
                await asyncio.sleep(1)
                
            await container.first.click(timeout=8000, no_wait_after=True)
            await asyncio.sleep(1.0)

            # Step 2: Find the now-visible combobox input and type the IATA code
            combo = page.locator("input[role='combobox'], input[type='text'][placeholder*='city'], input[placeholder*='airport']")
            visible_combo = None
            for i in range(await combo.count()):
                if await combo.nth(i).is_visible():
                    visible_combo = combo.nth(i)
                    break

            if not visible_combo:
                logger.warning("IndiGo: no visible combobox for %s", field_type)
                # Fallback: try to find any visible text input
                all_inputs = page.locator("input[type='text']")
                for i in range(await all_inputs.count()):
                    inp = all_inputs.nth(i)
                    if await inp.is_visible() and await inp.is_enabled():
                        visible_combo = inp
                        break
                if not visible_combo:
                    return False

            await visible_combo.fill("")
            await asyncio.sleep(0.2)
            await visible_combo.press_sequentially(iata, delay=80)
            await asyncio.sleep(1.5)

            # Step 3: Click the matching suggestion
            sugg = page.locator(".city-selection__list-item-wrapper, [class*='suggestion'], [class*='option'], [role='option']")
            if await sugg.count() > 0:
                for i in range(min(await sugg.count(), 20)):
                    item = sugg.nth(i)
                    try:
                        if await item.is_visible():
                            text = await item.inner_text()
                            if iata in text:
                                await item.click(timeout=3000)
                                return True
                    except:
                        continue
                # Click first visible item
                for i in range(await sugg.count()):
                    if await sugg.nth(i).is_visible():
                        await sugg.nth(i).click(timeout=3000)
                        return True

            # Fallback: keyboard selection
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.3)
            await page.keyboard.press("Enter")
            return True

        except Exception as e:
            logger.warning("IndiGo: %s airport error: %s", field_type, e)
            return False

    async def _fill_date(self, page, date) -> bool:
        """Fill departure date in the calendar."""
        try:
            # Check if calendar is already open (IndiGo auto-opens after destination)
            cal = page.locator(".rdrCalendarWrapper, .rdrDateRangeWrapper")
            cal_open = await cal.count() > 0 and await cal.first.is_visible()
            if cal_open:
                logger.info("IndiGo: calendar already open, skipping departure button click")
            else:
                # Click date field to open calendar
                date_btn = page.locator("button[class*='departureDate'], .popover__wrapper.search-widget-form-body__departure")
                if await date_btn.count() == 0:
                    date_btn = page.locator("[aria-label*='depart'], [aria-label*='Depart']")
                await date_btn.first.click(timeout=5000)
                await asyncio.sleep(1.0)

            # Navigate to target month
            target_month_year = date.strftime("%B %Y")
            for attempt in range(14):
                # Check .rdrMonthAndYearPickers first (single-month header), then .rdrMonthName (multi-month)
                header = page.locator(".rdrMonthAndYearPickers")
                if await header.count() > 0:
                    text = await header.first.inner_text()
                    if target_month_year.lower() in text.lower():
                        break

                month_names = page.locator(".rdrMonthName")
                found = False
                for i in range(await month_names.count()):
                    mn_text = await month_names.nth(i).inner_text()
                    if target_month_year.lower() in mn_text.lower():
                        found = True
                        break
                if found:
                    break

                nxt = page.locator(".rdrNextButton")
                if await nxt.count() > 0:
                    try:
                        await nxt.first.click(timeout=5000, force=True)
                    except Exception as nav_err:
                        logger.debug("IndiGo: next button click error: %s", nav_err)
                        break
                    await asyncio.sleep(0.5)
                else:
                    logger.debug("IndiGo: no .rdrNextButton found")
                    break

            # Click target day
            day_num = date.day
            month_name = date.strftime("%B")
            day_name = date.strftime("%A")

            # Try aria-label format: "Wednesday, 15 June 2026"
            aria = f"{day_name}, {day_num} {month_name} {date.year}"
            day_el = page.locator(f"span[aria-label='{aria}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.3)
                await day_el.first.click(timeout=5000, force=True)
                return True

            # Try partial match
            day_el = page.locator(f"span[aria-label*='{day_num} {month_name} {date.year}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.3)
                await day_el.first.click(timeout=5000, force=True)
                return True

            # Fallback: find by day number in rdrDay cells
            day_btns = page.locator(".rdrDay:not(.rdrDayDisabled) .rdrDayNumber span")
            for i in range(await day_btns.count()):
                btn = day_btns.nth(i)
                txt = (await btn.inner_text()).strip()
                if txt == str(day_num):
                    await btn.click(timeout=3000, force=True)
                    return True

            logger.warning("IndiGo: could not find day %s in calendar", day_num)
            return False

        except Exception as e:
            logger.warning("IndiGo: date fill error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        try:
            btn = page.locator("button[type='submit']:has-text('Search'), button:has-text('Search Flight')")
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
                return
            btn = page.get_by_role("button", name=re.compile(r"search", re.I))
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
        except Exception as e:
            logger.warning("IndiGo: search click error: %s", e)

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flights from DOM when API interception fails."""
        try:
            # Wait for flight results to appear - look for actual prices (₹ with numbers)
            for wait_round in range(8):  # Total 40 seconds of waiting
                await asyncio.sleep(5.0)
                
                # Check if actual flight prices are visible (not just "--")
                price_check = await page.evaluate("""() => {
                    const body = document.body.innerText || '';
                    // Count actual INR prices (not "--" placeholders)
                    const priceMatches = body.match(/₹\\s*[\\d,]+/g) || [];
                    const realPrices = priceMatches.filter(p => {
                        const num = parseInt(p.replace(/[₹,\\s]/g, ''));
                        return num > 500;  // Real flight prices are > 500 INR
                    });
                    return { priceCount: realPrices.length, sample: realPrices.slice(0, 5) };
                }""")
                
                if price_check.get('priceCount', 0) > 0:
                    logger.info("IndiGo: found %d real prices: %s", 
                               price_check.get('priceCount'), price_check.get('sample'))
                    break
                
                # Try scrolling to trigger lazy loading
                await page.evaluate("window.scrollBy(0, 500)")
                await asyncio.sleep(0.5)
            
            # Also check window globals for flight data
            window_data = await page.evaluate("""() => {
                const data = {};
                // Check common SPA state patterns
                if (window.__INITIAL_STATE__) {
                    data.type = 'INITIAL_STATE';
                    data.keys = Object.keys(window.__INITIAL_STATE__);
                    // Look for flight data in the state
                    const state = JSON.stringify(window.__INITIAL_STATE__);
                    if (state.includes('flightResponseList') || state.includes('journeys')) {
                        data.hasFlights = true;
                        data.stateSize = state.length;
                    }
                }
                if (window.__NUXT__) data.type = 'NUXT';
                if (window.__NEXT_DATA__) {
                    data.type = 'NEXT_DATA';
                    data.keys = Object.keys(window.__NEXT_DATA__);
                }
                if (window.searchResults) {
                    data.type = 'searchResults';
                    data.results = window.searchResults;
                }
                if (window.flightData) {
                    data.type = 'flightData';
                    data.results = window.flightData;
                }
                // Check for Redux store
                if (window.__REDUX_DEVTOOLS_EXTENSION__) data.hasRedux = true;
                
                // Look for any global with flight-like data
                for (const key of Object.keys(window)) {
                    try {
                        const val = window[key];
                        if (val && typeof val === 'object') {
                            const str = JSON.stringify(val).slice(0, 1000);
                            if (str.includes('flightResponseList') || str.includes('totalFare')) {
                                data.foundIn = key;
                                data.preview = str.slice(0, 200);
                                break;
                            }
                        }
                    } catch(e) {}
                }
                
                return data;
            }""")
            
            if window_data:
                logger.info("IndiGo: window data check: %s", window_data)
            
            # Try multiple selectors for flight cards - IndiGo specific
            data = await page.evaluate("""() => {
                // IndiGo specific selectors based on their SPA structure
                const selectors = [
                    '[class*="flight-card"]',
                    '[class*="FlightCard"]', 
                    '[class*="flight-item"]',
                    '[class*="itinerary"]',
                    '[class*="journey-card"]',
                    '[class*="flightSelectionCard"]',
                    '[data-testid*="flight"]',
                    '.search-result-row',
                    '.flight-result',
                    '[class*="sr-card"]',
                    '[class*="price-card"]',
                    '[class*="fare-card"]',
                    '[class*="fareCard"]',
                    // Generic selectors for card-like elements with prices
                    '.price-container', 
                    '[class*="list-item"]',
                ];
                
                let cards = [];
                let usedSelector = '';
                for (const sel of selectors) {
                    const els = document.querySelectorAll(sel);
                    if (els.length > 0) {
                        cards = Array.from(els);
                        usedSelector = sel;
                        break;
                    }
                }
                
                // If no cards found, look for elements with INR prices
                if (cards.length === 0) {
                    const allDivs = document.querySelectorAll('div');
                    for (const el of allDivs) {
                        // Look for elements that look like flight cards
                        const text = el.innerText || '';
                        if (text.length > 50 && text.length < 500) {  // Not too short, not too long
                            // Check for price AND time patterns
                            if (/[₹₨]\\s*[\\d,]+/.test(text) && /\\d{1,2}:\\d{2}/.test(text) && /6E\\s*\\d+/i.test(text)) {
                                cards.push(el);
                            }
                        }
                    }
                    if (cards.length > 0) usedSelector = 'inline-price-div';
                }
                
                const results = [];
                const seenPrices = new Set();  // Dedupe
                
                cards.forEach((card, idx) => {
                    const text = card.innerText || '';
                    // Match INR price
                    const priceMatch = text.match(/[₹₨]\\s*([\\d,]+)/);
                    // Match time patterns HH:MM AM/PM
                    const timeMatch = text.match(/(\\d{1,2}:\\d{2})\\s*(AM|PM)?/gi);
                    // Match flight number
                    const flightMatch = text.match(/6E\\s*(\\d+)/i);
                    
                    if (priceMatch && timeMatch && timeMatch.length >= 2) {
                        const price = parseInt(priceMatch[1].replace(/,/g, ''));
                        const key = `${price}_${timeMatch[0]}_${timeMatch[1]}`;
                        if (!seenPrices.has(key)) {
                            seenPrices.add(key);
                            results.push({
                                price: price,
                                depTime: timeMatch[0],
                                arrTime: timeMatch[1],
                                flightNo: flightMatch ? '6E' + flightMatch[1] : '6E',
                                text: text.slice(0, 300),
                            });
                        }
                    }
                });
                
                // Also check for data in window object (common pattern)
                let windowData = null;
                try {
                    if (window.__INITIAL_STATE__) windowData = 'INITIAL_STATE';
                    else if (window.__NUXT__) windowData = 'NUXT';
                    else if (window.__NEXT_DATA__) windowData = 'NEXT_DATA';
                    else if (window.searchResults) windowData = 'searchResults';
                    else if (window.flightData) windowData = 'flightData';
                } catch(e) {}
                
                return {
                    results: results.slice(0, 30),
                    cardsFound: cards.length,
                    selector: usedSelector,
                    windowData: windowData,
                    bodyPreview: document.body.innerText.slice(0, 1000).replace(/\\s+/g, ' '),
                    url: location.href,
                };
            }""")
            
            logger.info("IndiGo: DOM extraction - selector=%s cards=%d results=%d windowData=%s preview=%s...", 
                       data.get('selector', 'none'), data.get('cardsFound', 0), len(data.get('results', [])),
                       data.get('windowData', 'none'),
                       data.get('bodyPreview', '')[:300])

            offers = []
            for i, item in enumerate(data.get('results', []) or []):
                offer = FlightOffer(
                    id=f"6e_dom_{hashlib.md5(f'{req.origin}{req.destination}{i}'.encode()).hexdigest()[:8]}",
                    price=item.get("price", 0),
                    currency="INR",
                    price_formatted=f"₹{item.get('price', 0):,}",
                    outbound=FlightRoute(
                        segments=[FlightSegment(
                            airline="6E",
                            airline_name="IndiGo",
                            flight_no="6E",
                            origin=req.origin,
                            destination=req.destination,
                            departure=datetime.now(),
                            arrival=datetime.now(),
                            cabin_class="M",
                        )],
                        total_duration_seconds=0,
                        stopovers=0,
                    ),
                    inbound=None,
                    airlines=["IndiGo"],
                    owner_airline="6E",
                    booking_url=f"https://www.goindigo.in/",
                    is_locked=False,
                    source="indigo_direct",
                    source_tier="free",
                )
                offers.append(offer)
            return offers
        except Exception as e:
            logger.warning("IndiGo: DOM extraction error: %s", e)
            return []

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse IndiGo flight search API response (Navitaire + legacy formats)."""
        offers = []
        try:
            if not isinstance(data, dict):
                logger.warning("IndiGo: response is not a dict, type=%s", type(data))
                return offers

            keys = list(data.keys())[:15]
            logger.info("IndiGo: parsing response, keys=%s", keys)

            # ── Navitaire format: trips[].journeysAvailable[] ──
            trips = data.get("trips") or []
            if trips and isinstance(trips, list):
                currency = data.get("currencyCode") or "INR"
                logger.info("IndiGo: Navitaire format detected, %d trip(s), currency=%s", len(trips), currency)
                trip = trips[0]
                journeys = trip.get("journeysAvailable") or []
                logger.info("IndiGo: %d journeys available", len(journeys))
                for journey in journeys:
                    if journey.get("isSold"):
                        continue
                    offer = self._parse_navitaire_journey(journey, req, currency)
                    if offer:
                        offers.append(offer)
                if offers:
                    logger.info("IndiGo: parsed %d offers from Navitaire format", len(offers))
                    return sorted(offers, key=lambda o: o.price)

            # ── Legacy format: flightResponseList / flights / journeys ──
            flights = data.get("flightResponseList", []) or data.get("flights", []) or data.get("journeys", []) or []
            if not flights and "data" in data:
                nested = data["data"]
                if isinstance(nested, dict):
                    flights = nested.get("flightResponseList", []) or nested.get("flights", []) or nested.get("journeys", []) or []
                    # Also check nested Navitaire
                    if not flights:
                        nested_trips = nested.get("trips") or []
                        if nested_trips:
                            currency = nested.get("currencyCode") or "INR"
                            for trip in nested_trips[:1]:
                                for journey in trip.get("journeysAvailable") or []:
                                    if not journey.get("isSold"):
                                        offer = self._parse_navitaire_journey(journey, req, currency)
                                        if offer:
                                            offers.append(offer)
                            if offers:
                                return sorted(offers, key=lambda o: o.price)
            logger.info("IndiGo: found %d flights in legacy format", len(flights))

            for flt in flights:
                try:
                    fare = flt.get("totalFare", {})
                    price = fare.get("baseFare", 0) + fare.get("tax", 0)
                    if not price:
                        price = flt.get("fare", {}).get("total", 0)
                    if not price:
                        continue

                    segments = []
                    for leg in flt.get("legs", []) or [flt]:
                        dep_dt = self._parse_dt(leg.get("departureDateTime") or leg.get("departure"))
                        arr_dt = self._parse_dt(leg.get("arrivalDateTime") or leg.get("arrival"))
                        seg = FlightSegment(
                            airline="6E",
                            airline_name="IndiGo",
                            flight_no=leg.get("flightNumber", "") or leg.get("flight", "6E"),
                            origin=leg.get("origin", req.origin),
                            destination=leg.get("destination", req.destination),
                            departure=dep_dt,
                            arrival=arr_dt,
                            cabin_class="M",
                        )
                        segments.append(seg)

                    if not segments:
                        continue

                    total_dur = 0
                    if len(segments) >= 1:
                        total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
                        total_dur = max(total_dur, 0)

                    offer_id = f"6e_{hashlib.md5(f'{req.origin}{req.destination}{price}{len(offers)}'.encode()).hexdigest()[:10]}"
                    offer = FlightOffer(
                        id=offer_id,
                        price=round(price, 2),
                        currency="INR",
                        price_formatted=f"₹{price:,.0f}",
                        outbound=FlightRoute(
                            segments=segments,
                            total_duration_seconds=total_dur,
                            stopovers=max(len(segments) - 1, 0),
                        ),
                        inbound=None,
                        airlines=["IndiGo"],
                        owner_airline="6E",
                        booking_url=f"https://www.goindigo.in/",
                        is_locked=False,
                        source="indigo_direct",
                        source_tier="free",
                    )
                    offers.append(offer)
                except Exception:
                    continue
        except Exception as e:
            logger.warning("IndiGo: parse error: %s", e)
        return sorted(offers, key=lambda o: o.price)

    def _parse_navitaire_journey(self, journey: dict, req: FlightSearchRequest, currency: str) -> Optional[FlightOffer]:
        """Parse a single Navitaire journeysAvailable entry into a FlightOffer."""
        passenger_fares = journey.get("passengerFares") or []
        best_price = float("inf")
        for pf in passenger_fares:
            amt = pf.get("totalFareAmount")
            if amt is not None:
                try:
                    v = float(amt)
                    if 0 < v < best_price:
                        best_price = v
                except (TypeError, ValueError):
                    pass
        if best_price == float("inf"):
            return None

        segments: list[FlightSegment] = []
        for seg in journey.get("segments") or []:
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
                cabin_class="M",
            ))
        if not segments:
            desig = journey.get("designator") or {}
            segments.append(FlightSegment(
                airline="6E", airline_name="IndiGo", flight_no="",
                origin=desig.get("origin") or req.origin,
                destination=desig.get("destination") or req.destination,
                departure=self._parse_dt(desig.get("departure") or ""),
                arrival=self._parse_dt(desig.get("arrival") or ""),
                cabin_class="M",
            ))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            try:
                total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
            except Exception:
                pass

        jkey = journey.get("journeyKey") or journey.get("segKey") or f"{time.monotonic()}"
        return FlightOffer(
            id=f"6e_{hashlib.md5(str(jkey).encode()).hexdigest()[:10]}",
            price=round(best_price, 2),
            currency=currency,
            price_formatted=f"₹{best_price:,.0f}" if currency == "INR" else f"{best_price:.2f} {currency}",
            outbound=FlightRoute(
                segments=segments,
                total_duration_seconds=max(total_dur, 0),
                stopovers=max(len(segments) - 1, 0),
            ),
            inbound=None,
            airlines=["IndiGo"],
            owner_airline="6E",
            booking_url="https://www.goindigo.in/",
            is_locked=False,
            source="indigo_direct",
            source_tier="free",
        )

    def _parse_dt(self, s) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        if isinstance(s, datetime):
            return s
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except:
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except:
                return datetime(2000, 1, 1)

    def _combine_rt(self, ob_offers, ib_offers, req):
        """Combine outbound and inbound offers into round-trip offers."""
        combined = []
        for ob in ob_offers[:10]:
            for ib in ib_offers[:10]:
                rt_offer = ob.model_copy(update={
                    "id": f"6e_rt_{ob.id}_{ib.id}",
                    "price": ob.price + ib.price,
                    "price_formatted": f"₹{ob.price + ib.price:,.0f}",
                    "inbound": ib.outbound,
                })
                combined.append(rt_offer)
        return sorted(combined, key=lambda o: o.price)

    def _build_response(self, offers, req, elapsed) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"indigo{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="INR",
            offers=offers,
            total_results=len(offers),
        )

    def _empty(self, req) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"indigo{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="INR",
            offers=[],
            total_results=0,
        )
