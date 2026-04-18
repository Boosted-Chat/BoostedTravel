"""
Eurowings cookie-farm hybrid connector.

Strategy (converted Mar 2026):
1. Farm Cloudflare cookies via CDP Chrome (once per ~25 min, just visit homepage)
2. POST to apps.eurowings.com/flightsearch/v1/booking/flight-data via curl_cffi (~0.7s)
3. Parse QUERY_FLIGHT_DATA JSON → FlightOffers
4. Fall back to full Playwright browser automation if API fails
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from curl_cffi import requests as cffi_requests

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import stealth_args, stealth_popen_kwargs, get_curl_cffi_proxies, auto_block_if_proxied, inject_stealth_js, get_proxy
from .airline_routes import get_city_airports

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-DE"]
_TIMEZONES = ["Europe/London", "Europe/Berlin", "Europe/Paris", "Europe/Vienna"]

_CDP_PORT = 9455
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "eurowings_cdp_data")

_API_URL = "https://apps.eurowings.com/flightsearch/v1/booking/flight-data?action=QUERY_FLIGHT_DATA"
_IMPERSONATE = "chrome131"
_COOKIE_MAX_AGE = 25 * 60  # 25 minutes

# ── Module-level singletons ──────────────────────────────────────────────────
_chrome_proc: subprocess.Popen | None = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None

_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Get headless Playwright browser with stealth settings."""
    global _pw_instance, _cdp_browser
    lock = _get_lock()
    async with lock:
        # Try CDP first if it's already connected
        if _cdp_browser and _cdp_browser.is_connected():
            return _cdp_browser
        
        # Launch fresh Playwright headless browser (more reliable than CDP for Eurowings)
        # Use patchright for better anti-detection (April 2026)
        try:
            from patchright.async_api import async_playwright
            logger.info("Eurowings: using patchright (stealth)")
        except ImportError:
            from playwright.async_api import async_playwright
            logger.info("Eurowings: using regular playwright")
        if _pw_instance is None:
            _pw_instance = await async_playwright().start()
        
        browser = await _pw_instance.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--window-size=1920,1080",
            ],
        )
        _cdp_browser = browser
        logger.info("Eurowings: Playwright headless browser ready")
        return _cdp_browser


class EurowingsConnectorClient:
    """Eurowings hybrid scraper — cookie-farm + curl_cffi direct API."""

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
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Eurowings flights via cookie-farm + curl_cffi direct API.

        Fast path (~0.7s): curl_cffi with farmed CF cookies → POST QUERY_FLIGHT_DATA.
        Slow path (~15s): Playwright farms cookies first, then curl_cffi.
        Fallback: Full Playwright interception if API fails.
        """
        # ── City code expansion (LON → LHR, STN, etc.) ──
        origins = get_city_airports(req.origin)
        destinations = get_city_airports(req.destination)
        if len(origins) > 1 or len(destinations) > 1:
            tasks = []
            sem = asyncio.Semaphore(2)  # limit parallel sub-searches to avoid overwhelming Chrome
            for o in origins:
                for d in destinations:
                    if o == d:
                        continue
                    sub_req = FlightSearchRequest(
                        origin=o, destination=d,
                        date_from=req.date_from, return_from=req.return_from,
                        adults=req.adults, children=req.children, infants=req.infants,
                        cabin_class=req.cabin_class, currency=req.currency,
                        max_stopovers=req.max_stopovers,
                    )

                    async def _throttled(r=sub_req):
                        async with sem:
                            return await self._search_single(r)

                    tasks.append(_throttled())
            results = await asyncio.gather(*tasks, return_exceptions=True)
            all_offers: list[FlightOffer] = []
            for r in results:
                if isinstance(r, FlightSearchResponse):
                    all_offers.extend(r.offers)
            all_offers.sort(key=lambda o: o.price)
            h = hashlib.md5(
                f"ew{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
                currency=all_offers[0].currency if all_offers else "EUR",
                offers=all_offers, total_results=len(all_offers),
            )
        return await self._search_single(req)

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search a single origin→destination pair (no city expansion)."""
        t0 = time.monotonic()

        # April 2026: Eurowings has CF Turnstile + additional bot detection that blocks:
        # 1. Direct API calls to apps.eurowings.com (403 even with proxy)
        # 2. In-browser fetch from page context (Failed to fetch)
        # 3. SPA navigation (stuck at #/loading/shopping, never transitions to results)
        # Even with residential proxy, patchright stealth, and inject_stealth_js.
        # TODO: Investigate CAPTCHA solving service or mobile API.
        logger.warning("Eurowings: CF Turnstile + bot detection blocks all approaches - scraper disabled")
        return self._empty(req)

    # ------------------------------------------------------------------
    # Cookie farm — Playwright generates CF cookies
    # ------------------------------------------------------------------

    async def _ensure_cookies(self) -> list[dict]:
        """Return valid farmed cookies, farming new ones if needed."""
        global _farmed_cookies, _farm_timestamp
        lock = _get_farm_lock()
        async with lock:
            age = time.monotonic() - _farm_timestamp
            if _farmed_cookies and age < _COOKIE_MAX_AGE:
                return _farmed_cookies
            return await self._farm_cookies()

    async def _farm_cookies(self) -> list[dict]:
        """Visit Eurowings *booking* page to harvest Cloudflare cookies for the API."""
        global _farmed_cookies, _farm_timestamp

        browser = await _get_browser()
        ctx = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
        )
        try:
            page = await ctx.new_page()
            await auto_block_if_proxied(page)
            logger.info("Eurowings: farming cookies via booking page")
            # Visit the actual booking page (not just homepage) to get all session tokens
            await page.goto(
                "https://www.eurowings.com/en/booking/flights.html",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(random.uniform(2.5, 4))
            await self._dismiss_cookies(page)
            await asyncio.sleep(1)

            cookies = await ctx.cookies()
            _farmed_cookies = cookies
            _farm_timestamp = time.monotonic()
            logger.info("Eurowings: farmed %d cookies", len(cookies))
            return cookies
        except Exception as e:
            logger.error("Eurowings: cookie farm error: %s", e)
            return []
        finally:
            await ctx.close()

    # ------------------------------------------------------------------
    # Direct API via in-browser fetch (uses Chrome TLS to bypass CF)
    # ------------------------------------------------------------------

    async def _api_search(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[dict]:
        """POST QUERY_FLIGHT_DATA via in-browser fetch (bypasses Cloudflare TLS check)."""
        browser = await _get_browser()
        ctx = None
        page = None
        try:
            proxy = get_proxy("EUROWINGS_PROXY")
            logger.info("Eurowings API search: using proxy=%s", "yes" if proxy else "no")
            
            ctx = await browser.new_context(
                viewport=random.choice(_VIEWPORTS),
                locale=random.choice(_LOCALES),
                timezone_id=random.choice(_TIMEZONES),
                proxy=proxy,
            )
            page = await ctx.new_page()
            await inject_stealth_js(page)
            await auto_block_if_proxied(page)

            # Navigate to the actual booking page (same URL the cookie farm visits)
            # This generates the CF cookie for apps.eurowings.com subdomain
            await page.goto(
                "https://www.eurowings.com/en/booking/flights.html",
                wait_until="domcontentloaded",
                timeout=25000,
            )
            # Wait for Cloudflare challenge to resolve ("Just a moment..." page)
            for _cf_tick in range(20):
                title = await page.title()
                body_text = await page.evaluate("() => (document.body?.innerText || '').substring(0, 200)")
                if ("just a moment" not in title.lower()
                    and "checking" not in title.lower()
                    and "just a moment" not in body_text.lower()):
                    break
                logger.info("Eurowings: CF challenge in progress (%d)...", _cf_tick)
                await asyncio.sleep(2)
            await asyncio.sleep(2.0)

            # Dismiss cookies (same as farm does)
            await self._dismiss_cookies(page)
            await asyncio.sleep(1.0)

            body = self._build_api_body(req)
            result = await page.evaluate("""async (args) => {
                const [url, body] = args;
                try {
                    const resp = await fetch(url, {
                        method: 'POST',
                        headers: {
                            'content-type': 'application/json',
                            'accept': 'application/json, text/plain, */*',
                        },
                        body: JSON.stringify(body),
                        credentials: 'include',
                    });
                    if (!resp.ok) return {_error: resp.status, _text: (await resp.text()).substring(0, 200)};
                    return await resp.json();
                } catch (e) {
                    return {_error: -1, _text: e.message};
                }
            }""", [_API_URL, body])

            if not result or result.get("_error"):
                err = result.get("_error", "?") if result else "null"
                snippet = result.get("_text", "") if result else ""
                logger.warning("Eurowings: in-browser fetch returned %s — %s", err, snippet[:150])
                return None

            status = result.get("_payload", {}).get("_status", "")
            if status != "SUCCESS":
                logger.warning("Eurowings: API status=%s", status)
                return None

            logger.info("Eurowings: in-browser fetch SUCCESS")
            return result
        except Exception as e:
            logger.error("Eurowings: in-browser API error: %s", e)
            return None
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass

    @staticmethod
    def _build_api_body(req: FlightSearchRequest) -> dict:
        """Build the QUERY_FLIGHT_DATA request payload."""
        is_rt = bool(req.return_from)
        params = {
            "origin": req.origin,
            "destination": req.destination,
            "outwardDate": req.date_from.strftime("%Y-%m-%d"),
            "adultCount": req.adults,
            "childCount": req.children,
            "infantCount": req.infants,
            "tripType": "RETURN" if is_rt else "ONE_WAY",
            "locale": "en-GB",
            "systemType": "SYS_1N",
        }
        if is_rt:
            params["returnDate"] = req.return_from.strftime("%Y-%m-%d")
        return {
            "_payload": {
                "_type": "UPDATE_COMPONENT",
                "_updates": [{
                    "_type": "ew/components/booking/shoppingexperience",
                    "_path": "/content/eurowings/en/booking/flights/flight-search/shopping/select/jcr:content/main/flightselect",
                    "_action": "QUERY_FLIGHT_DATA",
                    "_parameters": params,
                }],
            }
        }

    # ------------------------------------------------------------------
    # Playwright browser fallback
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full browser automation fallback — deep link to search + intercept results."""
        browser = await _get_browser()
        vp = random.choice(_VIEWPORTS)
        
        # Get proxy for Eurowings (falls back to LETSFG_PROXY)
        proxy = get_proxy("EUROWINGS_PROXY")
        logger.info("Eurowings: using proxy=%s", "yes" if proxy else "no")
        
        ctx = await browser.new_context(
            viewport=vp,
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
            proxy=proxy,
        )
        page = await ctx.new_page()
        await inject_stealth_js(page)
        await auto_block_if_proxied(page)

        captured: dict = {}
        all_json_responses: list = []
        all_requests: list = []

        async def on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            status = response.status
            
            # Track all requests to find the real flight API
            if status == 200 and not any(x in url for x in ['cloudflare', 'onetrust', 'google', 'facebook', 'cookielaw']):
                all_requests.append((url[:80], ct[:20], status))
            
            # Log ALL JSON responses for debugging
            if "json" in ct and "cloudflare" not in url:
                try:
                    body = await response.body()
                    all_json_responses.append((url[:80], len(body)))
                except Exception:
                    pass
            
            if any(kw in url for kw in (
                "QUERY_FLIGHT_DATA", "flight-data", "flightdata",
                "search-results", "availability", "offers",
                "api/flights", "api/search", "graphql",
                "/content/eurowings/", "flight-search/sho",  # April 2026: AEM content API
            )):
                try:
                    if "json" in ct:
                        body = await response.json()
                        # Log structure of captured data
                        if isinstance(body, dict):
                            logger.info("Eurowings captured %s: keys=%s size=%d", 
                                url[-50:], list(body.keys())[:8], len(str(body)))
                        captured["data"] = body
                        captured["url"] = response.url
                        logger.info("Eurowings: captured API response from %s", response.url[:120])
                except Exception:
                    pass

        page.on("response", on_response)

        try:
            logger.info("Eurowings %s→%s: deep link search …", req.origin, req.destination)
            
            # Build deep link URL (April 2026 format)
            date_str = req.date_from.strftime("%Y-%m-%d")
            is_rt = bool(req.return_from)
            deep_url = (
                f"https://www.eurowings.com/en/booking/flights/flight-search.html"
                f"?isReward=false&destination={req.destination}&origin={req.origin}"
                f"&source=web&origins={req.origin}&fromdate={date_str}"
                f"&triptype={'return' if is_rt else 'oneway'}"
                f"&adults={req.adults}&children={req.children}&infants={req.infants}"
                f"&lng=en-GB"
            )
            if is_rt and req.return_from:
                deep_url += f"&todate={req.return_from.strftime('%Y-%m-%d')}"
            
            await page.goto(deep_url, wait_until="domcontentloaded", timeout=45000)
            
            # Wait for Cloudflare challenge to resolve
            for _cf_tick in range(15):
                title = await page.title()
                body_text = await page.evaluate("() => (document.body?.innerText || '').substring(0, 500)")
                if ("just a moment" not in title.lower()
                    and "checking" not in body_text.lower()
                    and "please wait" not in body_text.lower()):
                    break
                logger.info("Eurowings: CF challenge in progress (%d)...", _cf_tick)
                await asyncio.sleep(2)
            
            # Wait for SPA to transition from loading state
            # The hash changes from #/loading/shopping to #/shopping/outbound once results load
            for _spa_tick in range(12):
                current_url = page.url
                body_len = await page.evaluate("() => document.body?.innerText?.length || 0")
                if "/loading/" not in current_url and body_len > 2000:
                    logger.info("Eurowings: SPA loaded, bodyLen=%d", body_len)
                    break
                await asyncio.sleep(1)
                if _spa_tick % 4 == 3:
                    logger.info("Eurowings: waiting for SPA to load (%d), url=%s bodyLen=%d", 
                        _spa_tick, current_url[-60:], body_len)
            
            # Debug: check for console errors and log HTML snippet
            html_snippet = await page.evaluate("""() => {
                const html = document.documentElement.outerHTML;
                const body = document.body?.innerText || '';
                
                // Try to find Vue app instance
                let vueData = null;
                try {
                    // Vue 3 apps often mounted on #app
                    const app = document.querySelector('#app');
                    if (app && app.__vue_app__) {
                        const state = app.__vue_app__._context?.provides;
                        if (state) vueData = Object.keys(state).slice(0, 5);
                    }
                    // Also check __VUE__
                    if (window.__VUE__) {
                        vueData = ['__VUE__ found'];
                    }
                } catch(e) {}
                
                // Check for flight data in various places
                let flightDataFound = null;
                try {
                    // Check window for any flight-related data
                    for (const key of Object.keys(window)) {
                        const val = window[key];
                        if (val && typeof val === 'object') {
                            const str = JSON.stringify(val).substring(0, 500);
                            if (str.includes('flight') || str.includes('journey') || str.includes('fare')) {
                                flightDataFound = key;
                                break;
                            }
                        }
                    }
                } catch(e) {}
                
                // Check for #app content
                const appEl = document.querySelector('#app');
                const appHtml = appEl ? appEl.innerHTML.length : 0;
                const appText = appEl ? appEl.innerText?.length : 0;
                
                return {
                    htmlLen: html.length,
                    vueData,
                    flightDataFound,
                    appHtmlLen: appHtml,
                    appTextLen: appText,
                    titleTag: document.title,
                    bodySample: body.substring(0, 150)
                };
            }""")
            logger.info("Eurowings Vue debug: htmlLen=%d vueData=%s flightData=%s appHtml=%d appText=%d title=%s",
                html_snippet.get("htmlLen", 0), html_snippet.get("vueData"),
                html_snippet.get("flightDataFound"), html_snippet.get("appHtmlLen", 0),
                html_snippet.get("appTextLen", 0), html_snippet.get("titleTag", "?")[:50])
            
            # Wait for results to load (look for price indicator or flight count)
            try:
                await page.wait_for_function(
                    "() => /€\\s*\\d+|\\d+\\s*(out of|flights)/i.test(document.body.innerText)",
                    timeout=15000
                )
                logger.info("Eurowings: results indicator found")
            except Exception:
                # Log what we see on the page for debugging
                body_sample = await page.evaluate("() => document.body?.innerText?.substring(0, 300) || 'empty'")
                logger.warning("Eurowings: no results indicator, page says: %s", body_sample[:150])
            
            await asyncio.sleep(4)  # Let JS fully render
            
            # Debug: log page state after render wait
            post_wait_state = await page.evaluate("""() => {
                const body = document.body?.innerText || '';
                return {
                    url: location.href,
                    bodyLen: body.length,
                    hasPrice: /€\\s*\\d+/.test(body),
                    sampleText: body.substring(0, 200)
                };
            }""")
            logger.info("Eurowings post-wait: url=%s bodyLen=%d hasPrice=%s", 
                post_wait_state.get("url", "?")[:80], post_wait_state.get("bodyLen", 0), post_wait_state.get("hasPrice"))
            
            # Debug: log all JSON responses captured + any API requests
            if all_json_responses:
                logger.info("Eurowings: captured %d JSON responses: %s", 
                    len(all_json_responses), 
                    "; ".join(f"{sz}b:{u}" for u, sz in all_json_responses[:8]))
            else:
                logger.warning("Eurowings: no JSON responses captured")
            
            # Log any 'apps' or 'api' URLs we saw
            api_requests = [r for r in all_requests if 'apps.' in r[0] or '/api/' in r[0] or 'flightsearch' in r[0]]
            if api_requests:
                logger.info("Eurowings API requests: %s", api_requests[:5])

            offers: list[FlightOffer] = []
            if captured.get("data"):
                offers = self._parse_response(captured["data"], req)

            if not offers:
                offers = await self._extract_from_page(page, req)

            elapsed = time.monotonic() - t0
            return self._build_response(offers, req, elapsed) if offers else self._empty(req)
        except Exception as exc:
            logger.warning("Eurowings Playwright fallback error: %s", exc)
            return self._empty(req)
        finally:
            try:
                await ctx.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Cookie / overlay helpers
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        for sel in (
            "#onetrust-accept-btn-handler",
            'button:has-text("Accept All")',
            'button:has-text("Alle akzeptieren")',
        ):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.is_visible(timeout=2000):
                    await btn.click(timeout=3000)
                    await asyncio.sleep(1)
                    logger.info("Eurowings: dismissed cookies via %s", sel)
                    return
            except Exception:
                continue

    async def _remove_overlays(self, page) -> None:
        """Remove overlay and login panel that block pointer events on the form."""
        await page.evaluate("""() => {
            document.querySelectorAll('.o-layer__overlay').forEach(o => o.remove());
            document.querySelectorAll('.o-layer-myewlogin, [class*="myew-login"]').forEach(l => {
                l.style.display = 'none';
            });
        }""")

    async def _close_popover(self, page) -> None:
        """Close any open popover overlay (date picker / airport selector)."""
        await page.evaluate("""() => {
            document.querySelectorAll('[class*="_popoverOverlay_"]').forEach(o => o.remove());
        }""")

    # ------------------------------------------------------------------
    # Form filling
    # ------------------------------------------------------------------

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> None:
        await self._fill_airport(page, "Departure airport", req.origin)
        await asyncio.sleep(0.8)
        await self._remove_overlays(page)
        await self._fill_airport(page, "Destination airport", req.destination)
        await asyncio.sleep(0.8)
        await self._remove_overlays(page)
        await self._fill_date(page, req.date_from)
        await asyncio.sleep(0.5)

    async def _fill_airport(self, page, label: str, code: str) -> None:
        """Click trigger button → dialog opens → type in autocomplete → select matching option."""
        logger.info("Eurowings: filling %s → %s", label, code)

        # April 2026 UI: The form has buttons with role="button" containing a textbox
        # e.g. button with name "Departure airport" that contains textbox "Departure airport"
        trigger = page.get_by_role("button", name=label).first
        try:
            if await trigger.count() > 0:
                await trigger.click(timeout=5000)
            else:
                raise Exception("trigger not found by getByRole")
        except Exception:
            # Try broader selectors — Eurowings may have changed button text
            for sel in (
                f'button:has-text("{label}")',
                f'[aria-label="{label}"]',
                f'[aria-label*="{label.split()[0]}"]',  # "Departure" or "Destination"
                f'button[aria-haspopup="dialog"]:has-text("{label.split()[0]}")',
                '[data-testid*="origin"]' if "Departure" in label else '[data-testid*="destination"]',
            ):
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await self._remove_overlays(page)
                        await el.click(timeout=5000)
                        break
                except Exception:
                    continue
            else:
                logger.warning("Eurowings: could not find %s button", label)
                return
        await asyncio.sleep(1)

        # April 2026 UI: The dialog contains a placeholder-based textbox (not readonly)
        # Use getByPlaceholder which matches better on this SPA
        dialog_input = page.get_by_placeholder(label).first
        if await dialog_input.count() == 0:
            dialog_input = page.locator(
                f'[role="dialog"] input[type="text"]'
            ).first
        if await dialog_input.count() == 0:
            dialog_input = page.locator(
                f'input[aria-label="{label}"]:not([readonly])'
            ).first

        await dialog_input.fill("")
        await asyncio.sleep(0.2)
        await dialog_input.fill(code)
        logger.info("Eurowings: typed '%s' in %s field", code, label)
        await asyncio.sleep(1.5)

        # April 2026 UI: Suggestions are [role="option"] inside [role="listbox"]
        clicked = False
        options = page.get_by_role("option")
        opt_count = await options.count()
        logger.info("Eurowings: found %d autocomplete options", opt_count)
        for i in range(min(opt_count, 20)):
            try:
                text = await options.nth(i).inner_text()
                # Match e.g. "Dusseldorf • DUS Germany" or "Palma de Mallorca • PMI Spain"
                if code.upper() in text.upper():
                    await options.nth(i).click(timeout=3000)
                    logger.info("Eurowings: selected '%s'", text.strip()[:50])
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            # Fallback: click first option
            if opt_count > 0:
                await options.first.click(timeout=3000)
                logger.info("Eurowings: selected first option (fallback)")

        await asyncio.sleep(1)
        # Verify the readonly input got the value
        val = await page.evaluate(
            f'() => document.querySelector(\'input[aria-label="{label}"]\')?.value || ""'
        )
        logger.info("Eurowings: %s = '%s'", label, val)

    async def _fill_date(self, page, target) -> None:
        """Click date trigger → find and click the target date button in calendar."""
        logger.info("Eurowings: filling date %s", target)

        # April 2026 UI: Click the "Outgoing flight" button by role
        date_trigger = page.get_by_role("button", name="Outgoing flight").first
        try:
            if await date_trigger.count() > 0:
                await date_trigger.click(timeout=5000)
            else:
                raise Exception("date trigger not found by getByRole")
        except Exception:
            await self._remove_overlays(page)
            date_trigger = page.locator('button:has-text("Outgoing flight")').first
            await date_trigger.click(timeout=5000)
        await asyncio.sleep(1.5)

        # April 2026 UI: Calendar has buttons with aria-labels like "Saturday, 15 August 2026"
        # Find and click the target date button
        target_str = target.strftime("%A, %-d %B %Y")  # "Saturday, 15 August 2026"
        target_str_win = target.strftime("%A, %d %B %Y").replace(" 0", " ")  # Windows strftime
        date_btn = page.get_by_role("button", name=target_str).first
        if await date_btn.count() == 0:
            date_btn = page.get_by_role("button", name=target_str_win).first
        
        if await date_btn.count() > 0:
            await date_btn.click(timeout=3000)
            logger.info("Eurowings: clicked date '%s'", target_str[:30])
            await asyncio.sleep(1)
            return

        # Fallback: try locator with aria-label containing date components
        day = target.day
        month = target.strftime("%B")
        for sel in [
            f'button[aria-label*="{day} {month}"]',
            f'button[aria-label*="{day}, {month}"]',
            f'[role="gridcell"] button:has-text("{day}")',
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and not await btn.is_disabled():
                    await btn.click(timeout=3000)
                    logger.info("Eurowings: clicked date via fallback %s", sel[:30])
                    await asyncio.sleep(1)
                    return
            except Exception:
                continue

        # Older fallback: try typing in DD/MM/YY input
        date_input = page.get_by_placeholder("dateInputMask").first
        if await date_input.count() > 0:
            date_str = target.strftime("%d/%m/%y")
            await date_input.fill(date_str)
            await date_input.press("Enter")
            logger.info("Eurowings: typed date '%s' in input", date_str)
            await asyncio.sleep(1)
            return

        logger.warning("Eurowings: could not select date %s", target)
        await self._close_popover(page)

    async def _fill_date_calendar(self, page, target) -> None:
        """Fallback: navigate calendar and click the day."""
        for _ in range(6):
            try:
                header = await page.locator(
                    "[class*='calendar'] [class*='month'], [class*='datepicker'] [class*='header']"
                ).first.inner_text(timeout=2000)
                if target.strftime("%B") in header and str(target.year) in header:
                    break
            except Exception:
                pass
            try:
                next_btn = page.locator(
                    "button[aria-label*='next' i], button[aria-label*='Next' i]"
                ).first
                if await next_btn.count() > 0:
                    await next_btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                break

        day = target.day
        for sel in [
            f"[data-date='{target.isoformat()}']",
            f"button[aria-label*='{day} {target.strftime('%B')}']",
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(timeout=3000)
                    return
            except Exception:
                continue

    async def _click_search(self, page) -> None:
        """Click the "Search for flight" submit button."""
        # Ensure no overlays block the button
        await self._remove_overlays(page)
        await self._close_popover(page)
        await asyncio.sleep(0.3)

        # April 2026 UI: Use getByRole for the submit button
        submit = page.get_by_role("button", name="Search for flight").first
        if await submit.count() > 0:
            try:
                await submit.click(timeout=5000)
                logger.info("Eurowings: clicked search (getByRole)")
                return
            except Exception:
                pass

        # Fallback: data attribute
        submit = page.locator("button[data-flight-search-submit-button]").first
        if await submit.count() > 0:
            try:
                await submit.click(timeout=5000)
                logger.info("Eurowings: clicked search (data-attr)")
                return
            except Exception:
                pass

        # Fallback: by text
        for label in ["Search for flight", "Search", "Flug suchen"]:
            try:
                btn = page.locator(f'button:has-text("{label}")').first
                if await btn.count() > 0:
                    await self._remove_overlays(page)
                    await self._close_popover(page)
                    await btn.click(timeout=5000)
                    logger.info("Eurowings: clicked search (%s)", label)
                    return
            except Exception:
                continue

        # Last resort: submit button
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # ------------------------------------------------------------------
    # Results extraction
    # ------------------------------------------------------------------

    async def _extract_from_page(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Wait for results page and try multiple extraction methods."""
        await asyncio.sleep(5)

        url = page.url
        logger.info("Eurowings: results page URL: %s", url)

        # Method 1: Try __NUXT__ / appData
        data = await page.evaluate("""() => {
            if (window.__NUXT__) return window.__NUXT__;
            if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
            if (window.appData) return window.appData;
            const scripts = document.querySelectorAll('script[type="application/json"]');
            for (const s of scripts) {
                try {
                    const d = JSON.parse(s.textContent);
                    if (d && (d.flights || d.outbound || d.offers || d.fares)) return d;
                } catch {}
            }
            return null;
        }""")
        if data:
            offers = self._parse_response(data, req)
            if offers:
                return offers

        # Method 2: Scrape flight cards from DOM
        return await self._scrape_flight_cards(page, req)

    async def _scrape_flight_cards(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from rendered result cards."""
        # April 2026 UI: Flight results are in a region with clickable generic elements
        # Each flight row has departure/arrival times, duration, stops, and price
        cards_data = await page.evaluate(r"""() => {
            const results = [];
            
            // New UI (April 2026): flights are in [cursor=pointer] wrappers inside a region
            // Each has: times (6:05AM DUS → 8:30AM PMI), duration (2h 25min), price (€153.99)
            const cards = document.querySelectorAll(
                '[cursor="pointer"], [style*="cursor: pointer"], [role="button"], ' +
                '[class*="flight-card"], [class*="FlightCard"], [class*="result-card"], ' +
                '[class*="fare-card"], [data-flight], [class*="journey"]'
            );
            
            cards.forEach(card => {
                const text = card.textContent || '';
                // Skip non-flight elements
                if (text.length < 20 || text.length > 2000) return;
                if (!text.match(/\d{1,2}:\d{2}/) && !text.match(/€\s?\d+/)) return;
                
                // Extract price: €153.99 or 153.99 € or EUR 153.99
                const priceMatch = text.match(/€\s*(\d+[.,]\d{2})|(\d+[.,]\d{2})\s*€|EUR\s*(\d+[.,]\d{2})/);
                // Extract times: 6:05AM or 06:05 or 6:05 AM
                const timeMatches = text.match(/(\d{1,2}:\d{2}\s*(?:AM|PM)?)/gi);
                // Extract flight number: EW123 or EW1234
                const fnMatch = text.match(/EW\s*\d{3,4}/i);
                // Extract duration: 2h 25min or 2h25m
                const durMatch = text.match(/(\d+)h\s*(\d+)?\s*min/i);
                // Extract stops: Nonstop or 1 stop
                const stopsMatch = text.match(/Nonstop|(\d+)\s*stop/i);
                
                if (priceMatch && timeMatches && timeMatches.length >= 2) {
                    const priceStr = (priceMatch[1] || priceMatch[2] || priceMatch[3] || '').replace(',', '.');
                    results.push({
                        price: priceStr,
                        times: timeMatches.slice(0, 2),  // dep, arr
                        flightNo: fnMatch ? fnMatch[0].replace(' ', '') : '',
                        duration: durMatch ? (parseInt(durMatch[1] || 0) * 60 + parseInt(durMatch[2] || 0)) : 0,
                        stops: stopsMatch ? (stopsMatch[0].toLowerCase() === 'nonstop' ? 0 : parseInt(stopsMatch[1] || 1)) : 0,
                        text: text.trim().substring(0, 250),
                    });
                }
            });
            
            // Deduplicate by price + time combination
            const seen = new Set();
            return results.filter(r => {
                const key = r.price + r.times.join('_');
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
            });
        }""")

        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)
        logger.info("Eurowings: scraped %d flight cards from DOM", len(cards_data))
        
        # Debug: if no cards found, log DOM structure hints
        if not cards_data:
            debug_info = await page.evaluate(r"""() => {
                const body = document.body?.innerText || '';
                const hasPrice = /€\s*\d+/.test(body);
                const hasTime = /\d{1,2}:\d{2}/.test(body);
                const allClasses = [...document.querySelectorAll('[class]')].map(e => e.className).join(' ');
                const flightish = allClasses.match(/[a-z-_]*(?:flight|fare|price|journey|result|offer|bound)[a-z-_]*/gi) || [];
                const uniqueClasses = [...new Set(flightish)].slice(0, 15);
                return {
                    hasPrice,
                    hasTime,
                    bodyLen: body.length,
                    priceSnippet: (body.match(/€\s*\d+[.,]?\d*/g) || []).slice(0, 5),
                    flightClasses: uniqueClasses,
                    bodySample: body.substring(0, 400)
                };
            }""")
            logger.warning("Eurowings DOM debug: hasPrice=%s hasTime=%s bodyLen=%d classes=%s prices=%s",
                debug_info.get("hasPrice"), debug_info.get("hasTime"), debug_info.get("bodyLen", 0),
                debug_info.get("flightClasses", [])[:8], debug_info.get("priceSnippet", []))
            logger.info("Eurowings body sample: %s", debug_info.get("bodySample", "")[:200])
        
        for card in cards_data:
            try:
                price = float(card["price"]) if card.get("price") else None
                if not price or price <= 0:
                    continue
                times = card.get("times", [])
                dep_time = times[0] if len(times) >= 1 else None
                arr_time = times[1] if len(times) >= 2 else None
                flight_no = card.get("flightNo", "").replace(" ", "")
                duration_mins = card.get("duration", 0)
                stops = card.get("stops", 0)

                dep_dt = self._parse_time_on_date(dep_time, req.date_from) if dep_time else datetime(2000, 1, 1)
                arr_dt = self._parse_time_on_date(arr_time, req.date_from) if arr_time else datetime(2000, 1, 1)

                _ew_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                seg = FlightSegment(
                    airline="EW", airline_name="Eurowings", flight_no=flight_no,
                    origin=req.origin, destination=req.destination,
                    departure=dep_dt, arrival=arr_dt, cabin_class=_ew_cabin,
                )
                # Use scraped duration if available, else compute from times
                if duration_mins > 0:
                    dur = duration_mins * 60
                elif dep_dt.year > 2000 and arr_dt.year > 2000:
                    dur = int((arr_dt - dep_dt).total_seconds())
                else:
                    dur = 0
                route = FlightRoute(segments=[seg], total_duration_seconds=max(dur, 0), stopovers=stops)
                fkey = f"{flight_no}_{dep_dt.isoformat()}_{price}"
                offers.append(FlightOffer(
                    id=f"ew_{hashlib.md5(fkey.encode()).hexdigest()[:12]}",
                    price=round(price, 2), currency=req.currency or "EUR",
                    price_formatted=f"{price:.2f} EUR",
                    outbound=route, inbound=None,
                    airlines=["Eurowings"], owner_airline="EW",
                    booking_url=booking_url, is_locked=False,
                    source="eurowings_direct", source_tier="free",
                ))
            except Exception:
                continue
        return offers

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the QUERY_FLIGHT_DATA response format.

        Structure: _payload._updates[0]._resultData.flights[0].schedules[0].journeys[]
        Each journey has segments[] and fares[] with farePrices[].
        """
        if not isinstance(data, dict):
            return []
        
        # Debug: log top-level keys to understand structure
        top_keys = list(data.keys())[:10]
        logger.info("Eurowings API response keys: %s (total %d)", top_keys, len(data.keys()))
        
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        is_rt = bool(req.return_from)

        # Navigate to the journeys array
        ob_journeys, ib_journeys = self._extract_journeys(data)
        if ob_journeys:
            ob_offers: list[FlightOffer] = []
            for journey in ob_journeys:
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    ob_offers.append(offer)

            if is_rt and ib_journeys:
                # Parse inbound journeys
                ib_routes: list[tuple[FlightRoute, float, list[str]]] = []
                for journey in ib_journeys:
                    offer = self._parse_journey(journey, req, booking_url)
                    if offer and offer.outbound:
                        ib_routes.append((offer.outbound, offer.price, offer.airlines))

                if ib_routes:
                    ib_routes.sort(key=lambda x: x[1])
                    cheapest_ib_route, cheapest_ib_price, _ = ib_routes[0]
                    for ob in ob_offers:
                        total = round(ob.price + cheapest_ib_price, 2)
                        key = f"{ob.id}_rt_{total}"
                        offers.append(FlightOffer(
                            id=f"ew_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                            price=total,
                            currency="EUR",
                            price_formatted=f"{total:.2f} EUR",
                            outbound=ob.outbound,
                            inbound=cheapest_ib_route,
                            airlines=ob.airlines,
                            owner_airline="EW",
                            booking_url=booking_url,
                            is_locked=False,
                            source="eurowings_direct",
                            source_tier="free",
                        ))

            # Always emit OW outbound for combo engine
            offers.extend(ob_offers)
            return offers

        # Fallback: try legacy keys
        currency = data.get("currency", req.currency or "EUR")

        flights_raw = (
            data.get("outboundFlights") or data.get("outbound")
            or data.get("flights") or data.get("offers")
            or (data.get("journeys", {}).get("outbound") if isinstance(data.get("journeys"), dict) else None)
            or []
        )
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)

        if not offers:
            fares_raw = data.get("fares") or data.get("results") or []
            if isinstance(fares_raw, list):
                for fare in fares_raw:
                    offer = self._parse_single_flight(fare, currency, req, booking_url)
                    if offer:
                        offers.append(offer)

        return offers

    @staticmethod
    def _extract_journeys(data: dict) -> tuple[list, list]:
        """Navigate the nested QUERY_FLIGHT_DATA response to find outbound & inbound journeys."""
        try:
            updates = data.get("_payload", {}).get("_updates", [])
            for upd in updates:
                rd = upd.get("_resultData", {})
                flights = rd.get("flights", [])
                if flights:
                    schedules = flights[0].get("schedules", [])
                    ob = schedules[0].get("journeys", []) if len(schedules) > 0 else []
                    ib = schedules[1].get("journeys", []) if len(schedules) > 1 else []
                    return ob, ib
        except (KeyError, IndexError, TypeError):
            pass
        return [], []

    def _parse_journey(
        self, journey: dict, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        """Parse a single journey from the QUERY_FLIGHT_DATA response."""
        dep_dt_str = journey.get("departureDateTime", "")
        arr_dt_str = journey.get("arrivalDateTime", "")
        duration_str = journey.get("duration", "")
        is_nonstop = journey.get("nonstop", False)

        # Parse segments
        segments: list[FlightSegment] = []
        for seg_raw in journey.get("segments", []):
            dep_info = seg_raw.get("departure", {})
            arr_info = seg_raw.get("arrival", {})
            op_info = seg_raw.get("operatorInformation", {})

            origin = dep_info.get("station", {}).get("tlc", req.origin)
            dest = arr_info.get("station", {}).get("tlc", req.destination)
            dep_time = dep_info.get("dateTime", dep_dt_str)
            arr_time = arr_info.get("dateTime", arr_dt_str)
            airline_code = op_info.get("airlineCode", "EW")
            flight_no = op_info.get("flightNo", "")
            airline_name = op_info.get("operatingAirlineName", "Eurowings")

            _ew_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            segments.append(FlightSegment(
                airline=airline_code,
                airline_name=airline_name,
                flight_no=f"{airline_code}{flight_no}" if flight_no else "",
                origin=origin,
                destination=dest,
                departure=self._parse_dt(dep_time),
                arrival=self._parse_dt(arr_time),
                cabin_class=_ew_cabin,
            ))

        if not segments:
            return None

        # Extract cheapest fare price
        price = self._extract_cheapest_from_fares(journey.get("fares", []))
        if price is None or price <= 0:
            return None

        # Build duration
        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        # Parse HH:MM duration string as fallback
        if total_dur <= 0 and duration_str:
            try:
                parts = duration_str.split(":")
                total_dur = int(parts[0]) * 3600 + int(parts[1]) * 60
            except (ValueError, IndexError):
                pass

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = f"{dep_dt_str}_{arr_dt_str}_{segments[0].flight_no}"
        airlines = list({s.airline_name for s in segments})

        return FlightOffer(
            id=f"ew_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency="EUR",
            price_formatted=f"{price:.2f} EUR",
            outbound=route,
            inbound=None,
            airlines=airlines,
            owner_airline="EW",
            booking_url=booking_url,
            is_locked=False,
            source="eurowings_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_cheapest_from_fares(fares: list) -> Optional[float]:
        """Find the cheapest fare price from QUERY_FLIGHT_DATA fare structures."""
        prices: list[float] = []
        for fare in fares:
            for fp in fare.get("farePrices", []):
                # Try price.value first (may be promo), then originalPrice.value
                for price_key in ("price", "originalPrice", "totalPrice"):
                    p_obj = fp.get(price_key, {})
                    if isinstance(p_obj, dict) and "value" in p_obj:
                        try:
                            val = float(p_obj["value"])
                            if val > 0:
                                prices.append(val)
                                break
                        except (TypeError, ValueError):
                            continue
        return min(prices) if prices else None

    def _parse_single_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        price = (
            flight.get("price") or flight.get("totalPrice") or flight.get("lowestPrice")
            or flight.get("farePrice") or self._extract_cheapest_fare(flight)
        )
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        _ew_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination, _ew_cabin))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination, _ew_cabin))
        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments, total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("flightKey") or flight.get("id") or flight.get("flightId")
            or flight.get("flightNumber", "") + "_" + segments[0].departure.isoformat()
        )
        return FlightOffer(
            id=f"ew_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["Eurowings"], owner_airline="EW",
            booking_url=booking_url, is_locked=False,
            source="eurowings_direct", source_tier="free",
        )

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str, cabin_class: str = "economy") -> FlightSegment:
        dep_str = seg.get("departure") or seg.get("departureDate") or seg.get("departureTime") or seg.get("std") or ""
        arr_str = seg.get("arrival") or seg.get("arrivalDate") or seg.get("arrivalTime") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("flightNo") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or seg.get("departureCode") or default_origin
        destination = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or seg.get("arrivalCode") or default_dest
        return FlightSegment(
            airline="EW", airline_name="Eurowings", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class=cabin_class,
        )

    @staticmethod
    def _extract_cheapest_fare(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareBundles") or flight.get("fareOptions") or []
        prices: list[float] = []
        for f in fares:
            p = f.get("price") or f.get("amount") or f.get("totalPrice") or f.get("lowestPrice")
            if p is not None:
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    continue
        return min(prices) if prices else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Eurowings %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"eurowings{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[: len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _parse_time_on_date(time_str: str, date) -> datetime:
        """Parse time string (HH:MM, HH:MMAM, HH:MM PM, etc.) and combine with search date."""
        try:
            # Clean the time string
            t = time_str.strip().upper()
            
            # Check for AM/PM format: "6:05AM", "6:05 AM", "06:05AM"
            is_pm = "PM" in t
            is_am = "AM" in t
            t = t.replace("AM", "").replace("PM", "").strip()
            
            # Parse hour and minute
            parts = t.split(":")
            h = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 else 0
            
            # Convert 12-hour to 24-hour
            if is_pm and h < 12:
                h += 12
            elif is_am and h == 12:
                h = 0
            
            return datetime(date.year, date.month, date.day, h, m)
        except Exception:
            return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        is_rt = bool(req.return_from)
        url = (
            f"https://www.eurowings.com/en/booking/flights/search.html"
            f"?origin={req.origin}&destination={req.destination}"
            f"&outboundDate={dep}&adultCount={req.adults}"
            f"&childCount={req.children}&infantCount={req.infants}"
            f"&isOneWay={'false' if is_rt else 'true'}"
        )
        if is_rt:
            url += f"&returnDate={req.return_from.strftime('%Y-%m-%d')}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"eurowings{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
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
                    id=f"rt_euro_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
