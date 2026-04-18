"""
China Southern Airlines (CZ) — Patchright connector — Cloud Run patch.

Replaces CDP Chrome with Patchright headed browser to bypass AWS WAF.
Form fill + API interception + DOM scraping logic preserved from existing patch.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
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

logger = logging.getLogger(__name__)


async def _launch_browser():
    """Launch a Patchright browser using the system Chrome binary."""
    from patchright.async_api import async_playwright
    from .browser import find_chrome, inject_stealth_js, auto_block_if_proxied

    proxy = None
    _BYPASS = ".google.com,.googletagmanager.com,.gstatic.com,.googleapis.com,.google-analytics.com,.googlesyndication.com,.doubleclick.net"
    letsfg_proxy = os.environ.get("LETSFG_PROXY", "").strip()
    if letsfg_proxy:
        import socket as _sock
        try:
            _s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
            _s.connect(("127.0.0.1", 8899))
            _s.close()
            proxy = {"server": "http://127.0.0.1:8899", "bypass": _BYPASS}
            logger.info("ChinaSouthern: using proxy relay on port 8899")
        except OSError:
            from urllib.parse import urlparse
            p = urlparse(letsfg_proxy)
            proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}", "bypass": _BYPASS}
            if p.username:
                proxy["username"] = p.username
                proxy["password"] = p.password or ""
            logger.info("ChinaSouthern: using direct proxy %s:%s", p.hostname, p.port)
    else:
        logger.info("ChinaSouthern: no proxy, direct connection")

    try:
        chrome_path = find_chrome()
        logger.info("ChinaSouthern: using system Chrome at %s", chrome_path)
    except RuntimeError:
        chrome_path = None

    pw = await async_playwright().start()
    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--no-default-browser-check",
        "--window-size=1400,900",
    ]
    launch_kwargs = dict(
        headless=False,
        args=launch_args,
        proxy=proxy,
    )
    if chrome_path:
        launch_kwargs["executable_path"] = chrome_path
    browser = await pw.chromium.launch(**launch_kwargs)
    context = await browser.new_context(
        viewport={"width": 1400, "height": 900},
        locale="en-US",
        timezone_id="Asia/Shanghai",
        color_scheme="light",
    )
    page = await context.new_page()

    await inject_stealth_js(page)
    await auto_block_if_proxied(page)

    return pw, browser, context, page


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


class ChinaSouthernConnectorClient:
    """China Southern Airlines (CZ) CDP Chrome connector."""

    IATA = "CZ"
    AIRLINE_NAME = "China Southern Airlines"
    SOURCE = "chinasouthern_direct"
    HOMEPAGE = "https://www.csair.com/eu/en/index.shtml"
    DEFAULT_CURRENCY = "CNY"

    def __init__(self, timeout: float = 120.0):
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
        pw = browser = context = page = None
        pw, browser, context, page = await _launch_browser()

        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            url = response.url.lower()
            is_results_domain = "oversea.csair.com" in url or "b2c.csair.com" in url
            try:
                ct = response.headers.get("content-type", "")

                # For results domains: log non-200s AND non-JSON so we can see challenges
                if is_results_domain and response.status not in (200, 201):
                    logger.info("ChinaSouthern: results domain %d: url=%s", response.status, url[:120])
                    return
                if response.status not in (200, 201):
                    return

                # For the results domains, log ALL large responses (not just JSON) so
                # we can see bot-challenge pages, HTML state, etc.
                if is_results_domain and "json" not in ct and "javascript" not in ct:
                    body_peek = await response.text()
                    if len(body_peek) > 200:
                        logger.info("ChinaSouthern: non-JSON from results domain: ct=%s url=%s size=%d sample=%s",
                                    ct[:40], url[:100], len(body_peek), body_peek[:120].replace('\n',' '))
                    return

                if "json" not in ct and "javascript" not in ct:
                    return
                body = await response.text()
                if len(body) < 100:
                    return
                try:
                    data = json.loads(body)
                except Exception:
                    return
                if not isinstance(data, dict):
                    return

                # Log ALL JSON API responses for diagnostic (URL + top-level keys)
                keys_str = " ".join(str(k).lower() for k in data.keys())
                logger.info("ChinaSouthern: API response → %s | keys=%s | size=%d", url[:100], keys_str[:80], len(body))

                # Prefer the real shopping payload over any other JSON responses.
                if "/api/shop/search" in url or "/api/shop/poll" in url:
                    ita = data.get("ita") if isinstance(data, dict) else None
                    ita_keys = list(ita.keys())[:15] if isinstance(ita, dict) else []
                    logger.info("ChinaSouthern: /api/shop/ captured, ita_keys=%s", ita_keys)
                    
                    # Check if this response has actual flight data (not just skeleton)
                    has_flight_data = False
                    if isinstance(ita, dict):
                        # Check for populated solutionSet, sliceGrid with data, or flights array
                        solution_set = ita.get("solutionSet")
                        slice_grid = ita.get("sliceGrid")
                        flights = ita.get("flights") or ita.get("journeys") or ita.get("offers")
                        
                        if isinstance(solution_set, dict) and solution_set.get("solutions"):
                            has_flight_data = True
                            logger.info("ChinaSouthern: found %d solutions", len(solution_set.get("solutions", [])))
                        elif isinstance(slice_grid, list) and slice_grid:
                            has_flight_data = True
                            logger.info("ChinaSouthern: found %d items in sliceGrid", len(slice_grid))
                        elif isinstance(slice_grid, dict):
                            # sliceGrid might be {'column': [...], 'row': [...]} with actual data
                            cols = slice_grid.get("column") or []
                            rows = slice_grid.get("row") or []
                            if cols or rows:
                                has_flight_data = True
                                logger.info("ChinaSouthern: sliceGrid has cols=%d rows=%d", len(cols), len(rows))
                        elif isinstance(flights, list) and flights:
                            has_flight_data = True
                            logger.info("ChinaSouthern: found %d flights in ita", len(flights))
                    
                    search_data.clear()
                    search_data.update({"ita": ita if ita is not None else data, "_src": "shop_search"})
                    
                    # Only set event if we have real data (otherwise keep waiting)
                    if has_flight_data:
                        api_event.set()
                    else:
                        logger.info("ChinaSouthern: skeleton response, waiting for more data...")
                    return

                # Check if this looks like flight data
                if any(k in keys_str for k in ["flight", "itiner", "offer", "fare",
                                                 "bound", "trip", "result", "segment",
                                                 "avail", "journey", "price",
                                                 "dateflight", "success"]):
                    has_real_data = data.get("data") is not None
                    if has_real_data or not search_data:
                        search_data.update(data)
                        api_event.set()
                        logger.info("ChinaSouthern: captured flight data → %s (%d keys)", url[:80], len(data))
                    else:
                        logger.info("ChinaSouthern: skipping error response → %s", url[:80])
            except Exception:
                pass

        page.on("response", _on_response)

        # Also listen on any new pages opened by search (China Southern often opens b2c.csair.com in new tab)
        def _attach_listener(new_page):
            new_page.on("response", _on_response)
        context.on("page", _attach_listener)

        try:
            logger.info("ChinaSouthern: loading homepage for %s→%s", req.origin, req.destination)
            # Use "commit" (first bytes) + longer timeout when proxied (residential proxy adds latency)
            _proxy_active = bool(os.environ.get("LETSFG_PROXY"))
            _goto_timeout = 60000 if _proxy_active else 30000
            _wait_until = "commit" if _proxy_active else "domcontentloaded"
            await page.goto(self.HOMEPAGE, wait_until=_wait_until, timeout=_goto_timeout)
            await asyncio.sleep(3.0 if not _proxy_active else 5.0)

            # Diagnostic: log page title and URL
            page_title = await page.title()
            logger.info("ChinaSouthern: page loaded, url=%s, title=%s", page.url[:80], page_title[:40])

            await _dismiss_overlays(page)

            # Wait for the booking form to be ready (Vue/React hydration)
            try:
                form_field = page.get_by_role("textbox", name="From")
                await form_field.wait_for(state="visible", timeout=10000)
                logger.info("ChinaSouthern: booking form ready")
            except Exception:
                logger.warning("ChinaSouthern: form field 'From' not visible after 10s, trying anyway")

            # Click One-way toggle FIRST (before any other field interactions)
            # This changes the calendar to single-date mode
            try:
                one_way = page.locator('text=One-way').first
                await one_way.click(force=True)  # Force click in case of overlay
                await asyncio.sleep(2.0)  # Wait for Vue form re-render after toggle
                logger.info("ChinaSouthern: clicked One-way toggle")
            except Exception as e:
                logger.warning("ChinaSouthern: One-way click failed: %s", e)
                # Try via JS
                try:
                    await page.evaluate("document.querySelector('[data-value=\"1\"]')?.click()")
                    await asyncio.sleep(1.5)
                except Exception:
                    pass

            # Dismiss any calendar that might have opened
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)

            # Fill origin using modern form with autocomplete
            ok = await self._fill_airport_modern(page, "From", req.origin)
            if not ok:
                return self._empty(req)

            # After From selection Vue re-renders the whole form, temporarily removing input#tocity.
            # Poll until it's visible again (up to 6s) before attempting To fill.
            for _w in range(12):
                _tocity_vis = await page.evaluate("""() => {
                    const el = document.getElementById('tocity');
                    if (!el) return false;
                    const s = window.getComputedStyle(el);
                    return s.display !== 'none' && s.visibility !== 'hidden' && el.offsetWidth > 0;
                }""")
                if _tocity_vis:
                    break
                await asyncio.sleep(0.5)
            else:
                logger.warning("ChinaSouthern: tocity never reappeared after From selection")

            # Fill destination using modern form with autocomplete
            ok = await self._fill_airport_modern(page, "To", req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.8)

            # Dismiss calendar again (destination fill might trigger calendar)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.2)

            ok = await self._fill_date_modern(page, req)
            if not ok:
                return self._empty(req)

            # Dismiss the calendar popup that may be blocking the Search button
            # The calendar shadow overlay intercepts pointer events even when hidden
            try:
                await page.keyboard.press("Escape")  # Close calendar
                await asyncio.sleep(0.3)
                # Also click on the page body to ensure calendar is dismissed
                await page.locator("body").click(position={"x": 10, "y": 10}, force=True)
                await asyncio.sleep(0.3)
            except Exception:
                pass

            # Normalize departure date for the submit call (YYYY-MM-DD).
            if isinstance(req.date_from, datetime):
                dep_iso = req.date_from.strftime("%Y-%m-%d")
            elif isinstance(req.date_from, date):
                dep_iso = req.date_from.isoformat()
            else:
                dep_iso = str(req.date_from)[:10]

            # Pre-search diagnostic: log form state and check for validation errors
            pre_search = await page.evaluate("""() => {
                const from = document.getElementById('fromcity');
                const to = document.getElementById('tocity');
                const dep = document.getElementById('DepartureDate');
                // Find any visible error/validation messages
                const errors = Array.from(document.querySelectorAll(
                    '[class*=error],[class*=invalid],[class*=warn],[class*=tip]'
                )).filter(e => e.offsetHeight > 0).map(e => e.textContent.trim().slice(0, 40));
                // Find Search button(s)
                const btns = Array.from(document.querySelectorAll('a,button')).filter(
                    e => e.textContent.trim() === 'Search' && e.offsetHeight > 0
                ).map(e => ({tag: e.tagName, cls: e.className.slice(0, 50), eventno: e.getAttribute('eventno')}));
                // All a.eventcode elements (to debug which one gets clicked)
                const allEventcode = Array.from(document.querySelectorAll('a.eventcode')).map(
                    e => ({cls: e.className.slice(0, 60), eventno: e.getAttribute('eventno'), txt: e.textContent.trim().slice(0, 20), vis: e.offsetHeight > 0}));
                // Check hidden inputs for actual submitted values
                const hidden = Array.from(document.querySelectorAll('input[type=hidden],input[type=text]')).filter(
                    e => e.name && e.value
                ).map(e => ({name: e.name, val: e.value.slice(0, 20)})).slice(0, 15);
                return {
                    fromVal: from ? from.value : null,
                    toVal: to ? to.value : null,
                    depVal: dep ? dep.value : null,
                    errors: errors.slice(0, 5),
                    searchBtns: btns,
                    allEventcode: allEventcode,
                    hiddenInputs: hidden,
                };
            }""")
            logger.info("ChinaSouthern: pre-search state: %s", pre_search)

            # Click the Search button.
            # JS click on a.eventcode is the most reliable — the Playwright locator often
            # times out because the element is covered/intercepted. JS bypasses that.
            _clicked_ok = False
            try:
                click_result = await page.evaluate("""() => {
                    // Multiple elements share eventno="034" — the nav link AND the real search button.
                    // The nav link comes FIRST in DOM order and navigates without form params.
                    // Strategy: find the last a.eventcode[eventno="034"] (the form search button),
                    // or find the one inside the search form / not in nav.
                    const allBtns = Array.from(document.querySelectorAll('a.eventcode[eventno="034"]'));
                    let btn = null;
                    // Prefer one that is NOT a nav_title (i.e. not the generic nav link)
                    for (const el of allBtns) {
                        if (!el.className.includes('nav_title')) { btn = el; break; }
                    }
                    // If all are nav_title (or none found), try last one
                    if (!btn && allBtns.length > 0) btn = allBtns[allBtns.length - 1];
                    // Fallback: any a.eventcode that's not nav
                    if (!btn) {
                        for (const el of document.querySelectorAll('a.eventcode')) {
                            if (!el.className.includes('nav_title')) { btn = el; break; }
                        }
                    }
                    if (!btn) btn = document.querySelector('a.eventcode, #search-click');
                    if (btn) { btn.click(); return 'js:' + btn.className.slice(0,60); }
                    // Fall back to any visible 'Search' link
                    for (const el of document.querySelectorAll('a, button')) {
                        if (el.textContent.trim() === 'Search' && el.offsetHeight > 0) {
                            el.click(); return 'text:' + el.tagName;
                        }
                    }
                    // Last resort: submit the form directly
                    const form = document.querySelector('form');
                    if (form) { form.submit(); return 'form-submit'; }
                    return 'not-found';
                }""")
                logger.info("ChinaSouthern: Search JS click result: %s", click_result)
                _clicked_ok = click_result != 'not-found'
            except Exception as e:
                logger.warning("ChinaSouthern: Search JS click failed: %s", e)
                try:
                    await page.get_by_role("link", name="Search", exact=True).click(force=True, timeout=5000)
                    _clicked_ok = True
                except Exception:
                    pass

            logger.info("ChinaSouthern: submit triggered, current URL=%s", page.url)
            await asyncio.sleep(1.0)
            logger.info("ChinaSouthern: URL after 1s=%s", page.url)
            await asyncio.sleep(2.0)
            logger.info("ChinaSouthern: URL after 3s=%s", page.url)

            _still_on_homepage = (
                "index.shtml" in page.url
                or "/us/en/" in page.url
                or page.url.rstrip("/") == self.HOMEPAGE.rstrip("/")
            )
            if _still_on_homepage:
                logger.info("ChinaSouthern: still on homepage — trying Vue dispatch + direct nav")
                # Try calling Vue's search method directly
                vue_result = await page.evaluate("""() => {
                    // Walk all DOM nodes looking for a Vue 2 __vue__ instance with a search method
                    for (const sel of ['[id*=book]','[id*=search]','[class*=book]','[class*=search]',
                                        '#app','#main','body']) {
                        const el = document.querySelector(sel);
                        if (!el) continue;
                        // Vue 2: __vue__; Vue 3: __vueParentComponent
                        const vm = el.__vue__ || el.__vueParentComponent?.proxy;
                        if (!vm) continue;
                        for (const m of ['search','handleSearch','onSearch','doSearch','submit','go']) {
                            if (typeof vm[m] === 'function') {
                                vm[m]();
                                return 'vue:' + m;
                            }
                        }
                    }
                    return 'no-vue-method';
                }""")
                logger.info("ChinaSouthern: Vue dispatch result: %s", vue_result)
                await asyncio.sleep(2.0)

                # Final fallback: build URL from hidden inputs and navigate directly
                if "index.shtml" in page.url or page.url.rstrip("/") == self.HOMEPAGE.rstrip("/"):
                    nav_url = await page.evaluate("""() => {
                        const p = new URLSearchParams();
                        for (const i of document.querySelectorAll('input[name]')) {
                            if (i.value && i.value !== i.placeholder && i.name) p.set(i.name, i.value);
                        }
                        const base = 'https://www.csair.com/eu/en/tourpackage/booking/flight.shtml';
                        return base + '?' + p.toString();
                    }""")
                    logger.info("ChinaSouthern: navigating directly to search URL: %s", nav_url[:200])
                    try:
                        await page.goto(nav_url, wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(2.0)
                        logger.info("ChinaSouthern: direct nav URL=%s", page.url[:120])
                    except Exception as nav_err:
                        logger.warning("ChinaSouthern: direct nav failed: %s", nav_err)

            # Dismiss the "Tips / Reminder" dialog that appears on the results page
            try:
                continue_btn = page.get_by_text("Continue", exact=True)
                if await continue_btn.is_visible():
                    await continue_btn.click()
                    logger.info("ChinaSouthern: dismissed Tips dialog")
                    await asyncio.sleep(1.0)
            except Exception:
                pass

            # Attach listener to ALL pages in ALL contexts (not just oversea/b2c)
            for ctx in browser.contexts:
                for p in ctx.pages:
                    if p != page:
                        try:
                            p.on("response", _on_response)
                            logger.info("ChinaSouthern: attached listener to extra page: %s", p.url[:120])
                        except Exception:
                            pass
            for p in context.pages:
                if p != page:
                    try:
                        p.on("response", _on_response)
                    except Exception:
                        pass

            remaining = max(self.timeout - (time.monotonic() - t0), 20)
            deadline = time.monotonic() + remaining
            _dumped_results_page = False
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                # Log URL every ~10s to track navigation
                if int(time.monotonic()) % 10 == 0:
                    logger.info("ChinaSouthern: polling, URL=%s, pages=%d", url[:80],
                                sum(len(ctx.pages) for ctx in browser.contexts) if browser else 1)
                # Once we land on the results page, dump title + body text to diagnose bot challenge
                if not _dumped_results_page and ("oversea.csair.com" in url or "b2c.csair.com" in url):
                    _dumped_results_page = True
                    await asyncio.sleep(2.0)  # let the SPA render
                    try:
                        page_state = await page.evaluate("""() => ({
                            title: document.title,
                            bodyText: document.body ? document.body.innerText.slice(0, 400) : '',
                            scripts: Array.from(document.scripts).map(s => s.src).filter(Boolean).slice(0, 10),
                            metaRobots: (document.querySelector('meta[name=robots]') || {}).content || '',
                        })""")
                        logger.info("ChinaSouthern: results page state: title=%s meta=%s scripts=%d bodyText=%s",
                                    page_state.get('title','')[:80],
                                    page_state.get('metaRobots',''),
                                    len(page_state.get('scripts',[])),
                                    page_state.get('bodyText','')[:200].replace('\n',' '))
                    except Exception as dump_err:
                        logger.info("ChinaSouthern: results page dump failed: %s", dump_err)
                    continue
                await asyncio.sleep(1.0)

            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    pass

            offers = []
            if search_data:
                logger.info("ChinaSouthern: parsing %d keys: %s", len(search_data), list(search_data.keys())[:8])
                offers = self._parse_api_response(search_data, req)
            if not offers:
                # Try to get the correct page (might be new tab)
                scrape_page = page
                for ctx in browser.contexts:
                    for p in ctx.pages:
                        if "shop" in p.url or "book" in p.url or "result" in p.url:
                            scrape_page = p
                            break
                # First try extracting embedded state from the page
                offers = await self._extract_embedded_state(scrape_page, req)
                if not offers:
                    offers = await self._scrape_dom(scrape_page, req)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("ChinaSouthern %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"chinasouthern{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("ChinaSouthern error: %s", e)
            return self._empty(req)
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass
            try:
                context.remove_listener("page", _attach_listener)
            except Exception:
                pass
            try:
                if page: await page.close()
            except Exception:
                pass
            try:
                if context: await context.close()
            except Exception:
                pass
            try:
                if browser: await browser.close()
            except Exception:
                pass
            try:
                if pw: await pw.stop()
            except Exception:
                pass

    async def _fill_airport(self, page, input_sel: str, code_sel: str, iata: str) -> bool:
        """Fill China Southern airport field - direct JS set + hidden code field."""
        import airportsdata
        # Get full city name for display field
        airports = airportsdata.load()
        airport_info = airports.get(iata, {})
        city_name = airport_info.get("city") or iata
        full_name = airport_info.get("name", city_name)

        try:
            # Strategy: Set both visible field (city/airport name) and hidden code field directly.
            # The flightSearch() JS function reads from the hidden code fields, not the visible ones.
            result = await page.evaluate("""(args) => {
                const [inputSel, codeSel, iata, displayText] = args;
                const status = { visible: false, code: false };
                
                // Set the visible input field (for visual confirmation)
                const input = document.querySelector(inputSel);
                if (input) {
                    const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                    nativeSetter.call(input, displayText);
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                    status.visible = true;
                }
                
                // Set the hidden IATA code field (this is what flightSearch() actually uses)
                const codeField = document.querySelector(codeSel);
                if (codeField) {
                    codeField.value = iata;
                    codeField.dispatchEvent(new Event('change', { bubbles: true }));
                    status.code = true;
                }
                
                return status;
            }""", [input_sel, code_sel, iata, f"{city_name} ({iata})"])

            logger.info("ChinaSouthern: airport %s → %s (visible=%s, code=%s)",
                        input_sel, iata, result.get("visible"), result.get("code"))

            if not result.get("code"):
                logger.warning("ChinaSouthern: code field %s not found for %s", code_sel, iata)
                return False

            await asyncio.sleep(0.3)
            return True
        except Exception as e:
            logger.warning("ChinaSouthern: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill China Southern departure date — EU site uses #DepartureDate."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        # EU site date format: YYYY-MM-DD
        iso = dt.strftime("%Y-%m-%d")
        try:
            # EU site uses #DepartureDate (id) with class xinput-date choose-date
            filled = await page.evaluate("""(iso) => {
                // Try #DepartureDate first (EU site), fall back to #fDepDate (legacy)
                const el = document.getElementById('DepartureDate') || document.getElementById('fDepDate');
                if (!el) return false;
                const ns = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                ns.call(el, iso);
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                if (window.jQuery) { jQuery(el).val(iso).trigger('change'); }
                // Also set the hidden dateapp1 field
                const da = document.getElementById('dateapp1');
                if (da) { da.value = iso; da.dispatchEvent(new Event('change', {bubbles: true})); }
                return true;
            }""", iso)
            if not filled:
                logger.warning("ChinaSouthern: date field not found")
                return False
            logger.info("ChinaSouthern: date set %s", iso)
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("ChinaSouthern: date error: %s", e)
            return False

    async def _fill_airport_modern(self, page, field_name: str, iata: str) -> bool:
        """Fill airport field using modern EU site's component-based form.
        
        The EU site uses a Vue city-picker: clicking the input opens #ui-city-unit.
        Typing via press_sequentially triggers Vue's reactivity to filter the picker.
        For Chinese airports (country=CN), we switch to the China tab first.
        """
        import airportsdata
        airports = airportsdata.load()
        airport_info = airports.get(iata, {})
        city_name = airport_info.get("city") or iata
        country = airport_info.get("country", "")

        # airportsdata may not have all Chinese airports (e.g. CAN, PEK, SHA, PVG etc.)
        # Fall back to a known set of Chinese IATA codes
        _CHINA_IATA = {
            "PEK","PKX","SHA","PVG","CAN","SZX","CTU","KMG","WUH","XIY","CKG","HGH","NKG",
            "XMN","TAO","TNA","CGO","CSX","HAK","SYX","TYN","HRB","CGQ","SHE","DLC","NGB",
            "FOC","LHW","URC","KWE","TSN","HET","ZHY","LJG","YNZ","TXN",
        }
        if not country and iata in _CHINA_IATA:
            country = "CN"
        logger.info("ChinaSouthern: airport lookup %s → city=%s country=%s", iata, city_name, country)

        try:
            # 1. Find the field that actually triggers #ui-city-unit.
            # Strategy: find the visible input with aria-controls="ui-city-unit", OR
            # the input inside the From/To widget based on position (first/second visible input
            # inside .city-box or .search-box area). input#fromcity may be a hidden code field.
            field_info = await page.evaluate("""([fieldName]) => {
                const result = { selector: null, allInputs: [] };
                // Scan all inputs for the one with aria-controls="ui-city-unit"
                for (const inp of document.querySelectorAll('input')) {
                    const ac = inp.getAttribute('aria-controls') || '';
                    const s = window.getComputedStyle(inp);
                    const visible = s.display !== 'none' && s.visibility !== 'hidden' && inp.offsetWidth > 0;
                    result.allInputs.push({
                        id: inp.id, name: inp.name, type: inp.type,
                        placeholder: (inp.placeholder || '').slice(0, 30),
                        ariaControls: ac, visible: visible,
                        ariaLabel: (inp.getAttribute('aria-label') || '').slice(0, 30),
                    });
                    if (ac === 'ui-city-unit' && visible) {
                        result.selector = '#' + inp.id || null;
                    }
                }
                return result;
            }""", [field_name])
            logger.info("ChinaSouthern: field scan for '%s': ariaControlsField=%s, totalInputs=%d",
                field_name, field_info.get("selector"), len(field_info.get("allInputs", [])))
            # Log all visible inputs for diagnostics
            for inp in field_info.get("allInputs", []):
                if inp.get("visible"):
                    logger.info("  input id=%s name=%s ph=%s aria-controls=%s ariaLabel=%s",
                        inp.get("id"), inp.get("name"), inp.get("placeholder"),
                        inp.get("ariaControls"), inp.get("ariaLabel"))

            # Prefer the field with aria-controls="ui-city-unit"; fall back to #fromcity/#tocity
            _aria_sel = field_info.get("selector")
            _direct_sel = "input#fromcity" if field_name == "From" else "input#tocity"
            _sel = _aria_sel if _aria_sel else _direct_sel

            field = page.locator(_sel).first
            try:
                await field.wait_for(state="visible", timeout=8000)
                logger.info("ChinaSouthern: found field '%s' via %s", field_name, _sel)
            except Exception:
                # Last resort: the Nth visible textbox
                logger.warning("ChinaSouthern: %s not found, trying textbox index", _sel)
                idx = 0 if field_name == "From" else 1
                all_tb = await page.get_by_role("textbox").all()
                vis = [f for f in all_tb if await f.is_visible()]
                if len(vis) <= idx:
                    logger.warning("ChinaSouthern: field '%s' not found", field_name)
                    return False
                field = vis[idx]

            # Open the picker: use focus event (Vue @focus handler opens the picker),
            # then dispatch input events to trigger Vue filtering.
            # CSS force-open bypasses Vue's pickerOpen state — Vue click handlers may
            # see picker as 'closed' and ignore the click. Better to open via focus.
            await page.evaluate("""([sel]) => {
                const el = document.querySelector(sel);
                if (!el) return;
                el.focus();
                el.dispatchEvent(new FocusEvent('focus', {bubbles: true}));
                el.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true}));
            }""", [_sel])
            await asyncio.sleep(0.3)
            await field.click(force=True, timeout=5000)  # force=True bypasses overlay checks
            await asyncio.sleep(0.5)

            # Clear and type IATA to trigger Vue's autocomplete filter
            await field.press("Control+a")
            await asyncio.sleep(0.1)
            await field.press_sequentially(iata, delay=80)

            # Wait for the city picker to become visible (max 4s, poll every 0.5s).
            for _pw in range(8):
                _picker_visible = await page.evaluate("""() => {
                    const u = document.getElementById('ui-city-unit');
                    if (!u) return false;
                    const s = window.getComputedStyle(u);
                    return s.display !== 'none' && s.visibility !== 'hidden';
                }""")
                if _picker_visible:
                    break
                await asyncio.sleep(0.5)
            else:
                # Picker still hidden — force it open via CSS (Vue will handle the click)
                logger.info("ChinaSouthern: picker still closed after focus+typing, forcing display")
                await page.evaluate("""([sel]) => {
                    const u = document.getElementById('ui-city-unit');
                    const b = document.getElementById('ui-city-box');
                    if (u) { u.style.cssText += ';display:block!important;visibility:visible!important'; }
                    if (b) { b.style.cssText += ';display:block!important;visibility:visible!important'; }
                    // Also trigger Vue's internal click handler if accessible
                    const input = document.querySelector(sel);
                    if (input) {
                        input.dispatchEvent(new MouseEvent('click', {bubbles:true}));
                    }
                }""", [_sel])

            # 3. Check picker state
            picker_info = await page.evaluate("""() => {
                const unit = document.getElementById('ui-city-unit');
                if (!unit) return {exists: false, visible: false, display: 'n/a', items: 0};
                const style = window.getComputedStyle(unit);
                const buttons = unit.querySelectorAll('a[data-code], button[data-code], a[data-iata], button[data-iata]');
                const anyVisible = style.display !== 'none' && style.visibility !== 'hidden' && unit.offsetParent !== null;
                return {
                    exists: true, visible: anyVisible,
                    display: style.display, dataButtons: buttons.length,
                    inputVal: (document.querySelector('input[aria-controls="ui-city-unit"]') || {}).value || null,
                };
            }""")
            logger.info("ChinaSouthern: picker state: %s", picker_info)

            # 4. For Chinese airports, switch to the China tab in #ui-city-unit
            is_china = country == "CN"
            # IATA airport→city code mapping: picker uses IATA city codes, not airport codes.
            # PEK (airport) → BJS (Beijing city code), PVG → SHA (Shanghai city), etc.
            _AIRPORT_TO_CITY = {"PEK": "BJS", "PKX": "BJS", "PVG": "SHA"}
            city_code = _AIRPORT_TO_CITY.get(iata, iata)

            # 5. Select the airport. NOTE: Do NOT click China tab — it hides the
            # data-code elements on the International tab that we need for selection.
            selected = False

            # Strategy A: data attribute — try both airport IATA and city code
            for try_code in ([iata, city_code] if city_code != iata else [iata]):
                for attr in ["data-code", "data-iata", "data-value"]:
                    loc = page.locator(f'#ui-city-unit [{attr}="{try_code}"]').first
                    try:
                        await loc.wait_for(state="attached", timeout=1500)
                        await loc.click(force=True, timeout=5000)
                        logger.info("ChinaSouthern: selected %s via %s=%s", iata, attr, try_code)
                        selected = True
                        await asyncio.sleep(0.3)
                        break
                    except Exception:
                        continue
                if selected:
                    break

            # Strategy B: JS deep search — try all codes + full text scan of all elements
            if not selected:
                js_result = await page.evaluate("""([iata, cityCode, cityName]) => {
                    const unit = document.getElementById('ui-city-unit');
                    if (!unit) return 'no-unit';
                    // Try exact data-attr on both airport code and city code
                    for (const code of [iata, cityCode]) {
                        for (const attr of ['data-code', 'data-iata', 'data-value']) {
                            const el = unit.querySelector('[' + attr + '="' + code + '"]');
                            if (el) { el.click(); return 'attr:' + attr + ':' + code + ':' + el.textContent.trim().slice(0, 30); }
                        }
                    }
                    // Deep scan: any leaf element whose text or attributes contain IATA or city name
                    const needles = [iata.toLowerCase(), cityCode.toLowerCase(),
                                     (cityName || '').toLowerCase()].filter(Boolean);
                    let fuzzy = null;
                    for (const el of unit.querySelectorAll('a, button, li')) {
                        if (el.querySelector('a, button')) continue; // skip containers
                        const txt = el.textContent.trim().toLowerCase();
                        const attrStr = Array.from(el.attributes).map(a => a.value).join(' ').toLowerCase();
                        for (const needle of needles) {
                            if (needle && (txt === needle || attrStr === needle)) {
                                el.click(); return 'deep-exact:' + el.textContent.trim().slice(0, 40);
                            }
                            if (needle && (txt.includes(needle) || attrStr.includes(needle)) && !fuzzy) {
                                fuzzy = el;
                            }
                        }
                    }
                    if (fuzzy) { fuzzy.click(); return 'deep-fuzzy:' + fuzzy.textContent.trim().slice(0, 40); }
                    // Diagnostics: log first 30 data-code values
                    const codes = Array.from(unit.querySelectorAll('[data-code]')).map(e => e.getAttribute('data-code')).slice(0, 30);
                    return 'no-match:codes=' + codes.join(',');
                }""", [iata, city_code, city_name])
                logger.info("ChinaSouthern: JS deep search for %s: %s", iata, js_result)
                if not js_result.startswith("no-match"):
                    selected = True
                    await asyncio.sleep(0.4)

            # Strategy C: Playwright has-text (catches elements JS deep scan might miss)
            if not selected:
                for try_text in filter(None, [iata, city_code if city_code != iata else None, city_name]):
                    loc = page.locator(f'#ui-city-unit a:has-text("{try_text}"), #ui-city-unit button:has-text("{try_text}")').first
                    try:
                        await loc.wait_for(state="attached", timeout=1500)
                        await loc.click(force=True, timeout=5000)
                        logger.info("ChinaSouthern: selected %s via text '%s'", iata, try_text)
                        selected = True
                        await asyncio.sleep(0.3)
                        break
                    except Exception:
                        continue

            if not selected:
                logger.warning("ChinaSouthern: all strategies failed for %s", iata)
                return False

            # 6. Force Vue 2 reactive update.
            # The picker sets .value directly on hidden fields, bypassing Vue's change detection.
            # We ALSO need to sync the VISIBLE fields (depairport/returnairport) from the hidden
            # fields (fromcity/tocity) — Vue validates the visible fields before allowing Search.
            await page.evaluate("""() => {
                const nativeSetter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value').set;
                function sync(el) {
                    if (!el || !el.value) return;
                    nativeSetter.call(el, el.value);
                    el.dispatchEvent(new Event('input',  {bubbles: true}));
                    el.dispatchEvent(new Event('change', {bubbles: true}));
                }
                // Re-fire on the hidden data fields so Vue sees the picker values
                for (const sel of ['#fromcity', '#tocity',
                                    'input[name=city1_code]', 'input[name=city2_code]',
                                    'input[name=fromcity]', 'input[name=tocity]']) {
                    sync(document.querySelector(sel));
                }
                // Sync VISIBLE depairport/returnairport from the hidden fromcity/tocity
                // (Vue validates visible fields — if they still show placeholder, Search is blocked)
                const pairs = [['#fromcity', '[name=depairport]'],
                                ['#tocity',   '[name=returnairport]']];
                for (const [src, dst] of pairs) {
                    const srcEl = document.querySelector(src);
                    const dstEl = document.querySelector(dst);
                    if (srcEl && srcEl.value && dstEl) {
                        nativeSetter.call(dstEl, srcEl.value);
                        dstEl.dispatchEvent(new Event('input',  {bubbles: true}));
                        dstEl.dispatchEvent(new Event('change', {bubbles: true}));
                    }
                }
            }""")

            # 7. Verify selection registered (input value should change from raw IATA)
            try:
                field_val = await page.evaluate("""() => {
                    const from = document.getElementById('fromcity');
                    const to = document.getElementById('tocity');
                    return {from: from ? from.value : null, to: to ? to.value : null};
                }""")
                logger.info("ChinaSouthern: field values after %s selection — from=%s to=%s",
                    iata, field_val.get("from"), field_val.get("to"))
            except Exception:
                pass

            return True

        except Exception as e:
            logger.warning("ChinaSouthern: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date_modern(self, page, req: FlightSearchRequest) -> bool:
        """Fill departure date using modern EU site's date picker.
        
        The EU site date field is readonly — must click through the calendar UI.
        Calendar buttons have accessible names like "Friday, 24 April 2026".
        """
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False

        # Format the target day button's accessible name
        # Example: "Thursday, 24 April 2026" (no leading zero on day)
        # Cross-platform approach: use strftime with %d and strip the leading zero manually
        day_with_zero = dt.strftime("%A, %d %B %Y")  # "Friday, 04 April 2026"
        # Remove leading zero from day: "Friday, 04 April" -> "Friday, 4 April"
        import re
        day_name = re.sub(r', 0(\d) ', r', \1 ', day_with_zero)
        day_num = dt.day
        
        try:
            # 1. Click the Departure textbox to open calendar
            date_field = page.get_by_role("textbox", name="Departure")
            await date_field.click()
            await asyncio.sleep(1.0)  # Wait for calendar animation

            # 2. Navigate to correct month/year using Previous/Next buttons
            # The calendar shows 2 months at a time. Need to navigate if target is not visible.
            target_month_year = dt.strftime("%B %Y")  # "April 2026"
            
            for nav_attempt in range(12):  # Max 12 months forward
                # Check if target month is visible
                month_header = page.locator(f'text="{target_month_year}"')
                if await month_header.count() > 0:
                    logger.info("ChinaSouthern: found target month %s", target_month_year)
                    break
                    
                # Click "Next" / ">" button to advance month
                # The calendar has a "Next page" button or ">" icon
                next_btn = page.locator('button:has-text("Next"), button[aria-label="Next page"], button:has(svg[class*="right"]), .arrowr').first
                try:
                    if await next_btn.is_visible():
                        await next_btn.click()
                        await asyncio.sleep(0.5)
                    else:
                        # Try generic next button
                        await page.locator('[class*="next"], [class*="arrow-right"]').first.click()
                        await asyncio.sleep(0.5)
                except Exception:
                    break

            # 3. Click the target day button
            # The button has accessible name like "Thursday, 24 April 2026"
            day_button = page.get_by_role("button", name=day_name)
            await day_button.click(force=True)
            await asyncio.sleep(0.5)
            logger.info("ChinaSouthern: clicked day button '%s'", day_name)

            # 4. If there's a Confirm button, click it
            try:
                confirm_btn = page.get_by_role("button", name="Confirm")
                if await confirm_btn.is_visible(timeout=1000):
                    await confirm_btn.click()
                    await asyncio.sleep(0.3)
            except Exception:
                pass

            # 5. Close calendar by pressing Escape and clicking outside
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.2)
            await page.locator("body").click(position={"x": 10, "y": 10}, force=True)
            await asyncio.sleep(0.3)

            logger.info("ChinaSouthern: date set to %s via calendar click", dt.strftime("%Y-%m-%d"))
            return True
            
        except Exception as e:
            logger.warning("ChinaSouthern: modern date fill error: %s, trying legacy", e)
            # Fall back to legacy JS method (sets value but may not work)
            return await self._fill_date(page, req)

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []

        # New oversea booking API payload shape: { "ita": { ... } }
        if data.get("_src") == "shop_search" or "ita" in data:
            ita = data.get("ita")
            if isinstance(ita, dict):
                logger.info("ChinaSouthern: parsing ITA payload keys=%s", list(ita.keys())[:12])
                
                # Log key structures to understand the payload
                for key in ["solutionSet", "sliceGrid", "data"]:
                    val = ita.get(key)
                    if isinstance(val, dict):
                        logger.info("ChinaSouthern: ita.%s dict keys=%s", key, list(val.keys())[:10])
                    elif isinstance(val, list):
                        logger.info("ChinaSouthern: ita.%s list len=%d, first=%s", key, len(val), type(val[0]) if val else None)
                    elif isinstance(val, str) and val.startswith("{"):
                        logger.info("ChinaSouthern: ita.%s is JSON string, parsing...", key)
                    else:
                        logger.info("ChinaSouthern: ita.%s type=%s", key, type(val))
                
                # Try solutionSet first - oversea.csair uses this for flight solutions
                solution_set = ita.get("solutionSet")
                
                # solutionSet may be a JSON string - parse it
                if isinstance(solution_set, str):
                    try:
                        solution_set = json.loads(solution_set)
                        logger.info("ChinaSouthern: parsed solutionSet JSON string, keys=%s", list(solution_set.keys()) if isinstance(solution_set, dict) else "N/A")
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("ChinaSouthern: solutionSet is not valid JSON")
                        solution_set = None
                
                if isinstance(solution_set, dict):
                    # solutionSet may have solutions list or solutions dict
                    solutions = solution_set.get("solutions") or solution_set.get("solution") or []
                    if isinstance(solutions, dict):
                        solutions = list(solutions.values())
                    if isinstance(solutions, list) and solutions:
                        logger.info("ChinaSouthern: found %d solutions in solutionSet", len(solutions))
                        for sol in solutions:
                            offer = self._build_offer_from_solution(sol, ita, req)
                            if offer:
                                offers.append(offer)
                        if offers:
                            return offers
                
                # Try sliceGrid - contains flight legs in row/column structure
                slice_grid = ita.get("sliceGrid")
                if isinstance(slice_grid, dict):
                    # sliceGrid is { "column": [...], "row": [...] }
                    columns = slice_grid.get("column") or {}
                    rows = slice_grid.get("row") or []
                    col_count = len(columns) if isinstance(columns, (dict, list)) else 0
                    row_count = len(rows) if isinstance(rows, list) else 0
                    logger.info("ChinaSouthern: sliceGrid cols=%d rows=%d", col_count, row_count)
                    
                    # rows contain the flight options
                    if rows:
                        logger.info("ChinaSouthern: first row keys=%s", list(rows[0].keys())[:15] if isinstance(rows[0], dict) else type(rows[0]))
                        
                        # Get reference data from ita.data
                        ref_data = ita.get("data") or {}
                        airlines_ref = ref_data.get("airline") or {}
                        airports_ref = ref_data.get("airport") or {}
                        
                        # Each row is a flight option 
                        for row in rows:
                            if not isinstance(row, dict):
                                continue
                            offer = self._build_offer_from_row(row, columns, ref_data, req)
                            if offer:
                                offers.append(offer)
                        
                        if offers:
                            logger.info("ChinaSouthern: parsed %d offers from sliceGrid", len(offers))
                            return offers
                
                elif isinstance(slice_grid, list) and slice_grid:
                    logger.info("ChinaSouthern: found %d items in sliceGrid list", len(slice_grid))
                    for idx, grid_item in enumerate(slice_grid[:5]):  # Log first 5
                        if isinstance(grid_item, dict):
                            logger.info("ChinaSouthern: sliceGrid[%d] keys=%s", idx, list(grid_item.keys())[:10])
                
                # Fallback to generic _find_flights
                flights = self._find_flights(ita)
                for flight in flights:
                    offer = self._build_offer(flight, req)
                    if offer:
                        offers.append(offer)
                if offers:
                    return offers

        # China Southern queryInterFlight structure: data.data.dateFlights[]
        inner = data.get("data")
        if isinstance(inner, dict):
            inner2 = inner.get("data")
            if isinstance(inner2, dict):
                date_flights = inner2.get("dateFlights")
                if isinstance(date_flights, list) and date_flights:
                    logger.info("ChinaSouthern: found %d dateFlights in queryInterFlight", len(date_flights))
                    return self._parse_query_inter_flight(date_flights, data, req)
                else:
                    logger.info("ChinaSouthern: inner2 keys: %s", list(inner2.keys())[:10])
            else:
                logger.info("ChinaSouthern: inner keys: %s, type(inner.data)=%s", list(inner.keys())[:10], type(inner.get("data")))

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

    def _parse_query_inter_flight(self, date_flights: list, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse China Southern queryInterFlight API response."""
        offers = []
        # Get currency from first price entry or default
        currency = "CNY"

        for flight in date_flights:
            segments_raw = flight.get("segments") or []
            if not segments_raw:
                continue

            # Extract price from lowEconomyPrices (cheapest economy), fall back to prices
            price = 0.0
            price_currency = currency
            for price_key in ("lowEconomyPrices", "prices", "lowBfPrices"):
                price_list = flight.get(price_key)
                if isinstance(price_list, list) and price_list:
                    entry = price_list[0]
                    # displayPrice = fare + tax (total); adultSalePrice also works
                    p = entry.get("adultSalePrice") or entry.get("displayPrice") or entry.get("salePrice") or 0
                    price_currency = entry.get("saleCurrency") or entry.get("displayCurrency") or currency
                    if p and float(p) > 0:
                        price = float(p)
                        break

            if price <= 0:
                continue

            # Parse segments
            segments = []
            for seg in segments_raw:
                dep_date = seg.get("depDate") or str(req.date_from)
                dep_time = seg.get("depTime") or "00:00"
                arr_date = seg.get("arrDate") or dep_date
                arr_time = seg.get("arrTime") or "00:00"

                dep_dt = self._parse_dt(f"{dep_date}T{dep_time}" if "T" not in str(dep_time) else dep_time, req.date_from)
                arr_dt = self._parse_dt(f"{arr_date}T{arr_time}" if "T" not in str(arr_time) else arr_time, req.date_from)

                carrier = seg.get("carrier") or self.IATA
                fno = seg.get("flightNo") or ""
                full_fno = f"{carrier}{fno}" if fno and not fno.startswith(carrier) else (fno or f"{self.IATA}???")

                segments.append(FlightSegment(
                    airline=carrier[:2],
                    airline_name=seg.get("airlineName") or self.AIRLINE_NAME,
                    flight_no=full_fno,
                    origin=seg.get("depPort") or req.origin,
                    destination=seg.get("arrPort") or req.destination,
                    departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                ))

            if not segments:
                continue

            # Duration from flyTime string like "4h5m"
            total_dur = 0
            fly_time = flight.get("flyTime") or ""
            m_h = re.search(r"(\d+)h", fly_time)
            m_m = re.search(r"(\d+)m", fly_time)
            if m_h:
                total_dur += int(m_h.group(1)) * 3600
            if m_m:
                total_dur += int(m_m.group(1)) * 60

            stopovers = flight.get("stopNumber") or flight.get("zzCount") or max(0, len(segments) - 1)
            route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=stopovers)
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{segments[0].origin}_{segments[-1].destination}_{segments[0].departure}_{price}_{segments[0].flight_no}".encode()
            ).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=price_currency,
                price_formatted=f"{price_currency} {price:,.0f}", outbound=route, inbound=None,
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

    def _build_offer_from_solution(self, sol: dict, ita: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build FlightOffer from oversea.csair.com solutionSet solution structure."""
        try:
            # Log solution structure for debugging
            logger.info("ChinaSouthern: solution keys=%s", list(sol.keys())[:15])
            
            # Extract price from solution
            price = 0.0
            currency = "EUR"
            
            # Try various price keys
            for price_key in ["totalPrice", "price", "adultPrice", "farePrice", "displayPrice"]:
                p = sol.get(price_key)
                if isinstance(p, dict):
                    price = float(p.get("amount") or p.get("value") or 0)
                    currency = p.get("currency") or p.get("currencyCode") or currency
                elif p:
                    try:
                        price = float(p)
                    except (ValueError, TypeError):
                        pass
                if price > 0:
                    break
            
            if price <= 0:
                # Try to get price from fareInfo or passengerFare
                fare_info = sol.get("fareInfo") or sol.get("passengerFare") or {}
                if isinstance(fare_info, dict):
                    price = float(fare_info.get("totalFare") or fare_info.get("total") or 0)
                    currency = fare_info.get("currency") or currency
            
            if price <= 0:
                logger.warning("ChinaSouthern: no price in solution")
                return None
            
            # Extract segments from slices or segments
            segments = []
            slices = sol.get("slices") or sol.get("slice") or sol.get("segments") or sol.get("legs") or []
            if not isinstance(slices, list):
                slices = [slices] if slices else []
            
            for sl in slices:
                if not isinstance(sl, dict):
                    continue
                # Slice may contain segments
                segs = sl.get("segments") or sl.get("segment") or sl.get("flights") or [sl]
                if not isinstance(segs, list):
                    segs = [segs]
                
                for seg in segs:
                    if not isinstance(seg, dict):
                        continue
                    
                    # Extract segment details
                    dep_time = seg.get("departureTime") or seg.get("departure") or seg.get("depTime") or ""
                    arr_time = seg.get("arrivalTime") or seg.get("arrival") or seg.get("arrTime") or ""
                    
                    origin = seg.get("origin") or seg.get("dep") or seg.get("departureAirport") or req.origin
                    dest = seg.get("destination") or seg.get("arr") or seg.get("arrivalAirport") or req.destination
                    
                    carrier = seg.get("carrier") or seg.get("airline") or seg.get("operatingCarrier") or self.IATA
                    flight_no = seg.get("flightNumber") or seg.get("flightNo") or seg.get("number") or ""
                    if flight_no and not str(flight_no).startswith(carrier):
                        flight_no = f"{carrier}{flight_no}"
                    
                    dep_dt = self._parse_dt(dep_time, req.date_from)
                    arr_dt = self._parse_dt(arr_time, req.date_from)
                    
                    segments.append(FlightSegment(
                        airline=carrier[:2] if carrier else self.IATA,
                        airline_name=self.AIRLINE_NAME,
                        flight_no=flight_no or f"{self.IATA}???",
                        origin=origin[:3] if origin else req.origin,
                        destination=dest[:3] if dest else req.destination,
                        departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                    ))
            
            if not segments:
                logger.warning("ChinaSouthern: no segments in solution")
                return None
            
            # Build route and offer
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds()) if len(segments) > 1 else 7200
            stopovers = max(0, len(segments) - 1)
            route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=stopovers)
            
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{segments[0].origin}_{segments[-1].destination}_{segments[0].departure}_{price}".encode()
            ).hexdigest()[:12]
            
            return FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
                price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
                airlines=[self.IATA], owner_airline=self.IATA,
                booking_url=self._booking_url(req), is_locked=False,
                source=self.SOURCE, source_tier="free",
            )
        except Exception as e:
            logger.warning("ChinaSouthern: error building offer from solution: %s", e)
            return None

    def _build_offer_from_row(self, row: dict, columns: list, ref_data: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build FlightOffer from sliceGrid row structure (oversea.csair.com API).
        
        Row structure (from ITA payload / Vue store):
        - slice: {origin, destination, departure: [date,time,tz], arrival: [date,time,tz], duration (min), stop, segment: [...]}
        - cell: dict keyed by fare class (CCC, YYY, WWW, FFF) → {saleTotal: {amount, currency}, ...}
        - minPrc: cheapest price across all fare classes (may be absent in raw API)
        - currency: currency code (may be absent in raw API)
        """
        try:
            slice_data = row.get("slice") or {}
            cell_data = row.get("cell") or {}
            
            if not isinstance(slice_data, dict):
                return None
            
            # Extract flight info from slice
            origin = slice_data.get("origin") or req.origin
            destination = slice_data.get("destination") or req.destination
            duration_min = slice_data.get("duration") or 0
            stops = slice_data.get("stop") or slice_data.get("transfer") or 0
            
            # Departure/arrival can be arrays [date, time, timezone] or strings
            dep_raw = slice_data.get("departure") or ""
            arr_raw = slice_data.get("arrival") or ""
            
            dep_dt = self._parse_dt_array(dep_raw, req.date_from)
            arr_dt = self._parse_dt_array(arr_raw, req.date_from)
            
            # --- Pricing ---
            # 1) Try row-level minPrc (set by Vue store after processing)
            price = 0.0
            currency = row.get("currency") or "EUR"
            
            min_prc = row.get("minPrc")
            if isinstance(min_prc, (int, float)) and min_prc > 0:
                price = float(min_prc)
            
            # 2) Fallback: scan cell dict for cheapest saleTotal
            if price <= 0 and isinstance(cell_data, dict):
                for fare_class, cell_val in cell_data.items():
                    if not isinstance(cell_val, dict):
                        continue
                    st = cell_val.get("saleTotal") or cell_val.get("saleFareTotal") or {}
                    if isinstance(st, dict):
                        amt = st.get("amount") or st.get("value") or 0
                        cur = st.get("currency") or currency
                        try:
                            amt = float(amt)
                        except (ValueError, TypeError):
                            amt = 0
                        if amt > 0 and (price <= 0 or amt < price):
                            price = amt
                            currency = cur
                    # Direct price field
                    elif price <= 0:
                        for pk in ("price", "lowestPrice", "totalPrice", "adultPrice"):
                            pv = cell_val.get(pk)
                            if isinstance(pv, (int, float)) and pv > 0:
                                price = float(pv)
                                break
                            elif isinstance(pv, dict):
                                amt = float(pv.get("amount") or pv.get("value") or 0)
                                if amt > 0:
                                    price = amt
                                    currency = pv.get("currency") or currency
                                    break
            
            if price <= 0:
                return None
            
            # --- Segments ---
            segments_data = slice_data.get("segment") or slice_data.get("cardSegment") or []
            if not isinstance(segments_data, list):
                segments_data = [segments_data] if segments_data else []
            
            carrier = self.IATA
            segments = []
            for seg in segments_data:
                if not isinstance(seg, dict):
                    continue
                seg_carrier = seg.get("marketCarrier") or seg.get("operationCarrier") or seg.get("carrier") or self.IATA
                seg_flight = seg.get("marketFlight") or seg.get("operationFlight") or seg.get("flightNo") or ""
                seg_fno = f"{seg_carrier}{seg_flight}" if seg_flight else ""
                seg_origin = seg.get("origin") or origin
                seg_dest = seg.get("destination") or destination
                seg_dep = seg.get("departure")
                seg_arr = seg.get("arrival")
                
                seg_dep_dt = self._parse_dt_array(seg_dep, req.date_from) if seg_dep else dep_dt
                seg_arr_dt = self._parse_dt_array(seg_arr, req.date_from) if seg_arr else arr_dt
                
                if seg_carrier != self.IATA:
                    carrier = seg_carrier  # Use the actual operating/marketing carrier
                
                segments.append(FlightSegment(
                    airline=seg_carrier,
                    flight_no=seg_fno,
                    origin=seg_origin,
                    destination=seg_dest,
                    departure=seg_dep_dt,
                    arrival=seg_arr_dt,
                ))
            
            if not segments:
                segments = [FlightSegment(
                    airline=self.IATA, flight_no=f"{self.IATA}???",
                    origin=origin, destination=destination,
                    departure=dep_dt, arrival=arr_dt,
                )]
            
            # Use last segment carrier as the carrier for the offer
            carrier = segments[0].airline or self.IATA
            
            # Duration: slice.duration is in minutes
            dur_seconds = int(duration_min) * 60 if isinstance(duration_min, (int, float)) and duration_min > 0 else 0
            if dur_seconds <= 0:
                dur_seconds = max(int((segments[-1].arrival - segments[0].departure).total_seconds()), 3600)
            
            route = FlightRoute(
                segments=segments,
                total_duration_seconds=dur_seconds,
                stopovers=int(stops) if isinstance(stops, (int, float)) else max(0, len(segments) - 1)
            )
            
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{origin}_{destination}_{dep_dt}_{price}".encode()
            ).hexdigest()[:12]
            
            return FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
                price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
                airlines=[carrier], owner_airline=carrier,
                booking_url=self._booking_url(req), is_locked=False,
                source=self.SOURCE, source_tier="free",
            )
        except Exception as e:
            logger.warning("ChinaSouthern: error building offer from row: %s", e)
            import traceback
            traceback.print_exc()
            return None

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
            logger.debug("ChinaSouthern: offer parse error: %s", e)
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
    def _parse_dt_array(val, fallback_date) -> datetime:
        """Parse datetime from array [date, time, tz] or string format."""
        if isinstance(val, list) and len(val) >= 2:
            # Format: ["2026-04-24", "14:45", "+08:00"]
            date_str = val[0]
            time_str = val[1]
            try:
                return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                pass
        if isinstance(val, str):
            return ChinaSouthernConnectorClient._parse_dt(val, fallback_date)
        # Fallback
        try:
            dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
            return datetime(dt.year, dt.month, dt.day) if isinstance(dt, date) and not isinstance(dt, datetime) else dt
        except Exception:
            return datetime.now()

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

    async def _extract_embedded_state(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from embedded page state (window globals, script tags, etc.)."""
        try:
            url = page.url
            logger.info("ChinaSouthern: extracting embedded state from %s", url[:80])

            # Try to extract data from window globals
            state_data = await page.evaluate("""() => {
                const results = {};

                // Common state variable names
                const stateVars = [
                    '__INITIAL_STATE__', '__PRELOADED_STATE__', '__NUXT__', '__NEXT_DATA__',
                    'window.__data', 'pageData', 'flightData', 'searchResult', 'shopData',
                    '__APOLLO_STATE__', '_reactRoot'
                ];
                for (const v of stateVars) {
                    try {
                        const val = eval(v);
                        if (val && typeof val === 'object') {
                            results[v] = val;
                        }
                    } catch (e) {}
                }

                // Look for JSON in script tags
                const scripts = document.querySelectorAll('script:not([src])');
                for (const s of scripts) {
                    const text = s.textContent || '';
                    // Look for flight/shop data patterns
                    if (text.includes('flightList') || text.includes('dateFlights') ||
                        text.includes('routeList') || text.includes('shopData') ||
                        text.includes('"price"') && text.includes('"flight"')) {
                        // Try to extract JSON object
                        const jsonMatch = text.match(/\\{[^{}]*"(?:flight|price|route|data)"[^{}]*\\}/);
                        if (jsonMatch) {
                            try {
                                results['_scriptData'] = JSON.parse(jsonMatch[0]);
                            } catch (e) {}
                        }
                        // Also try to find assignments
                        const assignMatch = text.match(/(?:var|let|const)\\s+\\w+\\s*=\\s*(\\{[\\s\\S]*?\\});/);
                        if (assignMatch) {
                            try {
                                results['_scriptAssign'] = JSON.parse(assignMatch[1]);
                            } catch (e) {}
                        }
                    }
                }

                // Try extracting from data attributes
                const dataEls = document.querySelectorAll('[data-flights], [data-shop], [data-result]');
                for (const el of dataEls) {
                    for (const attr of el.attributes) {
                        if (attr.name.startsWith('data-') && attr.value.length > 50) {
                            try {
                                results[attr.name] = JSON.parse(attr.value);
                            } catch (e) {}
                        }
                    }
                }

                return results;
            }""")

            if state_data:
                logger.info("ChinaSouthern: found embedded state keys: %s", list(state_data.keys())[:5])
                for key, data in state_data.items():
                    if isinstance(data, dict):
                        offers = self._parse_api_response({"_src": f"embedded_{key}", **data}, req)
                        if offers:
                            logger.info("ChinaSouthern: extracted %d offers from %s", len(offers), key)
                            return offers

            return []
        except Exception as e:
            logger.warning("ChinaSouthern: embedded state extraction failed: %s", e)
            return []

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        # Wait for page to settle and content to load
        await asyncio.sleep(3.0)

        # Wait for loading indicators to disappear (up to 15s)
        for _ in range(15):
            loading = await page.evaluate("""() => {
                const loaders = document.querySelectorAll(
                    '[class*="loading"], [class*="spinner"], [class*="skeleton"], ' +
                    '.ant-spin, .el-loading, [class*="mask"], [aria-busy="true"]'
                );
                return Array.from(loaders).some(el => el.offsetHeight > 0 && el.offsetWidth > 0);
            }""")
            if not loading:
                break
            await asyncio.sleep(1.0)

        url = page.url
        logger.info("ChinaSouthern: DOM scrape on %s", url[:80])

        # Diagnostic: get page structure info
        page_info = await page.evaluate("""() => {
            const title = document.title;
            const bodyText = document.body?.innerText?.slice(0, 500) || '';
            const visibleDivs = document.querySelectorAll('div').length;
            const visibleLis = document.querySelectorAll('li').length;
            // Check for common error indicators
            const hasError = /no.*flight|unavailable|error|sorry|try.*again/i.test(bodyText);
            // Count potential flight-like content
            const flightMatches = (bodyText.match(/CZ\\d{2,4}|(\\d{1,2}:\\d{2})/g) || []).length;
            return { title, divCount: visibleDivs, liCount: visibleLis, hasError, flightMatches,
                     textSample: bodyText.replace(/\\s+/g, ' ').slice(0, 200) };
        }""")
        logger.info("ChinaSouthern: page info: title=%s, divs=%d, lis=%d, hasError=%s, flightMatches=%d",
                    page_info.get("title", "")[:50], page_info.get("divCount", 0),
                    page_info.get("liCount", 0), page_info.get("hasError"),
                    page_info.get("flightMatches", 0))
        if page_info.get("textSample"):
            logger.info("ChinaSouthern: page text sample: %s", page_info.get("textSample", "")[:150])

        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];

            // Extended selectors for oversea.csair.com shop page
            const cardSelectors = [
                '[class*="flight-card"]', '[class*="flight-row"]', '[class*="itinerary"]',
                '[class*="result-card"]', '[class*="bound"]', '[class*="flight-item"]',
                '[class*="flightInfo"]', '[class*="flight_item"]', '[class*="shop-item"]',
                '[class*="trip-item"]', '[class*="route-item"]', '[class*="fare-row"]',
                '[data-flight]', '[data-itinerary]', 'li[class*="list"]',
                '.flight-result', '.search-result', '.booking-item'
            ];

            let cards = [];
            for (const sel of cardSelectors) {
                try {
                    const found = document.querySelectorAll(sel);
                    if (found.length > cards.length) cards = Array.from(found);
                } catch (e) {}
            }

            // Log diagnostic
            console.log('ChinaSouthern DOM: found', cards.length, 'cards');

            for (const card of cards) {
                const text = card.innerText || '';
                if (text.length < 20) continue;

                // Extract times with multiple patterns
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;

                // Extended price patterns
                let priceMatch = text.match(/(CNY|RMB|USD|EUR|GBP|¥|\$|€)\s*[\d,]+\.?\d*/i) ||
                                 text.match(/[\d,]+\.?\d*\s*(CNY|RMB|USD|EUR|GBP|¥|\$|€)/i) ||
                                 text.match(/从?\s*([\d,]+)\s*(起|元|CNY|RMB)/);
                if (!priceMatch) continue;

                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0 || price > 100000) continue;

                let currency = 'CNY';
                if (/USD|\$/.test(priceMatch[0])) currency = 'USD';
                else if (/EUR|€/.test(priceMatch[0])) currency = 'EUR';
                else if (/GBP|£/.test(priceMatch[0])) currency = 'GBP';

                const fnMatch = text.match(/\b(CZ\s*\d{2,4})\b/i) || text.match(/\b([A-Z]{2}\s*\d{2,4})\b/);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'CZ',
                });
            }

            // If no cards found, try extracting from table rows
            if (results.length === 0) {
                const rows = document.querySelectorAll('tr, [role="row"]');
                for (const row of rows) {
                    const text = row.innerText || '';
                    if (text.length < 30) continue;
                    const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                    if (times.length < 2) continue;
                    let priceMatch = text.match(/([\d,]+)\s*(CNY|RMB|USD|EUR|¥|\$|€)/i) ||
                                     text.match(/(CNY|RMB|USD|EUR|¥|\$|€)\s*([\d,]+)/i);
                    if (!priceMatch) continue;
                    const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                    const price = parseFloat(priceStr);
                    if (!price || price <= 0 || price > 100000) continue;
                    let currency = 'CNY';
                    if (/USD|\$/.test(priceMatch[0])) currency = 'USD';
                    else if (/EUR|€/.test(priceMatch[0])) currency = 'EUR';
                    const fnMatch = text.match(/\b(CZ\s*\d{2,4})\b/i) || text.match(/\b([A-Z]{2}\s*\d{2,4})\b/);
                    results.push({
                        depTime: times[0], arrTime: times[1], price, currency,
                        flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'CZ',
                    });
                }
            }

            return results;
        }""", [req.origin, req.destination])

        logger.info("ChinaSouthern: DOM scrape found %d flights", len(flights or []))

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
        return f"https://www.csair.com/en?from={req.origin}&to={req.destination}&date={date_str}"

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"cz_rt_{o.id}_{i.id}",
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
        search_hash = hashlib.md5(f"chinasouthern{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
