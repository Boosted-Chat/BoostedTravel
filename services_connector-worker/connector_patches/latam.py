"""
LATAM Airlines patchright connector -- Cloud Run patch.

Replaces CDP Chrome with Patchright headed browser to bypass reCAPTCHA Enterprise.
URL-based navigation + BFF API interception logic preserved from SDK.
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
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

from ..models.flights import (
    AirlineSummary,
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .airline_routes import get_city_airports

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 1
_RESULTS_WAIT = 15


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
            logger.info("LATAM: using proxy relay on port 8899")
        except OSError:
            from urllib.parse import urlparse
            p = urlparse(letsfg_proxy)
            proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}", "bypass": _BYPASS}
            if p.username:
                proxy["username"] = p.username
                proxy["password"] = p.password or ""
            logger.info("LATAM: using direct proxy %s:%s", p.hostname, p.port)
    else:
        logger.info("LATAM: no proxy, direct connection")

    # Use system Chrome binary instead of patchright's bundled Chromium.
    # Real Chrome has a different (more trusted) browser fingerprint.
    try:
        chrome_path = find_chrome()
        logger.info("LATAM: using system Chrome at %s", chrome_path)
    except RuntimeError:
        chrome_path = None
        logger.info("LATAM: system Chrome not found, using bundled Chromium")

    pw = await async_playwright().start()
    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--no-default-browser-check",
        "--window-size=1366,768",
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
        viewport={"width": 1366, "height": 768},
        locale="en-US",
        timezone_id="America/Santiago",
        color_scheme="light",
    )
    page = await context.new_page()

    # Apply stealth patches BEFORE navigation
    # NOTE: auto_block_if_proxied disabled — was preventing SPA JS from loading
    await inject_stealth_js(page)

    return pw, browser, context, page


class LatamConnectorClient:
    """LATAM Airlines -- Patchright + URL navigation + BFF/offers API intercept."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob = await self._search_ow(req)
        if req.return_from and ob.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
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
                if result is not None:
                    return result
            except Exception as e:
                logger.warning("LATAM: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)
        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[FlightSearchResponse]:
        pw = browser = context = page = None
        try:
            pw, browser, context, page = await _launch_browser()

            # Build LATAM search URL (no form fill needed)
            date_str = req.date_from.strftime("%Y-%m-%d")
            cabin = {"M": "Economy", "W": "Premium+Economy", "C": "Business", "F": "First"}.get(req.cabin_class or "M", "Economy")
            url = (
                f"https://www.latamairlines.com/us/en/flight-offers"
                f"?outbound={date_str}T12%3A00%3A00.000Z"
                f"&origin={req.origin}&destination={req.destination}"
                f"&adt={req.adults}&chd={req.children or 0}&inf={req.infants or 0}"
                f"&cabin={cabin}&trip=OW"
            )

            api_responses: list[str] = []
            api_event = asyncio.Event()

            # Diagnostic: track failed requests and console errors
            _failed_reqs: list[str] = []
            _console_errors: list[str] = []

            def _on_requestfailed(req):
                _failed_reqs.append(f"{req.resource_type}:{req.url[:100]}→{req.failure}")

            def _on_console(msg):
                if msg.type == "error":
                    _console_errors.append(msg.text[:200])

            page.on("requestfailed", _on_requestfailed)
            page.on("console", _on_console)

            # Track ALL XHR/fetch responses for debugging
            _all_xhr_urls: list[str] = []
            # Capture the page's own BFF request details for replay
            _captured_bff_req: dict = {}

            async def _on_response(response):
                url_str = response.url
                # Log all XHR/fetch for diagnostics
                if response.request.resource_type in ("xhr", "fetch"):
                    _all_xhr_urls.append(f"{response.status}:{url_str[:120]}")
                if any(kw in url_str for kw in ("bff/air-offers", "offers/search", "air-booking/offers", "booking-api")):
                    try:
                        req_headers = response.request.headers
                        # Capture full request for replay (first BFF request only)
                        if not _captured_bff_req:
                            _captured_bff_req['headers'] = dict(req_headers)
                            _captured_bff_req['url'] = url_str
                            _captured_bff_req['method'] = response.request.method
                            try:
                                _captured_bff_req['post_data'] = response.request.post_data
                            except Exception:
                                _captured_bff_req['post_data'] = None
                            logger.info("LATAM: captured BFF request: method=%s url=%s headers=%s",
                                        _captured_bff_req['method'], url_str[:120],
                                        list(req_headers.keys()))
                        body = await response.text()
                        logger.info("LATAM: BFF response: url=%s status=%d len=%d preview=%s",
                                    url_str[:100], response.status, len(body), body[:300])
                        if response.status == 200 and len(body) > 500:
                            api_responses.append(body)
                            api_event.set()
                    except Exception as e:
                        logger.info("LATAM: BFF response read failed: %s", e)

            page.on("response", _on_response)

            # Warm up Akamai sensor by visiting homepage first
            logger.info("LATAM: warming up — visiting homepage to init Akamai sensor")
            try:
                await page.goto("https://www.latamairlines.com/us/en", wait_until="domcontentloaded", timeout=25000)
                await asyncio.sleep(2)
                await page.mouse.move(400, 300)
                await asyncio.sleep(0.5)
                await page.mouse.move(600, 400)
                await asyncio.sleep(1)
                logger.info("LATAM: homepage loaded, title=%s", (await page.title())[:50])
            except Exception as e:
                logger.info("LATAM: homepage warmup partial: %s", e)

            # ── Try BFF GET from homepage context (clean Akamai session) ──
            # Before navigating to search page, the Akamai sensor should have
            # validated this session. Try the BFF GET directly from here.
            import uuid
            date_str_bff = req.date_from.strftime("%Y-%m-%d")
            cabin_bff = {"M": "Economy", "W": "Premium+Economy", "C": "Business", "F": "First"}.get(req.cabin_class or "M", "Economy")
            bff_qs = (
                f"cabinType={cabin_bff}&redemption=false&sort=RECOMMENDED"
                f"&origin={req.origin}&destination={req.destination}"
                f"&outbound={date_str_bff}T12%3A00%3A00.000Z"
                f"&adt={req.adults}&chd={req.children or 0}&inf={req.infants or 0}"
            )
            bff_search_url = f"/bff/air-offers/v2/offers/search?{bff_qs}"
            bff_headers = {
                'accept': 'application/json',
                'x-latam-action-name': 'search-result.flightselection.offers-search',
                'x-latam-app-session-id': str(uuid.uuid4()),
                'x-latam-application-country': 'US',
                'x-latam-application-lang': 'en',
                'x-latam-application-name': 'web-air-offers',
                'x-latam-application-oc': 'us',
                'x-latam-client-name': 'web-air-offers',
                'x-latam-request-id': str(uuid.uuid4()),
                'x-latam-track-id': str(uuid.uuid4()),
                'x-latam-device-width': '1366',
            }
            logger.info("LATAM: trying BFF GET from homepage context: %s", bff_search_url[:120])
            try:
                hp_bff = await page.evaluate("""(args) => {
                    return fetch(args.url, {method: 'GET', headers: args.headers})
                        .then(r => r.text().then(t => ({status: r.status, len: t.length, body: t.slice(0, 2000)})))
                        .catch(e => ({error: e.message}));
                }""", {"url": bff_search_url, "headers": bff_headers})
                logger.info("LATAM: homepage BFF GET: status=%s len=%s body=%s",
                            hp_bff.get("status"), hp_bff.get("len"), str(hp_bff.get("body", ""))[:500])
                if hp_bff.get("status") == 200 and hp_bff.get("len", 0) > 500:
                    full = await page.evaluate("""(args) => {
                        return fetch(args.url, {method: 'GET', headers: args.headers}).then(r => r.text());
                    }""", {"url": bff_search_url, "headers": bff_headers})
                    api_responses.append(full)
                    logger.info("LATAM: homepage BFF succeeded! %d bytes", len(full))
            except Exception as hp_err:
                logger.info("LATAM: homepage BFF error: %s", hp_err)

            # Also try server-side request with browser cookies
            if not api_responses:
                try:
                    cookies = await context.cookies()
                    cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
                    ua = await page.evaluate("() => navigator.userAgent")
                    logger.info("LATAM: trying server-side BFF with %d cookies", len(cookies))
                    import aiohttp
                    ss_headers = {
                        **bff_headers,
                        'cookie': cookie_str,
                        'user-agent': ua,
                        'referer': 'https://www.latamairlines.com/us/en',
                    }
                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(
                            f"https://www.latamairlines.com{bff_search_url}",
                            headers=ss_headers,
                            timeout=aiohttp.ClientTimeout(total=15),
                            proxy=None,  # direct, no proxy
                        ) as resp:
                            body = await resp.text()
                            logger.info("LATAM: server-side BFF: status=%d len=%d body=%s",
                                        resp.status, len(body), body[:500])
                            if resp.status == 200 and len(body) > 500:
                                api_responses.append(body)
                                logger.info("LATAM: server-side BFF succeeded!")
                except Exception as ss_err:
                    logger.info("LATAM: server-side BFF error: %s", ss_err)

            if api_responses:
                logger.info("LATAM: got BFF data from homepage, skipping search page navigation")

            if not api_responses:
                logger.info("LATAM: homepage BFF failed, navigating to search page")
                logger.info("LATAM: navigating to search URL %s->%s on %s", req.origin, req.destination, req.date_from)
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Give Akamai sensor time on search page too
            await page.mouse.move(500, 350)
            await asyncio.sleep(1)
            await page.evaluate("window.scrollBy(0, 150)")

            # Diagnostic: log page state after initial load
            try:
                page_info = await page.evaluate("""() => {
                    return {
                        url: location.href,
                        title: document.title,
                        bodyLen: (document.body?.innerText || '').length,
                        snippet: (document.body?.innerText || '').slice(0, 300).replace(/\\s+/g, ' '),
                        hasRecaptcha: !!document.querySelector('iframe[src*="recaptcha"], [class*="recaptcha"], #captcha'),
                        hasError: !!(document.querySelector('[class*="error"]') || document.title.toLowerCase().includes('error')),
                        iframeCount: document.querySelectorAll('iframe').length,
                    };
                }""")
                logger.info("LATAM: page loaded — url=%s title=%s bodyLen=%d iframes=%d recaptcha=%s",
                            page_info.get("url", "?")[:80], page_info.get("title", "?")[:50],
                            page_info.get("bodyLen", 0), page_info.get("iframeCount", 0),
                            page_info.get("hasRecaptcha", False))
                if page_info.get("bodyLen", 0) < 500:
                    logger.info("LATAM: page snippet: %s", page_info.get("snippet", "")[:300])
            except Exception as diag_err:
                logger.info("LATAM: diagnostic eval failed: %s", diag_err)

            # Dismiss cookie consent banners that may block API calls
            try:
                await page.evaluate("""() => {
                    // OneTrust
                    const ot = document.querySelector('#onetrust-accept-btn-handler');
                    if (ot && ot.offsetHeight > 0) { ot.click(); return; }
                    // Generic accept buttons
                    const btns = document.querySelectorAll('button');
                    for (const b of btns) {
                        const t = b.textContent.trim().toLowerCase();
                        if ((t.includes('accept') || t.includes('agree') || t === 'ok' || t.includes('got it'))
                            && b.offsetHeight > 0) { b.click(); return; }
                    }
                }""")
                await asyncio.sleep(1.0)
            except Exception:
                pass

            # Wait for API response or page settling
            try:
                await asyncio.wait_for(api_event.wait(), timeout=_RESULTS_WAIT)
            except asyncio.TimeoutError:
                # Log diagnostic info
                title = await page.title()
                cur_url = page.url
                logger.warning("LATAM: BFF/offers response timed out after %ds, title='%s', url=%s",
                             _RESULTS_WAIT, title[:50], cur_url[:80])
                # Log what network requests were made to latamairlines.com
                try:
                    net_info = await page.evaluate("""() => {
                        return performance.getEntriesByType('resource')
                            .filter(e => e.name.includes('latamairlines.com'))
                            .map(e => e.name.split('?')[0])
                            .slice(0, 20)
                            .join('\\n');
                    }""")
                    logger.info("LATAM: network requests to latamairlines.com:\\n%s", net_info[:1000])
                except Exception:
                    pass

            # Log diagnostic: failed requests and console errors
            if _failed_reqs:
                logger.info("LATAM: %d failed requests, first 10: %s", len(_failed_reqs), _failed_reqs[:10])
            if _console_errors:
                logger.info("LATAM: %d console errors, first 5: %s", len(_console_errors), _console_errors[:5])
            # Log ALL XHR/fetch URLs
            if _all_xhr_urls:
                logger.info("LATAM: %d XHR/fetch responses: %s", len(_all_xhr_urls), _all_xhr_urls[:20])
            else:
                logger.info("LATAM: 0 XHR/fetch responses seen")
            # Log total resource count
            try:
                res_count = await page.evaluate("() => performance.getEntriesByType('resource').length")
                body_len = await page.evaluate("() => (document.body?.innerText || '').length")
                logger.info("LATAM: total resources loaded: %d, bodyLen now: %d", res_count, body_len)
            except Exception:
                pass

            # ── Immediate BFF POST retry with captured headers ──
            # The page's own GET returns 403 (Akamai), but POST from browser
            # context carries cookies and bypasses Akamai.
            # Try multiple URL/header variants to find what works.
            if not api_responses and _captured_bff_req:
                import uuid
                captured_h = _captured_bff_req.get('headers', {})
                captured_url = _captured_bff_req.get('url', '')

                # Extract path+query from captured URL for variant B
                from urllib.parse import urlparse
                parsed = urlparse(captured_url)
                captured_path_qs = parsed.path
                if parsed.query:
                    captured_path_qs += '?' + parsed.query

                cabin_map = {"M": "Economy", "W": "Premium+Economy", "C": "Business", "F": "First"}
                search_body = json.dumps({
                    "passengers": {"adults": req.adults, "children": req.children or 0, "infants": req.infants or 0},
                    "itinerary": [{
                        "departureCity": req.origin,
                        "arrivalCity": req.destination,
                        "departureDate": req.date_from.strftime("%Y-%m-%d"),
                    }],
                    "cabin": cabin_map.get(req.cabin_class or "M", "Economy"),
                    "redemption": False,
                })

                # Base headers (without searchToken)
                base_headers = {
                    'Content-Type': 'application/json',
                    'x-latam-action-name': captured_h.get('x-latam-action-name', 'search-result.flightselection.offers-search'),
                    'x-latam-app-session-id': captured_h.get('x-latam-app-session-id', str(uuid.uuid4())),
                    'x-latam-application-country': captured_h.get('x-latam-application-country', 'US'),
                    'x-latam-application-lang': captured_h.get('x-latam-application-lang', 'en'),
                    'x-latam-application-name': captured_h.get('x-latam-application-name', 'web-air-offers'),
                    'x-latam-application-oc': captured_h.get('x-latam-application-oc', 'us'),
                    'x-latam-client-name': captured_h.get('x-latam-client-name', 'web-air-offers'),
                    'x-latam-request-id': str(uuid.uuid4()),
                    'x-latam-track-id': captured_h.get('x-latam-track-id', str(uuid.uuid4())),
                    'x-latam-device-width': captured_h.get('x-latam-device-width', '1366'),
                }
                if captured_h.get('x-latam-captcha-token'):
                    base_headers['x-latam-captcha-token'] = captured_h['x-latam-captcha-token']

                # Variant A: POST to /bff/... WITHOUT searchToken
                # Variant B: POST to captured URL (with query params) WITHOUT searchToken
                # Variant C: GET to captured URL with all captured headers
                variants = [
                    ("A-POST-no-token", 'POST', '/bff/air-offers/v2/offers/search', base_headers, search_body),
                    ("B-POST-captured-url", 'POST', captured_path_qs, base_headers, search_body),
                    ("C-GET-captured-url", 'GET', captured_path_qs, {**base_headers, **{k: v for k, v in captured_h.items() if k.startswith('x-latam-')}}, None),
                ]

                logger.info("LATAM: trying %d BFF variants, captured_url=%s", len(variants), captured_path_qs[:200])
                for vname, method, url, hdrs, body in variants:
                    try:
                        r = await page.evaluate("""(args) => {
                            const opts = {method: args.method, headers: args.headers};
                            if (args.body) opts.body = args.body;
                            return fetch(args.url, opts)
                                .then(r => r.text().then(t => ({status: r.status, len: t.length, body: t.slice(0, 2000)})))
                                .catch(e => ({error: e.message}));
                        }""", {"method": method, "url": url, "headers": hdrs, "body": body})
                        logger.info("LATAM: variant %s → status=%s len=%s body=%s",
                                    vname, r.get("status"), r.get("len"), str(r.get("body", ""))[:500])
                        if r.get("status") == 200 and r.get("len", 0) > 500:
                            # Success! Fetch full body
                            full = await page.evaluate("""(args) => {
                                const opts = {method: args.method, headers: args.headers};
                                if (args.body) opts.body = args.body;
                                return fetch(args.url, opts).then(r => r.text());
                            }""", {"method": method, "url": url, "headers": hdrs, "body": body})
                            api_responses.append(full)
                            logger.info("LATAM: variant %s succeeded, got %d bytes", vname, len(full))
                            break
                    except Exception as ve:
                        logger.info("LATAM: variant %s error: %s", vname, ve)

            # Fall through to __NEXT_DATA__ polling if all variants failed
            if not api_responses:
                logger.info("LATAM: no BFF success, waiting for page redirect with __NEXT_DATA__...")
                for i in range(12):  # Wait up to 60s in 5s increments
                    await asyncio.sleep(5)
                    next_data = await page.evaluate("""() => {
                        const el = document.getElementById('__NEXT_DATA__');
                        return el ? el.textContent.length : 0;
                    }""")
                    cur_title = (await page.title())[:60]
                    if next_data > 10000:  # Meaningful data (not just shell)
                        logger.info("LATAM: found __NEXT_DATA__ len=%d after redirect (iter %d), title=%s", next_data, i, cur_title)
                        break
                    if i % 3 == 0:  # Log every 15s to reduce noise
                        logger.info("LATAM: waiting... __NEXT_DATA__ len=%d title=%s (iter %d)", next_data, cur_title, i)
            else:
                await asyncio.sleep(2)

            if not api_responses:
                # Check if page has navigated to a different URL (timeout/error page with data)
                cur_url = page.url
                cur_title = await page.title()
                logger.info("LATAM: after extra wait, title='%s' url=%s", cur_title[:60], cur_url[:100])

            if not api_responses:
                # Try extracting __NEXT_DATA__ or embedded JSON
                next_data = await page.evaluate("""() => {
                    const el = document.getElementById('__NEXT_DATA__');
                    return el ? el.textContent : null;
                }""")
                if next_data:
                    logger.info("LATAM: found __NEXT_DATA__ len=%d", len(next_data))
                    try:
                        nd = json.loads(next_data)
                        top_keys = list(nd.keys()) if isinstance(nd, dict) else type(nd).__name__
                        logger.info("LATAM: __NEXT_DATA__ top keys: %s", top_keys)
                        pp = nd.get("props", {}).get("pageProps", {})
                        if pp:
                            pp_keys = list(pp.keys())[:30]
                            logger.info("LATAM: pageProps keys: %s", pp_keys)
                            # Log structure for diagnostics
                            for k in pp_keys[:10]:
                                v = pp[k]
                                if isinstance(v, dict):
                                    logger.info("LATAM: pp.%s = dict(%d) keys=%s", k, len(v), list(v.keys())[:15])
                                elif isinstance(v, list):
                                    logger.info("LATAM: pp.%s = list(%d)", k, len(v))
                                else:
                                    logger.info("LATAM: pp.%s = %s val=%s", k, type(v).__name__, str(v)[:100])
                    except Exception as e:
                        logger.info("LATAM: __NEXT_DATA__ parse error: %s", e)
                    api_responses.append(next_data)

            if not api_responses:
                # DOM scraping fallback: check if body has meaningful content
                try:
                    body_len = await page.evaluate("() => (document.body?.innerText || '').length")
                    logger.info("LATAM: no API response, body length=%d -- attempting DOM extraction", body_len)
                    # Log page DOM snippet for debugging
                    dom_snippet = await page.evaluate("() => (document.body?.innerText || '').slice(0, 1500)")
                    logger.info("LATAM: DOM content: %s", dom_snippet[:1000])
                except Exception as e:
                    logger.info("LATAM: DOM extraction failed: %s", e)
                logger.warning("LATAM: no API response captured")
                return None

            elapsed = time.monotonic() - t0
            all_offers: list[FlightOffer] = []
            for raw in api_responses:
                try:
                    data = json.loads(raw)
                    parsed = self._parse_flights(data, req)
                    all_offers.extend(parsed)
                except Exception:
                    continue

            # Deduplicate by id
            seen = set()
            unique = []
            for o in all_offers:
                if o.id not in seen:
                    seen.add(o.id)
                    unique.append(o)
            return self._build_response(unique, req, elapsed)

        except asyncio.TimeoutError:
            logger.warning("LATAM: search timed out")
            return None
        except Exception as e:
            logger.warning("LATAM: search error: %s", e)
            return None
        finally:
            try:
                if page: await page.close()
            except Exception: pass
            try:
                if context: await context.close()
            except Exception: pass
            try:
                if browser: await browser.close()
            except Exception: pass
            try:
                if pw: await pw.stop()
            except Exception: pass

    # -- Response parsing (generic, from SDK) --

    def _parse_flights(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        if isinstance(data, dict):
            # __NEXT_DATA__ structure: props.pageProps.{searchResult, flights, etc.}
            pp = data.get("props", {}).get("pageProps", {})
            if pp:
                # Try LATAM-specific deep paths
                latam = self._parse_latam_next_data(pp, req)
                if latam:
                    return latam

            # Try various known response shapes
            for key in ("flights", "offers", "data", "results", "itineraries", "props"):
                nested = data.get(key)
                if isinstance(nested, list) and nested:
                    for item in nested:
                        o = self._try_parse_offer(item, req)
                        if o:
                            offers.append(o)
                    if offers:
                        return offers
                elif isinstance(nested, dict):
                    sub = self._parse_flights(nested, req)
                    if sub:
                        return sub

            # pageProps -> ... deep chain for __NEXT_DATA__
            page_props = data.get("pageProps", {})
            if isinstance(page_props, dict):
                sub = self._parse_flights(page_props, req)
                if sub:
                    return sub

            # Direct list items
            for key in ("slices", "legs", "segments", "journeys", "outbound", "departure"):
                nested = data.get(key)
                if isinstance(nested, list) and nested:
                    for item in nested:
                        o = self._try_parse_offer(item, req)
                        if o:
                            offers.append(o)
                    if offers:
                        return offers
        elif isinstance(data, list):
            for item in data:
                o = self._try_parse_offer(item, req)
                if o:
                    offers.append(o)
        return offers

    def _parse_latam_next_data(self, page_props: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse LATAM's __NEXT_DATA__ pageProps structure."""
        offers = []
        # LATAM uses various keys for flight data
        # Common patterns: flightOffers, searchResult.flights, recommendedFlights, etc.
        # Deep-search for any dict that has both price and flight segment info
        candidates = self._deep_find_offers(page_props, depth=0, max_depth=6)
        logger.info("LATAM: deep-search found %d candidate offer objects", len(candidates))
        if candidates:
            # Log a sample candidate's keys
            sample = candidates[0]
            logger.info("LATAM: sample candidate keys: %s", list(sample.keys())[:20])
        for item in candidates:
            o = self._try_parse_offer(item, req)
            if o:
                offers.append(o)
        return offers

    def _deep_find_offers(self, obj: Any, depth: int, max_depth: int) -> list[dict]:
        """Recursively find dicts that look like flight offers (have price info)."""
        if depth > max_depth:
            return []
        results = []
        if isinstance(obj, dict):
            # Check if this dict looks like a flight offer
            has_price = any(k in obj for k in ("price", "totalPrice", "amount", "fare",
                                                 "lowestPrice", "bestPrice", "displayPrice",
                                                 "cabinClass", "fareFamily"))
            has_flight = any(k in obj for k in ("segments", "legs", "flights", "flightSegments",
                                                  "departure", "origin", "carrier",
                                                  "departureDateTime", "departureAirport",
                                                  "duration", "flightNumber"))
            if has_price and has_flight:
                results.append(obj)
            elif has_price and depth >= 2:
                # Might be a pricing object within an offer
                results.append(obj)
            # Recurse into values
            for v in obj.values():
                results.extend(self._deep_find_offers(v, depth + 1, max_depth))
        elif isinstance(obj, list):
            for item in obj:
                results.extend(self._deep_find_offers(item, depth + 1, max_depth))
        return results

    def _try_parse_offer(self, item: Any, req: FlightSearchRequest) -> Optional[FlightOffer]:
        if not isinstance(item, dict):
            return None
        price = None
        currency = req.currency or "USD"
        for price_key in ("price", "totalPrice", "amount", "fare", "displayPrice", "lowestPrice", "bestPrice"):
            val = item.get(price_key)
            if val is not None:
                if isinstance(val, dict):
                    currency = val.get("currency") or val.get("currencyCode") or currency
                    price = val.get("amount") or val.get("value") or val.get("total")
                else:
                    price = val
                if price is not None:
                    break
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        # Segments
        segments: list[FlightSegment] = []
        for seg_key in ("segments", "legs", "flights", "flightSegments"):
            seg_raw = item.get(seg_key, [])
            if isinstance(seg_raw, list) and seg_raw:
                for seg in seg_raw:
                    if isinstance(seg, dict):
                        segments.append(self._build_segment(seg, req))
                break

        if not segments:
            segments.append(self._build_segment(item, req))

        total_dur = 0
        dur = item.get("duration") or item.get("totalDuration") or item.get("durationMinutes")
        if dur:
            try:
                total_dur = int(dur) * 60 if int(dur) < 5000 else int(dur)
            except (TypeError, ValueError):
                pass
        if total_dur == 0 and segments[0].departure and segments[-1].arrival:
            try:
                t0 = datetime.fromisoformat(segments[0].departure)
                t1 = datetime.fromisoformat(segments[-1].arrival)
                total_dur = int((t1 - t0).total_seconds())
            except Exception:
                pass

        route = FlightRoute(segments=segments, total_duration_seconds=max(total_dur, 0), stopovers=max(len(segments) - 1, 0))
        airlines = list(dict.fromkeys(s.airline for s in segments if s.airline))
        fid = item.get("id") or item.get("offerId") or hashlib.md5(json.dumps(item, default=str).encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"la-{hashlib.md5(str(fid).encode()).hexdigest()[:14]}",
            price=round(price, 2), currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route, airlines=airlines or ["LA"],
            owner_airline="LA", source="latam_direct", source_tier="protocol", is_locked=True,
            booking_url=f"https://www.latamairlines.com/us/en/flight-offers?origin={req.origin}&destination={req.destination}&outbound={req.date_from}&trip=OW",
        )

    def _build_segment(self, seg: dict, req: FlightSearchRequest) -> FlightSegment:
        dep = seg.get("departure") or seg.get("departureDateTime") or seg.get("std") or ""
        arr = seg.get("arrival") or seg.get("arrivalDateTime") or seg.get("sta") or ""
        carrier = seg.get("carrier") or seg.get("airline") or seg.get("marketingCarrier", {}).get("code", "") or seg.get("operatingCarrier", {}).get("code", "") or "LA"
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("number") or "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or ""
        if isinstance(origin, dict):
            origin = origin.get("code") or origin.get("iata") or ""
        dest = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or ""
        if isinstance(dest, dict):
            dest = dest.get("code") or dest.get("iata") or ""
        return FlightSegment(
            airline=carrier, airline_name=_carrier_name(carrier),
            flight_no=f"{carrier}{flight_no}" if flight_no and not flight_no.startswith(carrier) else flight_no,
            origin=origin or req.origin, destination=dest or req.destination,
            departure=_parse_dt(dep), arrival=_parse_dt(arr),
        )

    def _build_response(self, offers, req, elapsed):
        offers.sort(key=lambda o: o.price)
        by_airline: dict[str, list[FlightOffer]] = defaultdict(list)
        for o in offers:
            by_airline[o.owner_airline or "LA"].append(o)
        airlines_summary = [
            AirlineSummary(airline_code=c, airline_name=_carrier_name(c), cheapest_price=min(al, key=lambda o: o.price).price, currency=min(al, key=lambda o: o.price).currency, offer_count=len(al), cheapest_offer_id=min(al, key=lambda o: o.price).id, sample_route=f"{req.origin}->{req.destination}")
            for c, al in by_airline.items()
        ]
        logger.info("LATAM: %d offers for %s->%s on %s (%.1fs)", len(offers), req.origin, req.destination, req.date_from, elapsed)
        return FlightSearchResponse(
            search_id=hashlib.md5(f"la-{req.origin}-{req.destination}-{req.date_from}-{time.time()}".encode()).hexdigest()[:12],
            origin=req.origin, destination=req.destination, currency=offers[0].currency if offers else "USD",
            offers=offers[:req.limit], total_results=len(offers), airlines_summary=airlines_summary,
            search_params={"source": "latam_direct", "method": "patchright_url_nav_bff_intercept", "elapsed": round(elapsed, 2)},
            source_tiers={"protocol": "LATAM Airlines direct (latamairlines.com)"},
        )

    @staticmethod
    def _combine_rt(ob, ib, req):
        combos = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(id=f"rt_la_{cid}", price=price, currency=o.currency, outbound=o.outbound, inbound=i.outbound, airlines=list(dict.fromkeys(o.airlines + i.airlines)), owner_airline=o.owner_airline, booking_url=o.booking_url, is_locked=False, source=o.source, source_tier=o.source_tier))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req):
        return FlightSearchResponse(origin=req.origin, destination=req.destination, currency="USD", offers=[], total_results=0, search_params={"source": "latam_direct", "error": "no_results"}, source_tiers={"protocol": "LATAM Airlines direct (latamairlines.com)"})


def _parse_dt(s: str) -> str:
    if not s:
        return ""
    try:
        return datetime.fromisoformat(s).isoformat()
    except (ValueError, TypeError):
        return s

def _carrier_name(code: str) -> str:
    return {"LA": "LATAM Airlines", "JJ": "LATAM Brasil", "4C": "LATAM Colombia", "4M": "LATAM Argentina", "LP": "LATAM Peru", "XL": "LATAM Ecuador", "AA": "American Airlines", "DL": "Delta Air Lines", "IB": "Iberia", "QR": "Qatar Airways", "BA": "British Airways"}.get(code, code)