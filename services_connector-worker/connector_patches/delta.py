"""
Delta Air Lines patchright connector -- Cloud Run patch.

Replaces CDP Chrome with Patchright headed browser to bypass Akamai Bot Manager.
Form fill + GraphQL API interception logic preserved from SDK.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
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
_RESULTS_WAIT = 40
_DOM_POLL_INTERVAL = 5
_DOM_POLL_ROUNDS = 6


async def _launch_browser():
    """Launch a Patchright browser using the system Chrome binary."""
    from patchright.async_api import async_playwright
    from .browser import find_chrome, inject_stealth_js, auto_block_if_proxied

    proxy = None
    _BYPASS = ".google.com,.googletagmanager.com,.gstatic.com,.googleapis.com,.google-analytics.com,.googlesyndication.com,.doubleclick.net"
    # Residential proxy required: GCP datacenter IPs get Kasada 444 "Access Denied"
    # on /flightsearch/ results page. EU proxy → mach_core form (handled below).
    letsfg_proxy = os.environ.get("LETSFG_PROXY", "").strip()
    if letsfg_proxy:
        import socket as _sock
        try:
            _s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
            _s.connect(("127.0.0.1", 8899))
            _s.close()
            proxy = {"server": "http://127.0.0.1:8899", "bypass": _BYPASS}
            logger.info("Delta: using proxy relay on port 8899")
        except OSError:
            from urllib.parse import urlparse
            p = urlparse(letsfg_proxy)
            proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}", "bypass": _BYPASS}
            if p.username:
                proxy["username"] = p.username
                proxy["password"] = p.password or ""
            logger.info("Delta: using direct proxy %s:%s", p.hostname, p.port)
    else:
        logger.info("Delta: no proxy (direct connection)")

    try:
        chrome_path = find_chrome()
        logger.info("Delta: using system Chrome at %s", chrome_path)
    except RuntimeError:
        chrome_path = None
        logger.info("Delta: system Chrome not found, using bundled Chromium")

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
        timezone_id="America/New_York",
        color_scheme="light",
    )
    page = await context.new_page()

    # Do NOT use add_init_script or inject_stealth_js — Patchright's init script
    # mechanism creates a visible patchright-init-script-inject.internal/ request
    # that Akamai flags, resulting in "Access Denied" on the results page.
    #
    # Instead, inject fingerprint overrides via raw CDP which runs before any
    # page JS without creating detectable resource URLs.  This masks the
    # SwiftShader GPU and Xvfb display that Kasada uses to fingerprint Cloud Run.
    cdp = await context.new_cdp_session(page)
    await cdp.send("Page.addScriptToEvaluateOnNewDocument", {"source": """
        // ── Anti-fingerprint injection for Cloud Run ──
        // Kasada collects canvas, WebGL, audio, and hardware fingerprints.
        // SwiftShader + Xvfb produce recognisable signatures.  We inject
        // per-session noise so no two runs produce the same hash, and the
        // hash doesn't match any known-bad Cloud Run profile.
        (function() {
            // Deterministic seed from crypto for this page lifetime
            const _seed = new Uint32Array(4);
            crypto.getRandomValues(_seed);
            let _si = 0;
            function rng() {
                // xorshift128 — fast, deterministic per page load
                let s = _seed[_si & 3];
                s ^= s << 11; s ^= s >>> 8;
                _seed[_si & 3] = s ^ _seed[(_si + 3) & 3] ^ (_seed[(_si + 3) & 3] >>> 19);
                _si++;
                return (_seed[_si & 3] >>> 0) / 4294967296;
            }

            // ── Canvas 2D noise ──
            // Inject ±1 LSB noise into canvas pixel data so toDataURL()
            // produces a unique hash every session.
            const origGetImageData = CanvasRenderingContext2D.prototype.getImageData;
            CanvasRenderingContext2D.prototype.getImageData = function() {
                const imageData = origGetImageData.apply(this, arguments);
                const d = imageData.data;
                // Perturb ~5% of pixels by ±1 in one channel
                for (let i = 0; i < d.length; i += 16) {
                    const idx = i + ((rng() * 4) | 0);
                    if (idx < d.length) {
                        d[idx] = Math.max(0, Math.min(255, d[idx] + (rng() > 0.5 ? 1 : -1)));
                    }
                }
                return imageData;
            };

            const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function() {
                const ctx = this.getContext('2d');
                if (ctx) {
                    // Touch one pixel to trigger noise path
                    const px = ctx.getImageData(0, 0, 1, 1);
                    ctx.putImageData(px, 0, 0);
                }
                return origToDataURL.apply(this, arguments);
            };

            const origToBlob = HTMLCanvasElement.prototype.toBlob;
            HTMLCanvasElement.prototype.toBlob = function(cb) {
                const ctx = this.getContext('2d');
                if (ctx) {
                    const px = ctx.getImageData(0, 0, 1, 1);
                    ctx.putImageData(px, 0, 0);
                }
                return origToBlob.apply(this, arguments);
            };

            // ── WebGL parameter + readPixels noise ──
            const REAL_RENDERER = 'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)';
            const REAL_VENDOR  = 'Google Inc. (Intel)';
            const UNMASKED_R   = 'Intel(R) UHD Graphics 630';
            const UNMASKED_V   = 'Intel Inc.';

            function patchGL(proto) {
                const origGet = proto.getParameter;
                proto.getParameter = function(p) {
                    if (p === 0x1F01) return REAL_RENDERER;
                    if (p === 0x1F00) return REAL_VENDOR;
                    if (p === 0x9246) return UNMASKED_R;
                    if (p === 0x9245) return UNMASKED_V;
                    return origGet.call(this, p);
                };
                const origRead = proto.readPixels;
                proto.readPixels = function() {
                    origRead.apply(this, arguments);
                    // arguments[6] is the output buffer
                    const buf = arguments[6];
                    if (buf && buf.length) {
                        for (let i = 0; i < buf.length; i += 32) {
                            buf[i] = (buf[i] + (rng() > 0.5 ? 1 : -1) + 256) & 0xFF;
                        }
                    }
                };
            }
            patchGL(WebGLRenderingContext.prototype);
            if (typeof WebGL2RenderingContext !== 'undefined') {
                patchGL(WebGL2RenderingContext.prototype);
            }

            // ── AudioContext fingerprint ──
            // Kasada hashes oscillator → analyser → destination output.
            // We add micro-noise to the analyser frequency data.
            const origGetFloat = AnalyserNode.prototype.getFloatFrequencyData;
            AnalyserNode.prototype.getFloatFrequencyData = function(arr) {
                origGetFloat.call(this, arr);
                for (let i = 0; i < arr.length; i += 8) {
                    arr[i] += (rng() - 0.5) * 0.001;
                }
            };
            const origGetByte = AnalyserNode.prototype.getByteFrequencyData;
            AnalyserNode.prototype.getByteFrequencyData = function(arr) {
                origGetByte.call(this, arr);
                for (let i = 0; i < arr.length; i += 8) {
                    arr[i] = Math.max(0, Math.min(255, arr[i] + (rng() > 0.5 ? 1 : -1)));
                }
            };

            // ── Hardware signals ──
            Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
            Object.defineProperty(navigator, 'deviceMemory',        {get: () => 8});
            Object.defineProperty(screen, 'colorDepth', {get: () => 24});
            Object.defineProperty(screen, 'pixelDepth',  {get: () => 24});

            // ── Plugins (headless Chrome has empty plugins) ──
            Object.defineProperty(navigator, 'plugins', {get: () => {
                return [
                    {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer',
                     description: 'Portable Document Format', length: 1},
                    {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
                     description: '', length: 1},
                    {name: 'Native Client', filename: 'internal-nacl-plugin',
                     description: '', length: 2},
                ];
            }});
            Object.defineProperty(navigator, 'mimeTypes', {get: () => {
                return [{type: 'application/pdf', suffixes: 'pdf',
                         description: 'Portable Document Format', enabledPlugin: navigator.plugins[0]}];
            }});

            // ── Permissions API ──
            const origQuery = navigator.permissions.query;
            navigator.permissions.query = function(desc) {
                if (desc.name === 'notifications') {
                    return Promise.resolve({state: Notification.permission});
                }
                return origQuery.call(this, desc);
            };

            // ── Chrome runtime (missing in Patchright sometimes) ──
            if (!window.chrome) window.chrome = {};
            if (!window.chrome.runtime) {
                window.chrome.runtime = {
                    connect: function() {},
                    sendMessage: function() {},
                };
            }
        })();
    """})
    await cdp.detach()

    return pw, browser, context, page


class DeltaConnectorClient:
    """Delta Air Lines -- Patchright headed Chrome + form fill + GraphQL intercept."""

    def __init__(self, timeout: float = 60.0):
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
        origins = get_city_airports(req.origin)
        if len(origins) > 1:
            req = FlightSearchRequest(origin=origins[0], destination=req.destination, date_from=req.date_from, return_from=req.return_from, adults=req.adults, children=req.children, infants=req.infants, cabin_class=req.cabin_class, currency=req.currency, max_stopovers=req.max_stopovers)
        dests = get_city_airports(req.destination)
        if len(dests) > 1:
            req = FlightSearchRequest(origin=req.origin, destination=dests[0], date_from=req.date_from, return_from=req.return_from, adults=req.adults, children=req.children, infants=req.infants, cabin_class=req.cabin_class, currency=req.currency, max_stopovers=req.max_stopovers)

        t0 = time.monotonic()
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._attempt_search(req, t0)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning("Delta: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)
        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[FlightSearchResponse]:
        pw = browser = context = page = None
        try:
            pw, browser, context, page = await _launch_browser()

            # No page.route() or add_init_script() — Patchright init scripts create
            # a detectable patchright-init-script-inject.internal/ request that Akamai flags.
            # Handle overlays via page.evaluate() after page loads instead.

            api_response_body: list[str] = []
            api_response_event = asyncio.Event()
            _api_urls_seen: list[str] = []
            _search_phase = False
            _got_429 = False
            _offer_req_body: list[str | None] = [None]
            _offer_req_url: list[str | None] = [None]
            _offer_req_headers: list[dict | None] = [None]

            async def _on_request(request):
                if "offer-api" in request.url and request.method == "POST":
                    try:
                        _offer_req_body[0] = request.post_data
                        _offer_req_url[0] = request.url
                        hdrs = {k: v for k, v in request.headers.items()
                                if k.lower() in ("content-type", "accept", "accept-language")}
                        _offer_req_headers[0] = hdrs
                        logger.info("Delta: captured offer-api request body len=%d url=%s",
                                   len(_offer_req_body[0]) if _offer_req_body[0] else 0, request.url[:120])
                    except Exception:
                        pass

            async def _on_response(response):
                nonlocal _got_429
                url = response.url
                # After search click, log ALL responses (not just delta.com)
                if _search_phase:
                    ct = response.headers.get('content-type', '')
                    if not any(url.endswith(ext) for ext in ('.png', '.jpg', '.svg', '.gif', '.ico', '.woff', '.woff2', '.css', '.js')):
                        _api_urls_seen.append(f"{response.status} {url[:140]}")
                        if len(_api_urls_seen) <= 50:
                            logger.info("Delta: post-nav resp: %d %s ct=%s", response.status, url[:160], ct[:40])
                if "offer-api" in url:
                    try:
                        body = await response.text()
                        logger.info("Delta: OFFER API: %s status=%d len=%d body_preview=%s",
                                   url[:120], response.status, len(body), body[:200])
                        if response.status == 200 and len(body) > 500:
                            api_response_body.append(body)
                            api_response_event.set()
                        elif response.status == 429:
                            _got_429 = True
                            logger.warning("Delta: offer API 429 (Kasada challenge), nav blocker should keep page on results")
                    except Exception as e:
                        logger.warning("Delta: offer body error: %s", e)

            page.on("request", _on_request)
            page.on("response", _on_response)

            logger.info("Delta: searching %s->%s on %s", req.origin, req.destination, req.date_from)

            # ── Phase 1: Load homepage for Akamai sensor warm-up ──
            # Force US locale to get classic/modern form instead of mach_core EU form
            await page.goto("https://www.delta.com/en-us/", wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            logger.info("Delta: landed on %s", page.url)

            # If redirected to /eu/ form, dismiss welcome dialog and try to switch locale
            if "/eu/" in page.url or "/it/" in page.url or "/fr/" in page.url:
                logger.info("Delta: EU locale detected, attempting to dismiss welcome dialog + switch")
                # Dismiss welcome / country-picker dialog
                await page.evaluate("""() => {
                    document.querySelectorAll('[role="dialog"], .pop-up').forEach(d => d.remove());
                    // Also remove backdrop
                    document.querySelectorAll('[class*="backdrop"], [class*="overlay"]').forEach(d => {
                        if (d.style.position === 'fixed' || window.getComputedStyle(d).position === 'fixed') d.remove();
                    });
                    if (document.body) document.body.style.overflow = 'auto';
                }""")
                await asyncio.sleep(0.5)

            # Apply minimal stealth patches via evaluate (not add_init_script)
            try:
                await page.evaluate("""() => {
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    if (!window.chrome) { window.chrome = {runtime: {}, loadTimes: () => ({}), csi: () => ({})}; }
                }""")
            except Exception:
                pass

            # Early detection of Akamai "Access Denied" block
            early_title = await page.evaluate("() => document.title")
            early_body = await page.evaluate("() => (document.body ? document.body.innerText : '').slice(0, 200)")
            if "Access Denied" in (early_title or "") or "Access Denied" in (early_body or ""):
                logger.warning("Delta: homepage blocked by Akamai (title=%s body=%s), aborting",
                              early_title, early_body[:100])
                return None

            # Wait for booking form to appear AND be interactive
            form_type = "none"
            for _w in range(10):
                has_classic = await page.evaluate(
                    "() => !!document.querySelector('#fromAirportName')"
                )
                if has_classic:
                    form_type = "classic"
                    break
                has_modern = await page.evaluate("""() => {
                    const btn = document.querySelector('#findFilghtsCta');
                    if (!btn) return false;
                    const inputs = document.querySelectorAll('mach-route-picker input');
                    return inputs.length >= 2;
                }""")
                if has_modern:
                    form_type = "modern"
                    break
                # Check for EU/APAC button-based form (mach-core)
                has_mach_core = await page.evaluate("""() => {
                    const originBtn = document.querySelector('#one-way-route-picker-origin-button, [id*="route-picker-origin"]');
                    const findBtn = document.querySelector('#findFilghtsCta');
                    return !!(originBtn && findBtn);
                }""")
                if has_mach_core:
                    form_type = "mach_core"
                    break
                await asyncio.sleep(1)
            await asyncio.sleep(0.5)
            logger.warning("Delta: form_type=%s (after %ds) url=%s", form_type, _w + 1, page.url)

            if form_type == "none":
                diag = await page.evaluate("""() => {
                    const url = location.href;
                    const title = document.title;
                    const bodyLen = document.body ? document.body.innerHTML.length : 0;
                    const bodyText = (document.body ? document.body.innerText : '').slice(0, 300);
                    const inputs = Array.from(document.querySelectorAll('input')).slice(0, 10).map(i => ({
                        id: i.id, name: i.name, type: i.type, placeholder: i.placeholder,
                    }));
                    const buttons = Array.from(document.querySelectorAll('button')).slice(0, 10).map(b => ({
                        id: b.id, text: b.textContent.trim().slice(0, 40),
                    }));
                    return {url, title, bodyLen, bodyText, inputs, buttons};
                }""")
                logger.warning("Delta: page diagnostic: %s", json.dumps(diag)[:600])
                return None

            # Dismiss overlays (cookie banners, welcome dialogs)
            await page.evaluate("""() => {
                document.querySelectorAll('[id*="onetrust"], .onetrust-pc-dark-filter, .ot-fade-in').forEach(e => e.remove());
                document.querySelectorAll('[role="dialog"], .pop-up').forEach(d => d.remove());
                document.querySelectorAll('[class*="backdrop"]').forEach(d => {
                    if (window.getComputedStyle(d).position === 'fixed') d.remove();
                });
                if (document.body) document.body.style.overflow = 'auto';
            }""")

            # ── Brief warm-up: mouse + scroll for Akamai sensor ──
            for _ in range(4):
                await page.mouse.move(random.randint(100, 1200), random.randint(80, 600), steps=random.randint(8, 15))
                await asyncio.sleep(random.uniform(0.15, 0.35))
            await page.evaluate("window.scrollTo({top: 300, behavior: 'smooth'})")
            await asyncio.sleep(0.6)
            await page.evaluate("window.scrollTo({top: 0, behavior: 'smooth'})")
            await asyncio.sleep(0.8)
            logger.info("Delta: warm-up complete, starting form fill")

            # Fill origin
            if form_type == "classic":
                ok = await self._fill_airport_classic(page, "origin", req.origin)
            elif form_type == "mach_core":
                ok = await self._fill_airport_mach_core(page, "origin", req.origin)
            else:
                ok = await self._fill_airport_modern(page, "origin", req.origin)
            if not ok:
                logger.warning("Delta: origin fill failed")
                return None
            await asyncio.sleep(0.4)

            # Fill destination
            if form_type == "classic":
                ok = await self._fill_airport_classic(page, "destination", req.destination)
            elif form_type == "mach_core":
                ok = await self._fill_airport_mach_core(page, "destination", req.destination)
            else:
                ok = await self._fill_airport_modern(page, "destination", req.destination)
            if not ok:
                logger.warning("Delta: destination fill failed")
                return None
            await asyncio.sleep(0.4)

            # Select One Way trip type
            if form_type == "classic":
                await self._select_one_way_classic(page)
            elif form_type == "mach_core":
                await self._select_one_way_mach_core(page)
            else:
                await self._select_one_way_modern(page)
            await asyncio.sleep(0.3)

            # Fill departure date
            if form_type == "classic":
                await self._fill_date_classic(page, req)
            elif form_type == "mach_core":
                await self._fill_date_mach_core(page, req)
            else:
                await self._fill_date_modern(page, req)
            await asyncio.sleep(0.3)

            # Pre-click diagnostic: check form state and button
            pre_click = await page.evaluate("""() => {
                const btn = document.querySelector('#findFilghtsCta');
                const btnInfo = btn ? {
                    disabled: btn.disabled, ariaDisabled: btn.getAttribute('aria-disabled'),
                    cls: btn.className.slice(0, 80), text: btn.textContent.trim().slice(0, 40),
                    visible: btn.offsetWidth > 0,
                } : 'not-found';
                // Check form field values
                const fields = {};
                // Check origin/destination display
                const originBtn = document.querySelector('#one-way-route-picker-origin-button, [id*="route-picker-origin"]');
                if (originBtn) fields.origin = originBtn.textContent.trim().slice(0, 30);
                const destBtn = document.querySelector('#one-way-route-picker-destination-button, [id*="route-picker-destination"]');
                if (destBtn) fields.dest = destBtn.textContent.trim().slice(0, 30);
                // Check trip type
                const tripType = document.querySelector('[role="combobox"]');
                if (tripType) fields.tripType = tripType.textContent.trim().slice(0, 20);
                // Check date display
                const dateTrigger = document.querySelector('[id*="date-picker-trigger"]');
                if (dateTrigger) fields.date = dateTrigger.textContent.trim().slice(0, 30);
                return {btn: btnInfo, fields};
            }""")
            logger.info("Delta: pre-click state: %s", json.dumps(pre_click)[:500])

            # Click search/submit
            _search_phase = True
            if form_type == "classic":
                await page.evaluate("""() => {
                    const btn = document.querySelector('#btn-book-submit');
                    if (btn) btn.click();
                }""")
            else:
                # For mach_core, try both JS click and Playwright click
                click_result = await page.evaluate("""() => {
                    const btn = document.querySelector('#findFilghtsCta');
                    if (!btn) return 'not-found';
                    if (btn.disabled) return 'disabled';
                    btn.click();
                    return 'clicked:' + btn.textContent.trim().slice(0, 30);
                }""")
                logger.info("Delta: search button JS click: %s", click_result)
                if click_result == 'disabled' or click_result == 'not-found':
                    # Fallback: try Playwright click
                    try:
                        await page.locator('#findFilghtsCta').click(timeout=3000, force=True)
                        logger.info("Delta: search button Playwright force-click")
                    except Exception:
                        fb = page.locator('button[class*="find-flight"], button[aria-label*="Find"], button:has-text("Search"), button:has-text("Find")').first
                        try:
                            await fb.click(timeout=3000, force=True)
                            logger.info("Delta: search button fallback click")
                        except Exception as e2:
                            logger.warning("Delta: all search click attempts failed: %s", e2)

            logger.info("Delta: search clicked, waiting for offer API (url=%s)", page.url)

            # Wait for navigation to search results page
            try:
                await page.wait_for_url("**/flightsearch/**", timeout=15000)
                logger.info("Delta: navigated to results: %s", page.url)
            except Exception:
                await asyncio.sleep(3)
                post_url = page.url
                if "flightsearch" in post_url or "flight-search" in post_url:
                    logger.info("Delta: navigated to results (delayed): %s", post_url)
                else:
                    logger.warning("Delta: no nav to results: url=%s", post_url)

            # Block SPA error-redirect to book-a-flight
            await page.evaluate("""() => {
                function blocked(u) { return String(u||'').includes('book-a-flight'); }
                const _push = history.pushState.bind(history);
                history.pushState = function(...a) { if (!blocked(a[2])) return _push(...a); };
                const _repl = history.replaceState.bind(history);
                history.replaceState = function(...a) { if (!blocked(a[2])) return _repl(...a); };
                const _assign = Location.prototype.assign;
                Location.prototype.assign = function(u) { if (!blocked(u)) return _assign.call(this, u); };
                const _lrepl = Location.prototype.replace;
                Location.prototype.replace = function(u) { if (!blocked(u)) return _lrepl.call(this, u); };
            }""")

            # Let the SPA hydrate
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            # Check for Access Denied
            results_title = await page.evaluate("() => document.title")
            results_body = await page.evaluate("() => (document.body ? document.body.innerText : '').slice(0, 200)")
            if "Access Denied" in (results_title or "") or "Access Denied" in (results_body or ""):
                logger.warning("Delta: results page blocked by Akamai, aborting")
                return None

            # ── Wait for offer-api 200 (Kasada challenge → auto-solve → retry) ──
            try:
                await asyncio.wait_for(api_response_event.wait(), timeout=_RESULTS_WAIT)
                logger.info("Delta: offer-api 200 received within %ds", _RESULTS_WAIT)
            except asyncio.TimeoutError:
                cur_url = page.url
                logger.info("Delta: offer-api wait timed out (%ds), url=%s got_429=%s",
                           _RESULTS_WAIT, cur_url, _got_429)

            # ── Kasada 429 retry: reload search results to trigger fresh offer-api ──
            if not api_response_body and _got_429:
                # Capture the search results URL before SPA redirects
                search_url = None
                for url_entry in _api_urls_seen:
                    if "search-results" in url_entry and "200" in url_entry:
                        # Extract URL from "200 https://..." format
                        parts = url_entry.split(" ", 1)
                        if len(parts) == 2:
                            search_url = parts[1]
                            break

                if search_url:
                    logger.info("Delta: got 429, reloading search results URL for retry: %s", search_url[:120])
                    _got_429 = False  # Reset for retry
                    try:
                        await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
                        try:
                            await page.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            pass
                        # Wait for offer-api on retry
                        try:
                            await asyncio.wait_for(api_response_event.wait(), timeout=30)
                            logger.info("Delta: offer-api 200 received on retry")
                        except asyncio.TimeoutError:
                            logger.info("Delta: offer-api retry timed out, got_429=%s", _got_429)
                    except Exception as e:
                        logger.warning("Delta: search results reload failed: %s", e)
                else:
                    logger.info("Delta: no search-results URL found for retry")

            # ── DOM polling: check for rendered flight cards every few seconds ──
            # After Kasada solves, the SPA renders results — poll DOM for cards
            if not api_response_body:
                logger.info("Delta: starting DOM poll loop (%d rounds x %ds) url=%s",
                           _DOM_POLL_ROUNDS, _DOM_POLL_INTERVAL, page.url)
                for poll_round in range(1, _DOM_POLL_ROUNDS + 1):
                    # Check if api_response_event fired in the background
                    if api_response_body:
                        logger.info("Delta: API response captured during DOM poll round %d", poll_round)
                        break

                    dom_offers = await self._scrape_results_dom(page, req)
                    if dom_offers:
                        logger.info("Delta: DOM poll round %d found %d offers", poll_round, len(dom_offers))
                        elapsed = time.monotonic() - t0
                        return self._build_response(dom_offers, req, elapsed)

                    # Log page state periodically
                    if poll_round % 3 == 1:
                        page_state = await page.evaluate("""() => {
                            const bodyLen = document.body ? document.body.innerHTML.length : 0;
                            const title = document.title;
                            const url = location.href;
                            const text = (document.body ? document.body.innerText : '').slice(0, 200);
                            const loading = !!document.querySelector('[class*="loading"], [class*="spinner"], [class*="skeleton"]');
                            return {bodyLen, title, url, text, loading};
                        }""")
                        logger.info("Delta: DOM poll round %d/%d: bodyLen=%d title=%s loading=%s url=%s text=%s",
                                   poll_round, _DOM_POLL_ROUNDS,
                                   page_state.get('bodyLen', 0), page_state.get('title', '?'),
                                   page_state.get('loading'), page_state.get('url', '?'),
                                   page_state.get('text', '')[:120])

                    await asyncio.sleep(_DOM_POLL_INTERVAL)

                if not api_response_body:
                    logger.warning("Delta: DOM poll exhausted (%d rounds), no offers found", _DOM_POLL_ROUNDS)

            if not api_response_body:
                # Final diagnostics
                results_diag = await page.evaluate("""() => {
                    const bodyLen = document.body ? document.body.innerHTML.length : 0;
                    const scripts = document.querySelectorAll('script[src]').length;
                    const title = document.title;
                    const bodyText = (document.body ? document.body.innerText : '').slice(0, 300);
                    const hasAngular = !!window.angular || !!document.querySelector('[ng-app]');
                    const hasReact = !!document.querySelector('[data-reactroot]');
                    const iframes = document.querySelectorAll('iframe').length;
                    const inputs = document.querySelectorAll('input').length;
                    return {bodyLen, scripts, title, bodyText, hasAngular, hasReact, iframes, inputs};
                }""")
                logger.warning("Delta: results page diag: bodyLen=%d scripts=%d title=%s angular=%s iframes=%d text=%s",
                              results_diag.get('bodyLen',0), results_diag.get('scripts',0),
                              results_diag.get('title','?'), results_diag.get('hasAngular'),
                              results_diag.get('iframes',0), results_diag.get('bodyText','')[:200])
                logger.warning("Delta: offer API response timed out (url=%s), saw %d API URLs: %s",
                              page.url, len(_api_urls_seen), _api_urls_seen[:8])
                return None

            await asyncio.sleep(0.5)
            if not api_response_body:
                logger.warning("Delta: no API response captured")
                return None

            raw = max(api_response_body, key=len)
            data = json.loads(raw)

            elapsed = time.monotonic() - t0
            offers = self._parse_response(data, req)
            return self._build_response(offers, req, elapsed)

        except asyncio.TimeoutError:
            logger.warning("Delta: search timed out")
            return None
        except Exception as e:
            logger.warning("Delta: search error: %s", e)
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

    # -- Form helpers: classic (Angular) + modern (mach-route-picker) --------

    async def _select_one_way_classic(self, page) -> None:
        """Select 'One Way' from the classic Trip Type combobox."""
        try:
            # Check if already One Way
            text = await page.evaluate("""() => {
                const combo = document.querySelector('[role="combobox"][aria-owns*="TripType" i], [role="combobox"][aria-labelledby*="TripType" i], .select-ui-wrapper');
                return combo ? combo.textContent.trim() : '';
            }""")
            if "One Way" in text:
                return

            # Force-dismiss any remaining modals first
            await page.evaluate("""() => {
                document.querySelectorAll('modal-container, .modal-backdrop, .modal.show').forEach(e => e.remove());
                document.querySelectorAll('.modal-open').forEach(e => e.classList.remove('modal-open'));
                if (document.body) { document.body.style.overflow = 'auto'; }
            }""")
            await asyncio.sleep(0.3)

            # Try approach 1: Click the visible span[role="combobox"] wrapper, NOT the hidden <select>
            opened = await page.evaluate("""() => {
                // Find the visible combobox wrapper (Angular custom select)
                const wrappers = document.querySelectorAll('[role="combobox"]');
                for (const w of wrappers) {
                    const label = w.getAttribute('aria-labelledby') || w.getAttribute('aria-owns') || '';
                    if (label.toLowerCase().includes('triptype') || label.toLowerCase().includes('trip-type')) {
                        w.click();
                        return 'clicked:' + w.tagName + ':' + label;
                    }
                }
                // Fallback: click any select-ui-wrapper near trip type
                const wrapper = document.querySelector('.select-ui-wrapper');
                if (wrapper) { wrapper.click(); return 'clicked:fallback-wrapper'; }
                return 'no-combo-found';
            }""")
            logger.info("Delta: trip type open: %s", opened)
            await asyncio.sleep(0.5)

            # Select One Way option
            selected = await page.evaluate("""() => {
                const opts = document.querySelectorAll(
                    '#trip-type-listbox li, [id*="trip-type"] li, [id*="TripType"] li, [role="option"], [role="listbox"] li'
                );
                for (const o of opts) {
                    if (o.textContent.includes('One Way')) { o.click(); return 'clicked:' + o.textContent.trim(); }
                }
                return 'no-option-found:' + opts.length;
            }""")
            logger.info("Delta: trip type select: %s", selected)
            await asyncio.sleep(0.3)

            # Verify it stuck
            verify = await page.evaluate("""() => {
                const sel = document.querySelector('select#selectTripType, select[name="tripType"]');
                if (sel) return 'select-value:' + sel.value;
                const combo = document.querySelector('[role="combobox"]');
                return combo ? 'combo-text:' + combo.textContent.trim().slice(0,30) : 'no-element';
            }""")
            logger.info("Delta: trip type verify: %s", verify)

            # If still not One Way, try direct select value manipulation  
            if "One Way" not in verify and "ONE_WAY" not in verify:
                logger.warning("Delta: trip type visual click failed, trying direct value set")
                await page.evaluate("""() => {
                    const sel = document.querySelector('select#selectTripType, select[name="tripType"]');
                    if (sel) {
                        sel.value = 'ONE_WAY';
                        sel.dispatchEvent(new Event('change', {bubbles: true}));
                        sel.dispatchEvent(new Event('input', {bubbles: true}));
                    }
                    // Also try Angular scope update
                    try {
                        const el = document.querySelector('#selectTripType') || document.querySelector('[name="tripType"]');
                        if (el && window.angular) {
                            const scope = window.angular.element(el).scope();
                            if (scope) scope.$apply(() => { scope.tripType = 'ONE_WAY'; });
                        }
                    } catch(e) {}
                }""")
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning("Delta: classic trip type error: %s", e)

    async def _select_one_way_modern(self, page) -> None:
        """Select 'One Way' from the modern trip-type dropdown."""
        try:
            trip_type = page.locator('[class*="trip-type"]').first
            await trip_type.click(timeout=3000)
            await asyncio.sleep(0.5)
            opt = page.locator("[role='option']").filter(has_text="One Way").first
            await opt.click(timeout=3000)
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning("Delta: modern trip type error: %s", e)

    async def _select_one_way_mach_core(self, page) -> None:
        """Select 'One Way' from the mach-core Trip Type combobox."""
        try:
            # The mach_core trip type is a combobox button with dynamic id like select-oerhx3ytfo
            result = await page.evaluate("""() => {
                // Find the trip type combobox button by role or text content
                const combos = document.querySelectorAll('[role="combobox"], button.dropdown');
                for (const c of combos) {
                    const text = c.textContent.trim();
                    if (text.includes('Round Trip') || text.includes('Trip Type') || text.includes('One Way')) {
                        c.click();
                        return 'clicked:' + c.id + ':' + text.slice(0, 40);
                    }
                }
                // Fallback: look for book-widget-select-dropdown containing "Trip Type"
                const dropdowns = document.querySelectorAll('[class*="book-widget-select"]');
                for (const d of dropdowns) {
                    if (d.textContent.includes('Trip Type')) {
                        const btn = d.querySelector('button');
                        if (btn) { btn.click(); return 'clicked-dropdown:' + btn.id; }
                    }
                }
                return 'not-found';
            }""")
            logger.info("Delta: mach_core trip type open: %s", result)
            await asyncio.sleep(0.8)

            # Select One Way from the dropdown
            selected = await page.evaluate("""() => {
                const opts = document.querySelectorAll('[role="option"], [role="listbox"] li, li');
                for (const o of opts) {
                    const text = o.textContent.trim();
                    if (text === 'One Way' || text === 'One way') {
                        o.click();
                        return 'clicked:' + text;
                    }
                }
                // Try buttons inside dropdown
                const btns = document.querySelectorAll('[class*="dropdown"] button, [class*="listbox"] button');
                for (const b of btns) {
                    if (b.textContent.trim().includes('One Way')) {
                        b.click();
                        return 'clicked-btn:' + b.textContent.trim().slice(0, 30);
                    }
                }
                return 'not-found:' + opts.length;
            }""")
            logger.info("Delta: mach_core trip type select: %s", selected)
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning("Delta: mach_core trip type error: %s", e)

    async def _fill_airport_classic(self, page, field_type: str, iata: str) -> bool:
        """Fill airport via the classic #fromAirportName / #toAirportName link → modal."""
        try:
            link_id = "fromAirportName" if field_type == "origin" else "toAirportName"
            link = page.locator(f"#{link_id}")

            # Diagnose visibility
            state = await page.evaluate("""(id) => {
                const el = document.getElementById(id);
                if (!el) return {exists: false};
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                const overlays = document.querySelectorAll('[id*="onetrust"], .onetrust-pc-dark-filter, [class*="modal-backdrop"], [class*="overlay"]');
                return {
                    exists: true,
                    display: style.display,
                    visibility: style.visibility,
                    opacity: style.opacity,
                    rect: {top: rect.top, left: rect.left, width: rect.width, height: rect.height},
                    tagName: el.tagName,
                    overlayCount: overlays.length,
                    parentDisplay: el.parentElement ? window.getComputedStyle(el.parentElement).display : null,
                };
            }""", link_id)
            logger.info("Delta: airport %s element state: %s", field_type, json.dumps(state))

            # Force dismiss any overlays
            await page.evaluate("""() => {
                document.querySelectorAll('[id*="onetrust"], .onetrust-pc-dark-filter, .ot-fade-in, [class*="modal-backdrop"], [class*="cookie"], [class*="overlay"]').forEach(e => e.remove());
                if (document.body) { document.body.style.overflow = 'auto'; document.body.style.position = ''; }
                document.querySelectorAll('.modal-open').forEach(e => e.classList.remove('modal-open'));
            }""")
            await asyncio.sleep(0.3)

            # Use JS click directly — Playwright .click() fails when overlays intercept
            await page.evaluate("(id) => document.getElementById(id).click()", link_id)
            await asyncio.sleep(0.6)

            # Modal opens with #search_input
            search_input = page.locator("#search_input")
            await search_input.wait_for(state="visible", timeout=5000)
            await search_input.fill("")
            await asyncio.sleep(0.2)
            await search_input.press_sequentially(iata, delay=120)
            await asyncio.sleep(0.5)

            # Click matching airport link in the modal popup
            airport_link = page.locator(
                "modal-container a, .airport-lookup a"
            ).filter(has_text=iata).first
            await airport_link.click(timeout=5000)
            await asyncio.sleep(0.8)

            # Force-dismiss any lingering modal-container (prevents blocking trip type + date clicks)
            await page.evaluate("""() => {
                document.querySelectorAll('modal-container, .modal-backdrop, .modal.show').forEach(e => e.remove());
                document.querySelectorAll('.modal-open').forEach(e => e.classList.remove('modal-open'));
                if (document.body) { document.body.style.overflow = 'auto'; }
            }""")
            await asyncio.sleep(0.3)

            return True
        except Exception as e:
            logger.warning("Delta: classic airport '%s' error: %s", field_type, e)
            return False

    async def _fill_airport_modern(self, page, field_type: str, iata: str) -> bool:
        """Fill airport via the modern mach-route-picker component."""
        try:
            if field_type == "origin":
                inp = page.locator("mach-route-picker input").first
            else:
                inp = page.locator("mach-route-picker input").last
            
            await inp.click(timeout=5000)
            await asyncio.sleep(0.5)
            await inp.fill("")
            await asyncio.sleep(0.2)
            await inp.press_sequentially(iata, delay=100)
            await asyncio.sleep(0.5)

            opt = page.locator("[role='option']").filter(has_text=iata).first
            await opt.click(timeout=5000)
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("Delta: modern airport '%s' error: %s", field_type, e)
            return False

    async def _fill_airport_mach_core(self, page, field_type: str, iata: str) -> bool:
        """Fill airport via the EU/APAC mach-core button-based form."""
        try:
            if field_type == "origin":
                btn = page.locator('#one-way-route-picker-origin-button, [id*="route-picker-origin"]').first
            else:
                btn = page.locator('#one-way-route-picker-destination-button, [id*="route-picker-destination"]').first

            logger.info("Delta: mach_core airport '%s' clicking trigger button", field_type)
            await btn.scroll_into_view_if_needed(timeout=3000)
            await asyncio.sleep(0.3)
            try:
                await btn.click(timeout=3000)
            except Exception:
                logger.info("Delta: mach_core airport '%s' normal click failed, using force click", field_type)
                await btn.click(timeout=3000, force=True)
            await asyncio.sleep(1.0)

            # Diagnostic: dump what appeared after clicking the button
            post_click = await page.evaluate("""() => {
                // Find all visible inputs
                const inputs = Array.from(document.querySelectorAll('input')).filter(i => {
                    const rect = i.getBoundingClientRect();
                    const style = window.getComputedStyle(i);
                    return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden';
                }).map(i => ({
                    id: i.id, name: i.name, type: i.type,
                    placeholder: i.placeholder,
                    ariaLabel: i.getAttribute('aria-label'),
                    cls: i.className.slice(0, 80),
                    role: i.getAttribute('role'),
                }));
                // Find open dialogs/modals/dropdowns
                const overlays = Array.from(document.querySelectorAll(
                    '[role="dialog"], [role="listbox"], [role="combobox"], [class*="dropdown"], [class*="modal"], [class*="overlay"][style*="visible"], [class*="picker"][style*="block"], [class*="open"]'
                )).map(e => ({
                    tag: e.tagName, id: e.id, role: e.getAttribute('role'),
                    cls: e.className.slice(0, 80),
                    childCount: e.children.length,
                    text: e.innerText.slice(0, 200),
                }));
                // Find any text input or search box
                const searchLike = Array.from(document.querySelectorAll(
                    'input[type="text"], input[type="search"], input:not([type]), [contenteditable="true"]'
                )).filter(i => {
                    const rect = i.getBoundingClientRect();
                    return rect.width > 0 && rect.height > 0;
                }).map(i => ({
                    id: i.id, placeholder: i.placeholder, ariaLabel: i.getAttribute('aria-label'),
                    cls: i.className.slice(0, 80), tag: i.tagName,
                }));
                return {visibleInputs: inputs.length, inputs: inputs.slice(0, 10),
                        overlays: overlays.slice(0, 5), searchLike: searchLike.slice(0, 10)};
            }""")
            logger.info("Delta: mach_core post-click diagnostic: visibleInputs=%d searchLike=%d overlays=%d",
                       post_click.get('visibleInputs', 0), len(post_click.get('searchLike', [])),
                       len(post_click.get('overlays', [])))
            logger.info("Delta: mach_core searchLike inputs: %s", json.dumps(post_click.get('searchLike', []))[:500])
            logger.info("Delta: mach_core overlays: %s", json.dumps(post_click.get('overlays', []))[:500])

            # Try broad set of selectors for the search input
            search_input = None
            selectors = [
                '[class*="airport-search"] input',
                '[class*="route-picker"] input',
                'input[placeholder*="City"]',
                'input[placeholder*="Airport"]',
                'input[placeholder*="city"]',
                'input[placeholder*="airport"]',
                'input[placeholder*="Where"]',
                'input[placeholder*="Search"]',
                'input[placeholder*="search"]',
                'input[aria-label*="search" i]',
                'input[aria-label*="airport" i]',
                'input[aria-label*="origin" i]',
                'input[aria-label*="destination" i]',
                'input[aria-label*="city" i]',
                'input[aria-label*="from" i]',
                'input[aria-label*="to" i]',
                '[role="combobox"] input',
                '[role="searchbox"]',
                '[role="dialog"] input[type="text"]',
                '[role="dialog"] input:not([type="hidden"])',
            ]
            for sel in selectors:
                loc = page.locator(sel).first
                try:
                    if await loc.count() > 0 and await loc.is_visible(timeout=500):
                        search_input = loc
                        logger.info("Delta: mach_core found search input via: %s", sel)
                        break
                except Exception:
                    pass

            if search_input is None:
                # Try inside the airport-search-modal specifically
                modal_input = page.locator('.airport-search-modal input, .airport-search-modal [contenteditable], .airport-search-modal textarea').first
                try:
                    if await modal_input.count() > 0:
                        search_input = modal_input
                        logger.info("Delta: mach_core found input inside airport-search-modal")
                except Exception:
                    pass

            if search_input is None:
                # Deeper diagnostic: dump full modal HTML
                modal_html = await page.evaluate("""() => {
                    const m = document.querySelector('.airport-search-modal');
                    return m ? m.innerHTML.slice(0, 1000) : 'no modal';
                }""")
                logger.info("Delta: mach_core airport-search-modal HTML: %s", modal_html[:500])

                # Last resort: DON'T use footer search input (id contains 'footer')
                all_text = post_click.get('searchLike', [])
                for inp in all_text:
                    if 'footer' in (inp.get('id') or '').lower():
                        continue
                    if inp.get('id'):
                        search_input = page.locator(f"#{inp['id']}").first
                    elif inp.get('placeholder'):
                        search_input = page.locator(f"input[placeholder='{inp['placeholder']}']").first
                    logger.info("Delta: mach_core using non-footer fallback input: %s", inp)
                    break

            if search_input is None:
                logger.warning("Delta: mach_core no search input found after clicking '%s' button", field_type)
                return False

            logger.info("Delta: mach_core airport '%s' typing %s", field_type, iata)
            await search_input.fill("")
            await asyncio.sleep(0.2)
            await search_input.press_sequentially(iata, delay=120)
            await asyncio.sleep(1.0)

            # Click matching option
            opt = page.locator("[role='option'], [class*='airport-list'] button, [class*='suggestion'] button, li button, [role='listbox'] li").filter(has_text=iata).first
            try:
                await opt.wait_for(state="visible", timeout=5000)
                logger.info("Delta: mach_core airport '%s' clicking option", field_type)
                await opt.click(timeout=3000)
            except Exception:
                # Try clicking any list item that contains the IATA code
                clicked = await page.evaluate("""(code) => {
                    const items = document.querySelectorAll('li, [role="option"], button');
                    for (const item of items) {
                        if (item.textContent.includes(code) && item.offsetWidth > 0) {
                            item.click();
                            return 'clicked:' + item.tagName + ':' + item.textContent.trim().slice(0, 40);
                        }
                    }
                    return 'none';
                }""", iata)
                logger.info("Delta: mach_core airport '%s' JS click fallback: %s", field_type, clicked)
                if clicked == 'none':
                    return False
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("Delta: mach_core airport '%s' error: %s", field_type, e)
            return False

    async def _fill_date_mach_core(self, page, req: FlightSearchRequest) -> None:
        """Fill date via the EU/APAC mach-core date picker."""
        try:
            target = req.date_from
            target_month = target.strftime("%B")
            target_year = str(target.year)
            target_day = str(target.day)
            date_iso = target.strftime("%Y-%m-%d")
            logger.info("Delta: mach_core date fill: target=%s (month=%s day=%s)", target, target_month, target_day)

            # Diagnostic: find all date-related elements before clicking
            date_diag = await page.evaluate("""() => {
                const candidates = document.querySelectorAll(
                    '[id*="date"], [class*="date"], [aria-label*="date" i], [aria-label*="depart" i], [class*="calendar"], input[type="date"]'
                );
                return Array.from(candidates).slice(0, 15).map(e => ({
                    tag: e.tagName, id: e.id, cls: e.className.slice(0, 60),
                    role: e.getAttribute('role'), ariaLabel: e.getAttribute('aria-label'),
                    text: e.textContent.trim().slice(0, 50), type: e.type || '',
                }));
            }""")
            logger.info("Delta: mach_core date candidates: %s", json.dumps(date_diag)[:600])

            # Try multiple selectors for the date picker trigger
            date_opened = False
            date_selectors = [
                '[id*="date-picker-trigger"]',
                '[class*="date-picker"] button',
                '[id*="date-input"]',
                '[aria-label*="Depart" i]',
                '[aria-label*="departure" i]',
                '[class*="date-field"]',
                'button[class*="date"]',
                '[class*="calendar-trigger"]',
                'input[type="date"]',
            ]
            for sel in date_selectors:
                loc = page.locator(sel).first
                try:
                    if await loc.count() > 0 and await loc.is_visible(timeout=500):
                        await loc.click(timeout=3000)
                        date_opened = True
                        logger.info("Delta: mach_core date opened via: %s", sel)
                        break
                except Exception:
                    pass

            if not date_opened:
                # JS fallback: click any element with "date" in id/class that looks interactive
                js_result = await page.evaluate("""() => {
                    const els = document.querySelectorAll('[id*="date"], [class*="date"]');
                    for (const el of els) {
                        if ((el.tagName === 'BUTTON' || el.tagName === 'INPUT' || el.getAttribute('role') === 'button') && el.offsetWidth > 0) {
                            el.click();
                            return 'js-clicked:' + el.tagName + '#' + el.id + '.' + el.className.slice(0, 40);
                        }
                    }
                    return 'none';
                }""")
                logger.info("Delta: mach_core date JS fallback: %s", js_result)
                if js_result != 'none':
                    date_opened = True

            if not date_opened:
                logger.warning("Delta: mach_core could not open date picker")
                # Try setting date directly via JS as last resort
                await page.evaluate("""(dateStr) => {
                    const inputs = document.querySelectorAll('input[type="date"], input[name*="date" i], input[id*="date" i]');
                    for (const inp of inputs) {
                        inp.value = dateStr;
                        inp.dispatchEvent(new Event('input', {bubbles: true}));
                        inp.dispatchEvent(new Event('change', {bubbles: true}));
                    }
                }""", date_iso)
                return

            await asyncio.sleep(0.8)

            # Navigate to the correct month — check ONLY header/title elements,
            # not day button aria-labels which contain month names too
            for i in range(12):
                month_header = await page.evaluate("""(args) => {
                    const targetMonth = args.targetMonth;
                    const targetYear = args.targetYear;
                    // Only check actual calendar header/title elements, NOT day buttons
                    const headers = document.querySelectorAll(
                        '[class*="calendar"] [class*="month"], [class*="date-picker"] h2, ' +
                        '[class*="date-picker"] [class*="title"], [class*="calendar-header"], ' +
                        '[class*="month-year"], [class*="calendar"] caption, ' +
                        '[class*="calendar"] [class*="heading"], [role="heading"]'
                    );
                    for (const h of headers) {
                        const t = h.textContent.trim();
                        if (t.includes(targetMonth) && t.includes(targetYear)) return 'header:' + t.slice(0, 40);
                    }
                    // Also check if the target day button is visible (offsetWidth > 0)
                    const dayBtns = document.querySelectorAll('button.date-button, button[role="gridcell"]');
                    for (const b of dayBtns) {
                        const label = b.getAttribute('aria-label') || '';
                        if (label.includes(targetMonth) && label.includes(String(args.day)) && label.includes(targetYear) && b.offsetWidth > 0) {
                            return 'visible-day:' + label;
                        }
                    }
                    return false;
                }""", {"targetMonth": target_month, "targetYear": target_year, "day": target.day})

                if month_header:
                    logger.info("Delta: mach_core date found target month at step %d: %s", i, month_header)
                    break
                next_btn = page.locator("button[aria-label*='next' i], button[aria-label*='Next'], [class*='next'], button[aria-label*='forward' i]").first
                if await next_btn.count() > 0:
                    await next_btn.click(timeout=2000)
                    await asyncio.sleep(0.5)
                else:
                    break

            # Click the target day using the BUTTON.date-button inside the calendar
            # The calendar shows multiple months; each day button has aria-label like "June 15, 2026"
            target_label_us = target.strftime("%B %d, %Y")  # "June 15, 2026" — US format in aria-label
            target_label_uk = f"{target.day} {target.strftime('%B')} {target.year}"  # "15 June 2026"

            # Diagnostic: dump calendar structure for the target day
            cal_diag = await page.evaluate("""(args) => {
                const day = args.day;
                const month = args.month;
                const year = args.year;
                // Find all day buttons
                const btns = document.querySelectorAll('button.date-button, button[role="gridcell"], [class*="date-picker"] button');
                const matches = [];
                for (const b of btns) {
                    const label = b.getAttribute('aria-label') || '';
                    if (label.includes(month) && label.includes(String(day)) && label.includes(year)) {
                        matches.push({
                            tag: b.tagName, cls: b.className.slice(0, 60),
                            ariaLabel: label, disabled: b.disabled,
                            text: b.textContent.trim(), w: b.offsetWidth,
                        });
                    }
                }
                return matches;
            }""", {"day": target.day, "month": target_month, "year": target_year})
            logger.info("Delta: mach_core target day buttons: %s", json.dumps(cal_diag)[:500])

            day_clicked = False

            # Primary: click button by aria-label containing the target date
            for label in [target_label_us, target_label_uk]:
                loc = page.locator(f"button[aria-label*='{label}']").first
                try:
                    if await loc.count() > 0:
                        await loc.click(timeout=3000)
                        day_clicked = True
                        logger.info("Delta: mach_core date day clicked via button aria-label: %s", label)
                        break
                except Exception:
                    pass

            if not day_clicked:
                # Fallback: click via JS using full aria-label match with month
                # Try WITHOUT the offsetWidth > 0 guard since some calendars hide buttons
                js_day = await page.evaluate("""(args) => {
                    const month = args.month;
                    const day = String(args.day);
                    const year = args.year;
                    const btns = document.querySelectorAll('button.date-button, button[role="gridcell"]');
                    for (const b of btns) {
                        const label = b.getAttribute('aria-label') || '';
                        if (label.includes(month) && label.includes(day) && label.includes(year)) {
                            // Scroll into view first if hidden
                            b.scrollIntoView({block: 'center'});
                            // Dispatch full mouse event sequence
                            const rect = b.getBoundingClientRect();
                            const x = rect.left + rect.width/2;
                            const y = rect.top + rect.height/2;
                            const opts = {bubbles: true, cancelable: true, clientX: x, clientY: y, button: 0};
                            b.dispatchEvent(new PointerEvent('pointerdown', opts));
                            b.dispatchEvent(new MouseEvent('mousedown', opts));
                            b.dispatchEvent(new PointerEvent('pointerup', opts));
                            b.dispatchEvent(new MouseEvent('mouseup', opts));
                            b.dispatchEvent(new MouseEvent('click', opts));
                            return 'dispatched:' + b.getAttribute('aria-label');
                        }
                    }
                    return 'none';
                }""", {"month": target_month, "day": target.day, "year": target_year})
                logger.info("Delta: mach_core date JS targeted day: %s", js_day)

            # Third fallback: Playwright force-click (works even on hidden elements)
            if not day_clicked:
                for label in [target_label_us, target_label_uk]:
                    loc = page.locator(f"button[aria-label*='{label}']").first
                    try:
                        if await loc.count() > 0:
                            await loc.click(timeout=3000, force=True)
                            day_clicked = True
                            logger.info("Delta: mach_core date day force-clicked via: %s", label)
                            break
                    except Exception as e:
                        logger.info("Delta: mach_core date force-click failed: %s", e)

            await asyncio.sleep(0.5)

            # Verify date was set
            date_verify = await page.evaluate("""() => {
                const trigger = document.querySelector('[id*="date-picker-trigger"]');
                return trigger ? trigger.textContent.trim().slice(0, 40) : 'no-trigger';
            }""")
            logger.info("Delta: mach_core date verify after day click: %s", date_verify)

            # If date still shows placeholder, the calendar may need Done/Apply FIRST
            # or the day click didn't register — try clicking Done then re-checking
            if "Depart" in date_verify or date_verify == "no-trigger":
                logger.info("Delta: mach_core date NOT set, trying Done button then re-check")
                done_btn = page.locator('button:has-text("Done"), button:has-text("Apply"), button:has-text("Select")').first
                if await done_btn.count() > 0:
                    try:
                        await done_btn.click(timeout=2000)
                        await asyncio.sleep(0.5)
                    except Exception:
                        pass

                # Re-check date
                date_verify2 = await page.evaluate("""() => {
                    const trigger = document.querySelector('[id*="date-picker-trigger"]');
                    return trigger ? trigger.textContent.trim().slice(0, 40) : 'no-trigger';
                }""")
                logger.info("Delta: mach_core date verify after Done: %s", date_verify2)

                # If STILL not set, dump the full calendar DOM structure for debugging
                if "Depart" in date_verify2 or date_verify2 == "no-trigger":
                    cal_dump = await page.evaluate("""() => {
                        const cal = document.querySelector('[class*="calendar"], [class*="date-picker"]');
                        if (!cal) return 'no-calendar-found';
                        // Get the calendar's HTML structure summary
                        const headings = Array.from(cal.querySelectorAll('h1,h2,h3,h4,h5,h6,[role="heading"],caption,[class*="title"],[class*="month"]'))
                            .map(h => h.textContent.trim().slice(0, 50));
                        const visibleBtns = Array.from(cal.querySelectorAll('button'))
                            .filter(b => b.offsetWidth > 0)
                            .slice(0, 5)
                            .map(b => ({text: b.textContent.trim().slice(0, 30), aria: (b.getAttribute('aria-label')||'').slice(0, 40)}));
                        return JSON.stringify({headings, visibleBtns});
                    }""")
                    logger.warning("Delta: mach_core calendar dump: %s", str(cal_dump)[:400])
            else:
                # Click Done/Apply if present
                done_btn = page.locator('button:has-text("Done"), button:has-text("Apply"), button:has-text("Select")').first
                if await done_btn.count() > 0 and await done_btn.is_visible():
                    await done_btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning("Delta: mach_core date error: %s", e)

    async def _fill_date_classic(self, page, req: FlightSearchRequest) -> None:
        """Fill date using the classic dl-datepicker calendar, with JS fallback."""
        try:
            target = req.date_from
            target_month = target.strftime("%B")   # e.g. "June"
            target_year = str(target.year)          # e.g. "2026"
            target_day = str(target.day)            # e.g. "15"
            # Formatted date string for input value (MM/DD/YYYY)
            date_display = target.strftime("%m/%d/%Y")

            # Open the calendar via JS click (Playwright click blocked by overlays/modals)
            await page.evaluate("""() => {
                const el = document.querySelector('#input_departureDate_1');
                if (el) el.click();
            }""")
            await asyncio.sleep(1.0)

            # Check if calendar opened
            has_calendar = await page.evaluate(
                "() => document.querySelectorAll('.dl-datepicker-calendar-cont').length"
            )
            logger.warning("Delta: calendar containers found: %d", has_calendar)

            if has_calendar > 0:
                # Diagnostic: dump calendar container content
                cal_diag = await page.evaluate("""() => {
                    const cals = document.querySelectorAll('.dl-datepicker-calendar-cont');
                    return Array.from(cals).map((c, i) => ({
                        idx: i,
                        innerHTML_len: c.innerHTML.length,
                        innerText: c.innerText.slice(0, 200),
                        childCount: c.children.length,
                        tagNames: Array.from(c.querySelectorAll('*')).slice(0, 20).map(e => e.tagName),
                    }));
                }""")
                logger.warning("Delta: calendar diagnostic: %s", json.dumps(cal_diag)[:500])
                # Navigate to the correct month
                # Month header is NOT inside .dl-datepicker-calendar-cont
                # Search the entire datepicker widget for month/year text
                found_month = False
                for i in range(12):
                    month_text = await page.evaluate("""() => {
                        const dp = document.querySelector('.dl-datepicker')
                                || document.querySelector('[class*="datepicker"]')
                                || document.body;
                        const all = dp.querySelectorAll('*');
                        const monthNames = ['January','February','March','April','May','June',
                                          'July','August','September','October','November','December'];
                        const found = [];
                        for (const el of all) {
                            if (el.children.length > 3) continue;
                            const t = el.textContent.trim();
                            if (t.length > 3 && t.length < 30) {
                                for (const m of monthNames) {
                                    if (t.includes(m) && /\\d{4}/.test(t)) {
                                        found.push(t);
                                        break;
                                    }
                                }
                            }
                        }
                        return found;
                    }""")
                    if i == 0:
                        logger.warning("Delta: calendar month texts=%s", month_text)
                    else:
                        logger.info("Delta: calendar nav step %d: month texts=%s", i, month_text)
                    # Match when target month is the FIRST (left) calendar — click day in first container
                    if month_text and target_month in month_text[0] and target_year in month_text[0]:
                        found_month = True
                        break
                    # Use JS click for next arrow — Playwright locator times out on this element
                    nav_ok = await page.evaluate("""() => {
                        const selectors = [
                            '.dl-datepicker-next',
                            'a.dl-datepicker-next',
                            '[class*="datepicker-next"]',
                            '[class*="datepicker"] [aria-label*="Next"]',
                            '[class*="datepicker"] [aria-label*="next"]',
                            '[class*="datepicker"] .next',
                            '[class*="calendar-nav"] button:last-child',
                        ];
                        for (const sel of selectors) {
                            const el = document.querySelector(sel);
                            if (el) { el.click(); return 'clicked:' + sel; }
                        }
                        // Fallback: find any clickable element with "next" or ">" text near the calendar
                        const dp = document.querySelector('.dl-datepicker') || document.querySelector('[class*="datepicker"]');
                        if (dp) {
                            const btns = dp.querySelectorAll('a, button, [role="button"]');
                            for (const b of btns) {
                                const t = b.textContent.trim();
                                const label = b.getAttribute('aria-label') || '';
                                if (t === '>' || t === '›' || t === '→' || t === 'Next'
                                    || label.toLowerCase().includes('next')
                                    || b.className.includes('next') || b.className.includes('forward')) {
                                    b.click();
                                    return 'clicked-text:' + (t || label).slice(0, 20);
                                }
                            }
                        }
                        return 'no-next-found';
                    }""")
                    logger.info("Delta: calendar next arrow: %s", nav_ok)
                    if nav_ok == "no-next-found":
                        break
                    await asyncio.sleep(0.5)

                if found_month:
                    # Click the target day in the FIRST container (left calendar = target month)
                    clicked = await page.evaluate("""(tDay) => {
                        const cal = document.querySelector('.dl-datepicker-calendar-cont');
                        if (!cal) return false;
                        const tds = cal.querySelectorAll('td');
                        for (const td of tds) {
                            if (td.textContent.trim() === tDay
                                && !td.classList.contains('dl-datepicker-other-month')
                                && !td.classList.contains('dl-datepicker-disabled')) {
                                const a = td.querySelector('a');
                                (a || td).click();
                                return true;
                            }
                        }
                        return false;
                    }""", target_day)
                    logger.info("Delta: calendar day %s click=%s", target_day, clicked)
                    await asyncio.sleep(0.5)

                    done_btn = page.locator('button:has-text("Done")').first
                    if await done_btn.count() > 0 and await done_btn.is_visible():
                        await done_btn.click(timeout=2000)
                        await asyncio.sleep(0.3)

                    # Verify date was actually set
                    dep_val = await page.evaluate("""() => {
                        const inp = document.querySelector('#input_departureDate_1');
                        return inp ? inp.value : null;
                    }""")
                    if dep_val:
                        logger.info("Delta: calendar date set OK: %s", dep_val)
                        return
                    else:
                        logger.warning("Delta: calendar day clicked but input still empty, falling through to JS")

            # Fallback: set date value directly via JS (calendar nav failed or didn't open)
            logger.warning("Delta: setting date via JS fallback: %s", date_display)
            await page.evaluate("""(dateStr) => {
                // Close any open calendar first
                document.querySelectorAll('.dl-datepicker').forEach(e => e.remove());

                const inp = document.querySelector('#input_departureDate_1');
                if (inp) {
                    // Set the native input value via property descriptor to trigger Angular
                    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                        window.HTMLInputElement.prototype, 'value').set;
                    nativeInputValueSetter.call(inp, dateStr);
                    inp.dispatchEvent(new Event('input', {bubbles: true}));
                    inp.dispatchEvent(new Event('change', {bubbles: true}));
                    inp.dispatchEvent(new Event('blur', {bubbles: true}));
                }
                // Also try hidden model input
                const hidden = document.querySelector('input[name="departureDate"]');
                if (hidden) {
                    const setter = Object.getOwnPropertyDescriptor(
                        window.HTMLInputElement.prototype, 'value').set;
                    setter.call(hidden, dateStr);
                    hidden.dispatchEvent(new Event('input', {bubbles: true}));
                    hidden.dispatchEvent(new Event('change', {bubbles: true}));
                }
                // Try Angular model update
                try {
                    const el = document.querySelector('#input_departureDate_1');
                    if (el && window.angular) {
                        const scope = window.angular.element(el).scope();
                        if (scope) {
                            scope.$apply(() => {
                                scope.departureDate = dateStr;
                            });
                        }
                    }
                } catch(e) {}
            }""", date_display)
            await asyncio.sleep(0.5)

            dep_verify = await page.evaluate("""() => {
                const inp = document.querySelector('#input_departureDate_1');
                return inp ? inp.value : null;
            }""")
            logger.info("Delta: JS fallback date verify: %s", dep_verify)

        except Exception as e:
            logger.warning("Delta: classic date error: %s", e)
            # Last resort: try JS date set even after exception
            try:
                date_display = req.date_from.strftime("%m/%d/%Y")
                await page.evaluate("""(dateStr) => {
                    const inp = document.querySelector('#input_departureDate_1');
                    if (inp) {
                        const setter = Object.getOwnPropertyDescriptor(
                            window.HTMLInputElement.prototype, 'value').set;
                        setter.call(inp, dateStr);
                        inp.dispatchEvent(new Event('input', {bubbles: true}));
                        inp.dispatchEvent(new Event('change', {bubbles: true}));
                        inp.dispatchEvent(new Event('blur', {bubbles: true}));
                    }
                }""", date_display)
                logger.info("Delta: last-resort JS date set: %s", date_display)
            except Exception:
                pass

    async def _fill_date_modern(self, page, req: FlightSearchRequest) -> None:
        """Fill date via the modern mach-date-picker component."""
        try:
            target = req.date_from
            try:
                target_label = target.strftime("%#d %B %Y")  # Windows
            except ValueError:
                target_label = target.strftime("%-d %B %Y")  # Unix

            date_trigger = page.locator("mach-date-picker").first
            await date_trigger.click(timeout=5000)
            await asyncio.sleep(0.5)

            for _ in range(12):
                day_btn = page.locator(f"[aria-label*='{target_label}']").first
                if await day_btn.count() > 0 and await day_btn.is_visible():
                    await day_btn.click(timeout=3000)
                    await asyncio.sleep(0.3)
                    done_btn = page.locator('button:has-text("Done")').first
                    if await done_btn.count() > 0:
                        await done_btn.click(timeout=2000)
                    return
                next_btn = page.locator("button[aria-label*='next' i], button[aria-label*='Next']").first
                if await next_btn.count() > 0:
                    await next_btn.click(timeout=2000)
                    await asyncio.sleep(0.5)
                else:
                    break
        except Exception as e:
            logger.warning("Delta: modern date error: %s", e)

    # -- DOM scraping fallback (when offer-api is 429'd by Kasada) -----

    async def _scrape_results_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Try to extract flight offers from the results page DOM when the API is blocked."""
        try:
            # Check for embedded state data in the page (window globals, script tags, Angular scope)
            embedded = await page.evaluate("""() => {
                // Check well-known globals
                if (window.__NEXT_DATA__) return {source: 'nextData', data: JSON.stringify(window.__NEXT_DATA__).slice(0, 10000)};
                if (window.__INITIAL_STATE__) return {source: 'initialState', data: JSON.stringify(window.__INITIAL_STATE__).slice(0, 10000)};
                for (const key of ['spaData', 'searchData', 'pageData', '__APP_DATA__', 'shopPage',
                                   'flightSearchResults', 'searchResults', 'deltaData', 'appData']) {
                    if (window[key]) return {source: key, data: JSON.stringify(window[key]).slice(0, 10000)};
                }
                // Scan ALL window keys for objects containing flight-related data
                for (const key of Object.keys(window)) {
                    try {
                        const val = window[key];
                        if (val && typeof val === 'object' && !Array.isArray(val) && !(val instanceof HTMLElement)) {
                            const str = JSON.stringify(val);
                            if (str && str.length > 500 && (str.includes('gqlSearchOffers') || str.includes('flightOffer')
                                || str.includes('tripId') || str.includes('offerItems') || str.includes('flightSegment'))) {
                                return {source: 'window.' + key, data: str.slice(0, 10000)};
                            }
                        }
                    } catch(e) {}
                }
                // Check Angular scope data
                try {
                    const ngEls = document.querySelectorAll('[ng-controller], [data-ng-controller], [ng-app]');
                    for (const el of ngEls) {
                        const scope = window.angular && window.angular.element(el).scope();
                        if (scope) {
                            for (const k of Object.keys(scope)) {
                                if (k.startsWith('$')) continue;
                                try {
                                    const v = scope[k];
                                    if (v && typeof v === 'object') {
                                        const s = JSON.stringify(v);
                                        if (s && (s.includes('offer') || s.includes('flight') || s.includes('price'))) {
                                            return {source: 'angularScope.' + k, data: s.slice(0, 10000)};
                                        }
                                    }
                                } catch(e) {}
                            }
                        }
                    }
                } catch(e) {}
                // Check script tags for embedded JSON
                const scripts = document.querySelectorAll('script:not([src])');
                for (const s of scripts) {
                    const t = s.textContent;
                    if (t && (t.includes('gqlSearchOffers') || t.includes('flightOffer') || t.includes('tripId'))) {
                        return {source: 'inlineScript', data: t.slice(0, 10000)};
                    }
                }
                return null;
            }""")
            if embedded:
                logger.info("Delta: found embedded data source=%s len=%d preview=%s",
                           embedded.get('source'), len(embedded.get('data', '')),
                           embedded.get('data', '')[:200])
                data_str = embedded.get('data', '')
                if any(k in data_str for k in ('gqlSearchOffers', 'flightOffer', 'tripId', 'offerItems')):
                    try:
                        data = json.loads(data_str)
                        offers = self._parse_response(data, req)
                        if offers:
                            logger.info("Delta: parsed %d offers from embedded data", len(offers))
                            return offers
                    except Exception:
                        pass

            # Try DOM scraping of visible flight cards
            cards = await page.evaluate("""() => {
                const results = [];
                // Delta uses various card selectors — try broad set
                const cardSels = [
                    '.flight-card', '[class*="flight-result"]', '[class*="FlightResult"]',
                    '[class*="itinerary-card"]', '[class*="ItineraryCard"]',
                    '.flight-listing', '[data-testid*="flight"]',
                    '[class*="offer-card"]', '[class*="OfferCard"]',
                    '[class*="FlightSelection"]', '[class*="flight-selection"]',
                    '[class*="result-card"]', '[class*="ResultCard"]',
                    '[class*="shopCard"]', '[class*="shop-card"]',
                    '[class*="slice-card"]', '[class*="SliceCard"]',
                    'app-results-list > div', '[class*="fare-card"]',
                ];
                let usedSel = 'none';
                let cards = [];
                for (const sel of cardSels) {
                    const found = document.querySelectorAll(sel);
                    if (found.length > 0) {
                        cards = found;
                        usedSel = sel + ':' + found.length;
                        break;
                    }
                }
                // Fallback: find any element containing a dollar price + time pattern
                if (cards.length === 0) {
                    const allDivs = document.querySelectorAll('div, li, section, article');
                    const pricePattern = /\\$\\d{2,}/;
                    const timePattern = /\\d{1,2}:\\d{2}/;
                    const candidates = [];
                    for (const div of allDivs) {
                        const t = div.innerText || '';
                        if (t.length > 30 && t.length < 800 && pricePattern.test(t) && timePattern.test(t)
                            && (t.includes('AM') || t.includes('PM') || t.includes('am') || t.includes('pm'))) {
                            // Avoid parent containers — prefer leaf-ish elements
                            const children = div.querySelectorAll('div, li, section, article');
                            let isLeaf = true;
                            for (const child of children) {
                                if (pricePattern.test(child.innerText || '') && timePattern.test(child.innerText || '')) {
                                    isLeaf = false;
                                    break;
                                }
                            }
                            if (isLeaf) candidates.push(div);
                        }
                    }
                    if (candidates.length > 0) {
                        cards = candidates;
                        usedSel = 'heuristic:' + candidates.length;
                    }
                }
                for (const card of cards) {
                    const text = card.innerText;
                    // Extract price ($NNN or $N,NNN)
                    const priceMatch = text.match(/\\$(\\d[\\d,]*)/);
                    const price = priceMatch ? parseFloat(priceMatch[1].replace(/,/g, '')) : null;
                    // Extract times (HH:MM AM/PM)
                    const timeMatch = text.match(/(\\d{1,2}:\\d{2}\\s*[AaPp][Mm])/g);
                    // Extract duration
                    const durMatch = text.match(/(\\d+)h\\s*(\\d+)?m?/);
                    // Extract stops
                    const nonstop = /nonstop|non-stop|direct/i.test(text);
                    const stopsMatch = text.match(/(\\d+)\\s*stop/i);
                    // Extract flight number
                    const flightMatch = text.match(/DL\\s*(\\d{1,4})/);
                    results.push({
                        text: text.slice(0, 400),
                        price, times: timeMatch || [], duration: durMatch ? durMatch[0] : null,
                        stops: nonstop ? 0 : (stopsMatch ? parseInt(stopsMatch[1]) : null),
                        flightNo: flightMatch ? 'DL' + flightMatch[1] : null,
                        cls: card.className ? card.className.slice(0, 80) : '',
                    });
                }
                return {count: cards.length, selector: usedSel, results: results.slice(0, 30)};
            }""")
            logger.info("Delta: DOM scrape: %d cards via %s", cards.get('count', 0), cards.get('selector', '?'))

            if cards.get('count', 0) > 0:
                offers = []
                for i, card in enumerate(cards.get('results', [])):
                    if card.get('price') is None:
                        continue
                    try:
                        dep_time = card['times'][0] if len(card.get('times', [])) >= 1 else ""
                        arr_time = card['times'][1] if len(card.get('times', [])) >= 2 else ""
                        dur_secs = 0
                        if card.get('duration'):
                            import re as _re
                            dm = _re.match(r'(\d+)h\s*(\d+)?', card['duration'])
                            if dm:
                                dur_secs = int(dm.group(1)) * 3600 + (int(dm.group(2) or 0)) * 60
                        flight_no = card.get('flightNo') or "DL???"
                        dep_str = f"{req.date_from}T{dep_time}" if dep_time else str(req.date_from)
                        arr_str = f"{req.date_from}T{arr_time}" if arr_time else str(req.date_from)
                        offer = FlightOffer(
                            id=f"delta-dom-{i}",
                            price=card['price'],
                            currency="USD",
                            outbound=FlightRoute(
                                segments=[FlightSegment(
                                    airline="DL",
                                    flight_no=flight_no,
                                    origin=req.origin,
                                    destination=req.destination,
                                    departure=dep_str,
                                    arrival=arr_str,
                                )],
                                total_duration_seconds=dur_secs,
                                stopovers=card.get('stops', 0) or 0,
                            ),
                            airlines=[AirlineSummary(code="DL", name="Delta Air Lines")],
                            booking_url=f"https://www.delta.com/flight-search/search-results?departureDate={req.date_from}&originCity={req.origin}&destinationCity={req.destination}",
                        )
                        offers.append(offer)
                    except Exception as e:
                        logger.warning("Delta: DOM offer parse error: %s", e)
                if offers:
                    logger.info("Delta: DOM scrape produced %d offers", len(offers))
                    return offers

            return []
        except Exception as e:
            logger.warning("Delta: DOM scrape error: %s", e)
            return []

    # -- Response parsing (unchanged from SDK) -------------------------

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        try:
            search_offers = data.get("data", {}).get("gqlSearchOffers", {})
            offer_sets = search_offers.get("gqlOffersSets", [])
        except Exception:
            return offers
        if not offer_sets:
            return offers
        for set_idx, offer_set in enumerate(offer_sets):
            trips = offer_set.get("trips", [])
            raw_offers = offer_set.get("offers", [])
            if not trips or not raw_offers:
                continue
            trip = trips[0]
            cheapest = self._find_cheapest_offer(raw_offers)
            if cheapest is None:
                continue
            offer = self._build_offer(trip, cheapest, req, set_idx)
            if offer:
                offers.append(offer)
        return offers

    def _find_cheapest_offer(self, raw_offers: list[dict]) -> Optional[dict]:
        best = None
        for raw_offer in raw_offers:
            try:
                props = raw_offer.get("additionalOfferProperties", {})
                if props.get("soldOut") or raw_offer.get("soldOut"):
                    continue
                if not props.get("offered", True):
                    continue
                price = self._extract_price(raw_offer)
                if price is None:
                    continue
                brand_id = props.get("dominantSegmentBrandId", "")
                entry = {"offer_id": raw_offer.get("offerId", ""), "brand_id": brand_id, "price": float(price), "currency": "USD", "refundable": props.get("refundable", False)}
                if best is None or entry["price"] < best["price"]:
                    best = entry
            except Exception:
                continue
        return best

    def _extract_price(self, raw_offer: dict) -> Optional[float]:
        items = raw_offer.get("offerItems", [])
        if not items:
            return None
        retail = items[0].get("retailItems", [])
        if not retail:
            return None
        meta = retail[0].get("retailItemMetaData", {})
        fare_info = meta.get("fareInformation", [])
        if not fare_info:
            return None
        fare_prices = fare_info[0].get("farePrice", [])
        if not fare_prices:
            return None
        total = fare_prices[0].get("totalFarePrice", {})
        curr_price = total.get("currencyEquivalentPrice", {})
        price = curr_price.get("roundedCurrencyAmt")
        formatted = curr_price.get("formattedCurrencyAmt")
        if formatted and "." in str(formatted):
            try:
                return float(formatted)
            except (ValueError, TypeError):
                pass
        return float(price) if price is not None else None

    def _build_offer(self, trip: dict, cheapest: dict, req: FlightSearchRequest, idx: int) -> Optional[FlightOffer]:
        try:
            segments: list[FlightSegment] = []
            for seg in trip.get("flightSegment", []):
                mkt = seg.get("marketingCarrier", {})
                oper = seg.get("operatingCarrier", {})
                carrier_code = mkt.get("carrierCode", "DL")
                flight_num = str(mkt.get("carrierNum", ""))
                legs = seg.get("flightLeg", [])
                duration_secs = 0
                aircraft = ""
                if legs:
                    dom_leg = legs[0]
                    dur = dom_leg.get("duration", {})
                    duration_secs = dur.get("dayCnt", 0) * 86400 + dur.get("hourCnt", 0) * 3600 + dur.get("minuteCnt", 0) * 60
                    ac = dom_leg.get("aircraft", {})
                    aircraft = ac.get("fleetTypeCode", "") or ac.get("subFleetTypeCode", "")
                dep_ts = seg.get("scheduledDepartureLocalTs", "")
                arr_ts = seg.get("scheduledArrivalLocalTs", "")
                segments.append(FlightSegment(
                    airline=carrier_code, airline_name=oper.get("carrierName") or _carrier_name(carrier_code),
                    flight_no=flight_num, origin=seg.get("originAirportCode", ""),
                    destination=seg.get("destinationAirportCode", trip.get("destinationAirportCode", "")),
                    departure=_parse_dt(dep_ts), arrival=_parse_dt(arr_ts),
                    duration_seconds=duration_secs, aircraft=aircraft,
                ))
            if not segments:
                return None
            total_time = trip.get("totalTripTime", {})
            total_secs = total_time.get("dayCnt", 0) * 86400 + total_time.get("hourCnt", 0) * 3600 + total_time.get("minuteCnt", 0) * 60
            if total_secs == 0:
                total_secs = sum(s.duration_seconds for s in segments)
            outbound = FlightRoute(segments=segments, total_duration_seconds=total_secs, stopovers=max(trip.get("stopCnt", 0), len(segments) - 1))
            airlines = list(dict.fromkeys(s.airline for s in segments if s.airline))
            price = cheapest["price"]
            currency = cheapest.get("currency", "USD")
            offer_id = hashlib.md5(f"DL-{trip.get('tripId', idx)}-{req.date_from}-{price}".encode()).hexdigest()[:16]
            return FlightOffer(
                id=f"dl-{offer_id}", price=price, currency=currency,
                price_formatted=f"${price:.2f}" if currency == "USD" else f"{price:.2f} {currency}",
                outbound=outbound, airlines=airlines, owner_airline="DL",
                source="delta_direct", source_tier="protocol", is_locked=True,
                booking_url=f"https://www.delta.com/flight-search/search-results?tripType={'ROUND_TRIP' if req.return_from else 'ONE_WAY'}&action=findFlights&originCity={req.origin}&destinationCity={req.destination}&departureDate={req.date_from}&paxCount=1&currencyCode={req.currency or 'USD'}",
            )
        except Exception as e:
            logger.debug("Delta: failed to build offer: %s", e)
            return None

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        by_airline: dict[str, list[FlightOffer]] = defaultdict(list)
        for o in offers:
            by_airline[o.owner_airline or (o.airlines[0] if o.airlines else "DL")].append(o)
        airlines_summary = [
            AirlineSummary(airline_code=code, airline_name=_carrier_name(code), cheapest_price=min(al, key=lambda o: o.price).price, currency=min(al, key=lambda o: o.price).currency, offer_count=len(al), cheapest_offer_id=min(al, key=lambda o: o.price).id, sample_route=f"{req.origin}->{req.destination}")
            for code, al in by_airline.items()
        ]
        logger.info("Delta: %d offers for %s->%s on %s (%.1fs)", len(offers), req.origin, req.destination, req.date_from, elapsed)
        return FlightSearchResponse(
            search_id=hashlib.md5(f"dl-{req.origin}-{req.destination}-{req.date_from}-{time.time()}".encode()).hexdigest()[:12],
            origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers[:req.limit], total_results=len(offers),
            airlines_summary=airlines_summary,
            search_params={"source": "delta_direct", "method": "patchright_form_fill_graphql_intercept", "elapsed": round(elapsed, 2)},
            source_tiers={"protocol": "Delta Air Lines direct (delta.com)"},
        )

    @staticmethod
    def _combine_rt(ob, ib, req):
        combos = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(id=f"rt_delt_{cid}", price=price, currency=o.currency, outbound=o.outbound, inbound=i.outbound, airlines=list(dict.fromkeys(o.airlines + i.airlines)), owner_airline=o.owner_airline, booking_url=o.booking_url, is_locked=False, source=o.source, source_tier=o.source_tier))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req):
        return FlightSearchResponse(origin=req.origin, destination=req.destination, currency="USD", offers=[], total_results=0, search_params={"source": "delta_direct", "error": "no_results"}, source_tiers={"protocol": "Delta Air Lines direct (delta.com)"})


def _parse_dt(s: str) -> str:
    if not s:
        return ""
    try:
        return datetime.fromisoformat(s).isoformat()
    except (ValueError, TypeError):
        return s

def _carrier_name(code: str) -> str:
    return {"DL": "Delta Air Lines", "AA": "American Airlines", "UA": "United Airlines", "WN": "Southwest Airlines", "AS": "Alaska Airlines", "B6": "JetBlue", "NK": "Spirit Airlines", "F9": "Frontier Airlines", "AF": "Air France", "KL": "KLM", "VS": "Virgin Atlantic", "KE": "Korean Air", "LA": "LATAM Airlines", "AM": "Aeromexico"}.get(code, code)