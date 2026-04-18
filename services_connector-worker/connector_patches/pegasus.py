"""
Pegasus Airlines patchright connector -- Cloud Run patch.

Replaces CDP Chrome with Patchright headed browser to bypass Akamai Bot Manager.
URL navigation + availability API interception logic preserved from SDK.
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

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 2
_RESULTS_WAIT = 30

_TURKEY_AIRPORTS = {"IST", "SAW", "ESB", "AYT", "ADB", "ADA", "TZX", "BJV", "DLM", "GZT", "VAN", "ERZ", "DIY", "SZF", "KYA", "ASR", "EZS", "MLX", "NAV", "HTY", "ECN", "KSY", "MZH", "BAL", "CKZ", "NOP", "GNY", "OGU", "YEI", "IGD", "NKT", "MSR", "TJK", "USQ", "AOE", "EDO", "DNZ", "SFQ", "BZI", "BDM"}


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
            logger.info("Pegasus: using proxy relay on port 8899")
        except OSError:
            from urllib.parse import urlparse
            p = urlparse(letsfg_proxy)
            proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}", "bypass": _BYPASS}
            if p.username:
                proxy["username"] = p.username
                proxy["password"] = p.password or ""
            logger.info("Pegasus: using direct proxy %s:%s", p.hostname, p.port)
    else:
        logger.info("Pegasus: no proxy, direct connection")

    # Use system Chrome binary for a more trusted fingerprint
    try:
        chrome_path = find_chrome()
        logger.info("Pegasus: using system Chrome at %s", chrome_path)
    except RuntimeError:
        chrome_path = None
        logger.info("Pegasus: system Chrome not found, using bundled Chromium")

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
        timezone_id="Europe/Istanbul",
        color_scheme="light",
    )
    page = await context.new_page()

    # Stealth JS injection (original SDK does this for Pegasus)
    await inject_stealth_js(page)
    # Block heavy resources to save bandwidth
    await auto_block_if_proxied(page)

    return pw, browser, context, page


class PegasusConnectorClient:
    """Pegasus Airlines -- Patchright + URL navigation + availability API intercept."""

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
                logger.warning("Pegasus: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)
        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[FlightSearchResponse]:
        pw = browser = context = page = None
        try:
            pw, browser, context, page = await _launch_browser()

            # Build Pegasus booking URL (SDK-matching param names)
            date_str = req.date_from.strftime("%Y-%m-%d")
            currency = self._resolve_currency(req)
            url = (
                f"https://web.flypgs.com/booking?"
                f"language=en&adultCount={req.adults}&childCount={req.children or 0}"
                f"&infantCount={req.infants or 0}&departurePort={req.origin}"
                f"&arrivalPort={req.destination}&currency={currency}"
                f"&dateOption=1&departureDate={date_str}"
            )

            api_responses: list[str] = []
            api_event = asyncio.Event()

            async def _on_response(response):
                url_str = response.url
                if "flypgs.com" not in url_str or response.status != 200:
                    return
                content_type = response.headers.get("content-type", "")
                if "json" not in content_type:
                    return
                try:
                    body = await response.body()
                    if len(body) < 200:
                        return
                    import json as _json
                    data = _json.loads(body)
                    if not isinstance(data, dict):
                        return
                    # Match SDK: look for availability response with departureRouteList
                    if "pegasus/availability" in url_str and "departureRouteList" in data:
                        api_responses.append(body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body)
                        api_event.set()
                        logger.info("Pegasus: captured availability (%d bytes)", len(body))
                    elif "departureRouteList" in str(body[:2000]):
                        api_responses.append(body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body)
                        api_event.set()
                        logger.info("Pegasus: captured flight data from %s (%d bytes)", url_str[:80], len(body))
                except Exception:
                    pass

            page.on("response", _on_response)

            # ── Akamai warm-up: visit web.flypgs.com (same subdomain as search URL) ──
            logger.info("Pegasus: warming up on web.flypgs.com...")
            await page.goto("https://web.flypgs.com/", wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(3.0)
            for _ in range(4):
                await page.mouse.move(random.randint(100, 1200), random.randint(100, 600), steps=random.randint(3, 8))
                await asyncio.sleep(random.uniform(0.3, 0.7))
            await page.evaluate("window.scrollTo(0, 300)")
            await asyncio.sleep(0.5)
            await page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(1.0)

            # ── Navigate to pre-filled search URL ──
            logger.info("Pegasus: navigating to search URL %s->%s on %s", req.origin, req.destination, req.date_from)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2.0)

            # Diagnostic: log page state after load
            try:
                page_info = await page.evaluate("""() => {
                    return {
                        url: location.href,
                        title: document.title,
                        bodyLen: (document.body?.innerText || '').length,
                        snippet: (document.body?.innerText || '').slice(0, 300).replace(/\\s+/g, ' '),
                        hasRecaptcha: !!document.querySelector('iframe[src*="recaptcha"], [class*="recaptcha"], #captcha'),
                        iframeCount: document.querySelectorAll('iframe').length,
                    };
                }""")
                logger.info("Pegasus: page loaded — url=%s title=%s bodyLen=%d iframes=%d recaptcha=%s",
                            page_info.get("url", "?")[:80], page_info.get("title", "?")[:50],
                            page_info.get("bodyLen", 0), page_info.get("iframeCount", 0),
                            page_info.get("hasRecaptcha", False))
                if page_info.get("bodyLen", 0) < 500:
                    logger.info("Pegasus: page snippet: %s", page_info.get("snippet", "")[:300])
            except Exception as diag_err:
                logger.info("Pegasus: diagnostic eval failed: %s", diag_err)

            # Dismiss cookie consent — Pegasus uses Turkish "Kabul Et" or English "Accept"
            try:
                await page.evaluate("""() => {
                    const btns = document.querySelectorAll('button');
                    for (const b of btns) {
                        const t = b.textContent.trim().toLowerCase();
                        if (t === 'kabul et' || t === 'accept all' || t === 'accept'
                            || t.includes('accept all') || t.includes('kabul')) {
                            b.click(); return;
                        }
                    }
                    // Also try common cookie consent selectors
                    const accept = document.querySelector('#onetrust-accept-btn-handler, .cookie-accept, [data-testid="cookie-accept"]');
                    if (accept) accept.click();
                }""")
                await asyncio.sleep(1.5)
                logger.info("Pegasus: cookie consent dismissed")
            except Exception:
                pass

            # Check if the SPA processed URL params - wait a bit for Angular routing
            await asyncio.sleep(2.0)

            try:
                await asyncio.wait_for(api_event.wait(), timeout=_RESULTS_WAIT)
            except asyncio.TimeoutError:
                # Log what we see on the page
                title = await page.title()
                cur_url = page.url
                logger.warning("Pegasus: availability response timed out after %ds, title='%s', url=%s",
                             _RESULTS_WAIT, title[:50], cur_url[:80])
                # Try clicking search/find button as fallback to trigger the API call
                try:
                    for sel in [
                        'button:has-text("Search")', 'button:has-text("Find")',
                        'button:has-text("Ara")', 'button.search-button',
                        '[class*="search"] button', 'button[type="submit"]',
                    ]:
                        btn = page.locator(sel).first
                        if await btn.count() > 0 and await btn.is_visible():
                            await btn.click(timeout=3000)
                            logger.info("Pegasus: clicked search button via '%s'", sel)
                            break
                except Exception:
                    pass
                # Wait a bit more for the API response after clicking search
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=15)
                except asyncio.TimeoutError:
                    pass

            await asyncio.sleep(1.5)

            if not api_responses:
                logger.warning("Pegasus: no API response captured")
                return None

            elapsed = time.monotonic() - t0
            all_offers: list[FlightOffer] = []
            booking_url = f"https://web.flypgs.com/booking?type=1&adult={req.adults}&origin={req.origin}&destination={req.destination}&departureDate={date_str}"

            for raw in api_responses:
                try:
                    data = json.loads(raw)
                    parsed = self._parse_response(data, req, currency, booking_url)
                    all_offers.extend(parsed)
                except Exception:
                    continue

            seen = set()
            unique = []
            for o in all_offers:
                if o.id not in seen:
                    seen.add(o.id)
                    unique.append(o)
            return self._build_response(unique, req, elapsed)

        except asyncio.TimeoutError:
            logger.warning("Pegasus: search timed out")
            return None
        except Exception as e:
            logger.warning("Pegasus: search error: %s", e)
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

    # -- Parsing (from SDK) --

    def _resolve_currency(self, req: FlightSearchRequest) -> str:
        if req.origin in _TURKEY_AIRPORTS and req.destination in _TURKEY_AIRPORTS:
            return "TRY"
        return req.currency or "EUR"

    def _parse_response(self, data: dict, req: FlightSearchRequest, currency: str, booking_url: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        if not isinstance(data, dict):
            return offers

        currency = self._resolve_currency_from_data(data, currency)

        # Primary: departureRouteList (availability endpoint)
        dep_routes = data.get("departureRouteList") or data.get("departureDateRouteList") or []
        if isinstance(dep_routes, list) and dep_routes:
            for route in dep_routes:
                if not isinstance(route, dict):
                    continue
                daily_flights = route.get("dailyFlightList") or []
                if not isinstance(daily_flights, list):
                    continue
                for daily in daily_flights:
                    if not isinstance(daily, dict):
                        continue
                    day_cheapest = None
                    day_currency = currency
                    cf = daily.get("cheapestFare")
                    if isinstance(cf, dict):
                        day_cheapest = cf.get("amount")
                        day_currency = cf.get("currency") or currency
                    flight_list = daily.get("flightList") or []
                    if isinstance(flight_list, list):
                        for flight in flight_list:
                            offer = self._parse_pegasus_flight(flight, day_currency, req, booking_url, fallback_price=day_cheapest)
                            if offer:
                                offers.append(offer)
            if offers:
                return offers

        # Fallback: outboundFlights / outbound / journeys
        outbound_raw = (
            data.get("outboundFlights") or data.get("outbound")
            or (data.get("journeys", {}).get("outbound") if isinstance(data.get("journeys"), dict) else None)
            or data.get("departureDateFlights") or data.get("flights", [])
        )
        if isinstance(outbound_raw, list):
            for flight in outbound_raw:
                offer = self._parse_single_flight(flight, currency, req, booking_url)
                if offer:
                    offers.append(offer)
        return offers

    def _resolve_currency_from_data(self, data: dict, default: str) -> str:
        if data.get("currency"):
            return data["currency"]
        return default

    def _parse_pegasus_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str, fallback_price: float = None) -> Optional[FlightOffer]:
        if not isinstance(flight, dict):
            return None

        price = None

        # 1) fareBundleList
        fare_bundles = flight.get("fareBundleList") or flight.get("fareList") or flight.get("fares") or []
        if isinstance(fare_bundles, list) and fare_bundles:
            prices = []
            for fb in fare_bundles:
                if isinstance(fb, dict):
                    p = fb.get("price") or fb.get("amount") or fb.get("totalPrice") or fb.get("basePrice") or fb.get("adultPrice")
                    if isinstance(p, dict):
                        p = p.get("amount") or p.get("value")
                    if p is not None:
                        try:
                            prices.append(float(p))
                        except (TypeError, ValueError):
                            pass
            if prices:
                price = min(prices)

        # 2) Single fare
        if price is None:
            fare = flight.get("fare") or {}
            if isinstance(fare, dict):
                for key in ("amount", "price", "totalPrice", "basePrice", "adultPrice"):
                    val = fare.get(key)
                    if val is not None:
                        try:
                            price = float(val)
                            break
                        except (TypeError, ValueError):
                            pass
                fc = fare.get("currency") or fare.get("currencyCode")
                if fc:
                    currency = str(fc)

        # 3) Direct fields
        if price is None:
            price = flight.get("price") or flight.get("totalPrice") or flight.get("lowestFare") or flight.get("cheapestFare")
            if isinstance(price, dict):
                currency = price.get("currency") or currency
                price = price.get("amount") or price.get("value")

        # 4) Fallback
        if price is None and fallback_price is not None:
            price = fallback_price

        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        # Currency from fare bundles
        for fb in (fare_bundles if isinstance(fare_bundles, list) else []):
            if isinstance(fb, dict):
                fc = fb.get("currency") or fb.get("currencyCode")
                if isinstance(fc, dict):
                    fc = fc.get("code")
                if fc:
                    currency = str(fc)
                    break

        # Build segments
        seg_raw = flight.get("segmentList") or flight.get("segments") or flight.get("legs") or []
        segments: list[FlightSegment] = []
        if isinstance(seg_raw, list) and seg_raw:
            for seg in seg_raw:
                if isinstance(seg, dict):
                    segments.append(self._build_segment(seg, req.origin, req.destination))

        if not segments:
            dep_loc = flight.get("departureLocation") or {}
            arr_loc = flight.get("arrivalLocation") or {}
            origin = dep_loc.get("portCode") or flight.get("origin") or req.origin
            dest = arr_loc.get("portCode") or flight.get("destination") or req.destination
            dep_dt = flight.get("departureDateTime") or flight.get("departure") or ""
            arr_dt = flight.get("arrivalDateTime") or flight.get("arrival") or ""
            flight_no = str(flight.get("flightNo") or flight.get("flightNumber") or "").strip()
            airline = flight.get("airline") or "PC"
            segments.append(FlightSegment(
                airline=airline, airline_name="Pegasus Airlines",
                flight_no=f"{airline}{flight_no}" if flight_no and not flight_no.startswith(airline) else flight_no,
                origin=origin, destination=dest,
                departure=_parse_dt(dep_dt), arrival=_parse_dt(arr_dt),
                cabin_class="M",
            ))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            try:
                t0 = datetime.fromisoformat(segments[0].departure)
                t1 = datetime.fromisoformat(segments[-1].arrival)
                total_dur = int((t1 - t0).total_seconds())
            except Exception:
                pass
        if total_dur <= 0:
            fd = flight.get("flightDuration")
            if isinstance(fd, dict):
                vals = fd.get("values") or []
                if isinstance(vals, list) and len(vals) >= 2:
                    try:
                        total_dur = int(vals[0]) * 3600 + int(vals[1]) * 60
                    except (TypeError, ValueError):
                        pass

        route = FlightRoute(segments=segments, total_duration_seconds=max(total_dur, 0), stopovers=max(len(segments) - 1, 0))
        flight_key = flight.get("segmentId") or flight.get("flightKey") or flight.get("id") or (flight.get("flightNo", "") + "_" + str(flight.get("departureDateTime", "")))
        return FlightOffer(
            id=f"pc_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route, airlines=["Pegasus Airlines"], owner_airline="PC",
            booking_url=booking_url, is_locked=False,
            source="pegasus_direct", source_tier="free",
        )

    def _parse_single_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        if not isinstance(flight, dict):
            return None
        price = flight.get("price") or flight.get("totalPrice") or flight.get("farePrice") or flight.get("lowestFare")
        if price is None:
            fares = flight.get("fares") or flight.get("fareBundles") or flight.get("bundles") or []
            prices = []
            for f in fares:
                if isinstance(f, dict):
                    p = f.get("price") or f.get("amount") or f.get("totalPrice")
                    if p is not None:
                        try:
                            prices.append(float(p))
                        except (TypeError, ValueError):
                            pass
            if prices:
                price = min(prices)
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                if isinstance(seg, dict):
                    segments.append(self._build_segment(seg, req.origin, req.destination))
        if not segments:
            segments.append(self._build_segment(flight, req.origin, req.destination))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            try:
                t0 = datetime.fromisoformat(segments[0].departure)
                t1 = datetime.fromisoformat(segments[-1].arrival)
                total_dur = int((t1 - t0).total_seconds())
            except Exception:
                pass

        route = FlightRoute(segments=segments, total_duration_seconds=max(total_dur, 0), stopovers=max(len(segments) - 1, 0))
        flight_key = flight.get("flightKey") or flight.get("id") or flight.get("flightNumber", "") + "_" + segments[0].departure
        return FlightOffer(
            id=f"pc_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route, airlines=["Pegasus Airlines"], owner_airline="PC",
            booking_url=booking_url, is_locked=False,
            source="pegasus_direct", source_tier="free",
        )

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departure") or seg.get("departureDate") or seg.get("departureTime") or seg.get("departureDateTime") or seg.get("std") or ""
        arr_str = seg.get("arrival") or seg.get("arrivalDate") or seg.get("arrivalTime") or seg.get("arrivalDateTime") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("flightNo") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or default_origin
        destination = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or default_dest
        return FlightSegment(
            airline="PC", airline_name="Pegasus Airlines", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=_parse_dt(dep_str), arrival=_parse_dt(arr_str),
            cabin_class="M",
        )

    def _build_response(self, offers, req, elapsed):
        offers.sort(key=lambda o: o.price)
        logger.info("Pegasus: %d offers for %s->%s on %s (%.1fs)", len(offers), req.origin, req.destination, req.date_from, elapsed)
        return FlightSearchResponse(
            search_id=hashlib.md5(f"pc-{req.origin}-{req.destination}-{req.date_from}-{time.time()}".encode()).hexdigest()[:12],
            origin=req.origin, destination=req.destination, currency=offers[0].currency if offers else "EUR",
            offers=offers[:req.limit], total_results=len(offers),
            search_params={"source": "pegasus_direct", "method": "patchright_url_nav_api_intercept", "elapsed": round(elapsed, 2)},
            source_tiers={"free": "Pegasus Airlines direct (flypgs.com)"},
        )

    @staticmethod
    def _combine_rt(ob, ib, req):
        combos = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(id=f"rt_pc_{cid}", price=price, currency=o.currency, outbound=o.outbound, inbound=i.outbound, airlines=["Pegasus Airlines"], owner_airline="PC", booking_url=o.booking_url, is_locked=False, source=o.source, source_tier=o.source_tier))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req):
        return FlightSearchResponse(origin=req.origin, destination=req.destination, currency="EUR", offers=[], total_results=0, search_params={"source": "pegasus_direct", "error": "no_results"}, source_tiers={"free": "Pegasus Airlines direct (flypgs.com)"})


def _parse_dt(s: str) -> str:
    if not s:
        return ""
    try:
        return datetime.fromisoformat(s).isoformat()
    except (ValueError, TypeError):
        return s