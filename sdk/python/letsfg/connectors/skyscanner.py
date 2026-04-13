"""
Skyscanner connector — CDP Chrome + API response interception.

Skyscanner is the world's #1 flight meta-search engine, aggregating from
1200+ partners. Direct API access is blocked by PerimeterX, but loading
the results page in a REAL Chrome browser fires the internal fps3/search API.

Strategy:
1.  Launch REAL system Chrome via CDP (not bundled Chromium) to bypass PerimeterX.
2.  Navigate to Skyscanner search results URL.
3.  Intercept the fps3/search (or similar) API response with flight data.
4.  Parse itineraries from the JSON response.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import shutil
import subprocess
import time
from datetime import datetime, date as date_type
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# ── CDP Chrome singleton ──
_DEBUG_PORT = 9452
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".skyscanner_chrome_data")
_browser = None
_chrome_proc = None
_pw_instance = None


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00").split("+")[0])
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


async def _get_browser():
    """Get or launch CDP Chrome browser (singleton)."""
    global _browser, _chrome_proc, _pw_instance

    # Reuse existing connection
    if _browser:
        try:
            if _browser.is_connected():
                return _browser
        except Exception:
            pass

    from playwright.async_api import async_playwright
    from .browser import (
        find_chrome,
        stealth_popen_kwargs,
        proxy_chrome_args,
        disable_background_networking_args,
        _launched_procs,
    )

    # Try connecting to existing Chrome on the port first
    pw = None
    try:
        pw = await async_playwright().start()
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        _pw_instance = pw
        logger.info("Skyscanner: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch Chrome HEADED (no --headless) — PerimeterX detects headless Chrome
    chrome = find_chrome()
    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    args = [
        chrome,
        f"--remote-debugging-port={_DEBUG_PORT}",
        f"--user-data-dir={_USER_DATA_DIR}",
        "--no-first-run",
        *proxy_chrome_args(),
        *disable_background_networking_args(),
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",
        "--disable-http2",
        "--window-position=-2400,-2400",
        "--window-size=1366,768",
        "about:blank",
    ]
    _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
    _launched_procs.append(_chrome_proc)
    await asyncio.sleep(2.0)

    pw = await async_playwright().start()
    _pw_instance = pw
    _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
    logger.info("Skyscanner: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
    return _browser


async def _reset_chrome_profile():
    """Kill Chrome and wipe user-data-dir to clear PerimeterX-flagged sessions."""
    global _browser, _chrome_proc, _pw_instance
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    _browser = None
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    _pw_instance = None
    if _chrome_proc:
        pid = _chrome_proc.pid
        try:
            _chrome_proc.kill()  # forceful kill (SIGKILL / TerminateProcess)
        except Exception:
            pass
        try:
            _chrome_proc.wait(timeout=5)  # wait for process to actually exit
        except Exception:
            # Force-kill via OS if still running
            try:
                import signal
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        _chrome_proc = None
    # Brief pause to let OS release file handles
    await asyncio.sleep(0.5)
    if os.path.isdir(_USER_DATA_DIR):
        for retry in range(3):
            try:
                shutil.rmtree(_USER_DATA_DIR)
                logger.info("Skyscanner: deleted stale Chrome profile %s", _USER_DATA_DIR)
                break
            except Exception as e:
                if retry < 2:
                    await asyncio.sleep(1.0)  # wait for file handles to release
                else:
                    logger.warning("Skyscanner: failed to delete Chrome profile: %s", e)


class SkyscannerConnectorClient:
    """Skyscanner — meta-search, Playwright + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(2):
            try:
                offers = await self._do_search(req)
                if offers is not None:
                    offers.sort(
                        key=lambda o: o.price if o.price > 0 else float("inf")
                    )
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "SKYSCANNER %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"skyscanner{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_ss_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("SKYSCANNER attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from .browser import inject_stealth_js, auto_block_if_proxied

        api_responses: list[dict] = []
        px_blocked = False

        async def on_response(response):
            nonlocal px_blocked
            url = response.url
            # PerimeterX block detection
            if response.status == 403 or "captcha" in url.lower() or "challenge" in url.lower():
                px_blocked = True
                return
            # Skyscanner uses multiple API patterns for search results
            hit = (
                "fps3/search" in url
                or "flights/live/search" in url
                or "flights/indicative" in url
                or ("graphql" in url and "flight" in url.lower())
            )
            if not hit:
                return
            try:
                if response.status == 200:
                    body = await response.text()
                    if len(body) > 5000:
                        data = json.loads(body)
                        # fps3 returns {content: {results: {itineraries: {...}}}}
                        content = data.get("content") or data.get("data") or data
                        results = content.get("results") or content
                        if (
                            results.get("itineraries")
                            or results.get("legs")
                            or results.get("quotes")
                        ):
                            api_responses.append(data)
            except Exception:
                pass

        try:
            browser = await _get_browser()
            ctx = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            await inject_stealth_js(page)
            await auto_block_if_proxied(page)
            page.on("response", on_response)

            # Skyscanner URL pattern: /transport/flights/{origin}/{dest}/{YYMMDD}/
            d = req.date_from
            date_str = f"{d.year % 100:02d}{d.month:02d}{d.day:02d}"
            origin = req.origin.lower()
            dest = req.destination.lower()
            _sky_cabin = {"M": "economy", "W": "premiumeconomy", "C": "business", "F": "first"}
            cabin = _sky_cabin.get(req.cabin_class, "economy") if req.cabin_class else "economy"
            currency = req.currency or "EUR"
            # Use .eu domain — less aggressive bot detection than .net
            url = (
                f"https://www.skyscanner.eu/transport/flights/"
                f"{origin}/{dest}/{date_str}/"
                f"?adultsv2={req.adults or 1}"
                f"&cabinclass={cabin}"
                f"&currency={currency}"
                f"&locale=en-GB"
                f"&market=EU"
                f"&ref=home"
            )

            # Small random delay before navigation to appear more human
            await page.wait_for_timeout(random.randint(500, 1500))
            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            # Wait for API responses — Skyscanner progressively loads results.
            # Wait up to 30s total, but once first response arrives, keep
            # collecting for 15s more to capture late-arriving cheap offers.
            first_response_at = None
            for _ in range(15):
                await page.wait_for_timeout(2000)
                if px_blocked:
                    break
                if api_responses and first_response_at is None:
                    first_response_at = time.monotonic()
                if first_response_at and (time.monotonic() - first_response_at) >= 15:
                    break  # 15s after first response — enough progressive loading time

            await page.close()
            await ctx.close()
        except Exception as e:
            logger.error("SKYSCANNER browser error: %s", e)
            return None

        if px_blocked:
            logger.warning("SKYSCANNER: PerimeterX blocked, resetting profile")
            await _reset_chrome_profile()
            return None

        if not api_responses:
            logger.warning("SKYSCANNER: no flight API response captured")
            return None

        logger.info("SKYSCANNER: captured %d API responses, merging all", len(api_responses))
        # Merge ALL captured responses — cheapest offers may arrive in any response
        all_offers: list[FlightOffer] = []
        seen_ids: set[str] = set()
        for resp_data in api_responses:
            parsed = _parse_skyscanner(resp_data, req)
            for offer in parsed:
                if offer.id not in seen_ids:
                    seen_ids.add(offer.id)
                    all_offers.append(offer)
        all_offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        return all_offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
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
                    id=f"rt_ss_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_skyscanner(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse Skyscanner fps3/search response into FlightOffer list.

    Structure: data.content.results.{itineraries, legs, segments, carriers, places}
    Itineraries reference leg IDs, legs reference segment IDs, etc.
    """
    content = data.get("content") or data.get("data") or data
    results = content.get("results") or content
    sort_data = content.get("sortingOptions") or {}

    # Build lookup maps
    legs_map: dict[str, dict] = {}
    for lid, leg in (results.get("legs") or {}).items():
        legs_map[lid] = leg

    segments_map: dict[str, dict] = {}
    for sid, seg in (results.get("segments") or {}).items():
        segments_map[sid] = seg

    carriers_map: dict[str, dict] = {}
    for cid, carrier in (results.get("carriers") or {}).items():
        carriers_map[cid] = carrier

    places_map: dict[str, dict] = {}
    for pid, place in (results.get("places") or {}).items():
        places_map[pid] = place

    # Extract currency from API response — prefer actual data over request assumption
    target_cur = req.currency or "EUR"
    # fps3 responses may contain currency info in sortingOptions or context
    ctx_currency = (
        sort_data.get("currency")
        or (content.get("context") or {}).get("currency")
        or (content.get("context") or {}).get("currencyCode")
    )
    if ctx_currency and len(ctx_currency) == 3:
        target_cur = ctx_currency
    offers: list[FlightOffer] = []

    itineraries = results.get("itineraries") or {}
    for itin_id, itin in itineraries.items():
        try:
            # Price — look in pricingOptions
            pricing_opts = itin.get("pricingOptions") or []
            if not pricing_opts:
                continue
            best_price = None
            for po in pricing_opts:
                p = po.get("price") or {}
                amount = p.get("amount")
                if amount is not None:
                    # Skyscanner prices are in micros or as strings
                    amt = float(str(amount).replace(",", ""))
                    if best_price is None or amt < best_price:
                        best_price = amt
            if not best_price or best_price <= 0:
                continue

            # Leg IDs
            leg_ids = itin.get("legIds") or []
            if not leg_ids:
                continue

            # Build outbound from first leg
            out_leg = legs_map.get(leg_ids[0], {})
            seg_ids = out_leg.get("segmentIds") or []
            flight_segments: list[FlightSegment] = []

            for sid in seg_ids:
                seg = segments_map.get(sid, {})
                carrier_id = str(seg.get("marketingCarrierId") or seg.get("operatingCarrierId", ""))
                carrier_info = carriers_map.get(carrier_id, {})
                carrier_name = carrier_info.get("name", carrier_id)

                origin_id = str(seg.get("originPlaceId", ""))
                dest_id = str(seg.get("destinationPlaceId", ""))
                origin_place = places_map.get(origin_id, {})
                dest_place = places_map.get(dest_id, {})

                flight_segments.append(FlightSegment(
                    airline=carrier_info.get("iata", carrier_id),
                    airline_name=carrier_name,
                    flight_no=f"{carrier_info.get('iata', carrier_id)}{seg.get('marketingFlightNumber', '')}",
                    origin=origin_place.get("iata", req.origin),
                    destination=dest_place.get("iata", req.destination),
                    departure=_parse_dt(seg.get("departureDateTime") or seg.get("departure")),
                    arrival=_parse_dt(seg.get("arrivalDateTime") or seg.get("arrival")),
                ))

            if not flight_segments:
                continue

            total_dur = (out_leg.get("durationInMinutes") or 0) * 60
            stopovers = out_leg.get("stopCount", max(0, len(flight_segments) - 1))
            outbound = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=total_dur,
                stopovers=stopovers,
            )

            # Build inbound (if round-trip)
            inbound = None
            if len(leg_ids) > 1:
                ret_leg = legs_map.get(leg_ids[1], {})
                ret_seg_ids = ret_leg.get("segmentIds") or []
                ret_segments: list[FlightSegment] = []
                for sid in ret_seg_ids:
                    seg = segments_map.get(sid, {})
                    cid = str(seg.get("marketingCarrierId", ""))
                    ci = carriers_map.get(cid, {})
                    oid = str(seg.get("originPlaceId", ""))
                    did = str(seg.get("destinationPlaceId", ""))
                    ret_segments.append(FlightSegment(
                        airline=ci.get("iata", cid),
                        airline_name=ci.get("name", cid),
                        flight_no=f"{ci.get('iata', cid)}{seg.get('marketingFlightNumber', '')}",
                        origin=places_map.get(oid, {}).get("iata", req.destination),
                        destination=places_map.get(did, {}).get("iata", req.origin),
                        departure=_parse_dt(seg.get("departureDateTime") or seg.get("departure")),
                        arrival=_parse_dt(seg.get("arrivalDateTime") or seg.get("arrival")),
                    ))
                if ret_segments:
                    inbound = FlightRoute(
                        segments=ret_segments,
                        total_duration_seconds=(ret_leg.get("durationInMinutes") or 0) * 60,
                        stopovers=ret_leg.get("stopCount", max(0, len(ret_segments) - 1)),
                    )

            all_airlines = list(dict.fromkeys(
                s.airline for s in flight_segments if s.airline
            ))
            h = hashlib.md5(
                f"ss_{itin_id}_{best_price}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"ss_{h}",
                price=best_price,
                currency=target_cur,
                price_formatted=f"{target_cur} {best_price:.2f}",
                outbound=outbound,
                inbound=inbound,
                airlines=all_airlines,
                owner_airline=all_airlines[0] if all_airlines else "",
                source="skyscanner_meta",
                source_tier="free",
                is_locked=False,
                booking_url=(
                    f"https://www.skyscanner.eu/transport/flights/"
                    f"{req.origin.lower()}/{req.destination.lower()}/"
                ),
            ))
        except Exception as e:
            logger.warning("SKYSCANNER: parse itinerary failed: %s", e)

    return offers
