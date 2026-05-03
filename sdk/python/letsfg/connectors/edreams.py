"""
eDreams connector — CDP Chrome + GraphQL API interception.

eDreams (eDreams ODIGEO group) is a major European OTA covering 600+ airlines.
Also powers Opodo, GoVoyages, Liligo.

Strategy:
1.  Launch REAL system Chrome via CDP (not bundled Chromium) to bypass bot detection.
2.  Navigate to direct results URL.
3.  Intercept GraphQL searchItinerary response (145+ itineraries).
4.  Parse structured data: itineraries → segments → sections → carriers/locations.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
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

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min

# ── CDP Chrome singleton ──
_DEBUG_PORT = 9504
_USER_DATA_DIR = os.path.join(os.getcwd(), ".edreams_chrome_data")
_browser = None
_chrome_proc = None
_pw_instance = None


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        # Handle "2026-04-15T07:05:00+01:00"
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
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
        logger.info("eDreams: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch Chrome HEADED (no --headless) — bot protection detects headless Chrome
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
    logger.info("eDreams: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
    return _browser


async def _reset_chrome_profile():
    """Kill Chrome and wipe user-data-dir to clear flagged sessions."""
    global _browser, _chrome_proc
    try:
        if _browser:
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
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("eDreams: deleted stale Chrome profile %s", _USER_DATA_DIR)
        except Exception as e:
            logger.warning("eDreams: failed to delete Chrome profile: %s", e)


class EdreamsConnectorClient:
    """eDreams — European OTA, Playwright + GraphQL interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            # When we navigate to the RT URL, _parse_graphql already builds
            # complete RT offers (total price + inbound leg). Combining again
            # would double-count the return leg cost.
            if any(o.inbound is not None for o in ob_result.offers):
                return ob_result
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
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, ob_result.currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        return {
            "checked_bag": "not included – add-on from ~35 € at eDreams checkout (OTA markup; book airline direct for lower fees)",
            "bags_note": "personal item free; cabin bag add-on from ~30 € at eDreams checkout",
            "seat_note": "seat add-on from ~10 € at eDreams checkout; skip for random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_price")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["cabin_bag"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from == 0.0:
                offer.bags_price["cabin_bag"] = 0.0
            if checked_from == 0.0:
                offer.bags_price["checked_bag"] = 0.0

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
                        "EDREAMS %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"edreams{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_ed_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("EDREAMS attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from .browser import inject_stealth_js, auto_block_if_proxied

        graphql_data: list[dict] = []
        blocked = False

        async def on_response(response):
            nonlocal blocked
            if "graphql" not in response.url:
                return
            try:
                if response.status == 403:
                    blocked = True
                    return
                if response.status == 200:
                    body = await response.text()
                    if len(body) > 50000:
                        data = json.loads(body)
                        si = data.get("data", {}).get("searchItinerary")
                        if si and si.get("itineraries"):
                            graphql_data.append(si)
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

            # Direct URL to results — avoids form fill
            dep_date = req.date_from.isoformat()
            trip_type = "R" if req.return_from else "O"
            _ed_cabin = {"M": "E", "W": "W", "C": "B", "F": "F"}
            cabin = _ed_cabin.get(req.cabin_class, "E") if req.cabin_class else "E"
            url = (
                f"https://www.edreams.com/travel/"
                f"#results/type={trip_type}"
                f";dep={dep_date}"
                f";from={req.origin}"
                f";to={req.destination}"
                f";pa={req.adults or 1}"
                f";py={cabin}"
            )
            if req.return_from:
                url += f";ret={req.return_from.isoformat()}"

            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)

            # Dismiss cookie consent
            for sel in [
                "#didomi-notice-agree-button",
                "button:has-text('Continue without agreeing')",
                "button:has-text('Accept')",
            ]:
                try:
                    btn = page.locator(sel)
                    if await btn.count() > 0:
                        await btn.first.click(force=True)
                        await page.wait_for_timeout(500)
                        break
                except Exception:
                    pass

            # Wait for GraphQL response (up to 30s)
            for _ in range(6):
                await page.wait_for_timeout(5000)
                if graphql_data or blocked:
                    break

            await page.close()
            await ctx.close()
        except Exception as e:
            logger.error("EDREAMS browser error: %s", e)
            return None

        if blocked:
            logger.warning("EDREAMS: bot protection blocked, resetting profile")
            await _reset_chrome_profile()
            return None

        if not graphql_data:
            logger.warning("EDREAMS: no GraphQL searchItinerary captured")
            return None

        return _parse_graphql(graphql_data[0], req)

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
                    id=f"rt_ed_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_graphql(si: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse eDreams GraphQL searchItinerary response into FlightOffer list."""
    # Build lookup maps
    carrier_map: dict[str, str] = {}
    for c in si.get("carriers") or []:
        info = c.get("carrier") or {}
        carrier_map[c.get("id", "")] = info.get("name", c.get("id", ""))

    loc_map: dict[str, str] = {}
    for loc in si.get("locations") or []:
        info = loc.get("location") or {}
        loc_map[loc.get("id", "")] = info.get("iata", "")

    sec_map: dict[str, dict] = {}
    for s in si.get("sections") or []:
        sec_map[s.get("id", "")] = s.get("section") or s

    seg_map: dict[str, dict] = {}
    for s in si.get("segments") or []:
        seg_map[s.get("id", "")] = s.get("segment") or s

    offers: list[FlightOffer] = []
    for itin in si.get("itineraries") or []:
        try:
            # Extract price — use MEMBER_PRICE_POLICY_UNDISCOUNTED fee
            price = 0.0
            currency = "EUR"
            for fee in itin.get("fees") or []:
                if fee.get("type") == "MEMBER_PRICE_POLICY_UNDISCOUNTED":
                    pr = fee.get("price") or {}
                    price = float(pr.get("amount", 0))
                    currency = pr.get("currency", "EUR")
                    break
            if price <= 0:
                # Fallback: try first fee with a price
                for fee in itin.get("fees") or []:
                    pr = fee.get("price") or {}
                    amt = float(pr.get("amount", 0))
                    if amt > 0:
                        price = amt
                        currency = pr.get("currency", "EUR")
                        break
            if price <= 0:
                continue

            legs = itin.get("legs") or []
            if not legs:
                continue

            # Build outbound route from first leg
            out_leg = legs[0]
            seg_id = out_leg.get("segmentId", "")
            seg_data = seg_map.get(seg_id, {})

            section_ids = seg_data.get("sections") or []
            flight_segments: list[FlightSegment] = []
            total_dur = (seg_data.get("duration") or 0) * 60  # minutes → seconds

            for sec_ref in section_ids:
                sec_id = sec_ref if isinstance(sec_ref, str) else sec_ref.get("id", "")
                sec = sec_map.get(sec_id, {})

                dep_id = str(sec.get("departureId", ""))
                arr_id = str(sec.get("destinationId", ""))
                origin_iata = loc_map.get(dep_id, req.origin)
                dest_iata = loc_map.get(arr_id, req.destination)
                carrier_id = sec.get("carrierId", "")
                carrier_name = carrier_map.get(carrier_id, carrier_id)
                flight_code = sec.get("flightCode", "")

                flight_segments.append(FlightSegment(
                    airline=carrier_id,
                    airline_name=carrier_name,
                    flight_no=f"{carrier_id}{flight_code}",
                    origin=origin_iata,
                    destination=dest_iata,
                    departure=_parse_dt(sec.get("departureDate")),
                    arrival=_parse_dt(sec.get("arrivalDate")),
                ))

            if not flight_segments:
                continue

            stopovers = max(0, len(flight_segments) - 1)
            outbound = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=total_dur,
                stopovers=stopovers,
            )

            # Build inbound route from second leg (if round-trip)
            inbound = None
            if len(legs) > 1:
                ret_leg = legs[1]
                ret_seg_id = ret_leg.get("segmentId", "")
                ret_seg_data = seg_map.get(ret_seg_id, {})
                ret_section_ids = ret_seg_data.get("sections") or []
                ret_segments: list[FlightSegment] = []
                ret_dur = (ret_seg_data.get("duration") or 0) * 60

                for sec_ref in ret_section_ids:
                    sec_id = sec_ref if isinstance(sec_ref, str) else sec_ref.get("id", "")
                    sec = sec_map.get(sec_id, {})
                    dep_id = str(sec.get("departureId", ""))
                    arr_id = str(sec.get("destinationId", ""))
                    carrier_id = sec.get("carrierId", "")
                    ret_segments.append(FlightSegment(
                        airline=carrier_id,
                        airline_name=carrier_map.get(carrier_id, carrier_id),
                        flight_no=f"{carrier_id}{sec.get('flightCode', '')}",
                        origin=loc_map.get(dep_id, req.destination),
                        destination=loc_map.get(arr_id, req.origin),
                        departure=_parse_dt(sec.get("departureDate")),
                        arrival=_parse_dt(sec.get("arrivalDate")),
                    ))

                if ret_segments:
                    inbound = FlightRoute(
                        segments=ret_segments,
                        total_duration_seconds=ret_dur,
                        stopovers=max(0, len(ret_segments) - 1),
                    )

            # Collect unique airline codes
            all_airlines = list(dict.fromkeys(
                s.airline for s in flight_segments if s.airline
            ))
            owner = flight_segments[0].airline if flight_segments else ""

            itin_key = itin.get("key", "")
            h = hashlib.md5(
                f"ed_{itin_key}_{price}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"ed_{h}",
                price=price,
                currency=currency,
                price_formatted=f"{currency} {price:.2f}",
                outbound=outbound,
                inbound=inbound,
                airlines=all_airlines,
                owner_airline=owner,
                source="edreams_ota",
                source_tier="free",
                is_locked=False,
                booking_url=(
                    f"https://www.edreams.com/travel/"
                    f"#results/type=O"
                    f";dep={req.date_from.isoformat()}"
                    f";from={req.origin};to={req.destination}"
                    f";pa={req.adults or 1};py=E"
                ),
            ))
        except Exception as e:
            logger.warning("EDREAMS: parse itinerary failed: %s", e)

    return offers
