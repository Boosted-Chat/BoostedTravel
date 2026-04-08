"""
AirAsia MOVE connector — Playwright browser + API response interception.

AirAsia MOVE (airasia.com) is AirAsia's super-app covering flights across
Southeast Asia and beyond.  The search page fires a POST to
``flights.airasia.com/web/fp/search/flights/v5/aggregated-results``
which returns all available trips with full pricing.

Strategy:
1. Launch Playwright Chromium (offscreen).
2. Navigate to the AirAsia search-results URL (date in dd/mm/yyyy).
3. Intercept the ``aggregated-results`` POST response (178 KB+).
4. Parse ``searchResults.trips[]`` into ``FlightOffer`` objects.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime
from typing import Any

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_proxy

logger = logging.getLogger(__name__)

# ── Airline-profile → display name mapping ──────────────────────────
_PROFILE_NAMES: dict[str, str] = {
    "dotrez": "AirAsia",
    "goquo": "AirAsia",
    "kiwi": "Kiwi.com",
}


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s).replace("Z", "")
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except Exception:
        return datetime(2000, 1, 1)


# ── Parse a single segment from the designator-level dict ───────────
def _seg_from_designator(d: dict, airline: str = "AirAsia") -> FlightSegment | None:
    origin = d.get("departureStation") or ""
    dest = d.get("arrivalStation") or ""
    if not origin or not dest:
        return None
    dep_dt = _parse_dt(d.get("departureTime"))
    arr_dt = _parse_dt(d.get("arrivalTime"))
    dur = max(0, int((arr_dt - dep_dt).total_seconds())) if dep_dt.year > 2000 and arr_dt.year > 2000 else 0
    carrier_code = d.get("carrierCode") or ""
    flight_num = d.get("flightNumber") or d.get("identifier", {}).get("identifier") or ""
    flight_no = f"{carrier_code}{flight_num}" if carrier_code and flight_num else str(flight_num)
    return FlightSegment(
        airline=airline,
        flight_no=flight_no,
        origin=str(origin)[:3].upper(),
        destination=str(dest)[:3].upper(),
        departure=dep_dt,
        arrival=arr_dt,
        duration_seconds=dur,
    )


def _build_leg(leg_data: dict | None, airline: str) -> FlightRoute | None:
    """Build a FlightRoute from a depart/return leg dict."""
    if not leg_data:
        return None

    segments: list[FlightSegment] = []

    # Multi-segment trips: leg_data.segments[]
    raw_segs = leg_data.get("segments") or leg_data.get("journeySegments") or []
    if raw_segs and isinstance(raw_segs, list):
        for rs in raw_segs:
            if not isinstance(rs, dict):
                continue
            des = rs.get("designator") or rs
            seg = _seg_from_designator(des, airline)
            if seg:
                segments.append(seg)
    # Fallback: single segment from leg designator
    if not segments:
        des = leg_data.get("designator") or leg_data
        seg = _seg_from_designator(des, airline)
        if seg:
            segments.append(seg)

    if not segments:
        return None

    dep = segments[0].departure
    arr = segments[-1].arrival
    total_dur = max(0, int((arr - dep).total_seconds())) if dep.year > 2000 and arr.year > 2000 else 0
    return FlightRoute(
        segments=segments,
        total_duration_seconds=total_dur,
        stopovers=max(0, len(segments) - 1),
    )


def _booking_url(req: FlightSearchRequest) -> str:
    dep = req.date_from.strftime("%d/%m/%Y")
    adults = req.adults or 1
    children = req.children or 0
    infants = req.infants or 0
    trip_type = "R" if req.return_from else "O"
    url = (
        f"https://www.airasia.com/flights/search/"
        f"?origin={req.origin}&destination={req.destination}"
        f"&departDate={dep}&tripType={trip_type}"
        f"&adult={adults}&child={children}&infant={infants}"
        f"&locale=en-gb&currency=USD"
    )
    if req.return_from:
        url += f"&returnDate={req.return_from.strftime('%d/%m/%Y')}"
    return url


def _extract_offers(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse ``searchResults.trips[]`` from the aggregated-results API."""
    offers: list[FlightOffer] = []
    book_url = _booking_url(req)

    search_results = data.get("searchResults") or {}
    trips = search_results.get("trips") or []
    if not isinstance(trips, list):
        return offers

    for i, trip in enumerate(trips[:100]):
        if not isinstance(trip, dict):
            continue
        try:
            price = float(trip.get("price") or trip.get("convertedPrice") or 0)
            if price <= 0:
                continue

            cur = trip.get("currency") or trip.get("userCurrencyCode") or "USD"
            profile = trip.get("airlineProfile") or "dotrez"
            airline_name = _PROFILE_NAMES.get(profile, profile.capitalize())

            fd = trip.get("flightDetails") or {}
            dur_info = fd.get("duration") or {}

            # Outbound
            depart_data = fd.get("depart")
            outbound = _build_leg(depart_data, airline_name)
            # Fallback: use top-level designator
            if not outbound:
                des = fd.get("designator")
                if des:
                    seg = _seg_from_designator(des, airline_name)
                    if seg:
                        total_dur = int(dur_info.get("departure") or seg.duration_seconds or 0)
                        outbound = FlightRoute(
                            segments=[seg],
                            total_duration_seconds=total_dur,
                            stopovers=0,
                        )
            if not outbound:
                continue

            # Override duration from API if available
            if dur_info.get("departure"):
                outbound.total_duration_seconds = int(dur_info["departure"])

            # Inbound (round-trips)
            return_data = fd.get("return")
            inbound = _build_leg(return_data, airline_name) if return_data else None
            if inbound and dur_info.get("return"):
                inbound.total_duration_seconds = int(dur_info["return"])

            airlines = list({s.airline for s in outbound.segments if s.airline})
            if not airlines:
                airlines = [airline_name]

            h = hashlib.md5(
                f"aam{req.origin}{req.destination}{i}{price}{trip.get('tripId', '')}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"off_aam_{h}",
                source="airasiamove_ota",
                price=price,
                currency=cur,
                airlines=airlines,
                owner_airline=airlines[0],
                outbound=outbound,
                inbound=inbound,
                booking_url=book_url,
            ))
        except Exception as e:
            logger.debug("AirAsiaMOVE parse trip %d: %s", i, e)

    return offers


class AirasiamoveConnectorClient:
    """AirAsia MOVE — ASEAN OTA, Playwright + API interception."""

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
                        "AIRASIAMOVE %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"aam{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_aam_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("AIRASIAMOVE attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            # Target the real AirAsia aggregated-results API
            if "aggregated-results" not in url and "lowfare" not in url:
                return
            try:
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct or "text" in ct:
                        body = await response.text()
                        if len(body) > 500:
                            data = json.loads(body)
                            if isinstance(data, dict) and data.get("searchResults"):
                                api_responses.append(data)
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("AIRASIAMOVE_PROXY")
            launch_kw: dict = {
                "headless": False,
                "args": [
                    "--headless=new",
                    "--no-sandbox",
                    "--disable-http2",
                    "--window-position=-2400,-2400",
                    "--window-size=1366,768",
                    "--disable-blink-features=AutomationControlled",
                ],
            }
            if proxy:
                launch_kw["proxy"] = proxy
            browser = await pw.chromium.launch(**launch_kw)
            ctx = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            if proxy:
                from .browser import auto_block_if_proxied
                await auto_block_if_proxied(page)
            page.on("response", on_response)

            # AirAsia expects dd/mm/yyyy date format in the URL
            dep = req.date_from.strftime("%d/%m/%Y")
            adults = req.adults or 1
            children = req.children or 0
            infants = req.infants or 0
            trip_type = "R" if req.return_from else "O"

            url = (
                f"https://www.airasia.com/flights/search/"
                f"?origin={req.origin}&destination={req.destination}"
                f"&departDate={dep}&tripType={trip_type}"
                f"&adult={adults}&child={children}&infant={infants}"
                f"&locale=en-gb&currency=USD&type=bundled"
                f"&providers=&taIDs="
            )
            if req.return_from:
                url += f"&returnDate={req.return_from.strftime('%d/%m/%Y')}"

            await page.goto(url, wait_until="domcontentloaded", timeout=35000)

            # Wait for the aggregated-results API response (typically 5-15s)
            for _ in range(20):
                await page.wait_for_timeout(2000)
                if api_responses:
                    # Give a little extra time for pagination / additional data
                    await page.wait_for_timeout(3000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("AIRASIAMOVE browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("AIRASIAMOVE: no aggregated-results response captured")
            return None

        all_offers: list[FlightOffer] = []
        for resp_data in api_responses:
            all_offers.extend(_extract_offers(resp_data, req))

        seen: set[str] = set()
        unique: list[FlightOffer] = []
        for o in all_offers:
            key = f"{o.price}_{o.outbound.segments[0].origin}_{o.outbound.segments[0].destination}_{o.outbound.segments[0].departure.isoformat() if o.outbound and o.outbound.segments else ''}"
            if key not in seen:
                seen.add(key)
                unique.append(o)

        return unique

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
                    id=f"rt_aam_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
