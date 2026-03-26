"""
SmartFares connector — Playwright browser + API response interception.

SmartFares is a US-based OTA/consolidator (smartfares.com) that markets
"unpublished fares" and aggregates deals from multiple airlines.  ASP.NET
backend with Cloudflare protection.

Strategy:
1. Launch Playwright browser (non-headless, offscreen).
2. Navigate to search results URL.
3. Intercept XHR/fetch JSON responses containing flight results.
4. Parse itinerary data from the captured responses.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
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


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except Exception:
        return datetime(2000, 1, 1)


def _dur_seconds(segments: list[FlightSegment]) -> int:
    if not segments:
        return 0
    dep = segments[0].departure
    arr = segments[-1].arrival
    return max(0, int((arr - dep).total_seconds()))


def _parse_segment(seg: dict, idx: int) -> FlightSegment | None:
    """Try to extract a FlightSegment from various JSON shapes."""
    origin = (
        seg.get("departureAirport")
        or seg.get("origin")
        or seg.get("departure", {}).get("airport")
        or seg.get("from")
        or seg.get("dep")
        or ""
    )
    dest = (
        seg.get("arrivalAirport")
        or seg.get("destination")
        or seg.get("arrival", {}).get("airport")
        or seg.get("to")
        or seg.get("arr")
        or ""
    )
    dep_time = (
        seg.get("departureDateTime")
        or seg.get("departureTime")
        or seg.get("departure", {}).get("dateTime")
        or seg.get("departure", {}).get("time")
        or seg.get("depTime")
        or ""
    )
    arr_time = (
        seg.get("arrivalDateTime")
        or seg.get("arrivalTime")
        or seg.get("arrival", {}).get("dateTime")
        or seg.get("arrival", {}).get("time")
        or seg.get("arrTime")
        or ""
    )
    airline = (
        seg.get("airlineName")
        or seg.get("airline")
        or seg.get("carrier")
        or seg.get("marketingCarrier")
        or seg.get("operatingCarrier")
        or ""
    )
    airline_code = (
        seg.get("airlineCode")
        or seg.get("carrierCode")
        or seg.get("marketingCarrierCode")
        or ""
    )
    flight_no = (
        seg.get("flightNumber")
        or seg.get("flightNo")
        or seg.get("number")
        or ""
    )
    if airline_code and flight_no:
        flight_no = f"{airline_code}{flight_no}"

    if not origin or not dest:
        return None

    return FlightSegment(
        airline=str(airline),
        flight_no=str(flight_no),
        origin=str(origin)[:3].upper(),
        destination=str(dest)[:3].upper(),
        departure=_parse_dt(dep_time),
        arrival=_parse_dt(arr_time),
        duration_seconds=0,
    )


def _extract_offers(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse flight offers from SmartFares response JSON."""
    offers: list[FlightOffer] = []
    currency = req.currency or "USD"

    # Try various response shapes
    results = (
        data.get("flights")
        or data.get("results")
        or data.get("itineraries")
        or data.get("offers")
        or data.get("data", {}).get("flights")
        or data.get("data", {}).get("results")
        or data.get("data", {}).get("itineraries")
        or data.get("AirSearchResults")
        or data.get("PricedItineraries")
        or []
    )
    if isinstance(results, dict):
        results = results.get("items") or results.get("list") or []

    if not isinstance(results, list):
        return offers

    for i, itin in enumerate(results[:80]):
        if not isinstance(itin, dict):
            continue
        try:
            price = (
                itin.get("totalPrice")
                or itin.get("price")
                or itin.get("TotalFare")
                or itin.get("amount")
                or itin.get("fare", {}).get("total")
                or itin.get("fare", {}).get("totalAmount")
                or itin.get("pricing", {}).get("total")
                or 0
            )
            price = float(price) if price else 0
            if price <= 0:
                continue

            cur = (
                itin.get("currency")
                or itin.get("Currency")
                or itin.get("fare", {}).get("currency")
                or currency
            )

            # Outbound segments
            ob_segs_raw = (
                itin.get("outbound", {}).get("segments")
                or itin.get("outboundSegments")
                or itin.get("legs", [{}])[0].get("segments")
                or itin.get("segments")
                or itin.get("Segments", [{}])[0:1]
                or []
            )
            # Handle flat segment list for one-way
            if isinstance(ob_segs_raw, list) and ob_segs_raw:
                if isinstance(ob_segs_raw[0], dict) and "segments" in ob_segs_raw[0]:
                    ob_segs_raw = ob_segs_raw[0]["segments"]

            ob_segments = []
            for si, seg in enumerate(ob_segs_raw if isinstance(ob_segs_raw, list) else []):
                fs = _parse_segment(seg, si)
                if fs:
                    ob_segments.append(fs)

            if not ob_segments:
                continue

            # Fix segment durations
            for s in ob_segments:
                if s.duration_seconds == 0 and s.departure.year > 2000 and s.arrival.year > 2000:
                    s.duration_seconds = max(0, int((s.arrival - s.departure).total_seconds()))

            outbound = FlightRoute(
                segments=ob_segments,
                total_duration_seconds=_dur_seconds(ob_segments),
                stopovers=max(0, len(ob_segments) - 1),
            )

            # Inbound (if round-trip)
            inbound = None
            ib_segs_raw = (
                itin.get("inbound", {}).get("segments")
                or itin.get("inboundSegments")
                or (itin.get("legs", [{}])[1].get("segments") if len(itin.get("legs", [])) > 1 else None)
                or itin.get("returnSegments")
                or None
            )
            if ib_segs_raw and isinstance(ib_segs_raw, list):
                ib_segments = []
                for si, seg in enumerate(ib_segs_raw):
                    fs = _parse_segment(seg, si)
                    if fs:
                        ib_segments.append(fs)
                if ib_segments:
                    for s in ib_segments:
                        if s.duration_seconds == 0 and s.departure.year > 2000 and s.arrival.year > 2000:
                            s.duration_seconds = max(0, int((s.arrival - s.departure).total_seconds()))
                    inbound = FlightRoute(
                        segments=ib_segments,
                        total_duration_seconds=_dur_seconds(ib_segments),
                        stopovers=max(0, len(ib_segments) - 1),
                    )

            airlines = list({s.airline for s in ob_segments if s.airline})
            if not airlines:
                airlines = ["SmartFares"]

            h = hashlib.md5(
                f"smf{req.origin}{req.destination}{i}{price}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"off_smf_{h}",
                source="smartfares_ota",
                price=price,
                currency=cur,
                airlines=airlines,
                owner_airline=airlines[0],
                outbound=outbound,
                inbound=inbound,
                deep_link=f"https://www.smartfares.com/flights",
                booking_url=f"https://www.smartfares.com/flights",
            ))
        except Exception as e:
            logger.debug("SmartFares parse offer %d: %s", i, e)

    return offers


class SmartfaresConnectorClient:
    """SmartFares — US consolidator OTA, Playwright + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(
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
                        "SMARTFARES %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"smf{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_smf_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("SMARTFARES attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            hit = any(k in url.lower() for k in [
                "/api/search", "/api/flight", "/search/result",
                "/airresult", "/getflights", "searchflight",
                "/result", "airsearch", "pricediti",
            ])
            if not hit:
                return
            try:
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct or "javascript" in ct:
                        body = await response.text()
                        if len(body) > 2000:
                            data = json.loads(body)
                            if isinstance(data, dict):
                                api_responses.append(data)
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("SMARTFARES_PROXY")
            launch_kw: dict = {
                "headless": False,
                "args": [
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
                from .browser import block_heavy_resources
                await block_heavy_resources(page)
            page.on("response", on_response)

            dep = req.date_from.strftime("%m/%d/%Y")
            adults = req.adults or 1
            cabin_map = {"M": "Economy", "W": "PremiumEconomy", "C": "Business", "F": "First"}
            cabin = cabin_map.get(req.cabin_class or "M", "Economy")

            # SmartFares search URL
            url = (
                f"https://www.smartfares.com/flights/search-results"
                f"?from={req.origin}&to={req.destination}"
                f"&depart={dep}&adults={adults}"
                f"&cabin={cabin}&trip=oneway"
            )
            if req.return_from:
                ret = req.return_from.strftime("%m/%d/%Y")
                url = url.replace("trip=oneway", f"trip=roundtrip&return={ret}")

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Wait for API responses (progressive loading)
            for _ in range(15):
                await page.wait_for_timeout(2500)
                if api_responses:
                    await page.wait_for_timeout(4000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("SMARTFARES browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("SMARTFARES: no flight API response captured")
            return None

        all_offers: list[FlightOffer] = []
        for resp_data in api_responses:
            all_offers.extend(_extract_offers(resp_data, req))

        # Deduplicate by price+route
        seen: set[str] = set()
        unique: list[FlightOffer] = []
        for o in all_offers:
            key = f"{o.price}_{o.outbound.segments[0].flight_no if o.outbound and o.outbound.segments else ''}"
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
