"""
Skyscanner connector — Playwright browser + API response interception.

Skyscanner is the world's #1 flight meta-search engine, aggregating from
1200+ partners. Direct API access is blocked by PerimeterX, but loading
the results page in a real browser fires the internal fps3/search API.

Strategy:
1.  Launch Playwright browser (non-headless to bypass PerimeterX).
2.  Navigate to Skyscanner search results URL.
3.  Intercept the fps3/search (or similar) API response with flight data.
4.  Parse itineraries from the JSON response.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00").split("+")[0])
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


class SkyscannerConnectorClient:
    """Skyscanner — meta-search, Playwright + API interception."""

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
                        "SKYSCANNER %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"skyscanner{req.origin}{req.destination}{req.date_from}".encode()
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
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
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

        pw = await async_playwright().start()
        try:
            browser = await pw.chromium.launch(
                headless=False,
                args=[
                    "--window-position=-2400,-2400",
                    "--window-size=1366,768",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            ctx = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            page.on("response", on_response)

            # Skyscanner URL pattern: /transport/flights/{origin}/{dest}/{YYMMDD}/
            d = req.date_from
            date_str = f"{d.year % 100:02d}{d.month:02d}{d.day:02d}"
            origin = req.origin.lower()
            dest = req.destination.lower()
            url = (
                f"https://www.skyscanner.net/transport/flights/"
                f"{origin}/{dest}/{date_str}/"
                f"?adultsv2={req.adults or 1}"
                f"&cabinclass=economy"
                f"&ref=home"
            )

            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            # Wait for API responses — Skyscanner progressively loads results
            for _ in range(10):
                await page.wait_for_timeout(3000)
                if api_responses:
                    # Give time for progressive loading
                    await page.wait_for_timeout(8000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("SKYSCANNER browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("SKYSCANNER: no flight API response captured")
            return None

        # Use the largest (most complete) response
        best = max(api_responses, key=lambda d: len(json.dumps(d)))
        return _parse_skyscanner(best, req)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


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

    target_cur = req.currency or "EUR"
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
                    f"https://www.skyscanner.net/transport/flights/"
                    f"{req.origin.lower()}/{req.destination.lower()}/"
                ),
            ))
        except Exception as e:
            logger.warning("SKYSCANNER: parse itinerary failed: %s", e)

    return offers
