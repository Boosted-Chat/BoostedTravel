"""
iXigo connector — Playwright browser + SSE stream interception.

iXigo is India's leading travel meta-search engine. It searches across
multiple OTAs and airlines to find the best fares.

Strategy:
1.  Launch Playwright browser (non-headless for anti-bot).
2.  Navigate to iXigo flight search results URL.
3.  Intercept /flights/v2/search/stream SSE response (text/event-stream).
4.  Parse SSE data → flightJourneys → flightFare/flightDetails.
"""

from __future__ import annotations

import asyncio
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

logger = logging.getLogger(__name__)


def _epoch_ms_to_dt(epoch_ms: Any) -> datetime:
    """Convert epoch milliseconds to datetime."""
    if not epoch_ms:
        return datetime(2000, 1, 1)
    try:
        return datetime.fromtimestamp(int(epoch_ms) / 1000)
    except (ValueError, TypeError, OSError):
        return datetime(2000, 1, 1)


class IxigoConnectorClient:
    """iXigo — Indian meta-search, Playwright + SSE stream interception."""

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
                        "IXIGO %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"ixigo{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_ixg_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("IXIGO attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright

        stream_data: list[dict] = []

        async def on_response(response):
            url = response.url
            try:
                if response.status != 200:
                    return
                # iXigo uses /flights/v2/search/stream — SSE text/event-stream
                if "/flights/" not in url or "search/stream" not in url:
                    return
                ct = response.headers.get("content-type", "")
                if "event-stream" not in ct and "json" not in ct:
                    return
                body = await response.text()
                if len(body) < 5000:
                    return
                # Parse SSE: strip "data:" prefix
                text = body.strip()
                if text.startswith("data:"):
                    text = text[5:]
                data = json.loads(text)
                inner = data.get("data") if isinstance(data, dict) else None
                if isinstance(inner, dict):
                    journeys = inner.get("flightJourneys")
                    if isinstance(journeys, list) and journeys:
                        fares = journeys[0].get("flightFare")
                        if isinstance(fares, list) and len(fares) > 0:
                            stream_data.append(data)
                            logger.info("IXIGO: captured %d fares from SSE", len(fares))
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            from .browser import get_proxy
            proxy = get_proxy("IXIGO_PROXY")
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

            # iXigo URL format: /search/result/flight
            dep = req.date_from.strftime("%d%m%Y")
            url = (
                f"https://www.ixigo.com/search/result/flight"
                f"?from={req.origin}&to={req.destination}"
                f"&date={dep}"
                f"&adults={req.adults or 1}"
                f"&children={req.children or 0}&infants=0"
                f"&class=e&source=Search+Form"
            )
            if req.return_from:
                ret = req.return_from.strftime("%d%m%Y")
                url += f"&returnDate={ret}"

            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            # Wait for SSE response
            for _ in range(8):
                await page.wait_for_timeout(3000)
                if stream_data:
                    await page.wait_for_timeout(2000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("IXIGO browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not stream_data:
            logger.warning("IXIGO: no SSE stream captured")
            return None

        return _parse_ixigo(stream_data[0], req)

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
                    id=f"rt_ixg_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )

def _parse_ixigo(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse iXigo SSE stream response.

    Structure: data.data.flightJourneys[].flightFare[] where each fare has:
    - flightDetails[]: segments with times, airline, duration
    - fares[].fareDetails.displayFare: price in INR
    - flightFilter[].stops: stop count
    """
    offers: list[FlightOffer] = []

    inner = data.get("data")
    if not isinstance(inner, dict):
        return offers

    journeys = inner.get("flightJourneys")
    if not isinstance(journeys, list) or not journeys:
        return offers

    airport_details = inner.get("airportDetails", {})

    for journey in journeys:
        fare_list = journey.get("flightFare")
        if not isinstance(fare_list, list):
            continue

        for fare_item in fare_list:
            try:
                if not isinstance(fare_item, dict):
                    continue

                # --- Price ---
                fares = fare_item.get("fares")
                if not isinstance(fares, list) or not fares:
                    continue
                fare_details = fares[0].get("fareDetails", {})
                price = fare_details.get("displayFare", 0)
                if not isinstance(price, (int, float)) or price <= 0:
                    continue

                currency = "INR"

                # --- Flight details (segments) ---
                details = fare_item.get("flightDetails")
                if not isinstance(details, list) or not details:
                    continue

                d0 = details[0]  # Primary segment info
                airline_code = d0.get("airlineCode", "")
                airline_name = d0.get("headerTextWeb", airline_code)
                flight_no_str = d0.get("subHeaderTextWeb", "")
                dep_epoch = d0.get("departureTimeEpoch")
                arr_epoch = d0.get("arrivalTimeEpoch")
                dep_dt = _epoch_ms_to_dt(dep_epoch)
                arr_dt = _epoch_ms_to_dt(arr_epoch)
                duration_min = d0.get("duration", {}).get("time", 0) if isinstance(d0.get("duration"), dict) else 0
                stops = d0.get("stop", 0)
                origin = d0.get("origin", req.origin)
                destination = d0.get("destination", req.destination)

                # Build segments from flightKeys (e.g. "DEL-BOM-6E6318-24042026")
                flight_keys = fare_item.get("flightKeys", "")
                key_parts = flight_keys.split("*") if "*" in flight_keys else [flight_keys]

                flight_segments: list[FlightSegment] = []
                for part in key_parts:
                    # Format: ORIGIN-DEST-FLIGHTNO-DATE
                    fields = part.split("-") if part else []
                    if len(fields) >= 4:
                        seg_origin = fields[0]
                        seg_dest = fields[1]
                        seg_flight = fields[2]
                        seg_airline = seg_flight[:2] if len(seg_flight) >= 2 else airline_code
                    else:
                        seg_origin = origin
                        seg_dest = destination
                        seg_flight = flight_no_str
                        seg_airline = airline_code

                    flight_segments.append(FlightSegment(
                        airline=seg_airline,
                        flight_no=seg_flight,
                        origin=seg_origin,
                        destination=seg_dest,
                        departure=dep_dt,
                        arrival=arr_dt,
                    ))

                if not flight_segments:
                    continue

                # For connecting flights, use layover info for better segment times
                layovers = d0.get("layover", [])
                if len(flight_segments) > 1 and layovers:
                    # Adjust: only first and last segment have precise times
                    pass  # Times from flightKeys don't have per-segment times

                outbound = FlightRoute(
                    segments=flight_segments,
                    total_duration_seconds=int(duration_min) * 60,
                    stopovers=stops,
                )

                # Conditions
                refund_type = fare_item.get("refundableType", "")
                conditions = {}
                if refund_type == "REFUNDABLE":
                    conditions["refund_before_departure"] = "allowed"
                elif refund_type == "PARTIALLY_REFUNDABLE":
                    conditions["refund_before_departure"] = "allowed_with_fee"
                elif refund_type == "NON_REFUNDABLE":
                    conditions["refund_before_departure"] = "not_allowed"

                airlines = list({s.airline for s in flight_segments if s.airline})
                h = hashlib.md5(
                    f"ixg{flight_keys}{price}".encode()
                ).hexdigest()[:12]

                booking_url = (
                    f"https://www.ixigo.com/search/result/flight"
                    f"?from={req.origin}&to={req.destination}"
                    f"&date={req.date_from.strftime('%d%m%Y')}"
                    f"&adults={req.adults or 1}&children={req.children or 0}&infants=0&class=e"
                )

                offers.append(FlightOffer(
                    id=f"off_ixg_{h}",
                    price=float(price),
                    currency=currency,
                    outbound=outbound,
                    inbound=None,
                    airlines=airlines or [airline_code],
                    owner_airline=airline_name or airline_code or "iXigo",
                    source="ixigo_meta",
                    source_tier="meta",
                    booking_url=booking_url,
                    conditions=conditions,
                ))
            except Exception as e:
                logger.debug("IXIGO: skipped fare: %s", e)
                continue

    return offers
