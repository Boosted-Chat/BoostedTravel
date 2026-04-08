"""
CheapOair connector — Playwright browser + API response interception.

CheapOair (Fareportal group) is a major US OTA specialising in consolidator
fares. Also powers OneTravel.com (same backend).

Strategy:
1.  Launch Playwright browser (non-headless for anti-bot).
2.  Navigate to CheapOair search results URL.
3.  Intercept JSON API responses containing flight/itinerary data.
4.  Parse response into FlightOffers.
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


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(s[:len(fmt.replace("%", "X"))], fmt)
        except (ValueError, IndexError):
            continue
    try:
        return datetime.fromisoformat(s.split("+")[0].split(".")[0])
    except Exception:
        return datetime(2000, 1, 1)


class CheapoairConnectorClient:
    """CheapOair (Fareportal) — Playwright + API interception."""

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
                        "CHEAPOAIR %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"cheapoair{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_co_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("CHEAPOAIR attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright

        api_data: list[dict] = []

        async def on_response(response):
            url = response.url
            try:
                ct = response.headers.get("content-type", "")
                if "json" not in ct and "javascript" not in ct:
                    return
                if response.status != 200:
                    return
                # Broad filter: look for flight search API responses
                keywords = ("itinerar", "flight", "fare", "airResult", "search")
                if not any(k in url.lower() for k in keywords):
                    return
                body = await response.text()
                if len(body) < 5000:
                    return
                data = json.loads(body)
                if isinstance(data, dict):
                    # Look for list-like structures with prices
                    for key in ("itineraries", "flights", "airResults",
                                "results", "fares", "data", "offers"):
                        val = data.get(key)
                        if isinstance(val, list) and len(val) > 2:
                            api_data.append(data)
                            logger.info("CHEAPOAIR: captured %s (%d items) from %s",
                                        key, len(val), url[:80])
                            return
                    # Check nested data.results
                    inner = data.get("data")
                    if isinstance(inner, dict):
                        for key in ("itineraries", "flights", "results", "fares"):
                            val = inner.get(key)
                            if isinstance(val, list) and len(val) > 2:
                                api_data.append(data)
                                logger.info("CHEAPOAIR: captured data.%s from %s",
                                            key, url[:80])
                                return
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            from .browser import get_proxy
            proxy = get_proxy("CHEAPOAIR_PROXY")
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
                from .browser import auto_block_if_proxied
                await auto_block_if_proxied(page)
            page.on("response", on_response)

            # CheapOair URL format
            dep = req.date_from.strftime("%m/%d/%Y")
            is_rt = bool(req.return_from)
            url = (
                f"https://www.cheapoair.com/flights/results"
                f"?fl_dep_apt={req.origin}"
                f"&fl_arr_apt={req.destination}"
                f"&fl_dep_dt={dep}"
                f"&fl_ADT={req.adults or 1}"
                f"&fl_CHD={req.children or 0}"
                f"&fl_INF=0"
                f"&fl_class=Economy"
                f"&tripType={'2' if is_rt else '1'}"
            )
            if is_rt:
                url += f"&fl_ret_dt={req.return_from.strftime('%m/%d/%Y')}"

            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            # Dismiss cookie/consent banners
            for sel in [
                "button:has-text('Accept')",
                "button:has-text('Got it')",
                "#onetrust-accept-btn-handler",
            ]:
                try:
                    btn = page.locator(sel)
                    if await btn.count() > 0:
                        await btn.first.click(force=True, timeout=2000)
                        break
                except Exception:
                    pass

            # Wait for API response
            for _ in range(8):
                await page.wait_for_timeout(5000)
                if api_data:
                    await page.wait_for_timeout(3000)  # extra wait for more results
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("CHEAPOAIR browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_data:
            logger.warning("CHEAPOAIR: no flight API response captured")
            return None

        return _parse_cheapoair(api_data[0], req)

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
                    id=f"rt_co_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_cheapoair(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse CheapOair API response into FlightOffer list.

    Fareportal uses various response formats across their brands.
    This parser handles the common patterns.
    """
    offers: list[FlightOffer] = []

    # Try several known keys for the itinerary list
    itins = None
    for key in ("itineraries", "flights", "airResults", "results", "fares", "offers"):
        itins = data.get(key)
        if isinstance(itins, list) and len(itins) > 0:
            break
        inner = data.get("data", {})
        if isinstance(inner, dict):
            itins = inner.get(key)
            if isinstance(itins, list) and len(itins) > 0:
                break

    if not itins:
        return offers

    for itin in itins:
        try:
            if not isinstance(itin, dict):
                continue

            # Extract price
            price = 0.0
            currency = req.currency or "USD"
            for pkey in ("totalPrice", "price", "total", "fare", "totalFare",
                         "displayPrice", "grandTotal"):
                val = itin.get(pkey)
                if isinstance(val, (int, float)) and val > 0:
                    price = float(val)
                    break
                if isinstance(val, dict):
                    amt = val.get("amount") or val.get("value") or val.get("total")
                    if amt and float(amt) > 0:
                        price = float(amt)
                        currency = val.get("currency", currency)
                        break
            if price <= 0:
                # Try nested pricing
                pricing = itin.get("pricing") or itin.get("fareBreakdown") or {}
                if isinstance(pricing, dict):
                    price = float(pricing.get("total", 0) or pricing.get("grandTotal", 0))
            if price <= 0:
                continue

            # Extract segments
            legs = itin.get("legs") or itin.get("slices") or itin.get("segments") or []
            if not legs:
                # Maybe flat segment list
                legs = [itin] if itin.get("flights") or itin.get("segments") else []
            if not legs:
                continue

            outbound_leg = legs[0] if legs else {}
            segments_data = (
                outbound_leg.get("segments") or
                outbound_leg.get("flights") or
                outbound_leg.get("flightSegments") or
                []
            )

            flight_segments: list[FlightSegment] = []
            for seg in segments_data:
                if not isinstance(seg, dict):
                    continue
                airline = (
                    seg.get("airlineCode") or seg.get("airline") or
                    seg.get("marketingCarrier") or seg.get("carrier", "")
                )
                flight_no = (
                    seg.get("flightNumber") or seg.get("flightNo") or
                    seg.get("number", "")
                )
                origin = (
                    seg.get("departureAirport") or seg.get("origin") or
                    seg.get("from", req.origin)
                )
                dest = (
                    seg.get("arrivalAirport") or seg.get("destination") or
                    seg.get("to", req.destination)
                )
                dep_time = (
                    seg.get("departureDateTime") or seg.get("departure") or
                    seg.get("departTime", "")
                )
                arr_time = (
                    seg.get("arrivalDateTime") or seg.get("arrival") or
                    seg.get("arrivalTime", "")
                )

                # Handle nested objects
                if isinstance(origin, dict):
                    origin = origin.get("code") or origin.get("iata", req.origin)
                if isinstance(dest, dict):
                    dest = dest.get("code") or dest.get("iata", req.destination)
                if isinstance(airline, dict):
                    airline = airline.get("code") or airline.get("iata", "")

                flight_segments.append(FlightSegment(
                    airline=str(airline),
                    flight_no=f"{airline}{flight_no}" if flight_no else str(airline),
                    origin=str(origin),
                    destination=str(dest),
                    departure=_parse_dt(dep_time),
                    arrival=_parse_dt(arr_time),
                ))

            if not flight_segments:
                continue

            # Duration
            dur = outbound_leg.get("duration") or itin.get("duration") or 0
            if isinstance(dur, str):
                # Parse "PT5H30M" or "5h 30m" format
                dur = 0  # fallback
            total_dur = int(dur) * 60 if dur and int(dur) < 2000 else int(dur) if dur else 0

            stopovers = max(0, len(flight_segments) - 1)
            outbound = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=total_dur,
                stopovers=stopovers,
            )

            # Inbound (if round-trip)
            inbound = None
            if len(legs) > 1 and req.return_from:
                ib_leg = legs[1]
                ib_segments_data = (
                    ib_leg.get("segments") or
                    ib_leg.get("flights") or
                    ib_leg.get("flightSegments") or
                    []
                )
                ib_segs: list[FlightSegment] = []
                for seg in ib_segments_data:
                    if not isinstance(seg, dict):
                        continue
                    airline = (
                        seg.get("airlineCode") or seg.get("airline") or
                        seg.get("marketingCarrier") or seg.get("carrier", "")
                    )
                    flight_no = (
                        seg.get("flightNumber") or seg.get("flightNo") or
                        seg.get("number", "")
                    )
                    seg_origin = (
                        seg.get("departureAirport") or seg.get("origin") or
                        seg.get("from", req.destination)
                    )
                    seg_dest = (
                        seg.get("arrivalAirport") or seg.get("destination") or
                        seg.get("to", req.origin)
                    )
                    dep_time = (
                        seg.get("departureDateTime") or seg.get("departure") or
                        seg.get("departTime", "")
                    )
                    arr_time = (
                        seg.get("arrivalDateTime") or seg.get("arrival") or
                        seg.get("arrivalTime", "")
                    )
                    if isinstance(seg_origin, dict):
                        seg_origin = seg_origin.get("code") or seg_origin.get("iata", req.destination)
                    if isinstance(seg_dest, dict):
                        seg_dest = seg_dest.get("code") or seg_dest.get("iata", req.origin)
                    if isinstance(airline, dict):
                        airline = airline.get("code") or airline.get("iata", "")
                    ib_segs.append(FlightSegment(
                        airline=str(airline),
                        flight_no=f"{airline}{flight_no}" if flight_no else str(airline),
                        origin=str(seg_origin),
                        destination=str(seg_dest),
                        departure=_parse_dt(dep_time),
                        arrival=_parse_dt(arr_time),
                    ))
                if ib_segs:
                    ib_dur = ib_leg.get("duration") or 0
                    if isinstance(ib_dur, str):
                        ib_dur = 0
                    ib_total_dur = int(ib_dur) * 60 if ib_dur and int(ib_dur) < 2000 else int(ib_dur) if ib_dur else 0
                    inbound = FlightRoute(
                        segments=ib_segs,
                        total_duration_seconds=ib_total_dur,
                        stopovers=max(0, len(ib_segs) - 1),
                    )

            airlines = list({s.airline for s in flight_segments if s.airline})
            booking_url = (
                itin.get("deepLink") or itin.get("bookingUrl") or
                f"https://www.cheapoair.com/"
            )

            h = hashlib.md5(
                f"co{req.origin}{req.destination}{price}{len(flight_segments)}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"off_co_{h}",
                price=price,
                currency=currency,
                outbound=outbound,
                inbound=inbound,
                airlines=airlines,
                owner_airline=airlines[0] if airlines else "CheapOair",
                source="cheapoair",
                source_tier="ota",
                booking_url=booking_url,
            ))
        except Exception as e:
            logger.debug("CHEAPOAIR: skipped itinerary: %s", e)
            continue

    return offers
