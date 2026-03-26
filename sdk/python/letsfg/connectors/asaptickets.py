"""
ASAP Tickets connector — Playwright browser + API response interception.

ASAP Tickets (asaptickets.com) is part of the Dyninno Group.  Next.js
frontend that markets "unpublished fares" with phone-booking emphasis.
The site fires XHR search requests to a backend API; we intercept those.

Strategy:
1. Launch Playwright browser (non-headless, offscreen).
2. Navigate to search results URL.
3. Intercept JSON responses containing flight itineraries.
4. Parse offers from captured data.
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


def _parse_segment(seg: dict) -> FlightSegment | None:
    origin = (
        seg.get("departureAirport") or seg.get("origin")
        or seg.get("from") or seg.get("departure", {}).get("airport")
        or seg.get("dep") or ""
    )
    dest = (
        seg.get("arrivalAirport") or seg.get("destination")
        or seg.get("to") or seg.get("arrival", {}).get("airport")
        or seg.get("arr") or ""
    )
    dep_time = (
        seg.get("departureDateTime") or seg.get("departureTime")
        or seg.get("departure", {}).get("dateTime")
        or seg.get("depTime") or ""
    )
    arr_time = (
        seg.get("arrivalDateTime") or seg.get("arrivalTime")
        or seg.get("arrival", {}).get("dateTime")
        or seg.get("arrTime") or ""
    )
    airline = (
        seg.get("airlineName") or seg.get("airline")
        or seg.get("carrier") or seg.get("marketingCarrier") or ""
    )
    airline_code = seg.get("airlineCode") or seg.get("carrierCode") or ""
    flight_no = seg.get("flightNumber") or seg.get("flightNo") or ""
    if airline_code and flight_no:
        flight_no = f"{airline_code}{flight_no}"
    if not origin or not dest:
        return None
    dep_dt = _parse_dt(dep_time)
    arr_dt = _parse_dt(arr_time)
    dur = max(0, int((arr_dt - dep_dt).total_seconds())) if dep_dt.year > 2000 and arr_dt.year > 2000 else 0
    return FlightSegment(
        airline=str(airline),
        flight_no=str(flight_no),
        origin=str(origin)[:3].upper(),
        destination=str(dest)[:3].upper(),
        departure=dep_dt,
        arrival=arr_dt,
        duration_seconds=dur,
    )


def _build_route(segs_raw: list) -> FlightRoute | None:
    segments = []
    for seg in segs_raw:
        if isinstance(seg, dict):
            fs = _parse_segment(seg)
            if fs:
                segments.append(fs)
    if not segments:
        return None
    return FlightRoute(
        segments=segments,
        total_duration_seconds=_dur_seconds(segments),
        stopovers=max(0, len(segments) - 1),
    )


def _extract_offers(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    offers: list[FlightOffer] = []
    currency = req.currency or "USD"

    results = (
        data.get("flights") or data.get("results")
        or data.get("itineraries") or data.get("offers")
        or data.get("data", {}).get("flights")
        or data.get("data", {}).get("results")
        or data.get("data", {}).get("itineraries")
        or data.get("data", {}).get("offers")
        or data.get("searchResults")
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
            price = float(
                itin.get("totalPrice") or itin.get("price")
                or itin.get("amount") or itin.get("total")
                or itin.get("fare", {}).get("total") or 0
            )
            if price <= 0:
                continue

            cur = itin.get("currency") or itin.get("currencyCode") or currency

            # Outbound
            ob_raw = (
                itin.get("outbound", {}).get("segments")
                or itin.get("outboundSegments")
                or (itin.get("legs", [{}])[0].get("segments") if itin.get("legs") else None)
                or itin.get("segments")
                or itin.get("slices", [{}])[0].get("segments") if itin.get("slices") else None
            )
            if not ob_raw and itin.get("legs") and isinstance(itin["legs"], list):
                ob_raw = itin["legs"][0].get("segments") if isinstance(itin["legs"][0], dict) else None
            if not ob_raw and itin.get("slices") and isinstance(itin["slices"], list):
                ob_raw = itin["slices"][0].get("segments") if isinstance(itin["slices"][0], dict) else None

            outbound = _build_route(ob_raw) if ob_raw else None
            if not outbound:
                continue

            # Inbound
            inbound = None
            ib_raw = (
                itin.get("inbound", {}).get("segments")
                or itin.get("inboundSegments")
                or itin.get("returnSegments")
                or None
            )
            if not ib_raw and itin.get("legs") and len(itin.get("legs", [])) > 1:
                ib_raw = itin["legs"][1].get("segments") if isinstance(itin["legs"][1], dict) else None
            if not ib_raw and itin.get("slices") and len(itin.get("slices", [])) > 1:
                ib_raw = itin["slices"][1].get("segments") if isinstance(itin["slices"][1], dict) else None
            if ib_raw:
                inbound = _build_route(ib_raw)

            airlines = list({s.airline for s in outbound.segments if s.airline})
            if not airlines:
                airlines = ["ASAP Tickets"]

            h = hashlib.md5(
                f"asap{req.origin}{req.destination}{i}{price}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"off_asap_{h}",
                source="asaptickets_ota",
                price=price,
                currency=cur,
                airlines=airlines,
                owner_airline=airlines[0],
                outbound=outbound,
                inbound=inbound,
                deep_link="https://www.asaptickets.com",
                booking_url="https://www.asaptickets.com",
            ))
        except Exception as e:
            logger.debug("ASAP parse offer %d: %s", i, e)

    return offers


class AsapticketsConnectorClient:
    """ASAP Tickets — Dyninno Group OTA, Playwright + API interception."""

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
                        "ASAPTICKETS %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"asap{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_asap_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("ASAPTICKETS attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            hit = any(k in url.lower() for k in [
                "/api/search", "/api/flight", "/search/result",
                "/_next/data", "/flights/search", "/getresult",
                "/ajax/", "searchapi", "/v1/flight", "/v2/flight",
            ])
            if not hit:
                return
            try:
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        body = await response.text()
                        if len(body) > 2000:
                            data = json.loads(body)
                            if isinstance(data, dict):
                                api_responses.append(data)
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("ASAPTICKETS_PROXY")
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

            dep = req.date_from.strftime("%Y-%m-%d")
            adults = req.adults or 1

            # ASAP Tickets search URL  (Next.js routing)
            url = (
                f"https://www.asaptickets.com/flights/"
                f"{req.origin}-{req.destination}/{dep}"
                f"?adults={adults}&cabin=economy"
            )
            if req.return_from:
                ret = req.return_from.strftime("%Y-%m-%d")
                url += f"&return={ret}"

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Fallback: try alternate URL pattern
            if not api_responses:
                alt_url = (
                    f"https://www.asaptickets.com/search"
                    f"?from={req.origin}&to={req.destination}"
                    f"&departure={dep}&adults={adults}"
                )
                if req.return_from:
                    alt_url += f"&return={req.return_from.strftime('%Y-%m-%d')}"
                try:
                    await page.goto(alt_url, wait_until="domcontentloaded", timeout=20000)
                except Exception:
                    pass

            for _ in range(15):
                await page.wait_for_timeout(2500)
                if api_responses:
                    await page.wait_for_timeout(4000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("ASAPTICKETS browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("ASAPTICKETS: no flight API response captured")
            return None

        all_offers: list[FlightOffer] = []
        for resp_data in api_responses:
            all_offers.extend(_extract_offers(resp_data, req))

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
