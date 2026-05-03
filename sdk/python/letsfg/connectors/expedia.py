"""
Expedia connector — Playwright browser + API response interception.

Expedia (expedia.com) is one of the world's largest OTAs, part of Expedia
Group (along with Hotels.com, Vrbo, Orbitz, Travelocity).  Protected by
Akamai Bot Manager.

Strategy:
1. Launch Playwright browser (non-headless, offscreen) — essential for Akamai.
2. Navigate to Expedia flight search results.
3. Intercept the flight search API responses (REST/GraphQL).
4. Parse offers from the captured JSON.
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
    """Parse a FlightSegment from Expedia's various response shapes."""
    origin = (
        seg.get("departureAirportCode") or seg.get("departureAirport")
        or seg.get("origin") or seg.get("from")
        or seg.get("departure", {}).get("airport")
        or seg.get("departure", {}).get("airportCode")
        or seg.get("departurePlaceId") or ""
    )
    dest = (
        seg.get("arrivalAirportCode") or seg.get("arrivalAirport")
        or seg.get("destination") or seg.get("to")
        or seg.get("arrival", {}).get("airport")
        or seg.get("arrival", {}).get("airportCode")
        or seg.get("arrivalPlaceId") or ""
    )
    dep_time = (
        seg.get("departureDateTime") or seg.get("departureTime")
        or seg.get("departure", {}).get("dateTime")
        or seg.get("departure", {}).get("time")
        or seg.get("departureDate") or ""
    )
    arr_time = (
        seg.get("arrivalDateTime") or seg.get("arrivalTime")
        or seg.get("arrival", {}).get("dateTime")
        or seg.get("arrival", {}).get("time")
        or seg.get("arrivalDate") or ""
    )
    airline = (
        seg.get("airlineName") or seg.get("airline")
        or seg.get("carrier", {}).get("name") if isinstance(seg.get("carrier"), dict) else None
        or seg.get("carrier") or seg.get("marketingCarrier")
        or seg.get("operatingCarrier") or ""
    )
    if not airline:
        airline = ""
    carrier_code = (
        seg.get("airlineCode") or seg.get("carrierCode")
        or seg.get("carrier", {}).get("code") if isinstance(seg.get("carrier"), dict) else None
        or seg.get("marketingCarrierCode") or ""
    )
    if not carrier_code:
        carrier_code = ""
    flight_no = (
        seg.get("flightNumber") or seg.get("flightNo")
        or seg.get("number") or ""
    )
    if carrier_code and flight_no:
        if not str(flight_no).startswith(str(carrier_code)):
            flight_no = f"{carrier_code}{flight_no}"

    if not origin or not dest:
        return None
    dep_dt = _parse_dt(dep_time)
    arr_dt = _parse_dt(arr_time)
    dur = max(0, int((arr_dt - dep_dt).total_seconds())) if dep_dt.year > 2000 and arr_dt.year > 2000 else 0

    # Expedia sometimes provides duration in minutes
    dur_min = seg.get("durationMinutes") or seg.get("duration") or 0
    if isinstance(dur_min, (int, float)) and dur_min > 0 and dur == 0:
        dur = int(dur_min) * 60

    return FlightSegment(
        airline=str(airline), flight_no=str(flight_no),
        origin=str(origin)[:3].upper(), destination=str(dest)[:3].upper(),
        departure=dep_dt, arrival=arr_dt, duration_seconds=dur,
    )


def _build_route(segs_raw: list) -> FlightRoute | None:
    segments = [_parse_segment(s) for s in segs_raw if isinstance(s, dict)]
    segments = [s for s in segments if s is not None]
    if not segments:
        return None
    total = _dur_seconds(segments)
    # Fallback: sum individual durations
    if total == 0:
        total = sum(s.duration_seconds for s in segments)
    return FlightRoute(
        segments=segments,
        total_duration_seconds=total,
        stopovers=max(0, len(segments) - 1),
    )


def _extract_offers(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse flight offers from Expedia's API response."""
    offers: list[FlightOffer] = []
    currency = req.currency or "USD"

    # Expedia response shapes:
    # 1. data.listings[] — main search results
    # 2. data.search.flights[] — GraphQL/persisted queries
    # 3. offers[] / itineraries[] — legacy formats
    # 4. content.listings[] — newer UI-driven format
    results = (
        data.get("listings")
        or data.get("offers")
        or data.get("itineraries")
        or data.get("flights")
        or data.get("results")
        or data.get("data", {}).get("listings")
        or data.get("data", {}).get("flights")
        or data.get("data", {}).get("search", {}).get("flights")
        or data.get("data", {}).get("search", {}).get("results")
        or data.get("content", {}).get("listings")
        or []
    )
    if isinstance(results, dict):
        results = results.get("items") or results.get("list") or list(results.values())
    if not isinstance(results, list):
        return offers

    for i, itin in enumerate(results[:80]):
        if not isinstance(itin, dict):
            continue
        try:
            # Price — Expedia has complex pricing structures
            price = 0.0
            # Direct price field
            for pk in ("totalPrice", "price", "rawPrice", "displayPrice", "amount"):
                val = itin.get(pk)
                if val:
                    if isinstance(val, str):
                        val = val.replace("$", "").replace(",", "").replace("€", "").replace("£", "")
                    price = float(val)
                    break
            # Nested pricing
            if price <= 0:
                pricing = (
                    itin.get("price") or itin.get("pricing")
                    or itin.get("priceDetails") or itin.get("fare")
                    or {}
                )
                if isinstance(pricing, dict):
                    for pk in ("total", "totalPrice", "amount", "displayTotal",
                               "totalFare", "grandTotal"):
                        val = pricing.get(pk)
                        if val:
                            if isinstance(val, dict):
                                val = val.get("amount") or val.get("value") or 0
                            if isinstance(val, str):
                                val = val.replace("$", "").replace(",", "")
                            price = float(val)
                            if price > 0:
                                break
            if price <= 0:
                continue

            cur = (
                itin.get("currency") or itin.get("currencyCode")
                or itin.get("price", {}).get("currency") if isinstance(itin.get("price"), dict) else None
                or currency
            )
            if not cur:
                cur = currency

            # Legs / slices
            legs = itin.get("legs") or itin.get("slices") or itin.get("journeys") or []
            ob_raw = None
            ib_raw = None

            if legs and isinstance(legs, list):
                if isinstance(legs[0], dict):
                    ob_raw = legs[0].get("segments") or legs[0].get("flights") or []
                    if len(legs) > 1 and isinstance(legs[1], dict):
                        ib_raw = legs[1].get("segments") or legs[1].get("flights") or []

            if not ob_raw:
                ob_raw = (
                    itin.get("outbound", {}).get("segments")
                    or itin.get("outboundSegments")
                    or itin.get("segments")
                    or []
                )

            outbound = _build_route(ob_raw) if ob_raw else None
            if not outbound:
                continue

            inbound = None
            if not ib_raw:
                ib_raw = (
                    itin.get("inbound", {}).get("segments")
                    or itin.get("inboundSegments")
                    or itin.get("returnSegments")
                    or None
                )
            if ib_raw:
                inbound = _build_route(ib_raw)

            airlines = list({s.airline for s in outbound.segments if s.airline})
            if not airlines:
                carrier = itin.get("carrierName") or itin.get("airline") or ""
                airlines = [carrier] if carrier else ["Expedia"]

            h = hashlib.md5(
                f"exp{req.origin}{req.destination}{i}{price}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"off_exp_{h}",
                source="expedia_meta",
                price=price,
                currency=cur,
                airlines=airlines,
                owner_airline=airlines[0],
                outbound=outbound,
                inbound=inbound,
                deep_link="https://www.expedia.com/Flights",
                booking_url="https://www.expedia.com/Flights",
            ))
        except Exception as e:
            logger.debug("Expedia parse offer %d: %s", i, e)

    return offers


class ExpediaConnectorClient:
    """Expedia — Global OTA (Akamai-protected), Playwright + API interception."""

    def __init__(self, timeout: float = 65.0):
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
                        "EXPEDIA %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"exp{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_exp_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("EXPEDIA attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            hit = any(k in url.lower() for k in [
                "/api/flight", "/lx/flight", "/graphql",
                "/flights/search", "/flights/listings",
                "/api/search", "/shopping/v1",
                "/offers/v2", "/search/results",
            ])
            if not hit:
                return
            try:
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        body = await response.text()
                        if len(body) > 5000:
                            data = json.loads(body)
                            if isinstance(data, dict):
                                api_responses.append(data)
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("EXPEDIA_PROXY")
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
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            if proxy:
                from .browser import auto_block_if_proxied
                await auto_block_if_proxied(page)
            page.on("response", on_response)

            dep = req.date_from.strftime("%m/%d/%Y")
            dep_iso = req.date_from.isoformat()
            adults = req.adults or 1
            cabin_map = {"M": "coach", "W": "premiumeconomy", "C": "business", "F": "first"}
            cabin = cabin_map.get(req.cabin_class or "M", "coach")

            # Expedia search URL (standard deep-link format)
            url = (
                f"https://www.expedia.com/Flights-search"
                f"?leg1=from:{req.origin},to:{req.destination},"
                f"departure:{dep}TANYT"
                f"&passengers=adults:{adults}"
                f"&options=cabinclass:{cabin}"
                f"&mode=search&trip=oneway"
            )
            if req.return_from:
                ret = req.return_from.strftime("%m/%d/%Y")
                url = (
                    f"https://www.expedia.com/Flights-search"
                    f"?leg1=from:{req.origin},to:{req.destination},"
                    f"departure:{dep}TANYT"
                    f"&leg2=from:{req.destination},to:{req.origin},"
                    f"departure:{ret}TANYT"
                    f"&passengers=adults:{adults}"
                    f"&options=cabinclass:{cabin}"
                    f"&mode=search&trip=roundtrip"
                )

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Expedia may redirect to a results page — wait for data
            for _ in range(16):
                await page.wait_for_timeout(3000)
                if api_responses:
                    await page.wait_for_timeout(5000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("EXPEDIA browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("EXPEDIA: no flight API response captured")
            return None

        all_offers: list[FlightOffer] = []
        for resp_data in api_responses:
            all_offers.extend(_extract_offers(resp_data, req))

        # Deduplicate
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
                    id=f"rt_exp_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        return {
            "checked_bag_note": "baggage depends on airline and fare – may be included or add-on from ~30 USD; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        pass  # conditions + bags_price set per-offer by engine's apply_ref_ancillaries via owner_airline
