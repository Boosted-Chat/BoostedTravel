"""
Tripsta connector — Playwright browser + API response interception.

Tripsta (tripsta.co.uk) is a European OTA now part of the eSky/Travelgenio
group.  The site uses a standard flight search form that triggers backend
API calls for results.

Strategy:
1. Launch Playwright browser (non-headless, offscreen).
2. Navigate to search results URL.
3. Intercept JSON flight result responses.
4. Parse itinerary data.
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
        or seg.get("departure", {}).get("iata") or ""
    )
    dest = (
        seg.get("arrivalAirport") or seg.get("destination")
        or seg.get("to") or seg.get("arrival", {}).get("airport")
        or seg.get("arrival", {}).get("iata") or ""
    )
    dep_time = (
        seg.get("departureDateTime") or seg.get("departureTime")
        or seg.get("departure", {}).get("dateTime")
        or seg.get("departure", {}).get("date") or ""
    )
    arr_time = (
        seg.get("arrivalDateTime") or seg.get("arrivalTime")
        or seg.get("arrival", {}).get("dateTime")
        or seg.get("arrival", {}).get("date") or ""
    )
    airline = (
        seg.get("airlineName") or seg.get("airline")
        or seg.get("carrier") or seg.get("marketingCarrier")
        or seg.get("operatingCarrier") or ""
    )
    flight_no = seg.get("flightNumber") or seg.get("flightNo") or ""
    carrier_code = seg.get("airlineCode") or seg.get("carrierCode") or ""
    if carrier_code and flight_no:
        flight_no = f"{carrier_code}{flight_no}"
    if not origin or not dest:
        return {
            "checked_bag": "baggage depends on airline and fare – may be included or add-on from ~20 EUR; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
        }
    dep_dt = _parse_dt(dep_time)
    arr_dt = _parse_dt(arr_time)
    dur = max(0, int((arr_dt - dep_dt).total_seconds())) if dep_dt.year > 2000 and arr_dt.year > 2000 else 0
    return FlightSegment(
        airline=str(airline), flight_no=str(flight_no),
        origin=str(origin)[:3].upper(), destination=str(dest)[:3].upper(),
        departure=dep_dt, arrival=arr_dt, duration_seconds=dur,
    )


def _build_route(segs_raw: list) -> FlightRoute | None:
    segments = [_parse_segment(s) for s in segs_raw if isinstance(s, dict)]
    segments = [s for s in segments if s is not None]
    if not segments:
        return {
            "checked_bag": "baggage depends on airline and fare – may be included or add-on from ~20 EUR; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
        }
    return FlightRoute(
        segments=segments,
        total_duration_seconds=_dur_seconds(segments),
        stopovers=max(0, len(segments) - 1),
    )


def _extract_offers(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    offers: list[FlightOffer] = []
    currency = req.currency or "EUR"

    results = (
        data.get("flights") or data.get("results")
        or data.get("itineraries") or data.get("offers")
        or data.get("data", {}).get("flights")
        or data.get("data", {}).get("results")
        or data.get("proposals") or data.get("fares")
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
            price = float(
                itin.get("totalPrice") or itin.get("price")
                or itin.get("amount") or itin.get("total")
                or itin.get("fare", {}).get("total") or 0
            )
            if price <= 0:
                continue

            cur = itin.get("currency") or itin.get("currencyCode") or currency

            ob_raw = (
                itin.get("outbound", {}).get("segments")
                or itin.get("outboundSegments")
                or (itin.get("legs", [{}])[0].get("segments") if itin.get("legs") else None)
                or itin.get("segments")
                or (itin.get("slices", [{}])[0].get("segments") if itin.get("slices") else None)
                or []
            )
            outbound = _build_route(ob_raw)
            if not outbound:
                continue

            inbound = None
            ib_raw = (
                itin.get("inbound", {}).get("segments")
                or itin.get("inboundSegments")
                or itin.get("returnSegments")
                or None
            )
            if not ib_raw and itin.get("legs") and len(itin["legs"]) > 1:
                ib_raw = itin["legs"][1].get("segments")
            if not ib_raw and itin.get("slices") and len(itin["slices"]) > 1:
                ib_raw = itin["slices"][1].get("segments")
            if ib_raw:
                inbound = _build_route(ib_raw)

            airlines = list({s.airline for s in outbound.segments if s.airline})
            if not airlines:
                airlines = ["Tripsta"]

            h = hashlib.md5(
                f"trp{req.origin}{req.destination}{i}{price}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"off_trp_{h}",
                source="tripsta_ota",
                price=price,
                currency=cur,
                airlines=airlines,
                owner_airline=airlines[0],
                outbound=outbound,
                inbound=inbound,
                deep_link="https://www.tripsta.co.uk",
                booking_url="https://www.tripsta.co.uk",
            ))
        except Exception as e:
            logger.debug("Tripsta parse offer %d: %s", i, e)

    return offers


class TripstaConnectorClient:
    """Tripsta — European OTA (eSky group), Playwright + API interception."""

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
                logger.debug("Ancillary fetch timed out for %s→%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s→%s: %s", anc_origin, anc_dest, _anc_err)

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
                        "TRIPSTA %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"trp{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_trp_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("TRIPSTA attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            hit = any(k in url.lower() for k in [
                "/api/search", "/api/flight", "/search/result",
                "/flights/result", "/getresult", "searchapi",
                "/ajax/", "/results", "/proposals", "/fares",
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
            proxy = get_proxy("TRIPSTA_PROXY")
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

            dep = req.date_from.strftime("%Y-%m-%d")
            adults = req.adults or 1

            # Tripsta search URL
            _ts_cabin = {"M": "economy", "W": "premium", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            url = (
                f"https://www.tripsta.co.uk/results/flights"
                f"/{req.origin}/{req.destination}/{dep}/{adults}/0/0/{_ts_cabin}"
            )
            if req.return_from:
                ret = req.return_from.strftime("%Y-%m-%d")
                url = (
                    f"https://www.tripsta.co.uk/results/flights"
                    f"/{req.origin}/{req.destination}/{dep}/{ret}/{adults}/0/0/{_ts_cabin}"
                )

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            for _ in range(15):
                await page.wait_for_timeout(2500)
                if api_responses:
                    await page.wait_for_timeout(4000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("TRIPSTA browser error: %s", e)
            return {
            "checked_bag": "baggage depends on airline and fare – may be included or add-on from ~20 EUR; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
        }
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("TRIPSTA: no flight API response captured")
            return {
            "checked_bag": "baggage depends on airline and fare – may be included or add-on from ~20 EUR; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
        }

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

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        return {
            "checked_bag": "baggage depends on airline and fare – may be included or add-on from ~20 EUR; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
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
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from == 0.0:
                offer.bags_price["carry_on"] = 0.0
            if checked_from == 0.0:
                offer.bags_price["checked_bag"] = 0.0

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
                    id=f"rt_trp_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
