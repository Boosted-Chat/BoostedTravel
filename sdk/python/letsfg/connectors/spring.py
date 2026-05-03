"""
Spring Airlines direct API scraper -- queries en.ch.com REST endpoints.

Spring Airlines (IATA: 9C) is a Chinese LCC headquartered in Shanghai.
Website: en.ch.com (English), flights.ch.com (Chinese).

API backend: en.ch.com (publicly accessible, no auth key required)
  - Flight search:     POST /Flights/SearchByTime
  - Low price calendar: POST /Flights/MinPriceTrends
  - City/routes:       GET  /Default/GetReRoutesByCity?CityCode={code}&Lang=en-us
  - City detection:    GET  /Default/GetCity

Parameters (form-encoded):
  Departure, Arrival (city codes), DepartureDate (YYYY-MM-DD),
  Currency, AdtNum, ChdNum, InfNum, IsIJFlight, SType.

Discovered via Playwright network interception + JS analysis, Mar 2026.
Rewritten from 539-line Playwright scraper to direct httpx API client.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Optional

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_httpx_proxy_url

logger = logging.getLogger(__name__)

_BASE = "https://en.ch.com"
_BASE_CN = "https://flights.ch.com"
_SEARCH_URL = f"{_BASE}/Flights/SearchByTime"
_SEARCH_URL_CN = f"{_BASE_CN}/Flights/SearchByTime"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{_BASE}/flights",
    "Origin": _BASE,
}

_HEADERS_CN = {
    **_HEADERS,
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": f"{_BASE_CN}/",
    "Origin": _BASE_CN,
}


class SpringConnectorClient:
    """Spring Airlines scraper -- direct httpx API client for SearchByTime."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
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
                    self._fetch_ancillaries(
                        anc_origin, anc_dest,
                        req.date_from.isoformat() if hasattr(req.date_from, 'isoformat') else str(req.date_from),
                        req.adults, ob_result.currency or req.currency,
                    ),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s->%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error: %s", _anc_err)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")

        form_data = {
            "Departure": req.origin,
            "Arrival": req.destination,
            "DepartureDate": date_str,
            "ReturnDate": req.return_from.strftime("%Y-%m-%d") if req.return_from else "",
            "IsIJFlight": "false",
            "SType": "0",
            "Currency": req.currency or "CNY",
            "AdtNum": str(req.adults),
            "ChdNum": str(req.children),
            "InfNum": str(req.infants),
        }

        # Try English site first, then Chinese site for domestic routes
        outbound_offers: list[FlightOffer] = []
        for base, search_url, headers, cookies in (
            (_BASE, _SEARCH_URL, _HEADERS, {"lang": "en-us"}),
            (_BASE_CN, _SEARCH_URL_CN, _HEADERS_CN, {"lang": "zh-cn"}),
        ):
            data = await self._api_call(base, search_url, form_data, headers, cookies)
            if data and data.get("Code") == "0" and data.get("Route"):
                outbound_offers = self._parse_routes(data, req)
                if outbound_offers:
                    break

        if not outbound_offers:
            logger.info("Spring %s->%s: no results from either site", req.origin, req.destination)
            return self._empty(req)

        # ── Round-trip: search return leg + build combos ──
        if req.return_from and outbound_offers:
            inbound_offers = await self._search_return(req)
            if inbound_offers:
                combos = self._build_rt_combos(outbound_offers, inbound_offers, req)
                if combos:
                    outbound_offers = combos + outbound_offers

        elapsed = time.monotonic() - t0
        outbound_offers.sort(key=lambda o: o.price)
        logger.info(
            "Spring %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(outbound_offers), elapsed,
        )
        search_id = hashlib.md5(
            f"spring{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CNY",
            offers=outbound_offers,
            total_results=len(outbound_offers),
        )

    async def _search_return(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Search return leg (reversed origin/dest on return date)."""
        ret_date = req.return_from.strftime("%Y-%m-%d")
        form_data = {
            "Departure": req.destination,
            "Arrival": req.origin,
            "DepartureDate": ret_date,
            "ReturnDate": "",
            "IsIJFlight": "false",
            "SType": "0",
            "Currency": req.currency or "CNY",
            "AdtNum": str(req.adults),
            "ChdNum": str(req.children),
            "InfNum": str(req.infants),
        }
        for base, search_url, headers, cookies in (
            (_BASE, _SEARCH_URL, _HEADERS, {"lang": "en-us"}),
            (_BASE_CN, _SEARCH_URL_CN, _HEADERS_CN, {"lang": "zh-cn"}),
        ):
            data = await self._api_call(base, search_url, form_data, headers, cookies)
            if data and data.get("Code") == "0" and data.get("Route"):
                from copy import deepcopy
                ret_req = deepcopy(req)
                ret_req.origin, ret_req.destination = req.destination, req.origin
                ret_req.date_from = req.return_from
                offers = self._parse_routes(data, ret_req)
                if offers:
                    return offers
        return []

    def _build_rt_combos(
        self,
        outbound: list[FlightOffer],
        inbound: list[FlightOffer],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Combine outbound × inbound into RT offers."""
        combos: list[FlightOffer] = []
        for ob in outbound[:15]:
            for ib in inbound[:10]:
                price = round(ob.price + ib.price, 2)
                combo_key = f"9c_rt_{ob.id}_{ib.id}"
                combos.append(FlightOffer(
                    id=f"9c_{hashlib.md5(combo_key.encode()).hexdigest()[:12]}",
                    price=price,
                    currency=ob.currency,
                    price_formatted=f"{price:.0f} {ob.currency}",
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    airlines=list(set(ob.airlines + ib.airlines)),
                    owner_airline="9C",
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source="spring_direct",
                    source_tier="free",
                ))
        combos.sort(key=lambda o: o.price)
        return combos[:50]

    async def _api_call(
        self, base: str, search_url: str, form_data: dict,
        headers: dict, cookies: dict,
    ) -> dict | None:
        """Make a single API call to a Spring Airlines endpoint."""
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, follow_redirects=True,
                cookies=cookies,
                proxy=get_httpx_proxy_url(),) as client:
                # Get session cookie
                await client.get(
                    base,
                    headers={"User-Agent": headers["User-Agent"], "Accept": "text/html,*/*"},
                )
                resp = await client.post(search_url, data=form_data, headers=headers)
        except httpx.HTTPError as exc:
            logger.debug("Spring API call to %s failed: %s", base, exc)
            return None

        if resp.status_code != 200:
            logger.debug("Spring %s returned %d", base, resp.status_code)
            return None

        try:
            return resp.json()
        except Exception:
            return None

    def _parse_routes(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        raw_routes = data.get("Route", [])
        if not raw_routes:
            return []

        booking_url = self._booking_url(req)
        offers: list[FlightOffer] = []
        currency = req.currency or "CNY"

        for route in raw_routes:
            if not isinstance(route, list) or not route:
                continue

            # Each route is a list of segment dicts (1 for direct, 2+ for connecting)
            first = route[0]
            price = first.get("MinCabinPrice") or first.get("MinCabinPriceForDisplay") or 0
            if price <= 0:
                continue

            tax = first.get("RouteTotalTax", 0) or 0
            total_price = price + tax

            segments: list[FlightSegment] = []
            stopovers_list = first.get("Stopovers", [])

            if stopovers_list:
                # Connecting flight — segments come from Stopovers
                for stop in stopovers_list:
                    segments.append(self._build_segment(stop, req))
            else:
                # Direct flight — build from the route itself
                segments.append(self._build_segment(first, req))

            if not segments:
                continue

            # Calculate total duration
            total_dur = 0
            flight_time = first.get("FlightsTime", "") or first.get("FlightTime", "")
            if flight_time:
                total_dur = self._parse_duration(flight_time)
            elif segments[0].departure and segments[-1].arrival:
                delta = segments[-1].arrival - segments[0].departure
                total_dur = max(int(delta.total_seconds()), 0)

            route_obj = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=max(len(segments) - 1, 0),
            )

            seg_id = first.get("SegmentId", "")
            fid = hashlib.md5(
                f"9c_{req.origin}{req.destination}{seg_id}{price}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"9c_{fid}",
                price=round(total_price, 2),
                currency=currency,
                price_formatted=f"{total_price:.0f} {currency}",
                outbound=route_obj,
                inbound=None,
                airlines=["Spring Airlines"],
                owner_airline="9C",
                booking_url=booking_url,
                is_locked=False,
                source="spring_direct",
                source_tier="free",
            ))

        return offers

    def _build_segment(self, seg: dict, req: FlightSearchRequest) -> FlightSegment:
        flight_no = seg.get("No", "") or ""
        origin_code = seg.get("DepartureCode") or seg.get("DepartureAirportCode") or req.origin
        dest_code = seg.get("ArrivalCode") or seg.get("ArrivalAirportCode") or req.destination
        dep_str = seg.get("DepartureTime") or seg.get("DepartureTimeBJ") or ""
        arr_str = seg.get("ArrivalTime") or seg.get("ArrivalTimeBJ") or ""
        aircraft = seg.get("Type", "") or ""

        dep_dt = self._parse_dt(dep_str)
        arr_dt = self._parse_dt(arr_str)
        dur = 0
        if dep_dt and arr_dt:
            dur = max(int((arr_dt - dep_dt).total_seconds()), 0)

        return FlightSegment(
            airline="9C",
            airline_name="Spring Airlines",
            flight_no=flight_no,
            origin=origin_code,
            destination=dest_code,
            departure=dep_dt or datetime(2000, 1, 1),
            arrival=arr_dt or datetime(2000, 1, 1),
            duration_seconds=dur,
            cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy"),
            aircraft=aircraft,
        )

    @staticmethod
    def _parse_duration(s: str) -> int:
        """Parse '2 H 40 M' or '2H40M' to seconds."""
        import re
        m = re.search(r'(\d+)\s*[Hh]', s)
        mins_match = re.search(r'(\d+)\s*[Mm]', s)
        hours = int(m.group(1)) if m else 0
        mins = int(mins_match.group(1)) if mins_match else 0
        return hours * 3600 + mins * 60

    @staticmethod
    def _parse_dt(raw: str) -> Optional[datetime]:
        if not raw:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return None

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://en.ch.com/flights/{req.origin}-{req.destination}.html"
            f"?departure={dep}&adults={req.adults}&tripType={'RT' if req.return_from else 'OW'}"
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
                    id=f"rt_spri_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
    ) -> dict | None:
        """Return Spring Airlines (9C) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("9C")
        if not ref:
            return None
        carry_on = ref.get("carry_on")
        checked = ref.get("checked_bag")
        seat = ref.get("seat")
        ref_cur = ref.get("currency", "USD")

        # Carry-on note
        if note := ref.get("carry_on_note"):
            bags_note = note
        elif carry_on == 0.0:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"1 cabin bag included ({kg} kg)"
        elif carry_on is not None:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"cabin bag from ~{ref_cur} {carry_on:.0f} ({kg} kg)"
        else:
            bags_note = None

        # Checked bag note
        if note := ref.get("checked_bag_note"):
            checked_note = note
        elif checked == 0.0:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"1 checked bag included ({kg} kg)"
        elif checked is not None:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"checked bag from ~{ref_cur} {checked:.0f} ({kg} kg)"
        else:
            checked_note = None

        # Seat note
        if seat == 0.0:
            seat_note = "seat selection included"
        elif seat is not None:
            seat_note = f"seat selection from ~{ref_cur} {seat:.0f}"
        else:
            seat_note = "seat selection available at checkout"

        return {
            "bags_from": carry_on,
            "bags_note": bags_note,
            "checked_bag_from": checked,
            "checked_bag_note": checked_note,
            "seat_from": seat,
            "seat_note": seat_note,
            "currency": ref_cur,
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag_note")
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_from")
        seat_from = ancillary.get("seat_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions["checked_bag"] = checked_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from is not None:
                offer.bags_price.setdefault("carry_on", bags_from)
            if checked_from is not None:
                offer.bags_price.setdefault("checked_bag", checked_from)
            if seat_from is not None:
                offer.bags_price.setdefault("seat", seat_from)

    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        search_id = hashlib.md5(
            f"spring{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CNY",
            offers=[],
            total_results=0,
        )
