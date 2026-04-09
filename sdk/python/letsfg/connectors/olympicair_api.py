"""
Olympic Air connector -- direct low-fare calendar JSON via curl_cffi.

Olympic Air (IATA: OA) is the Greek regional carrier in the Aegean Group.
Its public low-fare calendar page calls a JSON endpoint that returns daily
lowest fares for a route and month without requiring a browser session.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import date, datetime, timezone

from curl_cffi import requests as creq

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_curl_cffi_proxies

logger = logging.getLogger(__name__)

_BASE = "https://www.olympicair.com"
_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.olympicair.com/en/flight-deals/low-fare-calendar/",
}
_DATE_RE = re.compile(r"Date\((\d+)\)")


def _as_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


class OlympicAirConnectorClient:
    def __init__(self, timeout: float = 20.0):
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
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        started = time.monotonic()

        try:
            payload = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_month_sync, req.origin, req.destination, req.date_from, req.return_from
            )
        except Exception as exc:
            logger.warning("Olympic Air fetch failed for %s->%s: %s", req.origin, req.destination, exc)
            return self._empty(req)

        offers = self._build_offers(payload, req)
        offers.sort(key=lambda offer: offer.price if offer.price > 0 else float("inf"))
        logger.info(
            "Olympic Air %s->%s: %d offers in %.1fs",
            req.origin,
            req.destination,
            len(offers),
            time.monotonic() - started,
        )

        search_hash = hashlib.md5(
            f"olympicair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    def _fetch_month_sync(self, origin: str, destination: str, date_from: date | datetime, return_from: date | datetime | None = None) -> dict:
        search_date = _as_date(date_from)
        month_value = f"{search_date.year}-{search_date.month}"
        trip_type = "RT" if return_from else "OW"
        ret_month = ""
        if return_from:
            ret_date = _as_date(return_from)
            ret_month = f"{ret_date.year}-{ret_date.month}"
        url = (
            f"{_BASE}/en/sys/lowfares/RouteLowFares/"
            f"?DepartureAirport={origin}"
            f"&ArrivalAirport={destination}"
            f"&TripType={trip_type}"
            f"&DepartureDate={month_value}"
            f"&ReturnDate={ret_month or month_value}"
            f"&SelectedDepartureDate="
            f"&SelectedReturnDate="
            f"&Type=Fares"
        )
        session = creq.Session(impersonate="chrome131", headers=_HEADERS, proxies=get_curl_cffi_proxies())
        response = session.get(url, timeout=self.timeout)
        if response.status_code != 200:
            raise RuntimeError(f"HTTP {response.status_code}")
        return json.loads(response.text)

    def _build_offers(self, payload: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        outbound = payload.get("Outbound") or []
        inbound_raw = payload.get("Inbound") or payload.get("Return") or []
        search_date = _as_date(req.date_from)

        # Build cheapest IB route if RT
        _ib_route: FlightRoute | None = None
        _ib_price = 0.0
        if req.return_from and inbound_raw:
            ret_date = _as_date(req.return_from)
            best_ib_price = float("inf")
            best_ib_item = None
            for ib_item in inbound_raw:
                ib_dep = self._parse_oa_date(ib_item.get("Date", ""))
                if ib_dep is None:
                    continue
                ib_p = ib_item.get("FullPrice") or ib_item.get("Price")
                if ib_p is None:
                    continue
                try:
                    ib_pf = float(ib_p)
                except (TypeError, ValueError):
                    continue
                if ib_pf <= 0:
                    continue
                # Prefer exact return date match
                if ib_dep.date() == ret_date and ib_pf < best_ib_price:
                    best_ib_price = ib_pf
                    best_ib_item = ib_item
            # Fallback: cheapest overall IB
            if best_ib_item is None:
                for ib_item in inbound_raw:
                    ib_dep = self._parse_oa_date(ib_item.get("Date", ""))
                    if ib_dep is None:
                        continue
                    ib_p = ib_item.get("FullPrice") or ib_item.get("Price")
                    if ib_p is None:
                        continue
                    try:
                        ib_pf = float(ib_p)
                    except (TypeError, ValueError):
                        continue
                    if ib_pf <= 0:
                        continue
                    if ib_pf < best_ib_price:
                        best_ib_price = ib_pf
                        best_ib_item = ib_item
            if best_ib_item and best_ib_price < float("inf"):
                _ib_price = round(best_ib_price, 2)
                ib_dep_dt = self._parse_oa_date(best_ib_item.get("Date", "")) or datetime(2000, 1, 1)
                _ib_route = FlightRoute(
                    segments=[FlightSegment(
                        airline="OA", airline_name="Olympic Air", flight_no="",
                        origin=req.destination, destination=req.origin,
                        departure=ib_dep_dt, arrival=ib_dep_dt,
                        duration_seconds=0, cabin_class="economy",
                    )],
                    total_duration_seconds=0, stopovers=0,
                )

        is_rt = _ib_route is not None
        travel_type = "R" if is_rt else "O"
        booking_url = (
            f"{_BASE}/en/flight-deals/low-fare-calendar/"
            f"?TravelType={travel_type}"
            f"&AirportFrom={req.origin}"
            f"&AirportTo={req.destination}"
            f"&MonthFrom={search_date.year}-{search_date.month}"
            f"&SelectedDepartureDate={search_date.strftime('%Y-%m-%d')}"
        )
        if is_rt and req.return_from:
            ret_date = _as_date(req.return_from)
            booking_url += f"&MonthTo={ret_date.year}-{ret_date.month}&SelectedReturnDate={ret_date.strftime('%Y-%m-%d')}"

        offers: list[FlightOffer] = []
        for item in outbound:
            departure = self._parse_oa_date(item.get("Date", ""))
            if departure is None or departure.date() != search_date:
                continue

            price = item.get("FullPrice") or item.get("Price")
            if price is None or float(price) <= 0:
                continue

            cabin = (item.get("Class") or "Economy").lower()
            price_value = round(float(price), 2)
            total_price = round(price_value + _ib_price, 2) if is_rt else price_value
            prefix = "oa_rt_" if is_rt else "oa_"
            segment = FlightSegment(
                airline="OA",
                airline_name="Olympic Air",
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                origin_city="",
                destination_city="",
                departure=departure,
                arrival=departure,
                duration_seconds=0,
                cabin_class=cabin,
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
            offer_hash = hashlib.md5(
                f"oa_{req.origin}{req.destination}{departure.date().isoformat()}{total_price}{cabin}".encode()
            ).hexdigest()[:12]
            offers.append(
                FlightOffer(
                    id=f"{prefix}{offer_hash}",
                    price=total_price,
                    currency="EUR",
                    price_formatted=f"EUR {total_price:.2f}",
                    outbound=route,
                    inbound=_ib_route,
                    airlines=["Olympic Air"],
                    owner_airline="OA",
                    booking_url=booking_url,
                    is_locked=False,
                    source="olympicair_direct",
                    source_tier="free",
                )
            )

        return offers

    @staticmethod
    def _parse_oa_date(raw_value: str) -> datetime | None:
        match = _DATE_RE.search(raw_value or "")
        if not match:
            return None
        millis = int(match.group(1))
        return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).replace(tzinfo=None)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"olympicair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
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
                    id=f"rt_olym_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
