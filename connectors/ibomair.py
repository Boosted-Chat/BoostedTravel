"""
Ibom Air connector — Nigerian regional carrier direct API.

Ibom Air (IATA: QI) is a Nigerian airline based in Uyo, Akwa Ibom State.
Operates domestic Nigerian routes (Lagos, Abuja, Port Harcourt, Calabar,
Enugu, Uyo) and regional flights to Accra (Ghana).

Strategy (httpx direct API — availability search):
  1. POST to ibomair.com booking/availability API
  2. Parse JSON → FlightOffer objects
  3. Construct booking URL
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date, datetime, timedelta
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

_AVAIL_URL = "https://www.ibomair.com/api/booking/availability"
_SEARCH_URL = "https://www.ibomair.com/api/flights/search"
_LOWFARE_URL = "https://www.ibomair.com/api/booking/lowfare"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.ibomair.com",
    "Referer": "https://www.ibomair.com/booking",
}

# Key domestic Nigerian + regional airports
_IBOMAIR_STATIONS = {
    "LOS",  # Lagos Murtala Muhammed
    "ABV",  # Abuja Nnamdi Azikiwe
    "PHC",  # Port Harcourt
    "CBQ",  # Calabar
    "ENU",  # Enugu
    "QUO",  # Uyo (Akwa Ibom)
    "ACC",  # Accra, Ghana
}


class IbomAirConnectorClient:
    """Ibom Air (QI) — Nigerian airline, direct httpx API client."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination, "destination": req.origin,
                "date_from": req.return_from, "return_from": None,
            })
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            dt = (
                req.date_from
                if isinstance(req.date_from, (datetime, date))
                else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            )
            if isinstance(dt, datetime):
                dt = dt.date()
        except (ValueError, TypeError):
            dt = date.today() + timedelta(days=30)

        date_str = dt.strftime("%Y-%m-%d")
        pax = req.adults or 1

        payload_avail = {
            "origin": req.origin,
            "destination": req.destination,
            "departureDate": date_str,
            "passengerCount": {"adult": pax, "child": req.children or 0, "infant": req.infants or 0},
            "currency": "NGN",
        }

        payload_search = {
            "from": req.origin,
            "to": req.destination,
            "date": date_str,
            "adults": pax,
            "children": req.children or 0,
            "infants": req.infants or 0,
            "oneWay": True,
        }

        payload_lowfare = {
            "origin": req.origin,
            "destination": req.destination,
            "beginDate": date_str,
            "endDate": (dt + timedelta(days=3)).strftime("%Y-%m-%d"),
            "passengers": {"ADT": pax, "CHD": req.children or 0, "INF": req.infants or 0},
        }

        attempts = [
            (_AVAIL_URL, payload_avail),
            (_SEARCH_URL, payload_search),
            (_LOWFARE_URL, payload_lowfare),
        ]

        offers: list[FlightOffer] = []

        for url, payload in attempts:
            try:
                async with httpx.AsyncClient(
                    timeout=self.timeout, proxy=get_httpx_proxy_url(),
                ) as client:
                    resp = await client.post(url, headers=_HEADERS, json=payload)

                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse_results(data, req, date_str)
                    if offers:
                        break
                else:
                    logger.debug("IbomAir %s returned %d", url, resp.status_code)

            except httpx.HTTPError as exc:
                logger.debug("IbomAir %s failed: %s", url, exc)
            except Exception as exc:
                logger.debug("IbomAir error: %s", exc)

        offers.sort(key=lambda o: o.price)

        elapsed = time.monotonic() - t0
        logger.info(
            "IbomAir %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"ibomair{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "NGN",
            offers=offers,
            total_results=len(offers),
        )

    def _parse_results(
        self, data: dict, req: FlightSearchRequest, date_str: str
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        flights = (
            data.get("flights", data.get("availability", data.get("fares", data.get("data", []))))
        )
        if isinstance(flights, dict):
            flights = flights.get("items", flights.get("journeys", flights.get("flights", [])))
        if not isinstance(flights, list):
            return offers

        for flight in flights:
            if not isinstance(flight, dict):
                continue
            if flight.get("soldOut") or flight.get("unavailable"):
                continue

            price = (
                flight.get("price")
                or flight.get("totalPrice")
                or flight.get("fareAmount")
                or flight.get("amount")
                or flight.get("lowestFare")
            )
            if isinstance(price, dict):
                price = price.get("amount", price.get("value", price.get("total")))
            if not price:
                continue

            try:
                price_f = round(float(price), 2)
            except (ValueError, TypeError):
                continue
            if price_f <= 0:
                continue

            currency = flight.get("currency", flight.get("currencyCode", "NGN"))

            dedup_key = f"{req.origin}_{req.destination}_{date_str}_{price_f}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            flight_no = flight.get("flightNumber", flight.get("number", ""))

            dep_time = flight.get("departureTime", flight.get("departure", date_str))
            arr_time = flight.get("arrivalTime", flight.get("arrival", date_str))

            dep_dt = _parse_dt(dep_time, date_str)
            arr_dt = _parse_dt(arr_time, date_str)

            dur = flight.get("duration", flight.get("durationMinutes", 0))
            dur_sec = int(dur) * 60 if dur and int(dur) < 1000 else int(dur or 0)
            if dur_sec == 0 and dep_dt != arr_dt:
                dur_sec = max(0, int((arr_dt - dep_dt).total_seconds()))

            stops = int(flight.get("stops", flight.get("stopovers", 0)))

            _qi_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="QI",
                airline_name="Ibom Air",
                flight_no=str(flight_no),
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur_sec,
                cabin_class=_qi_cabin,
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=dur_sec,
                stopovers=stops,
            )

            fid = hashlib.md5(
                f"qi_{req.origin}{req.destination}{date_str}{price_f}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"qi_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Ibom Air"],
                owner_airline="QI",
                booking_url=(
                    f"https://www.ibomair.com/booking?"
                    f"from={req.origin}&to={req.destination}"
                    f"&date={date_str}&pax={req.adults or 1}"
                ),
                is_locked=False,
                source="ibomair_direct",
                source_tier="free",
            ))

        return offers

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
                    id=f"rt_qi_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"ibomair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="NGN",
            offers=[],
            total_results=0,
        )


def _parse_dt(value: Any, fallback_date: str) -> datetime:
    if isinstance(value, datetime):
        return value
    s = str(value) if value else fallback_date
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return datetime(2000, 1, 1)
