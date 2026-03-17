"""
EgyptAir connector — Egypt's flag carrier.

EgyptAir (IATA: MS) is Egypt's state-owned flag carrier. Star Alliance member.
CAI (Cairo) hub. 80+ destinations across Africa, Europe, Middle East, Asia, Americas.

Strategy:
  EgyptAir website at egyptair.com — try calendar lowfare API.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

import httpx

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.egyptair.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


class EgyptAirConnectorClient:
    """EgyptAir — calendar lowfare API."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        client = await self._client()
        date_str = req.date_from.strftime("%Y-%m-%d")

        offers = []
        for endpoint in [
            f"{_BASE}/api/offers/calendar-fares",
            f"{_BASE}/api/v1/lowest-fares",
        ]:
            params = {
                "origin": req.origin,
                "destination": req.destination,
                "departureDate": date_str,
                "adults": str(req.adults or 1),
                "tripType": "ONE_WAY",
            }
            try:
                resp = await client.get(endpoint, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse(data, req, date_str)
                    if offers:
                        break
            except Exception as e:
                logger.debug("EgyptAir endpoint %s error: %s", endpoint, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("EgyptAir %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"egyptair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "EGP",
            offers=offers, total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        fares = data.get("fares") or data.get("calendarFares") or data.get("dates") or []
        for fare in fares:
            dep = (fare.get("departureDate") or fare.get("date") or "")[:10]
            if dep != target_date:
                continue
            price = fare.get("price") or fare.get("amount") or fare.get("totalPrice") or 0
            currency = fare.get("currency") or "EGP"
            if float(price) <= 0:
                continue
            dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
            seg = FlightSegment(
                airline="EgyptAir", flight_no="MS", origin=req.origin,
                destination=req.destination, departure=dep_dt, arrival=dep_dt, duration_seconds=0,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
            oid = hashlib.md5(f"ms_{req.origin}{req.destination}{target_date}{price}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"ms_{oid}", price=round(float(price), 2), currency=currency,
                price_formatted=f"{float(price):.2f} {currency}",
                outbound=route, inbound=None, airlines=["EgyptAir"], owner_airline="MS",
                booking_url=f"https://www.egyptair.com/en/book-and-manage?origin={req.origin}&destination={req.destination}&date={target_date}&adults={req.adults or 1}",
                is_locked=False, source="egyptair_direct", source_tier="free",
            ))
        return offers

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="EGP", offers=[], total_results=0,
        )
