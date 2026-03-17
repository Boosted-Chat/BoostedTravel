"""
Finnair connector — calendar lowfare API.

Finnair (IATA: AY) is Finland's flag carrier. Oneworld member.
Key for Nordic/Asia routes via HEL hub. 130+ destinations.

Strategy:
  Finnair.com uses a lowfare calendar API:
  GET https://www.finnair.com/api/ndc/offers/lowest-fares
  Params: origin, destination, departureDate, passengers, tripType
  Returns daily cheapest fares as JSON.
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

_BASE = "https://www.finnair.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": _BASE,
    "Referer": f"{_BASE}/",
}


class FinnairConnectorClient:
    """Finnair — NDC lowfare calendar API."""

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
        # Try multiple API patterns (Finnair has changed APIs)
        for endpoint in [
            f"{_BASE}/api/ndc/offers/lowest-fares",
            f"{_BASE}/api/calendar/lowest-fares",
        ]:
            params = {
                "origin": req.origin,
                "destination": req.destination,
                "departureDate": date_str,
                "adults": str(req.adults or 1),
                "children": str(req.children or 0),
                "infants": str(req.infants or 0),
                "tripType": "ONE_WAY",
                "cabin": "ECONOMY",
            }
            try:
                resp = await client.get(endpoint, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse(data, req, date_str)
                    if offers:
                        break
            except Exception as e:
                logger.debug("Finnair endpoint %s error: %s", endpoint, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Finnair %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"finnair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers, total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        fares = data.get("fares") or data.get("lowestFares") or data.get("dates") or []
        for fare in fares:
            dep = (fare.get("departureDate") or fare.get("date") or "")[:10]
            if dep != target_date:
                continue
            price = fare.get("price") or fare.get("amount") or fare.get("totalPrice") or 0
            currency = fare.get("currency") or fare.get("currencyCode") or "EUR"
            if float(price) <= 0:
                continue

            dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
            seg = FlightSegment(
                airline="Finnair", flight_no="AY", origin=req.origin,
                destination=req.destination, departure=dep_dt, arrival=dep_dt, duration_seconds=0,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
            oid = hashlib.md5(f"ay_{req.origin}{req.destination}{dep}{price}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"ay_{oid}", price=round(float(price), 2), currency=currency,
                price_formatted=f"{float(price):.2f} {currency}",
                outbound=route, inbound=None, airlines=["Finnair"], owner_airline="AY",
                booking_url=f"https://www.finnair.com/en/booking/flight-selection?origin={req.origin}&destination={req.destination}&departureDate={target_date}&adults={req.adults or 1}&tripType=oneway",
                is_locked=False, source="finnair_direct", source_tier="free",
            ))
        return offers

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="EUR", offers=[], total_results=0,
        )
