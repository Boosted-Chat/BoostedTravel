"""
Traveloka connector — Southeast Asia's largest OTA.

Traveloka covers 100+ airlines across SE Asia, with exclusive fares
from AirAsia, Lion Group, Garuda, Cebu Pacific, VietJet, etc.
Often has OTA-exclusive promotional pricing not available on airline sites.

Strategy:
  Traveloka has a public flight search API at their API gateway.
  No auth required for initial search.
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

_BASE = "https://www.traveloka.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.traveloka.com",
    "Referer": "https://www.traveloka.com/en-id/flight",
}


class TravelokaConnectorClient:
    """Traveloka — SE Asia's largest OTA flight search."""

    def __init__(self, timeout: float = 25.0):
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

        offers: list[FlightOffer] = []

        # Try Traveloka's flight search endpoints
        endpoints = [
            f"{_BASE}/api/v2/flight/search",
            f"{_BASE}/api/flight/search",
        ]

        payload = {
            "journeys": [{
                "origin": req.origin,
                "destination": req.destination,
                "departureDate": date_str,
            }],
            "passengers": {
                "adults": req.adults or 1,
                "children": req.children or 0,
                "infants": req.infants or 0,
            },
            "cabinClass": "ECONOMY",
            "currency": "USD",
        }

        for endpoint in endpoints:
            try:
                resp = await client.post(endpoint, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse(data, req, date_str)
                    if offers:
                        break
            except Exception as e:
                logger.debug("Traveloka endpoint %s error: %s", endpoint, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Traveloka %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"traveloka{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers, total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        results = (
            data.get("flights") or data.get("data", {}).get("flights")
            or data.get("results") or data.get("itineraries") or []
        )
        for flight in results:
            price = (
                flight.get("price") or flight.get("totalPrice")
                or flight.get("fare", {}).get("total") or 0
            )
            currency = flight.get("currency") or "USD"
            if float(price) <= 0:
                continue

            airline_name = flight.get("airline") or flight.get("carrierName") or "Unknown"
            airline_code = flight.get("airlineCode") or flight.get("carrierCode") or ""
            flight_no = flight.get("flightNumber") or flight.get("flightNo") or airline_code

            dep_str = flight.get("departureTime") or flight.get("departure") or ""
            arr_str = flight.get("arrivalTime") or flight.get("arrival") or ""

            try:
                dep_dt = datetime.fromisoformat(dep_str.replace("Z", "+00:00")) if dep_str else datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
                arr_dt = datetime.fromisoformat(arr_str.replace("Z", "+00:00")) if arr_str else dep_dt
            except (ValueError, TypeError):
                dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
                arr_dt = dep_dt

            duration = flight.get("duration") or flight.get("durationMinutes") or 0
            duration_secs = int(duration) * 60 if duration else 0

            seg = FlightSegment(
                airline=airline_name, flight_no=flight_no,
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=arr_dt, duration_seconds=duration_secs,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=duration_secs, stopovers=0)
            oid = hashlib.md5(f"tvlk_{req.origin}{req.destination}{target_date}{price}{flight_no}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"tvlk_{oid}", price=round(float(price), 2), currency=currency,
                price_formatted=f"{float(price):.2f} {currency}",
                outbound=route, inbound=None,
                airlines=[airline_name], owner_airline=airline_code,
                booking_url=f"https://www.traveloka.com/en-id/flight/fullsearch?ap={req.origin}.{req.destination}&dt={target_date}&ps={req.adults or 1}.0.0&sc=ECONOMY",
                is_locked=False, source="traveloka_ota", source_tier="free",
            ))
        return offers
