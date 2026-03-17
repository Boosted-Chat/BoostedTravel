"""
Cleartrip connector — India's leading OTA.

Cleartrip (Flipkart/Walmart-owned) covers all Indian domestic + international airlines.
Often has OTA-exclusive fares cheaper than airline websites.
Covers IndiGo, Air India, SpiceJet, Vistara, GoFirst, AirAsia India, etc.

Strategy:
  Cleartrip has a JSON API behind their React SPA.
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

_BASE = "https://www.cleartrip.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-IN,en;q=0.9",
    "Origin": "https://www.cleartrip.com",
    "Referer": "https://www.cleartrip.com/flights",
}


class CleartripConnectorClient:
    """Cleartrip — India's leading OTA flight search."""

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
        date_str = req.date_from.strftime("%d/%m/%Y")
        date_iso = req.date_from.strftime("%Y-%m-%d")

        offers: list[FlightOffer] = []
        endpoints = [
            f"{_BASE}/api/air/search",
            f"{_BASE}/api/v2/flights/search",
        ]

        payload = {
            "origin": req.origin,
            "destination": req.destination,
            "departDate": date_str,
            "adults": req.adults or 1,
            "children": req.children or 0,
            "infants": req.infants or 0,
            "class": "Economy",
            "tripType": "O",
        }

        for endpoint in endpoints:
            try:
                resp = await client.post(endpoint, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse(data, req, date_iso)
                    if offers:
                        break
            except Exception as e:
                logger.debug("Cleartrip endpoint %s error: %s", endpoint, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Cleartrip %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"cleartrip{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "INR",
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
                flight.get("price") or flight.get("fare", {}).get("total")
                or flight.get("totalFare") or 0
            )
            currency = flight.get("currency") or "INR"
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
            oid = hashlib.md5(f"ct_{req.origin}{req.destination}{target_date}{price}{flight_no}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"ct_{oid}", price=round(float(price), 2), currency=currency,
                price_formatted=f"{float(price):.2f} {currency}",
                outbound=route, inbound=None,
                airlines=[airline_name], owner_airline=airline_code,
                booking_url=f"https://www.cleartrip.com/flights/results?adults={req.adults or 1}&childs=0&infants=0&class=Economy&depart_date={target_date}&from={req.origin}&to={req.destination}&intl=n&origin=search",
                is_locked=False, source="cleartrip_ota", source_tier="free",
            ))
        return offers
