"""
Despegar connector — Latin America's largest OTA.

Despegar (NASDAQ: DESP) covers all Latin American airlines + international.
Often has OTA-exclusive fares for LATAM, GOL, Azul, Avianca, Copa, JetSmart, etc.
Also operates as Decolar (Brazil), BestDay (Mexico).

Strategy:
  Despegar has a JSON API behind their React SPA.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

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

_BASE = "https://www.despegar.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "x-client": "WEB",
}


class DespegarConnectorClient:
    """Despegar — Latin America's largest OTA flight search."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True,
                proxy=get_httpx_proxy_url(),)
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        client = await self._client()
        date_str = req.date_from.strftime("%Y-%m-%d")

        offers: list[FlightOffer] = []
        endpoints = [
            f"{_BASE}/api/v3/flights/search",
            f"{_BASE}/api/flights/oneways",
        ]

        params = {
            "from": req.origin,
            "to": req.destination,
            "departure": date_str,
            "adults": str(req.adults or 1),
            "cabinClass": "economy",
        }

        for endpoint in endpoints:
            try:
                resp = await client.get(endpoint, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse(data, req, date_str)
                    if offers:
                        break
            except Exception as e:
                logger.debug("Despegar endpoint %s error: %s", endpoint, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Despegar %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"despegar{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers, total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        results = (
            data.get("flights") or data.get("items") or data.get("clusters")
            or data.get("itineraries") or data.get("data", {}).get("items") or []
        )
        for flight in results:
            price_obj = flight.get("priceDetail") or flight.get("price") or flight
            price = (
                price_obj.get("totalPrice") or price_obj.get("total")
                or price_obj.get("price") or price_obj.get("amount") or 0
            )
            currency = price_obj.get("currency") or flight.get("currency") or "USD"
            if float(price) <= 0:
                continue

            # Extract airline info from segments or top-level
            segments_data = flight.get("segments") or flight.get("legs") or [flight]
            seg_info = segments_data[0] if segments_data else flight
            airline_name = seg_info.get("airline") or seg_info.get("carrierName") or seg_info.get("marketingCarrier", {}).get("name", "Unknown")
            airline_code = seg_info.get("airlineCode") or seg_info.get("marketingCarrier", {}).get("code", "")
            flight_no = seg_info.get("flightNumber") or airline_code

            dep_str = seg_info.get("departure") or seg_info.get("departureTime") or ""
            arr_str = seg_info.get("arrival") or seg_info.get("arrivalTime") or ""

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
            oid = hashlib.md5(f"desp_{req.origin}{req.destination}{target_date}{price}{flight_no}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"desp_{oid}", price=round(float(price), 2), currency=currency,
                price_formatted=f"{float(price):.2f} {currency}",
                outbound=route, inbound=None,
                airlines=[airline_name], owner_airline=airline_code,
                booking_url=f"https://www.despegar.com/shop/flights/results/oneway/{req.origin}/{req.destination}/{target_date}/{req.adults or 1}/0/0",
                is_locked=False, source="despegar_ota", source_tier="free",
            ))
        return offers
