"""
Wego connector — Middle East and Asia's leading metasearch.

Wego covers 700+ airlines across Middle East, South Asia, SE Asia.
Popular in GCC countries, India, Indonesia. Has exclusive OTA partner fares.

Strategy:
  Wego has a public search API at their places/search endpoint.
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

_BASE = "https://srv.wego.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


class WegoConnectorClient:
    """Wego — ME/Asia metasearch flight search."""

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
        endpoints = [
            f"{_BASE}/flights/api/k/searches",
            f"https://www.wego.com/api/flights/search",
        ]

        payload = {
            "trips": [{
                "departureCode": req.origin,
                "arrivalCode": req.destination,
                "outboundDate": date_str,
            }],
            "cabin": "economy",
            "adults": req.adults or 1,
            "children": req.children or 0,
            "infants": req.infants or 0,
        }

        for endpoint in endpoints:
            try:
                resp = await client.post(endpoint, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    search_id = data.get("id") or data.get("searchId") or ""
                    if search_id:
                        # Poll for results
                        offers = await self._poll_results(client, search_id, req, date_str)
                    else:
                        offers = self._parse(data, req, date_str)
                    if offers:
                        break
            except Exception as e:
                logger.debug("Wego endpoint %s error: %s", endpoint, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Wego %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"wego{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers, total_results=len(offers),
        )

    async def _poll_results(self, client: httpx.AsyncClient, search_id: str, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        """Poll Wego for search results (max 3 attempts)."""
        import asyncio
        for _ in range(3):
            await asyncio.sleep(2)
            try:
                resp = await client.get(f"{_BASE}/flights/api/k/searches/{search_id}/results")
                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse(data, req, target_date)
                    if offers:
                        return offers
            except Exception:
                pass
        return []

    def _parse(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        results = (
            data.get("fares") or data.get("trips") or data.get("results")
            or data.get("itineraries") or data.get("flights") or []
        )
        for fare in results:
            price_obj = fare.get("price") or fare
            price = price_obj.get("amount") or price_obj.get("totalAmount") or price_obj.get("price") or 0
            currency = price_obj.get("currencyCode") or price_obj.get("currency") or "USD"
            if float(price) <= 0:
                continue

            airline_name = fare.get("airlineName") or fare.get("airline") or "Unknown"
            airline_code = fare.get("airlineCode") or ""
            flight_no = fare.get("flightNumber") or airline_code

            dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
            seg = FlightSegment(
                airline=airline_name, flight_no=flight_no,
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=dep_dt, duration_seconds=0,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
            oid = hashlib.md5(f"wego_{req.origin}{req.destination}{target_date}{price}{flight_no}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"wego_{oid}", price=round(float(price), 2), currency=currency,
                price_formatted=f"{float(price):.2f} {currency}",
                outbound=route, inbound=None,
                airlines=[airline_name], owner_airline=airline_code,
                booking_url=f"https://www.wego.com/flights/{req.origin}/{req.destination}/{target_date}?adults={req.adults or 1}",
                is_locked=False, source="wego_ota", source_tier="free",
            ))
        return offers
