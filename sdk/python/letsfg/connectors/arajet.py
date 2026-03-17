"""
Arajet connector — Dominican Republic ULCC.

Arajet (IATA: DM) is the Dominican Republic's first ultra-low-cost carrier.
Hub at SDQ (Santo Domingo). Rapidly growing, serving Caribbean + Americas.
35+ destinations in Colombia, Mexico, Central America, Canada, US.

Strategy:
  Arajet uses Radixx booking engine at book.arajet.com.
  Calendar pricing via API endpoint.
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

_BASE = "https://book.arajet.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Origin": "https://www.arajet.com",
    "Referer": "https://www.arajet.com/",
}


class ArajetConnectorClient:
    """Arajet — Radixx booking engine API."""

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

        offers = []
        for endpoint in [
            f"{_BASE}/api/flights/lowfare",
            f"{_BASE}/api/availability/calendar",
        ]:
            params = {
                "origin": req.origin,
                "destination": req.destination,
                "departureDate": date_str,
                "adults": str(req.adults or 1),
                "children": str(req.children or 0),
                "infants": str(req.infants or 0),
                "currencyCode": req.currency or "USD",
            }
            try:
                resp = await client.get(endpoint, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse(data, req, date_str)
                    if offers:
                        break
            except Exception as e:
                logger.debug("Arajet endpoint %s error: %s", endpoint, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Arajet %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"arajet{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers, total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        fares = data.get("fares") or data.get("flights") or data.get("lowFares") or []
        for fare in fares:
            dep = (fare.get("departureDate") or fare.get("date") or "")[:10]
            if dep and dep != target_date:
                continue
            price = fare.get("price") or fare.get("lowestFare") or fare.get("totalAmount") or 0
            currency = fare.get("currency") or fare.get("currencyCode") or "USD"
            if float(price) <= 0:
                continue
            dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
            seg = FlightSegment(
                airline="Arajet", flight_no="DM", origin=req.origin,
                destination=req.destination, departure=dep_dt, arrival=dep_dt, duration_seconds=0,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
            oid = hashlib.md5(f"dm_{req.origin}{req.destination}{target_date}{price}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"dm_{oid}", price=round(float(price), 2), currency=currency,
                price_formatted=f"{float(price):.2f} {currency}",
                outbound=route, inbound=None, airlines=["Arajet"], owner_airline="DM",
                booking_url=f"https://book.arajet.com/search?origin={req.origin}&destination={req.destination}&date={target_date}&adults={req.adults or 1}",
                is_locked=False, source="arajet_direct", source_tier="free",
            ))
        return offers

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
