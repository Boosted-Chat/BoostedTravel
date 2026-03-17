"""
Air Canada connector — calendar lowfare API.

Air Canada (IATA: AC) is Canada's flag carrier and largest airline.
Star Alliance member, 200+ destinations globally. YYZ/YVR/YUL hubs.

Strategy:
  Air Canada's booking engine at aircanada.com uses a lowfare calendar API:
  GET https://acosp-api.aircanada.com/ac-ota-cal/cal/v2/lowfare
  Parameters: org=YYZ&dest=LHR&depDate=2026-04-15&tripType=O&cabin=ECO&adult=1
  Returns: daily lowest fares as JSON with pricing per date.
  No authentication needed — publicly accessible.
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

_API_URL = "https://acosp-api.aircanada.com/ac-ota-cal/cal/v2/lowfare"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-CA,en;q=0.9",
    "Origin": "https://www.aircanada.com",
    "Referer": "https://www.aircanada.com/",
}


class AirCanadaConnectorClient:
    """Air Canada — lowfare calendar API (no auth)."""

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
        params = {
            "org": req.origin,
            "dest": req.destination,
            "depDate": date_str,
            "tripType": "O",
            "cabin": "ECO",
            "adult": str(req.adults or 1),
            "child": str(req.children or 0),
            "infant": str(req.infants or 0),
            "lang": "en-CA",
        }

        offers = []
        try:
            resp = await client.get(_API_URL, params=params)
            if resp.status_code == 200:
                data = resp.json()
                offers = self._parse(data, req, date_str)
        except Exception as e:
            logger.warning("Air Canada API error: %s", e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Air Canada %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        search_hash = hashlib.md5(
            f"aircanada{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "CAD",
            offers=offers,
            total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        # API may return under "fares", "dates", "calendar", or directly as list
        fares = data.get("fares") or data.get("dates") or data.get("lowFares") or []
        if isinstance(data, list):
            fares = data

        for fare in fares:
            dep = (fare.get("departureDate") or fare.get("date") or fare.get("depDate") or "")[:10]
            if dep != target_date:
                continue

            price = (fare.get("totalAmount") or fare.get("price") or
                     fare.get("amount") or fare.get("lowestFare") or 0)
            currency = fare.get("currency") or fare.get("currencyCode") or "CAD"
            if float(price) <= 0:
                continue

            cabin = fare.get("cabin") or fare.get("cabinClass") or "Economy"
            seats = fare.get("seatsAvailable")

            dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
            seg = FlightSegment(
                airline="Air Canada", flight_no="AC",
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=dep_dt, duration_seconds=0,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            oid = hashlib.md5(f"ac_{req.origin}{req.destination}{dep}{price}{cabin}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"ac_{oid}",
                price=round(float(price), 2),
                currency=currency,
                price_formatted=f"{float(price):.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Air Canada"],
                owner_airline="AC",
                availability_seats=seats,
                booking_url=f"https://www.aircanada.com/booking/search?org={req.origin}&dest={req.destination}&depDate={target_date}&ADT={req.adults or 1}&tripType=O",
                is_locked=False,
                source="aircanada_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="CAD", offers=[], total_results=0,
        )
