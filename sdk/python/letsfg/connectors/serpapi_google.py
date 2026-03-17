"""
Google Flights via SerpAPI — massive global coverage (900+ airlines).

SerpAPI wraps Google Flights into a clean JSON API. This is the single
highest-ROI connector: one integration covers virtually every airline globally.

Requires: SERPAPI_KEY environment variable.
Free tier: 100 searches/month.
Docs: https://serpapi.com/google-flights-api

Strategy:
1. POST to serpapi.com/search with engine=google_flights
2. Parse best_flights + other_flights arrays
3. Map to FlightOffer objects
"""

from __future__ import annotations

import hashlib
import logging
import os
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

_SERPAPI_URL = "https://serpapi.com/search"


class SerpApiGoogleConnectorClient:
    """Google Flights via SerpAPI — global coverage metasearch."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._api_key = os.environ.get("SERPAPI_KEY", "")
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(timeout=self.timeout, follow_redirects=True)
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        if not self._api_key:
            return self._empty(req)

        t0 = time.monotonic()
        client = await self._client()

        params = {
            "engine": "google_flights",
            "departure_id": req.origin,
            "arrival_id": req.destination,
            "outbound_date": req.date_from.strftime("%Y-%m-%d"),
            "type": "2",  # one-way
            "adults": str(req.adults or 1),
            "children": str(req.children or 0),
            "infants_in_seat": str(req.infants or 0),
            "currency": req.currency or "USD",
            "hl": "en",
            "api_key": self._api_key,
        }

        try:
            resp = await client.get(_SERPAPI_URL, params=params)
        except Exception as e:
            logger.warning("SerpAPI Google Flights error: %s", e)
            return self._empty(req)

        if resp.status_code != 200:
            logger.warning("SerpAPI %d: %s", resp.status_code, resp.text[:300])
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            return self._empty(req)

        offers = []
        for flight_list in [data.get("best_flights", []), data.get("other_flights", [])]:
            for item in flight_list:
                offer = self._parse_flight(item, req)
                if offer:
                    offers.append(offer)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0

        logger.info(
            "SerpAPI Google %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"serpapi_google{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
            offers=offers,
            total_results=len(offers),
        )

    def _parse_flight(self, item: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        price = item.get("price")
        if not price or price <= 0:
            return None

        flights = item.get("flights", [])
        if not flights:
            return None

        segments = []
        airlines = set()
        for leg in flights:
            dep_airport = leg.get("departure_airport", {})
            arr_airport = leg.get("arrival_airport", {})

            dep_time = dep_airport.get("time", "")
            arr_time = arr_airport.get("time", "")

            dep_dt = self._parse_dt(dep_time)
            arr_dt = self._parse_dt(arr_time)

            airline_name = leg.get("airline", "")
            carrier = leg.get("airline_logo", "")  # not IATA, use name
            flight_no = leg.get("flight_number", "")
            duration = leg.get("duration", 0) * 60  # minutes → seconds

            airlines.add(airline_name)

            segments.append(FlightSegment(
                airline=airline_name,
                flight_no=flight_no,
                origin=dep_airport.get("id", ""),
                destination=arr_airport.get("id", ""),
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=duration,
                aircraft=leg.get("airplane", ""),
            ))

        if not segments:
            return None

        total_duration = item.get("total_duration", 0) * 60
        stopovers = max(0, len(segments) - 1)

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_duration,
            stopovers=stopovers,
        )

        flight_key = "_".join(s.flight_no for s in segments)
        offer_id = hashlib.md5(
            f"gf_{flight_key}_{price}".encode()
        ).hexdigest()[:12]

        currency = req.currency or "USD"

        return FlightOffer(
            id=f"gf_{offer_id}",
            price=float(price),
            currency=currency,
            price_formatted=f"{price} {currency}",
            outbound=route,
            inbound=None,
            airlines=sorted(airlines),
            booking_url=item.get("booking_token", ""),
            is_locked=False,
            source="serpapi_google",
            source_tier="free",
        )

    @staticmethod
    def _parse_dt(dt_str: str) -> datetime:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
            try:
                return datetime.strptime(dt_str, fmt)
            except (ValueError, TypeError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
            offers=[],
            total_results=0,
        )
