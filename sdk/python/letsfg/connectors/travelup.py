"""
TravelUp connector — UK OTA with consolidator fares (direct API).

TravelUp.com is a UK-based OTA that sources fares from multiple consolidators
and GDS backends. Uses their flight-search API with a date-range cheapest-fare
endpoint to retrieve pricing for nearby dates.

Strategy (direct API):
1. Call tup-flightsearch-api.azurewebsites.net/api/search/cheapest with api-key.
2. Query the target date ±3 days to get multiple price points.
3. Parse results → FlightOffers.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timedelta
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

_BASE = "https://www.travelup.com"
_API_URL = "https://tup-flightsearch-api.azurewebsites.net/api/search/cheapest"
_API_KEY = "9a9635e3240c41018ddadfa51bb378e4"


class TravelupConnectorClient:
    """TravelUp — UK OTA, direct API for cheapest fares."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        target = req.date_from
        date_str = target.strftime("%Y-%m-%d")

        # Query a ±3-day window around the target date to get multiple price points
        ds = (target - timedelta(days=3)).strftime("%Y-%m-%d")
        de = (target + timedelta(days=3)).strftime("%Y-%m-%d")

        headers = {
            "api-key": _API_KEY,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        params = {
            "di": req.origin,
            "ai": req.destination,
            "ap": str(req.adults or 1),
            "cp": str(req.children or 0),
            "ip": str(req.infants or 0),
            "c": "1",  # economy
            "sm": "2",  # search mode
            "l": "en-GB",
            "ds": ds,
            "de": de,
            "rf": "false",  # one-way
            "d": "0",
        }

        offers: list[FlightOffer] = []
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, proxy=get_httpx_proxy_url(),
            ) as client:
                resp = await client.get(_API_URL, headers=headers, params=params)
                if resp.status_code != 200:
                    logger.warning("TravelUp API %d for %s→%s", resp.status_code, req.origin, req.destination)
                    return self._empty(req)

                data = resp.json()
                results = data.get("r", [])
                if not isinstance(results, list):
                    return self._empty(req)

                for item in results:
                    price = item.get("cf")
                    dep_date_str = item.get("dd", "")
                    if not price or price <= 0 or not dep_date_str:
                        continue

                    try:
                        dep_date = datetime.strptime(dep_date_str, "%Y-%m-%d")
                    except ValueError:
                        continue

                    booking_dep = dep_date.strftime("%Y-%m-%d")
                    segments = [FlightSegment(
                        airline="TravelUp",
                        flight_no="",
                        origin=req.origin,
                        destination=req.destination,
                        departure=dep_date,
                        arrival=dep_date,
                        duration_seconds=0,
                    )]
                    route = FlightRoute(
                        segments=segments,
                        total_duration_seconds=0,
                        stopovers=0,
                    )
                    oid = hashlib.md5(
                        f"tvup_{req.origin}{req.destination}{booking_dep}{price}".encode()
                    ).hexdigest()[:12]

                    offers.append(FlightOffer(
                        id=f"tvup_{oid}",
                        price=round(float(price), 2),
                        currency="GBP",
                        price_formatted=f"{price:.2f} GBP",
                        outbound=route,
                        inbound=None,
                        airlines=["TravelUp"],
                        owner_airline="TravelUp",
                        booking_url=f"{_BASE}/en-gb/flights?from={req.origin}&to={req.destination}&departure={booking_dep}",
                        is_locked=False,
                        source="travelup_ota",
                        source_tier="free",
                    ))

        except httpx.HTTPError as e:
            logger.error("TravelUp HTTP error: %s", e)
            return self._empty(req)
        except Exception as e:
            logger.error("TravelUp error: %s", e)
            return self._empty(req)

        offers.sort(key=lambda o: o.price)
        elapsed = time.monotonic() - t0
        logger.info(
            "TravelUp %s→%s: %d offers in %.1fs (API)",
            req.origin, req.destination, len(offers), elapsed,
        )

        sh = hashlib.md5(f"travelup{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "GBP",
            offers=offers,
            total_results=len(offers),
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"travelup{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="GBP", offers=[], total_results=0,
        )
