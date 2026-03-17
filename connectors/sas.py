"""
SAS Scandinavian Airlines connector — BFF datepicker lowfare API.

SAS (IATA: SK) is the flag carrier of Denmark, Norway, and Sweden.
SkyTeam member. CPH/OSL/ARN hubs. 180+ destinations.

Strategy:
  SAS exposes a public BFF datepicker API that returns daily lowest fares
  for a full month. Works via plain httpx — no browser or cookies needed.

  GET https://www.flysas.com/bff/datepicker/flights/offers/v1
    ?market=en&origin=CPH&destination=LHR&adult=1
    &bookingFlow=revenue&departureDate=2026-05-01
  Response: {
    "currency": "EUR",
    "outbound": {
      "2026-05-01": {"totalPrice": 110, "points": 0},
      "2026-05-02": {"totalPrice": 78.05, "points": 0},
      ...
    }
  }

  Returns 31 daily prices per request. Works for all routes, not just hubs.
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

_API = "https://www.flysas.com/bff/datepicker/flights/offers/v1"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.flysas.com/en/low-fare-calendar",
}


class SASConnectorClient:
    """SAS Scandinavian Airlines — BFF datepicker lowfare calendar API."""

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
            "market": "en",
            "origin": req.origin,
            "destination": req.destination,
            "adult": str(req.adults or 1),
            "bookingFlow": "revenue",
            "departureDate": date_str,
        }

        offers: list[FlightOffer] = []
        try:
            resp = await client.get(_API, params=params)
            if resp.status_code == 200:
                data = resp.json()
                offers = self._parse(data, req)
        except Exception as e:
            logger.error("SAS API error: %s", e)

        offers.sort(key=lambda o: o.price)
        elapsed = time.monotonic() - t0
        logger.info(
            "SAS %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        sh = hashlib.md5(
            f"sas{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        currency = data.get("currency", "EUR")
        outbound = data.get("outbound", {})
        target_date = req.date_from.strftime("%Y-%m-%d")

        for date_str, info in outbound.items():
            price = info.get("totalPrice", 0)
            if price <= 0:
                continue

            # Filter to requested date only
            if date_str != target_date:
                continue

            dep_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=8)
            seg = FlightSegment(
                airline="SK",
                airline_name="SAS",
                flight_no="SK",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
            )
            route = FlightRoute(
                segments=[seg], total_duration_seconds=0, stopovers=0
            )

            key = f"sk_{req.origin}{req.destination}{date_str}{price}"
            oid = hashlib.md5(key.encode()).hexdigest()[:12]

            booking_url = (
                f"https://www.flysas.com/en/book/flights?"
                f"origin={req.origin}&destination={req.destination}"
                f"&outboundDate={date_str.replace('-', '')}"
                f"&adults={req.adults or 1}&trip=OW"
            )

            offers.append(
                FlightOffer(
                    id=f"sk_{oid}",
                    price=round(float(price), 2),
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=["SAS"],
                    owner_airline="SK",
                    conditions={"price_type": "lowest_fare"},
                    booking_url=booking_url,
                    is_locked=False,
                    source="sas_direct",
                    source_tier="free",
                )
            )

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        sh = hashlib.md5(
            f"sas{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
