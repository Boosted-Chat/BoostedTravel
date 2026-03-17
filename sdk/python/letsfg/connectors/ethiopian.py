"""
Ethiopian Airlines connector — Africa's largest carrier.

Ethiopian Airlines (IATA: ET) is Africa's largest and most profitable airline.
Star Alliance member. ADD (Addis Ababa) hub — Africa's busiest transit hub.
130+ destinations across Africa, Europe, Middle East, Asia, Americas.

Strategy:
  Ethiopian uses Amadeus Altéa IBE. Try multiple API approaches:
  1. Calendar lowfare API at ethiopianairlines.com
  2. Offer search API
  3. EveryMundo fare pages as fallback
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
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

_BASE = "https://www.ethiopianairlines.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


class EthiopianConnectorClient:
    """Ethiopian Airlines — calendar API + EveryMundo fare pages."""

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
            f"{_BASE}/api/offers/calendar-fares",
            f"{_BASE}/api/v1/lowfares",
        ]:
            params = {
                "origin": req.origin,
                "destination": req.destination,
                "departureDate": date_str,
                "adults": str(req.adults or 1),
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
                logger.debug("Ethiopian endpoint %s error: %s", endpoint, e)

        # Try EveryMundo page fallback
        if not offers:
            offers = await self._everymundo_search(client, req)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Ethiopian %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"ethiopian{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers, total_results=len(offers),
        )

    async def _everymundo_search(self, client: httpx.AsyncClient, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: fetch EveryMundo fare page and extract __NEXT_DATA__."""
        slug_map = {
            "ADD": "addis-ababa", "NBO": "nairobi", "DAR": "dar-es-salaam",
            "LOS": "lagos", "ACC": "accra", "JNB": "johannesburg",
            "LHR": "london", "CDG": "paris", "FRA": "frankfurt",
            "DXB": "dubai", "JED": "jeddah", "PEK": "beijing",
            "BOM": "mumbai", "IAD": "washington-dc", "ORD": "chicago",
            "GRU": "sao-paulo", "EBB": "entebbe", "KGL": "kigali",
            "MPM": "maputo", "LUN": "lusaka", "HRE": "harare",
        }
        o_slug = slug_map.get(req.origin)
        d_slug = slug_map.get(req.destination)
        if not o_slug or not d_slug:
            return []
        url = f"{_BASE}/flights/en-us/flights-from-{o_slug}-to-{d_slug}"
        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                return []
        except Exception:
            return []
        m = re.search(r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>', resp.text, re.S)
        if not m:
            return []
        try:
            nd = json.loads(m.group(1))
        except json.JSONDecodeError:
            return []
        offers = []
        target_date = req.date_from.strftime("%Y-%m-%d")
        props = nd.get("props", {}).get("pageProps", {})
        for module in props.get("modules", []):
            if module.get("type") != "StandardFareModule":
                continue
            for fare in module.get("fares", []):
                dep = (fare.get("departureDate") or "")[:10]
                if dep != target_date:
                    continue
                price = fare.get("price", {}).get("amount") or fare.get("amount") or 0
                currency = fare.get("price", {}).get("currencyCode") or "USD"
                if float(price) <= 0:
                    continue
                offer = self._build_offer(req, float(price), currency)
                if offer:
                    offers.append(offer)
        return offers

    def _parse(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        fares = data.get("fares") or data.get("calendarFares") or data.get("dates") or []
        for fare in fares:
            dep = (fare.get("departureDate") or fare.get("date") or "")[:10]
            if dep != target_date:
                continue
            price = fare.get("price") or fare.get("amount") or fare.get("lowestPrice") or 0
            currency = fare.get("currency") or "USD"
            if float(price) <= 0:
                continue
            offer = self._build_offer(req, float(price), currency)
            if offer:
                offers.append(offer)
        return offers

    def _build_offer(self, req: FlightSearchRequest, price: float, currency: str) -> Optional[FlightOffer]:
        price = round(price, 2)
        dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
        seg = FlightSegment(
            airline="Ethiopian Airlines", flight_no="ET", origin=req.origin,
            destination=req.destination, departure=dep_dt, arrival=dep_dt, duration_seconds=0,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
        oid = hashlib.md5(f"et_{req.origin}{req.destination}{req.date_from}{price}".encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"et_{oid}", price=price, currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route, inbound=None, airlines=["Ethiopian Airlines"], owner_airline="ET",
            booking_url=f"https://www.ethiopianairlines.com/book?origin={req.origin}&destination={req.destination}&date={req.date_from.strftime('%Y-%m-%d')}&adults={req.adults or 1}",
            is_locked=False, source="ethiopian_direct", source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
