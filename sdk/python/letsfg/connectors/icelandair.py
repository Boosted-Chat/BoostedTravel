"""
Icelandair connector — EveryMundo airTRFX + calendar API.

Icelandair (IATA: FI) is Iceland's flag carrier. Key for transatlantic routes
via KEF (Reykjavik-Keflavik) hub connecting Europe ↔ North America.
90+ destinations including US, Canada, and European cities.

Strategy:
  1. Calendar API: icelandair.com/api/offers/calendar-fares
  2. Fallback: EveryMundo fare page scraping (__NEXT_DATA__)
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

_BASE = "https://www.icelandair.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_IATA_TO_SLUG: dict[str, str] = {
    "KEF": "reykjavik", "AEY": "akureyri", "EGS": "egilsstadir",
    "LHR": "london", "LGW": "london", "MAN": "manchester",
    "EDI": "edinburgh", "GLA": "glasgow", "BHX": "birmingham",
    "CDG": "paris", "AMS": "amsterdam", "BRU": "brussels",
    "FRA": "frankfurt", "MUC": "munich", "BER": "berlin",
    "ZRH": "zurich", "GVA": "geneva", "CPH": "copenhagen",
    "ARN": "stockholm", "OSL": "oslo", "HEL": "helsinki",
    "BCN": "barcelona", "MAD": "madrid", "LIS": "lisbon",
    "FCO": "rome", "MXP": "milan", "VIE": "vienna",
    "DUB": "dublin", "WAW": "warsaw", "PRG": "prague",
    "JFK": "new-york", "EWR": "newark", "BOS": "boston",
    "ORD": "chicago", "IAD": "washington-dc", "DEN": "denver",
    "SEA": "seattle", "MSP": "minneapolis", "PDX": "portland",
    "YYZ": "toronto", "YUL": "montreal", "YVR": "vancouver",
    "YYC": "calgary", "YOW": "ottawa", "YHZ": "halifax",
}


class IcelandairConnectorClient:
    """Icelandair — calendar API + EveryMundo fare pages."""

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

        offers = await self._calendar_search(client, req)
        if not offers:
            offers = await self._page_search(client, req)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Icelandair %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        search_hash = hashlib.md5(
            f"icelandair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    async def _calendar_search(self, client: httpx.AsyncClient, req: FlightSearchRequest) -> list[FlightOffer]:
        date_str = req.date_from.strftime("%Y-%m-%d")
        params = {
            "origin": req.origin,
            "destination": req.destination,
            "departureDate": date_str,
            "adults": str(req.adults or 1),
            "tripType": "oneway",
            "currency": req.currency or "USD",
        }
        try:
            resp = await client.get(f"{_BASE}/api/offers/calendar-fares", params=params)
            if resp.status_code != 200:
                return []
            data = resp.json()
            return self._parse_calendar(data, req, date_str)
        except Exception as e:
            logger.debug("Icelandair calendar error: %s", e)
            return []

    def _parse_calendar(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        for fare in data.get("fares", data.get("dates", [])):
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

    async def _page_search(self, client: httpx.AsyncClient, req: FlightSearchRequest) -> list[FlightOffer]:
        o_slug = _IATA_TO_SLUG.get(req.origin)
        d_slug = _IATA_TO_SLUG.get(req.destination)
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
        return self._parse_next_data(nd, req)

    def _parse_next_data(self, nd: dict, req: FlightSearchRequest) -> list[FlightOffer]:
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

    def _build_offer(self, req: FlightSearchRequest, price: float, currency: str) -> Optional[FlightOffer]:
        price = round(price, 2)
        dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
        seg = FlightSegment(
            airline="Icelandair", flight_no="FI",
            origin=req.origin, destination=req.destination,
            departure=dep_dt, arrival=dep_dt, duration_seconds=0,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
        oid = hashlib.md5(f"fi_{req.origin}{req.destination}{req.date_from}{price}".encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"fi_{oid}", price=price, currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route, inbound=None, airlines=["Icelandair"],
            owner_airline="FI",
            booking_url=f"https://www.icelandair.com/search/results?from={req.origin}&to={req.destination}&depart={req.date_from.strftime('%Y-%m-%d')}&adults={req.adults or 1}&type=oneway",
            is_locked=False, source="icelandair_direct", source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
