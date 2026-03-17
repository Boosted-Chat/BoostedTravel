"""
Aegean Airlines connector — EveryMundo airTRFX fare pages.

Aegean Airlines (IATA: A3) is Greece's largest airline, a Star Alliance member,
based in Athens. Operates 155+ routes to Europe, Middle East, and North Africa.

Strategy (httpx, no browser):
  Aegean uses the EveryMundo airTRFX platform (same as Thai Airways).
  1. Fetch route page: aegeanair.com/flights/en-gr/flights-from-{origin}-to-{dest}
  2. Extract __NEXT_DATA__ JSON from <script id="__NEXT_DATA__"> tag
  3. Parse StandardFareModule fares → FlightOffer objects
  4. Fares come as calendar-style daily cheapest prices

  Also has a calendar API at:
  GET https://www.aegeanair.com/api/v1/calendar-fares?origin=ATH&destination=LHR&...
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

_BASE = "https://www.aegeanair.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# IATA → EveryMundo slug for Aegean (city names lowercase-hyphenated)
_IATA_TO_SLUG: dict[str, str] = {
    "ATH": "athens", "SKG": "thessaloniki", "HER": "heraklion",
    "CFU": "corfu", "RHO": "rhodes", "CHQ": "chania",
    "JMK": "mykonos", "JTR": "santorini", "KGS": "kos",
    "ZTH": "zakynthos", "EFL": "kefalonia", "JSI": "skiathos",
    "LHR": "london", "LGW": "london", "CDG": "paris", "ORY": "paris",
    "FCO": "rome", "MXP": "milan", "FRA": "frankfurt",
    "MUC": "munich", "BER": "berlin", "DUS": "dusseldorf",
    "AMS": "amsterdam", "BRU": "brussels", "ZRH": "zurich",
    "GVA": "geneva", "VIE": "vienna", "BCN": "barcelona",
    "MAD": "madrid", "LIS": "lisbon", "IST": "istanbul",
    "SAW": "istanbul", "TLV": "tel-aviv", "CAI": "cairo",
    "CMN": "casablanca", "JED": "jeddah", "RUH": "riyadh",
    "DXB": "dubai", "AMM": "amman", "BEY": "beirut",
    "LCA": "larnaca", "PFO": "paphos", "SOF": "sofia",
    "BUD": "budapest", "OTP": "bucharest", "WAW": "warsaw",
    "PRG": "prague", "CPH": "copenhagen", "ARN": "stockholm",
    "OSL": "oslo", "HEL": "helsinki", "DUB": "dublin",
    "MAN": "manchester", "EDI": "edinburgh", "TBS": "tbilisi",
    "EVN": "yerevan",
}


class AegeanConnectorClient:
    """Aegean Airlines — EveryMundo airTRFX fare pages + calendar API."""

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

        # Try calendar API first (faster, structured)
        offers = await self._calendar_search(client, req)

        # Fallback to EveryMundo page scrape
        if not offers:
            offers = await self._page_search(client, req)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Aegean %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        search_hash = hashlib.md5(
            f"aegean{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
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
            "children": str(req.children or 0),
            "infants": str(req.infants or 0),
            "cabinClass": "ECONOMY",
            "tripType": "ONE_WAY",
        }
        try:
            resp = await client.get(f"{_BASE}/api/v1/calendar-fares", params=params)
            if resp.status_code != 200:
                return []
            data = resp.json()
            return self._parse_calendar(data, req, date_str)
        except Exception as e:
            logger.debug("Aegean calendar API error: %s", e)
            return []

    def _parse_calendar(self, data: dict, req: FlightSearchRequest, target_date: str) -> list[FlightOffer]:
        offers = []
        fares = data.get("fares", data.get("calendarFares", []))
        for fare in fares:
            dep_date = (fare.get("departureDate") or fare.get("date") or "")[:10]
            if dep_date != target_date:
                continue

            price = fare.get("price") or fare.get("amount") or fare.get("totalPrice") or 0
            currency = fare.get("currency") or "EUR"
            if float(price) <= 0:
                continue

            offer = self._build_offer(req, float(price), currency, "calendar")
            if offer:
                offers.append(offer)
        return offers

    async def _page_search(self, client: httpx.AsyncClient, req: FlightSearchRequest) -> list[FlightOffer]:
        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            return []

        url = f"{_BASE}/flights/en-gr/flights-from-{origin_slug}-to-{dest_slug}"
        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                return []
        except Exception as e:
            logger.debug("Aegean page fetch error: %s", e)
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
                currency = fare.get("price", {}).get("currencyCode") or fare.get("currency") or "EUR"
                if float(price) <= 0:
                    continue
                offer = self._build_offer(req, float(price), currency, "page")
                if offer:
                    offers.append(offer)
        return offers

    def _build_offer(self, req: FlightSearchRequest, price: float, currency: str, src: str) -> Optional[FlightOffer]:
        price = round(price, 2)
        dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))

        seg = FlightSegment(
            airline="Aegean Airlines",
            flight_no="A3",
            origin=req.origin,
            destination=req.destination,
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
        )

        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
        offer_id = hashlib.md5(f"a3_{req.origin}{req.destination}{req.date_from}{price}_{src}".encode()).hexdigest()[:12]

        return FlightOffer(
            id=f"a3_{offer_id}",
            price=price,
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Aegean Airlines"],
            owner_airline="A3",
            booking_url=f"https://www.aegeanair.com/book?origin={req.origin}&destination={req.destination}&date={req.date_from.strftime('%Y-%m-%d')}&adults={req.adults or 1}",
            is_locked=False,
            source="aegean_direct",
            source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="EUR", offers=[], total_results=0,
        )
