"""
Aegean Airlines connector — EveryMundo airTRFX fare pages.

Aegean Airlines (IATA: A3) is Greece's largest airline, a Star Alliance member,
based in Athens. Operates 155+ routes to Europe, Middle East, and North Africa.

Strategy (httpx, no browser):
  Aegean uses EveryMundo airTRFX at flights.aegeanair.com (subdomain).
  1. Fetch route page: flights.aegeanair.com/en/flights-from-{origin}-to-{dest}
  2. Extract __NEXT_DATA__ JSON from <script> tag
  3. Parse StandardFareModule fares from Apollo GraphQL state
  4. Filter by origin/destination airport codes and departure date
  Note: Aegean uses city codes (LON, CDG) in some fares — we match both.
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

_BASE = "https://flights.aegeanair.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# IATA → EveryMundo slug for Aegean route pages.
_IATA_TO_SLUG: dict[str, str] = {
    # Greece
    "ATH": "athens", "SKG": "thessaloniki", "HER": "heraklion",
    "CFU": "corfu", "RHO": "rhodes", "CHQ": "chania",
    "JMK": "mykonos", "JTR": "santorini", "KGS": "kos",
    "ZTH": "zakynthos", "EFL": "kefalonia", "JSI": "skiathos",
    "AXD": "alexandroupolis",
    # Europe
    "LHR": "london", "LGW": "london", "STN": "london",
    "CDG": "paris", "ORY": "paris",
    "FCO": "rome", "MXP": "milan",
    "FRA": "frankfurt", "MUC": "munich", "BER": "berlin",
    "DUS": "dusseldorf", "STR": "stuttgart",
    "AMS": "amsterdam", "BRU": "brussels",
    "ZRH": "zurich", "GVA": "geneva",
    "VIE": "vienna", "BCN": "barcelona", "MAD": "madrid",
    "LIS": "lisbon", "IST": "istanbul", "SAW": "istanbul",
    "SOF": "sofia", "BUD": "budapest", "OTP": "bucharest",
    "WAW": "warsaw", "PRG": "prague",
    "CPH": "copenhagen", "ARN": "stockholm", "OSL": "oslo",
    "HEL": "helsinki", "DUB": "dublin",
    "MAN": "manchester", "EDI": "edinburgh",
    # Middle East / Africa
    "TLV": "tel-aviv", "CAI": "cairo",
    "CMN": "casablanca", "AMM": "amman", "BEY": "beirut",
    "LCA": "larnaca", "PFO": "paphos",
    "TBS": "tbilisi", "EVN": "yerevan",
    "DXB": "dubai", "JED": "jeddah", "RUH": "riyadh",
}

# Map IATA airport codes to the city codes Aegean uses in fares.
_IATA_TO_CITY: dict[str, str] = {
    "LHR": "LON", "LGW": "LON", "STN": "LON",
    "CDG": "PAR", "ORY": "PAR",
}


class AegeanConnectorClient:
    """Aegean Airlines — EveryMundo airTRFX fare pages."""

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

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("Aegean: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/en/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Aegean: fetching %s", url)

        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Aegean: %s returned %d", url, resp.status_code)
                return self._empty(req)
        except Exception as e:
            logger.error("Aegean fetch error: %s", e)
            return self._empty(req)

        fares = self._extract_fares(resp.text)
        if not fares:
            logger.info("Aegean: no fares on page %s", url)
            return self._empty(req)

        offers = self._build_offers(fares, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Aegean %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"aegean{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _extract_fares(html: str) -> list[dict]:
        """Extract all fares from ALL StandardFareModules in __NEXT_DATA__."""
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
            re.S,
        )
        if not m:
            return []
        try:
            nd = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            return []

        apollo = (
            nd.get("props", {})
            .get("pageProps", {})
            .get("apolloState", {})
            .get("data", {})
        )
        if not apollo:
            return []

        all_fares: list[dict] = []
        for v in apollo.values():
            if not isinstance(v, dict) or v.get("__typename") != "StandardFareModule":
                continue
            for f in v.get("fares", []):
                if isinstance(f, dict) and "__ref" in f:
                    ref_data = apollo.get(f["__ref"])
                    if ref_data and isinstance(ref_data, dict):
                        all_fares.append(ref_data)
                elif isinstance(f, dict):
                    all_fares.append(f)
        return all_fares

    def _build_offers(self, fares: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []

        # Aegean uses city codes like LON, PAR in fares
        origin_codes = {req.origin, _IATA_TO_CITY.get(req.origin, req.origin)}
        dest_codes = {req.destination, _IATA_TO_CITY.get(req.destination, req.destination)}

        for fare in fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig not in origin_codes or dest not in dest_codes:
                continue

            dep_date = fare.get("departureDate", "")
            if dep_date[:10] != target_date:
                continue

            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            currency = fare.get("currencyCode") or "EUR"
            price_f = round(float(price), 2)

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            cabin = (fare.get("formattedTravelClass") or "Economy").lower()
            seg = FlightSegment(
                airline="A3",
                airline_name="Aegean Airlines",
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                origin_city=fare.get("originCity", ""),
                destination_city=fare.get("destinationCity", ""),
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            fid = hashlib.md5(
                f"a3_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"a3_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Aegean Airlines"],
                owner_airline="A3",
                booking_url=(
                    f"https://en.aegeanair.com/search/"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={target_date}"
                    f"&adults={req.adults or 1}&tripType=O"
                ),
                is_locked=False,
                source="aegean_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"aegean{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
