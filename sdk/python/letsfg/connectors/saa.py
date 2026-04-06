"""
South African Airways connector — EveryMundo airTRFX fare pages.

South African Airways (IATA: SA) — JNB hub.
Star Alliance member. 40+ destinations across Africa, Europe, Americas, Asia.

Strategy (httpx, no browser):
  SAA uses EveryMundo airTRFX at flysaa.com.
  1. Fetch route page: flysaa.com/en/flights-from-{o}-to-{d}
  2. Extract __NEXT_DATA__ JSON from <script> tag
  3. Parse StandardFareModule fares from Apollo GraphQL state
  4. Filter by origin/destination airport codes and departure date
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

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_httpx_proxy_url
from .airline_routes import city_match_set

logger = logging.getLogger(__name__)

_BASE = "https://www.flysaa.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Derived from flysaa.com sitemap — only slugs that actually have airTRFX pages.
_IATA_TO_SLUG: dict[str, str] = {
    # South Africa domestic
    "JNB": "johannesburg", "CPT": "cape-town",
    "DUR": "durban", "PLZ": "port-elizabeth",
    "BFN": "bloemfontein", "GRJ": "george",
    "UTW": "queenstown",
    # Southern Africa
    "WDH": "windhoek", "MPM": "maputo",
    "LUN": "lusaka", "HRE": "harare",
    "LLW": "lilongwe", "GBE": "gaborone",
    "MRU": "mauritius", "VFA": "victoria-falls",
    "LVI": "livingstone", "MUB": "maun",
    # East Africa
    "NBO": "nairobi", "DAR": "dar-es-salaam",
    "JRO": "kilimanjaro", "ZNZ": "zanzibar",
    "EBB": "entebbe", "KGL": "kigali",
    "ADD": "addis-ababa", "MBA": "mombasa",
    "KIS": "kisumu", "BJM": "bujumbura",
    # West Africa
    "LOS": "lagos", "ABV": "abuja",
    "ACC": "accra", "DKR": "dakar",
    "COO": "cotonou", "BKO": "bamako",
    "OUA": "ouagadougou",
    # Central Africa
    "FIH": "kinshasa", "LBV": "libreville",
    "DLA": "douala", "BZV": "brazzaville",
    "PNR": "pointe-noire", "FBM": "lubumbashi",
    "LAD": "luanda", "NDJ": "ndjamena",
    # North Africa / Middle East
    "CAI": "cairo", "SSH": "sharm-el-sheikh",
    "LXR": "luxor", "DXB": "dubai",
    # Europe
    "FRA": "frankfurt", "MUC": "munich",
    "ZRH": "zurich", "VIE": "vienna",
    "LIS": "lisbon", "BRU": "brussels",
    # South America
    "GRU": "sao-paulo", "GIG": "rio-de-janeiro",
    "EZE": "buenos-aires", "LIM": "lima",
    "BOG": "bogota", "SCL": "santiago",
    "MVD": "montevideo",
    # Oceania
    "SYD": "sydney", "MEL": "melbourne",
    "PER": "perth", "BNE": "brisbane",
    "ADL": "adelaide", "DRW": "darwin",
    "CNS": "cairns", "HBA": "hobart",
    "CBR": "canberra", "AKL": "auckland",
    "WLG": "wellington", "CHC": "christchurch",
    # Asia
    "SIN": "singapore",
}


class SouthAfricanAirwaysConnectorClient:
    """South African Airways — EveryMundo airTRFX fare pages."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True,
                proxy=get_httpx_proxy_url(),)
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
            logger.warning("SAA: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/en/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("SAA: fetching %s", url)

        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("SAA: %s returned %d", url, resp.status_code)
                return self._empty(req)
        except Exception as e:
            logger.error("SAA fetch error: %s", e)
            return self._empty(req)

        fares = self._extract_fares(resp.text)
        if not fares:
            logger.info("SAA: no fares on page %s", url)
            return self._empty(req)

        offers = self._build_offers(fares, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("SAA %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"saa{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "ZAR",
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _extract_fares(html: str) -> list[dict]:
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
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)

        # Separate exact-date and nearby fares (airTRFX shows cached snapshots)
        exact_fares: list[dict] = []
        nearby_fares: list[dict] = []
        for fare in fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig not in valid_origins or dest not in valid_dests:
                continue
            if not fare.get("totalPrice") or float(fare.get("totalPrice", 0)) <= 0:
                continue
            if fare.get("departureDate", "")[:10] == target_date:
                exact_fares.append(fare)
            else:
                nearby_fares.append(fare)

        # Prefer exact-date fares; fall back to all route fares
        use_fares = exact_fares if exact_fares else nearby_fares

        for fare in use_fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            dep_date = fare.get("departureDate", "")

            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            currency = fare.get("currencyCode") or "ZAR"
            price_f = round(float(price), 2)

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            cabin = (fare.get("formattedTravelClass") or "Economy").lower()
            seg = FlightSegment(
                airline="SA",
                airline_name="South African Airways",
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
                f"sa_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"sa_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["South African Airways"],
                owner_airline="SA",
                booking_url="https://www.flysaa.com/",
                is_locked=False,
                source="saa_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"saa{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="ZAR",
            offers=[],
            total_results=0,
        )
