"""
Air New Zealand connector — EveryMundo airTRFX fare pages.

Air New Zealand (IATA: NZ) — AKL/WLG/CHC hubs.
Star Alliance member. 50+ destinations across NZ, Pacific islands, AU, Asia, Americas.

Strategy (httpx, no browser):
  Air New Zealand uses EveryMundo airTRFX (same platform as Air Canada / Thai).
  1. Fetch route page: airnewzealand.com/flights/en-nz/flights-from-{origin}-to-{dest}
  2. Extract __NEXT_DATA__ JSON from <script> tag
  3. Parse StandardFareModule fares from Apollo GraphQL state
  4. Filter by matching origin/destination airport codes and departure date
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

_BASE = "https://www.airnewzealand.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-NZ,en;q=0.9",
}

# IATA → slug for Air New Zealand's EveryMundo fare pages.
_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "LON": "london", "NYC": "new-york", "PAR": "paris", "TYO": "tokyo",
    # New Zealand
    "AKL": "auckland", "WLG": "wellington", "CHC": "christchurch",
    "ZQN": "queenstown", "DUD": "dunedin", "PMR": "palmerston-north",
    "NPE": "napier", "NPL": "new-plymouth", "HLZ": "hamilton",
    "TRG": "tauranga", "ROT": "rotorua", "BHE": "blenheim",
    "NSN": "nelson", "IVC": "invercargill",
    # Australia
    "SYD": "sydney", "MEL": "melbourne", "BNE": "brisbane",
    "PER": "perth", "ADL": "adelaide", "OOL": "gold-coast",
    # Pacific Islands
    "NAN": "nadi", "APW": "apia", "TBU": "tongatapu",
    "RAR": "rarotonga", "NOU": "noumea", "PPT": "papeete",
    # Asia
    "SIN": "singapore", "HKG": "hong-kong", "NRT": "tokyo",
    "HND": "tokyo", "PVG": "shanghai", "PEK": "beijing",
    "ICN": "seoul", "TPE": "taipei", "MNL": "manila",
    "KUL": "kuala-lumpur", "BKK": "bangkok", "DPS": "bali",
    "SGN": "ho-chi-minh-city", "HAN": "hanoi",
    # Americas
    "LAX": "los-angeles", "SFO": "san-francisco", "JFK": "new-york",
    "IAH": "houston", "ORD": "chicago", "SEA": "seattle",
    "IAD": "washington-dc", "BOS": "boston", "DEN": "denver",
    "HNL": "honolulu", "YVR": "vancouver",
    # Europe / Middle East
    "LHR": "london", "CDG": "paris", "FRA": "frankfurt",
}


class AirNewZealandConnectorClient:
    """Air New Zealand — EveryMundo airTRFX fare pages."""

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
            logger.warning("Air NZ: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/flights/en-nz/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Air NZ: fetching %s", url)

        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Air NZ: %s returned %d", url, resp.status_code)
                return self._empty(req)
        except Exception as e:
            logger.error("Air NZ fetch error: %s", e)
            return self._empty(req)

        fares = self._extract_fares(resp.text)
        if not fares:
            logger.info("Air NZ: no fares on page %s", url)
            return self._empty(req)

        offers = self._build_offers(fares, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Air NZ %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"airnz{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "NZD",
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
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)

        for fare in fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig not in valid_origins or dest not in valid_dests:
                continue

            dep_date = fare.get("departureDate", "")
            if dep_date[:10] != target_date:
                continue

            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            currency = fare.get("currencyCode") or "USD"
            price_f = round(float(price), 2)

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            cabin = (fare.get("formattedTravelClass") or "Economy").lower()
            seg = FlightSegment(
                airline="NZ",
                airline_name="Air New Zealand",
                flight_no="",
                origin=orig,
                destination=dest,
                origin_city=fare.get("originCity", ""),
                destination_city=fare.get("destinationCity", ""),
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            fid = hashlib.md5(
                f"nz_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"nz_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Air New Zealand"],
                owner_airline="NZ",
                booking_url=(
                    f"https://www.airnewzealand.co.nz/booking/flights"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={target_date}"
                    f"&adults={req.adults or 1}"
                ),
                is_locked=False,
                source="airnewzealand_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"airnz{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="NZD",
            offers=[],
            total_results=0,
        )
