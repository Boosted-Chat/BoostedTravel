"""
Air Canada connector — EveryMundo airTRFX fare pages.

Air Canada (IATA: AC) is Canada's flag carrier and largest airline.
Star Alliance member, 200+ destinations globally. YYZ/YVR/YUL hubs.

Strategy (httpx, no browser):
  Air Canada uses EveryMundo airTRFX (same platform as Thai Airways).
  1. Fetch route page: aircanada.com/flights/en-ca/flights-from-{origin}-to-{dest}
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

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.aircanada.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
}

# IATA → slug for Air Canada's EveryMundo fare pages.
_IATA_TO_SLUG: dict[str, str] = {
    # Canada
    "YYZ": "toronto", "YUL": "montreal", "YVR": "vancouver",
    "YYC": "calgary", "YEG": "edmonton", "YOW": "ottawa",
    "YWG": "winnipeg", "YHZ": "halifax", "YQB": "quebec-city",
    "YYT": "st-johns", "YQR": "regina", "YXE": "saskatoon",
    "YYJ": "victoria", "YKF": "kitchener",
    # USA
    "JFK": "new-york", "EWR": "newark", "LAX": "los-angeles",
    "SFO": "san-francisco", "ORD": "chicago", "IAD": "washington-dc",
    "BOS": "boston", "MIA": "miami", "FLL": "fort-lauderdale",
    "MCO": "orlando", "LAS": "las-vegas", "DEN": "denver",
    "SEA": "seattle", "DFW": "dallas", "ATL": "atlanta",
    "PHX": "phoenix", "HNL": "honolulu",
    # Europe
    "LHR": "london", "LGW": "london", "CDG": "paris",
    "FRA": "frankfurt", "MUC": "munich", "ZRH": "zurich",
    "AMS": "amsterdam", "BRU": "brussels", "FCO": "rome",
    "MXP": "milan", "BCN": "barcelona", "MAD": "madrid",
    "LIS": "lisbon", "DUB": "dublin", "CPH": "copenhagen",
    "ARN": "stockholm", "ATH": "athens", "BUD": "budapest",
    # Asia
    "NRT": "tokyo", "HND": "tokyo", "HKG": "hong-kong",
    "PVG": "shanghai", "PEK": "beijing", "ICN": "seoul",
    "SIN": "singapore", "DEL": "new-delhi", "BOM": "mumbai",
    "MNL": "manila",
    # Other
    "CUN": "cancun", "SJD": "san-jose-del-cabo",
    "PUJ": "punta-cana", "BOG": "bogota",
    "GRU": "sao-paulo", "SCL": "santiago",
    "MEL": "melbourne", "SYD": "sydney",
    "MEX": "mexico-city",
}


class AirCanadaConnectorClient:
    """Air Canada — EveryMundo airTRFX fare pages."""

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
            logger.warning("Air Canada: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/flights/en-ca/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Air Canada: fetching %s", url)

        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Air Canada: %s returned %d", url, resp.status_code)
                return self._empty(req)
        except Exception as e:
            logger.error("Air Canada fetch error: %s", e)
            return self._empty(req)

        fares = self._extract_fares(resp.text)
        if not fares:
            logger.info("Air Canada: no fares on page %s", url)
            return self._empty(req)

        offers = self._build_offers(fares, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Air Canada %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"aircanada{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "CAD",
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _extract_fares(html: str) -> list[dict]:
        """Extract all fares from ALL StandardFareModules in __NEXT_DATA__."""
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html, re.S,
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

        for fare in fares:
            # Filter by matching origin AND destination
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig != req.origin or dest != req.destination:
                continue

            # Filter by departure date
            dep_date = fare.get("departureDate", "")
            if dep_date[:10] != target_date:
                continue

            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            currency = fare.get("currencyCode") or "CAD"
            price_f = round(float(price), 2)

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            cabin = (fare.get("formattedTravelClass") or "Economy").lower()
            seg = FlightSegment(
                airline="AC",
                airline_name="Air Canada",
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
                f"ac_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"ac_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Air Canada"],
                owner_airline="AC",
                booking_url=(
                    f"https://www.aircanada.com/booking/search"
                    f"?org={req.origin}&dest={req.destination}"
                    f"&depDate={target_date}"
                    f"&ADT={req.adults or 1}&tripType=O"
                ),
                is_locked=False,
                source="aircanada_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"aircanada{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="CAD",
            offers=[],
            total_results=0,
        )
