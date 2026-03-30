"""
TAP Air Portugal connector — EveryMundo airTRFX fare pages.

TAP Air Portugal (IATA: TP) is Portugal's flag carrier. Star Alliance member.
Key hub at LIS connecting Europe, Brazil, Africa, Americas.
90+ destinations. Strong on CPLP countries (Portuguese-speaking).

Strategy (curl_cffi required — Cloudflare blocks httpx Python TLS fingerprint):
  TAP uses EveryMundo airTRFX (same platform as Thai Airways, Air Canada).
  1. Fetch route page: flytap.com/flights/en-pt/flights-from-{origin}-to-{dest}
  2. Extract __NEXT_DATA__ JSON from <script> tag
  3. Parse StandardFareModule fares from Apollo GraphQL state
  4. Filter by matching origin/destination airport codes and departure date
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime
from typing import Optional

from curl_cffi import requests as creq

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_curl_cffi_proxies
from .airline_routes import get_city_airports, resolve_slug

logger = logging.getLogger(__name__)

_BASE = "https://www.flytap.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_IATA_TO_SLUG: dict[str, str] = {
    # Portugal
    "LIS": "lisbon", "OPO": "porto", "FAO": "faro",
    "FNC": "funchal", "PDL": "ponta-delgada",
    # Europe — city codes + airport codes
    "LON": "london", "LHR": "london", "LGW": "london",
    "PAR": "paris", "CDG": "paris",
    "ORY": "paris", "FRA": "frankfurt", "MUC": "munich",
    "AMS": "amsterdam", "BRU": "brussels", "ZRH": "zurich",
    "GVA": "geneva", "ROM": "rome", "FCO": "rome", "MXP": "milan",
    "BCN": "barcelona", "MAD": "madrid", "AGP": "malaga",
    "BER": "berlin", "DUS": "dusseldorf", "HAM": "hamburg",
    "VIE": "vienna", "CPH": "copenhagen", "ARN": "stockholm",
    "OSL": "oslo", "HEL": "helsinki", "WAW": "warsaw",
    "PRG": "prague", "BUD": "budapest", "DUB": "dublin",
    "MAN": "manchester", "EDI": "edinburgh", "ATH": "athens",
    "IST": "istanbul", "LCA": "larnaca",
    # Brazil
    "GRU": "sao-paulo", "GIG": "rio-de-janeiro",
    "BSB": "brasilia", "CNF": "belo-horizonte",
    "SSA": "salvador", "REC": "recife", "FOR": "fortaleza",
    "POA": "porto-alegre", "CWB": "curitiba", "BEL": "belem",
    "NAT": "natal", "MCP": "macapa",
    # Africa
    "CMN": "casablanca", "RAK": "marrakech",
    "ACC": "accra", "LOS": "lagos", "MPM": "maputo",
    "DSS": "dakar", "ABJ": "abidjan",
    "LAD": "luanda", "PRN": "pristina",
    # Americas
    "NYC": "new-york", "EWR": "newark", "JFK": "new-york", "BOS": "boston",
    "MIA": "miami", "IAD": "washington-dc", "SFO": "san-francisco",
    "YYZ": "toronto", "YUL": "montreal",
    "CUN": "cancun", "BOG": "bogota",
}


class TapConnectorClient:
    """TAP Air Portugal — EveryMundo airTRFX fare pages."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        origin_slug = resolve_slug(req.origin, _IATA_TO_SLUG)
        dest_slug = resolve_slug(req.destination, _IATA_TO_SLUG)
        if not origin_slug or not dest_slug:
            logger.warning("TAP: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/flights/en-pt/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("TAP: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("TAP fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        fares = self._extract_fares(html)
        if not fares:
            logger.info("TAP: no fares on page %s", url)
            return self._empty(req)

        offers = self._build_offers(fares, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("TAP %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"tap{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("TAP: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("TAP curl_cffi error: %s", e)
            return None

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

        # City-aware matching: LON matches LHR, LGW, STN, etc.
        valid_origins = set(get_city_airports(req.origin))
        valid_origins.add(req.origin)
        valid_dests = set(get_city_airports(req.destination))
        valid_dests.add(req.destination)

        # First pass: exact date. Second pass: any date (±30 days).
        matched_fares: list[dict] = []
        fallback_fares: list[dict] = []

        for fare in fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig not in valid_origins or dest not in valid_dests:
                continue
            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue
            dep_date = fare.get("departureDate", "")
            if dep_date[:10] == target_date:
                matched_fares.append(fare)
            else:
                fallback_fares.append(fare)

        # Use exact-date fares if available, otherwise use all route fares
        use_fares = matched_fares if matched_fares else fallback_fares

        for fare in use_fares:
            dep_date = fare.get("departureDate", "")
            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
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
                airline="TP",
                airline_name="TAP Air Portugal",
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
                f"tp_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            is_exact_date = dep_date[:10] == target_date
            conditions = {}
            if not is_exact_date:
                conditions["price_type"] = "nearby_date"
                conditions["fare_date"] = dep_date[:10]

            offers.append(FlightOffer(
                id=f"tp_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["TAP Air Portugal"],
                owner_airline="TP",
                conditions=conditions,
                booking_url=(
                    f"https://www.flytap.com/en-us/booking"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={target_date}&adults={req.adults or 1}&type=OW"
                ),
                is_locked=False,
                source="tap_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"tap{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
