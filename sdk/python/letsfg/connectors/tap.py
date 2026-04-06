"""
TAP Air Portugal connector — EveryMundo airTRFX Sputnik API + fare pages.

TAP Air Portugal (IATA: TP) is Portugal's flag carrier. Star Alliance member.
Key hub at LIS connecting Europe, Brazil, Africa, Americas.
90+ destinations. Strong on CPLP countries (Portuguese-speaking).

Strategy:
  Primary: EveryMundo Sputnik grouped-routes API (httpx)
  Fallback: curl_cffi route page with __NEXT_DATA__ extraction
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional

import httpx
from curl_cffi import requests as creq

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_curl_cffi_proxies, get_httpx_proxy_url
from .airline_routes import get_city_airports, resolve_slug, city_match_set

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

_API_URL = "https://openair-california.airtrfx.com/airfare-sputnik-service/v3/tp/fares/grouped-routes"
_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_SPUTNIK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://mm-prerendering-static-prod.airtrfx.com",
    "Referer": "https://mm-prerendering-static-prod.airtrfx.com/",
    "em-api-key": _API_KEY,
}

_MARKETS = ["PT", "BR", "US", "GB", "FR", "DE"]

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
    """TAP Air Portugal — EveryMundo Sputnik API + airTRFX fare pages."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self):
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_SPUTNIK_HEADERS,
                follow_redirects=True, proxy=get_httpx_proxy_url(),
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # Primary: Sputnik grouped-routes API
        offers = await self._try_sputnik(req)

        # Fallback: HTML route page (__NEXT_DATA__)
        if not offers:
            origin_slug = resolve_slug(req.origin, _IATA_TO_SLUG)
            dest_slug = resolve_slug(req.destination, _IATA_TO_SLUG)
            if origin_slug and dest_slug:
                url = f"{_BASE}/flights/en-pt/flights-from-{origin_slug}-to-{dest_slug}"
                logger.info("TAP: Sputnik empty, falling back to HTML %s", url)
                try:
                    html = await asyncio.get_event_loop().run_in_executor(
                        None, self._fetch_sync, url
                    )
                except Exception as e:
                    logger.error("TAP fetch error: %s", e)
                    html = None
                if html:
                    fares = self._extract_fares(html)
                    if fares:
                        offers = self._build_offers(fares, req)

        if not offers:
            offers = []
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

    async def _try_sputnik(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Try Sputnik grouped-routes API for TAP fares."""
        try:
            dt = req.date_from
            if isinstance(dt, datetime):
                dt = dt.date()
            elif not isinstance(dt, date):
                dt = datetime.strptime(str(dt), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            dt = date.today() + timedelta(days=30)

        start = dt - timedelta(days=3)
        end = dt + timedelta(days=30)

        payload = {
            "markets": _MARKETS,
            "languageCode": "en",
            "dataExpirationWindow": "7d",
            "datePattern": "dd MMM yy (E)",
            "outputCurrencies": ["EUR"],
            "departure": {"start": start.isoformat(), "end": end.isoformat()},
            "budget": {"maximum": None},
            "passengers": {"adults": max(1, req.adults or 1)},
            "travelClasses": ["ECONOMY"],
            "flightType": "ROUND_TRIP",
            "flexibleDates": True,
            "faresPerRoute": "10",
            "trfxRoutes": True,
            "routesLimit": 500,
            "sorting": [{"popularity": "DESC"}],
            "airlineCode": "tp",
        }

        try:
            client = await self._client()
            r = await client.post(_API_URL, json=payload)
            if r.status_code != 200:
                logger.info("TAP Sputnik: HTTP %d", r.status_code)
                return []
            data = r.json()
            if not isinstance(data, list):
                return []
        except Exception as e:
            logger.info("TAP Sputnik error: %s", e)
            return []

        origin_set = city_match_set(req.origin)
        dest_set = city_match_set(req.destination)

        offers = []
        for route in data:
            for fare in route.get("fares") or []:
                orig = (fare.get("originAirportCode") or route.get("origin") or "").upper()
                dest = (fare.get("destinationAirportCode") or route.get("destination") or "").upper()
                # Match either: strict (orig in origin_set AND dest in dest_set)
                # or hub-based (dest in dest_set only — TAP is LIS hub)
                if dest not in dest_set:
                    if orig not in origin_set:
                        continue

                price = fare.get("totalPrice") or fare.get("usdTotalPrice")
                if not price or float(price) <= 0:
                    continue
                if fare.get("redemption"):
                    continue

                price_f = round(float(price), 2)
                currency = fare.get("currencyCode") or "EUR"
                dep_str = (fare.get("departureDate") or "")[:10]
                ret_str = (fare.get("returnDate") or "")[:10]
                cabin = (fare.get("farenetTravelClass") or "ECONOMY").lower()

                dep_dt = datetime(2000, 1, 1)
                if dep_str:
                    try:
                        dep_dt = datetime.strptime(dep_str, "%Y-%m-%d")
                    except ValueError:
                        pass

                seg = FlightSegment(
                    airline="TP", airline_name="TAP Air Portugal", flight_no="",
                    origin=orig, destination=dest,
                    origin_city=fare.get("originCity") or "",
                    destination_city=fare.get("destinationCity") or "",
                    departure=dep_dt, arrival=dep_dt,
                    duration_seconds=0, cabin_class=cabin,
                )
                outbound = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

                inbound = None
                if ret_str:
                    try:
                        ret_dt = datetime.strptime(ret_str, "%Y-%m-%d")
                    except ValueError:
                        ret_dt = dep_dt
                    ret_seg = FlightSegment(
                        airline="TP", airline_name="TAP Air Portugal", flight_no="",
                        origin=dest, destination=orig,
                        origin_city=fare.get("destinationCity") or "",
                        destination_city=fare.get("originCity") or "",
                        departure=ret_dt, arrival=ret_dt,
                        duration_seconds=0, cabin_class=cabin,
                    )
                    inbound = FlightRoute(segments=[ret_seg], total_duration_seconds=0, stopovers=0)

                ret_token = f"_{ret_str}" if ret_str else ""
                fid = hashlib.md5(
                    f"tp_{orig}_{dest}_{dep_str}{ret_token}_{price_f}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"tp_{fid}",
                    price=price_f,
                    currency=currency,
                    price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                    outbound=outbound,
                    inbound=inbound,
                    airlines=["TAP Air Portugal"],
                    owner_airline="TP",
                    booking_url=f"{_BASE}/en-us/",
                    is_locked=False,
                    source="tap_direct",
                    source_tier="free",
                    conditions={
                        "trip_type": (fare.get("flightType") or "ROUND_TRIP").lower().replace("_", "-"),
                        "cabin": str(fare.get("formattedTravelClass") or cabin),
                        "fare_note": "Published fare from TAP Air Portugal fare module",
                    },
                ))

        logger.info("TAP Sputnik %s→%s: %d offers", req.origin, req.destination, len(offers))
        return offers

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
                booking_url="https://www.flytap.com/en-us/",
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
