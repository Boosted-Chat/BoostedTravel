"""
Aer Lingus connector — EveryMundo Sputnik API + airTRFX fare pages.

Aer Lingus (IATA: EI) — DUB hub.
IAG Group member. 100+ destinations across Europe and transatlantic.

Strategy:
  Primary: EveryMundo Sputnik grouped-routes API (httpx)
  Fallback: HTML route page with __NEXT_DATA__ extraction
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import date, datetime, timedelta
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

_BASE = "https://www.aerlingus.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IE,en;q=0.9",
}

_API_URL = "https://openair-california.airtrfx.com/airfare-sputnik-service/v3/ei/fares/grouped-routes"
_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_SPUTNIK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://mm-prerendering-static-prod.airtrfx.com",
    "Referer": "https://mm-prerendering-static-prod.airtrfx.com/",
    "em-api-key": _API_KEY,
}

_MARKETS = ["IE", "GB", "US"]

_IATA_TO_SLUG: dict[str, str] = {
    # Ireland
    "DUB": "dublin", "ORK": "cork", "SNN": "shannon",
    "KIR": "tralee", "NOC": "knock",
    # City codes (multi-airport cities)
    "LON": "london", "PAR": "paris", "ROM": "rome", "NYC": "new-york",
    # UK
    "LHR": "london", "LGW": "london", "STN": "london",
    "MAN": "manchester", "BHX": "birmingham",
    "EDI": "edinburgh", "GLA": "glasgow",
    "BRS": "bristol", "NCL": "newcastle",
    "LPL": "liverpool", "EMA": "nottingham",
    "BFS": "belfast", "LBA": "leeds",
    "EXT": "exeter", "CWL": "cardiff",
    # Europe
    "CDG": "paris", "AMS": "amsterdam", "FRA": "frankfurt",
    "BCN": "barcelona", "MAD": "madrid",
    "FCO": "rome", "MXP": "milan",
    "LIS": "lisbon", "PRG": "prague",
    "BUD": "budapest", "VIE": "vienna",
    "BRU": "brussels", "ZRH": "zurich",
    "GVA": "geneva", "MUC": "munich",
    "DUS": "dusseldorf", "HAM": "hamburg",
    "STR": "stuttgart", "BER": "berlin",
    "CPH": "copenhagen", "OSL": "oslo",
    "ARN": "stockholm", "HEL": "helsinki",
    "WAW": "warsaw", "VNO": "vilnius",
    "ATH": "athens",
    "NAP": "naples", "PSA": "pisa",
    "CTA": "catania", "BOD": "bordeaux",
    "NTE": "nantes", "LYS": "lyon",
    "NCE": "nice", "MRS": "marseille",
    "TLS": "toulouse", "SVQ": "sevilla",
    "AGP": "malaga", "ALC": "alicante",
    "FAO": "faro", "PMI": "mallorca",
    "ACE": "lanzarote", "FUE": "fuerteventura",
    "LPA": "gran-canaria", "TFS": "tenerife",
    "DLM": "dalaman", "IZM": "izmir",
    "SPU": "split", "DBV": "dubrovnik",
    "PUY": "pula", "HER": "heraklion",
    "RHO": "rhodes", "JTR": "santorini",
    "CFU": "corfu", "BOJ": "burgas",
    "SZG": "salzburg", "OLB": "olbia",
    "BRI": "brindisi", "TRN": "turin",
    "VCE": "venice", "VRN": "verona",
    "BLQ": "bologna", "FLR": "florence",
    "MLA": "malta", "JER": "jersey",
    # Transatlantic - US
    "JFK": "new-york", "EWR": "newark",
    "BOS": "boston", "ORD": "chicago",
    "LAX": "los-angeles", "SFO": "san-francisco",
    "MIA": "miami", "MCO": "orlando",
    "IAD": "washington", "PHL": "philadelphia",
    "CLT": "charlotte", "SEA": "seattle",
    "MSP": "minneapolis", "DEN": "denver",
    "ATL": "atlanta", "RDU": "raleigh",
    "SAN": "san-diego", "TPA": "tampa",
    "PHX": "phoenix", "SLC": "salt-lake-city",
    "LAS": "las-vegas", "PDX": "portland",
    "BNA": "nashville", "SJC": "san-jose",
    "HNL": "honolulu", "OGG": "maui",
    "CUN": "cancun", "MEX": "mexico-city",
    "SJU": "san-juan",
    # Canada
    "YYZ": "toronto", "YUL": "montreal",
    "YVR": "vancouver", "YYC": "calgary",
    # Caribbean
    "BGI": "bridgetown",
    # Morocco
    "RAK": "marrakech",
}


class AerLingusConnectorClient:
    """Aer Lingus — EveryMundo airTRFX fare pages."""

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

        # Primary: Sputnik grouped-routes API
        offers = await self._try_sputnik(req)

        # Fallback: HTML route page (__NEXT_DATA__)
        if not offers:
            client = await self._client()
            origin_slug = _IATA_TO_SLUG.get(req.origin)
            dest_slug = _IATA_TO_SLUG.get(req.destination)
            if origin_slug and dest_slug:
                url = f"{_BASE}/en-ie/flights-from-{origin_slug}-to-{dest_slug}"
                logger.info("AerLingus: Sputnik empty, falling back to HTML %s", url)
                try:
                    resp = await client.get(url)
                    if resp.status_code in (200, 404) and "__NEXT_DATA__" in resp.text:
                        fares = self._extract_fares(resp.text)
                        if fares:
                            offers = self._build_offers(fares, req)
                except Exception as e:
                    logger.error("AerLingus fetch error: %s", e)

        if not offers:
            offers = []
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("AerLingus %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"aerlingus{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    async def _try_sputnik(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Try Sputnik grouped-routes API for Aer Lingus fares."""
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
            "flightType": "ROUND_TRIP" if req.return_from else "ONE_WAY",
            "flexibleDates": True,
            "faresPerRoute": "10",
            "trfxRoutes": True,
            "routesLimit": 500,
            "sorting": [{"popularity": "DESC"}],
            "airlineCode": "ei",
        }

        try:
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome") as s:
                r = await s.post(_API_URL, json=payload, headers=_SPUTNIK_HEADERS, timeout=self.timeout)
            if r.status_code != 200:
                logger.info("AerLingus Sputnik: HTTP %d", r.status_code)
                return []
            data = r.json()
            if not isinstance(data, list):
                return []
        except Exception as e:
            logger.info("AerLingus Sputnik error: %s", e)
            return []

        origin_set = city_match_set(req.origin)
        dest_set = city_match_set(req.destination)

        offers = []
        for route in data:
            for fare in route.get("fares") or []:
                orig = (fare.get("originAirportCode") or route.get("origin") or "").upper()
                dest = (fare.get("destinationAirportCode") or route.get("destination") or "").upper()
                if orig not in origin_set or dest not in dest_set:
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
                    airline="EI", airline_name="Aer Lingus", flight_no="",
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
                        airline="EI", airline_name="Aer Lingus", flight_no="",
                        origin=dest, destination=orig,
                        origin_city=fare.get("destinationCity") or "",
                        destination_city=fare.get("originCity") or "",
                        departure=ret_dt, arrival=ret_dt,
                        duration_seconds=0, cabin_class=cabin,
                    )
                    inbound = FlightRoute(segments=[ret_seg], total_duration_seconds=0, stopovers=0)

                ret_token = f"_{ret_str}" if ret_str else ""
                fid = hashlib.md5(
                    f"ei_{orig}_{dest}_{dep_str}{ret_token}_{price_f}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"ei_{fid}",
                    price=price_f,
                    currency=currency,
                    price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                    outbound=outbound,
                    inbound=inbound,
                    airlines=["Aer Lingus"],
                    owner_airline="EI",
                    booking_url=f"{_BASE}/booking/select-flights?origin={orig}&destination={dest}&date={dep_str}&adults={req.adults or 1}",
                    is_locked=False,
                    source="aerlingus_direct",
                    source_tier="free",
                    conditions={
                        "trip_type": (fare.get("flightType") or "ROUND_TRIP").lower().replace("_", "-"),
                        "cabin": str(fare.get("formattedTravelClass") or cabin),
                        "fare_note": "Published fare from Aer Lingus fare module",
                    },
                ))

        logger.info("AerLingus Sputnik %s→%s: %d offers", req.origin, req.destination, len(offers))
        return offers

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

            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            currency = fare.get("currencyCode") or "EUR"
            price_f = round(float(price), 2)
            dep_date = fare.get("departureDate", "")

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            cabin = (fare.get("formattedTravelClass") or "Economy").lower()
            seg = FlightSegment(
                airline="EI",
                airline_name="Aer Lingus",
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
                f"ei_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"ei_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Aer Lingus"],
                owner_airline="EI",
                booking_url=(
                    f"https://www.aerlingus.com/booking/select-flights"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={target_date}"
                    f"&adults={req.adults or 1}"
                ),
                is_locked=False,
                source="aerlingus_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"aerlingus{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
