"""
Virgin Atlantic connector — EveryMundo airTRFX Sputnik API + route page fallback.

Virgin Atlantic (IATA: VS) is a UK long-haul airline.
Hub at London Heathrow (LHR) flying to 30+ destinations in the Americas,
Caribbean, Africa, Asia, and Middle East. Part of the SkyTeam alliance.

Strategy:
  Primary: EveryMundo Sputnik fare API with date-specific query (httpx)
  Fallback: curl_cffi route page scraping (__NEXT_DATA__ → DpaHeadline)
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
from .browser import get_httpx_proxy_url

logger = logging.getLogger(__name__)

_BASE = "https://flights.virginatlantic.com"
_SITE_EDITION = "en-gb"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Static slug mapping for VS destinations
_IATA_TO_SLUG: dict[str, str] = {
    # UK origins (+ city codes)
    "LHR": "london", "MAN": "manchester", "EDI": "edinburgh",
    "LON": "london", "LGW": "london", "STN": "london", "LCY": "london", "LTN": "london",
    # US (+ city codes)
    "NYC": "new-york", "JFK": "new-york", "EWR": "new-york", "LGA": "new-york",
    "WAS": "washington-dc",
    "LAX": "los-angeles", "SFO": "san-francisco",
    "BOS": "boston", "MIA": "miami", "ATL": "atlanta",
    "IAD": "washington-dc", "DCA": "washington-dc",
    "ORD": "chicago", "SEA": "seattle", "DFW": "dallas",
    "IAH": "houston", "DTW": "detroit", "MSP": "minneapolis",
    "MCO": "orlando", "TPA": "tampa", "LAS": "las-vegas",
    # Caribbean
    "BGI": "barbados", "MBJ": "montego-bay", "ANU": "antigua",
    "GND": "grenada", "UVF": "st-lucia", "POS": "trinidad",
    "NAS": "nassau", "PUJ": "punta-cana",
    # Americas
    "HAV": "havana", "CUN": "cancun",
    # Middle East / Asia
    "TLV": "tel-aviv", "DXB": "dubai",
    "DEL": "delhi", "BOM": "mumbai",
    "HKG": "hong-kong", "PVG": "shanghai",
    # Africa
    "JNB": "johannesburg", "CPT": "cape-town",
    "NBO": "nairobi", "LOS": "lagos",
    # Europe (partner routes + city codes)
    "PAR": "paris", "ROM": "rome",
    "AMS": "amsterdam", "CDG": "paris", "FCO": "rome",
    "BCN": "barcelona", "ATH": "athens",
}

_AIRPORT_API = "https://openair-california.airtrfx.com/hangar-service/v2/vs/airports/search"
_EM_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"

_SPUTNIK_URL = (
    "https://openair-california.airtrfx.com"
    "/airfare-sputnik-service/v3/vs/fares/search"
)
_SPUTNIK_HEADERS = {
    "EM-API-Key": _EM_API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.virginatlantic.com",
    "Referer": "https://www.virginatlantic.com/",
}

_slug_cache: dict[str, str] = {}
_slug_cache_loaded = False


def _load_slug_cache_sync() -> None:
    global _slug_cache, _slug_cache_loaded
    if _slug_cache_loaded:
        return
    try:
        sess = creq.Session(impersonate="chrome124")
        r = sess.post(
            _AIRPORT_API,
            json={"language": "en", "siteEdition": _SITE_EDITION},
            headers={
                "em-api-key": _EM_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=10,
        )
        if r.status_code == 200:
            for ap in r.json():
                iata = ap.get("iataCode", "")
                city = ap.get("city", {}).get("name", "")
                if iata and city:
                    _slug_cache[iata] = city.lower().replace(" ", "-")
            logger.info("VS: cached %d airport slugs", len(_slug_cache))
    except Exception as e:
        logger.warning("VS: airport cache load failed: %s", e)
    _slug_cache_loaded = True


def _resolve_slug(iata: str) -> str | None:
    slug = _IATA_TO_SLUG.get(iata)
    if slug:
        return slug
    if not _slug_cache_loaded:
        _load_slug_cache_sync()
    return _slug_cache.get(iata)


class VirginAtlanticConnectorClient:
    """Virgin Atlantic — EveryMundo airTRFX route pages via curl_cffi."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # Primary: Sputnik grouped-routes API (multi-fare, route-specific)
        offers = await self._try_sputnik_grouped(req)

        # Secondary: Sputnik search API (single cheapest fare)
        if not offers:
            offers = await self._try_sputnik(req)

        # Fallback: HTML route page (__NEXT_DATA__)
        if not offers:
            origin_slug = _resolve_slug(req.origin)
            dest_slug = _resolve_slug(req.destination)
            if origin_slug and dest_slug:
                url = f"{_BASE}/{_SITE_EDITION}/flights-from-{origin_slug}-to-{dest_slug}"
                logger.info("VS: Sputnik empty, falling back to HTML %s", url)
                try:
                    html = await asyncio.get_event_loop().run_in_executor(
                        None, self._fetch_sync, url
                    )
                except Exception as e:
                    logger.error("VS fetch error: %s", e)
                    html = None
                if html:
                    offers = self._extract_offers(html, req)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "VS %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"vs{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "GBP",
            offers=offers,
            total_results=len(offers),
        )

    async def _try_sputnik_grouped(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Try EveryMundo Sputnik grouped-routes API for multi-fare results."""
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
            "markets": ["GB", "US", "IE"],
            "languageCode": "en",
            "dataExpirationWindow": "7d",
            "datePattern": "dd MMM yy (E)",
            "outputCurrencies": ["GBP", "USD"],
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
            "airlineCode": "vs",
        }

        grouped_url = _SPUTNIK_URL.replace("/fares/search", "/fares/grouped-routes")
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, headers=_SPUTNIK_HEADERS,
                proxy=get_httpx_proxy_url(),
            ) as client:
                r = await client.post(grouped_url, json=payload)
                if r.status_code != 200:
                    logger.info("VS grouped-routes: HTTP %d", r.status_code)
                    return []
                data = r.json()
                if not isinstance(data, list):
                    return []
        except Exception as e:
            logger.info("VS grouped-routes error: %s", e)
            return []

        from .airline_routes import city_match_set
        origin_set = city_match_set(req.origin)
        dest_set = city_match_set(req.destination)

        offers = []
        for route in data:
            for fare in route.get("fares") or []:
                orig = (fare.get("originAirportCode") or route.get("origin") or "").upper()
                dest = (fare.get("destinationAirportCode") or route.get("destination") or "").upper()
                # Filter by correct route - BOTH origin AND destination must match
                if orig not in origin_set or dest not in dest_set:
                    continue

                price = fare.get("totalPrice") or fare.get("usdTotalPrice")
                if not price or float(price) <= 0:
                    continue
                # Skip redemption/miles fares
                if fare.get("redemption"):
                    continue

                price_f = round(float(price), 2)
                currency = fare.get("currencyCode") or "GBP"
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
                    airline="VS", airline_name="Virgin Atlantic", flight_no="",
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
                        airline="VS", airline_name="Virgin Atlantic", flight_no="",
                        origin=dest, destination=orig,
                        origin_city=fare.get("destinationCity") or "",
                        destination_city=fare.get("originCity") or "",
                        departure=ret_dt, arrival=ret_dt,
                        duration_seconds=0, cabin_class=cabin,
                    )
                    inbound = FlightRoute(segments=[ret_seg], total_duration_seconds=0, stopovers=0)

                ret_token = f"_{ret_str}" if ret_str else ""
                fid = hashlib.md5(
                    f"vs_{orig}_{dest}_{dep_str}{ret_token}_{price_f}".encode()
                ).hexdigest()[:12]

                target_date = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
                offers.append(FlightOffer(
                    id=f"vs_{fid}",
                    price=price_f,
                    currency=currency,
                    price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                    outbound=outbound,
                    inbound=inbound,
                    airlines=["Virgin Atlantic"],
                    owner_airline="VS",
                    booking_url=(
                        f"https://www.virginatlantic.com/book/flights"
                        f"?origin={req.origin}&destination={req.destination}"
                        f"&outboundDate={target_date}"
                        f"&adultCount={req.adults or 1}&tripType=ONE_WAY"
                    ),
                    is_locked=False,
                    source="virginatlantic_direct",
                    source_tier="free",
                    conditions={
                        "trip_type": (fare.get("flightType") or "ROUND_TRIP").lower().replace("_", "-"),
                        "cabin": str(fare.get("formattedTravelClass") or cabin),
                        "fare_note": "Published fare from Virgin Atlantic fare module",
                    },
                ))

        logger.info("VS grouped-routes %s→%s: %d offers", req.origin, req.destination, len(offers))
        return offers

    async def _try_sputnik(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Try EveryMundo Sputnik search API for single cheapest fare."""
        try:
            dt = req.date_from
            if isinstance(dt, datetime):
                dt = dt.date()
            elif not isinstance(dt, date):
                dt = datetime.strptime(str(dt), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            dt = date.today() + timedelta(days=30)

        days_from_now = (dt - date.today()).days
        if days_from_now < 1:
            days_from_now = 1

        payload = {
            "origins": [req.origin],
            "destinations": [req.destination],
            "departureDaysInterval": {
                "min": max(0, days_from_now - 1),
                "max": days_from_now + 3,
            },
            "journeyType": "ONE_WAY",
        }

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, headers=_SPUTNIK_HEADERS,
                proxy=get_httpx_proxy_url(),) as client:
                r = await client.post(_SPUTNIK_URL, json=payload)
                if r.status_code != 200:
                    logger.info("VS Sputnik: HTTP %d", r.status_code)
                    return []
                fares = r.json()
                if not isinstance(fares, list):
                    return []
        except Exception as e:
            logger.info("VS Sputnik error: %s", e)
            return []

        offers = []
        for fare in fares:
            offer = self._build_sputnik_offer(fare, req)
            if offer:
                offers.append(offer)

        logger.info("VS Sputnik %s→%s: %d fares", req.origin, req.destination, len(offers))
        return offers

    def _build_sputnik_offer(
        self, fare: dict, req: FlightSearchRequest,
    ) -> FlightOffer | None:
        ps = fare.get("priceSpecification", {})
        ob = fare.get("outboundFlight", {})

        price = ps.get("usdTotalPrice") or ps.get("totalPrice")
        if not price:
            return None
        try:
            price_f = round(float(price), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        currency = "USD" if ps.get("usdTotalPrice") else (ps.get("currencyCode") or "GBP")

        dep_date_str = fare.get("departureDate", "")[:10]
        if not dep_date_str:
            return None

        origin_code = ob.get("departureAirportIataCode") or req.origin
        dest_code = ob.get("arrivalAirportIataCode") or req.destination
        cabin_input = ob.get("fareClassInput") or ob.get("fareClass") or "Economy"
        cabin = cabin_input.split()[0].lower() if cabin_input else "economy"

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            dep_dt = datetime(2000, 1, 1)

        seg = FlightSegment(
            airline="VS",
            airline_name="Virgin Atlantic",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city="",
            destination_city="",
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        target_date = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
        fid = hashlib.md5(
            f"vs_{origin_code}{dest_code}{dep_date_str}{price_f}{cabin}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"vs_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Virgin Atlantic"],
            owner_airline="VS",
            booking_url=(
                f"https://www.virginatlantic.com/book/flights"
                f"?origin={req.origin}&destination={req.destination}"
                f"&outboundDate={target_date}"
                f"&adultCount={req.adults or 1}&tripType=ONE_WAY"
            ),
            is_locked=False,
            source="virginatlantic_direct",
            source_tier="free",
        )

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome124")
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("VS: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("VS curl_cffi error: %s", e)
            return None

    def _extract_offers(
        self, html: str, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
            re.S,
        )
        if not m:
            logger.info("VS: no __NEXT_DATA__ found")
            return []

        try:
            nd = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            logger.warning("VS: __NEXT_DATA__ JSON parse failed")
            return []

        props = nd.get("props", {}).get("pageProps", {})
        apollo = props.get("apolloState", {}).get("data", {})

        offers: list[FlightOffer] = []
        target_date = req.date_from.strftime("%Y-%m-%d")

        for key, val in apollo.items():
            if not isinstance(val, dict):
                continue
            if val.get("__typename") != "DpaHeadline":
                continue

            meta = val.get("metaData", {})
            if not isinstance(meta, dict):
                continue

            headline = meta.get("headline", {})
            if not isinstance(headline, dict):
                continue

            lowest_fare = headline.get("lowestFare", {})
            if not isinstance(lowest_fare, dict):
                continue

            offer = self._build_offer_from_fare(lowest_fare, req, target_date)
            if offer:
                offers.append(offer)

        return offers

    def _build_offer_from_fare(
        self,
        fare: dict,
        req: FlightSearchRequest,
        target_date: str,
    ) -> FlightOffer | None:
        price = fare.get("totalPrice")
        if not price:
            return None
        try:
            price_f = round(float(price), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        dep_date_str = fare.get("departureDate", "")[:10]
        if not dep_date_str:
            return None

        currency = fare.get("currencyCode") or "GBP"
        origin_code = fare.get("originAirportCode") or req.origin
        dest_code = fare.get("destinationAirportCode") or req.destination
        cabin = (fare.get("formattedTravelClass") or "Economy").lower()

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            dep_dt = datetime(2000, 1, 1)

        seg = FlightSegment(
            airline="VS",
            airline_name="Virgin Atlantic",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city="",
            destination_city="",
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        fid = hashlib.md5(
            f"vs_{origin_code}{dest_code}{dep_date_str}{price_f}{cabin}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"vs_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=(
                fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}"
            ),
            outbound=route,
            inbound=None,
            airlines=["Virgin Atlantic"],
            owner_airline="VS",
            booking_url=(
                f"https://www.virginatlantic.com/book/flights"
                f"?origin={req.origin}&destination={req.destination}"
                f"&outboundDate={target_date}"
                f"&adultCount={req.adults or 1}&tripType=ONE_WAY"
            ),
            is_locked=False,
            source="virginatlantic_direct",
            source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"vs{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="GBP",
            offers=[],
            total_results=0,
        )
