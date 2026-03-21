"""
EVA Air connector — EveryMundo airTRFX fare pages via curl_cffi.

EVA Air (IATA: BR) is a Taiwanese airline and Star Alliance member.
Hub at Taipei Taoyuan (TPE) with 60+ destinations across Asia, Europe,
North America, and Oceania.

Strategy (curl_cffi required — Cloudflare / Akamai protections):
  1. Resolve IATA codes to city slugs via static mapping
  2. Fetch route page: flights.evaair.com/en-us/flights-from-{origin}-to-{dest}
  3. Extract __NEXT_DATA__ JSON from <script> tag
  4. Parse DpaHeadline + StandardFareModule → fares for route pricing data
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime

from curl_cffi import requests as creq

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE = "https://flights.evaair.com"
_SITE_EDITION = "en-us"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Static slug mapping for EVA Air destinations
_IATA_TO_SLUG: dict[str, str] = {
    # Taiwan
    "TPE": "taipei", "KHH": "kaohsiung", "RMQ": "taichung",
    # Japan
    "NRT": "tokyo", "HND": "tokyo", "KIX": "osaka", "FUK": "fukuoka",
    "CTS": "sapporo", "OKA": "okinawa", "NGO": "nagoya",
    "SDJ": "sendai", "KOJ": "kagoshima", "MYJ": "matsuyama",
    # Korea
    "ICN": "seoul",
    # China / HK / Macau
    "PVG": "shanghai", "PEK": "beijing", "HKG": "hong-kong",
    "MFM": "macau", "CAN": "guangzhou", "SZX": "shenzhen",
    "CTU": "chengdu", "XIY": "xian", "KMG": "kunming",
    "NKG": "nanjing", "HGH": "hangzhou", "WUH": "wuhan",
    "TNA": "jinan", "CSX": "changsha", "TAO": "qingdao",
    # Southeast Asia
    "SIN": "singapore", "BKK": "bangkok",
    "SGN": "ho-chi-minh-city", "HAN": "hanoi",
    "MNL": "manila", "CEB": "cebu",
    "KUL": "kuala-lumpur", "PNH": "phnom-penh",
    "CGK": "jakarta", "DPS": "bali", "SUB": "surabaya",
    "RGN": "yangon",
    # South Asia
    "DEL": "delhi", "BOM": "mumbai", "BLR": "bengaluru",
    "CCU": "kolkata", "MAA": "chennai",
    # North America
    "JFK": "new-york", "EWR": "new-york", "LGA": "new-york",
    "LAX": "los-angeles", "SFO": "san-francisco",
    "ORD": "chicago", "IAH": "houston", "DFW": "dallas",
    "SEA": "seattle", "YVR": "vancouver", "YYZ": "toronto",
    # Europe
    "LHR": "london", "CDG": "paris", "AMS": "amsterdam",
    "FRA": "frankfurt", "VIE": "vienna", "MXP": "milan",
    # Oceania
    "SYD": "sydney", "MEL": "melbourne", "BNE": "brisbane",
    "AKL": "auckland",
    # Middle East
    "DXB": "dubai",
}


class EvaAirConnectorClient:
    """EVA Air — EveryMundo airTRFX route pages via curl_cffi."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("EVA Air: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/{_SITE_EDITION}/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("EVA Air: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("EVA Air fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        offers = self._extract_offers(html, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "EVA Air %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"evaair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome124")
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("EVA Air: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("EVA Air curl_cffi error: %s", e)
            return None

    def _extract_offers(
        self, html: str, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Extract fare offers from __NEXT_DATA__ — DpaHeadline + StandardFareModule."""
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
            re.S,
        )
        if not m:
            logger.info("EVA Air: no __NEXT_DATA__ found")
            return []

        try:
            nd = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            logger.warning("EVA Air: __NEXT_DATA__ JSON parse failed")
            return []

        props = nd.get("props", {}).get("pageProps", {})
        apollo = props.get("apolloState", {}).get("data", {})

        offers: list[FlightOffer] = []
        seen: set[str] = set()

        # 1) DpaHeadline → lowestFare (route-level cheapest)
        for key, val in apollo.items():
            if not isinstance(val, dict) or val.get("__typename") != "DpaHeadline":
                continue
            meta = val.get("metaData", {})
            if not isinstance(meta, dict):
                continue
            headline = meta.get("headline", {})
            if not isinstance(headline, dict):
                continue
            lf = headline.get("lowestFare", {})
            if isinstance(lf, dict) and "__ref" in lf:
                lf = apollo.get(lf["__ref"], {})
            if isinstance(lf, dict):
                offer = self._build_offer_from_fare(lf, req, seen)
                if offer:
                    offers.append(offer)

        # 2) StandardFareModule → fares (cabin-level fares)
        for key, val in apollo.items():
            if not isinstance(val, dict) or val.get("__typename") != "StandardFareModule":
                continue
            fares = val.get("fares", [])
            for fare_ref in fares:
                fare = fare_ref
                if isinstance(fare_ref, dict) and "__ref" in fare_ref:
                    fare = apollo.get(fare_ref["__ref"], {})
                if isinstance(fare, dict):
                    offer = self._build_offer_from_fare(fare, req, seen)
                    if offer:
                        offers.append(offer)

        return offers

    def _build_offer_from_fare(
        self,
        fare: dict,
        req: FlightSearchRequest,
        seen: set[str],
    ) -> FlightOffer | None:
        """Build a FlightOffer from a fare entry."""
        # Prefer USD price if available
        price = fare.get("usdTotalPrice") or fare.get("totalPrice")
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

        # Use USD if usdTotalPrice was used, else original currency
        if fare.get("usdTotalPrice"):
            currency = "USD"
        else:
            currency = fare.get("currencyCode") or "USD"

        origin_code = fare.get("originAirportCode") or req.origin
        dest_code = fare.get("destinationAirportCode") or req.destination
        cabin = (fare.get("formattedTravelClass") or "Economy").strip()

        # Dedup key
        dedup_key = f"{origin_code}_{dest_code}_{dep_date_str}_{price_f}_{cabin}"
        if dedup_key in seen:
            return None
        seen.add(dedup_key)

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            dep_dt = datetime(2000, 1, 1)

        seg = FlightSegment(
            airline="BR",
            airline_name="EVA Air",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city="",
            destination_city="",
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin.lower(),
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        fid = hashlib.md5(
            f"br_{origin_code}{dest_code}{dep_date_str}{price_f}{cabin}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"br_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["EVA Air"],
            owner_airline="BR",
            booking_url=(
                f"https://www.evaair.com/en-us/booking/flight-search/"
                f"?origin={req.origin}&destination={req.destination}"
                f"&outboundDate={dep_date_str}"
                f"&adultCount={req.adults or 1}&tripType=ONE_WAY"
            ),
            is_locked=False,
            source="evaair_direct",
            source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"evaair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
