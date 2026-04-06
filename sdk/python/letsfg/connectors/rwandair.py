"""
RwandAir connector — EveryMundo airTRFX fare pages via curl_cffi.

RwandAir (IATA: WB) is the flag carrier of Rwanda.
Hub at Kigali International Airport (KGL) with routes across Africa,
Europe, Middle East, and Asia.

Strategy (curl_cffi required — WAF protections):
  1. Resolve IATA codes to city slugs via static mapping
  2. Fetch route page: flights.rwandair.com/en-rw/flights-from-{origin}-to-{dest}
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

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE = "https://flights.rwandair.com"
_SITE_EDITION = "en-rw"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Static slug mapping for RwandAir destinations
_IATA_TO_SLUG: dict[str, str] = {
    # Rwanda
    "KGL": "kigali",
    # East Africa
    "NBO": "nairobi", "EBB": "entebbe", "DAR": "dar-es-salaam",
    "BJM": "bujumbura", "JUB": "juba", "ZNZ": "zanzibar",
    "ADD": "addis-ababa",
    # Southern Africa
    "JNB": "johannesburg", "CPT": "cape-town", "HRE": "harare",
    "LUN": "lusaka", "MPM": "maputo",
    # West Africa
    "LOS": "lagos", "ACC": "accra", "ABV": "abuja",
    "ABJ": "abidjan", "DSS": "dakar",
    # Central Africa
    "DLA": "douala", "BZV": "brazzaville", "FIH": "kinshasa",
    "LBV": "libreville", "LAD": "luanda",
    # Europe
    "CDG": "paris", "LGW": "london", "BRU": "brussels",
    "AMS": "amsterdam", "WAW": "warsaw",
    # Middle East
    "DXB": "dubai", "DOH": "doha",
    # Asia
    "BOM": "mumbai", "CAN": "guangzhou",
}


class RwandAirConnectorClient:
    """RwandAir (WB) — EveryMundo airTRFX route pages via curl_cffi."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("RwandAir: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/{_SITE_EDITION}/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("RwandAir: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("RwandAir fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        offers = self._extract_offers(html, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "RwandAir %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"rwandair{req.origin}{req.destination}{req.date_from}".encode()
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
                logger.warning("RwandAir: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("RwandAir curl_cffi error: %s", e)
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
            for script in re.findall(r'<script[^>]*>(.*?)</script>', html, re.S):
                if '"pageProps"' in script[:300] and len(script) > 50000:
                    m = type("M", (), {"group": lambda self, n: script})()
                    break
            if not m:
                logger.info("RwandAir: no __NEXT_DATA__ found")
                return []

        try:
            nd = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            logger.warning("RwandAir: __NEXT_DATA__ JSON parse failed")
            return []

        props = nd.get("props", {}).get("pageProps", {})

        offers: list[FlightOffer] = []
        seen: set[str] = set()

        def _collect_fares(obj: object) -> None:
            if isinstance(obj, dict):
                if obj.get("__typename") == "Fare" and obj.get("usdTotalPrice"):
                    offer = self._build_offer_from_fare(obj, req, seen)
                    if offer:
                        offers.append(offer)
                for v in obj.values():
                    _collect_fares(v)
            elif isinstance(obj, list):
                for item in obj:
                    _collect_fares(item)

        apollo = props.get("apolloState", {})
        _collect_fares(apollo)

        return offers

    def _build_offer_from_fare(
        self,
        fare: dict,
        req: FlightSearchRequest,
        seen: set[str],
    ) -> FlightOffer | None:
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

        if fare.get("usdTotalPrice"):
            currency = "USD"
        else:
            currency = fare.get("currencyCode") or "USD"

        origin_code = fare.get("originAirportCode") or req.origin
        dest_code = fare.get("destinationAirportCode") or req.destination
        cabin = (fare.get("formattedTravelClass") or "Economy").strip()

        dedup_key = f"{origin_code}_{dest_code}_{dep_date_str}_{price_f}_{cabin}"
        if dedup_key in seen:
            return None
        seen.add(dedup_key)

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            dep_dt = datetime(2000, 1, 1)

        seg = FlightSegment(
            airline="WB",
            airline_name="RwandAir",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city=fare.get("originCity", ""),
            destination_city=fare.get("destinationCity", ""),
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin.lower(),
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        fid = hashlib.md5(
            f"wb_{origin_code}{dest_code}{dep_date_str}{price_f}{cabin}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"wb_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["RwandAir"],
            owner_airline="WB",
            booking_url="https://www.rwandair.com/",
            is_locked=False,
            source="rwandair_direct",
            source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"rwandair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
