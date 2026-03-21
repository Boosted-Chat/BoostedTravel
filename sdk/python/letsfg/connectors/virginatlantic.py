"""
Virgin Atlantic connector — EveryMundo airTRFX fare pages via curl_cffi.

Virgin Atlantic (IATA: VS) is a UK long-haul airline.
Hub at London Heathrow (LHR) flying to 30+ destinations in the Americas,
Caribbean, Africa, Asia, and Middle East. Part of the SkyTeam alliance.

Strategy (curl_cffi required — Cloudflare / Akamai protections):
  1. Resolve IATA codes to city slugs via VS's airport lookup API
  2. Fetch route page: flights.virginatlantic.com/en-gb/flights-from-{origin}-to-{dest}
  3. Extract __NEXT_DATA__ JSON from <script> tag
  4. Parse DpaHeadline → lowestFare for route pricing data
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

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE = "https://flights.virginatlantic.com"
_SITE_EDITION = "en-gb"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Static slug mapping for VS destinations
_IATA_TO_SLUG: dict[str, str] = {
    # UK origins
    "LHR": "london", "MAN": "manchester", "EDI": "edinburgh",
    # US
    "JFK": "new-york", "EWR": "new-york", "LGA": "new-york",
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
    # Europe (partner routes)
    "AMS": "amsterdam", "CDG": "paris", "FCO": "rome",
    "BCN": "barcelona", "ATH": "athens",
}

_AIRPORT_API = "https://openair-california.airtrfx.com/hangar-service/v2/vs/airports/search"
_EM_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"

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

        origin_slug = _resolve_slug(req.origin)
        dest_slug = _resolve_slug(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("VS: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/{_SITE_EDITION}/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("VS: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("VS fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

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
