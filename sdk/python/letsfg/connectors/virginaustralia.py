"""
Virgin Australia connector — Australia's second-largest airline.

Virgin Australia (IATA: VA) — SYD/MEL/BNE hubs.
110+ domestic and short-haul international routes (NZ, Fiji, Bali).

Strategy:
  VA exposes a public JSON feed of promotional/sale fares at:
    GET https://www.virginaustralia.com/feeds/specials.fares_by_origin.json

  Returns ~170KB JSON keyed by origin IATA (lowercase):
    { "syd": { "port_name":"Sydney", "sale_items": [
        { "origin":"SYD", "destination":"MEL", "cabin":"Economy",
          "from_price":79, "display_price":79, "dir":"One Way",
          "travel_periods": [{"start_date":1776211200,"end_date":1782086400,
                              "from_price":79,"fare_brand":"choice"}],
          "url":"https://www.virginaustralia.com/au/en/specials/the-sale/",
          ... }, ...
    ]}}

  ~70 domestic AUS routes with real AUD prices. For each matching O/D pair
  we check whether the requested travel date falls inside any travel_period.
  Feed is cached for the lifetime of the client instance.
"""

from __future__ import annotations

import hashlib
import logging
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

_FARES_URL = "https://www.virginaustralia.com/feeds/specials.fares_by_origin.json"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-AU,en;q=0.9",
}


class VirginAustraliaConnectorClient:
    """Virgin Australia — public promotional fares feed (httpx, no auth)."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None
        self._feed_cache: Optional[dict] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True,
                proxy=get_httpx_proxy_url(),)
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def _load_feed(self) -> dict:
        if self._feed_cache is not None:
            return self._feed_cache
        client = await self._client()
        resp = await client.get(_FARES_URL)
        resp.raise_for_status()
        self._feed_cache = resp.json()
        return self._feed_cache

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        offers: list[FlightOffer] = []

        try:
            feed = await self._load_feed()
            offers = self._parse(feed, req)
        except Exception as e:
            logger.error("VirginAustralia feed error: %s", e)

        offers.sort(key=lambda o: o.price)
        elapsed = time.monotonic() - t0
        logger.info(
            "VirginAustralia %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        sh = hashlib.md5(
            f"va{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "AUD",
            offers=offers,
            total_results=len(offers),
        )

    def _parse(self, feed: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        target_ts = int(datetime.combine(req.date_from, datetime.min.time()).timestamp())
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)

        # Feed is keyed by origin IATA (lowercase)
        origin_data = feed.get(req.origin.lower())
        if not origin_data or not isinstance(origin_data, dict):
            return offers

        for item in origin_data.get("sale_items", []):
            if item.get("origin", "").upper() not in valid_origins:
                continue
            if item.get("destination", "").upper() not in valid_dests:
                continue

            # Check if travel date falls within any travel_period
            best_price = None
            best_brand = None
            for tp in item.get("travel_periods", []):
                start = tp.get("start_date", 0)
                end = tp.get("end_date", 0)
                if start <= target_ts <= end:
                    tp_price = tp.get("from_price", 0)
                    if tp_price > 0 and (best_price is None or tp_price < best_price):
                        best_price = tp_price
                        best_brand = tp.get("fare_brand", "")

            # Fallback: if no period matches, use display_price if route matches
            if best_price is None:
                dp = item.get("display_price") or item.get("from_price") or 0
                if dp > 0:
                    best_price = dp
                    best_brand = item.get("display_fare_brand", "")

            if best_price is None or best_price <= 0:
                continue

            cabin = item.get("cabin", "Economy")
            dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))

            seg = FlightSegment(
                airline="VA",
                airline_name="Virgin Australia",
                flight_no="VA",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            booking_url = item.get("url") or (
                f"https://www.virginaustralia.com/au/en/booking/flights/search/"
                f"?origin={req.origin}&destination={req.destination}"
                f"&date={req.date_from.strftime('%Y-%m-%d')}"
                f"&adults={req.adults or 1}"
            )

            key = f"va_{req.origin}{req.destination}{best_price}{best_brand}"
            oid = hashlib.md5(key.encode()).hexdigest()[:12]

            offers.append(
                FlightOffer(
                    id=f"va_{oid}",
                    price=round(float(best_price), 2),
                    currency="AUD",
                    price_formatted=f"{best_price:.2f} AUD",
                    outbound=route,
                    inbound=None,
                    airlines=["Virgin Australia"],
                    owner_airline="VA",
                    conditions={
                        "cabin": cabin,
                        "fare_brand": best_brand,
                        "price_type": "sale_fare",
                        "connection": item.get("connection", ""),
                    },
                    booking_url=booking_url,
                    is_locked=False,
                    source="virginaustralia_direct",
                    source_tier="free",
                )
            )

        return offers
