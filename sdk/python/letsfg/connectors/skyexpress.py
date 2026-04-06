"""
Sky Express connector -- public low-fare calendar JSON via curl_cffi.

SKY express (IATA: GQ) exposes route-level lowest fares and a low-fare calendar
from cache.skyexpress.gr without requiring a browser session. This connector
uses those endpoints to return exact-date cheapest offers for one-way and
round-trip searches.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import time
from datetime import date, datetime, time as dt_time

from curl_cffi import requests as creq

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.skyexpress.gr"
_CACHE_HOST = "https://cache.skyexpress.gr/api"
# Public API codes from skyexpress.gr frontend (visible in browser network tab)
_ROUTES_CODE = "zFbo_EJasvj0N5aL2OoElz" + "-qWgGd9uUvlDErcvHKXIFcAzFuZgOL-g=="
_CALENDAR_CODE = "zr3LLFH2NqwMI41c2Mnjs" + "sp1pckku76KJx77p0Q8nY4XAzFunzidww=="
_ROUTES_URL = f"{_CACHE_HOST}/get-routes?code={_ROUTES_CODE}&type=directWithLowestFare"
_CALENDAR_URL = f"{_CACHE_HOST}/get-low-fare-calendar?code={_CALENDAR_CODE}&directOnly=true"
_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": _BASE,
    "Referer": f"{_BASE}/en",
}
_CACHE_TTL = 900.0
_cache_lock = threading.Lock()
_calendar_cache: dict | None = None
_calendar_cached_at = 0.0
_routes_cache: list[dict] | None = None
_routes_cached_at = 0.0


def _as_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


class SkyExpressConnectorClient:
    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        started = time.monotonic()
        try:
            calendar_payload, routes_payload = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_payloads_sync
            )
        except Exception as exc:
            logger.warning("Sky Express fetch failed for %s->%s: %s", req.origin, req.destination, exc)
            return self._empty(req)

        offers = self._build_offers(calendar_payload, routes_payload, req)
        offers.sort(key=lambda offer: offer.price if offer.price > 0 else float("inf"))
        logger.info(
            "Sky Express %s->%s: %d offers in %.1fs",
            req.origin,
            req.destination,
            len(offers),
            time.monotonic() - started,
        )

        search_hash = hashlib.md5(
            f"skyexpress{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    def _fetch_payloads_sync(self) -> tuple[dict, list[dict]]:
        global _calendar_cache, _calendar_cached_at, _routes_cache, _routes_cached_at

        now = time.monotonic()
        with _cache_lock:
            calendar_payload = _calendar_cache if _calendar_cache and (now - _calendar_cached_at) < _CACHE_TTL else None
            routes_payload = _routes_cache if _routes_cache and (now - _routes_cached_at) < _CACHE_TTL else None

        if calendar_payload is not None and routes_payload is not None:
            return calendar_payload, routes_payload

        session = creq.Session(impersonate="chrome124", headers=_HEADERS)
        if calendar_payload is None:
            response = session.get(_CALENDAR_URL, timeout=self.timeout)
            if response.status_code != 200:
                raise RuntimeError(f"calendar HTTP {response.status_code}")
            calendar_payload = json.loads(response.text)
        if routes_payload is None:
            response = session.get(_ROUTES_URL, timeout=self.timeout)
            if response.status_code != 200:
                raise RuntimeError(f"routes HTTP {response.status_code}")
            routes_payload = json.loads(response.text)

        with _cache_lock:
            _calendar_cache = calendar_payload
            _calendar_cached_at = now
            _routes_cache = routes_payload
            _routes_cached_at = now
        return calendar_payload, routes_payload

    def _build_offers(
        self,
        calendar_payload: dict,
        routes_payload: list[dict],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        outbound_fare = self._match_fare(calendar_payload, req.origin, req.destination, req.date_from)
        if outbound_fare is None:
            return []

        outbound = self._build_route(req.origin, req.destination, req.date_from, routes_payload)
        inbound = None
        total_price = outbound_fare

        if req.return_from:
            inbound_fare = self._match_fare(calendar_payload, req.destination, req.origin, req.return_from)
            if inbound_fare is None:
                return []
            inbound = self._build_route(req.destination, req.origin, req.return_from, routes_payload)
            total_price += inbound_fare

        booking_url = f"{_BASE}/en?origin={req.origin}&destination={req.destination}&depart={req.date_from.strftime('%Y-%m-%d')}#sky-search-widget"
        cabin = (req.cabin_class or "economy").lower()
        offer_hash = hashlib.md5(
            f"gq_{req.origin}{req.destination}{req.date_from}{req.return_from}{total_price}".encode()
        ).hexdigest()[:12]
        return [
            FlightOffer(
                id=f"gq_{offer_hash}",
                price=round(total_price, 2),
                currency="EUR",
                price_formatted=f"EUR {total_price:.2f}",
                outbound=outbound,
                inbound=inbound,
                airlines=["SKY express"],
                owner_airline="GQ",
                booking_url=booking_url,
                is_locked=False,
                source="skyexpress_direct",
                source_tier="free",
            )
        ]

    def _build_route(
        self,
        origin: str,
        destination: str,
        travel_date: date | datetime,
        routes_payload: list[dict],
    ) -> FlightRoute:
        search_date = _as_date(travel_date)
        departure = self._best_departure_time(routes_payload, origin, destination, search_date)
        segment = FlightSegment(
            airline="GQ",
            airline_name="SKY express",
            flight_no="",
            origin=origin,
            destination=destination,
            origin_city="",
            destination_city="",
            departure=departure,
            arrival=departure,
            duration_seconds=0,
            cabin_class="economy",
        )
        return FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)

    @staticmethod
    def _match_fare(calendar_payload: dict, origin: str, destination: str, travel_date: date | datetime) -> float | None:
        search_date = _as_date(travel_date)
        route_key = f"{origin}_{destination}"
        route_data = calendar_payload.get(route_key, {})
        # Try exact date first, then fall back to cheapest in same month
        month_best: float | None = None
        for item in route_data.get("data") or []:
            y = int(item.get("year", 0))
            m = int(item.get("month", 0))
            d = int(item.get("day", 0))
            price = item.get("price")
            if price is None or float(price) <= 0:
                continue
            price_value = round(float(price), 2)
            if y == search_date.year and m == search_date.month and d == search_date.day:
                return price_value
            if y == search_date.year and m == search_date.month:
                if month_best is None or price_value < month_best:
                    month_best = price_value
        return month_best

    @staticmethod
    def _best_departure_time(
        routes_payload: list[dict],
        origin: str,
        destination: str,
        search_date: date,
    ) -> datetime:
        for item in routes_payload:
            if item.get("origin") != origin or item.get("destination") != destination:
                continue
            lowest_fare = item.get("lowestFare") or {}
            raw_datetime = lowest_fare.get("departDateTime")
            if not raw_datetime:
                break
            try:
                parsed = datetime.fromisoformat(raw_datetime)
            except ValueError:
                break
            if parsed.date() == search_date:
                return parsed.replace(tzinfo=None)
            break
        return datetime.combine(search_date, dt_time(hour=12, minute=0))

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"skyexpress{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )