"""
Google Flights connector via fli's reverse-engineered Google Flights client.

This replaces the old SerpAPI dependency path while keeping the same connector
ID and output shape expected by the worker.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime
from typing import Optional
from urllib.parse import quote

from fli.core import resolve_airport
from fli.models import (
    FlightSearchFilters,
    FlightSegment as FliFlightSegment,
    MaxStops,
    PassengerInfo,
    SeatType,
    SortBy,
    TripType,
)
from fli.search import SearchFlights
from letsfg.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

from .airline_routes import get_city_airports

logger = logging.getLogger(__name__)

_PRIMARY_CITY_AIRPORTS: dict[str, str] = {
    "NYC": "JFK",
    "LON": "LHR",
    "PAR": "CDG",
    "ROM": "FCO",
    "MIL": "MXP",
    "WAS": "IAD",
    "TYO": "HND",
    "OSA": "KIX",
    "SEL": "ICN",
    "RIO": "GIG",
    "CHI": "ORD",
    "BJS": "PEK",
    "SHA": "PVG",
    "STO": "ARN",
    "MOW": "SVO",
    "BUE": "EZE",
    "SAO": "GRU",
    "JKT": "CGK",
    "YTO": "YYZ",
    "YMQ": "YUL",
    "REK": "KEF",
}


class SerpApiGoogleConnectorClient:
    """Google Flights via fli's direct API client."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        return None

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        try:
            return await asyncio.wait_for(asyncio.to_thread(self._search_sync, req), timeout=self.timeout)
        except asyncio.TimeoutError:
            logger.warning("Google Flights (fli) timed out for %s→%s", req.origin, req.destination)
            return self._empty(req)
        except Exception as exc:
            logger.warning("Google Flights (fli) error: %s", exc)
            return self._empty(req)

    def _search_sync(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        filters = self._build_filters(req)
        raw_results = []
        for attempt in range(1, 3):
            raw_results = SearchFlights().search(filters) or []
            if raw_results:
                break

            remaining_budget = self.timeout - (time.monotonic() - t0)
            if attempt == 2 or remaining_budget <= 15:
                break

            logger.warning(
                "Google Flights (fli) returned no results for %s→%s on attempt %d; retrying",
                req.origin,
                req.destination,
                attempt,
            )

        offers_by_id: dict[str, FlightOffer] = {}
        for item in raw_results:
            offer = self._parse_result(item, req)
            if offer:
                offers_by_id[offer.id] = offer

        offers = list(offers_by_id.values())

        offers.sort(key=lambda offer: offer.price if offer.price > 0 else float("inf"))
        limit = req.limit or len(offers)
        offers = offers[:limit]
        elapsed = time.monotonic() - t0

        logger.info(
            "Google Flights (fli) %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"fli_google{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]

        currency = offers[0].currency if offers else (req.currency or "USD")
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=offers,
            total_results=len(offers),
        )

    def _build_filters(self, req: FlightSearchRequest) -> FlightSearchFilters:
        trip_type = TripType.ROUND_TRIP if req.return_from else TripType.ONE_WAY
        origin_airport = self._resolve_airport(req.origin)
        destination_airport = self._resolve_airport(req.destination)
        segments = [
            FliFlightSegment(
                departure_airport=[[origin_airport, 0]],
                arrival_airport=[[destination_airport, 0]],
                travel_date=req.date_from.strftime("%Y-%m-%d"),
            )
        ]
        if req.return_from:
            segments.append(
                FliFlightSegment(
                    departure_airport=[[destination_airport, 0]],
                    arrival_airport=[[origin_airport, 0]],
                    travel_date=req.return_from.strftime("%Y-%m-%d"),
                )
            )

        return FlightSearchFilters(
            trip_type=trip_type,
            passenger_info=PassengerInfo(
                adults=req.adults or 1,
                children=req.children or 0,
                infants_in_seat=req.infants or 0,
                infants_on_lap=0,
            ),
            flight_segments=segments,
            seat_type=self._map_seat_type(req.cabin_class),
            stops=self._map_stops(req.max_stopovers),
            sort_by=SortBy.CHEAPEST,
            show_all_results=True,
        )

    def _candidate_airports(self, code: str) -> list[str]:
        normalized = (code or "").strip().upper()
        candidates: list[str] = []

        if normalized:
            candidates.append(normalized)

        primary = _PRIMARY_CITY_AIRPORTS.get(normalized)
        if primary:
            candidates.append(primary)

        candidates.extend(get_city_airports(normalized))

        ordered: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            airport = (candidate or "").strip().upper()
            if airport and airport not in seen:
                seen.add(airport)
                ordered.append(airport)
        return ordered

    def _resolve_airport(self, code: str):
        normalized = (code or "").strip().upper()
        for candidate in self._candidate_airports(normalized):
            try:
                return resolve_airport(candidate)
            except Exception:
                continue

        raise ValueError(f"Unsupported Google Flights airport/city code: {normalized}")

    @staticmethod
    def _map_seat_type(cabin_class: str | None) -> SeatType:
        mapping = {
            "M": "ECONOMY",
            "W": "PREMIUM_ECONOMY",
            "C": "BUSINESS",
            "F": "FIRST",
        }
        return getattr(SeatType, mapping.get(cabin_class or "", "ECONOMY"), SeatType.ECONOMY)

    @staticmethod
    def _map_stops(max_stopovers: int | None) -> MaxStops:
        if max_stopovers == 0:
            return MaxStops.NON_STOP
        if max_stopovers == 1:
            return MaxStops.ONE_STOP_OR_FEWER
        if max_stopovers == 2:
            return MaxStops.TWO_OR_FEWER_STOPS
        return MaxStops.ANY

    def _parse_result(self, item, req: FlightSearchRequest) -> Optional[FlightOffer]:
        if isinstance(item, tuple):
            outbound_result = item[0] if len(item) > 0 else None
            inbound_result = item[1] if len(item) > 1 else None
        else:
            outbound_result = item
            inbound_result = None

        if outbound_result is None:
            return None

        outbound = self._build_route(outbound_result)
        inbound = self._build_route(inbound_result) if inbound_result is not None else None
        if outbound is None:
            return None

        prices = [
            float(result.price)
            for result in (outbound_result, inbound_result)
            if result is not None and getattr(result, "price", None)
        ]
        if not prices:
            return None
        price = max(prices)

        currency = None
        for result in (outbound_result, inbound_result):
            if result is not None and getattr(result, "currency", None):
                currency = result.currency
                break
        currency = currency or req.currency or "USD"

        all_segments = list(outbound.segments) + (list(inbound.segments) if inbound else [])
        airlines = list(dict.fromkeys(segment.airline for segment in all_segments if segment.airline))
        owner_airline = outbound.segments[0].airline if outbound.segments else (airlines[0] if airlines else "")

        offer_key = "|".join(
            f"{segment.airline}:{segment.flight_no}:{segment.origin}:{segment.destination}:{segment.departure.isoformat()}"
            for segment in all_segments
        )
        offer_id = hashlib.md5(f"gf_fli_{offer_key}_{price}".encode()).hexdigest()[:12]

        return FlightOffer(
            id=f"gf_{offer_id}",
            price=price,
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=outbound,
            inbound=inbound,
            airlines=airlines,
            owner_airline=owner_airline,
            booking_url=self._build_search_url(req, currency),
            is_locked=False,
            source="serpapi_google",
            source_tier="free",
        )

    @staticmethod
    def _build_route(result) -> Optional[FlightRoute]:
        if result is None or not getattr(result, "legs", None):
            return None

        segments: list[FlightSegment] = []
        for leg in result.legs:
            airline = getattr(leg.airline, "value", str(leg.airline or ""))
            origin = getattr(leg.departure_airport, "name", str(leg.departure_airport or ""))
            destination = getattr(leg.arrival_airport, "name", str(leg.arrival_airport or ""))
            segments.append(
                FlightSegment(
                    airline=airline,
                    flight_no=str(getattr(leg, "flight_number", "") or ""),
                    origin=origin,
                    destination=destination,
                    departure=getattr(leg, "departure_datetime", datetime(2000, 1, 1)),
                    arrival=getattr(leg, "arrival_datetime", datetime(2000, 1, 1)),
                    duration_seconds=int((getattr(leg, "duration", 0) or 0) * 60),
                )
            )

        return FlightRoute(
            segments=segments,
            total_duration_seconds=int((getattr(result, "duration", 0) or 0) * 60),
            stopovers=int(getattr(result, "stops", max(0, len(segments) - 1)) or 0),
        )

    @staticmethod
    def _build_search_url(req: FlightSearchRequest, currency: str) -> str:
        query = f"Flights from {req.origin} to {req.destination} on {req.date_from.strftime('%Y-%m-%d')}"
        if req.return_from:
            query += f" returning {req.return_from.strftime('%Y-%m-%d')}"
        return f"https://www.google.com/travel/flights?q={quote(query)}&curr={quote(currency)}"

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
            offers=[],
            total_results=0,
        )