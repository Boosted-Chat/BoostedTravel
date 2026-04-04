"""
Skiplagged connector — Hidden-city fare search.

Skiplagged specializes in finding "hidden city" fares — flights where
getting off at a layover is cheaper than flying to the final destination.
Known for US domestic routes especially.

Strategy:
  Uses curl_cffi for TLS impersonation to bypass Cloudflare.
  Endpoint: GET https://skiplagged.com/api/search.php
  Returns flights dict (keyed by flight ID) + itineraries dict.
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

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://skiplagged.com/api/search.php"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://skiplagged.com/",
}


class SkiplaggedConnectorClient:
    """Skiplagged — hidden-city fare search engine."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True,
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        offers: list[FlightOffer] = []
        date_str = req.date_from.strftime("%Y-%m-%d")
        return_date_str = req.return_from.strftime("%Y-%m-%d") if req.return_from else ""

        # Try curl_cffi first (Cloudflare bypass)
        try:
            offers = await self._search_curl(req, date_str, return_date_str)
        except Exception as e:
            logger.debug("Skiplagged curl_cffi failed: %s", e)

        # Fallback to httpx
        if not offers:
            try:
                offers = await self._search_httpx(req, date_str, return_date_str)
            except Exception as e:
                logger.debug("Skiplagged httpx failed: %s", e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Skiplagged %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"skiplagged{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers, total_results=len(offers),
        )

    async def _search_curl(self, req: FlightSearchRequest, date_str: str, return_date_str: str) -> list[FlightOffer]:
        """Use curl_cffi for TLS impersonation to bypass Cloudflare."""
        try:
            from curl_cffi.requests import AsyncSession
        except ImportError:
            return []

        params = {
            "from": req.origin,
            "to": req.destination,
            "depart": date_str,
            "return": return_date_str,
            "format": "v3",
            "counts[adults]": str(req.adults or 1),
            "counts[children]": "0",
        }

        async with AsyncSession(impersonate="chrome131") as s:
            r = await s.get(_SEARCH_URL, params=params, headers=_HEADERS, timeout=self.timeout)
            if r.status_code != 200:
                return []
            data = r.json()
            return self._parse(data, req, date_str, return_date_str)

    async def _search_httpx(self, req: FlightSearchRequest, date_str: str, return_date_str: str) -> list[FlightOffer]:
        """Fallback httpx search (may hit Cloudflare)."""
        client = await self._client()
        params = {
            "from": req.origin,
            "to": req.destination,
            "depart": date_str,
            "return": return_date_str,
            "format": "v3",
            "counts[adults]": str(req.adults or 1),
            "counts[children]": "0",
        }
        r = await client.get(_SEARCH_URL, params=params)
        if r.status_code != 200:
            return []
        data = r.json()
        return self._parse(data, req, date_str, return_date_str)

    def _parse(self, data: dict, req: FlightSearchRequest, date_str: str, return_date_str: str = "") -> list[FlightOffer]:
        """Parse Skiplagged search results.

        Response structure:
          flights: { flight_id: { segments: [...], count: N } }
          itineraries: { outbound: [ { flight, one_way_price, ... } ] }
          or itineraries: list of itinerary objects
        """
        offers: list[FlightOffer] = []
        flights_map = data.get("flights") or {}
        itineraries = data.get("itineraries") or {}

        # Itineraries can be a dict with outbound/inbound lists or a flat list.
        outbound_list: list = []
        inbound_list: list = []
        if isinstance(itineraries, dict):
            outbound_list = itineraries.get("outbound") or itineraries.get("depart") or []
            inbound_list = itineraries.get("inbound") or itineraries.get("return") or []
        elif isinstance(itineraries, list):
            outbound_list = itineraries

        parsed_inbound: list[dict] = []
        if req.return_from and inbound_list:
            for itin in inbound_list[:50]:
                try:
                    route, segments = self._build_route_from_itinerary(
                        itin,
                        flights_map,
                        req.return_from,
                        req.destination,
                        req.origin,
                    )
                    raw_in_price = float(itin.get("one_way_price") or itin.get("price") or itin.get("min_price") or 0)
                    in_price_cents = int(raw_in_price) if raw_in_price > 0 else 0
                    if not segments:
                        continue
                    parsed_inbound.append({
                        "route": route,
                        "segments": segments,
                        "price_cents": in_price_cents,
                        "flight_key": itin.get("flight") or itin.get("flight_key") or "",
                    })
                except Exception as e:
                    logger.debug("Skiplagged parse inbound itin error: %s", e)
                    continue

        for itin in outbound_list[:30]:
            try:
                flight_key = itin.get("flight") or itin.get("flight_key") or ""
                raw_price = float(itin.get("one_way_price") or itin.get("price") or itin.get("min_price") or 0)
                out_price_cents = int(raw_price) if raw_price > 0 else 0
                if out_price_cents <= 0:
                    continue

                currency = itin.get("currency") or "USD"
                min_round_trip_cents = int(float(itin.get("min_round_trip_price") or 0) or 0)

                route, segments = self._build_route_from_itinerary(
                    itin,
                    flights_map,
                    req.date_from,
                    req.origin,
                    req.destination,
                )
                if not segments:
                    continue

                inbound_route = None
                inbound_segments: list[FlightSegment] = []
                chosen_inbound_price_cents = 0
                if req.return_from and parsed_inbound:
                    target_inbound_cents = max(min_round_trip_cents - out_price_cents, 0) if min_round_trip_cents > 0 else 0
                    chosen_inbound = min(
                        parsed_inbound,
                        key=lambda x: abs(x["price_cents"] - target_inbound_cents) if target_inbound_cents > 0 else x["price_cents"],
                    )
                    inbound_route = chosen_inbound["route"]
                    inbound_segments = chosen_inbound["segments"]
                    chosen_inbound_price_cents = chosen_inbound["price_cents"]

                total_cents = min_round_trip_cents
                if total_cents <= 0:
                    # If Skiplagged doesn't provide round-trip total, approximate using one-way prices.
                    total_cents = out_price_cents + chosen_inbound_price_cents
                if total_cents <= 0:
                    total_cents = out_price_cents

                price = total_cents / 100
                oid = hashlib.md5(f"skip_{req.origin}{req.destination}{date_str}{price}{flight_key}".encode()).hexdigest()[:12]

                is_hidden_city = itin.get("is_hidden_city") or itin.get("hidden_city", False)
                booking_url = self._build_booking_url(req.origin, req.destination, date_str, return_date_str)

                airline_codes = list({s.airline for s in segments + inbound_segments if s.airline})
                owner_airline = segments[0].airline if segments and segments[0].airline else "Skiplagged"

                offers.append(FlightOffer(
                    id=f"skip_{oid}", price=round(price, 2), currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route, inbound=inbound_route,
                    airlines=airline_codes,
                    owner_airline=owner_airline,
                    booking_url=booking_url,
                    is_locked=False, source="skiplagged_meta", source_tier="free",
                ))
            except Exception as e:
                logger.debug("Skiplagged parse itin error: %s", e)

        return offers

    def _build_route_from_itinerary(
        self,
        itin: dict,
        flights_map: dict,
        fallback_date,
        default_origin: str,
        default_destination: str,
    ) -> tuple[FlightRoute, list[FlightSegment]]:
        flight_key = itin.get("flight") or itin.get("flight_key") or ""
        flight_data = flights_map.get(flight_key, {})
        seg_list = flight_data.get("segments") or itin.get("segments") or []

        segments: list[FlightSegment] = []
        for seg_data in seg_list:
            carrier = seg_data.get("airline") or seg_data.get("carrier") or ""
            flight_no = seg_data.get("flight_number") or seg_data.get("flightNumber") or ""
            dep_code = seg_data.get("departure") or seg_data.get("origin") or {}
            arr_code = seg_data.get("arrival") or seg_data.get("destination") or {}

            if isinstance(dep_code, dict):
                dep_airport = dep_code.get("airport") or dep_code.get("code") or default_origin
                dep_time = dep_code.get("time") or dep_code.get("datetime") or ""
            else:
                dep_airport = str(dep_code) if dep_code else default_origin
                dep_time = seg_data.get("departure_time") or ""

            if isinstance(arr_code, dict):
                arr_airport = arr_code.get("airport") or arr_code.get("code") or default_destination
                arr_time = arr_code.get("time") or arr_code.get("datetime") or ""
            else:
                arr_airport = str(arr_code) if arr_code else default_destination
                arr_time = seg_data.get("arrival_time") or ""

            dur = seg_data.get("duration") or 0

            try:
                dep_dt = datetime.fromisoformat(dep_time.replace("Z", "+00:00")) if dep_time else datetime.combine(fallback_date, datetime.min.time().replace(hour=8))
                arr_dt = datetime.fromisoformat(arr_time.replace("Z", "+00:00")) if arr_time else dep_dt
            except (ValueError, TypeError):
                dep_dt = datetime.combine(fallback_date, datetime.min.time().replace(hour=8))
                arr_dt = dep_dt

            segments.append(FlightSegment(
                airline=carrier,
                flight_no=f"{carrier}{flight_no}",
                origin=dep_airport,
                destination=arr_airport,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=int(dur),
            ))

        if not segments:
            segments.append(FlightSegment(
                airline="",
                flight_no="",
                origin=default_origin,
                destination=default_destination,
                departure=datetime.combine(fallback_date, datetime.min.time().replace(hour=8)),
                arrival=datetime.combine(fallback_date, datetime.min.time().replace(hour=8)),
                duration_seconds=0,
            ))

        total_dur = sum(s.duration_seconds for s in segments)
        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(0, len(segments) - 1),
        )
        return route, segments

    @staticmethod
    def _build_booking_url(origin: str, destination: str, depart_date: str, return_date: str = "") -> str:
        # Skiplagged uses /flights/<from>/<to>/<depart>/<return> for round-trip deeplinks.
        if return_date:
            return f"https://skiplagged.com/flights/{origin}/{destination}/{depart_date}/{return_date}"
        return f"https://skiplagged.com/flights/{origin}/{destination}/{depart_date}"
