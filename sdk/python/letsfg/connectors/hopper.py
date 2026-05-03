"""
Hopper commerce API connector — queries Hopper's flight availability endpoint.

Hopper exposes a commerce API at /commerce-api/v1/flights/availability used by
their Next.js SPA (hopper.com/air/shop). The commerce API requires session
cookies obtained from an initial page visit, so we bootstrap cookies by
loading the shop page HTML first, then call the JSON API.

Uses httpx with browser-like headers. Falls back gracefully on failure.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Optional

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_httpx_proxy_url

logger = logging.getLogger(__name__)

HOPPER_BASE = "https://hopper.com"
AVAILABILITY_URL = f"{HOPPER_BASE}/commerce-api/v1/flights/availability"
SESSION_URL = f"{HOPPER_BASE}/commerce-api/v1/session/current"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


class HopperConnectorClient:
    """Direct connector for Hopper's commerce availability API."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None
        self._cookies_ready = False

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout,
                headers=_HEADERS,
                follow_redirects=True,
                proxy=get_httpx_proxy_url(),
            )
            self._cookies_ready = False
        return self._http

    async def _bootstrap_session(self, client: httpx.AsyncClient, req: FlightSearchRequest) -> None:
        """Load the shop page to acquire session cookies, then POST session/current."""
        if self._cookies_ready:
            return
        try:
            # Step 1: GET the shop page — this sets __cf_bm, __cfruid, etc.
            shop_url = self._build_booking_url(req)
            page_resp = await client.get(
                shop_url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                },
            )
            logger.debug("Hopper page load: %d, cookies: %s",
                         page_resp.status_code,
                         list(client.cookies.jar))

            # Step 2: POST session/current — Next.js app does this on load
            await client.post(
                SESSION_URL,
                json={},
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Referer": shop_url,
                    "Origin": HOPPER_BASE,
                },
            )

            self._cookies_ready = True
        except Exception as e:
            logger.warning("Hopper session bootstrap failed: %s", e)

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Hopper commerce availability API.

        POST /commerce-api/v1/flights/availability
        Returns real-time fares from multiple airlines via Hopper's aggregation.
        """
        t0 = time.monotonic()
        client = await self._client()

        # Bootstrap session cookies from a page load
        await self._bootstrap_session(client, req)

        # Build request payload with Hopper's tagged-union format
        trip_type = "RoundTrip" if req.return_from else "OneWay"
        payload: dict[str, Any] = {
            "AvailabilityRequest": trip_type,
            "route": {
                "origin": {
                    "FlightsLocation": "Airport",
                    "code": req.origin,
                    "label": req.origin,
                    "iataCode": req.origin,
                },
                "destination": {
                    "FlightsLocation": "Airport",
                    "code": req.destination,
                    "label": req.destination,
                    "iataCode": req.destination,
                },
            },
            "departureDate": req.date_from.isoformat(),
            "passengerCounts": self._build_passenger_counts(req),
        }

        if req.return_from:
            payload["returnDate"] = req.return_from.isoformat()

        try:
            resp = await client.post(
                AVAILABILITY_URL,
                json=payload,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Referer": self._build_booking_url(req),
                    "Origin": HOPPER_BASE,
                },
            )
        except httpx.TimeoutException:
            logger.warning("Hopper API timed out (%s→%s)", req.origin, req.destination)
            return self._empty(req)
        except Exception as e:
            logger.error("Hopper API error (%s→%s): %s", req.origin, req.destination, e)
            return self._empty(req)

        elapsed = time.monotonic() - t0

        if resp.status_code != 200:
            logger.warning(
                "Hopper API %s→%s returned %d: %s",
                req.origin, req.destination,
                resp.status_code,
                resp.text[:300],
            )
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("Hopper returned non-JSON response")
            return self._empty(req)

        offers = self._parse_response(data, req)
        offers.sort(key=lambda o: o.price)

        # Respect limit
        if len(offers) > req.limit:
            offers = offers[: req.limit]

        logger.info(
            "Hopper %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"hopper{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=offers,
            total_results=len(offers),
        )

    # ── Internal helpers ────────────────────────────────────────────────────

    @staticmethod
    def _build_passenger_counts(req: FlightSearchRequest) -> list[dict]:
        counts = []
        if req.adults:
            counts.append({"passengerType": "adult", "count": req.adults})
        if req.children:
            counts.append({"passengerType": "child", "count": req.children})
        if req.infants:
            counts.append({"passengerType": "infantOnLap", "count": req.infants})
        return counts

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Hopper availability response into FlightOffer list."""
        flights = data.get("flights", {})
        if not flights:
            return []

        # Build lookup maps
        airline_map = self._build_airline_map(data.get("airlines", []))
        slice_map = self._build_slice_map(flights.get("slices", []))
        fare_slice_map = {
            fs["fareSliceId"]: fs
            for fs in flights.get("fareSlice", [])
            if "fareSliceId" in fs
        }
        fare_map = {
            f["fareId"]: f for f in flights.get("fares", []) if "fareId" in f
        }
        trip_map = {
            t["tripId"]: t for t in flights.get("trips", []) if "tripId" in t
        }

        offers = []
        seen_keys: set[str] = set()

        # Iterate over fares (each fare = one bookable offer)
        for fare in flights.get("fares", []):
            fare_id = fare.get("fareId", "")
            trip_id = fare.get("tripId", "")
            trip = trip_map.get(trip_id)
            if not trip:
                continue

            price_bd = fare.get("priceBreakdown", {})
            total = price_bd.get("total", 0)
            currency = price_bd.get("currencyCode", "USD")

            if total <= 0:
                continue

            # Get outbound slice
            outbound_slice_id = trip.get("outboundSlice", "")
            outbound_slice = slice_map.get(outbound_slice_id)
            if not outbound_slice:
                continue

            # Get fare brand info
            outbound_fare_slice_id = fare.get("outboundFareSliceId", "")
            fare_slice = fare_slice_map.get(outbound_fare_slice_id, {})
            brand_name = fare_slice.get("fareBrandName", "")

            # Build outbound route
            outbound_route = self._build_route(outbound_slice, airline_map, req.cabin_class or "M")
            if not outbound_route:
                continue

            # Filter by max stopovers
            if outbound_route.stopovers > req.max_stopovers:
                continue

            # Build inbound route for round trips
            inbound_route = None
            inbound_slice_id = (
                trip.get("inboundSlice", "")
                or trip.get("returnSlice", "")
            )
            if inbound_slice_id:
                inbound_slice = slice_map.get(inbound_slice_id)
                if inbound_slice:
                    inbound_route = self._build_route(inbound_slice, airline_map, req.cabin_class or "M")
                    if inbound_route and inbound_route.stopovers > req.max_stopovers:
                        continue

            # Collect all airline codes in this offer
            all_airlines = set()
            for seg in outbound_route.segments:
                all_airlines.add(seg.airline)
            if inbound_route:
                for seg in inbound_route.segments:
                    all_airlines.add(seg.airline)

            # Determine owner airline (marketing carrier of first segment)
            owner = outbound_route.segments[0].airline if outbound_route.segments else ""

            # Deduplicate: same slice + same brand = same offer
            dedup_key = f"{outbound_slice_id}_{inbound_slice_id}_{brand_name}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            offer_hash = hashlib.md5(
                f"hopper_{fare_id}".encode()
            ).hexdigest()[:12]

            # Build booking URL
            booking_url = self._build_booking_url(req)

            offers.append(FlightOffer(
                id=f"hp_{offer_hash}",
                price=round(total, 2),
                currency=currency,
                price_formatted=f"{total:.2f} {currency}",
                outbound=outbound_route,
                inbound=inbound_route,
                airlines=sorted(all_airlines),
                owner_airline=owner,
                booking_url=booking_url,
                is_locked=False,
                source="hopper_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _build_airline_map(airlines: list[dict]) -> dict[str, str]:
        """Map airline IATA code to display name."""
        m = {}
        for a in airlines:
            code = a.get("code", {}).get("value", "")
            name = a.get("displayName", a.get("fullName", code))
            if code:
                m[code] = name
        return m

    @staticmethod
    def _build_slice_map(slices: list[dict]) -> dict[str, dict]:
        """Map slice ID to slice data."""
        return {s["id"]: s for s in slices if "id" in s}

    def _build_route(self, sl: dict, airline_map: dict, cabin_code: str = "M") -> Optional[FlightRoute]:
        """Build a FlightRoute from a Hopper slice."""
        _hp_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(cabin_code, "economy")
        segments = []
        for seg in sl.get("segments", []):
            airline_code = seg.get("marketingAirline", {}).get("value", "")
            op_airline_code = seg.get("operatingAirline", {}).get("value", airline_code)
            dep_str = seg.get("departure", "")
            arr_str = seg.get("arrival", "")

            segments.append(FlightSegment(
                airline=op_airline_code or airline_code,
                airline_name=airline_map.get(op_airline_code, airline_map.get(airline_code, "")),
                flight_no=f"{airline_code}{seg.get('flightNumber', '')}",
                origin=seg.get("originAirportCode", {}).get("value", ""),
                destination=seg.get("destinationAirportCode", {}).get("value", ""),
                departure=self._parse_dt(dep_str),
                arrival=self._parse_dt(arr_str),
                cabin_class=_hp_cabin,
            ))

        if not segments:
            return None

        total_dur = sl.get("totalDurationMinutes", 0) * 60
        if total_dur <= 0 and len(segments) >= 1:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        return FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(sl.get("stops", len(segments) - 1), 0),
        )

    @staticmethod
    def _parse_dt(s: str) -> datetime:
        """Parse Hopper datetime string (e.g. '2026-04-20T13:00')."""
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s)
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        """Build a Hopper booking URL."""
        trip_type = "round-trip" if req.return_from else "one-way"
        url = (
            f"https://hopper.com/air/shop?"
            f"tripType={trip_type}"
            f"&departureDate={req.date_from.isoformat()}"
            f"&origin={req.origin}&originType=Airport"
            f"&destination={req.destination}&destinationType=Airport"
            f"&adults={req.adults}"
            f"&step=outbound"
        )
        if req.return_from:
            url += f"&returnDate={req.return_from.isoformat()}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"hopper{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req,
    ) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_hopp_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        return {
            "checked_bag_note": "baggage depends on airline and fare – may be included or add-on from ~30 USD; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        pass  # conditions + bags_price set per-offer by engine's apply_ref_ancillaries via owner_airline
