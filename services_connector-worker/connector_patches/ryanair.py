"""
Ryanair direct API scraper — queries Ryanair's public REST API.

PATCH: Uses farfnd/v4 API instead of booking/v4 (which returns 409).
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

import httpx

from letsfg.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .airline_routes import get_city_airports
from .browser import get_httpx_proxy_url

logger = logging.getLogger(__name__)

RYANAIR_API = "https://www.ryanair.com/api"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


class RyanairConnectorClient:
    """Direct scraper for Ryanair's public API — zero auth, real-time prices."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout,
                headers=_HEADERS,
                follow_redirects=True,
                proxy=get_httpx_proxy_url(),
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return await self._search_ow(req)

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Fan out to all origin/destination airports if city codes."""
        origins = get_city_airports(req.origin)
        destinations = get_city_airports(req.destination)

        if len(origins) > 1 or len(destinations) > 1:
            import asyncio as _aio
            tasks = []
            for o in origins:
                for d in destinations:
                    if o == d:
                        continue
                    sub_req = FlightSearchRequest(
                        origin=o,
                        destination=d,
                        date_from=req.date_from,
                        return_from=req.return_from,
                        adults=req.adults,
                        children=req.children,
                        infants=req.infants,
                        cabin_class=req.cabin_class,
                        currency=req.currency,
                        max_stopovers=req.max_stopovers,
                    )
                    tasks.append(self._search_single(sub_req))
            results = await _aio.gather(*tasks, return_exceptions=True)
            all_offers = []
            for r in results:
                if isinstance(r, FlightSearchResponse):
                    all_offers.extend(r.offers)
            all_offers.sort(key=lambda o: o.price)
            search_hash = hashlib.md5(
                f"ryanair{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=req.currency,
                offers=all_offers,
                total_results=len(all_offers),
            )
        return await self._search_single(req)

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search via farfnd/v4 API (booking/v4 returns 409)."""
        client = await self._client()
        date_str = req.date_from.isoformat()

        t0 = time.monotonic()

        if req.return_from:
            params = {
                "departureAirportIataCode": req.origin,
                "arrivalAirportIataCode": req.destination,
                "outboundDepartureDateFrom": date_str,
                "outboundDepartureDateTo": date_str,
                "inboundDepartureDateFrom": req.return_from.isoformat(),
                "inboundDepartureDateTo": req.return_from.isoformat(),
                "currency": req.currency,
                "adultPaxCount": req.adults,
            }
            url = f"{RYANAIR_API}/farfnd/v4/roundTripFares"
        else:
            params = {
                "departureAirportIataCode": req.origin,
                "arrivalAirportIataCode": req.destination,
                "outboundDepartureDateFrom": date_str,
                "outboundDepartureDateTo": date_str,
                "currency": req.currency,
                "adultPaxCount": req.adults,
            }
            url = f"{RYANAIR_API}/farfnd/v4/oneWayFares"

        try:
            resp = await client.get(url, params=params)
        except httpx.TimeoutException:
            logger.warning("Ryanair farfnd timed out (%s→%s)", req.origin, req.destination)
            return self._empty(req)
        except Exception as e:
            logger.error("Ryanair farfnd error (%s→%s): %s", req.origin, req.destination, e)
            return self._empty(req)

        elapsed = time.monotonic() - t0

        if resp.status_code != 200:
            logger.warning(
                "Ryanair farfnd %s→%s returned %d: %s",
                req.origin, req.destination,
                resp.status_code,
                resp.text[:300],
            )
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("Ryanair farfnd returned non-JSON response")
            return self._empty(req)

        offers = []
        currency = req.currency

        for fare_entry in data.get("fares", []):
            ob_leg = fare_entry.get("outbound")
            if not ob_leg:
                continue

            ob_price_obj = ob_leg.get("price", {})
            ob_price = float(ob_price_obj.get("value", 0))
            currency = ob_price_obj.get("currencyCode", currency)

            ob_route = self._parse_farfnd_leg(ob_leg)
            if not ob_route:
                continue

            if req.return_from:
                ib_leg = fare_entry.get("inbound")
                if not ib_leg:
                    continue
                ib_price_obj = ib_leg.get("price", {})
                ib_price = float(ib_price_obj.get("value", 0))
                ib_route = self._parse_farfnd_leg(ib_leg)
                if not ib_route:
                    continue

                total_price = round(ob_price + ib_price, 2)
                ob_key = ob_leg.get("flightKey", "")
                ib_key = ib_leg.get("flightKey", "")
                offer_id = f"ry_{hashlib.md5((ob_key + ib_key).encode()).hexdigest()[:12]}"

                offers.append(FlightOffer(
                    id=offer_id,
                    price=total_price,
                    currency=currency,
                    price_formatted=f"{total_price:.2f} {currency}",
                    outbound=ob_route,
                    inbound=ib_route,
                    airlines=["Ryanair"],
                    owner_airline="FR",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="ryanair_direct",
                    source_tier="free",
                ))
            else:
                ob_key = ob_leg.get("flightKey", "")
                offer_id = f"ry_{hashlib.md5(ob_key.encode()).hexdigest()[:12]}"

                offers.append(FlightOffer(
                    id=offer_id,
                    price=round(ob_price, 2),
                    currency=currency,
                    price_formatted=f"{ob_price:.2f} {currency}",
                    outbound=ob_route,
                    inbound=None,
                    airlines=["Ryanair"],
                    owner_airline="FR",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="ryanair_direct",
                    source_tier="free",
                ))

        offers.sort(key=lambda o: o.price)

        logger.info(
            "Ryanair farfnd %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"ryanair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=offers,
            total_results=len(offers),
        )

    def _parse_farfnd_leg(self, leg: dict) -> Optional[FlightRoute]:
        """Parse a farfnd leg into a FlightRoute."""
        dep_airport = leg.get("departureAirport", {})
        arr_airport = leg.get("arrivalAirport", {})
        dep_str = leg.get("departureDate", "")
        arr_str = leg.get("arrivalDate", "")
        flight_no = leg.get("flightNumber", "")

        dep_dt = self._parse_dt(dep_str)
        arr_dt = self._parse_dt(arr_str)

        if dep_dt.year == 2000 and arr_dt.year == 2000:
            return None

        segment = FlightSegment(
            airline="FR",
            airline_name="Ryanair",
            flight_no=flight_no,
            origin=dep_airport.get("iataCode", ""),
            destination=arr_airport.get("iataCode", ""),
            departure=dep_dt,
            arrival=arr_dt,
            cabin_class="M",
        )

        total_dur = max(int((arr_dt - dep_dt).total_seconds()), 0)

        return FlightRoute(
            segments=[segment],
            total_duration_seconds=total_dur,
            stopovers=0,
        )

    def _parse_dt(self, s: str) -> datetime:
        """Parse Ryanair datetime string."""
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        """Build a direct Ryanair booking URL."""
        date_out = req.date_from.isoformat()
        date_in = req.return_from.isoformat() if req.return_from else ""
        is_return = "true" if req.return_from else "false"
        return (
            f"https://www.ryanair.com/gb/en/trip/flights/select"
            f"?adults={req.adults}&teens=0&children={req.children}"
            f"&infants={req.infants}&dateOut={date_out}&dateIn={date_in}"
            f"&isConnectedFlight=false&discount=0&isReturn={is_return}"
            f"&promoCode=&originIata={req.origin}&destinationIata={req.destination}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"ryanair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
