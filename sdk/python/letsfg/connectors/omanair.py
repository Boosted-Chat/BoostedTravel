"""
Oman Air connector — EveryMundo airTRFX sputnik fare API.

Oman Air (IATA: WY) is the national airline of Oman. Hub at Muscat (MCT)
with 50+ destinations across the Middle East, Asia, Africa, and Europe.

Strategy (direct API — no browser required):
  1. POST to airTRFX sputnik fare search with EM-API-Key header
  2. Parse fare response → FlightOffer objects
  3. Construct booking URL for omanair.com
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import date, datetime, timedelta

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

_SPUTNIK_URL = (
    "https://openair-california.airtrfx.com"
    "/airfare-sputnik-service/v3/wy/fares/search"
)
_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_HEADERS = {
    "EM-API-Key": _API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.omanair.com",
    "Referer": "https://www.omanair.com/",
}


class OmanairConnectorClient:
    """Oman Air (WY) — EveryMundo sputnik fare API."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            dt = (
                req.date_from
                if isinstance(req.date_from, (datetime, date))
                else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            )
            if isinstance(dt, datetime):
                dt = dt.date()
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

        fares = await self._call_sputnik(payload)
        offers = [
            o for o in (self._build_offer(f, req) for f in fares) if o is not None
        ]
        # Filter: only keep offers within ±1 day of the requested date
        offers = [
            o for o in offers
            if o.outbound and o.outbound.segments
            and abs((o.outbound.segments[0].departure.date() - dt).days) <= 1
        ]
        offers.sort(key=lambda o: o.price)

        elapsed = time.monotonic() - t0
        logger.info(
            "OmanAir %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"omanair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    async def _call_sputnik(self, payload: dict) -> list[dict]:
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS,
                proxy=get_httpx_proxy_url(),) as client:
                r = await client.post(_SPUTNIK_URL, json=payload)
                if r.status_code != 200:
                    logger.warning("OmanAir sputnik: %d %s", r.status_code, r.text[:200])
                    return []
                data = r.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            logger.error("OmanAir sputnik error: %s", e)
            return []

    def _build_offer(
        self, fare: dict, req: FlightSearchRequest
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

        if ps.get("usdTotalPrice"):
            currency = "USD"
        else:
            currency = ps.get("currencyCode") or "USD"

        dep_date_str = fare.get("departureDate", "")[:10]
        if not dep_date_str:
            return None

        origin_code = ob.get("departureAirportIataCode") or req.origin
        dest_code = ob.get("arrivalAirportIataCode") or req.destination
        cabin_input = ob.get("fareClassInput") or ob.get("fareClass") or "Economy"
        cabin = cabin_input.split()[0].lower() if cabin_input else "economy"

        origin_city = (ob.get("origin", {}).get("city", {}).get("name", ""))
        dest_city = (ob.get("destination", {}).get("city", {}).get("name", ""))

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            dep_dt = datetime(2000, 1, 1)

        seg = FlightSegment(
            airline="WY",
            airline_name="Oman Air",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city=origin_city,
            destination_city=dest_city,
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        dedup = f"wy_{origin_code}{dest_code}{dep_date_str}{price_f}{cabin}"
        fid = hashlib.md5(dedup.encode()).hexdigest()[:12]

        try:
            date_str = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
        except Exception:
            date_str = dep_date_str

        return FlightOffer(
            id=f"wy_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Oman Air"],
            owner_airline="WY",
            booking_url=(
                f"https://www.omanair.com/flights/en/"
                f"?from={req.origin}&to={req.destination}"
                f"&departDate={date_str}"
                f"&tripType=OW&adults={req.adults or 1}"
            ),
            is_locked=False,
            source="omanair_direct",
            source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"omanair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
