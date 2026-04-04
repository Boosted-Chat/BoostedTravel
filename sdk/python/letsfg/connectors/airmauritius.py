"""
Air Mauritius (MK) -- EveryMundo sputnik fare search API connector.

Air Mauritius (IATA: MK) is the flag carrier of Mauritius.
Hub at Mauritius Sir Seewoosagur Ramgoolam (MRU) with routes to
Europe, India, South Africa, Southeast Asia, and Australia.

Strategy (direct API — no browser required):
  1. POST to airTRFX sputnik fare search with EM-API-Key header
  2. Parse fare response → FlightOffer objects
  3. Construct booking URL for airmauritius.com
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
    "/airfare-sputnik-service/v3/mk/fares/search"
)
_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_HEADERS = {
    "EM-API-Key": _API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.airmauritius.com",
    "Referer": "https://www.airmauritius.com/",
}
_HOME_URL = "https://www.airmauritius.com/"


class AirmauritiusConnectorClient:
    """Air Mauritius (MK) — EveryMundo sputnik fare search API."""

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
                "min": max(0, days_from_now - 7),
                "max": days_from_now + 14,
            },
            "journeyType": "ONE_WAY",
        }

        fares = await self._call_sputnik(payload)
        offers = [
            o for o in (self._build_offer(f, req) for f in fares) if o is not None
        ]
        offers = [
            o
            for o in offers
            if o.outbound
            and o.outbound.segments
            and abs((o.outbound.segments[0].departure.date() - dt).days) <= 7
        ]
        offers.sort(key=lambda o: o.price)

        elapsed = time.monotonic() - t0
        logger.info(
            "Air Mauritius %s→%s: %d offers in %.1fs",
            req.origin,
            req.destination,
            len(offers),
            elapsed,
        )

        h = hashlib.md5(
            f"mk{req.origin}{req.destination}{req.date_from}".encode()
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
                proxy=get_httpx_proxy_url(),
            ) as client:
                r = await client.post(_SPUTNIK_URL, json=payload)
                if r.status_code != 200:
                    logger.warning(
                        "Air Mauritius sputnik: %d %s",
                        r.status_code,
                        r.text[:200],
                    )
                    return []
                data = r.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            logger.error("Air Mauritius sputnik error: %s", e)
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

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            return None

        segment = FlightSegment(
            airline="MK",
            airline_name="Air Mauritius",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city="",
            destination_city="",
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin_input.lower() if cabin_input else "economy",
        )
        outbound = FlightRoute(
            segments=[segment], total_duration_seconds=0, stopovers=0
        )

        offer_hash = hashlib.md5(
            f"mk_{origin_code}_{dest_code}_{dep_date_str}_{price_f}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"mk_{offer_hash}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=outbound,
            inbound=None,
            airlines=["Air Mauritius"],
            owner_airline="MK",
            booking_url=_HOME_URL,
            is_locked=False,
            source="airmauritius_direct",
            source_tier="free",
            conditions={
                "cabin": cabin_input or "Economy",
                "fare_note": "Fare from Air Mauritius EveryMundo module",
            },
        )
