"""
Gulf Air connector — EveryMundo airTRFX sputnik fare API.

Gulf Air (IATA: GF) is the national airline of Bahrain. Hub at Bahrain
International Airport (BAH) with 50+ destinations across the Middle East,
South Asia, Europe, and Africa.

Strategy (direct API — no browser required):
  1. POST to airTRFX sputnik fare search with EM-API-Key header
  2. Parse fare response → FlightOffer objects
  3. Construct booking URL for gulfair.com
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
    "/airfare-sputnik-service/v3/gf/fares/search"
)
_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_HEADERS = {
    "EM-API-Key": _API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.gulfair.com",
    "Referer": "https://www.gulfair.com/",
}


class GulfAirConnectorClient:
    """Gulf Air (GF) — EveryMundo sputnik fare API."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination, "destination": req.origin,
                "date_from": req.return_from, "return_from": None,
            })
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
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

        is_rt = bool(req.return_from)
        payload = {
            "origins": [req.origin],
            "destinations": [req.destination],
            "departureDaysInterval": {
                "min": max(0, days_from_now - 7),
                "max": days_from_now + 7,
            },
            "journeyType": "ROUND_TRIP" if is_rt else "ONE_WAY",
        }

        fares = await self._call_sputnik(payload)
        offers = [
            o for o in (self._build_offer(f, req) for f in fares) if o is not None
        ]
        _td = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
        offers = [o for o in offers if o.outbound and o.outbound.segments and o.outbound.segments[0].departure.date() == _td]
        offers.sort(key=lambda o: o.price)

        elapsed = time.monotonic() - t0
        logger.info(
            "GulfAir %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"gulfair{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
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
                timeout=self.timeout, proxy=get_httpx_proxy_url(),
            ) as client:
                resp = await client.post(_SPUTNIK_URL, headers=_HEADERS, json=payload)

            if resp.status_code != 200:
                logger.warning("GulfAir sputnik returned %d", resp.status_code)
                return []

            data = resp.json()
            fares = data.get("fares", [])
            if not isinstance(fares, list):
                fares = []
            return fares
        except (httpx.HTTPError, Exception) as exc:
            logger.error("GulfAir sputnik error: %s", exc)
            return []

    def _build_offer(
        self, fare: dict, req: FlightSearchRequest
    ) -> FlightOffer | None:
        if not isinstance(fare, dict):
            return None

        price = fare.get("price") or fare.get("totalPrice") or fare.get("amount")
        if isinstance(price, dict):
            price = price.get("amount") or price.get("value")
        if not price:
            return None
        try:
            price_f = round(float(price), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        currency = fare.get("currency", fare.get("currencyCode", "USD"))
        dep_date = fare.get("departureDate", "")[:10]
        if not dep_date:
            return None

        origin = fare.get("origin", fare.get("originAirportCode", req.origin))
        dest = fare.get("destination", fare.get("destinationAirportCode", req.destination))

        try:
            dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            dep_dt = datetime(2000, 1, 1)

        # Map cabin code to cabin name for segment
        _gf_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        seg = FlightSegment(
            airline="GF",
            airline_name="Gulf Air",
            flight_no="",
            origin=origin,
            destination=dest,
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=_gf_cabin,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        fid = hashlib.md5(
            f"gf_{origin}{dest}{dep_date}{price_f}".encode()
        ).hexdigest()[:12]

        # Gulf Air cabin codes: Y=Economy, C=Business, F=First
        _gf_cabin_url = {"M": "Y", "W": "Y", "C": "C", "F": "F"}.get(req.cabin_class or "M", "Y")
        return FlightOffer(
            id=f"gf_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Gulf Air"],
            owner_airline="GF",
            booking_url=(
                f"https://booking.gulfair.com/GF/dyn/air/booking/availability?"
                f"from={req.origin}&to={req.destination}&outboundDate={dep_date}"
                f"&ADT={req.adults or 1}&tripType={'RT' if req.return_from else 'OW'}"
                f"&cabinClass={_gf_cabin_url}"
            ),
            is_locked=False,
            source="gulfair_direct",
            source_tier="free",
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
                    id=f"rt_gulf_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"gulfair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
