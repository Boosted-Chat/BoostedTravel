"""
flyadeal (F3) -- EveryMundo sputnik fare search API connector.

flyadeal (IATA: F3) is a Saudi low-cost carrier, subsidiary of Saudia.
Hub at Jeddah (JED) and Riyadh (RUH) with routes across the Middle East,
South Asia, Turkey, Egypt, and East Africa.

Strategy (direct API — no browser required):
  1. POST to airTRFX sputnik fare search with EM-API-Key header
  2. Parse fare response → FlightOffer objects
  3. Construct booking URL for flyadeal.com
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date, datetime, timedelta

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_SPUTNIK_URL = (
    "https://openair-california.airtrfx.com"
    "/airfare-sputnik-service/v3/f3/fares/search"
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
    "Origin": "https://www.flyadeal.com",
    "Referer": "https://www.flyadeal.com/",
}
_HOME_URL = "https://www.flyadeal.com/en"


class FlyadealConnectorClient:
    """flyadeal (F3) — EveryMundo sputnik fare search API."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

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
                "max": days_from_now + 14,
            },
            "journeyType": "ROUND_TRIP" if is_rt else "ONE_WAY",
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
            and o.outbound.segments[0].departure.date() == dt
        ]
        offers.sort(key=lambda o: o.price)

        elapsed = time.monotonic() - t0
        logger.info(
            "flyadeal %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"f3{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
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
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome") as s:
                r = await s.post(_SPUTNIK_URL, json=payload, headers=_HEADERS, timeout=self.timeout)
            if r.status_code != 200:
                logger.warning(
                    "flyadeal sputnik: %d %s",
                    r.status_code, r.text[:200],
                )
                return []
            data = r.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error("flyadeal sputnik error: %s", e)
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
            airline="F3",
            airline_name="flyadeal",
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

        # Parse inbound (return) flight if present in the fare
        inbound = None
        ret_flight = fare.get("returnFlight") or fare.get("inboundFlight")
        ret_date_str = fare.get("returnDate", "")[:10] if fare.get("returnDate") else ""
        if ret_flight or ret_date_str:
            ret_origin = (ret_flight or {}).get("departureAirportIataCode") or req.destination
            ret_dest = (ret_flight or {}).get("arrivalAirportIataCode") or req.origin
            try:
                ret_dt = datetime.strptime(ret_date_str, "%Y-%m-%d") if ret_date_str else dep_dt
            except ValueError:
                ret_dt = dep_dt
            ret_seg = FlightSegment(
                airline="F3",
                airline_name="flyadeal",
                flight_no="",
                origin=ret_origin,
                destination=ret_dest,
                departure=ret_dt,
                arrival=ret_dt,
                duration_seconds=0,
                cabin_class=cabin_input.lower() if cabin_input else "economy",
            )
            inbound = FlightRoute(segments=[ret_seg], total_duration_seconds=0, stopovers=0)

        offer_hash = hashlib.md5(
            f"f3_{origin_code}_{dest_code}_{dep_date_str}_{ret_date_str}_{price_f}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"f3_{offer_hash}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=outbound,
            inbound=inbound,
            airlines=["flyadeal"],
            owner_airline="F3",
            booking_url=_HOME_URL,
            is_locked=False,
            source="flyadeal_direct",
            source_tier="free",
            conditions={
                "cabin": cabin_input or "Economy",
                "fare_note": "Fare from flyadeal EveryMundo module",
            },
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
                    id=f"rt_flya_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
