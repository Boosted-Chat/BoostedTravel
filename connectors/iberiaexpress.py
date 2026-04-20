"""
Iberia Express connector — reuses Iberia's LD+JSON fare data.

Iberia Express (IATA: I2) is a low-cost subsidiary of Iberia.
Hub at Madrid-Barajas (MAD), operates Spanish domestic + short-haul EU routes.
All I2 flights are sold through iberia.com under the Iberia brand.

Strategy:
  - Reuse iberia.py's fare cache (same LD+JSON data from iberia.com)
  - Filter to I2-operated routes (MAD hub, domestic Spain + EU short-haul)
  - Brand as Iberia Express with I2 codes
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

from .iberia import (
    _get_cached_fares,
    _ORIGIN_TO_MARKET,
    _AIRPORT_TO_CITY,
)
from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# I2 operates from MAD to these destinations (domestic Spain + short-haul EU)
_I2_DESTINATIONS = {
    # Canary Islands
    "TCI", "ACE", "LPA", "FUE", "SPC", "TFS",
    # Balearic Islands
    "PMI", "IBZ", "MAH",
    # Mainland Spain
    "AGP", "ALC", "BCN", "BIO", "GRX", "LCG", "OVD", "PMI",
    "SCQ", "SDR", "SVQ", "VGO", "VLC", "XRY", "LEI", "EAS",
    "BJZ", "PNA", "CDT", "LEN",
    # EU short-haul (I2 operates some of these)
    "ATH", "BER", "DUB", "MIL", "ROM", "LIS", "OPO", "NAP",
    "BLQ", "FLR", "VCE", "CPH", "PRG", "BUD",
    # UK (I2 flies MAD→LGW)
    "LON", "LHR", "LGW", "STN",
    # Also serve MAD as destination from other origins
    "MAD",
}

# I2 departs from MAD primarily, and BCN on some routes
_I2_ORIGINS = {"MAD", "BCN"}


class IberiaExpressConnectorClient:
    """Iberia Express — fare data from iberia.com via shared cache."""

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

        # I2 only operates from MAD/BCN
        if req.origin not in _I2_ORIGINS:
            return self._empty(req)

        market = _ORIGIN_TO_MARKET.get(req.origin, "es")

        try:
            fares = await asyncio.get_event_loop().run_in_executor(
                None, _get_cached_fares, market
            )
        except Exception as e:
            logger.error("I2 fare load error: %s", e)
            return self._empty(req)

        # Try exact IATA match, then city code
        fare = fares.get(req.destination)
        if not fare:
            city_code = _AIRPORT_TO_CITY.get(req.destination)
            if city_code:
                fare = fares.get(city_code)

        if not fare:
            return self._empty(req)

        # Only return fares for known I2 destinations
        dest_check = req.destination
        city_code = _AIRPORT_TO_CITY.get(req.destination, req.destination)
        if dest_check not in _I2_DESTINATIONS and city_code not in _I2_DESTINATIONS:
            return self._empty(req)

        price_f, currency, dest_name = fare

        # RT: try reverse fare from iberia cache
        _ib_price = 0.0
        _ib_fare = None
        if req.return_from and req.destination in _I2_ORIGINS:
            ib_market = _ORIGIN_TO_MARKET.get(req.destination, "es")
            try:
                ib_fares = await asyncio.get_event_loop().run_in_executor(
                    None, _get_cached_fares, ib_market
                )
                _ib_fare = ib_fares.get(req.origin)
                if not _ib_fare:
                    ib_city = _AIRPORT_TO_CITY.get(req.origin)
                    if ib_city:
                        _ib_fare = ib_fares.get(ib_city)
            except Exception:
                pass
            if _ib_fare:
                _ib_price = _ib_fare[0]

        offer = self._build_offer(price_f, currency, dest_name, req, ib_price=_ib_price, ib_fare=_ib_fare)

        elapsed = time.monotonic() - t0
        logger.info("I2 %s→%s: %.2f %s in %.1fs", req.origin, req.destination, price_f, currency, elapsed)

        h = hashlib.md5(
            f"i2{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=[offer],
            total_results=1,
        )

    def _build_offer(
        self,
        price: float,
        currency: str,
        dest_name: str,
        req: FlightSearchRequest,
        *,
        ib_price: float = 0.0,
        ib_fare: tuple | None = None,
    ) -> FlightOffer:
        target_date = req.date_from.strftime("%Y-%m-%d")
        dep_dt = datetime.combine(req.date_from, datetime.min.time())

        _i2_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        seg = FlightSegment(
            airline="I2",
            airline_name="Iberia Express",
            flight_no="",
            origin=req.origin,
            destination=req.destination,
            origin_city="",
            destination_city=dest_name,
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=_i2_cabin,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        # RT: build inbound route
        _ib_route = None
        if ib_price > 0 and req.return_from:
            ib_dt = datetime.combine(req.return_from, datetime.min.time())
            ib_seg = FlightSegment(
                airline="I2", airline_name="Iberia Express", flight_no="",
                origin=req.destination, destination=req.origin,
                departure=ib_dt, arrival=ib_dt,
                duration_seconds=0, cabin_class=_i2_cabin,
            )
            _ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)

        total_price = round(price + ib_price, 2) if _ib_route else price
        id_prefix = "i2_rt_" if _ib_route else "i2_"

        fid = hashlib.md5(
            f"i2_{req.origin}{req.destination}{total_price}{currency}{req.return_from or ''}".encode()
        ).hexdigest()[:12]

        fmt_map = {"GBP": "\u00a3", "EUR": "\u20ac", "USD": "$"}
        sym = fmt_map.get(currency, currency)

        bk_url = (
            f"https://www.iberiaexpress.com/en/booking"
            f"?origin={req.origin}&destination={req.destination}"
            f"&outbound={target_date}"
            f"&adults={req.adults or 1}"
        )
        if _ib_route and req.return_from:
            bk_url += f"&inbound={req.return_from.strftime('%Y-%m-%d')}"

        return FlightOffer(
            id=f"{id_prefix}{fid}",
            price=total_price,
            currency=currency,
            price_formatted=f"{sym}{total_price:.0f}",
            outbound=route,
            inbound=_ib_route,
            airlines=["Iberia Express"],
            owner_airline="I2",
            booking_url=bk_url,
            is_locked=False,
            source="iberiaexpress_direct",
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
                    id=f"rt_iber_{cid}", price=price, currency=o.currency,
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
            f"i2{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
