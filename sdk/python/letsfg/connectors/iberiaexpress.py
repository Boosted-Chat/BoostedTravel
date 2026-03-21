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

from connectors.iberia import (
    _get_cached_fares,
    _ORIGIN_TO_MARKET,
    _AIRPORT_TO_CITY,
)
from models.flights import (
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
        offer = self._build_offer(price_f, currency, dest_name, req)

        elapsed = time.monotonic() - t0
        logger.info("I2 %s→%s: %.2f %s in %.1fs", req.origin, req.destination, price_f, currency, elapsed)

        h = hashlib.md5(
            f"i2{req.origin}{req.destination}{req.date_from}".encode()
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
    ) -> FlightOffer:
        target_date = req.date_from.strftime("%Y-%m-%d")
        dep_dt = datetime.combine(req.date_from, datetime.min.time())

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
            cabin_class="economy",
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        fid = hashlib.md5(
            f"i2_{req.origin}{req.destination}{price}{currency}".encode()
        ).hexdigest()[:12]

        fmt_map = {"GBP": "£", "EUR": "€", "USD": "$"}
        sym = fmt_map.get(currency, currency)

        return FlightOffer(
            id=f"i2_{fid}",
            price=price,
            currency=currency,
            price_formatted=f"{sym}{price:.0f}",
            outbound=route,
            inbound=None,
            airlines=["Iberia Express"],
            owner_airline="I2",
            booking_url=(
                f"https://www.iberiaexpress.com/en/booking"
                f"?origin={req.origin}&destination={req.destination}"
                f"&outbound={target_date}"
                f"&adults={req.adults or 1}"
            ),
            is_locked=False,
            source="iberiaexpress_direct",
            source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"i2{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
