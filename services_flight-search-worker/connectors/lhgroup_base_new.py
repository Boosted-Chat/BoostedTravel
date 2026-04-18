"""
Shared base for Lufthansa Group connectors (LH, LX, OS, SN).

Calls the fare teaser API directly via curl_cffi with TLS impersonation.
No browser needed — fast (~0.5s) and reliable.

The fare teaser returns lowest indicative fares from an origin to ~27 popular
destinations. Coverage is limited to promoted routes; for comprehensive LH Group
coverage rely on OTA connectors (Kiwi, Skyscanner).

Strategy:
1. POST to {BASE_URL}/service/api/lhg-fare-teaser/fareteaser/offers/{ORIGIN}
   with X-Portal, X-Portal-Site, X-Portal-Language headers
2. Parse fareTeaserOffers list
3. Check if destination exists in the ~27 offered cities
4. Return indicative fare if found
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

from curl_cffi.requests import AsyncSession

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_curl_cffi_proxies

logger = logging.getLogger(__name__)

# Reverse: map destination 3-letter codes to city codes in fare teaser response
# The API returns city codes like "LON", "NYC", "PAR" for multi-airport cities
IATA_TO_CITY = {
    "LHR": "LON", "LGW": "LON", "LCY": "LON", "STN": "LON",
    "JFK": "NYC", "EWR": "NYC", "LGA": "NYC",
    "CDG": "PAR", "ORY": "PAR",
    "FCO": "ROM", "CIA": "ROM",
    "MXP": "MIL", "LIN": "MIL",
    "NRT": "TYO", "HND": "TYO",
    "ORD": "CHI", "MDW": "CHI",
    "IAD": "WAS", "DCA": "WAS",
    "GRU": "SAO", "CGH": "SAO",
    "EZE": "BUE", "AEP": "BUE",
    "SVO": "MOW", "DME": "MOW", "VKO": "MOW",
    "PEK": "BJS", "PKX": "BJS",
    "ICN": "SEL",
    "BOM": "BOM", "DEL": "DEL",
    "SFO": "SFO", "LAX": "LAX",
}


class LHGroupBaseConnector:
    """Base connector for Lufthansa Group airlines using fare teaser API.

    Subclasses set: AIRLINE_CODE, AIRLINE_NAME, SOURCE_KEY, DEFAULT_CURRENCY,
    PORTAL_CODE, BASE_URL, MARKET_CODE.
    """

    AIRLINE_CODE: str = "LH"
    AIRLINE_NAME: str = "Lufthansa"
    SOURCE_KEY: str = "lufthansa_direct"
    DEFAULT_CURRENCY: str = "EUR"
    PORTAL_CODE: str = "LH"
    BASE_URL: str = "https://www.lufthansa.com"
    MARKET_CODE: str = "DE"
    BOOKING_PATH: str = "book-and-manage/destination"

    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout

    async def close(self):
        pass  # No persistent resources

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        dest_city = IATA_TO_CITY.get(req.destination, req.destination)

        url = (
            f"{self.BASE_URL}/service/api/lhg-fare-teaser/"
            f"fareteaser/offers/{req.origin}"
        )

        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "x-portal": self.PORTAL_CODE,
            "x-portal-site": self.MARKET_CODE,
            "x-portal-language": "en",
            "x-portal-countryid": "",
            "x-portal-taxonomy": "",
            "referer": f"{self.BASE_URL}/{self.MARKET_CODE.lower()}/en/homepage",
            "origin": self.BASE_URL,
        }

        body = {
            "touchPointId": "200",
            "decisionTypeId": "50",
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            "timezone": 120,
            "tripType": "R",
            "airlineDestWhitelist": False,
        }

        try:
            async with AsyncSession(
                impersonate="chrome136", proxies=get_curl_cffi_proxies()
            ) as session:
                resp = await session.post(
                    url, headers=headers, json=body, timeout=self.timeout
                )

            if resp.status_code != 200:
                logger.warning(
                    "%s: Fare teaser %s returned %d",
                    self.AIRLINE_NAME, req.origin, resp.status_code,
                )
                return self._empty(req)

            data = resp.json()
            offers_list = data.get("fareTeaserOffers", [])
            currency = data.get("currency", self.DEFAULT_CURRENCY)

            logger.info(
                "%s: Fare teaser %s → %d destinations",
                self.AIRLINE_NAME, req.origin, len(offers_list),
            )

            # Find destination in offers
            target_offer = None
            for offer in offers_list:
                city = offer.get("city", "")
                if city == dest_city or city == req.destination:
                    target_offer = offer
                    break

            if not target_offer:
                avail = [o.get("city") for o in offers_list[:8]]
                logger.info(
                    "%s: %s not in fare teaser from %s (sample: %s)",
                    self.AIRLINE_NAME, req.destination, req.origin, avail,
                )
                return self._empty(req)

            price = float(target_offer.get("price", 0))
            if price <= 0:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            logger.info(
                "%s %s→%s: %.0f %s (fare teaser) in %.1fs",
                self.AIRLINE_NAME, req.origin, req.destination,
                price, currency, elapsed,
            )

            offer = self._build_offer(req, price, currency, target_offer)

            fid = hashlib.md5(
                f"{self.AIRLINE_CODE}{req.origin}{req.destination}"
                f"{req.date_from}{price}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"{self.AIRLINE_CODE.lower()}_{fid}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=[offer],
                total_results=1,
            )

        except Exception as e:
            logger.error("%s fare teaser error: %s", self.AIRLINE_NAME, e)
            return self._empty(req)

    def _build_offer(
        self,
        req: FlightSearchRequest,
        price: float,
        currency: str,
        teaser: dict,
    ) -> FlightOffer:
        dep_date = req.date_from
        dep_dt = (
            datetime.combine(dep_date, datetime.min.time())
            if not isinstance(dep_date, datetime)
            else dep_date
        )

        segment = FlightSegment(
            airline=self.AIRLINE_CODE,
            airline_name=self.AIRLINE_NAME,
            flight_no="",
            origin=req.origin,
            destination=req.destination,
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class="economy",
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=0,
            stopovers=0,
        )

        fid = hashlib.md5(
            f"{self.AIRLINE_CODE}_{req.origin}{req.destination}"
            f"{dep_date}{price}".encode()
        ).hexdigest()[:12]

        booking_url = (
            f"{self.BASE_URL}/{self.MARKET_CODE.lower()}/en/"
            f"{self.BOOKING_PATH}"
            f"?OriginCode={req.origin}&DestinationCode={req.destination}"
        )

        return FlightOffer(
            id=f"{self.AIRLINE_CODE.lower()}_{fid}",
            source=self.SOURCE_KEY,
            price=round(price, 2),
            currency=currency,
            booking_url=booking_url,
            airlines=[self.AIRLINE_CODE],
            outbound=route,
            inbound=None,
            available_seats=None,
            conditions={
                "note": "Indicative starting price from fare teaser",
            },
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"{self.AIRLINE_CODE}{req.origin}{req.destination}".encode()
        ).hexdigest()[:8]
        return FlightSearchResponse(
            search_id=f"{self.AIRLINE_CODE.lower()}_empty_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=self.DEFAULT_CURRENCY,
            offers=[],
            total_results=0,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Airline-specific subclasses
# ──────────────────────────────────────────────────────────────────────────────


class LufthansaDirectConnector(LHGroupBaseConnector):
    AIRLINE_CODE = "LH"
    AIRLINE_NAME = "Lufthansa"
    SOURCE_KEY = "lufthansa_direct"
    BASE_URL = "https://www.lufthansa.com"
    DEFAULT_CURRENCY = "EUR"
    PORTAL_CODE = "LH"
    MARKET_CODE = "DE"


class SwissDirectConnector(LHGroupBaseConnector):
    AIRLINE_CODE = "LX"
    AIRLINE_NAME = "SWISS"
    SOURCE_KEY = "swiss_direct"
    BASE_URL = "https://www.swiss.com"
    DEFAULT_CURRENCY = "CHF"
    PORTAL_CODE = "LX"
    MARKET_CODE = "CH"


class AustrianDirectConnector(LHGroupBaseConnector):
    AIRLINE_CODE = "OS"
    AIRLINE_NAME = "Austrian Airlines"
    SOURCE_KEY = "austrian_direct"
    BASE_URL = "https://www.austrian.com"
    DEFAULT_CURRENCY = "EUR"
    PORTAL_CODE = "OS"
    MARKET_CODE = "AT"


class BrusselsAirlinesDirectConnector(LHGroupBaseConnector):
    AIRLINE_CODE = "SN"
    AIRLINE_NAME = "Brussels Airlines"
    SOURCE_KEY = "brusselsairlines_direct"
    BASE_URL = "https://www.brusselsairlines.com"
    DEFAULT_CURRENCY = "EUR"
    PORTAL_CODE = "SN"
    MARKET_CODE = "BE"
    BOOKING_PATH = "book-a-flight/destination"
