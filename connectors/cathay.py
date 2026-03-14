"""
Cathay Pacific open-search API scraper — direct curl_cffi, no auth needed.

Cathay Pacific (IATA: CX) is a Hong Kong-based full-service airline.
open-search API: book.cathaypacific.com — calendar pricing, no cookies/tokens.

Strategy (Mar 2026):
1. GET open-search endpoint with curl_cffi (impersonate Chrome)
2. Returns cheapest one-way fares per destination from a given origin
3. Filter for requested destination/date
4. Parse JSON → FlightOffer objects

API details:
- GET https://book.cathaypacific.com/CathayPacificV3/dyn/air/api/instant/open-search
- Params: ORIGIN=HKG&LANGUAGE=GB&CABIN=Y&SITE=CBEUCBEU&TRIP_TYPE=O
- No auth/cookies needed — publicly accessible calendar pricing
- Response: array of {date_departure, base_fare, total_fare, currency, tax,
    outbound_cabin, origin, destination, tax_inclusive}
- Currency varies by origin: HKG→HKD, SIN→SGD, SYD→AUD, TPE→TWD, BKK→THB
- Supported origins (CBEUCBEU site): HKG, SIN, SYD, TPE, BKK, and many Asia-Pacific
- NRT, LHR unsupported with this SITE code (400 Bad Request)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

from curl_cffi import requests as creq

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_API_URL = "https://book.cathaypacific.com/CathayPacificV3/dyn/air/api/instant/open-search"

# Airports that should be treated as equivalent for multi-airport cities
_CITY_AIRPORTS: dict[str, set[str]] = {
    "TYO": {"NRT", "HND"},
    "NRT": {"NRT", "HND"},
    "HND": {"NRT", "HND"},
    "LON": {"LHR", "LGW", "STN", "LTN", "SEN"},
    "LHR": {"LHR", "LGW", "STN", "LTN", "SEN"},
    "BJS": {"PEK", "PKX"},
    "PEK": {"PEK", "PKX"},
    "SEL": {"ICN", "GMP"},
    "ICN": {"ICN", "GMP"},
    "SHA": {"PVG", "SHA"},
    "PVG": {"PVG", "SHA"},
    "OSA": {"KIX", "ITM"},
    "KIX": {"KIX", "ITM"},
    "BKK": {"BKK", "DMK"},
    "JKT": {"CGK", "HLP"},
    "CGK": {"CGK", "HLP"},
    "CTU": {"CTU", "TFU"},
    "TFU": {"CTU", "TFU"},
}


class CathayConnectorClient:
    """Cathay Pacific scraper — direct open-search API via curl_cffi."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            offers = await asyncio.get_event_loop().run_in_executor(
                None, self._api_search_sync, req,
            )
            if offers is not None:
                elapsed = time.monotonic() - t0
                return self._build_response(offers, req, elapsed)
        except Exception as e:
            logger.warning("Cathay: API search failed: %s", e)

        return self._empty(req)

    def _api_search_sync(self, req: FlightSearchRequest) -> list[FlightOffer] | None:
        params = {
            "ORIGIN": req.origin,
            "LANGUAGE": "GB",
            "CABIN": "Y",
            "SITE": "CBEUCBEU",
            "TRIP_TYPE": "O",
        }

        logger.info("Cathay: API %s→%s on %s", req.origin, req.destination,
                     req.date_from.strftime("%Y-%m-%d"))

        sess = creq.Session(impersonate="chrome131")
        try:
            r = sess.get(
                _API_URL,
                params=params,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.cathaypacific.com/",
                },
                timeout=15,
            )
        except Exception as e:
            logger.warning("Cathay: API request failed: %s", e)
            return None

        if r.status_code == 400:
            logger.info("Cathay: origin %s not supported (400)", req.origin)
            return []

        if r.status_code != 200:
            logger.warning("Cathay: API returned %d", r.status_code)
            return None

        try:
            data = r.json()
        except Exception:
            # Response may be JSON wrapped in HTML
            import re as _re
            text = r.text.strip()
            # Strip HTML wrapping if present
            if text.startswith("<"):
                match = _re.search(r'\[.*\]', text, _re.DOTALL)
                if match:
                    import json
                    data = json.loads(match.group())
                else:
                    logger.warning("Cathay: could not parse response")
                    return None
            else:
                logger.warning("Cathay: invalid JSON response")
                return None

        if not isinstance(data, list):
            return None

        return self._parse_offers(data, req)

    def _parse_offers(self, data: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        target_dest = req.destination.upper()
        target_date = req.date_from.strftime("%Y%m%d")

        # Build set of matching destination codes (handle multi-airport cities)
        dest_codes = _CITY_AIRPORTS.get(target_dest, {target_dest})

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for entry in data:
            dest = entry.get("destination", "")
            if dest not in dest_codes:
                continue

            dep_date = entry.get("date_departure", "")
            total_fare = entry.get("total_fare")
            currency = entry.get("currency", "HKD")

            if total_fare is None or total_fare <= 0:
                continue

            # Parse departure date
            try:
                dep_dt = datetime.strptime(dep_date, "%Y%m%d")
            except (ValueError, TypeError):
                continue

            # Build a calendar-pricing offer (no flight-level detail)
            seg = FlightSegment(
                airline="CX",
                airline_name="Cathay Pacific",
                flight_no="",
                origin=req.origin,
                destination=dest,
                departure=dep_dt,
                arrival=dep_dt,  # no arrival time in calendar data
                cabin_class="M",
            )

            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=0,
                stopovers=0,
            )

            offer_id = hashlib.md5(
                f"cx_{req.origin}_{dest}_{dep_date}_{total_fare}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"cx_{offer_id}",
                price=round(float(total_fare), 2),
                currency=currency,
                price_formatted=f"{total_fare:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Cathay Pacific"],
                owner_airline="CX",
                booking_url=booking_url,
                is_locked=False,
                source="cathay_direct",
                source_tier="free",
            ))

        return offers

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float,
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Cathay %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(
            f"cathay{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y%m%d")
        return (
            f"https://www.cathaypacific.com/cx/en_HK.html"
            f"?origin={req.origin}&destination={req.destination}"
            f"&date={dep}"
        )
