"""
British Airways connector — SOLR pricing feed via curl_cffi.

British Airways (IATA: BA) is the flag carrier of the United Kingdom.
Main hubs at London Heathrow (LHR), London Gatwick (LGW), and London City (LCY).
Member of oneworld alliance, part of International Airlines Group (IAG).

Strategy (curl_cffi required — Akamai WAF bypass):
  1. Query BA's SOLR pricing endpoint: /solr/lpbd/safe
  2. SOLR contains lowest-fare data grouped by month for each route
  3. Filter results by departure date
  4. Return pricing data with route/airport details

SOLR config discovered from clientlib-site.min.js:
  {buildUrl: "/solr/", core: "lpbd", handler: "safe", rows: 12}
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import date, datetime, timezone
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

_BASE = "https://www.britishairways.com"
_SOLR_PATH = "/solr/lpbd/safe"
_HEADERS = {
    "Accept": "application/json, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Content-Type": "application/json",
    "Referer": "https://www.britishairways.com/en-gb/offers",
}

# BA SOLR uses city codes (not IATA airport codes) for departure/arrival.
# Map common UK departure IATA codes to BA city codes.
_IATA_TO_BA_CITY: dict[str, str] = {
    # UK departures (BA's primary market)
    "LHR": "LON", "LGW": "LON", "LCY": "LON", "STN": "LON", "LTN": "LON",
    "MAN": "MAN", "EDI": "EDI", "GLA": "GLA", "BHX": "BHX",
    "BRS": "BRS", "NCL": "NCL", "ABZ": "ABZ", "BFS": "BFS",
    # European city codes
    "CDG": "PAR", "ORY": "PAR",
    "FCO": "ROM", "CIA": "ROM",
    "JFK": "NYC", "EWR": "NYC", "LGA": "NYC",
    "LAX": "LAX", "SFO": "SFO", "ORD": "CHI",
    "IAD": "WAS", "DCA": "WAS",
    "NRT": "TYO", "HND": "TYO",
}

# Currency symbols for formatting
_CURRENCY_SYMBOLS: dict[str, str] = {
    "GBP": "£", "EUR": "€", "USD": "$", "AUD": "AUD",
    "INR": "₹", "JPY": "¥",
}


def _iata_to_city(iata: str) -> str:
    """Convert IATA airport code to BA city code. Falls back to IATA itself."""
    return _IATA_TO_BA_CITY.get(iata, iata)


class BritishAirwaysConnectorClient:
    """British Airways — SOLR pricing feed via curl_cffi."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        dep_city = _iata_to_city(req.origin)
        arr_city = _iata_to_city(req.destination)

        offers = await self._query_solr(dep_city, arr_city, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "BA %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"ba{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "GBP",
            offers=offers,
            total_results=len(offers),
        )

    async def _query_solr(
        self, dep_city: str, arr_city: str, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Query BA SOLR for one-way pricing data."""
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        future_iso = f"{req.date_from.year + 1}-{req.date_from.month:02d}-01T00:00:00Z"

        fq = (
            f"departure_city:{dep_city}+AND+"
            f"departure_country_code:*GB*+AND+"
            f"arrival_city:{arr_city}+AND+"
            f"trip_type:OW+AND+"
            f"cabin:M+AND+"
            f"outbound_date:[{now_iso}+TO+{future_iso}]"
        )

        url = (
            f"{_BASE}{_SOLR_PATH}?fq={fq}"
            f"&rows=50"
            f"&locale=en_GB"
            f"&sort=lowest_price%20asc"
            f"&wt=json"
        )

        logger.info("BA SOLR: querying %s→%s", dep_city, arr_city)

        try:
            data = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_solr_sync, url,
            )
        except Exception as e:
            logger.error("BA SOLR error: %s", e)
            return []

        if not data:
            return []

        return self._parse_solr_docs(data, req)

    def _fetch_solr_sync(self, url: str) -> dict | None:
        sess = creq.Session(impersonate="chrome124")
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("BA SOLR: returned %d", r.status_code)
                return None
            return r.json()
        except Exception as e:
            logger.warning("BA SOLR curl_cffi error: %s", e)
            return None

    def _parse_solr_docs(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Parse SOLR response docs into FlightOffer list."""
        docs = data.get("response", {}).get("docs", [])
        if not docs:
            return []

        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []

        for doc in docs:
            price = doc.get("lowest_price")
            if not price:
                continue
            try:
                price_f = round(float(price), 2)
            except (ValueError, TypeError):
                continue
            if price_f <= 0:
                continue

            currency = (doc.get("currency_code") or "GBP").strip()
            dep_airport = doc.get("departure_airport") or req.origin
            arr_airport = doc.get("arrival_airport") or req.destination
            dep_city_name = doc.get("dep_city_name") or ""
            arr_city_name = doc.get("arr_city_name") or ""
            outbound_date_str = (doc.get("outbound_date") or "")[:10]

            if not outbound_date_str:
                continue

            try:
                dep_dt = datetime.strptime(outbound_date_str, "%Y-%m-%d")
            except ValueError:
                continue

            # Filter: only include offers on or after the requested date
            req_date = req.date_from if isinstance(req.date_from, date) else req.date_from.date()
            if dep_dt.date() < req_date:
                continue

            journey_time = doc.get("journey_time") or 0
            is_direct = doc.get("route_connection_ind", "N") == "N"

            sym = _CURRENCY_SYMBOLS.get(currency, currency + " ")
            price_fmt = f"{sym}{price_f:.0f}" if price_f == int(price_f) else f"{sym}{price_f:.2f}"

            seg = FlightSegment(
                airline="BA",
                airline_name="British Airways",
                flight_no="",
                origin=dep_airport,
                destination=arr_airport,
                origin_city=dep_city_name,
                destination_city=arr_city_name,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=int(journey_time) * 60 if journey_time else 0,
                cabin_class="economy",
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=int(journey_time) * 60 if journey_time else 0,
                stopovers=0 if is_direct else 1,
            )

            fid = hashlib.md5(
                f"ba_{dep_airport}{arr_airport}{outbound_date_str}{price_f}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"ba_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=price_fmt,
                outbound=route,
                inbound=None,
                airlines=["British Airways"],
                owner_airline="BA",
                booking_url=(
                    f"https://www.britishairways.com/travel/fx/public/en_gb"
                    f"?from={dep_airport}&to={arr_airport}"
                    f"&depDate={outbound_date_str[:7].replace('-', '')}"
                    f"&cabin=M&oneWay=true&ad={req.adults or 1}"
                ),
                is_locked=False,
                source="britishairways_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"ba{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="GBP",
            offers=[],
            total_results=0,
        )
