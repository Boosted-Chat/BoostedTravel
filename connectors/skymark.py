"""
Skymark Airlines connector — Japanese LCC direct API.

Skymark Airlines (IATA: BC) is a Japanese low-cost carrier.
Hub at Tokyo Haneda (HND) with domestic routes across Japan including
Sapporo, Fukuoka, Kobe, Naha, Nagasaki, Kagoshima, and Shimojishima.

Strategy (httpx direct API — fare search):
  1. GET/POST to skymark.co.jp fare search API
  2. Parse JSON response → FlightOffer objects
  3. Construct booking URL
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

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

_SEARCH_URL = "https://www.skymark.co.jp/api/search/fare"
_LOWFARE_URL = "https://www.skymark.co.jp/api/search/lowfare"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://www.skymark.co.jp",
    "Referer": "https://www.skymark.co.jp/en/",
}

# Skymark station codes (domestic Japan only)
_SKYMARK_STATIONS = {
    "HND", "CTS", "FUK", "UKB", "OKA", "NGS", "KOJ", "SHI",
    "TKS", "MMJ", "SDJ", "FSZ", "IBR",
}


class SkymarkConnectorClient:
    """Skymark Airlines (BC) — Japanese LCC, direct httpx API client."""

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

        date_str = dt.strftime("%Y-%m-%d")

        payload = {
            "origin": req.origin,
            "destination": req.destination,
            "departureDate": date_str,
            "adults": req.adults or 1,
            "children": req.children or 0,
            "infants": req.infants or 0,
        }

        offers: list[FlightOffer] = []

        for url in [_SEARCH_URL, _LOWFARE_URL]:
            try:
                async with httpx.AsyncClient(
                    timeout=self.timeout, proxy=get_httpx_proxy_url(),
                ) as client:
                    resp = await client.post(url, headers=_HEADERS, json=payload)

                if resp.status_code == 200:
                    data = resp.json()
                    offers = self._parse_results(data, req, date_str)
                    if offers:
                        break
                else:
                    logger.debug("Skymark %s returned %d", url, resp.status_code)

            except httpx.HTTPError as exc:
                logger.debug("Skymark %s failed: %s", url, exc)
            except Exception as exc:
                logger.debug("Skymark error: %s", exc)

        offers.sort(key=lambda o: o.price)

        elapsed = time.monotonic() - t0
        logger.info(
            "Skymark %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"skymark{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "JPY",
            offers=offers,
            total_results=len(offers),
        )

    def _parse_results(
        self, data: dict, req: FlightSearchRequest, date_str: str
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        flights = (
            data.get("flights", data.get("fares", data.get("results", data.get("data", []))))
        )
        if isinstance(flights, dict):
            flights = flights.get("items", flights.get("flights", flights.get("fares", [])))
        if not isinstance(flights, list):
            return offers

        for flight in flights:
            if not isinstance(flight, dict):
                continue
            if flight.get("soldOut") or flight.get("noFlights"):
                continue

            price = (
                flight.get("price")
                or flight.get("totalPrice")
                or flight.get("fareAmount")
                or flight.get("amount")
                or flight.get("lowestFare")
            )
            if isinstance(price, dict):
                price = price.get("amount", price.get("value", price.get("total")))
            if not price:
                continue

            try:
                price_f = round(float(price), 2)
            except (ValueError, TypeError):
                continue
            if price_f <= 0:
                continue

            currency = flight.get("currency", flight.get("currencyCode", "JPY"))
            dep_date = flight.get("departureDate", flight.get("date", date_str))
            if isinstance(dep_date, str):
                dep_date = dep_date[:10]

            flight_no = flight.get("flightNumber", flight.get("number", ""))

            dedup_key = f"{req.origin}_{req.destination}_{dep_date}_{price_f}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            try:
                dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
            except ValueError:
                dep_dt = datetime(2000, 1, 1)

            duration = flight.get("duration", flight.get("durationMinutes", 0))
            dur_sec = int(duration) * 60 if duration and int(duration) < 1000 else int(duration or 0)
            _bc_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")

            seg = FlightSegment(
                airline="BC",
                airline_name="Skymark Airlines",
                flight_no=str(flight_no),
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=dur_sec,
                cabin_class=_bc_cabin,
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=dur_sec,
                stopovers=0,
            )

            fid = hashlib.md5(
                f"bc_{req.origin}{req.destination}{dep_date}{price_f}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"bc_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=f"{price_f:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Skymark Airlines"],
                owner_airline="BC",
                booking_url=(
                    f"https://www.skymark.co.jp/en/reservation/?"
                    f"from={req.origin}&to={req.destination}"
                    f"&date={dep_date}&adults={req.adults or 1}"
                ),
                is_locked=False,
                source="skymark_direct",
                source_tier="free",
            ))

        return offers

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
                    id=f"rt_sky_{cid}", price=price, currency=o.currency,
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
            f"skymark{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="JPY",
            offers=[],
            total_results=0,
        )
