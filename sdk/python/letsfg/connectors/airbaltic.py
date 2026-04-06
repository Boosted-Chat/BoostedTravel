"""
airBaltic connector -- calendar fare data from airbaltic.com public API via curl_cffi.

airBaltic (IATA: BT) is a Latvian flag carrier based in Riga, operating
short/medium-haul flights across Europe, the Middle East, and Central Asia.
Default currency EUR.

Strategy (curl_cffi required — WAF blocks httpx Python TLS fingerprint):
1. Call /api/fsf/outbound with origin, destination, month -> daily prices
2. Each day entry has price + isDirect flag
3. Build one FlightOffer per day with the cheapest calendar price
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

from curl_cffi import requests as creq

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_curl_cffi_proxies

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.airbaltic.com/en/book-flight",
    "Origin": "https://www.airbaltic.com",
}

_API_BASE = "https://www.airbaltic.com/api/fsf"


class AirbalticConnectorClient:
    """airBaltic calendar-fare scraper via public API."""

    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        date_from = req.date_from
        date_to = req.date_to or date_from
        is_rt = req.return_from is not None

        # Collect all months in the search range
        months: list[str] = []
        current = date_from.replace(day=1)
        end_month = date_to.replace(day=1)
        while current <= end_month:
            months.append(current.strftime("%Y-%m"))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        try:
            all_days = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_months_sync, req, months
            )
        except Exception as e:
            logger.error("airBaltic API error: %s", e)
            return self._empty(req)

        # Filter to requested date range and build offers
        outbound_offers = self._build_offers(all_days, req, date_from, date_to)

        # For RT, fetch return leg calendar and build combos
        if is_rt and outbound_offers:
            ret_date = req.return_from
            ret_months = [ret_date.replace(day=1).strftime("%Y-%m")]
            from copy import copy
            ret_req = copy(req)
            ret_req.origin = req.destination
            ret_req.destination = req.origin
            try:
                ret_days = await asyncio.get_event_loop().run_in_executor(
                    None, self._fetch_months_sync, ret_req, ret_months
                )
            except Exception:
                ret_days = []

            if ret_days:
                inbound_offers = self._build_offers(ret_days, ret_req, ret_date, ret_date)
                if inbound_offers:
                    booking_url = self._build_booking_url_rt(req)
                    rt_offers = []
                    for ob in outbound_offers[:15]:
                        for ib in inbound_offers[:10]:
                            combined = round(ob.price + ib.price, 2)
                            rt_id = hashlib.md5(
                                f"bt_rt_{ob.id}_{ib.id}".encode()
                            ).hexdigest()[:12]
                            rt_offers.append(FlightOffer(
                                id=f"bt_{rt_id}",
                                price=combined,
                                currency="EUR",
                                price_formatted=f"{combined:.2f} EUR",
                                outbound=ob.outbound,
                                inbound=ib.outbound,
                                airlines=["airBaltic"],
                                owner_airline="BT",
                                booking_url=booking_url,
                                is_locked=False,
                                source="airbaltic_direct",
                                source_tier="free",
                            ))
                    rt_offers.sort(key=lambda o: o.price)
                    outbound_offers = rt_offers[:50] if rt_offers else outbound_offers

        elapsed = time.monotonic() - t0
        outbound_offers.sort(key=lambda o: o.price)
        logger.info(
            "airBaltic %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(outbound_offers), elapsed,
        )

        h = hashlib.md5(
            f"bt{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"bt_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=outbound_offers,
            total_results=len(outbound_offers),
        )

    def _fetch_months_sync(self, req: FlightSearchRequest, months: list[str]) -> list[dict]:
        """Fetch calendar data for all months synchronously via curl_cffi."""
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        all_days: list[dict] = []
        to = int(self.timeout)

        for month in months:
            params = {
                "origin": req.origin,
                "destin": req.destination,
                "tripType": "oneway",
                "numAdt": str(req.adults),
                "numChd": str(req.children),
                "numInf": str(req.infants),
                "departureMonth": month,
                "flightMode": "oneway",
            }
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{_API_BASE}/outbound?{qs}"

            try:
                resp = sess.get(url, headers=_HEADERS, timeout=to)
            except Exception as e:
                logger.warning("airBaltic outbound %s error: %s", month, e)
                continue

            if resp.status_code != 200:
                logger.warning("airBaltic outbound %s: HTTP %d", month, resp.status_code)
                continue

            try:
                body = resp.json()
            except Exception:
                continue

            if not body.get("success"):
                logger.warning("airBaltic outbound %s: %s", month, body.get("error", ""))
                continue

            data = body.get("data", [])
            if isinstance(data, list):
                all_days.extend(data)

        return all_days

    @staticmethod
    def _build_offers(
        days: list[dict],
        req: FlightSearchRequest,
        date_from,
        date_to,
    ) -> list[FlightOffer]:
        from_str = date_from.isoformat()
        to_str = date_to.isoformat()
        offers: list[FlightOffer] = []

        for day in days:
            price = day.get("price")
            if not price or price <= 0:
                continue

            dep_date = day.get("date", "")
            if not dep_date:
                continue

            # Filter to requested date range
            if dep_date < from_str or dep_date > to_str:
                continue

            is_direct = day.get("isDirect", False)

            dep_dt = datetime(2000, 1, 1)
            try:
                dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
            except ValueError:
                pass

            segment = FlightSegment(
                airline="BT",
                airline_name="airBaltic",
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
                stopovers=0 if is_direct else 1,
            )

            fid = hashlib.md5(
                f"bt_{req.origin}{req.destination}{dep_date}{price}".encode()
            ).hexdigest()[:12]

            booking_url = (
                f"https://www.airbaltic.com/en/book-flight"
                f"?originCode={req.origin}&destinCode={req.destination}"
                f"&tripType={'roundtrip' if req.return_from else 'oneway'}&numAdt={req.adults}"
                f"&numChd={req.children}&numInf={req.infants}"
            )

            offers.append(FlightOffer(
                id=f"bt_{fid}",
                price=round(price, 2),
                currency="EUR",
                price_formatted=f"{price:.2f} EUR",
                outbound=route,
                inbound=None,
                airlines=["airBaltic"],
                owner_airline="BT",
                booking_url=booking_url,
                is_locked=False,
                source="airbaltic_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _build_booking_url_rt(req: FlightSearchRequest) -> str:
        return (
            f"https://www.airbaltic.com/en/book-flight"
            f"?originCode={req.origin}&destinCode={req.destination}"
            f"&tripType=roundtrip&numAdt={req.adults}"
            f"&numChd={req.children}&numInf={req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"bt{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"bt_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "EUR",
            offers=[],
            total_results=0,
        )
