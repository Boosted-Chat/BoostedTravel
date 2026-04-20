"""
SAS Scandinavian Airlines connector — BFF datepicker lowfare API.

SAS (IATA: SK) is the flag carrier of Denmark, Norway, and Sweden.
SkyTeam member. CPH/OSL/ARN hubs. 180+ destinations.

Strategy:
  SAS exposes a public BFF datepicker API that returns daily lowest fares
  for a full month. Works via plain httpx — no browser or cookies needed.

  GET https://www.flysas.com/bff/datepicker/flights/offers/v1
    ?market=en&origin=CPH&destination=LHR&adult=1
    &bookingFlow=revenue&departureDate=2026-05-01
  Response: {
    "currency": "EUR",
    "outbound": {
      "2026-05-01": {"totalPrice": 110, "points": 0},
      "2026-05-02": {"totalPrice": 78.05, "points": 0},
      ...
    }
  }

  Returns 31 daily prices per request. Works for all routes, not just hubs.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

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

_API = "https://www.flysas.com/bff/datepicker/flights/offers/v1"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.flysas.com/en/low-fare-calendar",
}


class SASConnectorClient:
    """SAS Scandinavian Airlines — BFF datepicker lowfare calendar API."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True,
                proxy=get_httpx_proxy_url(),)
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

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
        client = await self._client()
        date_str = req.date_from.strftime("%Y-%m-%d")

        params = {
            "market": "en",
            "origin": req.origin,
            "destination": req.destination,
            "adult": str(req.adults or 1),
            "bookingFlow": "revenue",
            "departureDate": date_str,
        }

        offers: list[FlightOffer] = []
        ib_price_map: dict[str, float] = {}  # date→price for return direction
        try:
            resp = await client.get(_API, params=params)
            if resp.status_code == 200:
                data = resp.json()

                # RT: fetch return direction calendar
                if req.return_from:
                    ret_date_str = req.return_from.strftime("%Y-%m-%d")
                    ib_params = {
                        "market": "en",
                        "origin": req.destination,
                        "destination": req.origin,
                        "adult": str(req.adults or 1),
                        "bookingFlow": "revenue",
                        "departureDate": ret_date_str,
                    }
                    try:
                        ib_resp = await client.get(_API, params=ib_params)
                        if ib_resp.status_code == 200:
                            ib_data = ib_resp.json()
                            for d, info in ib_data.get("outbound", {}).items():
                                p = info.get("totalPrice", 0)
                                if p > 0:
                                    ib_price_map[d] = float(p)
                    except Exception as e2:
                        logger.warning("SAS IB calendar error: %s", e2)

                offers = self._parse(data, req, ib_price_map=ib_price_map)
        except Exception as e:
            logger.error("SAS API error: %s", e)

        offers.sort(key=lambda o: o.price)
        elapsed = time.monotonic() - t0
        logger.info(
            "SAS %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        sh = hashlib.md5(
            f"sas{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest, *, ib_price_map: dict[str, float] | None = None) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        currency = data.get("currency", "EUR")
        outbound = data.get("outbound", {})
        target_date = req.date_from.strftime("%Y-%m-%d")
        ret_date = req.return_from.strftime("%Y-%m-%d") if req.return_from else None

        for date_str, info in outbound.items():
            price = info.get("totalPrice", 0)
            if price <= 0:
                continue

            # Filter to requested date only
            if date_str != target_date:
                continue

            dep_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=8)
            # Map cabin code to cabin name for segment
            _sk_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="SK",
                airline_name="SAS",
                flight_no="SK",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                cabin_class=_sk_cabin,
            )
            route = FlightRoute(
                segments=[seg], total_duration_seconds=0, stopovers=0
            )

            # RT: build inbound route if return price available
            _ib_route = None
            _ib_price = 0.0
            if ret_date and ib_price_map:
                # Prefer exact return date, fall back to cheapest
                if ret_date in ib_price_map:
                    _ib_price = ib_price_map[ret_date]
                elif ib_price_map:
                    _ib_price = min(ib_price_map.values())
                if _ib_price > 0:
                    ib_dt = datetime.strptime(ret_date, "%Y-%m-%d").replace(hour=8)
                    ib_seg = FlightSegment(
                        airline="SK", airline_name="SAS", flight_no="SK",
                        origin=req.destination, destination=req.origin,
                        departure=ib_dt, arrival=ib_dt,
                        cabin_class=_sk_cabin,
                    )
                    _ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)

            total_price = round(float(price) + _ib_price, 2) if _ib_route else round(float(price), 2)
            id_prefix = "sk_rt_" if _ib_route else "sk_"

            key = f"sk_{req.origin}{req.destination}{date_str}{total_price}{ret_date or ''}"
            oid = hashlib.md5(key.encode()).hexdigest()[:12]

            trip = "RT" if _ib_route else "OW"
            booking_url = (
                f"https://www.flysas.com/en/book/flights?"
                f"origin={req.origin}&destination={req.destination}"
                f"&outboundDate={date_str.replace('-', '')}"
                f"&adults={req.adults or 1}&trip={trip}"
            )
            if _ib_route and ret_date:
                booking_url += f"&inboundDate={ret_date.replace('-', '')}"

            offers.append(
                FlightOffer(
                    id=f"{id_prefix}{oid}",
                    price=total_price,
                    currency=currency,
                    price_formatted=f"{total_price:.2f} {currency}",
                    outbound=route,
                    inbound=_ib_route,
                    airlines=["SAS"],
                    owner_airline="SK",
                    conditions={"price_type": "lowest_fare"},
                    booking_url=booking_url,
                    is_locked=False,
                    source="sas_direct",
                    source_tier="free",
                )
            )

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        sh = hashlib.md5(
            f"sas{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
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
                    id=f"rt_sas_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
