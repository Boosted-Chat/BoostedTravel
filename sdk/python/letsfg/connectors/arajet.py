"""
Arajet connector — Dominican Republic ULCC.

Arajet (IATA: DM) is the Dominican Republic's first ultra-low-cost carrier.
Hub at SDQ (Santo Domingo). Rapidly growing, serving Caribbean + Americas.
28 destinations in Colombia, Mexico, Central America, Canada, US.

Strategy:
  Pure httpx — Arajet exposes a Radixx PSS calendar API at
  /pss/calendar?origin=X&destination=Y&month=YYYY-MM
  Returns fareProducts per day with baseAmount + totalAmount in USD.
  Fare classes: E (Economy), S (Sale), H (High season).
  Variants: BAS (basic), CLS (classic), COM (comfort), EXT (extra).
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

_BASE = "https://www.arajet.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer": "https://www.arajet.com/",
}

# Fare variant labels derived from Radixx fare codes
_VARIANT_LABELS = {
    "BAS": "Basic",
    "CLS": "Classic",
    "COM": "Comfort",
    "EXT": "Extra",
}


class ArajetConnectorClient:
    """Arajet — Radixx PSS calendar API."""

    def __init__(self, timeout: float = 25.0):
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
        target = req.date_from.strftime("%Y-%m-%d")
        month = req.date_from.strftime("%Y-%m")

        try:
            resp = await client.get(
                f"{_BASE}/pss/calendar",
                params={"origin": req.origin, "destination": req.destination, "month": month},
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Arajet calendar %s→%s: %s", req.origin, req.destination, exc)
            return self._empty(req)

        logger.info("Arajet: calendar %s->%s for %s: %d products", req.origin, req.destination, target, len(data.get("months", {}).get("items", [])))
        offers = self._parse(data, req, target)
        if not offers:
            logger.info("Arajet: no offers for %s->%s on %s", req.origin, req.destination, target)
            return self._empty(req)

        # RT: fetch reverse calendar for inbound
        ib_offers: list[FlightOffer] = []
        if req.return_from:
            ret_target = req.return_from.strftime("%Y-%m-%d")
            ret_month = req.return_from.strftime("%Y-%m")
            try:
                ib_resp = await client.get(
                    f"{_BASE}/pss/calendar",
                    params={"origin": req.destination, "destination": req.origin, "month": ret_month},
                )
                ib_resp.raise_for_status()
                ib_data = ib_resp.json()
                from copy import copy
                ib_req = copy(req)
                ib_req.origin = req.destination
                ib_req.destination = req.origin
                ib_req.date_from = req.return_from
                ib_offers = self._parse(ib_data, ib_req, ret_target)
            except Exception as e_ib:
                logger.warning("Arajet IB calendar %s→%s: %s", req.destination, req.origin, e_ib)

            if ib_offers:
                rt_offers: list[FlightOffer] = []
                for ob in offers[:15]:
                    for ib in ib_offers[:10]:
                        combined = round(ob.price + ib.price, 2)
                        rt_id = hashlib.md5(f"dm_rt_{ob.id}_{ib.id}".encode()).hexdigest()[:12]
                        rt_offers.append(FlightOffer(
                            id=f"dm_rt_{rt_id}",
                            price=combined,
                            currency=ob.currency,
                            price_formatted=f"{combined:.2f} {ob.currency}",
                            outbound=ob.outbound,
                            inbound=ib.outbound,
                            airlines=["Arajet"],
                            owner_airline="DM",
                            conditions=ob.conditions,
                            booking_url=(
                                f"https://www.arajet.com/en-us/booking/select"
                                f"?origin={req.origin}&destination={req.destination}"
                                f"&date={target}&return={ret_target}"
                                f"&adt={req.adults or 1}"
                            ),
                            is_locked=False,
                            source="arajet_direct",
                            source_tier="free",
                        ))
                if rt_offers:
                    rt_offers.sort(key=lambda o: o.price)
                    offers = rt_offers[:50]

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Arajet %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"arajet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    # ------------------------------------------------------------------

    def _parse(self, data: dict, req: FlightSearchRequest, target: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        exact: list[FlightOffer] = []
        cheapest_nearby: tuple[float, FlightOffer | None] = (float("inf"), None)

        for month in data.get("months", {}).get("items", []):
            for week in month.get("weeks", {}).get("items", []):
                for day in week.get("days", {}).get("items", []):
                    date_str = day.get("description", "")[:10]
                    products = day.get("fareProducts")
                    if not products or not products.get("items"):
                        continue
                    for product in products["items"]:
                        prices = product.get("prices", {}).get("items", [])
                        if not prices:
                            continue
                        price_item = prices[0]
                        total = price_item.get("totalAmount", {})
                        price = total.get("value") or price_item.get("baseAmount", {}).get("value")
                        if not price or float(price) <= 0:
                            continue
                        currency = (total.get("currency") or {}).get("code") or "USD"
                        price_f = round(float(price), 2)
                        fare_code = product.get("code", "")

                        dep_dt = datetime(req.date_from.year, req.date_from.month, req.date_from.day)
                        if date_str:
                            try:
                                dep_dt = datetime.strptime(date_str, "%Y-%m-%d")
                            except ValueError:
                                pass

                        seg = FlightSegment(
                            airline="DM",
                            airline_name="Arajet",
                            flight_no="",
                            origin=req.origin,
                            destination=req.destination,
                            departure=dep_dt,
                            arrival=dep_dt,
                            duration_seconds=0,
                            cabin_class="economy",
                        )
                        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
                        fid = hashlib.md5(f"dm_{req.origin}{req.destination}{date_str}{price_f}".encode()).hexdigest()[:12]
                        offer = FlightOffer(
                            id=f"dm_{fid}",
                            price=price_f,
                            currency=currency,
                            price_formatted=f"{price_f:.2f} {currency}",
                            outbound=route,
                            inbound=None,
                            airlines=["Arajet"],
                            owner_airline="DM",
                            conditions={
                                "carry_on": "1x personal item included; overhead carry-on from ~USD 20",
                                "checked_bag": "no free checked bag — add-on from ~USD 25",
                                "seat": "seat selection from ~USD 5",
                            },
                            booking_url=(
                                f"https://www.arajet.com/en-us/booking/select"
                                f"?origin={req.origin}&destination={req.destination}"
                                f"&date={target}&adt={req.adults or 1}"
                            ),
                            is_locked=False,
                            source="arajet_calendar",
                            source_tier="free",
                        )
                        if date_str == target:
                            exact.append(offer)
                        else:
                            if price_f < cheapest_nearby[0]:
                                cheapest_nearby = (price_f, offer)

        if exact:
            exact.sort(key=lambda o: o.price)
            return exact
        elif cheapest_nearby[1]:
            return [cheapest_nearby[1]]
        return []

    @staticmethod
    def _cabin_label(fare_class: str, code: str) -> str:
        variant = ""
        for k, v in _VARIANT_LABELS.items():
            if k in code.upper():
                variant = v
                break
        cls_map = {"E": "Economy", "S": "Economy", "H": "Economy"}
        cls = cls_map.get(fare_class, "Economy")
        return f"{cls} {variant}".strip() if variant else cls

    def _build_offer(
        self, req: FlightSearchRequest, target: str,
        total: float, currency: str, cabin: str, code: str,
    ) -> FlightOffer:
        raise RuntimeError("Arajet fare-only calendar offers are suppressed until real schedule times are available")


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
                    id=f"rt_arajet_{cid}", price=price, currency=o.currency,
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
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
