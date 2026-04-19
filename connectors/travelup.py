"""
TravelUp connector — UK OTA with consolidator fares (direct API).

TravelUp.com is a UK-based OTA that sources fares from multiple consolidators
and GDS backends. Uses their flight-search API with a date-range cheapest-fare
endpoint to retrieve pricing for nearby dates.

Strategy (direct API):
1. Call tup-flightsearch-api.azurewebsites.net/api/search/cheapest with api-key.
2. Query the target date ±3 days to get multiple price points.
3. Parse results → FlightOffers.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timedelta
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

_BASE = "https://www.travelup.com"
_MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


def _build_travelup_url(
    origin: str, dest: str, dep_date: datetime, ret_date: datetime | None = None,
    adults: int = 1, children: int = 0, infants: int = 0,
) -> str:
    """Build a valid TravelUp search URL with the required SEO slug.

    TravelUp uses path-based URLs that REQUIRE a slug or the page returns 404.
    Format: /en-gb/flight-search/{orig}/{dest}/{depYYMMDD}/{retYYMMDD}/{slug}?params
    """
    dep_short = dep_date.strftime("%y%m%d")
    # TravelUp only supports round-trip URLs — use dep+7d if no return
    if ret_date is None:
        ret_date = dep_date + timedelta(days=7)
    ret_short = ret_date.strftime("%y%m%d")
    month_name = _MONTH_NAMES[dep_date.month - 1]
    slug = f"flying-from-{origin.lower()}-to-{dest.lower()}-in-{month_name}-{dep_date.year}"
    return (
        f"{_BASE}/en-gb/flight-search/{origin.lower()}/{dest.lower()}"
        f"/{dep_short}/{ret_short}/{slug}"
        f"?adults={adults}&children={children}&infants={infants}&class=0"
    )
_API_URL = "https://tup-flightsearch-api.azurewebsites.net/api/search/cheapest"
_API_KEY = "9a9635e3240c41018ddadfa51bb378e4"


class TravelupConnectorClient:
    """TravelUp — UK OTA, direct API for cheapest fares."""

    def __init__(self, timeout: float = 55.0):
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
        target = req.date_from
        date_str = target.strftime("%Y-%m-%d")

        # Query a ±3-day window around the target date to get multiple price points
        ds = (target - timedelta(days=3)).strftime("%Y-%m-%d")
        de = (target + timedelta(days=3)).strftime("%Y-%m-%d")

        headers = {
            "api-key": _API_KEY,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        }
        params = {
            "di": req.origin,
            "ai": req.destination,
            "ap": str(req.adults or 1),
            "cp": str(req.children or 0),
            "ip": str(req.infants or 0),
            "c": {"M": "1", "W": "2", "C": "3", "F": "4"}.get(req.cabin_class or "M", "1"),  # cabin class
            "sm": "2",  # search mode
            "l": "en-GB",
            "ds": ds,
            "de": de,
            "rf": "false",  # one-way
            "d": "0",
        }

        offers: list[FlightOffer] = []
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, proxy=get_httpx_proxy_url(),
            ) as client:
                resp = await client.get(_API_URL, headers=headers, params=params)
                if resp.status_code != 200:
                    logger.warning("TravelUp API %d for %s→%s", resp.status_code, req.origin, req.destination)
                    return self._empty(req)

                data = resp.json()
                results = data.get("r", [])
                if not isinstance(results, list):
                    return self._empty(req)

                for item in results:
                    price = item.get("cf")
                    dep_date_str = item.get("dd", "")
                    if not price or price <= 0 or not dep_date_str:
                        continue

                    try:
                        dep_date = datetime.strptime(dep_date_str, "%Y-%m-%d")
                    except ValueError:
                        continue

                    booking_dep = dep_date.strftime("%Y-%m-%d")
                    # TravelUp uses path-based URLs with YYMMDD dates
                    booking_dep_short = dep_date.strftime("%y%m%d")
                    segments = [FlightSegment(
                        airline="TravelUp",
                        flight_no="",
                        origin=req.origin,
                        destination=req.destination,
                        departure=dep_date,
                        arrival=dep_date,
                        duration_seconds=0,
                    )]
                    route = FlightRoute(
                        segments=segments,
                        total_duration_seconds=0,
                        stopovers=0,
                    )
                    oid = hashlib.md5(
                        f"tvup_{req.origin}{req.destination}{booking_dep}{price}".encode()
                    ).hexdigest()[:12]

                    offers.append(FlightOffer(
                        id=f"tvup_{oid}",
                        price=round(float(price), 2),
                        currency="GBP",
                        price_formatted=f"from {price:.2f} GBP",
                        outbound=route,
                        inbound=None,
                        airlines=["TravelUp"],
                        owner_airline="TravelUp",
                        booking_url=_build_travelup_url(
                            req.origin, req.destination, dep_date,
                            ret_date=req.return_from if req.return_from else None,
                            adults=req.adults or 1,
                            children=req.children or 0,
                            infants=req.infants or 0,
                        ),
                        is_locked=False,
                        source="travelup_ota",
                        source_tier="free",
                        conditions={"price_type": "indicative", "note": "Starting-from price; actual fare may differ at checkout"},
                    ))

        except httpx.HTTPError as e:
            logger.error("TravelUp HTTP error: %s", e)
            return self._empty(req)
        except Exception as e:
            logger.error("TravelUp error: %s", e)
            return self._empty(req)

        _td = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
        offers = [o for o in offers if o.outbound and o.outbound.segments and o.outbound.segments[0].departure.date() == _td]
        offers.sort(key=lambda o: o.price)
        elapsed = time.monotonic() - t0
        logger.info(
            "TravelUp %s→%s: %d offers in %.1fs (API)",
            req.origin, req.destination, len(offers), elapsed,
        )

        sh = hashlib.md5(f"travelup{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "GBP",
            offers=offers,
            total_results=len(offers),
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
                # Rebuild URL with actual inbound departure date
                ib_dep = i.outbound.segments[0].departure if i.outbound and i.outbound.segments else None
                rt_url = _build_travelup_url(
                    req.origin, req.destination,
                    o.outbound.segments[0].departure,
                    ret_date=ib_dep,
                    adults=req.adults or 1,
                    children=req.children or 0,
                    infants=req.infants or 0,
                ) if o.outbound and o.outbound.segments else o.booking_url
                combos.append(FlightOffer(
                    id=f"rt_tvup_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=rt_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"travelup{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="GBP", offers=[], total_results=0,
        )
