"""
Flair Airlines connector -- fetches fare data from flights.flyflair.com
(EveryMundo airTRFX platform) via curl_cffi.

Flair Airlines (IATA: F8) is a Canadian ultra-low-cost carrier based in
Edmonton, Alberta. Operates domestic Canadian, transborder US, and
Mexico/Caribbean routes. Default currency CAD.

Strategy (curl_cffi required — Cloudflare blocks httpx Python TLS fingerprint):
1. Map IATA codes to city slugs used by flights.flyflair.com
2. Fetch route page: flights.flyflair.com/en-ca/flights-from-{origin}-to-{dest}
3. Extract __NEXT_DATA__ JSON from page
4. Parse StandardFareModule fares -> FlightOffers
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
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

# IATA code -> URL slug mapping for flights.flyflair.com route pages
_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "YTO": "toronto", "YMQ": "montreal",
    # Canada
    "YXX": "abbotsford",
    "YYC": "calgary",
    "YYG": "charlottetown",
    "YEG": "edmonton",
    "YHZ": "halifax",
    "YLW": "kelowna",
    "YKF": "kitchener-waterloo",
    "YQM": "moncton",
    "YUL": "montreal",
    "YSJ": "saint-john",
    "YYT": "st-johns",
    "YQT": "thunder-bay",
    "YYZ": "toronto",
    "YVR": "vancouver",
    "YYJ": "victoria-bc",
    "YWG": "winnipeg",
    # Caribbean
    "PUJ": "punta-cana",
    "KIN": "kingston",
    "MBJ": "montego-bay",
    # Mexico
    "CUN": "cancun",
    "GDL": "guadalajara",
    "MEX": "mexico-city",
    "PVR": "puerto-vallarta",
    # USA
    "FLL": "fort-lauderdale",
    "LAS": "las-vegas",
    "LAX": "los-angeles",
    "MCO": "orlando",
    "SFO": "san-francisco",
}

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
}

_BASE = "https://flights.flyflair.com/en-ca"


class FlairConnectorClient:
    """Flair Airlines httpx scraper -- flights.flyflair.com fare pages."""

    def __init__(self, timeout: float = 15.0):
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

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("Flair: unmapped IATA code %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Flair: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
            if not html:
                return self._empty(req)

            fares = self._extract_fares(html)
            if not fares:
                logger.warning("Flair: no fares found in page")
                return self._empty(req)

            offers = self._build_offers(fares, req)

            # RT: fetch reverse route for inbound fares
            if req.return_from and offers and dest_slug:
                try:
                    _rev_url = f"{_BASE}/flights-from-{dest_slug}-to-{origin_slug}"
                    _rev_html = await asyncio.get_event_loop().run_in_executor(
                        None, self._fetch_sync, _rev_url
                    )
                    if _rev_html:
                        _ib_fares = self._extract_fares(_rev_html)
                        if _ib_fares:
                            _ib_best_price = float("inf")
                            for _f in _ib_fares:
                                _p = _f.get("totalPrice")
                                if _p and 0 < float(_p) < _ib_best_price:
                                    _ib_best_price = float(_p)
                            if _ib_best_price < float("inf"):
                                _ret = req.return_from
                                _ret_dt = datetime.combine(_ret, datetime.min.time()) if not isinstance(_ret, datetime) else _ret
                                _f8_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                                _ib_seg = FlightSegment(
                                    airline="F8", airline_name="Flair Airlines", flight_no="",
                                    origin=req.destination, destination=req.origin,
                                    departure=_ret_dt, arrival=_ret_dt,
                                    duration_seconds=0, cabin_class=_f8_cabin,
                                )
                                _ib_route = FlightRoute(segments=[_ib_seg], total_duration_seconds=0, stopovers=0)
                                for _i, _o in enumerate(offers):
                                    _total = round(_o.price + _ib_best_price, 2)
                                    _rd = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, "strftime") else str(req.return_from)
                                    _burl = _o.booking_url + f"&return={_rd}"
                                    offers[_i] = FlightOffer(
                                        id=f"rt_{_o.id}", price=_total, currency=_o.currency,
                                        price_formatted=f"{_total:.2f} {_o.currency}",
                                        outbound=_o.outbound, inbound=_ib_route,
                                        airlines=_o.airlines, owner_airline=_o.owner_airline,
                                        booking_url=_burl, is_locked=False,
                                        source=_o.source, source_tier=_o.source_tier,
                                    )
                except Exception:
                    pass

            _td = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
            exact = [o for o in offers if o.outbound and o.outbound.segments and o.outbound.segments[0].departure.date() == _td]
            offers = exact  # Never fall back to wrong-date offers
            elapsed = time.monotonic() - t0

            offers.sort(key=lambda o: o.price)
            logger.info(
                "Flair %s->%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            h = hashlib.md5(
                f"flair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else (req.currency or "CAD"),
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Flair error: %s", e)
            return self._empty(req)

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("Flair: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("Flair curl_cffi error: %s", e)
            return None

    @staticmethod
    def _extract_fares(html: str) -> list[dict]:
        """Extract fare dicts from __NEXT_DATA__ StandardFareModule."""
        nd_match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
        )
        if not nd_match:
            return []

        try:
            nd = json.loads(nd_match.group(1))
        except (json.JSONDecodeError, ValueError):
            return []

        apollo = (
            nd.get("props", {})
            .get("pageProps", {})
            .get("apolloState", {})
            .get("data", {})
        )
        if not apollo:
            return []

        for v in apollo.values():
            if isinstance(v, dict) and v.get("__typename") == "StandardFareModule":
                fares = v.get("fares", [])
                if fares and isinstance(fares, list):
                    return [f for f in fares if isinstance(f, dict)]
        return []

    def _build_offers(
        self, fares: list[dict], req: FlightSearchRequest
    ) -> list[FlightOffer]:
        booking_url = self._build_booking_url(req)
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []

        for fare in fares:
            price = fare.get("totalPrice")
            if not price or price <= 0:
                continue

            currency = fare.get("currencyCode") or req.currency or "CAD"
            dep_date = fare.get("departureDate", "")
            origin_code = fare.get("originAirportCode") or req.origin
            dest_code = fare.get("destinationAirportCode") or req.destination

            # Parse departure date for the segment
            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
                except ValueError:
                    pass

            segment = FlightSegment(
                airline="F8",
                airline_name="Flair Airlines",
                flight_no="",
                origin=origin_code,
                destination=dest_code,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=fare.get("travelClass", "Economy").lower(),
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=0,
                stopovers=0,
            )

            fid = hashlib.md5(
                f"f8_{origin_code}{dest_code}{dep_date}{price}".encode()
            ).hexdigest()[:12]

            # Use date-specific booking URL
            dep_url = dep_date or target_date
            offer_booking = (
                f"https://flyflair.com/flights"
                f"?from={origin_code}&to={dest_code}"
                f"&depart={dep_url}&adults={req.adults}&children={req.children}"
            )

            offers.append(FlightOffer(
                id=f"f8_{fid}",
                price=round(price, 2),
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Flair Airlines"],
                owner_airline="F8",
                booking_url=offer_booking,
                is_locked=False,
                source="flair_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://flyflair.com/flights"
            f"?from={req.origin}&to={req.destination}"
            f"&depart={dep}&adults={req.adults}&children={req.children}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"flair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CAD",
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
                    id=f"rt_flai_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
