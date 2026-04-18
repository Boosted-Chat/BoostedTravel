"""
Icelandair connector — www.icelandair.com via curl_cffi.

Icelandair (IATA: FI) is Iceland's flag carrier. Key for transatlantic routes
via KEF (Reykjavik-Keflavik) hub connecting Europe <> North America.

Status: Icelandair migrated from EveryMundo airTRFX to Next.js App Router (RSC).
  - Old URL (/en-us/flights/) → 404 since early 2026
  - New URL (/us/flights/) → 200, RSC page, fare data loaded client-side only
  - No inline fare data available without browser JS execution
  - Connector returns empty gracefully; will auto-recover if upstream adds RSC fares.

Strategy:
  1. Fetch /us/flights/flights-from-{origin}-to-{dest} page
  2. Try __NEXT_DATA__ extraction (for any future EveryMundo partial pages)
  3. Returns empty if no structured fare data found
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

from .airline_routes import city_match_set

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_curl_cffi_proxies

logger = logging.getLogger(__name__)

_BASE = "https://www.icelandair.com"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# IATA → slug for Icelandair EveryMundo route pages.
_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "LON": "london", "NYC": "new-york", "PAR": "paris",
    # Iceland
    "KEF": "reykjavik", "AEY": "akureyri", "EGS": "egilsstadir",
    # UK / Ireland
    "LHR": "london", "LGW": "london", "MAN": "manchester",
    "EDI": "edinburgh", "GLA": "glasgow", "BHX": "birmingham",
    "DUB": "dublin",
    # Europe
    "CDG": "paris", "AMS": "amsterdam", "BRU": "brussels",
    "FRA": "frankfurt", "MUC": "munich", "BER": "berlin",
    "ZRH": "zurich", "GVA": "geneva",
    "CPH": "copenhagen", "ARN": "stockholm", "OSL": "oslo", "HEL": "helsinki",
    "BCN": "barcelona", "MAD": "madrid", "LIS": "lisbon",
    "FCO": "rome", "MXP": "milan", "VIE": "vienna",
    "WAW": "warsaw", "PRG": "prague",
    # North America
    "JFK": "new-york", "EWR": "newark", "BOS": "boston",
    "ORD": "chicago", "IAD": "washington-dc", "DEN": "denver",
    "SEA": "seattle", "MSP": "minneapolis", "PDX": "portland",
    # Canada
    "YYZ": "toronto", "YUL": "montreal", "YVR": "vancouver",
    "YYC": "calgary", "YOW": "ottawa", "YHZ": "halifax",
}

# Map IATA airport codes to city codes used in Icelandair fares.
_IATA_TO_CITY: dict[str, str] = {
    "JFK": "NYC", "EWR": "NYC", "LGA": "NYC",
    "LHR": "LON", "LGW": "LON", "STN": "LON",
    "CDG": "PAR", "ORY": "PAR",
    "KEF": "REK",
}


class IcelandairConnectorClient:
    """Icelandair — EveryMundo airTRFX via curl_cffi (Cloudflare bypass)."""

    def __init__(self, timeout: float = 25.0):
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
            logger.warning("Icelandair: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/us/flights/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Icelandair: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("Icelandair fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        fares = self._extract_fares(html)
        if not fares:
            logger.info("Icelandair: no fares on page")
            return self._empty(req)

        offers = self._build_offers(fares, req)

        # RT: fetch reverse route for inbound fares
        if req.return_from and offers and dest_slug:
            try:
                _rev_url = f"{_BASE}/us/flights/flights-from-{dest_slug}-to-{origin_slug}"
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
                            _ib_seg = FlightSegment(
                                airline="FI", airline_name="Icelandair", flight_no="",
                                origin=req.destination, destination=req.origin,
                                departure=_ret_dt, arrival=_ret_dt,
                                duration_seconds=0, cabin_class="economy",
                            )
                            _ib_route = FlightRoute(segments=[_ib_seg], total_duration_seconds=0, stopovers=0)
                            for _i, _o in enumerate(offers):
                                _total = round(_o.price + _ib_best_price, 2)
                                _rd = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, "strftime") else str(req.return_from)
                                offers[_i] = FlightOffer(
                                    id=f"rt_{_o.id}", price=_total, currency=_o.currency,
                                    price_formatted=f"{_total:.2f} {_o.currency}",
                                    outbound=_o.outbound, inbound=_ib_route,
                                    airlines=_o.airlines, owner_airline=_o.owner_airline,
                                    booking_url=_o.booking_url.replace("&type=oneway", f"&type=roundtrip&return={_rd}"),
                                    is_locked=False,
                                    source=_o.source, source_tier=_o.source_tier,
                                )
            except Exception:
                pass

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Icelandair %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"icelandair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("Icelandair: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("Icelandair curl_cffi error: %s", e)
            return None

    @staticmethod
    def _extract_fares(html: str) -> list[dict]:
        """Extract all fares from ALL StandardFareModules in __NEXT_DATA__."""
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
            re.S,
        )
        if not m:
            return []
        try:
            nd = json.loads(m.group(1))
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

        all_fares: list[dict] = []
        for v in apollo.values():
            if not isinstance(v, dict) or v.get("__typename") != "StandardFareModule":
                continue
            for f in v.get("fares", []):
                if isinstance(f, dict) and "__ref" in f:
                    ref_data = apollo.get(f["__ref"])
                    if ref_data and isinstance(ref_data, dict):
                        all_fares.append(ref_data)
                elif isinstance(f, dict):
                    all_fares.append(f)
        return all_fares

    def _build_offers(self, fares: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []

        origin_codes = city_match_set(req.origin)
        dest_codes = city_match_set(req.destination)

        # Separate exact-date and nearby fares (airTRFX shows cached snapshots)
        exact_fares: list[dict] = []
        nearby_fares: list[dict] = []
        for fare in fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig not in origin_codes or dest not in dest_codes:
                continue
            if not fare.get("totalPrice") or float(fare.get("totalPrice", 0)) <= 0:
                continue
            if fare.get("departureDate", "")[:10] == target_date:
                exact_fares.append(fare)
            else:
                nearby_fares.append(fare)

        # Prefer exact-date fares; fall back to all route fares
        use_fares = exact_fares if exact_fares else nearby_fares

        for fare in use_fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            dep_date = fare.get("departureDate", "")

            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            currency = fare.get("currencyCode") or "USD"
            price_f = round(float(price), 2)

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            cabin = (fare.get("formattedTravelClass") or "Economy").lower()
            seg = FlightSegment(
                airline="FI",
                airline_name="Icelandair",
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                origin_city=fare.get("originCity", ""),
                destination_city=fare.get("destinationCity", ""),
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            fid = hashlib.md5(
                f"fi_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"fi_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Icelandair"],
                owner_airline="FI",
                booking_url=(
                    f"https://www.icelandair.com/search/results"
                    f"?from={req.origin}&to={req.destination}"
                    f"&depart={target_date}"
                    f"&adults={req.adults or 1}&type=oneway"
                ),
                is_locked=False,
                source="icelandair_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"icelandair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
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
                    id=f"rt_icel_{cid}", price=price, currency=o.currency,
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
