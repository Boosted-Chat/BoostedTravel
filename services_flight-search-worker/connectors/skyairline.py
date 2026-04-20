"""
Sky Airline connector — EveryMundo airTRFX fare pages via curl_cffi.

Sky Airline (IATA: H2) is Chile's largest low-cost carrier.
Operates 45+ domestic and regional routes from SCL hub.
Destinations in Chile, Peru, Argentina, Brazil, Uruguay.

Strategy (curl_cffi, no browser):
  Sky Airline uses EveryMundo airTRFX at skyairline.com/flights/.
  1. Fetch route page: skyairline.com/flights/en/flights-from-{o}-to-{d}
  2. Extract __NEXT_DATA__ JSON from <script> tag
  3. Parse StandardFareModule fares from Apollo GraphQL state
  4. Filter by origin/destination airport codes and departure date
"""

from __future__ import annotations

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
from .airline_routes import city_match_set

logger = logging.getLogger(__name__)

_BASE = "https://www.skyairline.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "BUE": "buenos-aires", "SAO": "sao-paulo",
    # Chile
    "SCL": "santiago", "ANF": "antofagasta", "ARI": "arica",
    "IQQ": "iquique", "CJC": "calama", "CCP": "concepcion",
    "PMC": "puerto-montt", "ZOS": "osorno", "ZAL": "valdivia",
    "LSC": "la-serena", "CPO": "copiapo", "BBA": "balmaceda",
    "PUQ": "punta-arenas", "GXQ": "coyhaique",
    "WCA": "castro", "FTE": "el-calafate",
    # Peru
    "LIM": "lima", "CUZ": "cusco", "AQP": "arequipa",
    "IQT": "iquitos", "PIU": "piura", "TRU": "trujillo",
    "TPP": "tarapoto", "JUL": "juliaca", "AYP": "ayacucho",
    "TCQ": "tacna", "CIX": "chiclayo", "JAU": "jauja",
    # Argentina
    "EZE": "buenos-aires", "BRC": "bariloche",
    "MDZ": "mendoza", "COR": "cordoba",
    # Brazil
    "GRU": "sao-paulo", "FLN": "florianopolis",
    "CNF": "belo-horizonte", "BSB": "brasilia",
    # Uruguay
    "MVD": "montevideo",
    # Other
    "CUN": "cancun", "MIA": "miami",
}


class SkyAirlineConnectorClient:
    """Sky Airline Chile — EveryMundo airTRFX fare pages via curl_cffi."""

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
            logger.warning("Sky Airline: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/flights/en/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Sky Airline: fetching %s", url)

        try:
            with creq.Session(impersonate="chrome136", proxies=get_curl_cffi_proxies()) as sess:
                resp = sess.get(url, timeout=self.timeout, headers=_HEADERS)
            if resp.status_code != 200:
                logger.warning("Sky Airline: %s returned %d", url, resp.status_code)
                return self._empty(req)
        except Exception as e:
            logger.error("Sky Airline fetch error: %s", e)
            return self._empty(req)

        fares = self._extract_fares(resp.text)
        if not fares:
            logger.info("Sky Airline: no fares on page %s", url)
            return self._empty(req)

        offers = self._build_offers(fares, req)

        # RT: fetch reverse route for inbound fares
        if req.return_from and offers:
            try:
                _rev_url = f"{_BASE}/flights/en/flights-from-{dest_slug}-to-{origin_slug}"
                with creq.Session(impersonate="chrome136", proxies=get_curl_cffi_proxies()) as sess:
                    _rev_resp = sess.get(_rev_url, timeout=self.timeout, headers=_HEADERS)
                if _rev_resp.status_code == 200:
                    _ib_fares = self._extract_fares(_rev_resp.text)
                    _ib_best = float("inf")
                    for _f in _ib_fares:
                        _p = _f.get("totalPrice")
                        if _p:
                            try:
                                _pf = float(_p)
                                if 0 < _pf < _ib_best:
                                    _ib_best = _pf
                            except (ValueError, TypeError):
                                pass
                    if _ib_best < float("inf"):
                        _ret = req.return_from
                        _ret_dt = datetime.combine(_ret, datetime.min.time()) if not isinstance(_ret, datetime) else _ret
                        _ib_seg = FlightSegment(
                            airline="H2",
                            airline_name="Sky Airline",
                            flight_no="",
                            origin=req.destination,
                            destination=req.origin,
                            departure=_ret_dt,
                            arrival=_ret_dt,
                            duration_seconds=0,
                            cabin_class="economy",
                        )
                        _ib_route = FlightRoute(segments=[_ib_seg], total_duration_seconds=0, stopovers=0)
                        for _i, _o in enumerate(offers):
                            offers[_i] = FlightOffer(
                                id=f"rt_{_o.id}",
                                price=round(_o.price + _ib_best, 2),
                                currency=_o.currency,
                                price_formatted=f"{round(_o.price + _ib_best, 2):.2f} {_o.currency}",
                                outbound=_o.outbound,
                                inbound=_ib_route,
                                airlines=_o.airlines,
                                owner_airline=_o.owner_airline,
                                booking_url=_o.booking_url,
                                is_locked=False,
                                source=_o.source,
                                source_tier=_o.source_tier,
                            )
            except Exception:
                pass

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Sky Airline %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"skyairline{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _extract_fares(html: str) -> list[dict]:
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
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)

        # Separate exact-date and nearby fares (airTRFX shows cached snapshots)
        exact_fares: list[dict] = []
        nearby_fares: list[dict] = []
        for fare in fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig not in valid_origins or dest not in valid_dests:
                continue
            if not fare.get("totalPrice") or float(fare.get("totalPrice", 0)) <= 0:
                continue
            if fare.get("departureDate", "")[:10] == target_date:
                exact_fares.append(fare)
            else:
                nearby_fares.append(fare)

        # Prefer exact-date fares; fall back to all route fares
        use_fares = exact_fares

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
                airline="H2",
                airline_name="Sky Airline",
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
                f"h2_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"h2_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Sky Airline"],
                owner_airline="H2",
                booking_url=(
                    f"https://booking.skyairline.com/search/"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={target_date}"
                    f"&adults={req.adults or 1}&tripType={'R' if req.return_from else 'O'}"
                ),
                is_locked=False,
                source="skyairline_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"skyairline{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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
                    id=f"rt_skya_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
