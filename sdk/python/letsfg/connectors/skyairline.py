"""Sky Airline connector - EveryMundo Sputnik API via curl_cffi.

Sky Airline (IATA: H2) is Chile's largest low-cost carrier.
Operates 45+ domestic and regional routes from SCL hub.
Destinations in Chile, Peru, Argentina, Brazil, Uruguay.

Strategy (curl_cffi, no browser):
  Sky Airline uses EveryMundo airTRFX. The SSR pages embed empty fare arrays;
  actual fares come from the EveryMundo Sputnik grouped-routes API.
  POST https://openair-california.airtrfx.com/airfare-sputnik-service/v3/h2/fares/grouped-routes
  Headers: em-api-key, Origin: https://mm-prerendering-static-prod.airtrfx.com
  Returns fare calendar (date + price) - no departure times.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta
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

_SPUTNIK_URL = "https://openair-california.airtrfx.com/airfare-sputnik-service/v3/h2/fares/grouped-routes"
_SPUTNIK_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_SPUTNIK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://mm-prerendering-static-prod.airtrfx.com",
    "Referer": "https://mm-prerendering-static-prod.airtrfx.com/",
    "em-api-key": _SPUTNIK_KEY,
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

        dt = req.date_from
        if not isinstance(dt, datetime):
            dt = datetime(dt.year, dt.month, dt.day)
        start = (dt - timedelta(days=3)).strftime("%Y-%m-%d")
        end = (dt + timedelta(days=30)).strftime("%Y-%m-%d")

        payload = {
            "markets": ["CL", "PE", "AR", "BR", "UY"],
            "languageCode": "en",
            "dataExpirationWindow": "7d",
            "datePattern": "dd MMM yy (E)",
            "outputCurrencies": ["USD"],
            "departure": {"start": start, "end": end},
            "budget": {"maximum": None},
            "passengers": {"adults": max(1, req.adults or 1)},
            "travelClasses": ["ECONOMY"],
            "flightType": "ONE_WAY",
            "flexibleDates": True,
            "faresPerRoute": "10",
            "trfxRoutes": True,
            "routesLimit": 500,
            "sorting": [{"popularity": "DESC"}],
            "airlineCode": "h2",
        }

        logger.info("Sky Airline: calling Sputnik grouped-routes %s→%s", req.origin, req.destination)
        try:
            with creq.Session(impersonate="chrome136", proxies=get_curl_cffi_proxies()) as sess:
                resp = sess.post(_SPUTNIK_URL, json=payload, timeout=self.timeout, headers=_SPUTNIK_HEADERS)
            if resp.status_code != 200:
                logger.warning("Sky Airline Sputnik: HTTP %d", resp.status_code)
                return self._empty(req)
            routes_data = resp.json()
            if not isinstance(routes_data, list):
                return self._empty(req)
        except Exception as e:
            logger.error("Sky Airline Sputnik error: %s", e)
            return self._empty(req)

        # Flatten fares for the requested route from all route objects
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)
        fares: list[dict] = []
        for route_obj in routes_data:
            if route_obj.get("origin") in valid_origins and route_obj.get("destination") in valid_dests:
                fares.extend(route_obj.get("fares", []))

        if not fares:
            logger.info("Sky Airline: no fares for %s→%s in Sputnik response", req.origin, req.destination)
            return self._empty(req)

        offers = self._build_offers(fares, req)

        # RT: fetch reverse route inbound fares via Sputnik (same payload, swapped airports)
        if req.return_from and offers:
            try:
                ret_dt = req.return_from
                if not isinstance(ret_dt, datetime):
                    ret_dt = datetime(ret_dt.year, ret_dt.month, ret_dt.day)
                ret_start = (ret_dt - timedelta(days=3)).strftime("%Y-%m-%d")
                ret_end = (ret_dt + timedelta(days=30)).strftime("%Y-%m-%d")
                rev_payload = {**payload, "departure": {"start": ret_start, "end": ret_end}}
                with creq.Session(impersonate="chrome136", proxies=get_curl_cffi_proxies()) as sess:
                    _rev_resp = sess.post(_SPUTNIK_URL, json=rev_payload, timeout=self.timeout, headers=_SPUTNIK_HEADERS)
                if _rev_resp.status_code == 200:
                    _rev_routes = _rev_resp.json()
                    _ib_fares = []
                    for _route_obj in (_rev_routes if isinstance(_rev_routes, list) else []):
                        if _route_obj.get("origin") in valid_dests and _route_obj.get("destination") in valid_origins:
                            _ib_fares.extend(_route_obj.get("fares", []))
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
                        _ret_dt_val = ret_dt
                        _ib_seg = FlightSegment(
                            airline="H2",
                            airline_name="Sky Airline",
                            flight_no="",
                            origin=req.destination,
                            destination=req.origin,
                            departure=_ret_dt_val,
                            arrival=_ret_dt_val,
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

        # Sputnik fares use "origin"/"destination" (not "originAirportCode")
        # Separate exact-date and nearby fares
        exact_fares: list[dict] = []
        nearby_fares: list[dict] = []
        for fare in fares:
            if not fare.get("totalPrice") or float(fare.get("totalPrice", 0)) <= 0:
                continue
            if fare.get("departureDate", "")[:10] == target_date:
                exact_fares.append(fare)
            else:
                nearby_fares.append(fare)

        # Prefer exact-date fares; fall back to cheapest nearby fare as indicative price
        if exact_fares:
            use_fares = exact_fares
        elif nearby_fares:
            cheapest = min(nearby_fares, key=lambda f: float(f.get("totalPrice") or 999999))
            use_fares = [cheapest]
        else:
            use_fares = []

        for fare in use_fares:
            orig = fare.get("origin", "") or fare.get("originAirportCode", "")
            dest = fare.get("destination", "") or fare.get("destinationAirportCode", "")
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

            conditions = self._extract_fare_conditions(fare)

            offers.append(FlightOffer(
                id=f"h2_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Sky Airline"],
                owner_airline="H2",
                conditions=conditions,
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

    @staticmethod
    def _extract_fare_conditions(fare: dict) -> dict[str, str]:
        conditions: dict[str, str] = {}
        branded_fare = fare.get("brandedFareClass")
        if isinstance(branded_fare, str) and branded_fare.strip():
            fare_name = branded_fare.strip()
            conditions["fare_family"] = fare_name
            name_upper = fare_name.upper()
            # Sky Airline fare families: BASE/LIGHT=no bag, CLASS/ECONOMY=1 bag, FULL/FLEX=2 bags
            if any(k in name_upper for k in ("BASE", "LIGHT", "BASIC", "ZERO", "MINI", "SKY BASE")):
                conditions["checked_bag"] = "no free checked bag"
                conditions["carry_on"] = "no free overhead carry-on — add-on available at checkout"
            elif any(k in name_upper for k in ("FULL", "FLEX", "PLUS", "PREMIUM", "BUSINESS")):
                conditions["checked_bag"] = "2x 23kg bags included"
                conditions["carry_on"] = "1x 10kg carry-on included"
            elif any(k in name_upper for k in ("CLASS", "CLASSIC", "ECONOMY", "STANDARD", "SKY CLASS")):
                conditions["checked_bag"] = "1x 23kg bag included"
                conditions["carry_on"] = "1x 10kg carry-on included"
            else:
                conditions["carry_on"] = "carry-on policy depends on fare — check at checkout"
        else:
            conditions["fare_upgrade_note"] = "Sputnik fare calendar — base price only; ancillary prices not available"
            conditions["carry_on"] = "carry-on not included on base fare — add at checkout"
            conditions["checked_bag"] = "checked bag not included on base fare — add at checkout"
        conditions.setdefault("seat", "seat selection available at checkout")
        return conditions

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
