"""
TAP Air Portugal connector — EveryMundo airTRFX Sputnik API + fare pages.

TAP Air Portugal (IATA: TP) is Portugal's flag carrier. Star Alliance member.
Key hub at LIS connecting Europe, Brazil, Africa, Americas.
90+ destinations. Strong on CPLP countries (Portuguese-speaking).

Strategy:
  Primary: EveryMundo Sputnik grouped-routes API (httpx)
  Fallback: curl_cffi route page with __NEXT_DATA__ extraction
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional

import httpx
from curl_cffi import requests as creq

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_curl_cffi_proxies, get_httpx_proxy_url
from .airline_routes import get_city_airports, resolve_slug, city_match_set

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
_BASE = "https://www.flytap.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_API_URL = "https://openair-california.airtrfx.com/airfare-sputnik-service/v3/tp/fares/grouped-routes"
_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_SPUTNIK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://mm-prerendering-static-prod.airtrfx.com",
    "Referer": "https://mm-prerendering-static-prod.airtrfx.com/",
    "em-api-key": _API_KEY,
}

_MARKETS = ["PT", "BR", "US", "GB", "FR", "DE"]

_IATA_TO_SLUG: dict[str, str] = {
    # Portugal
    "LIS": "lisbon", "OPO": "porto", "FAO": "faro",
    "FNC": "funchal", "PDL": "ponta-delgada",
    # Europe — city codes + airport codes
    "LON": "london", "LHR": "london", "LGW": "london",
    "PAR": "paris", "CDG": "paris",
    "ORY": "paris", "FRA": "frankfurt", "MUC": "munich",
    "AMS": "amsterdam", "BRU": "brussels", "ZRH": "zurich",
    "GVA": "geneva", "ROM": "rome", "FCO": "rome", "MXP": "milan",
    "BCN": "barcelona", "MAD": "madrid", "AGP": "malaga",
    "BER": "berlin", "DUS": "dusseldorf", "HAM": "hamburg",
    "VIE": "vienna", "CPH": "copenhagen", "ARN": "stockholm",
    "OSL": "oslo", "HEL": "helsinki", "WAW": "warsaw",
    "PRG": "prague", "BUD": "budapest", "DUB": "dublin",
    "MAN": "manchester", "EDI": "edinburgh", "ATH": "athens",
    "IST": "istanbul", "LCA": "larnaca",
    # Brazil
    "GRU": "sao-paulo", "GIG": "rio-de-janeiro",
    "BSB": "brasilia", "CNF": "belo-horizonte",
    "SSA": "salvador", "REC": "recife", "FOR": "fortaleza",
    "POA": "porto-alegre", "CWB": "curitiba", "BEL": "belem",
    "NAT": "natal", "MCP": "macapa",
    # Africa
    "CMN": "casablanca", "RAK": "marrakech",
    "ACC": "accra", "LOS": "lagos", "MPM": "maputo",
    "DSS": "dakar", "ABJ": "abidjan",
    "LAD": "luanda", "PRN": "pristina",
    # Americas
    "NYC": "new-york", "EWR": "newark", "JFK": "new-york", "BOS": "boston",
    "MIA": "miami", "IAD": "washington-dc", "SFO": "san-francisco",
    "YYZ": "toronto", "YUL": "montreal",
    "CUN": "cancun", "BOG": "bogota",
}


class TapConnectorClient:
    """TAP Air Portugal — EveryMundo Sputnik API + airTRFX fare pages."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self):
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_SPUTNIK_HEADERS,
                follow_redirects=True, proxy=get_httpx_proxy_url(),
            )
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
        if ob_result.offers:
            segs = ob_result.offers[0].outbound.segments if ob_result.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, ob_result.currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        # TAP Discount: no bag. Classic/Executive: 1×23 kg included.
        return {
            "bags_note": "Economy Discount (cheapest): no checked bag, first bag from EUR 20. Economy Classic/Executive: 1×23 kg included. Carry-on included.",
            "seat_note": "Seat selection: from EUR 12 (Discount). Included on Classic/Executive fares.",
            "bags_from": None,
            "currency": currency,
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_price")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from == 0.0:
                offer.bags_price["carry_on"] = 0.0
            if checked_from == 0.0:
                offer.bags_price["checked_bag"] = 0.0

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # Primary: Sputnik grouped-routes API
        offers = await self._try_sputnik(req)

        # Fallback: HTML route page (__NEXT_DATA__)
        if not offers:
            origin_slug = resolve_slug(req.origin, _IATA_TO_SLUG)
            dest_slug = resolve_slug(req.destination, _IATA_TO_SLUG)
            if origin_slug and dest_slug:
                url = f"{_BASE}/flights/en-pt/flights-from-{origin_slug}-to-{dest_slug}"
                logger.info("TAP: Sputnik empty, falling back to HTML %s", url)
                try:
                    html = await asyncio.get_event_loop().run_in_executor(
                        None, self._fetch_sync, url
                    )
                except Exception as e:
                    logger.error("TAP fetch error: %s", e)
                    html = None
                if html:
                    fares = self._extract_fares(html)
                    if fares:
                        # RT: fetch reverse route page for IB fares
                        ib_fares: list[dict] = []
                        if req.return_from:
                            ib_url = f"{_BASE}/flights/en-pt/flights-from-{dest_slug}-to-{origin_slug}"
                            logger.info("TAP: fetching IB fares %s", ib_url)
                            try:
                                ib_html = await asyncio.get_event_loop().run_in_executor(
                                    None, self._fetch_sync, ib_url
                                )
                            except Exception as ibe:
                                logger.warning("TAP IB fetch error: %s", ibe)
                                ib_html = None
                            if ib_html:
                                ib_fares = self._extract_fares(ib_html)
                        offers = self._build_offers(fares, req, ib_fares=ib_fares)

        if not offers:
            offers = []
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("TAP %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"tap{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    async def _try_sputnik(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Try Sputnik grouped-routes API for TAP fares."""
        try:
            dt = req.date_from
            if isinstance(dt, datetime):
                dt = dt.date()
            elif not isinstance(dt, date):
                dt = datetime.strptime(str(dt), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            dt = date.today() + timedelta(days=30)

        start = dt - timedelta(days=3)
        end = dt + timedelta(days=30)

        payload = {
            "markets": _MARKETS,
            "languageCode": "en",
            "dataExpirationWindow": "7d",
            "datePattern": "dd MMM yy (E)",
            "outputCurrencies": ["EUR"],
            "departure": {"start": start.isoformat(), "end": end.isoformat()},
            "budget": {"maximum": None},
            "passengers": {"adults": max(1, req.adults or 1)},
            "travelClasses": [{"M": "ECONOMY", "W": "PREMIUM_ECONOMY", "C": "BUSINESS", "F": "FIRST"}.get(req.cabin_class or "M", "ECONOMY")],
            "flightType": "ROUND_TRIP" if req.return_from else "ONE_WAY",
            "flexibleDates": True,
            "faresPerRoute": "10",
            "trfxRoutes": True,
            "routesLimit": 500,
            "sorting": [{"popularity": "DESC"}],
            "airlineCode": "tp",
        }

        try:
            client = await self._client()
            r = await client.post(_API_URL, json=payload)
            if r.status_code != 200:
                logger.info("TAP Sputnik: HTTP %d", r.status_code)
                return []
            data = r.json()
            if not isinstance(data, list):
                return []
        except Exception as e:
            logger.info("TAP Sputnik error: %s", e)
            return []

        origin_set = city_match_set(req.origin)
        dest_set = city_match_set(req.destination)

        offers = []
        for route in data:
            for fare in route.get("fares") or []:
                orig = (fare.get("originAirportCode") or route.get("origin") or "").upper()
                dest = (fare.get("destinationAirportCode") or route.get("destination") or "").upper()
                # Match either: strict (orig in origin_set AND dest in dest_set)
                # or hub-based (dest in dest_set only — TAP is LIS hub)
                if dest not in dest_set:
                    if orig not in origin_set:
                        continue

                price = fare.get("totalPrice") or fare.get("usdTotalPrice")
                if not price or float(price) <= 0:
                    continue
                if fare.get("redemption"):
                    continue

                price_f = round(float(price), 2)
                currency = fare.get("currencyCode") or "EUR"
                dep_str = (fare.get("departureDate") or "")[:10]
                ret_str = (fare.get("returnDate") or "")[:10]
                cabin = (fare.get("farenetTravelClass") or "ECONOMY").lower()

                dep_dt = datetime(2000, 1, 1)
                if dep_str:
                    try:
                        dep_dt = datetime.strptime(dep_str, "%Y-%m-%d")
                    except ValueError:
                        pass

                seg = FlightSegment(
                    airline="TP", airline_name="TAP Air Portugal", flight_no="",
                    origin=orig, destination=dest,
                    origin_city=fare.get("originCity") or "",
                    destination_city=fare.get("destinationCity") or "",
                    departure=dep_dt, arrival=dep_dt,
                    duration_seconds=0, cabin_class=cabin,
                )
                outbound = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

                inbound = None
                if ret_str:
                    try:
                        ret_dt = datetime.strptime(ret_str, "%Y-%m-%d")
                    except ValueError:
                        ret_dt = dep_dt
                    ret_seg = FlightSegment(
                        airline="TP", airline_name="TAP Air Portugal", flight_no="",
                        origin=dest, destination=orig,
                        origin_city=fare.get("destinationCity") or "",
                        destination_city=fare.get("originCity") or "",
                        departure=ret_dt, arrival=ret_dt,
                        duration_seconds=0, cabin_class=cabin,
                    )
                    inbound = FlightRoute(segments=[ret_seg], total_duration_seconds=0, stopovers=0)

                ret_token = f"_{ret_str}" if ret_str else ""
                fid = hashlib.md5(
                    f"tp_{orig}_{dest}_{dep_str}{ret_token}_{price_f}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"tp_{fid}",
                    price=price_f,
                    currency=currency,
                    price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                    outbound=outbound,
                    inbound=inbound,
                    airlines=["TAP Air Portugal"],
                    owner_airline="TP",
                    booking_url=f"{_BASE}/booking/flights",
                    is_locked=False,
                    source="tap_direct",
                    source_tier="free",
                    conditions={
                        "trip_type": (fare.get("flightType") or "ROUND_TRIP").lower().replace("_", "-"),
                        "cabin": str(fare.get("formattedTravelClass") or cabin),
                        "fare_note": "Published fare from TAP Air Portugal fare module",
                    },
                ))

        logger.info("TAP Sputnik %s→%s: %d offers", req.origin, req.destination, len(offers))
        return offers

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("TAP: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("TAP curl_cffi error: %s", e)
            return None

    @staticmethod
    def _extract_fares(html: str) -> list[dict]:
        """Extract all fares from ALL StandardFareModules in __NEXT_DATA__."""
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html, re.S,
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

    def _build_offers(self, fares: list[dict], req: FlightSearchRequest, *, ib_fares: list[dict] | None = None) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        ret_date = req.return_from.strftime("%Y-%m-%d") if req.return_from else None
        offers: list[FlightOffer] = []

        # City-aware matching: LON matches LHR, LGW, STN, etc.
        valid_origins = set(get_city_airports(req.origin))
        valid_origins.add(req.origin)
        valid_dests = set(get_city_airports(req.destination))
        valid_dests.add(req.destination)

        # First pass: exact date. Second pass: any date (±30 days).
        matched_fares: list[dict] = []
        fallback_fares: list[dict] = []

        for fare in fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig not in valid_origins or dest not in valid_dests:
                continue
            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue
            dep_date = fare.get("departureDate", "")
            if dep_date[:10] == target_date:
                matched_fares.append(fare)
            else:
                fallback_fares.append(fare)

        # Only use exact-date fares — never fall back to wrong-date fares
        use_fares = matched_fares

        for fare in use_fares:
            dep_date = fare.get("departureDate", "")
            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            currency = fare.get("currencyCode") or "EUR"
            price_f = round(float(price), 2)

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            cabin = (fare.get("formattedTravelClass") or "Economy").lower()
            seg = FlightSegment(
                airline="TP",
                airline_name="TAP Air Portugal",
                flight_no="",
                origin=orig,
                destination=dest,
                origin_city=fare.get("originCity", ""),
                destination_city=fare.get("destinationCity", ""),
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            # ── IB route from reverse-page fares ──
            _ib_route = None
            _ib_price = 0.0
            if ret_date and ib_fares:
                best_ib = None
                best_ib_exact = None
                for ibf in ib_fares:
                    ibp = ibf.get("totalPrice")
                    if not ibp or float(ibp) <= 0:
                        continue
                    ib_dep = (ibf.get("departureDate") or "")[:10]
                    if ib_dep == ret_date:
                        if best_ib_exact is None or float(ibp) < float(best_ib_exact.get("totalPrice", 9e9)):
                            best_ib_exact = ibf
                    if best_ib is None or float(ibp) < float(best_ib.get("totalPrice", 9e9)):
                        best_ib = ibf
                chosen_ib = best_ib_exact or best_ib
                if chosen_ib:
                    _ib_price = round(float(chosen_ib["totalPrice"]), 2)
                    ib_dep_str = (chosen_ib.get("departureDate") or ret_date)[:10]
                    try:
                        ib_dt = datetime.strptime(ib_dep_str, "%Y-%m-%d")
                    except ValueError:
                        ib_dt = datetime(2000, 1, 1)
                    ib_seg = FlightSegment(
                        airline="TP", airline_name="TAP Air Portugal", flight_no="",
                        origin=req.destination, destination=req.origin,
                        departure=ib_dt, arrival=ib_dt,
                        duration_seconds=0, cabin_class=cabin,
                    )
                    _ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)

            total_price = round(price_f + _ib_price, 2) if _ib_route else price_f
            id_prefix = "tp_rt_" if _ib_route else "tp_"

            fid = hashlib.md5(
                f"tp_{orig}{dest}{dep_date}{total_price}{cabin}{ret_date or ''}".encode()
            ).hexdigest()[:12]

            is_exact_date = dep_date[:10] == target_date
            conditions = {}
            if not is_exact_date:
                conditions["price_type"] = "nearby_date"
                conditions["fare_date"] = dep_date[:10]

            bk_url = (
                f"https://www.flytap.com/en-us/booking"
                f"?origin={req.origin}&destination={req.destination}"
                f"&date={target_date}&adults={req.adults or 1}"
            )
            if _ib_route and ret_date:
                bk_url += f"&type=RT&return={ret_date}"
            else:
                bk_url += "&type=OW"

            offers.append(FlightOffer(
                id=f"{id_prefix}{fid}",
                price=total_price,
                currency=currency,
                price_formatted=f"{total_price:.2f} {currency}",
                outbound=route,
                inbound=_ib_route,
                airlines=["TAP Air Portugal"],
                owner_airline="TP",
                conditions=conditions,
                booking_url=bk_url,
                is_locked=False,
                source="tap_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"tap{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
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
                    id=f"rt_tap_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
