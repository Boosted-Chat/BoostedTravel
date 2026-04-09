"""
Air Niugini connector — EveryMundo airTRFX fare pages via curl_cffi.

Air Niugini (IATA: PX) is the flag carrier of Papua New Guinea, based at
Jacksons International Airport (POM) in Port Moresby. International network
covers SYD, BNE, CNS (Australia), SIN, HKG, NRT, MNL, NAN, HIR plus
domestic PNG destinations.

Strategy (curl_cffi — same EveryMundo pattern as Rex, Fiji Airways):
  1. Resolve IATA codes to city slugs via static mapping
  2. Fetch route page: www.airniugini.com.pg/flights/en/flights-from-{origin}-to-{dest}
  3. Extract __NEXT_DATA__ JSON from <script> tag
  4. Parse DpaHeadline + StandardFareModule from apolloState.data → fare offers
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime

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

_BASE = "https://www.airniugini.com.pg/flights"
_SITE_EDITION = "en"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# IATA → city slug mapping for Air Niugini routes
_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "TYO": "tokyo",
    # PNG domestic
    "POM": "port-moresby",
    "LAE": "lae",
    "GKA": "goroka",
    "HGU": "mount-hagen",
    "MDG": "madang",
    "RAB": "rabaul",
    "WWK": "wewak",
    "MAS": "manus",
    "BUA": "buka",
    "KVG": "kavieng",
    "TBG": "tabubil",
    "DAU": "daru",
    "GUR": "gurney",
    "PNP": "popondetta",
    "KRI": "kikori",
    "TIZ": "tari",
    # Australia
    "SYD": "sydney",
    "BNE": "brisbane",
    "CNS": "cairns",
    # Asia
    "SIN": "singapore",
    "HKG": "hong-kong",
    "NRT": "tokyo",
    "MNL": "manila",
    "KUL": "kuala-lumpur",
    # Pacific
    "NAN": "nadi",
    "HIR": "honiara",
    "SUV": "suva",
    "VLI": "port-vila",
}


class AirNiuginiConnectorClient:
    """Air Niugini (PX) — EveryMundo airTRFX route pages via curl_cffi."""

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
            logger.warning("AirNiugini: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/{_SITE_EDITION}/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("AirNiugini: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("AirNiugini fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        offers = self._extract_offers(html, req)

        # RT: fetch reverse route for inbound fares
        if req.return_from and offers:
            _rev_url = f"{_BASE}/{_SITE_EDITION}/flights-from-{dest_slug}-to-{origin_slug}"
            try:
                _rev_html = await asyncio.get_event_loop().run_in_executor(
                    None, self._fetch_sync, _rev_url
                )
                if _rev_html:
                    _ib_offers = self._extract_offers(_rev_html, req)
                    _ib_valid = [o for o in _ib_offers if o.price > 0]
                    if _ib_valid:
                        _ib_best = min(_ib_valid, key=lambda o: o.price)
                        _ret = req.return_from
                        _ret_dt = datetime.combine(_ret, datetime.min.time()) if not isinstance(_ret, datetime) else _ret
                        _ib_seg = FlightSegment(
                            airline="PX",
                            airline_name="Air Niugini",
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
                                price=round(_o.price + _ib_best.price, 2),
                                currency=_o.currency,
                                price_formatted=f"{round(_o.price + _ib_best.price, 2):.2f} {_o.currency}",
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
        _td = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
        exact = [o for o in offers if o.outbound and o.outbound.segments and o.outbound.segments[0].departure.date() == _td]
        offers = exact if exact else offers
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "AirNiugini %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"airniugini{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "PGK",
            offers=offers,
            total_results=len(offers),
        )

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("AirNiugini: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("AirNiugini curl_cffi error: %s", e)
            return None

    def _extract_offers(
        self, html: str, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
            re.S,
        )
        if not m:
            logger.info("AirNiugini: no __NEXT_DATA__ found")
            return []

        try:
            nd = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            logger.warning("AirNiugini: __NEXT_DATA__ JSON parse failed")
            return []

        props = nd.get("props", {}).get("pageProps", {})
        apollo = props.get("apolloState", {}).get("data", {})

        offers: list[FlightOffer] = []
        seen: set[str] = set()

        # 1) DpaHeadline → lowestFare
        for key, val in apollo.items():
            if not isinstance(val, dict) or val.get("__typename") != "DpaHeadline":
                continue
            meta = val.get("metaData", {})
            if not isinstance(meta, dict):
                continue
            headline = meta.get("headline", {})
            if not isinstance(headline, dict):
                continue
            lf = headline.get("lowestFare", {})
            if isinstance(lf, dict) and "__ref" in lf:
                lf = apollo.get(lf["__ref"], {})
            if isinstance(lf, dict):
                offer = self._build_offer_from_fare(lf, req, seen)
                if offer:
                    offers.append(offer)

        # 2) StandardFareModule → fares
        for key, val in apollo.items():
            if not isinstance(val, dict) or val.get("__typename") != "StandardFareModule":
                continue
            fares_raw = val.get("fares", [])
            if not isinstance(fares_raw, list):
                continue
            for fare_ref in fares_raw:
                fare = fare_ref
                if isinstance(fare_ref, dict) and "__ref" in fare_ref:
                    fare = apollo.get(fare_ref["__ref"], {})
                if isinstance(fare, dict):
                    offer = self._build_offer_from_fare(fare, req, seen)
                    if offer:
                        offers.append(offer)

        return offers

    def _build_offer_from_fare(
        self,
        fare: dict,
        req: FlightSearchRequest,
        seen: set[str],
    ) -> FlightOffer | None:
        price = fare.get("usdTotalPrice") or fare.get("totalPrice")
        if not price:
            return None
        try:
            price_f = round(float(price), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        dep_date_str = fare.get("departureDate", "")[:10]
        if not dep_date_str:
            return None

        if fare.get("usdTotalPrice"):
            currency = "USD"
        else:
            currency = fare.get("currencyCode") or "PGK"

        origin_code = fare.get("originAirportCode") or req.origin
        dest_code = fare.get("destinationAirportCode") or req.destination
        cabin = (fare.get("formattedTravelClass") or "Economy").strip()

        dedup_key = f"{origin_code}_{dest_code}_{dep_date_str}_{price_f}_{cabin}"
        if dedup_key in seen:
            return None
        seen.add(dedup_key)

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            dep_dt = datetime(2000, 1, 1)

        seg = FlightSegment(
            airline="PX",
            airline_name="Air Niugini",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city=fare.get("originCity", ""),
            destination_city=fare.get("destinationCity", ""),
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin.lower(),
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        fid = hashlib.md5(
            f"px_{origin_code}{dest_code}{dep_date_str}{price_f}{cabin}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"px_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Air Niugini"],
            owner_airline="PX",
            booking_url=(
                f"https://www.airniugini.com.pg"
                f"?origin={req.origin}&destination={req.destination}"
                f"&departureDate={dep_date_str}"
                f"&adults={req.adults or 1}&tripType={'ROUND_TRIP' if req.return_from else 'ONE_WAY'}"
                + (f"&inboundDate={req.return_from.strftime('%Y-%m-%d')}" if req.return_from else "")
            ),
            is_locked=False,
            source="airniugini_direct",
            source_tier="free",
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
                    id=f"rt_airn_{cid}", price=price, currency=o.currency,
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
            f"airniugini{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="PGK",
            offers=[],
            total_results=0,
        )
