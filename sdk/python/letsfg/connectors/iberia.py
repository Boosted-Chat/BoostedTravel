"""
Iberia connector — LD+JSON fare data from cheap-flights pages via curl_cffi.

Iberia (IATA: IB) is the flag carrier of Spain.
Hub at Madrid-Barajas (MAD) with 130+ destinations worldwide.
Part of the IAG Group (with British Airways), oneworld alliance.

Strategy (curl_cffi required — Akamai protections):
  1. Determine market from origin airport (gb, es, us)
  2. Fetch overview page for that market: iberia.com/{market}/cheap-flights/
  3. Parse LD+JSON schema.org Flight entries for destination prices
  4. Return matching fare for requested destination
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

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
_BASE = "https://www.iberia.com"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Market configs: path suffix, default currency
_MARKET_PATHS = {
    "gb": "gb/cheap-flights/",
    "es": "es/vuelos-baratos/",
    "us": "us/cheap-flights/",
    "fr": "fr/vols-pas-cher/",
    "de": "de/billigfluege/",
    "it": "it/voli-economici/",
    "nl": "nl/goedkope-vluchten/",
}

# Map common IATA origin airports to Iberia market
_ORIGIN_TO_MARKET: dict[str, str] = {
    # UK
    "LHR": "gb", "LGW": "gb", "STN": "gb", "LTN": "gb", "SEN": "gb",
    "LCY": "gb", "MAN": "gb", "EDI": "gb", "GLA": "gb", "BHX": "gb",
    "BRS": "gb", "NCL": "gb", "ABZ": "gb", "CWL": "gb",
    # Spain
    "MAD": "es", "BCN": "es", "AGP": "es", "ALC": "es", "PMI": "es",
    "VLC": "es", "BIO": "es", "SVQ": "es", "TFS": "es", "LPA": "es",
    "SCQ": "es", "OVD": "es", "IBZ": "es", "MJV": "es",
    # US
    "JFK": "us", "EWR": "us", "LGA": "us", "LAX": "us", "SFO": "us",
    "ORD": "us", "MIA": "us", "BOS": "us", "IAD": "us", "DCA": "us",
    "DFW": "us", "ATL": "us", "IAH": "us",
    # France
    "CDG": "fr", "ORY": "fr", "NCE": "fr", "LYS": "fr",
    # Germany
    "FRA": "de", "MUC": "de", "BER": "de", "HAM": "de", "DUS": "de",
    # Italy
    "FCO": "it", "MXP": "it", "VCE": "it", "NAP": "it",
    # Netherlands
    "AMS": "nl",
}

# Map airport IATA to city code (for Iberia's city-code destinations)
_AIRPORT_TO_CITY: dict[str, str] = {
    "LHR": "LON", "LGW": "LON", "STN": "LON", "LTN": "LON",
    "LCY": "LON", "SEN": "LON",
    "JFK": "NYC", "EWR": "NYC", "LGA": "NYC",
    "ORD": "CHI", "MDW": "CHI",
    "EZE": "BUE", "AEP": "BUE",
    "NRT": "TYO", "HND": "TYO",
    "CDG": "PAR", "ORY": "PAR",
    "FRA": "FRA",
    "DFW": "DFW",
    "IAH": "HOU", "HOU": "HOU",
    "IAD": "WAS", "DCA": "WAS",
    "SVO": "MOW", "DME": "MOW", "VKO": "MOW",
    # Iberia uses city codes for these
    "TFS": "TCI", "TFN": "TCI",  # Tenerife
    "FCO": "ROM", "CIA": "ROM",  # Rome
    "MXP": "MIL", "LIN": "MIL",  # Milan
}

# Module-level fare cache: market -> {dest_iata: (price_f, currency_symbol, city_name)}
_fare_cache: dict[str, dict[str, tuple[float, str, str]]] = {}
_cache_ts: dict[str, float] = {}
_CACHE_TTL = 3600  # 1 hour


def _currency_symbol_to_code(sym: str) -> str:
    return {
        "£": "GBP", "\u00a3": "GBP",
        "€": "EUR", "\u20ac": "EUR",
        "$": "USD",
    }.get(sym, sym)


def _load_market_fares_sync(market: str) -> dict[str, tuple[float, str, str]]:
    """Fetch overview page and parse LD+JSON flights. Returns {dest_iata: (price, currency, city_name)}."""
    path = _MARKET_PATHS.get(market)
    if not path:
        return {}

    url = f"{_BASE}/{path}"
    sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
    try:
        r = sess.get(url, headers=_HEADERS, timeout=20)
        if r.status_code != 200:
            logger.warning("IB: %s returned %d", url, r.status_code)
            return {}
    except Exception as e:
        logger.warning("IB curl_cffi error for %s: %s", url, e)
        return {}

    scripts = re.findall(
        r'<script type="application/ld\+json">(.*?)</script>', r.text, re.S
    )
    fares: dict[str, tuple[float, str, str]] = {}
    seen: set[str] = set()

    for s in scripts:
        try:
            d = json.loads(s)
        except (json.JSONDecodeError, ValueError):
            continue

        if not isinstance(d, dict) or d.get("@type") != "Flight":
            continue

        arr = d.get("arrivalAirport", {})
        dest_iata = arr.get("iataCode", "")
        dest_name = arr.get("name", "")
        offers = d.get("offers", {})
        price_str = offers.get("price", "")
        currency_sym = offers.get("priceCurrency", "")

        if not dest_iata or not price_str or dest_iata in seen:
            continue
        seen.add(dest_iata)

        try:
            price_f = round(float(price_str), 2)
        except (ValueError, TypeError):
            continue

        if price_f <= 0:
            continue

        currency = _currency_symbol_to_code(currency_sym)
        fares[dest_iata] = (price_f, currency, dest_name)

    logger.info("IB: loaded %d fares from %s market", len(fares), market)
    return fares


def _get_cached_fares(market: str) -> dict[str, tuple[float, str, str]]:
    """Get fares from cache, refreshing if stale."""
    now = time.time()
    if market in _fare_cache and (now - _cache_ts.get(market, 0)) < _CACHE_TTL:
        return _fare_cache[market]
    fares = _load_market_fares_sync(market)
    _fare_cache[market] = fares
    _cache_ts[market] = now
    return fares


class IberiaConnectorClient:
    """Iberia — LD+JSON cheap-flights pages via curl_cffi."""

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
        # Iberia Classic: 1×23 kg included. Basic: no bag.
        return {
            "bags_note": "Economy Basic (cheapest): no checked bag, first bag from EUR 30. Economy Classic/Flex: 1×23 kg included. Carry-on included.",
            "seat_note": "Seat selection: from EUR 12 (standard). Included on Classic/Flex fares.",
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
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["carry_on"] = bags_from
            if checked_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["checked_bag"] = checked_from

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        market = _ORIGIN_TO_MARKET.get(req.origin, "gb")

        try:
            fares = await asyncio.get_event_loop().run_in_executor(
                None, _get_cached_fares, market
            )
        except Exception as e:
            logger.error("IB fare load error: %s", e)
            fares = {}

        # Try exact IATA match first, then city code
        fare = fares.get(req.destination)
        if not fare:
            city_code = _AIRPORT_TO_CITY.get(req.destination)
            if city_code:
                fare = fares.get(city_code)

        # Fallback to /gb market if primary market had no data
        if not fare and market != "gb":
            try:
                gb_fares = await asyncio.get_event_loop().run_in_executor(
                    None, _get_cached_fares, "gb"
                )
            except Exception:
                gb_fares = {}
            fare = gb_fares.get(req.destination)
            if not fare:
                city_code = _AIRPORT_TO_CITY.get(req.destination)
                if city_code:
                    fare = gb_fares.get(city_code)

        if not fare:
            logger.info("IB: no fare for %s->%s in %s market", req.origin, req.destination, market)
            return self._empty(req)

        price_f, currency, dest_name = fare

        # RT: fetch reverse fare (dest market → origin) for inbound
        _ib_route: FlightRoute | None = None
        _ib_price = 0.0
        if req.return_from:
            rev_market = _ORIGIN_TO_MARKET.get(req.destination, "gb")
            try:
                rev_fares = await asyncio.get_event_loop().run_in_executor(
                    None, _get_cached_fares, rev_market
                )
            except Exception:
                rev_fares = {}
            rev_fare = rev_fares.get(req.origin)
            if not rev_fare:
                origin_city = _AIRPORT_TO_CITY.get(req.origin)
                if origin_city:
                    rev_fare = rev_fares.get(origin_city)
            # Fallback to gb market reverse
            if not rev_fare and rev_market != "gb":
                try:
                    rev_gb = await asyncio.get_event_loop().run_in_executor(
                        None, _get_cached_fares, "gb"
                    )
                except Exception:
                    rev_gb = {}
                rev_fare = rev_gb.get(req.origin)
                if not rev_fare:
                    origin_city = _AIRPORT_TO_CITY.get(req.origin)
                    if origin_city:
                        rev_fare = rev_gb.get(origin_city)
            if rev_fare:
                ib_price_f, ib_curr, ib_name = rev_fare
                _ib_price = ib_price_f
                ret_dt = datetime.combine(req.return_from, datetime.min.time()) if hasattr(req.return_from, 'year') else datetime(2000, 1, 1)
                _ib_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                _ib_route = FlightRoute(
                    segments=[FlightSegment(
                        airline="IB", airline_name="Iberia", flight_no="",
                        origin=req.destination, destination=req.origin,
                        departure=ret_dt, arrival=ret_dt,
                        duration_seconds=0, cabin_class=_ib_cabin,
                    )],
                    total_duration_seconds=0, stopovers=0,
                )

        offer = self._build_offer(price_f, currency, dest_name, req, _ib_route, _ib_price)

        elapsed = time.monotonic() - t0
        logger.info("IB %s→%s: %.2f %s in %.1fs", req.origin, req.destination, price_f, currency, elapsed)

        h = hashlib.md5(
            f"ib{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=[offer],
            total_results=1,
        )

    def _build_offer(
        self,
        price: float,
        currency: str,
        dest_name: str,
        req: FlightSearchRequest,
        _ib_route: FlightRoute | None = None,
        _ib_price: float = 0.0,
    ) -> FlightOffer:
        target_date = req.date_from.strftime("%Y-%m-%d")
        dep_dt = datetime.combine(req.date_from, datetime.min.time())
        _ib_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")

        seg = FlightSegment(
            airline="IB",
            airline_name="Iberia",
            flight_no="",
            origin=req.origin,
            destination=req.destination,
            origin_city="",
            destination_city=dest_name,
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=_ib_cabin,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        is_rt = _ib_route is not None
        total_price = round(price + _ib_price, 2) if is_rt else price
        prefix = "ib_rt_" if is_rt else "ib_"

        fid = hashlib.md5(
            f"ib_{req.origin}{req.destination}{total_price}{currency}".encode()
        ).hexdigest()[:12]

        fmt_map = {"GBP": "£", "EUR": "€", "USD": "$"}
        sym = fmt_map.get(currency, currency)

        burl = (
            f"https://www.iberia.com/gb/flights/"
            f"?market=gb&language=en"
            f"&origin={req.origin}&destination={req.destination}"
            f"&outbound={target_date}"
            f"&adults={req.adults or 1}"
        )
        if is_rt and req.return_from:
            ret_str = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, 'strftime') else str(req.return_from)
            burl += f"&inbound={ret_str}"

        return FlightOffer(
            id=f"{prefix}{fid}",
            price=total_price,
            currency=currency,
            price_formatted=f"{sym}{total_price:.0f}",
            outbound=route,
            inbound=_ib_route,
            airlines=["Iberia"],
            owner_airline="IB",
            booking_url=burl,
            is_locked=False,
            source="iberia_direct",
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
                    id=f"rt_iber_{cid}", price=price, currency=o.currency,
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
            f"ib{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
