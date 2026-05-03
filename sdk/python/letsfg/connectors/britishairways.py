"""
British Airways connector — SOLR pricing feed via curl_cffi.

British Airways (IATA: BA) is the flag carrier of the United Kingdom.
Main hubs at London Heathrow (LHR), London Gatwick (LGW), and London City (LCY).
Member of oneworld alliance, part of International Airlines Group (IAG).

Strategy (curl_cffi required — Akamai WAF bypass):
  1. Query BA's SOLR pricing endpoint: /solr/lpbd/safe
  2. SOLR contains lowest-fare data grouped by month for each route
  3. Filter results by departure date
  4. Return pricing data with route/airport details

SOLR config discovered from clientlib-site.min.js:
  {buildUrl: "/solr/", core: "lpbd", handler: "safe", rows: 12}
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
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
_BASE = "https://www.britishairways.com"
_SOLR_PATH = "/solr/lpbd/safe"
_HEADERS = {
    "Accept": "application/json, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Content-Type": "application/json",
    "Referer": "https://www.britishairways.com/en-gb/offers",
}

# BA SOLR uses city codes (not IATA airport codes) for departure/arrival.
# Map common UK departure IATA codes to BA city codes.
_IATA_TO_BA_CITY: dict[str, str] = {
    # UK departures (BA's primary market)
    "LHR": "LON", "LGW": "LON", "LCY": "LON", "STN": "LON", "LTN": "LON",
    "MAN": "MAN", "EDI": "EDI", "GLA": "GLA", "BHX": "BHX",
    "BRS": "BRS", "NCL": "NCL", "ABZ": "ABZ", "BFS": "BFS",
    # European city codes
    "CDG": "PAR", "ORY": "PAR",
    "FCO": "ROM", "CIA": "ROM",
    "JFK": "NYC", "EWR": "NYC", "LGA": "NYC",
    "LAX": "LAX", "SFO": "SFO", "ORD": "CHI",
    "IAD": "WAS", "DCA": "WAS",
    "NRT": "TYO", "HND": "TYO",
}

# Currency symbols for formatting
_CURRENCY_SYMBOLS: dict[str, str] = {
    "GBP": "£", "EUR": "€", "USD": "$", "AUD": "AUD",
    "INR": "₹", "JPY": "¥",
}


def _iata_to_city(iata: str) -> str:
    """Convert IATA airport code to BA city code. Falls back to IATA itself."""
    return _IATA_TO_BA_CITY.get(iata, iata)


class BritishAirwaysConnectorClient:
    """British Airways — SOLR pricing feed via curl_cffi."""

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
        # BA: Euro Traveller includes 1 bag; Basic has cabin bag only.
        return {
            "bags_note": "Euro Traveller: 1×23 kg included. Basic fare (cheapest): cabin bag only, first checked bag from GBP 30.",
            "seat_note": "Seat selection: Choose Your Seat from GBP 7 (standard). Extra-legroom from GBP 25.",
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

        dep_city = _iata_to_city(req.origin)
        arr_city = _iata_to_city(req.destination)

        offers = await self._query_solr(dep_city, arr_city, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "BA %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"ba{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "GBP",
            offers=offers,
            total_results=len(offers),
        )

    async def _query_solr(
        self, dep_city: str, arr_city: str, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Query BA SOLR for pricing data (one-way or round-trip)."""
        # Narrow to ±7 days around the requested date so we get date-specific fares
        # instead of cheapest-across-all-months calendar data.
        target = req.date_from if isinstance(req.date_from, date) else req.date_from.date()
        start_dt = target - timedelta(days=1)
        end_dt = target + timedelta(days=7)
        start_iso = f"{start_dt}T00:00:00Z"
        end_iso = f"{end_dt}T23:59:59Z"

        is_rt = bool(req.return_from)
        trip_type = "RT" if is_rt else "OW"
        _ba_cabin = req.cabin_class or "M"  # BA SOLR: M/W/C/F

        fq = (
            f"departure_city:{dep_city}+AND+"
            f"arrival_city:{arr_city}+AND+"
            f"trip_type:{trip_type}+AND+"
            f"cabin:{_ba_cabin}+AND+"
            f"outbound_date:[{start_iso}+TO+{end_iso}]"
        )
        if is_rt and req.return_from:
            ret_target = req.return_from if isinstance(req.return_from, date) else req.return_from.date()
            ret_start = f"{ret_target - timedelta(days=1)}T00:00:00Z"
            ret_end = f"{ret_target + timedelta(days=7)}T23:59:59Z"
            fq += f"+AND+inbound_date:[{ret_start}+TO+{ret_end}]"

        url = (
            f"{_BASE}{_SOLR_PATH}?fq={fq}"
            f"&rows=50"
            f"&locale=en_GB"
            f"&sort=lowest_price%20asc"
            f"&wt=json"
        )

        logger.info("BA SOLR: querying %s→%s", dep_city, arr_city)

        try:
            data = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_solr_sync, url,
            )
        except Exception as e:
            logger.error("BA SOLR error: %s", e)
            return []

        if not data:
            return []

        offers = self._parse_solr_docs(data, req)

        # If round-trip query returned no results, fall back to one-way
        if not offers and is_rt:
            logger.info("BA SOLR: no RT results, falling back to OW")
            fq_ow = fq.replace(f"trip_type:{trip_type}", "trip_type:OW")
            # Remove inbound_date filter for OW fallback
            if "+AND+inbound_date:" in fq_ow:
                fq_ow = fq_ow[:fq_ow.index("+AND+inbound_date:")]
            url_ow = (
                f"{_BASE}{_SOLR_PATH}?fq={fq_ow}"
                f"&rows=50"
                f"&locale=en_GB"
                f"&sort=lowest_price%20asc"
                f"&wt=json"
            )
            try:
                data_ow = await asyncio.get_event_loop().run_in_executor(
                    None, self._fetch_solr_sync, url_ow,
                )
                if data_ow:
                    offers = self._parse_solr_docs(data_ow, req)
            except Exception:
                pass

        return offers

    def _fetch_solr_sync(self, url: str) -> dict | None:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("BA SOLR: returned %d", r.status_code)
                return None
            return r.json()
        except Exception as e:
            logger.warning("BA SOLR curl_cffi error: %s", e)
            return None

    def _parse_solr_docs(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Parse SOLR response docs into FlightOffer list."""
        docs = data.get("response", {}).get("docs", [])
        if not docs:
            return []

        _ba_cabin = req.cabin_class or "M"
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []

        for doc in docs:
            price = doc.get("lowest_price")
            if not price:
                continue
            try:
                price_f = round(float(price), 2)
            except (ValueError, TypeError):
                continue
            if price_f <= 0:
                continue

            currency = (doc.get("currency_code") or "GBP").strip()
            dep_airport = doc.get("departure_airport") or req.origin
            arr_airport = doc.get("arrival_airport") or req.destination
            dep_city_name = doc.get("dep_city_name") or ""
            arr_city_name = doc.get("arr_city_name") or ""
            outbound_date_str = (doc.get("outbound_date") or "")[:10]

            if not outbound_date_str:
                continue

            try:
                dep_dt = datetime.strptime(outbound_date_str, "%Y-%m-%d")
            except ValueError:
                continue

            # Filter: only include offers on or after the requested date
            req_date = req.date_from if isinstance(req.date_from, date) else req.date_from.date()
            if dep_dt.date() < req_date:
                continue

            journey_time = doc.get("journey_time") or 0
            is_direct = doc.get("route_connection_ind", "N") == "N"

            sym = _CURRENCY_SYMBOLS.get(currency, currency + " ")
            price_fmt = f"{sym}{price_f:.0f}" if price_f == int(price_f) else f"{sym}{price_f:.2f}"

            seg = FlightSegment(
                airline="BA",
                airline_name="British Airways",
                flight_no="",
                origin=dep_airport,
                destination=arr_airport,
                origin_city=dep_city_name,
                destination_city=arr_city_name,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=int(journey_time) * 60 if journey_time else 0,
                cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(_ba_cabin, "economy"),
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=int(journey_time) * 60 if journey_time else 0,
                stopovers=0 if is_direct else 1,
            )

            # Parse inbound date for RT offers
            inbound = None
            inbound_date_str = (doc.get("inbound_date") or "")[:10]
            if inbound_date_str:
                try:
                    ret_dt = datetime.strptime(inbound_date_str, "%Y-%m-%d")
                    ret_seg = FlightSegment(
                        airline="BA",
                        airline_name="British Airways",
                        flight_no="",
                        origin=arr_airport,
                        destination=dep_airport,
                        departure=ret_dt,
                        arrival=ret_dt,
                        duration_seconds=0,
                        cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(_ba_cabin, "economy"),
                    )
                    inbound = FlightRoute(
                        segments=[ret_seg],
                        total_duration_seconds=0,
                        stopovers=0 if is_direct else 1,
                    )
                except ValueError:
                    pass

            fid = hashlib.md5(
                f"ba_{dep_airport}{arr_airport}{outbound_date_str}{inbound_date_str}{price_f}".encode()
            ).hexdigest()[:12]

            rt_params = ""
            if inbound:
                rt_params = f"&retDate={inbound_date_str[:7].replace('-', '')}&oneWay=false"

            offers.append(FlightOffer(
                id=f"ba_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=price_fmt,
                outbound=route,
                inbound=inbound,
                airlines=["British Airways"],
                owner_airline="BA",
                booking_url=(
                    f"https://www.britishairways.com/travel/fx/public/en_gb"
                    f"?from={dep_airport}&to={arr_airport}"
                    f"&depDate={outbound_date_str[:7].replace('-', '')}"
                    f"&cabin={_ba_cabin}&{'oneWay=false' if inbound else 'oneWay=true'}&ad={req.adults or 1}"
                    + rt_params
                ),
                is_locked=False,
                source="britishairways_direct",
                source_tier="free",
            ))

        return offers


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
                    id=f"rt_brit_{cid}", price=price, currency=o.currency,
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
            f"ba{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="GBP",
            offers=[],
            total_results=0,
        )
