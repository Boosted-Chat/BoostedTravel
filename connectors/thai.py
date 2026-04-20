"""
Thai Airways httpx connector -- fetches fare data from thaiairways.com
(EveryMundo airTRFX platform).

Thai Airways International (IATA: TG) is Thailand's flag carrier based in
Bangkok. Operates long-haul and regional routes from BKK hub to Asia,
Europe, Australia, and the Middle East. Default currency THB.

Strategy:
1. Map IATA codes to city slugs used by thaiairways.com/flights/
2. Fetch route page: thaiairways.com/flights/en-th/flights-from-{origin}-to-{dest}
3. Extract __NEXT_DATA__ JSON from page
4. Parse StandardFareModule fares -> FlightOffers
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
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

# IATA code -> URL slug mapping for thaiairways.com EveryMundo fare pages.
# Validated against live site. Slugs are lowercase-hyphenated city names.
_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "LON": "london", "PAR": "paris", "TYO": "tokyo",
    # Thailand (domestic)
    "BKK": "bangkok",
    "CNX": "chiang-mai",
    "CEI": "chiang-rai",
    "HDY": "hat-yai",
    "KKC": "khon-kaen",
    "KBV": "krabi",
    "HKT": "phuket",
    "UBP": "ubon-ratchathani",
    "UTH": "udon-thani",
    # East Asia
    "PEK": "beijing",
    "CTU": "chengdu",
    "CAN": "guangzhou",
    "KMG": "kunming",
    "PVG": "shanghai",
    "SHA": "shanghai",
    "HKG": "hong-kong",
    "KHH": "kaohsiung-city",
    "TPE": "taipei",
    "FUK": "fukuoka",
    "NGO": "nagoya",
    "NRT": "tokyo",
    "HND": "tokyo",
    "CTS": "sapporo",
    "ICN": "seoul",
    "MNL": "manila",
    # Southeast Asia
    "PNH": "phnom-penh",
    "CGK": "jakarta",
    "DPS": "denpasar",
    "KUL": "kuala-lumpur",
    "PEN": "penang",
    "RGN": "yangon",
    "SIN": "singapore",
    "HAN": "hanoi",
    "SGN": "ho-chi-minh-city",
    "VTE": "vientiane",
    # South Asia
    "AMD": "ahmedabad",
    "BLR": "bangalore",
    "BOM": "mumbai",
    "CCU": "kolkata",
    "COK": "kochi",
    "DEL": "new-delhi",
    "GAY": "gaya",
    "HYD": "hyderabad",
    "MAA": "chennai",
    "CMB": "colombo",
    "DAC": "dhaka",
    "KTM": "kathmandu",
    "ISB": "islamabad",
    "KHI": "karachi",
    "LHE": "lahore",
    # Europe
    "BRU": "brussels",
    "CPH": "copenhagen",
    "FRA": "frankfurt",
    "IST": "istanbul",
    "LHR": "london",
    "MXP": "milan",
    "MUC": "munich",
    "OSL": "oslo",
    "CDG": "paris",
    "ARN": "stockholm",
    "ZRH": "zurich",
    # Oceania
    "MEL": "melbourne",
    "PER": "perth",
    "SYD": "sydney",
}

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_BASE = "https://www.thaiairways.com/flights/en-th"


class ThaiConnectorClient:
    """Thai Airways httpx connector -- thaiairways.com EveryMundo fare pages."""

    def __init__(self, timeout: float = 20.0):
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
            logger.warning("Thai: unmapped IATA code %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Thai: fetching %s", url)

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                headers=_HEADERS,
                proxy=get_httpx_proxy_url(),) as client:
                resp = await client.get(url)

            if resp.status_code != 200:
                logger.warning("Thai: %s returned %d", url, resp.status_code)
                return self._empty(req)

            fares = self._extract_fares(resp.text)
            if not fares:
                logger.warning("Thai: no fares found on page %s", url)
                return self._empty(req)

            # ── Round-trip: fetch reverse route page for IB fares ──
            ib_fares: list[dict] = []
            if req.return_from:
                ib_url = f"{_BASE}/flights-from-{dest_slug}-to-{origin_slug}"
                logger.info("Thai: fetching IB fares %s", ib_url)
                try:
                    async with httpx.AsyncClient(
                        timeout=self.timeout,
                        follow_redirects=True,
                        headers=_HEADERS,
                        proxy=get_httpx_proxy_url(),) as ib_client:
                        ib_resp = await ib_client.get(ib_url)
                    if ib_resp.status_code == 200:
                        ib_fares = self._extract_fares(ib_resp.text)
                except Exception as e:
                    logger.warning("Thai IB fetch error: %s", e)

            offers = self._build_offers(fares, req, ib_fares=ib_fares)
            _td = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
            offers = [o for o in offers if o.outbound and o.outbound.segments and o.outbound.segments[0].departure.date() == _td]
            elapsed = time.monotonic() - t0

            offers.sort(key=lambda o: o.price)
            logger.info(
                "Thai %s->%s returned %d offers in %.1fs (httpx)",
                req.origin, req.destination, len(offers), elapsed,
            )

            h = hashlib.md5(
                f"thai{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else (req.currency or "THB"),
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Thai httpx error: %s", e)
            return self._empty(req)

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

        # Collect all Fare objects from Apollo state (StandardFareModule refs)
        for v in apollo.values():
            if isinstance(v, dict) and v.get("__typename") == "StandardFareModule":
                raw_fares = v.get("fares", [])
                if not raw_fares:
                    continue
                # Resolve Apollo refs
                resolved = []
                for f in raw_fares:
                    if isinstance(f, dict) and "__ref" in f:
                        ref_data = apollo.get(f["__ref"])
                        if ref_data and isinstance(ref_data, dict):
                            resolved.append(ref_data)
                    elif isinstance(f, dict):
                        resolved.append(f)
                if resolved:
                    return resolved
        return []

    def _build_offers(
        self, fares: list[dict], req: FlightSearchRequest, *, ib_fares: list[dict] | None = None,
    ) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        ret_date = req.return_from.strftime("%Y-%m-%d") if req.return_from else None
        offers: list[FlightOffer] = []

        for fare in fares:
            price = fare.get("totalPrice")
            if not price or price <= 0:
                continue

            currency = fare.get("currencyCode") or req.currency or "THB"
            dep_date = fare.get("departureDate", "")
            origin_code = fare.get("originAirportCode") or req.origin
            dest_code = fare.get("destinationAirportCode") or req.destination

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
                except ValueError:
                    pass

            segment = FlightSegment(
                airline="TG",
                airline_name="Thai Airways",
                flight_no="",
                origin=origin_code,
                destination=dest_code,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=(fare.get("formattedTravelClass") or "Economy").lower(),
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=0,
                stopovers=0,
            )

            # ── IB route from reverse-page fares ──
            _ib_route = None
            _ib_price = 0.0
            if ret_date and ib_fares:
                best_ib = None
                best_ib_exact = None
                for ibf in ib_fares:
                    ibp = ibf.get("totalPrice")
                    if not ibp or ibp <= 0:
                        continue
                    ib_dep = (ibf.get("departureDate") or "")[:10]
                    if ib_dep == ret_date:
                        if best_ib_exact is None or ibp < (best_ib_exact.get("totalPrice") or 9e9):
                            best_ib_exact = ibf
                    if best_ib is None or ibp < (best_ib.get("totalPrice") or 9e9):
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
                        airline="TG", airline_name="Thai Airways", flight_no="",
                        origin=req.destination, destination=req.origin,
                        departure=ib_dt, arrival=ib_dt,
                        duration_seconds=0, cabin_class=(fare.get("formattedTravelClass") or "Economy").lower(),
                    )
                    _ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)

            total_price = round(float(price) + _ib_price, 2) if _ib_route else round(float(price), 2)
            id_prefix = "tg_rt_" if _ib_route else "tg_"

            fid = hashlib.md5(
                f"tg_{origin_code}{dest_code}{dep_date}{total_price}{ret_date or ''}".encode()
            ).hexdigest()[:12]

            offer_booking = (
                f"https://www.thaiairways.com/en-th/"
                f"?from={origin_code}&to={dest_code}"
                f"&depart={dep_date or target_date}"
                f"&adults={req.adults}&children={req.children}"
            )
            if _ib_route and ret_date:
                offer_booking += f"&return={ret_date}"

            offers.append(FlightOffer(
                id=f"{id_prefix}{fid}",
                price=total_price,
                currency=currency,
                price_formatted=f"{total_price:.2f} {currency}",
                outbound=route,
                inbound=_ib_route,
                airlines=["Thai Airways"],
                owner_airline="TG",
                booking_url=offer_booking,
                is_locked=False,
                source="thai_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"thai{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "THB",
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
                    id=f"rt_thai_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
