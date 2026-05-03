"""
Air New Zealand connector — EveryMundo airTRFX fare pages.

Air New Zealand (IATA: NZ) — AKL/WLG/CHC hubs.
Star Alliance member. 50+ destinations across NZ, Pacific islands, AU, Asia, Americas.

Strategy (httpx, no browser):
  Air New Zealand uses EveryMundo airTRFX (same platform as Air Canada / Thai).
  1. Fetch route page: airnewzealand.com/flights/en-nz/flights-from-{origin}-to-{dest}
  2. Extract __NEXT_DATA__ JSON from <script> tag
  3. Parse StandardFareModule fares from Apollo GraphQL state
  4. Filter by matching origin/destination airport codes and departure date
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

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_httpx_proxy_url
from .airline_routes import city_match_set

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
_BASE = "https://www.airnewzealand.co.nz"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-NZ,en;q=0.9",
}

# IATA → slug for Air New Zealand's EveryMundo fare pages.
_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "LON": "london", "NYC": "new-york", "PAR": "paris", "TYO": "tokyo",
    # New Zealand
    "AKL": "auckland", "WLG": "wellington", "CHC": "christchurch",
    "ZQN": "queenstown", "DUD": "dunedin", "PMR": "palmerston-north",
    "NPE": "napier", "NPL": "new-plymouth", "HLZ": "hamilton",
    "TRG": "tauranga", "ROT": "rotorua", "BHE": "blenheim",
    "NSN": "nelson", "IVC": "invercargill",
    # Australia
    "SYD": "sydney", "MEL": "melbourne", "BNE": "brisbane",
    "PER": "perth", "ADL": "adelaide", "OOL": "gold-coast",
    # Pacific Islands
    "NAN": "nadi", "APW": "apia", "TBU": "tongatapu",
    "RAR": "rarotonga", "NOU": "noumea", "PPT": "papeete",
    # Asia
    "SIN": "singapore", "HKG": "hong-kong", "NRT": "tokyo",
    "HND": "tokyo", "PVG": "shanghai", "PEK": "beijing",
    "ICN": "seoul", "TPE": "taipei", "MNL": "manila",
    "KUL": "kuala-lumpur", "BKK": "bangkok", "DPS": "bali",
    "SGN": "ho-chi-minh-city", "HAN": "hanoi",
    # Americas
    "LAX": "los-angeles", "SFO": "san-francisco", "JFK": "new-york",
    "IAH": "houston", "ORD": "chicago", "SEA": "seattle",
    "IAD": "washington-dc", "BOS": "boston", "DEN": "denver",
    "HNL": "honolulu", "YVR": "vancouver",
    # Europe / Middle East
    "LHR": "london", "CDG": "paris", "FRA": "frankfurt",
}


class AirNewZealandConnectorClient:
    """Air New Zealand — EveryMundo airTRFX fare pages."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True,
                proxy=get_httpx_proxy_url(),)
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
        try:
            from .ancillary_live_probe import probe_ancillaries
            result = await probe_ancillaries("NZ", origin, dest, date_str=date_str)
            if result:
                return result
        except Exception:
            pass
        # Static fallback: Air New Zealand Economy includes bag on most fares; Seat only / Bare fares do not.
        return {
            "bags_note": "Economy (Seat+Bag): 1×23 kg included. Bare fare: no bag, first bag from NZD 25. Carry-on 7 kg included.",
            "seat_note": "Seat selection: free standard seats at check-in. ChoiceSeats from NZD 30.",
            "bags_from": None,
            "checked_bag_price": 25.0,
            "currency": currency,
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        checked_from = ancillary.get("checked_bag_price")
        for offer in offers:
            offer.bags_price.setdefault("carry_on", 0.0)
            if checked_from is not None:
                offer.bags_price.setdefault("checked_bag", float(checked_from))
            if bags_note:
                offer.conditions.setdefault("carry_on", bags_note)
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions.setdefault("seat", seat_note)

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        client = await self._client()

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("Air NZ: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/flights/en-nz/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Air NZ: fetching %s", url)

        try:
            resp = await client.get(url)
            if resp.status_code not in (200, 404) or "__NEXT_DATA__" not in resp.text:
                logger.warning("Air NZ: %s returned %d (no fare data)", url, resp.status_code)
                return self._empty(req)
        except Exception as e:
            logger.error("Air NZ fetch error: %s", e)
            return self._empty(req)

        fares = self._extract_fares(resp.text)
        if not fares:
            logger.info("Air NZ: no fares on page %s", url)
            return self._empty(req)

        # RT: fetch reverse route for inbound fares
        ib_fares: list[dict] = []
        if req.return_from and dest_slug and origin_slug:
            rev_url = f"{_BASE}/flights/en-nz/flights-from-{dest_slug}-to-{origin_slug}"
            try:
                rev_resp = await client.get(rev_url)
                if rev_resp.status_code == 200 and "__NEXT_DATA__" in rev_resp.text:
                    ib_fares = self._extract_fares(rev_resp.text)
            except Exception:
                pass

        offers = self._build_offers(fares, req, ib_fares=ib_fares)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Air NZ %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"airnz{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "NZD",
            offers=offers,
            total_results=len(offers),
        )

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

    def _build_offers(self, fares: list[dict], req: FlightSearchRequest, ib_fares: list[dict] | None = None) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)

        # Build cheapest IB route if RT
        _ib_route: FlightRoute | None = None
        _ib_price = 0.0
        if req.return_from and ib_fares:
            ret_str = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, 'strftime') else str(req.return_from)[:10]
            best_ib_p = float("inf")
            best_ib_fare = None
            # Prefer exact return date match
            for f in ib_fares:
                orig = f.get("originAirportCode", "")
                dest = f.get("destinationAirportCode", "")
                if orig not in valid_dests or dest not in valid_origins:
                    continue
                p = float(f.get("totalPrice", 0) or 0)
                if p <= 0:
                    continue
                dep = (f.get("departureDate", "") or "")[:10]
                if dep == ret_str and p < best_ib_p:
                    best_ib_p = p
                    best_ib_fare = f
            # Fallback: cheapest overall IB
            if not best_ib_fare:
                for f in ib_fares:
                    orig = f.get("originAirportCode", "")
                    dest = f.get("destinationAirportCode", "")
                    if orig not in valid_dests or dest not in valid_origins:
                        continue
                    p = float(f.get("totalPrice", 0) or 0)
                    if 0 < p < best_ib_p:
                        best_ib_p = p
                        best_ib_fare = f
            if best_ib_fare and best_ib_p < float("inf"):
                _ib_price = round(best_ib_p, 2)
                ib_dep = (best_ib_fare.get("departureDate", "") or "")[:10]
                ib_dep_dt = datetime(2000, 1, 1)
                if ib_dep:
                    try:
                        ib_dep_dt = datetime.strptime(ib_dep, "%Y-%m-%d")
                    except ValueError:
                        pass
                _ib_route = FlightRoute(
                    segments=[FlightSegment(
                        airline="NZ", airline_name="Air New Zealand", flight_no="",
                        origin=best_ib_fare.get("originAirportCode", req.destination),
                        destination=best_ib_fare.get("destinationAirportCode", req.origin),
                        departure=ib_dep_dt, arrival=ib_dep_dt,
                        duration_seconds=0,
                        cabin_class=(best_ib_fare.get("formattedTravelClass") or "Economy").lower(),
                    )],
                    total_duration_seconds=0, stopovers=0,
                )

        is_rt = _ib_route is not None

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
            total_price = round(price_f + _ib_price, 2) if is_rt else price_f
            prefix = "nz_rt_" if is_rt else "nz_"

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            cabin = (fare.get("formattedTravelClass") or "Economy").lower()
            seg = FlightSegment(
                airline="NZ",
                airline_name="Air New Zealand",
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

            fid = hashlib.md5(
                f"nz_{orig}{dest}{dep_date}{total_price}{cabin}".encode()
            ).hexdigest()[:12]

            burl = (
                f"https://www.airnewzealand.co.nz/booking/flights"
                f"?origin={req.origin}&destination={req.destination}"
                f"&date={target_date}"
                f"&adults={req.adults or 1}"
            )
            if is_rt and req.return_from:
                r_str = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, 'strftime') else str(req.return_from)[:10]
                burl += f"&returnDate={r_str}"

            offers.append(FlightOffer(
                id=f"{prefix}{fid}",
                price=total_price,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{total_price:.2f} {currency}",
                outbound=route,
                inbound=_ib_route,
                airlines=["Air New Zealand"],
                owner_airline="NZ",
                booking_url=burl,
                is_locked=False,
                source="airnewzealand_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"airnz{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="NZD",
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
                    id=f"rt_airnz_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
