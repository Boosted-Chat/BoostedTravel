"""
Aegean Airlines connector — EveryMundo airTRFX fare pages.

Aegean Airlines (IATA: A3) is Greece's largest airline, a Star Alliance member,
based in Athens. Operates 155+ routes to Europe, Middle East, and North Africa.

Strategy (httpx, no browser):
  Aegean uses EveryMundo airTRFX at flights.aegeanair.com (subdomain).
  1. Fetch route page: flights.aegeanair.com/en/flights-from-{origin}-to-{dest}
  2. Extract __NEXT_DATA__ JSON from <script> tag
  3. Parse StandardFareModule fares from Apollo GraphQL state
  4. Filter by origin/destination airport codes and departure date
  Note: Aegean uses city codes (LON, CDG) in some fares — we match both.
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

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
_BASE = "https://flights.aegeanair.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# IATA → EveryMundo slug for Aegean route pages.
_IATA_TO_SLUG: dict[str, str] = {
    # Greece
    "ATH": "athens", "SKG": "thessaloniki", "HER": "heraklion",
    "CFU": "corfu", "RHO": "rhodes", "CHQ": "chania",
    "JMK": "mykonos", "JTR": "santorini", "KGS": "kos",
    "ZTH": "zakynthos", "EFL": "kefalonia", "JSI": "skiathos",
    "AXD": "alexandroupolis",
    # Europe — city codes + airport codes
    "LON": "london", "LHR": "london", "LGW": "london", "STN": "london",
    "PAR": "paris", "CDG": "paris", "ORY": "paris",
    "ROM": "rome", "FCO": "rome", "MXP": "milan",
    "FRA": "frankfurt", "MUC": "munich", "BER": "berlin",
    "DUS": "dusseldorf", "STR": "stuttgart",
    "AMS": "amsterdam", "BRU": "brussels",
    "ZRH": "zurich", "GVA": "geneva",
    "VIE": "vienna", "BCN": "barcelona", "MAD": "madrid",
    "LIS": "lisbon", "IST": "istanbul", "SAW": "istanbul",
    "SOF": "sofia", "BUD": "budapest", "OTP": "bucharest",
    "WAW": "warsaw", "PRG": "prague",
    "CPH": "copenhagen", "ARN": "stockholm", "OSL": "oslo",
    "HEL": "helsinki", "DUB": "dublin",
    "MAN": "manchester", "EDI": "edinburgh",
    # Middle East / Africa
    "TLV": "tel-aviv", "CAI": "cairo",
    "CMN": "casablanca", "AMM": "amman", "BEY": "beirut",
    "LCA": "larnaca", "PFO": "paphos",
    "TBS": "tbilisi", "EVN": "yerevan",
    "DXB": "dubai", "JED": "jeddah", "RUH": "riyadh",
}

# Map IATA airport codes to the city codes Aegean uses in fares.
_IATA_TO_CITY: dict[str, str] = {
    "LHR": "LON", "LGW": "LON", "STN": "LON",
    "CDG": "PAR", "ORY": "PAR",
}


class AegeanConnectorClient:
    """Aegean Airlines — EveryMundo airTRFX fare pages."""

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
        # Aegean Basic: no bag. Standard/Flex: 1×23 kg included.
        return {
            "bags_note": "Economy Basic (cheapest): no checked bag, first bag from EUR 20. Economy Standard/Flex: 1×23 kg included. Carry-on included.",
            "seat_note": "Seat selection: from EUR 10 (Basic). Included on Standard/Flex fares.",
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
        client = await self._client()

        from .airline_routes import resolve_slug
        origin_slug = resolve_slug(req.origin, _IATA_TO_SLUG)
        dest_slug = resolve_slug(req.destination, _IATA_TO_SLUG)
        if not origin_slug or not dest_slug:
            # Still try origin-only page even if dest slug unknown
            if not origin_slug:
                logger.warning("Aegean: unmapped origin IATA %s", req.origin)
                return self._empty(req)
            # dest_slug may be None — we'll use origin-only page below

        # Try route-specific page first, then fallback to origin page
        urls_to_try = []
        if dest_slug:
            urls_to_try.append(f"{_BASE}/en/flights-from-{origin_slug}-to-{dest_slug}")
        urls_to_try.append(f"{_BASE}/en/flights-from-{origin_slug}")

        fares = []
        for url in urls_to_try:
            logger.info("Aegean: fetching %s", url)
            try:
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.info("Aegean: %s returned %d", url, resp.status_code)
                    continue
            except Exception as e:
                logger.error("Aegean fetch error: %s", e)
                continue

            fares = self._extract_fares(resp.text)
            if fares:
                break
        if not fares:
            logger.info("Aegean: no fares on page %s", url)
            return self._empty(req)

        offers = self._build_offers(fares, req)

        # RT: fetch reverse route for inbound fares
        if req.return_from and offers and dest_slug:
            try:
                _rev_url = f"{_BASE}/en/flights-from-{dest_slug}-to-{origin_slug}"
                _rev_resp = await client.get(_rev_url)
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
                        _a3_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                        _ib_seg = FlightSegment(
                            airline="A3",
                            airline_name="Aegean Airlines",
                            flight_no="",
                            origin=req.destination,
                            destination=req.origin,
                            departure=_ret_dt,
                            arrival=_ret_dt,
                            duration_seconds=0,
                            cabin_class=_a3_cabin,
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
        logger.info("Aegean %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"aegean{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
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

    def _build_offers(self, fares: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []

        # Aegean uses city codes like LON, PAR in fares — expand for matching
        origin_codes = {req.origin, _IATA_TO_CITY.get(req.origin, req.origin)}
        dest_codes = {req.destination, _IATA_TO_CITY.get(req.destination, req.destination)}
        # Also include all airport codes in the city group
        from .airline_routes import city_match_set
        origin_codes |= set(city_match_set(req.origin))
        dest_codes |= set(city_match_set(req.destination))

        # First pass: collect exact-date fares and route-matching fares
        exact_date_fares = []
        nearby_fares = []

        for fare in fares:
            orig = fare.get("originAirportCode", "")
            dest = fare.get("destinationAirportCode", "")
            if orig not in origin_codes or dest not in dest_codes:
                continue

            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue

            dep_date = fare.get("departureDate", "")
            if dep_date[:10] == target_date:
                exact_date_fares.append(fare)
            else:
                nearby_fares.append(fare)

        # Only use exact-date fares — never fall back to wrong-date fares.
        # Returning November offers for a June search is worse than returning empty.
        use_fares = exact_date_fares

        for fare in use_fares:
            price = fare.get("totalPrice")
            if not price or float(price) <= 0:
                continue
            dep_date = fare.get("departureDate", "")

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
                airline="A3",
                airline_name="Aegean Airlines",
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
                f"a3_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"a3_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Aegean Airlines"],
                owner_airline="A3",
                booking_url=(
                    f"https://en.aegeanair.com/search/"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={target_date}"
                    f"&adults={req.adults or 1}&tripType={'R' if req.return_from else 'O'}"
                ),
                is_locked=False,
                source="aegean_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"aegean{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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
                    id=f"rt_aegean_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
