"""
Wingo connector — EveryMundo airTRFX fare pages.

Wingo (IATA: P5) is a Colombian low-cost carrier, subsidiary of Copa Airlines.
Operates from BOG/CTG/MDE hubs to destinations in Colombia, Panama, Caribbean,
Central America, and northern South America. 40+ destinations.

Strategy (httpx, no browser):
  Wingo uses EveryMundo airTRFX at wingo.com.
  1. Fetch route page: wingo.com/en/flights-from-{origin}-to-{dest}
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

_BASE = "https://www.wingo.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_IATA_TO_SLUG: dict[str, str] = {
    # Colombia
    "BOG": "bogota", "MDE": "medellin", "CTG": "cartagena",
    "CLO": "cali", "BAQ": "barranquilla", "SMR": "santa-marta",
    "BGA": "bucaramanga", "ADZ": "san-andres-island",
    "PEI": "pereira", "AXM": "armenia", "VUP": "valledupar",
    # Caribbean
    "CUN": "cancun", "PUJ": "punta-cana", "SDQ": "santo-domingo",
    "HAV": "havana", "CUR": "curacao", "MBJ": "montego-bay",
    "AUA": "oranjestad",
    # Central America
    "PTY": "panama-city", "SJO": "san-jose-sjo",
    "GUA": "guatemala-city", "SAL": "san-salvador",
    # South America
    "GYE": "guayaquil", "UIO": "quito",
    "LIM": "lima", "SCL": "santiago",
    "CCS": "caracas",
    # North America
    "MIA": "miami",
}


class WingoConnectorClient:
    """Wingo — EveryMundo airTRFX fare pages."""

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
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        client = await self._client()

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("Wingo: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/en/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Wingo: fetching %s", url)

        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Wingo: %s returned %d", url, resp.status_code)
                return self._empty(req)
        except Exception as e:
            logger.error("Wingo fetch error: %s", e)
            return self._empty(req)

        fares = self._extract_fares(resp.text)
        if not fares:
            logger.info("Wingo: no fares on page %s", url)
            return self._empty(req)

        # For RT, fetch reverse route page for inbound fares
        ib_fares: list[dict] = []
        if req.return_from:
            rev_url = f"{_BASE}/en/flights-from-{dest_slug}-to-{origin_slug}"
            try:
                rev_resp = await client.get(rev_url)
                if rev_resp.status_code == 200:
                    ib_fares = self._extract_fares(rev_resp.text)
            except Exception:
                pass

        offers = self._build_offers(fares, req, ib_fares=ib_fares)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Wingo %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"wingo{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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

    @staticmethod
    def _extract_fare_conditions(fare: dict) -> dict[str, str]:
        conditions: dict[str, str] = {}
        branded_fare = fare.get("brandedFareClass")
        if isinstance(branded_fare, str) and branded_fare.strip():
            fare_name = branded_fare.strip()
            conditions["fare_family"] = fare_name
            name_upper = fare_name.upper()
            # Wingo fare families (Copa spin-off): LIGHT/BASIC=no bag, CLASSIC=1 bag, FLEX=2 bags
            if any(k in name_upper for k in ("LIGHT", "BASIC", "BASE", "ZERO", "MINI")):
                conditions["checked_bag"] = "no free checked bag"
                conditions["carry_on"] = "no free overhead carry-on — add-on available at checkout"
            elif any(k in name_upper for k in ("FLEX", "PLUS", "PREMIUM", "BUSINESS", "FULL")):
                conditions["checked_bag"] = "2x 23kg bags included"
                conditions["carry_on"] = "1x 10kg carry-on included"
            elif any(k in name_upper for k in ("CLASSIC", "ECONOMY", "STANDARD", "REGULAR")):
                conditions["checked_bag"] = "1x 23kg bag included"
                conditions["carry_on"] = "1x 10kg carry-on included"
            else:
                conditions["carry_on"] = "carry-on policy depends on fare — check at checkout"
        else:
            conditions["fare_upgrade_note"] = "Route page exposes base fare only; ancillary prices not available"
            conditions["carry_on"] = "carry-on not included on base fare — add at checkout"
            conditions["checked_bag"] = "checked bag not included on base fare — add at checkout"
        conditions.setdefault("seat", "seat selection available at checkout")
        return conditions

    def _build_offers(self, fares: list[dict], req: FlightSearchRequest,
                      ib_fares: list[dict] | None = None) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)
        is_rt = bool(req.return_from)

        # Build cheapest inbound route for RT
        ib_route: FlightRoute | None = None
        ib_price = 0.0
        if is_rt and ib_fares:
            ret_date = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, 'strftime') else str(req.return_from)[:10]
            valid_ib_origins = city_match_set(req.destination)
            valid_ib_dests = city_match_set(req.origin)
            best_ib_price = float("inf")
            for fare in ib_fares:
                orig = fare.get("originAirportCode", "")
                dest = fare.get("destinationAirportCode", "")
                if orig not in valid_ib_origins or dest not in valid_ib_dests:
                    continue
                p = fare.get("totalPrice")
                if not p or float(p) <= 0:
                    continue
                dep_date = fare.get("departureDate", "")[:10]
                if dep_date != ret_date:
                    continue
                pf = float(p)
                if pf < best_ib_price:
                    best_ib_price = pf
                    ib_price = pf
                    ib_dep_dt = datetime(2000, 1, 1)
                    if dep_date:
                        try:
                            ib_dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
                        except ValueError:
                            pass
                    ib_seg = FlightSegment(
                        airline="P5", airline_name="Wingo", flight_no="",
                        origin=req.destination, destination=req.origin,
                        departure=ib_dep_dt, arrival=ib_dep_dt, duration_seconds=0,
                        cabin_class=(fare.get("formattedTravelClass") or "Economy").lower(),
                    )
                    ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)
            # If no exact-date IB fare, try any IB fare as estimate
            if not ib_route and ib_fares:
                for fare in sorted(ib_fares, key=lambda f: float(f.get("totalPrice") or 999999)):
                    orig = fare.get("originAirportCode", "")
                    dest = fare.get("destinationAirportCode", "")
                    if orig not in valid_ib_origins or dest not in valid_ib_dests:
                        continue
                    p = fare.get("totalPrice")
                    if not p or float(p) <= 0:
                        continue
                    ib_price = float(p)
                    ib_seg = FlightSegment(
                        airline="P5", airline_name="Wingo", flight_no="",
                        origin=req.destination, destination=req.origin,
                        departure=datetime(2000, 1, 1), arrival=datetime(2000, 1, 1),
                        duration_seconds=0,
                    )
                    ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)
                    break

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

        # Prefer exact-date fares; fall back to cheapest nearby fare as indicative price
        if exact_fares:
            use_fares = exact_fares
        elif nearby_fares:
            # airTRFX pages show cached prices — use lowest-priced nearby fare as estimate
            cheapest = min(nearby_fares, key=lambda f: float(f.get("totalPrice") or 999999))
            use_fares = [cheapest]
        else:
            use_fares = []

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
                airline="P5",
                airline_name="Wingo",
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
                f"p5_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            total_price = round(price_f + ib_price, 2) if is_rt and ib_route else price_f
            prefix = "p5_rt_" if is_rt and ib_route else "p5_"
            conditions = self._extract_fare_conditions(fare)

            offers.append(FlightOffer(
                id=f"{prefix}{fid}",
                price=total_price,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{total_price:.2f} {currency}",
                outbound=route,
                inbound=ib_route,
                airlines=["Wingo"],
                owner_airline="P5",
                conditions=conditions,
                booking_url=(
                    f"https://booking.wingo.com/search/"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={target_date}"
                    f"&adults={req.adults or 1}&tripType={'R' if req.return_from else 'O'}"
                ),
                is_locked=False,
                source="wingo_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"wingo{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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
        def merge_conditions(outbound_conditions: dict[str, str], inbound_conditions: dict[str, str]) -> dict[str, str]:
            merged = dict(outbound_conditions or {})
            for key, value in (inbound_conditions or {}).items():
                if value in (None, ""):
                    continue
                existing = merged.get(key)
                if existing is None:
                    merged[key] = value
                    continue
                if existing == value:
                    continue
                merged.pop(key, None)
                if key in (outbound_conditions or {}):
                    merged[f"outbound_{key}"] = outbound_conditions[key]
                merged[f"inbound_{key}"] = value
            return merged

        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_wingo_{cid}", price=price, currency=o.currency,
                    price_formatted=f"{price:.2f} {o.currency}",
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    conditions=merge_conditions(o.conditions, i.conditions),
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
