"""
KLM connector — EveryMundo airTRFX fare pages via curl_cffi.

KLM Royal Dutch Airlines (IATA: KL) is the flag carrier of the Netherlands.
Hub at Amsterdam Schiphol (AMS) with 170+ destinations worldwide.
Part of the Air France-KLM Group, SkyTeam alliance.

Strategy (curl_cffi required — Cloudflare / Akamai protections):
  1. Resolve IATA codes to city slugs via KLM's airport lookup API
  2. Fetch route page: klm.nl/en-nl/flights-from-{origin}-to-{dest}
  3. Extract __NEXT_DATA__ JSON from <script> tag
  4. Parse DpaHeadline → lowestFare for route pricing data
  5. Filter by departure date
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

_BASE = "https://www.klm.nl"
_SITE_EDITION = "en-nl"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Static slug mapping for major KLM airports. Falls back to airport API.
_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "LON": "london", "NYC": "new-york", "PAR": "paris", "ROM": "rome",
    "MIL": "milan", "TYO": "tokyo", "WAS": "washington-dc",
    # Netherlands
    "AMS": "amsterdam",
    # UK / Ireland
    "LHR": "london", "LGW": "london", "STN": "london", "LTN": "london",
    "MAN": "manchester", "EDI": "edinburgh", "GLA": "glasgow",
    "BHX": "birmingham", "BRS": "bristol", "NCL": "newcastle",
    "DUB": "dublin", "SNN": "shannon", "ORK": "cork",
    # France
    "CDG": "paris", "ORY": "paris",
    "NCE": "nice", "LYS": "lyon", "MRS": "marseille", "TLS": "toulouse",
    "BOD": "bordeaux", "NTE": "nantes",
    # Germany
    "FRA": "frankfurt/main", "MUC": "munich", "BER": "berlin",
    "HAM": "hamburg", "DUS": "dusseldorf", "CGN": "cologne",
    "STR": "stuttgart", "HAJ": "hanover", "NUE": "nuremberg",
    # Spain
    "BCN": "barcelona", "MAD": "madrid", "AGP": "malaga",
    "ALC": "alicante", "PMI": "palma-de-mallorca", "VLC": "valencia",
    "BIO": "bilbao", "SVQ": "seville", "TFS": "tenerife",
    # Italy
    "FCO": "rome", "MXP": "milan", "VCE": "venice",
    "NAP": "naples", "BLQ": "bologna", "FLR": "florence",
    "CTA": "catania", "PSA": "pisa",
    # Nordics
    "CPH": "copenhagen", "ARN": "stockholm", "GOT": "gothenburg",
    "OSL": "oslo", "BGO": "bergen", "TRD": "trondheim",
    "HEL": "helsinki", "TMP": "tampere",
    # Central/Eastern Europe
    "VIE": "vienna", "ZRH": "zurich", "GVA": "geneva",
    "BRU": "brussels", "LUX": "luxembourg",
    "WAW": "warsaw", "KRK": "krakow",
    "PRG": "prague", "BUD": "budapest",
    "OTP": "bucharest", "SOF": "sofia", "ZAG": "zagreb",
    "BEG": "belgrade", "LJU": "ljubljana",
    # Greece / Turkey / Cyprus
    "ATH": "athens", "SKG": "thessaloniki",
    "IST": "istanbul", "SAW": "istanbul", "AYT": "antalya",
    "LCA": "larnaca", "PFO": "paphos",
    # Portugal
    "LIS": "lisbon", "OPO": "porto", "FAO": "faro",
    # Americas
    "JFK": "new-york", "EWR": "new-york", "LGA": "new-york",
    "LAX": "los-angeles", "SFO": "san-francisco",
    "ORD": "chicago", "IAH": "houston", "DFW": "dallas",
    "ATL": "atlanta", "MIA": "miami", "BOS": "boston",
    "IAD": "washington-dc", "DCA": "washington-dc",
    "SEA": "seattle", "MSP": "minneapolis", "DEN": "denver",
    "DTW": "detroit", "PHL": "philadelphia",
    "YYZ": "toronto", "YUL": "montreal", "YVR": "vancouver",
    "MEX": "mexico-city", "CUN": "cancun",
    "GRU": "sao-paulo", "GIG": "rio-de-janeiro",
    "EZE": "buenos-aires", "BOG": "bogota", "SCL": "santiago",
    "LIM": "lima", "PTY": "panama-city",
    # Middle East / Africa
    "DXB": "dubai", "AUH": "abu-dhabi", "DOH": "doha",
    "BAH": "bahrain", "KWI": "kuwait", "RUH": "riyadh",
    "JED": "jeddah", "TLV": "tel-aviv", "AMM": "amman",
    "CAI": "cairo", "CMN": "casablanca", "TUN": "tunis",
    "NBO": "nairobi", "DSS": "dakar", "ACC": "accra",
    "LOS": "lagos", "JNB": "johannesburg", "CPT": "cape-town",
    "ADD": "addis-ababa", "DAR": "dar-es-salaam",
    # Asia-Pacific
    "NRT": "tokyo", "HND": "tokyo", "KIX": "osaka",
    "ICN": "seoul", "PEK": "beijing", "PVG": "shanghai",
    "HKG": "hong-kong", "TPE": "taipei",
    "SIN": "singapore", "KUL": "kuala-lumpur",
    "BKK": "bangkok", "HAN": "hanoi", "SGN": "ho-chi-minh-city",
    "CGK": "jakarta", "DPS": "bali",
    "DEL": "delhi", "BOM": "mumbai", "BLR": "bengaluru",
    "SYD": "sydney", "MEL": "melbourne",
    "AKL": "auckland",
}

# Airport API for dynamic slug lookup
_AIRPORT_API = "https://openair-california.airtrfx.com/hangar-service/v2/kl/airports/search"
_EM_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"

# Module-level cache for dynamically fetched slugs
_slug_cache: dict[str, str] = {}
_slug_cache_loaded = False


def _load_slug_cache_sync() -> None:
    """Fetch all airport slugs from KLM's API (one-time)."""
    global _slug_cache, _slug_cache_loaded
    if _slug_cache_loaded:
        return
    try:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        r = sess.post(
            _AIRPORT_API,
            json={"language": "en", "siteEdition": _SITE_EDITION},
            headers={
                "em-api-key": _EM_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=10,
        )
        if r.status_code == 200:
            for ap in r.json():
                iata = ap.get("iataCode", "")
                city = ap.get("city", {}).get("name", "")
                if iata and city:
                    _slug_cache[iata] = city.lower().replace(" ", "-")
            logger.info("KLM: cached %d airport slugs", len(_slug_cache))
    except Exception as e:
        logger.warning("KLM: airport cache load failed: %s", e)
    _slug_cache_loaded = True


def _resolve_slug(iata: str) -> str | None:
    """Resolve IATA code to city slug. Static map first, then API cache."""
    slug = _IATA_TO_SLUG.get(iata)
    if slug:
        return slug
    if not _slug_cache_loaded:
        _load_slug_cache_sync()
    return _slug_cache.get(iata)


class KlmConnectorClient:
    """KLM — EveryMundo airTRFX route pages via curl_cffi."""

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

        origin_slug = _resolve_slug(req.origin)
        dest_slug = _resolve_slug(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("KLM: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/{_SITE_EDITION}/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("KLM: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("KLM fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        offers = self._extract_offers(html, req)

        # RT: fetch reverse route for inbound fares
        if req.return_from and offers and dest_slug:
            try:
                _rev_url = f"{_BASE}/{_SITE_EDITION}/flights-from-{dest_slug}-to-{origin_slug}"
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
                            airline="KL",
                            airline_name="KLM Royal Dutch Airlines",
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

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "KLM %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"klm{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EUR",
            offers=offers,
            total_results=len(offers),
        )

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("KLM: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("KLM curl_cffi error: %s", e)
            return None

    def _extract_offers(
        self, html: str, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Extract fare offers from __NEXT_DATA__ DpaHeadline."""
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
            re.S,
        )
        if not m:
            logger.info("KLM: no __NEXT_DATA__ found")
            return []

        try:
            nd = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            logger.warning("KLM: __NEXT_DATA__ JSON parse failed")
            return []

        props = nd.get("props", {}).get("pageProps", {})
        apollo = props.get("apolloState", {}).get("data", {})

        offers: list[FlightOffer] = []
        target_date = req.date_from.strftime("%Y-%m-%d")

        # Extract fares from DpaHeadline objects
        for key, val in apollo.items():
            if not isinstance(val, dict):
                continue
            if val.get("__typename") != "DpaHeadline":
                continue

            meta = val.get("metaData", {})
            if not isinstance(meta, dict):
                continue

            headline = meta.get("headline", {})
            if not isinstance(headline, dict):
                continue

            lowest_fare = headline.get("lowestFare", {})
            if not isinstance(lowest_fare, dict):
                # Might be a __ref
                if isinstance(lowest_fare, dict) and "__ref" in lowest_fare:
                    lowest_fare = apollo.get(lowest_fare["__ref"], {})
                else:
                    continue

            offer = self._build_offer_from_fare(lowest_fare, req, target_date)
            if offer:
                offers.append(offer)

        return offers

    def _build_offer_from_fare(
        self,
        fare: dict,
        req: FlightSearchRequest,
        target_date: str,
    ) -> FlightOffer | None:
        """Build a FlightOffer from a DPA headline fare entry."""
        price = fare.get("totalPrice")
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

        currency = fare.get("currencyCode") or "EUR"
        origin_code = fare.get("originAirportCode") or req.origin
        dest_code = fare.get("destinationAirportCode") or req.destination
        cabin = (fare.get("formattedTravelClass") or "Economy").lower()

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            dep_dt = datetime(2000, 1, 1)

        seg = FlightSegment(
            airline="KL",
            airline_name="KLM Royal Dutch Airlines",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city="",
            destination_city="",
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

        fid = hashlib.md5(
            f"kl_{origin_code}{dest_code}{dep_date_str}{price_f}{cabin}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"kl_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=(
                fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}"
            ),
            outbound=route,
            inbound=None,
            airlines=["KLM"],
            owner_airline="KL",
            booking_url=(
                f"https://www.klm.nl/search/offers"
                f"?origin={req.origin}&destination={req.destination}"
                f"&outboundDate={target_date}"
                f"&adultCount={req.adults or 1}&tripType={'ROUND_TRIP' if req.return_from else 'ONE_WAY'}"
            ),
            is_locked=False,
            source="klm_direct",
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
                    id=f"rt_klm_{cid}", price=price, currency=o.currency,
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
            f"klm{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
