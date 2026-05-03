"""
Shared base for Lufthansa Group connectors (LH, LX, OS, SN).

All LH Group airlines share the same aircore CMS platform. The
lufthansa.com/xx/en/flights/ pages contain JSON-LD structured data
with flight schedules and lowest-fare Product entries for routes
across all LH Group hubs (FRA, MUC, ZRH, VIE, BRU, etc.).

Each airline connector subclasses this with its own IATA code, name,
booking URL pattern, and source identifier.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
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

logger = logging.getLogger(__name__)

# IATA code -> URL slug mapping for lufthansa.com flight pages.
# Shared across all LH Group connectors. Slugs are lowercase-hyphenated
# city names. Multi-airport cities map to the primary airport.
IATA_TO_SLUG: dict[str, str] = {
    # ── Germany ──
    "FRA": "frankfurt", "MUC": "munich", "BER": "berlin", "HAM": "hamburg",
    "DUS": "dusseldorf", "STR": "stuttgart", "CGN": "cologne", "HAJ": "hannover",
    "NUE": "nuremberg", "LEJ": "leipzig", "BRE": "bremen", "DTM": "dortmund",
    "DRS": "dresden", "FMO": "muenster", "PAD": "paderborn",
    # ── Austria ──
    "VIE": "vienna", "GRZ": "graz", "SZG": "salzburg", "INN": "innsbruck",
    "LNZ": "linz",
    # ── Switzerland ──
    "ZRH": "zurich", "GVA": "geneva", "BSL": "basel", "BRN": "bern",
    # ── Belgium ──
    "BRU": "brussels",
    # ── UK & Ireland ──
    "LHR": "london", "LCY": "london", "LGW": "london", "STN": "london",
    "MAN": "manchester", "EDI": "edinburgh", "BHX": "birmingham",
    "GLA": "glasgow", "BRS": "bristol", "NCL": "newcastle",
    "DUB": "dublin", "SNN": "shannon", "ORK": "cork",
    # ── France ──
    "CDG": "paris", "ORY": "paris", "NCE": "nice", "LYS": "lyon",
    "MRS": "marseille", "TLS": "toulouse", "BOD": "bordeaux",
    "NTE": "nantes", "SXB": "strasbourg",
    # ── Italy ──
    "FCO": "rome", "MXP": "milan", "LIN": "milan", "VCE": "venice",
    "NAP": "naples", "CTA": "catania", "PMO": "palermo", "BLQ": "bologna",
    "FLR": "florence", "PSA": "pisa", "TRN": "turin", "OLB": "olbia",
    "CAG": "cagliari",
    # ── Spain & Portugal ──
    "BCN": "barcelona", "MAD": "madrid", "PMI": "palma-de-mallorca",
    "AGP": "malaga", "VLC": "valencia", "ALC": "alicante",
    "SVQ": "seville", "BIO": "bilbao", "TFS": "tenerife",
    "LPA": "gran-canaria", "IBZ": "ibiza",
    "LIS": "lisbon", "OPO": "porto", "FAO": "faro",
    # ── Scandinavia ──
    "CPH": "copenhagen", "ARN": "stockholm", "GOT": "gothenburg",
    "OSL": "oslo", "BGO": "bergen", "TRD": "trondheim", "SVG": "stavanger",
    "HEL": "helsinki", "TMP": "tampere", "OUL": "oulu",
    "BLL": "billund", "AAL": "aalborg",
    # ── Eastern Europe ──
    "WAW": "warsaw", "KRK": "krakow", "GDN": "gdansk", "WRO": "wroclaw",
    "POZ": "poznan", "KTW": "katowice",
    "PRG": "prague", "BRQ": "brno",
    "BUD": "budapest",
    "OTP": "bucharest", "CLJ": "cluj-napoca", "TSR": "timisoara",
    "SOF": "sofia", "VAR": "varna", "BOJ": "burgas",
    "BEG": "belgrade", "NIS": "nis",
    "ZAG": "zagreb", "SPU": "split", "DBV": "dubrovnik",
    "LJU": "ljubljana", "SJJ": "sarajevo",
    "SKP": "skopje", "TIA": "tirana", "TGD": "podgorica",
    # ── Benelux ──
    "AMS": "amsterdam", "EIN": "eindhoven", "RTM": "rotterdam",
    "LUX": "luxembourg",
    # ── Greece & Cyprus ──
    "ATH": "athens", "SKG": "thessaloniki", "HER": "heraklion",
    "CFU": "corfu", "RHO": "rhodes", "KGS": "kos", "JTR": "santorini",
    "CHQ": "chania",
    "LCA": "larnaca", "PFO": "paphos",
    # ── Turkey ──
    "IST": "istanbul", "ESB": "ankara", "AYT": "antalya",
    "ADB": "izmir", "DLM": "dalaman", "BJV": "bodrum",
    # ── Baltics ──
    "RIX": "riga", "TLL": "tallinn", "VNO": "vilnius",
    # ── Other EU ──
    "KEF": "reykjavik", "MLA": "malta", "KIV": "chisinau",
    # ── Americas ──
    "JFK": "new-york", "EWR": "new-york",
    "IAD": "washington", "DCA": "washington",
    "ORD": "chicago", "LAX": "los-angeles", "SFO": "san-francisco",
    "BOS": "boston", "MIA": "miami", "FLL": "fort-lauderdale",
    "ATL": "atlanta", "DFW": "dallas", "IAH": "houston",
    "DEN": "denver", "SEA": "seattle", "DTW": "detroit",
    "MSP": "minneapolis", "PHL": "philadelphia", "CLT": "charlotte",
    "MCO": "orlando", "TPA": "tampa", "SAN": "san-diego",
    "AUS": "austin", "RDU": "raleigh-durham",
    "YYZ": "toronto", "YVR": "vancouver", "YUL": "montreal",
    "YYC": "calgary", "YOW": "ottawa",
    "MEX": "mexico-city", "CUN": "cancun",
    "GRU": "sao-paulo", "GIG": "rio-de-janeiro",
    "EZE": "buenos-aires", "BOG": "bogota",
    "SCL": "santiago-de-chile", "LIM": "lima", "PTY": "panama-city",
    # ── Asia ──
    "NRT": "tokyo", "HND": "tokyo", "KIX": "osaka",
    "PEK": "beijing", "PVG": "shanghai", "CAN": "guangzhou",
    "HKG": "hong-kong", "ICN": "seoul",
    "SIN": "singapore", "BKK": "bangkok", "KUL": "kuala-lumpur",
    "CGK": "jakarta", "MNL": "manila",
    "DEL": "new-delhi", "BOM": "mumbai", "BLR": "bangalore",
    "MAA": "chennai", "HYD": "hyderabad", "CCU": "kolkata",
    "CMB": "colombo", "MLE": "male", "KTM": "kathmandu",
    "DAC": "dhaka", "ISB": "islamabad", "KHI": "karachi", "LHE": "lahore",
    "HAN": "hanoi", "SGN": "ho-chi-minh-city", "TPE": "taipei",
    "RGN": "yangon", "PNH": "phnom-penh",
    # ── Middle East ──
    "DXB": "dubai", "AUH": "abu-dhabi", "DOH": "doha",
    "RUH": "riyadh", "JED": "jeddah", "BAH": "bahrain",
    "MCT": "muscat", "KWI": "kuwait", "AMM": "amman",
    "BEY": "beirut", "TLV": "tel-aviv", "CAI": "cairo",
    # ── Africa ──
    "JNB": "johannesburg", "CPT": "cape-town", "NBO": "nairobi",
    "ADD": "addis-ababa", "LOS": "lagos", "ACC": "accra",
    "DAR": "dar-es-salaam", "CMN": "casablanca", "TUN": "tunis",
    "ALG": "algiers", "MRU": "mauritius",
    # ── Oceania ──
    "SYD": "sydney", "MEL": "melbourne", "BNE": "brisbane",
    "PER": "perth", "AKL": "auckland",
    # ── City codes (multi-airport cities) ──
    "LON": "london", "NYC": "new-york", "PAR": "paris", "ROM": "rome",
    "MIL": "milan", "WAS": "washington", "CHI": "chicago", "TYO": "tokyo",
    "OSA": "osaka", "SEL": "seoul", "BJS": "beijing", "SHA": "shanghai",
    "BUE": "buenos-aires", "STO": "stockholm", "REK": "reykjavik",
}

_BASE_URL = "https://www.lufthansa.com/xx/en/flights"

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min

_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Rotate fingerprints to avoid WAF blocks on a single TLS profile
_FINGERPRINTS = ["chrome136", "chrome133a", "chrome131", "chrome124", "chrome120"]


class LHGroupBaseConnector:
    """Base connector for all Lufthansa Group airlines.

    Subclasses must set:
        AIRLINE_CODE:  e.g. "LH"
        AIRLINE_NAME:  e.g. "Lufthansa"
        SOURCE_KEY:    e.g. "lufthansa_direct"
        DEFAULT_CURRENCY: e.g. "EUR"
        BOOKING_URL_TEMPLATE: format string with {origin}, {destination},
                              {date}, {adults}, {children}, {infants}
    """

    AIRLINE_CODE: str = "LH"
    AIRLINE_NAME: str = "Lufthansa"
    SOURCE_KEY: str = "lufthansa_direct"
    DEFAULT_CURRENCY: str = "EUR"
    BOOKING_URL_TEMPLATE: str = (
        "https://www.lufthansa.com/xx/en/flight-search?"
        "origin={origin}&destination={destination}"
        "&outbound-date={date}"
        "&adults={adults}&children={children}"
        "&infants={infants}&cabin-class={cabin}&trip-type=ONE_WAY"
    )

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        origin_slug = IATA_TO_SLUG.get(req.origin)
        dest_slug = IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning(
                "%s: missing route slug mapping origin=%s(%s) destination=%s(%s)",
                self.AIRLINE_NAME,
                req.origin,
                origin_slug,
                req.destination,
                dest_slug,
            )
            return self._empty(req)

        url = f"{_BASE_URL}/flight-{origin_slug}-{dest_slug}"

        try:
            resp = None
            last_exc = None
            # Try up to 2 fingerprints before giving up
            for fp in random.sample(_FINGERPRINTS, min(2, len(_FINGERPRINTS))):
                try:
                    with creq.Session(impersonate=fp, proxies=get_curl_cffi_proxies()) as sess:
                        resp = sess.get(url, timeout=self.timeout, headers=_HEADERS)
                    if resp.status_code == 200:
                        break
                    logger.warning("%s: %s returned %d (fp=%s)", self.AIRLINE_NAME, url, resp.status_code, fp)
                    resp = None
                except Exception as e:
                    last_exc = e
                    logger.debug("%s: fp=%s failed: %s", self.AIRLINE_NAME, fp, e)

            if resp is None or resp.status_code != 200:
                if last_exc:
                    logger.warning("%s: all fingerprints failed, last error: %s", self.AIRLINE_NAME, last_exc)
                return self._empty(req)

            flights, product = self._extract_jsonld(resp.text)
            if not flights and not product:
                logger.warning("%s: no JSON-LD on %s", self.AIRLINE_NAME, url)
                return self._empty(req)

            # RT: fetch reverse route page for inbound fares
            ib_route = None
            ib_price = 0.0
            if req.return_from and dest_slug != origin_slug:
                rev_url = f"{_BASE_URL}/flight-{dest_slug}-{origin_slug}"
                try:
                    rev_resp = None
                    for fp2 in random.sample(_FINGERPRINTS, min(2, len(_FINGERPRINTS))):
                        try:
                            with creq.Session(impersonate=fp2, proxies=get_curl_cffi_proxies()) as s2:
                                rev_resp = s2.get(rev_url, timeout=self.timeout, headers=_HEADERS)
                            if rev_resp and rev_resp.status_code == 200:
                                break
                            rev_resp = None
                        except Exception:
                            pass
                    if rev_resp and rev_resp.status_code == 200:
                        ib_flights, ib_product = self._extract_jsonld(rev_resp.text)
                        ib_p = ib_product["price"] if ib_product else 0
                        ret_date = req.return_from
                        ret_str = ret_date.strftime("%Y-%m-%d") if hasattr(ret_date, "strftime") else str(ret_date)
                        _lh_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                        if ib_flights:
                            ib_flt = ib_flights[0]
                            ib_prov = ib_flt.get("provider", {})
                            ib_seg = FlightSegment(
                                airline=ib_prov.get("iataCode", self.AIRLINE_CODE),
                                airline_name=ib_prov.get("name", self.AIRLINE_NAME),
                                flight_no=f"{ib_prov.get('iataCode', self.AIRLINE_CODE)}{ib_flt.get('flightNumber', '')}",
                                origin=ib_flt.get("departureAirport", {}).get("iataCode", req.destination),
                                destination=ib_flt.get("arrivalAirport", {}).get("iataCode", req.origin),
                                departure=datetime.combine(ret_date, datetime.min.time()) if not isinstance(ret_date, datetime) else ret_date,
                                arrival=datetime.combine(ret_date, datetime.min.time()) if not isinstance(ret_date, datetime) else ret_date,
                                duration_seconds=0,
                                cabin_class=_lh_cabin,
                            )
                            ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)
                            ib_price = ib_p
                        elif ib_p > 0:
                            ib_seg = FlightSegment(
                                airline=self.AIRLINE_CODE,
                                airline_name=self.AIRLINE_NAME,
                                flight_no="",
                                origin=req.destination,
                                destination=req.origin,
                                departure=datetime.combine(ret_date, datetime.min.time()) if not isinstance(ret_date, datetime) else ret_date,
                                arrival=datetime.combine(ret_date, datetime.min.time()) if not isinstance(ret_date, datetime) else ret_date,
                                duration_seconds=0,
                                cabin_class=_lh_cabin,
                            )
                            ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)
                            ib_price = ib_p
                except Exception as e:
                    logger.debug("%s: reverse route fetch failed: %s", self.AIRLINE_NAME, e)

            offers = self._build_offers(flights, product, req, ib_route=ib_route, ib_price=ib_price)
            elapsed = time.monotonic() - t0

            offers.sort(key=lambda o: o.price)
            logger.info(
                "%s %s->%s: %d offers in %.1fs",
                self.AIRLINE_NAME, req.origin, req.destination, len(offers), elapsed,
            )

            h = hashlib.md5(
                f"{self.AIRLINE_CODE}{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            _ob_result = FlightSearchResponse(
                search_id=f"{self.AIRLINE_CODE.lower()}_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=product.get("priceCurrency", self.DEFAULT_CURRENCY) if product else self.DEFAULT_CURRENCY,
                offers=offers,
                total_results=len(offers),
            )
            if _ob_result.offers:
                try:
                    _anc = await asyncio.wait_for(
                        self._fetch_ancillaries(req.origin, req.destination, req.date_from.isoformat(), req.adults, _ob_result.currency),
                        timeout=45.0,
                    )
                    if _anc:
                        self._apply_ancillaries(_ob_result.offers, _anc)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
                except Exception as _anc_err:
                    logger.debug("Ancillary fetch error %s->%s: %s", req.origin, req.destination, _anc_err)
            return _ob_result

        except Exception as e:
            logger.error("%s error: %s", self.AIRLINE_NAME, e)
            return self._empty(req)

    @staticmethod
    def _extract_jsonld(html: str) -> tuple[list[dict], Optional[dict]]:
        blocks = re.findall(
            r'<script type="application/ld\+json">\s*(.*?)\s*</script>',
            html,
            re.DOTALL,
        )
        flights: list[dict] = []
        product: Optional[dict] = None
        for raw in blocks:
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue
            schema_type = data.get("@type")
            if schema_type == "Flight":
                flights.append(data)
            elif schema_type == "Product":
                offers = data.get("offers", {})
                if isinstance(offers, dict) and offers.get("price"):
                    product = {
                        "price": float(offers["price"]),
                        "priceCurrency": offers.get("priceCurrency", "EUR"),
                        "url": offers.get("url", ""),
                    }
        return flights, product

    def _build_offers(
        self,
        flights: list[dict],
        product: Optional[dict],
        req: FlightSearchRequest,
        *,
        ib_route: Optional[FlightRoute] = None,
        ib_price: float = 0.0,
    ) -> list[FlightOffer]:
        dep_date = req.date_from
        price = product["price"] if product else 0
        currency = product.get("priceCurrency", self.DEFAULT_CURRENCY) if product else self.DEFAULT_CURRENCY

        # If no individual flights, return one route-level offer
        if not flights and product and price > 0:
            return [self._make_offer(
                flight_no="",
                airline_code=self.AIRLINE_CODE,
                airline_name=self.AIRLINE_NAME,
                origin=req.origin,
                destination=req.destination,
                dep_time="",
                arr_time="",
                dep_date=dep_date,
                price=price,
                currency=currency,
                req=req,
                ib_route=ib_route,
                ib_price=ib_price,
            )]

        offers: list[FlightOffer] = []
        for flt in flights:
            provider = flt.get("provider", {})
            airline_code = provider.get("iataCode", self.AIRLINE_CODE)
            airline_name = provider.get("name", self.AIRLINE_NAME)

            offers.append(self._make_offer(
                flight_no=flt.get("flightNumber", ""),
                airline_code=airline_code,
                airline_name=airline_name,
                origin=flt.get("departureAirport", {}).get("iataCode", req.origin),
                destination=flt.get("arrivalAirport", {}).get("iataCode", req.destination),
                dep_time=flt.get("departureTime", ""),
                arr_time=flt.get("arrivalTime", ""),
                dep_date=dep_date,
                price=price,
                currency=currency,
                req=req,
                ib_route=ib_route,
                ib_price=ib_price,
            ))

        return offers

    def _make_offer(
        self,
        *,
        flight_no: str,
        airline_code: str,
        airline_name: str,
        origin: str,
        destination: str,
        dep_time: str,
        arr_time: str,
        dep_date,
        price: float,
        currency: str,
        req: FlightSearchRequest,
        ib_route: Optional[FlightRoute] = None,
        ib_price: float = 0.0,
    ) -> FlightOffer:
        dep_dt = dep_date
        arr_dt = dep_date
        duration = 0

        if dep_time and arr_time:
            try:
                dep_t = datetime.strptime(dep_time, "%H:%M:%S")
                arr_t = datetime.strptime(arr_time, "%H:%M:%S")
                dep_dt = datetime.combine(dep_date, dep_t.time())
                arr_dt = datetime.combine(dep_date, arr_t.time())
                if arr_dt <= dep_dt:
                    arr_dt += timedelta(days=1)
                duration = int((arr_dt - dep_dt).total_seconds())
            except ValueError:
                pass

        display_fn = f"{airline_code}{flight_no}" if flight_no else ""
        dep_date_str = dep_date.strftime("%Y-%m-%d") if hasattr(dep_date, "strftime") else str(dep_date)

        _lh_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segment = FlightSegment(
            airline=airline_code or self.AIRLINE_CODE,
            airline_name=airline_name,
            flight_no=display_fn,
            origin=origin,
            destination=destination,
            departure=dep_dt if isinstance(dep_dt, datetime) else datetime.combine(dep_dt, datetime.min.time()),
            arrival=arr_dt if isinstance(arr_dt, datetime) else datetime.combine(arr_dt, datetime.min.time()),
            duration_seconds=duration,
            cabin_class=_lh_cabin,
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=duration,
            stopovers=0,
        )

        fid = hashlib.md5(
            f"{self.AIRLINE_CODE}_{origin}{destination}{dep_date_str}{flight_no}{price}".encode()
        ).hexdigest()[:12]

        total_price = round(price + ib_price, 2) if ib_route else round(price, 2)
        offer_id = f"{self.AIRLINE_CODE.lower()}_rt_{fid}" if ib_route else f"{self.AIRLINE_CODE.lower()}_{fid}"

        booking_url = self.BOOKING_URL_TEMPLATE.format(
            origin=origin,
            destination=destination,
            date=dep_date_str,
            adults=req.adults,
            children=req.children,
            infants=req.infants,
            cabin={"M": "economy", "W": "premium-economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy"),
        )
        # Upgrade booking URL to round-trip when return date is present
        if req.return_from:
            ret_str = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, "strftime") else str(req.return_from)
            booking_url = booking_url.replace(
                "trip-type=ONE_WAY",
                f"trip-type=ROUND_TRIP&inbound-date={ret_str}",
            ).replace(
                "tripType=ONE_WAY",
                f"tripType=ROUND_TRIP&returnDate={ret_str}",
            )

        return FlightOffer(
            id=offer_id,
            price=total_price,
            currency=currency,
            price_formatted=f"{total_price:.0f} {currency}",
            outbound=route,
            inbound=ib_route,
            airlines=[airline_name],
            owner_airline=airline_code or self.AIRLINE_CODE,
            booking_url=booking_url,
            is_locked=False,
            source=self.SOURCE_KEY,
            source_tier="free",
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"{self.AIRLINE_CODE}{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"{self.AIRLINE_CODE.lower()}_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or self.DEFAULT_CURRENCY,
            offers=[],
            total_results=0,
        )

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        global _ancillary_cache
        cache_key = f"{self.AIRLINE_CODE}:{origin}:{dest}"
        now = time.monotonic()
        if cache_key in _ancillary_cache:
            ts, data = _ancillary_cache[cache_key]
            if now - ts < _ANCILLARY_CACHE_TTL:
                return data
        # Try live prober first
        try:
            from .ancillary_live_probe import probe_ancillaries
            live = await probe_ancillaries(self.AIRLINE_CODE, origin, dest, date_str=date_str)
            if live:
                _ancillary_cache[cache_key] = (now, live)
                return live
        except Exception:
            pass
        # Static fallback: Economy Light fare pricing.
        result: dict = {
            "bags_note": f"Carry-on bag (up to 8 kg) included.",
            "checked_bag": (
                f"No free checked bag (Economy Light fare). "
                f"First checked bag: {self.DEFAULT_CURRENCY} 35."
            ),
            "seat_note": f"Seat selection from {self.DEFAULT_CURRENCY} 10.",
            "bags_from": 35.0,   # Economy Light: first checked bag EUR 35 (exact)
            "currency": self.DEFAULT_CURRENCY,
        }
        _ancillary_cache[cache_key] = (now, result)
        return result

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note", "")
        checked_note = ancillary.get("checked_bag") or ancillary.get("checked_bag_note") or bags_note
        seat_note = ancillary.get("seat_note", "")
        # Support both static fallback key ("bags_from") and live probe key ("checked_bag_from"/"checked_bag_price")
        bags_from = ancillary.get("checked_bag_from") or ancillary.get("checked_bag_price") or ancillary.get("bags_from")
        for offer in offers:
            # carry-on always free on LH Group fares; checked bag is a paid add-on
            offer.bags_price.setdefault("carry_on", 0.0)
            if bags_from is not None:
                offer.bags_price.setdefault("checked_bag", float(bags_from))
            if bags_note:
                offer.conditions.setdefault("carry_on", bags_note)
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions.setdefault("seat", seat_note)
