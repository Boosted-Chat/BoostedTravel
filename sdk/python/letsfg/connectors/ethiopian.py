"""
Ethiopian Airlines connector — EveryMundo airTRFX fare pages.

Ethiopian Airlines (IATA: ET) is Africa's largest and most profitable airline.
Star Alliance member. ADD (Addis Ababa) hub — Africa's busiest transit hub.
130+ destinations across Africa, Europe, Middle East, Asia, Americas.

Strategy (httpx, no browser):
  Ethiopian uses EveryMundo airTRFX at ethiopianairlines.com.
  1. Fetch route page: ethiopianairlines.com/en-et/flights-from-{o}-to-{d}
  2. Extract __NEXT_DATA__ JSON from <script> tag
  3. Parse StandardFareModule fares from Apollo GraphQL state
  4. Filter by origin/destination airport codes and departure date
  Note: Ethiopian uses /en-et/ locale prefix (not /en/).
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

_BASE = "https://www.ethiopianairlines.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_IATA_TO_SLUG: dict[str, str] = {
    # ---------- City codes (multi-airport) ----------
    "LON": "london", "PAR": "paris", "ROM": "rome",
    "NYC": "new-york", "YTO": "toronto", "BUE": "buenos-aires-bue",
    "OSA": "osaka-osa",
    # ---------- Ethiopia domestic ----------
    "ADD": "addis-ababa", "AWA": "hawassa", "LLI": "lalibela",
    "MQX": "mekelle", "AMH": "arba-minch", "ASO": "asosa",
    "BJR": "bahir-dar", "AXU": "axum", "DIR": "dire-dawa",
    "GDQ": "gondar", "JIM": "jimma", "JIJ": "jijiga",
    "GMB": "gambella", "SZE": "semera", "DSE": "kombolcha",
    "GOB": "goba", "GDE": "gode", "BCO": "jinka", "SHC": "shire",
    # ---------- Horn of Africa ----------
    "HGA": "hargeisa", "JIB": "djibouti", "MGQ": "mogadishu",
    "ASM": "asmara", "BBO": "berbera", "BSA": "bossaso",
    # ---------- East Africa ----------
    "NBO": "nairobi", "MBA": "mombasa", "ZNZ": "zanzibar",
    "DAR": "dar-es-salaam", "JRO": "kilimanjaro",
    "EBB": "entebbe", "KGL": "kigali",
    "BJM": "bujumbura", "SEZ": "mahe", "JUB": "juba",
    # ---------- Southern Africa ----------
    "JNB": "johannesburg", "CPT": "cape-town", "DUR": "durban",
    "PLZ": "port-elizabeth", "HDS": "hoedspruit",
    "LUN": "lusaka", "NLA": "ndola",
    "HRE": "harare", "BUQ": "bulawayo", "VFA": "victoria-falls",
    "WDH": "windhoek", "MPM": "maputo", "BEW": "beira",
    "LLW": "lilongwe", "BLZ": "blantyre",
    "MRU": "port-louis", "MUB": "maun", "GBE": "gaborone",
    # ---------- Indian Ocean ----------
    "TNR": "antananarivo", "NOS": "nosy-be",
    "HAH": "hahaia", "PRI": "praslin-island",
    # ---------- West Africa ----------
    "LOS": "lagos", "ABV": "abuja", "ENU": "enugu", "KAN": "kano",
    "ACC": "accra", "ABJ": "abidjan",
    "DSS": "dakar", "BKO": "bamako",
    "OUA": "ouagadougou", "CKY": "conakry",
    "FNA": "freetown", "LFW": "lome",
    "COO": "cotonou", "NIM": "niamey",
    "ROB": "monrovia", "BJL": "banjul",
    # ---------- Central Africa ----------
    "DLA": "douala", "YAO": "yaounde",
    "NDJ": "ndjamena", "LBV": "libreville",
    "FIH": "kinshasa", "FBM": "lubumbashi",
    "FKI": "kisangani", "GOM": "goma", "MJM": "mbuji-mayi",
    "BZV": "brazzaville", "PNR": "pointe-noire",
    "BGF": "bangui", "SSG": "malabo", "LAD": "luanda",
    # ---------- North Africa ----------
    "CAI": "cairo", "KRT": "khartoum", "PZU": "port-sudan",
    # ---------- Europe ----------
    "LHR": "london", "CDG": "paris", "FRA": "frankfurt",
    "FCO": "rome", "MAD": "madrid", "BRU": "brussels",
    "VIE": "vienna", "IST": "istanbul", "ATH": "athens",
    "MXP": "milan", "ARN": "stockholm", "GVA": "geneva",
    "DUB": "dublin", "MAN": "manchester-city",
    "AMS": "amsterdam", "CPH": "copenhagen",
    "MUC": "munich", "DUS": "dusseldorf",
    "OSL": "oslo", "HEL": "helsinki",
    "WAW": "warsaw", "PRG": "prague",
    "BCN": "barcelona", "LYS": "lyon", "MRS": "marseille",
    "OPO": "porto", "GOT": "gothenburg",
    "BGO": "bergen", "SVG": "stavanger",
    "LLA": "lulea", "TRD": "trondheim",
    "GNB": "grenoble", "MLA": "malta",
    "SVO": "moscow", "ZRH": "zurich",
    # ---------- Middle East ----------
    "DXB": "dubai-dxb", "AUH": "abu-dhabi", "SHJ": "sharjah",
    "DOH": "doha",
    "JED": "jeddah", "RUH": "riyadh", "DMM": "dammam",
    "MED": "medina", "GIZ": "jizan",
    "TLV": "tel-aviv", "AMM": "amman",
    "BAH": "bahrain", "MCT": "muscat",
    "KWI": "kuwait-city", "BEY": "beirut",
    # ---------- Asia ----------
    "PEK": "beijing", "PVG": "shanghai",
    "CAN": "guangzhou", "CTU": "chengdu", "HGH": "hangzhou",
    "HKG": "hong-kong",
    "ICN": "seoul", "NRT": "tokyo", "KIX": "osaka-osa",
    "BOM": "mumbai", "DEL": "delhi", "BLR": "bangalore",
    "AMD": "ahmedabad", "MAA": "chennai", "HYD": "hyderabad",
    "CNN": "kannur", "DAC": "dhaka", "KHI": "karachi",
    "BKK": "bangkok", "KUL": "kuala-lumpur",
    "SIN": "singapore", "CGK": "jakarta",
    "MNL": "manila", "HAN": "hanoi",
    "SYD": "sydney", "MEL": "melbourne",
    # ---------- Americas ----------
    "IAD": "washington-dc", "EWR": "newark",
    "JFK": "new-york", "ORD": "chicago", "IAH": "houston",
    "LAX": "los-angeles", "ATL": "atlanta", "BOS": "boston",
    "DEN": "denver", "DFW": "dallas",
    "SEA": "seattle", "MSP": "minneapolis",
    "SFO": "san-francisco", "SAN": "san-diego", "RDU": "raleigh",
    "YYZ": "toronto", "YUL": "montreal",
    "YYC": "calgary", "YVR": "vancouver",
    "YOW": "ottawa", "YWG": "winnipeg",
    "YEG": "edmonton", "YHZ": "halifax", "YQB": "quebec-yqb",
    "GRU": "sao-paulo", "GIG": "rio-de-janeiro",
    "EZE": "buenos-aires-bue",
}


class EthiopianConnectorClient:
    """Ethiopian Airlines — EveryMundo airTRFX fare pages."""

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
        t0 = time.monotonic()
        client = await self._client()

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("Ethiopian: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/en-et/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Ethiopian: fetching %s", url)

        try:
            resp = await client.get(url)
            if resp.status_code not in (200, 404) or "__NEXT_DATA__" not in resp.text:
                logger.warning("Ethiopian: %s returned %d (no fare data)", url, resp.status_code)
                return self._empty(req)
        except Exception as e:
            logger.error("Ethiopian fetch error: %s", e)
            return self._empty(req)

        fares = self._extract_fares(resp.text)
        if not fares:
            logger.info("Ethiopian: no fares on page %s", url)
            return self._empty(req)

        offers = self._build_offers(fares, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Ethiopian %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"ethiopian{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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

    def _build_offers(self, fares: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)

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
        use_fares = exact_fares if exact_fares else nearby_fares

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
                airline="ET",
                airline_name="Ethiopian Airlines",
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
                f"et_{orig}{dest}{dep_date}{price_f}{cabin}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"et_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Ethiopian Airlines"],
                owner_airline="ET",
                booking_url=(
                    f"https://www.ethiopianairlines.com/book/"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={target_date}"
                    f"&adults={req.adults or 1}&tripType={'R' if req.return_from else 'O'}"
                ),
                is_locked=False,
                source="ethiopian_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"ethiopian{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
