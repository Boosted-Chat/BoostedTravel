"""
I Want That Flight connector — Australian fare aggregator HTML scraping.

iwantthatflight.com.au aggregates fares from all major Australian carriers
(Qantas, Jetstar, Virgin Australia, Rex, etc.) for domestic + international
routes that touch Australia.

Strategy (curl_cffi — simple static page scraping):
  1. Build route URL: /Flights-From-{Origin}-to-{Dest}-{ORIG}{DEST}.aspx
  2. Parse FlightRow divs: data-price, data-depart, data-return, airline codes
  3. Group by AirlineCodesGroupHeader for carrier info
  4. Return cheapest return-fare offers per airline-combo / date combo
"""

from __future__ import annotations

import asyncio
import hashlib
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

_BASE = "https://iwantthatflight.com.au"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

# IATA → URL slug for common Australian + international airports.
# IWTF is tolerant of city names — the IATA pair at the end is what counts.
# Unknown airports fall back to the IATA code itself as the slug.
_IATA_TO_SLUG: dict[str, str] = {
    # Australia domestic
    "SYD": "Sydney",
    "MEL": "Melbourne",
    "BNE": "Brisbane",
    "PER": "Perth",
    "ADL": "Adelaide",
    "CBR": "Canberra",
    "OOL": "Gold-Coast",
    "CNS": "Cairns",
    "HBA": "Hobart",
    "DRW": "Darwin",
    "TSV": "Townsville",
    "ASP": "Alice-Springs",
    "BME": "Broome",
    "LST": "Launceston",
    "MCY": "Sunshine-Coast",
    "BNK": "Ballina",
    "AYQ": "Uluru",
    "KNX": "Kununurra",
    "LEA": "Exmouth",
    "NTL": "Newcastle",
    "AVV": "Avalon",
    "ARM": "Armidale",
    "HTI": "Hamilton-Island",
    "PPP": "Proserpine",
    "ROK": "Rockhampton",
    "MKY": "Mackay",
    "ISA": "Mount-Isa",
    "ABX": "Albury",
    "WGA": "Wagga-Wagga",
    "DBO": "Dubbo",
    "CFS": "Coffs-Harbour",
    "PQQ": "Port-Macquarie",
    "TMW": "Tamworth",
    # Popular international from AU
    "AKL": "Auckland",
    "WLG": "Wellington",
    "CHC": "Christchurch",
    "ZQN": "Queenstown",
    "DPS": "Bali",
    "SIN": "Singapore",
    "KUL": "Kuala-Lumpur",
    "BKK": "Bangkok",
    "HKG": "Hong-Kong",
    "NRT": "Tokyo",
    "HND": "Tokyo-Haneda",
    "ICN": "Seoul",
    "LAX": "Los-Angeles",
    "SFO": "San-Francisco",
    "LHR": "London",
    "DXB": "Dubai",
    "DOH": "Doha",
    "FJI": "Fiji",
    "NAN": "Nadi",
    "PPT": "Papeete",
    "NOU": "Noumea",
}

# Regex to extract FlightRow data attributes
_ROW_RE = re.compile(
    r"<div class='FlightRow[^']*'\s*"
    r"data-price='(\d+)'\s*"
    r"data-url='([^']*)'\s*"
    r"data-depart='([^']*)'\s*"
    r"data-return='([^']*)'\s*"
    r"data-origincode='([A-Z]{3})'\s*"
    r"data-destinationcode='([A-Z]{3})'\s*"
    r"data-seatclassid='(\d+)'"
)

# Regex to extract airline codes from group headers
_GROUP_HEADER_RE = re.compile(
    r"<div class='AirlineCodesGroupHeader'>"
    r".*?<span class='airline-codes'>([^<]+)</span>"
    r".*?</div>",
    re.S,
)


class IWantThatFlightConnectorClient:
    """I Want That Flight (iwantthatflight.com.au) — AU fare aggregator."""

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

        origin_slug = _IATA_TO_SLUG.get(req.origin, req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination, req.destination)

        url = (
            f"{_BASE}/Flights-From-{origin_slug}-to-{dest_slug}"
            f"-{req.origin}{req.destination}.aspx"
        )
        logger.info("IWTF: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("IWTF fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        offers = self._extract_offers(html, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "IWTF %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"iwtf{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="AUD",
            offers=offers,
            total_results=len(offers),
        )

    # ------------------------------------------------------------------
    # Network
    # ------------------------------------------------------------------

    def _fetch_sync(self, url: str) -> str | None:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            r = sess.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("IWTF: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("IWTF curl_cffi error: %s", e)
            return None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _extract_offers(
        self, html: str, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse AirlineCodesGroup blocks → FlightOffer list.

        Each group has a header with airline code(s) and multiple FlightRow
        divs with price/date data attributes.  We emit one FlightOffer per
        unique (airlines, depart-date, price) combination.
        """
        # Build a mapping: position-in-html → airline codes for the group
        # by finding each AirlineCodesGroupHeader and the FlightRows after it.
        group_airlines = self._map_group_airlines(html)

        offers: list[FlightOffer] = []
        seen: set[str] = set()

        for m in _ROW_RE.finditer(html):
            price_str, booking_url, depart_str, return_str, orig, dest, seat_class = (
                m.group(1), m.group(2), m.group(3),
                m.group(4), m.group(5), m.group(6), m.group(7),
            )

            try:
                price = float(price_str)
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue

            # Find which airline group this row belongs to
            row_pos = m.start()
            airlines_str = ""
            for pos, codes in sorted(group_airlines.items()):
                if pos <= row_pos:
                    airlines_str = codes
                else:
                    break

            airline_codes = [c.strip() for c in airlines_str.split(",") if c.strip()] if airlines_str else []

            # Parse dates
            dep_dt = self._parse_date(depart_str)
            ret_dt = self._parse_date(return_str) if return_str else None

            # Dedup key
            dedup = f"{orig}_{dest}_{depart_str}_{return_str}_{price}_{airlines_str}"
            if dedup in seen:
                continue
            seen.add(dedup)

            # Build segment(s) — we only know route-level info, not individual legs
            primary_airline = airline_codes[0] if airline_codes else ""
            seg_out = FlightSegment(
                airline=primary_airline,
                flight_no="",
                origin=orig,
                destination=dest,
                departure=dep_dt,
                arrival=dep_dt,  # exact arrival unknown
                duration_seconds=0,
                cabin_class="economy" if seat_class == "1" else "business",
            )
            outbound = FlightRoute(
                segments=[seg_out], total_duration_seconds=0, stopovers=0
            )

            inbound = None
            if ret_dt:
                seg_in = FlightSegment(
                    airline=primary_airline,
                    flight_no="",
                    origin=dest,
                    destination=orig,
                    departure=ret_dt,
                    arrival=ret_dt,
                    duration_seconds=0,
                    cabin_class="economy" if seat_class == "1" else "business",
                )
                inbound = FlightRoute(
                    segments=[seg_in], total_duration_seconds=0, stopovers=0
                )

            fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
            airline_names = [self._airline_name(c) for c in airline_codes]

            offers.append(
                FlightOffer(
                    id=f"iwtf_{fid}",
                    price=price,
                    currency="AUD",
                    price_formatted=f"${price:.0f} AUD",
                    outbound=outbound,
                    inbound=inbound,
                    airlines=airline_names or ["Unknown"],
                    owner_airline=primary_airline,
                    booking_url=booking_url,
                    source="iwantthatflight",
                    source_tier="free",
                )
            )

        return offers

    def _map_group_airlines(self, html: str) -> dict[int, str]:
        """Return {position: airline_codes_str} for each AirlineCodesGroupHeader."""
        mapping: dict[int, str] = {}
        for m in _GROUP_HEADER_RE.finditer(html):
            mapping[m.start()] = m.group(1).strip()
        return mapping

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        """Parse IWTF date like '31/Mar/26' or '31/Mar/2026'."""
        for fmt in ("%d/%b/%y", "%d/%b/%Y"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _airline_name(code: str) -> str:
        """Best-effort IATA → airline name."""
        names = {
            "QF": "Qantas",
            "JQ": "Jetstar",
            "VA": "Virgin Australia",
            "ZL": "Rex Airlines",
            "TL": "Airnorth",
            "6E": "IndiGo",
            "CZ": "China Southern",
            "TR": "Scoot",
            "SQ": "Singapore Airlines",
            "AK": "AirAsia",
            "D7": "AirAsia X",
            "MH": "Malaysia Airlines",
            "TG": "Thai Airways",
            "CX": "Cathay Pacific",
            "NZ": "Air New Zealand",
            "EK": "Emirates",
            "QR": "Qatar Airways",
            "EY": "Etihad",
            "5J": "Cebu Pacific",
            "Z2": "AirAsia Philippines",
            "FD": "Thai AirAsia",
            "LA": "LATAM",
            "AA": "American Airlines",
            "UA": "United Airlines",
            "DL": "Delta",
            "BA": "British Airways",
            "NH": "ANA",
            "JL": "Japan Airlines",
            "KE": "Korean Air",
            "OZ": "Asiana Airlines",
            "3K": "Jetstar Asia",
            "CI": "China Airlines",
            "BR": "EVA Air",
            "SB": "Aircalin",
            "FJ": "Fiji Airways",
            "PX": "Air Niugini",
        }
        return names.get(code, code)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"iwtf{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="AUD",
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
                    id=f"rt_iwan_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
