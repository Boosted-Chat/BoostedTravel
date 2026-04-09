"""
PNG Air connector — VARS PSS booking engine via curl_cffi.

PNG Air (IATA: CG) is a domestic airline of Papua New Guinea, based at
Jacksons International Airport (POM). Serves 21 domestic destinations
across PNG with Dash 8-Q400 turboprops.

Strategy (curl_cffi + AJAX):
  1. GET requirementsBS.aspx to establish VARS session + extract tokens
  2. POST GetFlightAvailability AJAX with search criteria → redirects to FlightCal
  3. POST/GET FlightCal.aspx with VarsSessionID → HTML with flight calendar
  4. Parse flt-row divs for times, flight numbers; fare panels for prices
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta

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

_BASE = "https://booking.pngair.com.pg/vars/public"
_REQ_PAGE = f"{_BASE}/CustomerPanels/requirementsBS.aspx"
_AVAIL_URL = f"{_BASE}/WebServices/AvailabilityWS.asmx/GetFlightAvailability"

# All 21 PNG Air domestic airports
_AIRPORTS: set[str] = {
    "KIE", "BUA", "DAU", "GKA", "GUR", "HKN", "KVG", "UNG",
    "LAE", "LNV", "MAG", "MDU", "HGU", "PNP", "POM", "RAB",
    "NIS", "TBG", "TIZ", "VAI", "WWK",
}

_MONTH_ABBR = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


class PNGAirConnectorClient:
    """PNG Air (CG) — VARS PSS booking engine via curl_cffi."""

    def __init__(self, timeout: float = 35.0):
        self.timeout = timeout

    async def close(self) -> None:
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
        if req.origin not in _AIRPORTS or req.destination not in _AIRPORTS:
            return self._empty(req)

        t0 = time.monotonic()

        try:
            offers = await asyncio.get_event_loop().run_in_executor(
                None, self._search_sync, req
            )
        except Exception as e:
            logger.error("PNGAir search error: %s", e)
            return self._empty(req)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info(
            "PNGAir %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        return FlightSearchResponse(
            search_id=self._sid(req),
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "PGK",
            offers=offers,
            total_results=len(offers),
        )

    # ── sync search flow ─────────────────────────────────────────

    def _search_sync(self, req: FlightSearchRequest) -> list[FlightOffer]:
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        try:
            return self._do_search(sess, req)
        finally:
            sess.close()

    def _do_search(
        self, sess: creq.Session, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        # Step 1: establish session
        r1 = sess.get(_REQ_PAGE, timeout=int(self.timeout), headers={
            "Accept": "text/html", "Referer": "https://pngair.com.pg/",
        })
        if r1.status_code != 200:
            logger.warning("PNGAir: page returned %d", r1.status_code)
            return []

        tokens = self._extract_tokens(r1.text)
        if not tokens.get("session_id"):
            logger.warning("PNGAir: no session ID found")
            return []

        # Step 2: submit availability AJAX
        dep_date = self._parse_date(req.date_from)
        if not dep_date:
            return []

        dep_str = f"{dep_date.day}-{_MONTH_ABBR[dep_date.month - 1]}-{dep_date.year}"
        ret_str = None
        if req.return_from:
            ret_date = self._parse_date(req.return_from)
            if ret_date:
                ret_str = f"{ret_date.day}-{_MONTH_ABBR[ret_date.month - 1]}-{ret_date.year}"
        payload = {
            "FormData": {
                "Origin": [req.origin],
                "Destination": [req.destination],
                "DepartureDate": [dep_str],
                "ReturnDate": [ret_str] if ret_str else None,
                "VarsSessionID": tokens["session_id"],
                "IsOpenReturn": False,
                "Currency": "",
                "DisplayCurrency": "",
                "Adults": str(req.adults or 1),
                "Children": str(req.children or 0),
                "SmallChildren": 0,
                "Infants": str(req.infants or 0),
                "Seniors": 0, "Students": 0, "Youths": 0,
                "Teachers": 0, "SeatedInfants": 0,
                "EVoucher": "",
                "recaptcha": "SHOW",
                "SearchUser": "PUBLIC",
                "SearchSource": "requirementsBS",
                "x": 500, "y": 400,
                "rqtm": tokens.get("rqtm", ""),
                "magic": None,
            },
            "IsMMBChangeFlightMode": False,
            "IsRefineSerach": False,
        }

        ajax_headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": _REQ_PAGE,
            "Origin": "https://booking.pngair.com.pg",
            "__RequestVerificationToken": tokens.get("rvt", ""),
            "__SkyFlyTok_V1": tokens.get("skyfly", ""),
        }

        r2 = sess.post(
            _AVAIL_URL, json=payload, timeout=int(self.timeout),
            headers=ajax_headers,
        )
        if r2.status_code != 200:
            logger.warning("PNGAir: availability returned %d", r2.status_code)
            return []

        try:
            data = r2.json().get("d", {})
        except (json.JSONDecodeError, ValueError):
            logger.warning("PNGAir: availability JSON parse failed")
            return []

        if data.get("Result") != "OK":
            logger.warning("PNGAir: availability result=%s", data.get("Result"))
            return []

        next_url = data.get("NextURL", "")
        if not next_url:
            logger.warning("PNGAir: no NextURL")
            return []

        # Step 3: follow to FlightCal page (PostToPage pattern)
        r3 = sess.post(
            next_url,
            data={"VarsSessionID": tokens["session_id"]},
            timeout=int(self.timeout),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": _REQ_PAGE,
                "Origin": "https://booking.pngair.com.pg",
            },
        )
        if r3.status_code != 200:
            logger.warning("PNGAir: FlightCal returned %d", r3.status_code)
            return []

        # Step 4: parse flights from HTML
        return self._parse_flights(r3.text, req, dep_date)

    # ── token extraction ──────────────────────────────────────────

    @staticmethod
    def _extract_tokens(html: str) -> dict[str, str]:
        tokens: dict[str, str] = {}

        m = re.search(r'name="VarsSessionID"\s+value="([^"]*)"', html)
        if m:
            tokens["session_id"] = m.group(1)

        m = re.search(
            r'name="__RequestVerificationToken"\s+[^>]*value="([^"]*)"', html
        )
        if m:
            tokens["rvt"] = m.group(1)

        m = re.search(r"name='SkyFlyTok'\s+value='([^']*)'", html)
        if m:
            tokens["skyfly"] = m.group(1)

        m = re.search(r'name="rqtm"\s+value="([^"]*)"', html)
        if m:
            tokens["rqtm"] = m.group(1)

        return tokens

    # ── HTML parsing ──────────────────────────────────────────────

    def _parse_flights(
        self, html: str, req: FlightSearchRequest, dep_date: datetime
    ) -> list[FlightOffer]:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        # RT: VARS PSS shows OB and IB in separate sections (journey-panel/segment divs)
        # Try to split by section headers or grid panels for IB
        _ib_route: FlightRoute | None = None
        _ib_price = 0.0
        if req.return_from:
            ret_date = self._parse_date(req.return_from) or dep_date
            # VARS uses journey/segment sections; try common class patterns
            sections = soup.find_all(class_=re.compile(r'journey|segment|direction', re.I))
            ib_container = None
            if len(sections) >= 2:
                ib_container = sections[1]  # second section = inbound
            else:
                # Fallback: look for heading text indicating return
                for h in soup.find_all(re.compile(r'h[1-6]|div')):
                    txt = h.get_text(strip=True).lower()
                    if 'return' in txt or 'inbound' in txt:
                        ib_container = h.find_next_sibling() or h.parent
                        break
            if ib_container:
                ib_rows = ib_container.find_all(class_="flt-row")
                best_ib_p = float("inf")
                best_ib_info = None
                for j in range(0, len(ib_rows), 2):
                    ib_detail = ib_rows[j]
                    ib_fare_row = ib_rows[j + 1] if j + 1 < len(ib_rows) else None
                    ib_fi = self._parse_flight_row(ib_detail, ret_date)
                    if not ib_fi:
                        continue
                    ib_fares = self._parse_fare_panels(ib_fare_row) if ib_fare_row else []
                    if not ib_fares:
                        ib_fares = self._parse_fare_panels(ib_detail)
                    for fn, fp, fc, avail in ib_fares:
                        if avail and 0 < fp < best_ib_p:
                            best_ib_p = fp
                            best_ib_info = (ib_fi, fc)
                if best_ib_info and best_ib_p < float("inf"):
                    ib_fi, ib_cur = best_ib_info
                    _ib_price = round(best_ib_p, 2)
                    _ib_route = FlightRoute(
                        segments=[FlightSegment(
                            airline="CG", airline_name="PNG Air",
                            flight_no=ib_fi["flight_no"],
                            origin=req.destination, destination=req.origin,
                            origin_city=ib_fi["origin_city"],
                            destination_city=ib_fi["dest_city"],
                            departure=ib_fi["dep_dt"], arrival=ib_fi["arr_dt"],
                            duration_seconds=ib_fi["duration_s"],
                            cabin_class="economy",
                        )],
                        total_duration_seconds=ib_fi["duration_s"], stopovers=0,
                    )

        # Flight rows come in pairs: even = flight details, odd = fare panels
        # For RT with sections, limit to first section (OB); otherwise use all
        sections_found = soup.find_all(class_=re.compile(r'journey|segment|direction', re.I))
        if req.return_from and len(sections_found) >= 2:
            flt_rows = sections_found[0].find_all(class_="flt-row")
        else:
            flt_rows = soup.find_all(class_="flt-row")

        # Group by pairs: (detail_row, fare_row)
        for i in range(0, len(flt_rows), 2):
            detail_row = flt_rows[i]
            fare_row = flt_rows[i + 1] if i + 1 < len(flt_rows) else None

            flight_info = self._parse_flight_row(detail_row, dep_date)
            if not flight_info:
                continue

            fares = self._parse_fare_panels(fare_row) if fare_row else []
            if not fares:
                # Try fare info from detail row itself
                fares = self._parse_fare_panels(detail_row)
            if not fares:
                # Extract the cheapest price from the row text
                price_m = re.search(
                    r'data-original-amount=["\'](\d+(?:\.\d+)?)["\']',
                    str(detail_row),
                )
                if price_m:
                    fares = [("Basic", float(price_m.group(1)), "PGK", True)]

            for fare_name, price, currency, available in fares:
                if not available or price <= 0:
                    continue

                dedup = f"{flight_info['flight_no']}_{fare_name}_{price}"
                if dedup in seen:
                    continue
                seen.add(dedup)

                offer = self._build_offer(flight_info, fare_name, price, currency, req, _ib_route, _ib_price)
                if offer:
                    offers.append(offer)

        return offers

    @staticmethod
    def _parse_flight_row(row, dep_date: datetime) -> dict | None:
        times = row.find_all(class_="time")
        if len(times) < 2:
            return None

        dep_time_str = times[0].get_text(strip=True)
        arr_time_str = times[1].get_text(strip=True)

        flt_el = row.find(class_="flightnumber")
        flight_no = flt_el.get_text(strip=True) if flt_el else ""

        cities = row.find_all(class_="city")
        origin_city = cities[0].get_text(strip=True) if len(cities) > 0 else ""
        dest_city = cities[1].get_text(strip=True) if len(cities) > 1 else ""

        # Parse times
        try:
            dep_h, dep_m = map(int, dep_time_str.split(":"))
            arr_h, arr_m = map(int, arr_time_str.split(":"))
            dep_dt = dep_date.replace(hour=dep_h, minute=dep_m, second=0)
            arr_dt = dep_date.replace(hour=arr_h, minute=arr_m, second=0)
            if arr_dt <= dep_dt:
                arr_dt += timedelta(days=1)
            duration_s = int((arr_dt - dep_dt).total_seconds())
        except (ValueError, AttributeError):
            dep_dt = dep_date
            arr_dt = dep_date
            duration_s = 0

        return {
            "dep_time": dep_time_str,
            "arr_time": arr_time_str,
            "dep_dt": dep_dt,
            "arr_dt": arr_dt,
            "duration_s": duration_s,
            "flight_no": flight_no,
            "origin_city": origin_city,
            "dest_city": dest_city,
        }

    @staticmethod
    def _parse_fare_panels(container) -> list[tuple[str, float, str, bool]]:
        """Extract fares from panels with data-fareid attributes.

        Returns list of (fare_name, price, currency, available).
        """
        fares: list[tuple[str, float, str, bool]] = []
        panels = container.find_all(attrs={"data-fareid": True})

        for panel in panels:
            classband = panel.get("data-classband", "")
            if not classband:
                continue

            price_el = panel.find(attrs={"data-original-amount": True})
            if not price_el:
                # Sold out — no price
                fares.append((classband, 0.0, "PGK", False))
                continue

            try:
                price = float(price_el["data-original-amount"])
            except (ValueError, KeyError):
                continue

            currency = (
                price_el.get("data-original-currency", "pgk").upper()
            )

            # Check if sold out
            sold_text = panel.find(string=re.compile(r"Sold out", re.I))
            available = not bool(sold_text)

            fares.append((classband, price, currency, available))

        return fares

    def _build_offer(
        self,
        flight: dict,
        fare_name: str,
        price: float,
        currency: str,
        req: FlightSearchRequest,
        _ib_route: FlightRoute | None = None,
        _ib_price: float = 0.0,
    ) -> FlightOffer:
        cabin = "economy"
        if fare_name in ("Corporate",):
            cabin = "premium_economy"

        seg = FlightSegment(
            airline="CG",
            airline_name="PNG Air",
            flight_no=flight["flight_no"],
            origin=req.origin,
            destination=req.destination,
            origin_city=flight["origin_city"],
            destination_city=flight["dest_city"],
            departure=flight["dep_dt"],
            arrival=flight["arr_dt"],
            duration_seconds=flight["duration_s"],
            cabin_class=cabin,
        )
        route = FlightRoute(
            segments=[seg],
            total_duration_seconds=flight["duration_s"],
            stopovers=0,
        )

        is_rt = _ib_route is not None
        total_price = round(price + _ib_price, 2) if is_rt else price
        prefix = "cg_rt_" if is_rt else "cg_"

        fid = hashlib.md5(
            f"cg_{flight['flight_no']}_{req.date_from}_{total_price}_{fare_name}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"{prefix}{fid}",
            price=total_price,
            currency=currency,
            price_formatted=f"{total_price:.2f} {currency}",
            outbound=route,
            inbound=_ib_route,
            airlines=["PNG Air"],
            owner_airline="CG",
            booking_url=(
                f"https://booking.pngair.com.pg/vars/public/CustomerPanels/"
                f"requirementsBS.aspx"
            ),
            is_locked=False,
            source="pngair_direct",
            source_tier="free",
        )

    # ── helpers ───────────────────────────────────────────────────

    @staticmethod
    def _parse_date(date_val: str | object) -> datetime | None:
        if isinstance(date_val, datetime):
            return date_val
        if hasattr(date_val, "year"):
            # datetime.date object
            return datetime(date_val.year, date_val.month, date_val.day)
        s = str(date_val)
        for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    def _sid(self, req: FlightSearchRequest) -> str:
        h = hashlib.md5(
            f"pngair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return f"fs_{h}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id=self._sid(req),
            origin=req.origin,
            destination=req.destination,
            currency="PGK",
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
                    id=f"rt_pnga_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
