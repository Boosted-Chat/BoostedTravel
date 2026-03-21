"""
Airnorth connector — .NET B2C form-POST fare scraping via curl_cffi.

Airnorth (IATA: TL) is an Australian regional airline based in Darwin,
Northern Territory.  Serves ~17 destinations across northern Australia
and Dili (Timor-Leste).

Strategy (curl_cffi required — .NET antiforgery token):
  1. GET /AirnorthB2C/Booking/Search → extract __RequestVerificationToken
  2. POST search form with origin/dest/date → redirects to /Booking/Select
  3. Parse flight-strip HTML: times, flight numbers, stops, fare prices
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import datetime, timedelta

from curl_cffi import requests as creq

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE = "https://secure.airnorth.com.au/AirnorthB2C"
_SEARCH_URL = f"{_BASE}/Booking/Search"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Origin": "https://secure.airnorth.com.au",
    "Referer": f"{_BASE}/Booking/Search",
}

# Airnorth destinations — all destinations served
_VALID_IATA: set[str] = {
    "DRW",  # Darwin
    "ASP",  # Alice Springs
    "BME",  # Broome
    "CNS",  # Cairns
    "DIL",  # Dili (Timor-Leste)
    "ELC",  # Elcho Island
    "GOV",  # Gove / Nhulunbuy
    "GTE",  # Groote Eylandt
    "KTR",  # Katherine / Tindal
    "KNX",  # Kununurra
    "MNG",  # Maningrida
    "MCV",  # McArthur River
    "MGT",  # Milingimbi
    "TCA",  # Tennant Creek
    "WTB",  # Toowoomba (Wellcamp)
    "TSV",  # Townsville
}


class AirnorthConnectorClient:
    """Airnorth — .NET B2C form POST via curl_cffi."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        if req.origin not in _VALID_IATA or req.destination not in _VALID_IATA:
            logger.warning(
                "Airnorth: unsupported route %s→%s", req.origin, req.destination
            )
            return self._empty(req)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, req
            )
        except Exception as e:
            logger.error("Airnorth fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        offers = self._extract_offers(html, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "Airnorth %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"airnorth{req.origin}{req.destination}{req.date_from}".encode()
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

    def _fetch_sync(self, req: FlightSearchRequest) -> str | None:
        sess = creq.Session(impersonate="chrome124")
        try:
            # Step 1: GET search page for CSRF token + cookies
            r1 = sess.get(_SEARCH_URL, headers=_HEADERS, timeout=int(self.timeout))
            if r1.status_code != 200:
                logger.warning("Airnorth: search page returned %d", r1.status_code)
                return None

            m = re.search(
                r'name="__RequestVerificationToken"[^>]+value="([^"]+)"', r1.text
            )
            if not m:
                logger.warning("Airnorth: CSRF token not found")
                return None
            token = m.group(1)

            # Step 2: POST search form
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_str = dep_date.strftime("%d %b %Y")  # e.g. "20 Apr 2026"

            form_data = {
                "__RequestVerificationToken": token,
                "SearchViewModel.TripType": "One-Way",
                "SearchViewModel.FlightSearches[0].Origin": req.origin,
                "SearchViewModel.FlightSearches[0].Destination": req.destination,
                "SearchViewModel.FlightSearches[0].DepartureDate": date_str,
                "SearchViewModel.AdultCount": str(req.adults or 1),
                "SearchViewModel.ChildCount": str(req.children or 0),
                "SearchViewModel.InfantCount": str(req.infants or 0),
                "assist": "No",
            }

            r2 = sess.post(
                _SEARCH_URL,
                data=form_data,
                headers=_HEADERS,
                timeout=int(self.timeout),
                allow_redirects=True,
            )
            if r2.status_code != 200:
                logger.warning("Airnorth: POST returned %d", r2.status_code)
                return None

            if "Booking/Select" not in str(r2.url):
                logger.warning("Airnorth: unexpected redirect to %s", r2.url)
                return None

            return r2.text

        except Exception as e:
            logger.warning("Airnorth curl_cffi error: %s", e)
            return None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _extract_offers(
        self, html: str, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse flight-strip blocks → FlightOffer list."""
        offers: list[FlightOffer] = []

        # Split on flight-strip boundaries
        strips = re.split(r'<div class="flight-strip">', html)
        if len(strips) < 2:
            logger.info("Airnorth: no flight-strip blocks found")
            return []

        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        for strip_html in strips[1:]:  # first element is before first strip
            flight_info = self._parse_strip(strip_html, req, dep_date)
            if flight_info:
                offers.extend(flight_info)

        return offers

    def _parse_strip(
        self,
        strip_html: str,
        req: FlightSearchRequest,
        dep_date: datetime,
    ) -> list[FlightOffer]:
        """Parse a single flight-strip block into offers (one per fare class)."""
        results: list[FlightOffer] = []

        # Extract departure/arrival times
        times = re.findall(
            r'<h5 class="time">(\d{1,2}:\d{2})<span>(am|pm)</span></h5>',
            strip_html,
            re.I,
        )
        dep_time_str = ""
        arr_time_str = ""
        dep_dt = dep_date
        arr_dt = dep_date
        if len(times) >= 2:
            dep_time_str = f"{times[0][0]} {times[0][1]}"
            arr_time_str = f"{times[1][0]} {times[1][1]}"
            try:
                dep_dt = datetime.strptime(
                    f"{dep_date.strftime('%Y-%m-%d')} {dep_time_str}",
                    "%Y-%m-%d %I:%M %p",
                )
                arr_dt = datetime.strptime(
                    f"{dep_date.strftime('%Y-%m-%d')} {arr_time_str}",
                    "%Y-%m-%d %I:%M %p",
                )
                # If arrival is before departure, it's next day
                if arr_dt < dep_dt:
                    arr_dt += timedelta(days=1)
            except ValueError:
                pass

        duration_secs = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

        # Extract flight details from the details div
        details_m = re.search(
            r'<div class="body" data-flightno="([^"]+)">(.*?)</div>\s*</div>',
            strip_html,
            re.S,
        )
        if not details_m:
            return []

        base_flightno = details_m.group(1)  # e.g. "TL162^TL362" or "TL250"
        details_body = details_m.group(2)

        # Individual flight numbers
        flight_nums = re.findall(r'<span>(TL\d+)</span>', details_body)
        if not flight_nums:
            flight_nums = base_flightno.replace("^", "|").split("|")
            flight_nums = [fn.strip() for fn in flight_nums if fn.strip()]

        # Stops
        stops_m = re.search(r'class="stops">([^<]+)', strip_html)
        stops_text = stops_m.group(1).strip() if stops_m else ""
        if "non-stop" in stops_text.lower() or "nonstop" in stops_text.lower():
            num_stops = 0
        else:
            stop_num_m = re.search(r'(\d+)\s*stop', stops_text, re.I)
            num_stops = int(stop_num_m.group(1)) if stop_num_m else 0

        # Build segments
        if len(flight_nums) == 1 or num_stops == 0:
            # Single segment (direct flight)
            segments = [
                FlightSegment(
                    airline="TL",
                    airline_name="Airnorth",
                    flight_no=flight_nums[0] if flight_nums else "",
                    origin=req.origin,
                    destination=req.destination,
                    origin_city="",
                    destination_city="",
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=duration_secs,
                    cabin_class="economy",
                )
            ]
        else:
            # Multi-segment (connecting flights) — we don't know intermediate
            # airports from HTML, so create one segment per flight number
            seg_duration = duration_secs // len(flight_nums) if flight_nums else 0
            segments = []
            for i, fn in enumerate(flight_nums):
                seg_dep = dep_dt + timedelta(seconds=i * seg_duration)
                seg_arr = dep_dt + timedelta(seconds=(i + 1) * seg_duration)
                segments.append(
                    FlightSegment(
                        airline="TL",
                        airline_name="Airnorth",
                        flight_no=fn,
                        origin=req.origin if i == 0 else "",
                        destination=req.destination if i == len(flight_nums) - 1 else "",
                        origin_city="",
                        destination_city="",
                        departure=seg_dep,
                        arrival=seg_arr,
                        duration_seconds=seg_duration,
                        cabin_class="economy",
                    )
                )

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=duration_secs,
            stopovers=num_stops,
        )

        # Extract fare boxes
        fare_boxes = re.finditer(
            r'<div class="box sale js-select\s+(\w+)"'
            r'[^>]*data-fare="(\w+)"'
            r'[^>]*data-price="([\d.]+)"',
            strip_html,
        )

        for fm in fare_boxes:
            fare_class = fm.group(2)  # airsaver / airflex
            try:
                price = round(float(fm.group(3)), 2)
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue

            cabin = "economy"
            if fare_class == "airflex":
                cabin = "economy_flex"

            fid = hashlib.md5(
                f"tl_{base_flightno}_{fare_class}_{price}_{req.date_from}".encode()
            ).hexdigest()[:12]

            flight_no_display = (
                base_flightno.replace("^", "/") if "^" in base_flightno
                else base_flightno
            )

            results.append(
                FlightOffer(
                    id=f"tl_{fid}",
                    price=price,
                    currency="AUD",
                    price_formatted=f"A${price:.2f}",
                    outbound=route,
                    inbound=None,
                    airlines=["Airnorth"],
                    owner_airline="TL",
                    booking_url=(
                        f"https://secure.airnorth.com.au/AirnorthB2C/Booking/Search"
                        f"?origin={req.origin}&destination={req.destination}"
                    ),
                    is_locked=False,
                    source="airnorth_direct",
                    source_tier="free",
                )
            )

        return results

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"airnorth{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="AUD",
            offers=[],
            total_results=0,
        )
