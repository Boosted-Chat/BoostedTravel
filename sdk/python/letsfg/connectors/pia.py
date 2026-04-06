"""
PIA Pakistan International Airlines (PK) — httpx client hitting Crane IBE server-rendered HTML.

PIA (IATA: PK) is Pakistan's flag carrier, operating from Karachi (KHI),
Islamabad (ISB), and Lahore (LHE) to 30+ destinations across Middle East,
Europe, Asia, and North America.

Booking engine: Crane IBE at pia-ports.hosting.aero (server-rendered HTML).

Strategy (verified Mar 2026):
  The PIA booking form redirects to book-pia.crane.aero (Cloudflare).
  However, the SAME Crane IBE backend is directly accessible at:
    https://pia-ports.hosting.aero/ibe/availability?depPort=KHI&arrPort=DXB&...
  This endpoint returns server-rendered HTML with all flight data — no JS needed.
  Ports API: https://pia-ports.hosting.aero/ibe/search/portNames (334 ports)
  Dates API: https://pia-ports.hosting.aero/ibe/search/availableFlightDates

  Date format: DD.MM.YYYY
  Currency: PKR (Pakistani Rupee)

  HTML structure (Crane IBE):
    <div class="availability-flight-table"> wraps the whole results table
      <div class="js-journey" data-journey-duration="..." ...> per flight
        <div class="selection-item">
          info-row > left-info-block (dep time/port/date) + middle-block (flight-no, duration, stops) + right-info-block (arr time/port/date)
          fare-container > mobile-fare-block > fare-item (cabin-mobile-economy / cabin-mobile-executive_economy) > price-text-single-line
"""

from __future__ import annotations

import hashlib
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

logger = logging.getLogger(__name__)

_BASE = "https://pia-ports.hosting.aero"
_AVAIL_URL = f"{_BASE}/ibe/availability"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_JOURNEY_DUR_RE = re.compile(r'data-journey-duration="(\d+)"')
_STOP_COUNT_RE = re.compile(r'data-stop-count="(\d+)"')
_TIME_RE = re.compile(r'<span[^>]*class="time"[^>]*>(\d{1,2}:\d{2}(?:\s*[AP]M)?)</span>')
_PORT_RE = re.compile(r'<span[^>]*class="port"[^>]*>([^<]+)</span>')
_DATE_RE = re.compile(r'<span[^>]*class="date"[^>]*>([^<]+)</span>')
_FLIGHT_NO_RE = re.compile(r'<span[^>]*class="flight-no"[^>]*>([^<]+)</span>')
_DURATION_RE = re.compile(r'<span[^>]*class="flight-duration"[^>]*>([^<]+)</span>')
_STOPS_RE = re.compile(r'<span[^>]*class="total-stop"[^>]*>([^<]+)</span>')
_PRICE_RE = re.compile(r'<span[^>]*class="price-text-single-line[^"]*"[^>]*>[^<]*?([\d,]+(?:\.\d+)?)\s*PKR</span>')
_CABIN_PRICE_RE = re.compile(
    r'cabin-mobile-(\w+).*?price-text-single-line[^"]*"[^>]*>[^<]*?([\d,]+(?:\.\d+)?)\s*PKR',
    re.DOTALL,
)


class PiaConnectorClient:
    """PIA Pakistan International Airlines httpx scraper — Crane IBE server-rendered HTML."""

    IATA = "PK"
    AIRLINE_NAME = "Pakistan International Airlines"
    SOURCE = "pia_direct"
    DEFAULT_CURRENCY = "PKR"

    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        dep_date = req.date_from.strftime("%d.%m.%Y")
        params = {
            "depPort": req.origin,
            "arrPort": req.destination,
            "departureDate": dep_date,
            "adult": str(req.adults),
            "child": str(req.children),
            "infant": str(req.infants),
            "tripType": "ONE_WAY",
            "lang": "en",
        }
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                headers=_HEADERS,
            ) as client:
                resp = await client.get(_AVAIL_URL, params=params)
                resp.raise_for_status()
                html = resp.text
        except Exception as e:
            logger.warning("PIA: request failed: %s", e)
            return self._empty(req)

        offers = self._parse_html(html, req)
        elapsed = time.monotonic() - t0
        offers.sort(key=lambda o: o.price)
        logger.info(
            "PIA %s→%s: %d offers in %.1fs (httpx)",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(f"pia{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=self.DEFAULT_CURRENCY,
            offers=offers,
            total_results=len(offers),
        )

    def _parse_html(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight cards from Crane IBE server-rendered HTML."""
        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)

        parts = re.split(r'<div[^>]*class="js-journey"', html)

        for part in parts[1:]:
            dur_attr = re.search(r'data-journey-duration="(\d+)"', part)
            stop_attr = re.search(r'data-stop-count="(\d+)"', part)

            flight_nos_raw = _FLIGHT_NO_RE.findall(part)
            flight_no_full = ""
            for fn in flight_nos_raw:
                fn = fn.strip()
                if fn and re.match(r"[A-Z0-9]{2}-?\d+", fn):
                    flight_no_full = fn
                    break
            if not flight_no_full:
                continue
            flight_no_parts = [f.strip().replace("-", "") for f in flight_no_full.split(" - ")]

            times = _TIME_RE.findall(part)
            ports = _PORT_RE.findall(part)
            dates = _DATE_RE.findall(part)
            durations = _DURATION_RE.findall(part)
            stops = _STOPS_RE.findall(part)

            dep_time = times[0] if times else ""
            arr_time = times[1] if len(times) > 1 else ""
            dep_port = self._extract_iata(ports[0]) if ports else req.origin
            arr_port = self._extract_iata(ports[1]) if len(ports) > 1 else req.destination
            dep_date_str = dates[0].strip() if dates else ""
            arr_date_str = dates[1].strip() if len(dates) > 1 else dep_date_str
            duration_str = durations[0].strip() if durations else ""
            stop_text = stops[0].strip() if stops else "Nonstop"
            stopovers = int(stop_attr.group(1)) if stop_attr else (
                0 if "nonstop" in stop_text.lower() or "direct" in stop_text.lower() else 1
            )

            prices_by_cabin: dict[str, float] = {}
            for cabin, price_str in _CABIN_PRICE_RE.findall(part):
                cabin = cabin.lower()
                if cabin in prices_by_cabin:
                    continue
                try:
                    prices_by_cabin[cabin] = float(price_str.replace(",", ""))
                except ValueError:
                    pass

            if not prices_by_cabin:
                all_prices = _PRICE_RE.findall(part)
                for p_str in all_prices:
                    try:
                        p = float(p_str.replace(",", ""))
                        if p > 0:
                            prices_by_cabin.setdefault("economy", p)
                            break
                    except ValueError:
                        pass

            if not prices_by_cabin:
                continue

            departure = self._parse_datetime(dep_date_str, dep_time)
            arrival = self._parse_datetime(arr_date_str or dep_date_str, arr_time)
            duration_secs = int(dur_attr.group(1)) if dur_attr else self._parse_duration(duration_str)
            if not duration_secs and departure and arrival and arrival > departure:
                duration_secs = int((arrival - departure).total_seconds())

            display_flight_no = "/".join(flight_no_parts)

            for cabin, price in prices_by_cabin.items():
                cabin_code = {
                    "economy": "M",
                    "executive_economy": "W",
                    "business": "C",
                    "first": "F",
                }.get(cabin, "M")

                segments = []
                if len(flight_no_parts) == 1:
                    segments.append(FlightSegment(
                        airline=self.IATA,
                        airline_name=self.AIRLINE_NAME,
                        flight_no=flight_no_parts[0],
                        origin=dep_port,
                        destination=arr_port,
                        departure=departure,
                        arrival=arrival,
                        duration_seconds=duration_secs,
                        cabin_class=cabin_code,
                    ))
                else:
                    for idx, fn in enumerate(flight_no_parts):
                        segments.append(FlightSegment(
                            airline=self.IATA,
                            airline_name=self.AIRLINE_NAME,
                            flight_no=fn,
                            origin=dep_port if idx == 0 else "---",
                            destination=arr_port if idx == len(flight_no_parts) - 1 else "---",
                            departure=departure if idx == 0 else departure,
                            arrival=arrival if idx == len(flight_no_parts) - 1 else departure,
                            duration_seconds=0,
                            cabin_class=cabin_code,
                        ))

                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=duration_secs,
                    stopovers=stopovers,
                )
                offer_id = hashlib.md5(
                    f"{display_flight_no}_{cabin}_{price}".encode()
                ).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"pk_{offer_id}",
                    price=round(price, 2),
                    currency=self.DEFAULT_CURRENCY,
                    price_formatted=f"{price:,.2f} PKR",
                    outbound=route,
                    inbound=None,
                    airlines=[self.AIRLINE_NAME],
                    owner_airline=self.IATA,
                    booking_url=booking_url,
                    is_locked=False,
                    source=self.SOURCE,
                    source_tier="free",
                ))

        return offers

    @staticmethod
    def _extract_iata(port_text: str) -> str:
        m = re.search(r"\(([A-Z]{3})\)", port_text)
        return m.group(1) if m else port_text.strip()[:3].upper()

    @staticmethod
    def _parse_datetime(date_str: str, time_str: str) -> Optional[datetime]:
        if not date_str or not time_str:
            return None
        clean_date = date_str.strip()
        clean_time = time_str.strip()
        for fmt in (
            "%d %b %Y %I:%M %p",
            "%d %b %Y %H:%M",
            "%d.%m.%Y %I:%M %p",
            "%d.%m.%Y %H:%M",
        ):
            try:
                return datetime.strptime(f"{clean_date} {clean_time}", fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_duration(dur_str: str) -> int:
        total = 0
        h = re.search(r"(\d+)\s*h", dur_str)
        m = re.search(r"(\d+)\s*m", dur_str)
        if h:
            total += int(h.group(1)) * 3600
        if m:
            total += int(m.group(1)) * 60
        return total

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%d.%m.%Y")
        return (
            f"https://book-pia.crane.aero/ibe/availability"
            f"?depPort={req.origin}&arrPort={req.destination}"
            f"&departureDate={dep}&adult={req.adults}"
            f"&child={req.children}&infant={req.infants}"
            f"&tripType={'ROUND_TRIP' if req.return_from else 'ONE_WAY'}&lang=en"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"pia{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=self.DEFAULT_CURRENCY,
            offers=[],
            total_results=0,
        )
