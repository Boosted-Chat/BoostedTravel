"""
FlyArystan direct scraper — httpx client hitting Crane IBE server-rendered HTML.

FlyArystan (IATA: FS) is a Kazakh low-cost carrier (subsidiary of Air Astana).
Booking engine: Crane IBE at kzr-ports.hosting.aero (server-rendered HTML).

Strategy (verified Mar 2026):
  The booking.flyarystan.com frontend is behind Cloudflare WAF.
  However, the SAME Crane IBE backend is directly accessible at:
    https://kzr-ports.hosting.aero/ibe/availability?depPort=NQZ&arrPort=ALA&...
  This endpoint returns server-rendered HTML with all flight data — no JS needed.

  Date format: DD.MM.YYYY
  Currency: KZT

  HTML structure (Crane IBE):
    <div class="js-journey" data-journey-duration="6300" data-stop-count="0"
         data-dep-date="1774379700000" data-arr-date="1774386000000"
         data-journeyType="OUTBOUND"> per flight
      info-row > left-info-block (dep time/port/date) + middle-block (flight-no, duration, stops) + right-info-block
      fare-container > fare-item with cabin-name-PROMO / cabin-name-REGULAR
        price-best-offer / price spans  +  currency-best-offer / currency spans
    Fare types: PROMO (LOWEST) and REGULAR (FLEXI)
    Prices: "43 780" (space-separated, no decimal) + "KZT" in separate span
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
from .browser import get_httpx_proxy_url

logger = logging.getLogger(__name__)

_BASE = "https://kzr-ports.hosting.aero"
_AVAIL_URL = f"{_BASE}/ibe/availability"
_BOOKING_BASE = "https://booking.flyarystan.com/ibe/availability"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_JOURNEY_SPLIT = re.compile(r'<div[^>]*class="js-journey"')
_JOURNEY_DUR_RE = re.compile(r'data-journey-duration="(\d+)"')
_STOP_COUNT_RE = re.compile(r'data-stop-count="(\d+)"')
_DEP_DATE_RE = re.compile(r'data-dep-date="(\d+)"')
_ARR_DATE_RE = re.compile(r'data-arr-date="(\d+)"')
_JOURNEY_TYPE_RE = re.compile(r'data-journeyType="(\w+)"')
_FLIGHT_NO_RE = re.compile(r'<span[^>]*class="flight-no"[^>]*>([^<]+)</span>')
_SOLD_OUT_RE = re.compile(r'НЕТ МЕСТ|NO SEATS|SOLD OUT', re.IGNORECASE)

# Fare extraction: cabin-name-{TYPE} ... price-best-offer or price ... then currency
_FARE_BLOCK_RE = re.compile(
    r'cabin-name-(\w+).*?<span[^>]*class="[^"]*(?:price-best-offer|price)\b[^"]*"[^>]*>([\d\s]+)</span>',
    re.DOTALL,
)
# Fallback: any price span with digits + space pattern (KZT format)
_PRICE_FALLBACK_RE = re.compile(
    r'<span[^>]*class="[^"]*(?:price-best-offer|price)\b[^"]*"[^>]*>([\d][\d\s]*\d)</span>'
)


class FlyArystanConnectorClient:
    """FlyArystan httpx scraper — Crane IBE server-rendered HTML."""

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
            "tripType": "ROUND_TRIP" if req.return_from else "ONE_WAY",
            "lang": "en",
        }
        if req.return_from:
            params["returnDate"] = req.return_from.strftime("%d.%m.%Y")

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                headers=_HEADERS,
                proxy=get_httpx_proxy_url(),) as client:
                resp = await client.get(_AVAIL_URL, params=params)
                resp.raise_for_status()
                html = resp.text
        except Exception as e:
            logger.warning("FlyArystan: request failed: %s", e)
            return self._empty(req)

        offers = self._parse_html(html, req)
        elapsed = time.monotonic() - t0
        offers.sort(key=lambda o: o.price)
        logger.info(
            "FlyArystan %s→%s: %d offers in %.1fs (httpx)",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(f"flyarystan{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="KZT",
            offers=offers,
            total_results=len(offers),
        )

    def _parse_html(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        parts = re.split(r'<div[^>]*class="js-journey"', html)
        booking_url = self._build_booking_url(req)

        # Separate outbound vs inbound journeys
        outbound_parts: list[str] = []
        inbound_parts: list[str] = []
        for part in parts[1:]:
            jtype = _JOURNEY_TYPE_RE.search(part)
            direction = jtype.group(1).upper() if jtype else "OUTBOUND"
            if direction == "INBOUND":
                inbound_parts.append(part)
            else:
                outbound_parts.append(part)

        outbound_offers = self._parse_direction(outbound_parts, req.origin, req.destination, booking_url, req)
        if not req.return_from:
            return outbound_offers[:req.limit]

        inbound_offers = self._parse_direction(inbound_parts, req.destination, req.origin, booking_url, req)
        if not outbound_offers or not inbound_offers:
            return outbound_offers[:req.limit]

        # Combine round-trip: pair each outbound with each inbound
        combined: list[FlightOffer] = []
        for out in outbound_offers:
            for inb in inbound_offers:
                total_price = round(out.price + inb.price, 2)
                offer_hash = hashlib.md5(
                    f"fs_{out.id}_{inb.id}_{total_price}".encode()
                ).hexdigest()[:12]
                combined.append(FlightOffer(
                    id=f"fs_{offer_hash}",
                    price=total_price,
                    currency="KZT",
                    price_formatted=f"{total_price:,.0f} KZT",
                    outbound=out.outbound,
                    inbound=inb.outbound,
                    airlines=["FlyArystan"],
                    owner_airline="FS",
                    conditions={
                        "outbound_fare": out.conditions.get("fare_brand", ""),
                        "inbound_fare": inb.conditions.get("fare_brand", ""),
                    },
                    booking_url=booking_url,
                    is_locked=False,
                    source="flyarystan_direct",
                    source_tier="free",
                ))
        combined.sort(key=lambda o: o.price)
        return combined[:req.limit]

    def _parse_direction(
        self,
        journey_parts: list[str],
        origin: str,
        destination: str,
        booking_url: str,
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        for part in journey_parts:
            if _SOLD_OUT_RE.search(part):
                continue

            # Data attributes (millisecond timestamps)
            dep_ms = _DEP_DATE_RE.search(part)
            arr_ms = _ARR_DATE_RE.search(part)
            dur_attr = _JOURNEY_DUR_RE.search(part)
            stop_attr = _STOP_COUNT_RE.search(part)

            dep_dt = self._dt_from_ms(int(dep_ms.group(1))) if dep_ms else None
            arr_dt = self._dt_from_ms(int(arr_ms.group(1))) if arr_ms else None
            if not dep_dt:
                continue

            duration_secs = int(dur_attr.group(1)) if dur_attr else 0
            stopovers = int(stop_attr.group(1)) if stop_attr else 0

            # Flight number
            fn_matches = _FLIGHT_NO_RE.findall(part)
            flight_no_full = ""
            for fn in fn_matches:
                fn = fn.strip()
                if fn and re.match(r"[A-Z0-9]{2}\s*\d+", fn):
                    flight_no_full = fn
                    break
            if not flight_no_full:
                continue

            # Connecting flights: "FS 7377 - FS 7617"
            flight_no_parts = [f.strip() for f in flight_no_full.split(" - ")]

            # Parse fares: cabin-name-{PROMO|REGULAR} + price
            fares: dict[str, float] = {}
            for cabin_name, price_str in _FARE_BLOCK_RE.findall(part):
                cabin_name = cabin_name.upper()
                if cabin_name in fares:
                    continue
                try:
                    fares[cabin_name] = float(price_str.replace(" ", ""))
                except ValueError:
                    pass

            # Fallback: raw price spans
            if not fares:
                for price_str in _PRICE_FALLBACK_RE.findall(part):
                    try:
                        p = float(price_str.replace(" ", ""))
                        if p > 0:
                            fares.setdefault("PROMO", p)
                            break
                    except ValueError:
                        pass

            if not fares:
                continue

            display_flight_no = "/".join(flight_no_parts)

            for fare_brand, price in fares.items():
                # Build segments
                segments = []
                if len(flight_no_parts) == 1:
                    segments.append(FlightSegment(
                        airline="FS",
                        airline_name="FlyArystan",
                        flight_no=flight_no_parts[0],
                        origin=origin,
                        destination=destination,
                        departure=dep_dt,
                        arrival=arr_dt,
                        duration_seconds=duration_secs,
                        cabin_class="M",
                    ))
                else:
                    for idx, fn in enumerate(flight_no_parts):
                        segments.append(FlightSegment(
                            airline="FS",
                            airline_name="FlyArystan",
                            flight_no=fn,
                            origin=origin if idx == 0 else "---",
                            destination=destination if idx == len(flight_no_parts) - 1 else "---",
                            departure=dep_dt if idx == 0 else dep_dt,
                            arrival=arr_dt if idx == len(flight_no_parts) - 1 else dep_dt,
                            duration_seconds=0,
                            cabin_class="M",
                        ))

                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=duration_secs,
                    stopovers=stopovers,
                )
                offer_id = hashlib.md5(
                    f"flyarystan_{display_flight_no}_{fare_brand}_{price}".encode()
                ).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"fs_{offer_id}",
                    price=round(price, 2),
                    currency="KZT",
                    price_formatted=f"{price:,.0f} KZT",
                    outbound=route,
                    inbound=None,
                    airlines=["FlyArystan"],
                    owner_airline="FS",
                    conditions={"fare_brand": fare_brand},
                    booking_url=booking_url,
                    is_locked=False,
                    source="flyarystan_direct",
                    source_tier="free",
                ))

        offers.sort(key=lambda o: o.price)
        return offers

    @staticmethod
    def _dt_from_ms(millis: int) -> Optional[datetime]:
        if millis <= 0:
            return None
        return datetime.fromtimestamp(millis / 1000)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%d.%m.%Y")
        trip = "ROUND_TRIP" if req.return_from else "ONE_WAY"
        url = (
            f"{_BOOKING_BASE}?tripType={trip}"
            f"&depPort={req.origin}&arrPort={req.destination}"
            f"&departureDate={dep}"
            f"&adult={req.adults}&child={req.children}&infant={req.infants}"
            f"&lang=en"
        )
        if req.return_from:
            url += f"&returnDate={req.return_from.strftime('%d.%m.%Y')}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"flyarystan{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="KZT",
            offers=[],
            total_results=0,
        )