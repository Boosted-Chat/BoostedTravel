"""
Despegar connector — Playwright + API response interception.

Despegar (NASDAQ: DESP) is Latin America's largest OTA covering all airlines.
Also operates as Decolar (Brazil), BestDay (Mexico).

Strategy:
  Despegar uses DataDome bot protection which blocks direct HTTP requests.
  We use Playwright with a persistent browser context to:
  1. Navigate to the search results page URL
  2. Intercept the /flights-busquets/api/v1/web/search JSON response
  3. Parse flight offers from the `items` array
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import acquire_browser_slot, release_browser_slot, patchright_bandwidth_args

logger = logging.getLogger(__name__)

# Airline code to name mapping for common LatAm airlines
_AIRLINE_NAMES = {
    "AR": "Aerolíneas Argentinas",
    "LA": "LATAM Airlines",
    "G3": "GOL",
    "AD": "Azul",
    "AV": "Avianca",
    "CM": "Copa Airlines",
    "JA": "JetSmart",
    "VB": "VivaAerobus",
    "Y4": "Volaris",
    "H2": "Sky Airline",
    "AA": "American Airlines",
    "UA": "United Airlines",
    "DL": "Delta Air Lines",
}


def _parse_duration(duration_str: str) -> int:
    """Convert duration string like '09:15' to seconds."""
    if not duration_str:
        return 0
    try:
        parts = duration_str.split(":")
        hours = int(parts[0]) if len(parts) > 0 else 0
        mins = int(parts[1]) if len(parts) > 1 else 0
        return hours * 3600 + mins * 60
    except (ValueError, IndexError):
        return 0


def _parse_datetime(dt_str: Any) -> Optional[datetime]:
    """Parse ISO datetime string."""
    if not dt_str:
        return None
    try:
        s = str(dt_str)
        # Handle timezone offset like "-03:00"
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


class DespegarConnectorClient:
    """Despegar OTA — Playwright browser + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser closed per-search

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search flights on Despegar using browser automation."""
        t0 = time.monotonic()

        ob_offers = await self._search_ow(req)

        # Handle round-trip by combining outbound + inbound
        if req.return_from and ob_offers:
            ib_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "return_from": None,
            })
            ib_offers = await self._search_ow(ib_req)
            if ib_offers:
                ob_offers = self._combine_rt(ob_offers, ib_offers, req)

        ob_offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Despegar %s→%s: %d offers in %.1fs", req.origin, req.destination, len(ob_offers), elapsed)

        sh = hashlib.md5(f"despegar{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=ob_offers[0].currency if ob_offers else "USD",
            offers=ob_offers[:30],
            total_results=len(ob_offers),
        )

    async def _search_ow(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Search one-way flights using browser interception."""
        from playwright.async_api import async_playwright

        offers: list[FlightOffer] = []
        search_data: dict = {}
        date_str = req.date_from.strftime("%Y-%m-%d")

        # Build search URL
        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0
        search_url = (
            f"https://www.despegar.com.ar/shop/flights/results/oneway/"
            f"{req.origin}/{req.destination}/{date_str}/{adults}/{children}/{infants}"
        )

        await acquire_browser_slot()
        try:
            async with async_playwright() as p:
                # Use persistent context to avoid DataDome blocks
                # NOTE: headless=False required — DataDome blocks headless browsers
                context = await p.chromium.launch_persistent_context(
                    user_data_dir="",  # Empty string = temp profile
                    headless=False,  # DataDome requires visible browser
                    viewport={"width": 1920, "height": 1080},
                    locale="es-AR",
                    timezone_id="America/Buenos_Aires",
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        *patchright_bandwidth_args(),
                    ],
                )

                page = context.pages[0] if context.pages else await context.new_page()

                # Anti-detection
                await page.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                )

                # Capture search API response
                async def capture_response(response):
                    nonlocal search_data
                    url = response.url
                    if "flights-busquets/api/v1/web/search" in url:
                        try:
                            if response.status == 200:
                                data = await response.json()
                                if "items" in data and len(data.get("items", [])) > 0:
                                    search_data = data
                                    logger.debug("Captured Despegar search data: %d items", len(data["items"]))
                        except Exception as e:
                            logger.debug("Error capturing response: %s", e)

                page.on("response", capture_response)

                # Navigate to homepage first to establish session
                await page.goto("https://www.despegar.com.ar/", wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(1)

                # Navigate to search
                logger.debug("Despegar navigating to: %s", search_url)
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

                # Wait for search API response
                for _ in range(20):  # Up to 20 seconds
                    await asyncio.sleep(1)
                    if search_data and len(search_data.get("items", [])) > 0:
                        break

                await context.close()

            # Parse results
            if search_data:
                offers = self._parse_search_response(search_data, req, date_str)

        except Exception as e:
            logger.warning("Despegar browser error: %s", e)
        finally:
            release_browser_slot()

        return offers

    def _parse_search_response(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse the flights-busquets search response."""
        offers: list[FlightOffer] = []
        currency_code = data.get("initialCurrency", "ARS")

        # Currency conversion ratio to USD
        usd_ratio = 1.0
        for curr in data.get("currencies", []):
            if curr.get("code") == "USD":
                usd_ratio = curr.get("ratio", 1.0)
                break

        items = data.get("items", [])
        for wrapper in items:
            try:
                item = wrapper.get("item", {})
                if not item:
                    continue

                # Get price
                price_detail = item.get("priceDetail", {})
                main_fare = price_detail.get("mainFare", {})
                price = main_fare.get("amount", 0)
                currency = price_detail.get("currencyCode", currency_code)

                if price <= 0:
                    continue

                # Get route info
                route_choices = item.get("routeChoices", [])
                if not route_choices:
                    continue

                outbound_choice = route_choices[0]
                routes = outbound_choice.get("routes", [])
                if not routes:
                    continue

                route = routes[0]
                segments_data = route.get("segments", [])
                airline_codes = item.get("airlines", [])
                validating = item.get("validatingCarrier", airline_codes[0] if airline_codes else "")

                # Build segments
                segments: list[FlightSegment] = []
                for seg_data in segments_data:
                    dep_info = seg_data.get("departure", {})
                    arr_info = seg_data.get("arrival", {})
                    airline_code = seg_data.get("airlineCode", validating)
                    flight_id = seg_data.get("flightId", "")
                    duration_str = seg_data.get("duration", "")

                    dep_dt = _parse_datetime(dep_info.get("date"))
                    arr_dt = _parse_datetime(arr_info.get("date"))
                    duration_secs = _parse_duration(duration_str)

                    if not dep_dt:
                        dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
                    if not arr_dt:
                        arr_dt = dep_dt

                    airline_name = _AIRLINE_NAMES.get(airline_code, airline_code)

                    segments.append(FlightSegment(
                        airline=airline_name,
                        flight_no=flight_id,
                        origin=dep_info.get("airportCode", req.origin),
                        destination=arr_info.get("airportCode", req.destination),
                        departure=dep_dt,
                        arrival=arr_dt,
                        duration_seconds=duration_secs,
                    ))

                if not segments:
                    continue

                # Calculate total duration
                total_duration_str = route.get("totalDuration", "")
                total_duration = _parse_duration(total_duration_str)
                if not total_duration and segments:
                    total_duration = sum(s.duration_seconds for s in segments)

                stops = route.get("stopsCount", len(segments) - 1)

                flight_route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=total_duration,
                    stopovers=stops,
                )

                # Build airline names list
                airline_names = [_AIRLINE_NAMES.get(c, c) for c in airline_codes] if airline_codes else [segments[0].airline]

                # Generate offer ID
                first_seg = segments[0]
                oid = hashlib.md5(
                    f"desp_{req.origin}{req.destination}{date_str}{price}{first_seg.flight_no}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"desp_{oid}",
                    price=round(float(price), 2),
                    currency=currency,
                    price_formatted=f"{float(price):,.0f} {currency}",
                    outbound=flight_route,
                    inbound=None,
                    airlines=airline_names,
                    owner_airline=validating,
                    booking_url=f"https://www.despegar.com.ar/shop/flights/results/oneway/{req.origin}/{req.destination}/{date_str}/{req.adults or 1}/0/0",
                    is_locked=False,
                    source="despegar_ota",
                    source_tier="free",
                ))

            except Exception as e:
                logger.debug("Error parsing Despegar item: %s", e)
                continue

        return offers

    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Combine outbound and inbound offers into round-trip offers."""
        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_desp_{cid}",
                    price=price,
                    currency=o.currency,
                    outbound=o.outbound,
                    inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url,
                    is_locked=False,
                    source=o.source,
                    source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
