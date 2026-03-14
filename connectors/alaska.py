"""
Alaska Airlines direct connector — Playwright + flightresults API.

Alaska Airlines (IATA: AS) is a major US carrier based in Seattle (SEA).
Post-Hawaiian Airlines merger, operates 300+ routes across the US, Hawaii,
Mexico, Costa Rica, Belize, and Canada.

Strategy:
  1. Launch headed Playwright Chrome → load alaskaair.com/search/ to establish
     session cookies (no WAF/CAPTCHA, SvelteKit SPA).
  2. Use in-browser fetch() to POST /search/api/flightresults with IATA codes
     and date. The API returns full itineraries with segments, fare classes,
     pricing, and seat availability.
  3. Parse rows → build FlightOffer per itinerary with cheapest economy fare.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from datetime import datetime
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    global _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        pw = await async_playwright().start()
        launch_kw: dict = {
            "headless": False,
            "channel": "chrome",
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
            ],
        }
        try:
            _browser = await pw.chromium.launch(**launch_kw)
        except Exception:
            launch_kw.pop("channel", None)
            _browser = await pw.chromium.launch(**launch_kw)
        logger.info("Alaska: headed Chrome launched")
        return _browser


# Fare class → cabin mapping
_CABIN_MAP = {
    "SAVER": "economy",
    "MAIN": "economy",
    "PREMIUM": "premium_economy",
    "FIRST": "first",
    "REFUNDABLE_MAIN": "economy",
    "REFUNDABLE_PREMIUM": "premium_economy",
    "REFUNDABLE_FIRST": "first",
}

# Preference order for cheapest economy fare
_ECONOMY_FARES = ["SAVER", "MAIN", "REFUNDABLE_MAIN"]


class AlaskaConnectorClient:
    """Alaska Airlines connector — Playwright + flightresults API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            offers = await self._search_via_browser(req)
            elapsed = time.monotonic() - t0
            logger.info(
                "Alaska: %s→%s on %s — %d offers in %.1fs",
                req.origin, req.destination, req.date_from, len(offers), elapsed,
            )
            return FlightSearchResponse(
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
                search_id=f"alaska_{req.origin}_{req.destination}_{req.date_from}",
            )
        except Exception as e:
            logger.error("Alaska search error: %s", e)
            return self._empty(req)

    async def _search_via_browser(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="America/Los_Angeles",
            service_workers="block",
        )
        try:
            page = await context.new_page()
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            logger.info("Alaska: loading search page for session cookies")
            await page.goto(
                "https://www.alaskaair.com/search/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            # Wait for SvelteKit SPA to hydrate
            await page.wait_for_load_state("networkidle", timeout=15000)

            date_str = req.date_from.strftime("%Y-%m-%d")

            result = await page.evaluate(
                """async ([origin, destination, dateStr, adults]) => {
                    try {
                        const body = {
                            origins: [origin],
                            destinations: [destination],
                            dates: [dateStr],
                            numADTs: adults,
                            numINFs: 0,
                            numCHDs: 0,
                            fareView: 'Default',
                            onba: false,
                            dnba: false,
                            discount: { code: '', type: 'NONE', memo: '' },
                            isAlaska: true,
                            isMobileApp: false,
                            sliceId: 0,
                            umnrAgeGroup: 'NONE',
                            isAddingToAdultRes: false,
                            lockFare: false,
                            sessionID: '',
                            solutionIDs: [],
                            solutionSetIDs: [],
                            qpxcVersion: '',
                            trackingTags: [],
                            isAwards: false,
                            isMultiCity: false,
                        };
                        const r = await fetch('/search/api/flightresults', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(body),
                        });
                        if (!r.ok) return { error: r.status };
                        return await r.json();
                    } catch (e) {
                        return { error: e.message };
                    }
                }""",
                [req.origin, req.destination, date_str, req.adults],
            )

            if not result or result.get("error"):
                logger.warning("Alaska: API error: %s", result)
                return []

            return self._parse_rows(result, req)
        finally:
            await context.close()

    def _parse_rows(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        rows = data.get("rows") or []
        max_stops = req.max_stopovers
        booking_url = (
            f"https://www.alaskaair.com/search/results"
            f"?A={req.adults}&OD1={req.origin},{req.destination},"
            f"{req.date_from.strftime('%Y-%m-%d')}"
        )
        offers: list[FlightOffer] = []

        for row in rows:
            segments_data = row.get("segments") or []
            solutions = row.get("solutions") or {}
            stopovers = len(segments_data) - 1

            if stopovers > max_stops:
                continue

            # Find cheapest economy fare
            fare = None
            for fare_key in _ECONOMY_FARES:
                if fare_key in solutions:
                    fare = solutions[fare_key]
                    break
            if not fare:
                continue

            price = fare.get("grandTotal")
            if not price or price <= 0:
                continue

            seats = fare.get("seatsRemaining")
            cabin_class = _CABIN_MAP.get(
                next((k for k in _ECONOMY_FARES if k in solutions), "MAIN"),
                "economy",
            )

            # Build segments
            segments: list[FlightSegment] = []
            for seg_data in segments_data:
                pub = seg_data.get("publishingCarrier") or {}
                carrier_code = pub.get("carrierCode", "AS")
                carrier_name = pub.get("carrierFullName", "Alaska Airlines")
                flight_num = pub.get("flightNumber", "")

                dep_str = seg_data.get("departureTime", "")
                arr_str = seg_data.get("arrivalTime", "")
                try:
                    dep_dt = datetime.fromisoformat(dep_str)
                    arr_dt = datetime.fromisoformat(arr_str)
                except (ValueError, TypeError):
                    continue

                duration_min = seg_data.get("duration", 0)
                aircraft = seg_data.get("aircraft", "")

                segments.append(FlightSegment(
                    airline=carrier_code,
                    airline_name=carrier_name,
                    flight_no=f"{carrier_code}{flight_num}",
                    origin=seg_data.get("departureStation", req.origin),
                    destination=seg_data.get("arrivalStation", req.destination),
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=duration_min * 60,
                    cabin_class=cabin_class,
                    aircraft=aircraft,
                ))

            if not segments:
                continue

            total_duration = row.get("duration", 0) * 60
            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_duration,
                stopovers=stopovers,
            )

            # Unique ID from route + schedule + price
            key = (
                f"{req.origin}{req.destination}"
                f"{segments[0].departure.isoformat()}"
                f"{price}"
            )
            offer_id = f"as_{hashlib.md5(key.encode()).hexdigest()[:12]}"

            # Collect all carriers
            airlines = list({s.airline_name for s in segments})

            offers.append(FlightOffer(
                id=offer_id,
                price=round(price, 2),
                currency="USD",
                price_formatted=f"${price:.2f}",
                outbound=route,
                inbound=None,
                airlines=airlines,
                owner_airline="AS",
                booking_url=booking_url,
                is_locked=False,
                source="alaska_direct",
                source_tier="free",
                availability_seats=seats if isinstance(seats, int) else None,
            ))

        return offers

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
            search_id=f"alaska_{req.origin}_{req.destination}_{req.date_from}",
        )
