"""
Lucky Air browser-based scraper — Playwright form fill + response interception.

Lucky Air (IATA: 8L) is a Chinese LCC headquartered in Kunming, Yunnan.
Hub: KMG (Kunming Changshui). Domestic network: 40+ Chinese cities.
Some international routes (Bangkok, Phuket, etc).

Website: www.luckyair.net (Chinese, micro-app SPA on Vue/Ant Design).
NOT available in GDS — must be scraped directly.

Strategy (browser-based, validated Mar 2026):
  1. Launch Playwright headed browser
  2. Navigate to https://www.luckyair.net/micro/main/flight/search
  3. Fill destination city → UI auto-calls calendar API
  4. Intercept POST /api/flight/query/price/calendar/fix response
  5. Parse cheapest daily prices → FlightOffers

The calendar API is called automatically by the Vue app when a destination
city is selected in the form. Calling it manually via fetch() returns 500.
We must trigger it via UI interaction and capture it via response interception.

Calendar API response:
  {"status": "success", "code": "200",
   "data": [{"date": "2026-03-13", "price": "1010", "currency": "CNY"}, ...]}

Discovered via Playwright interception probes, Mar 2026.
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
from connectors.browser import stealth_args

logger = logging.getLogger(__name__)

# ── Module-level browser state (cleaned up by engine.py) ───────────────────
_browser = None
_pw_instance = None

_BASE_URL = "https://www.luckyair.net"
_SEARCH_URL = f"{_BASE_URL}/micro/main/flight/search"

_VIEWPORTS = [
    {"width": 1280, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]


async def _get_browser():
    """Get or create the shared Playwright browser."""
    global _browser, _pw_instance
    if _browser and _browser.is_connected():
        return _browser

    from playwright.async_api import async_playwright
    _pw_instance = await async_playwright().start()
    try:
        _browser = await _pw_instance.chromium.launch(
            headless=True,
            channel="chrome",
            args=[*stealth_args(), "--lang=zh-CN"],
        )
    except Exception:
        _browser = await _pw_instance.chromium.launch(
            headless=True,
            args=[*stealth_args(), "--lang=zh-CN"],
        )
    logger.info("Lucky Air: browser launched")
    return _browser


class LuckyAirConnectorClient:
    """Lucky Air scraper — browser session + calendar API."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass  # Cleaned up by engine.py via module globals

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            browser = await _get_browser()
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
                viewport=random.choice(_VIEWPORTS),
            )

            try:
                page = await context.new_page()
                offers = await self._search_via_calendar(page, req, t0)

                elapsed = time.monotonic() - t0
                if offers:
                    offers.sort(key=lambda o: o.price)
                logger.info(
                    "Lucky Air %s→%s returned %d offers in %.1fs",
                    req.origin, req.destination, len(offers), elapsed,
                )

                search_hash = hashlib.md5(
                    f"luckyair{req.origin}{req.destination}{req.date_from}".encode()
                ).hexdigest()[:12]
                return FlightSearchResponse(
                    search_id=f"fs_{search_hash}",
                    origin=req.origin,
                    destination=req.destination,
                    currency=req.currency or "CNY",
                    offers=offers,
                    total_results=len(offers),
                )
            finally:
                await context.close()

        except Exception as exc:
            logger.error("Lucky Air error: %s", exc)
            return self._empty(req)

    async def _search_via_calendar(
        self, page, req: FlightSearchRequest, t0: float,
    ) -> list[FlightOffer]:
        """Fill form to trigger calendar API, intercept the response."""

        remaining = lambda: max(self.timeout - (time.monotonic() - t0), 5)

        captured_data: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                url = response.url
                if response.status == 200 and "price/calendar/fix" in url:
                    data = await response.json()
                    if data and isinstance(data, dict) and data.get("status") == "success":
                        captured_data["calendar"] = data
                        api_event.set()
            except Exception:
                pass

        page.on("response", on_response)

        # Step 1: Navigate to search page
        logger.info("Lucky Air: loading search page for %s→%s", req.origin, req.destination)
        try:
            await page.goto(
                _SEARCH_URL,
                wait_until="domcontentloaded",
                timeout=int(min(remaining(), 15) * 1000),
            )
        except Exception as exc:
            logger.warning("Lucky Air: failed to load search page: %s", exc)
            return []

        await asyncio.sleep(3.0)

        # Step 2: Change departure city if not KMG (default)
        if req.origin != "KMG":
            await self._fill_city(page, "出发城市", req.origin)

        # Step 3: Fill destination city → triggers calendar API automatically
        logger.info("Lucky Air: selecting destination %s", req.destination)
        await self._fill_city(page, "到达城市", req.destination)

        # Step 4: Wait for calendar API response
        try:
            await asyncio.wait_for(api_event.wait(), timeout=min(remaining(), 10))
        except asyncio.TimeoutError:
            logger.warning("Lucky Air: timed out waiting for calendar API")
            return []

        data = captured_data.get("calendar", {})
        if not data:
            return []

        entries = data.get("data", [])
        if not entries:
            logger.info("Lucky Air: no calendar data for %s→%s", req.origin, req.destination)
            return []

        return self._parse_calendar(entries, req)

    async def _fill_city(self, page, placeholder: str, code: str) -> bool:
        """Fill a city input field using the Ant Design Select dropdown."""
        try:
            inp = page.locator(f"input[placeholder='{placeholder}']")
            await inp.click(timeout=5000)
            await asyncio.sleep(0.5)
            # Clear existing text
            await inp.fill("")
            await asyncio.sleep(0.3)
            await inp.type(code, delay=80)
            await asyncio.sleep(2.0)

            # Click the first suggestion in the dropdown
            items = page.locator(".ant-select-dropdown-menu-item")
            if await items.count() > 0:
                await items.first.click()
                await asyncio.sleep(1.0)
                return True

            # Fallback: press Enter
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.5)
            return True
        except Exception as exc:
            logger.debug("Lucky Air: failed to fill %s: %s", placeholder, exc)
            return False

    def _parse_calendar(
        self, entries: list[dict], req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Convert calendar price entries into FlightOffers."""
        offers: list[FlightOffer] = []
        currency = "CNY"
        booking_base = self._booking_url(req)

        priced = [e for e in entries if e.get("price")]
        logger.info("Lucky Air: calendar returned %d priced days", len(priced))

        for entry in entries:
            price_str = entry.get("price")
            if not price_str:
                continue

            try:
                price = float(price_str)
            except (ValueError, TypeError):
                continue

            if price <= 0:
                continue

            flight_date_str = entry.get("date", "")
            if not flight_date_str:
                continue

            # If searching for a specific date, only include that date
            # Otherwise include all dates in the window
            try:
                flight_date = datetime.strptime(flight_date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            # For single-date search, match that exact date only
            if not req.date_to and flight_date != req.date_from:
                continue

            # For date range search, include all dates in range
            if req.date_to and (flight_date < req.date_from or flight_date > req.date_to):
                continue

            fid = hashlib.md5(
                f"8l_{req.origin}{req.destination}{flight_date_str}{price}".encode()
            ).hexdigest()[:12]

            # Build a minimal segment — calendar gives price only, not times
            dep_dt = datetime(flight_date.year, flight_date.month, flight_date.day, 0, 0)
            segment = FlightSegment(
                airline="8L",
                airline_name="Lucky Air",
                flight_no="8L",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class="economy",
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=0,
                stopovers=0,
            )

            offers.append(FlightOffer(
                id=f"8l_{fid}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Lucky Air"],
                owner_airline="8L",
                booking_url=booking_base,
                is_locked=False,
                source="luckyair_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.luckyair.net/micro/main/flight/search"
            f"?depCode={req.origin}&arrCode={req.destination}&flightDate={dep}"
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"luckyair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CNY",
            offers=[],
            total_results=0,
        )
