"""
Breeze Airways direct connector — Playwright API interception via Navitaire NewSkies.

Breeze Airways (IATA: MX) is a US low-cost carrier operating 75+ domestic
routes, plus limited international service (Cancún). Hub-less point-to-point
model with focus on underserved secondary airports.

Strategy:
  1. Navigate to the Breeze search results URL in a headed Chrome browser.
     The Angular SPA loads and calls the Navitaire GraphQL API automatically.
  2. Intercept the simpleAvailability API response for structured JSON data.
  3. Parse journeys + fares, cross-reference fare pricing, build offers.

The availability API (api.flybreeze.com) is behind Cloudflare WAF, so direct
httpx calls return 403. The browser-based approach passes Cloudflare naturally.
The token endpoint (/nsk/v2/token) is public and provides anonymous JWTs.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
from datetime import datetime
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import auto_block_if_proxied

logger = logging.getLogger(__name__)

_SEARCH_URL_TPL = (
    "https://www.flybreeze.com/booking/availability"
    "?origin={origin}&destination={dest}&beginDate={date}"
    "&searchDestinationMacs=false&searchOriginMacs=false"
    "&passengers=%7B%22types%22%3A%5B%7B%22count%22%3A{adults}%2C%22type%22%3A%22ADT%22%7D%5D%7D"
    "&infantCount=0"
)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

# Shared browser singleton
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_proxy_url() -> str:
    return os.environ.get("BREEZE_PROXY", "").strip()


def _get_pw_proxy() -> Optional[dict]:
    raw = _get_proxy_url()
    if not raw:
        return None
    from urllib.parse import urlparse
    p = urlparse(raw)
    result: dict[str, str] = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        result["username"] = p.username
    if p.password:
        result["password"] = p.password
    return result


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
        proxy = _get_pw_proxy()
        if proxy:
            launch_kw["proxy"] = proxy
        try:
            _browser = await pw.chromium.launch(**launch_kw)
        except Exception:
            launch_kw.pop("channel", None)
            _browser = await pw.chromium.launch(**launch_kw)
        logger.info("Breeze: headed Chrome launched (proxy=%s)", bool(proxy))
        return _browser


class BreezeConnectorClient:
    """Breeze Airways connector — Playwright + API interception."""

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
                "Breeze: %s→%s on %s — %d offers in %.1fs",
                req.origin, req.destination, req.date_from, len(offers), elapsed,
            )
            return FlightSearchResponse(
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
                search_id=f"breeze_{req.origin}_{req.destination}_{req.date_from}_{req.return_from or ''}",
            )
        except Exception as e:
            logger.error("Breeze search error: %s", e)
            return self._empty(req)

    async def _search_via_browser(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="America/New_York",
            service_workers="block",
        )
        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            captured: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    if "simpleAvailability" in response.url and response.status == 200:
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, dict):
                                captured["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            search_url = _SEARCH_URL_TPL.format(
                origin=req.origin,
                dest=req.destination,
                date=req.date_from.strftime("%Y-%m-%d"),
                adults=req.adults,
            )
            logger.info("Breeze: navigating to %s", search_url)
            await page.goto(search_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))

            # Wait for the API response (SPA calls it automatically)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=self.timeout - 5)
            except asyncio.TimeoutError:
                logger.warning("Breeze: simpleAvailability response not intercepted within timeout")
                return []

            data = captured.get("json")
            if not data:
                return []

            return self._parse_availability(data, req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_availability(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        sa = data.get("data", {}).get("simpleAvailability", {})
        avail = sa.get("availability")
        if not avail:
            err = sa.get("errorMessage")
            if err:
                logger.warning("Breeze API error: %s", err)
            return []

        currency = avail.get("currencyCode", "USD")

        # Build fare price lookup: fareKey → total price
        fare_prices: dict[str, float] = {}
        for fa in avail.get("faresAvailable") or []:
            fare_key = fa.get("fareKey", "")
            fares = fa.get("fareInfo", {}).get("fares") or []
            total = 0.0
            for f in fares:
                for pf in f.get("passengerFares") or []:
                    total += pf.get("fareAmount", 0)
            if total > 0:
                fare_prices[fare_key] = round(total, 2)

        booking_url = _SEARCH_URL_TPL.format(
            origin=req.origin,
            dest=req.destination,
            date=req.date_from.strftime("%Y-%m-%d"),
            adults=req.adults,
        )
        offers: list[FlightOffer] = []

        results = avail.get("results", [])
        if isinstance(results, list) and results:
            trips = results[0].get("trips") or []
        else:
            trips = []

        for trip in trips:
            jbm = trip.get("journeysAvailableByMarket")
            if not jbm:
                continue
            journeys = jbm.get("journey") or (jbm if isinstance(jbm, list) else [])
            for journey in journeys:
                offer = self._journey_to_offer(
                    journey, fare_prices, currency, booking_url, req,
                )
                if offer:
                    offers.append(offer)

        return offers

    def _journey_to_offer(
        self,
        journey: dict,
        fare_prices: dict[str, float],
        currency: str,
        booking_url: str,
        req: FlightSearchRequest,
    ) -> FlightOffer | None:
        des = journey.get("designator", {})
        dep_str = des.get("departure", "")
        arr_str = des.get("arrival", "")
        origin = des.get("origin", req.origin)
        destination = des.get("destination", req.destination)
        stops = journey.get("stops", 0)

        if not dep_str or not arr_str:
            return None

        departure = self._parse_dt(dep_str)
        arrival = self._parse_dt(arr_str)
        if not departure or not arrival:
            return None

        duration_s = int((arrival - departure).total_seconds())

        # Find cheapest fare for this journey
        price = None
        seats = None
        for fare_group in journey.get("fares") or []:
            fak = fare_group.get("fareAvailabilityKey", "")
            if fak in fare_prices:
                fp = fare_prices[fak]
                if price is None or fp < price:
                    price = fp
                    details = fare_group.get("details") or []
                    min_seats = None
                    for d in details:
                        cnt = d.get("availableCount", 0)
                        if cnt > 0 and (min_seats is None or cnt < min_seats):
                            min_seats = cnt
                    seats = min_seats

        if price is None or price <= 0:
            return None

        # Build segments
        segments: list[FlightSegment] = []
        for seg in journey.get("segments") or []:
            s_des = seg.get("designator", {})
            s_id = seg.get("identifier", {})
            carrier = s_id.get("carrierCode", "MX")
            flight_num = s_id.get("identifier", "")
            s_dep = self._parse_dt(s_des.get("departure", ""))
            s_arr = self._parse_dt(s_des.get("arrival", ""))
            if not s_dep or not s_arr:
                continue

            equipment = ""
            legs = seg.get("legs") or []
            if legs:
                equipment = legs[0].get("legInfo", {}).get("equipmentType", "")

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Breeze Airways",
                flight_no=f"{carrier}{flight_num}",
                origin=s_des.get("origin", ""),
                destination=s_des.get("destination", ""),
                departure=s_dep,
                arrival=s_arr,
                duration_seconds=int((s_arr - s_dep).total_seconds()),
                cabin_class="economy",
                aircraft=equipment,
            ))

        if not segments:
            return None

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=duration_s,
            stopovers=stops,
        )

        dep_key = f"{origin}{destination}{dep_str}{price}"
        offer_id = f"mx_{hashlib.md5(dep_key.encode()).hexdigest()[:12]}"

        return FlightOffer(
            id=offer_id,
            price=price,
            currency=currency,
            price_formatted=f"${price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Breeze Airways"],
            owner_airline="MX",
            booking_url=booking_url,
            is_locked=False,
            source="breeze_direct",
            source_tier="free",
            availability_seats=seats,
        )

    @staticmethod
    def _parse_dt(s: str) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
            search_id=f"breeze_{req.origin}_{req.destination}_{req.date_from}_{req.return_from or ''}",
        )
