"""
United Airlines direct connector — Playwright + SSE flight API.

United Airlines (IATA: UA) is a major US carrier based at Chicago O'Hare (ORD)
and Newark (EWR). Largest US carrier by destinations served (350+ domestic and
international routes across 70+ countries).

Strategy:
  1. Launch headed Playwright Chrome → load united.com to establish Akamai
     session cookies. CRITICAL: do NOT set a manual User-Agent string — Chrome
     must use its native UA or Akamai bot detection triggers a 428 block
     (sec-ch-ua version mismatch).
  2. Navigate to the FSR (Flight Search Results) deep-link URL. The React SPA
     fires POST /api/flight/FetchSSENestedFlights (Server-Sent Events).
  3. Capture the SSE response via CDP Network domain.
  4. Parse SSE events → build FlightOffer per itinerary with cheapest economy fare.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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
from .browser import auto_block_if_proxied, get_default_proxy

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
            "proxy": get_default_proxy(),
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
        logger.info("United: headed Chrome launched")
        return _browser


# Product type → cabin mapping
_CABIN_MAP = {
    "ECO-BASIC": "economy",
    "ECONOMY": "economy",
    "ECONOMY-UNRESTRICTED": "economy",
    "ECONOMY-MERCH-EPLUS": "premium_economy",
    "ECONOMY-UNRESTRICTED-MERCH-EPLUS": "premium_economy",
    "ECO-PREMIUM": "premium_economy",
    "ECO-PREMIUM-UNRESTRICTED": "premium_economy",
    "MIN-BUSINESS-OR-FIRST": "business",
    "MIN-BUSINESS-OR-FIRST-UNRESTRICTED": "business",
}

# Economy fare preference (cheapest first)
_ECONOMY_PRODUCT_TYPES = ["ECO-BASIC", "ECONOMY", "ECONOMY-UNRESTRICTED"]


class UnitedConnectorClient:
    """United Airlines connector — Playwright + SSE flight API."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            offers = await self._search_via_browser(req)
            elapsed = time.monotonic() - t0
            logger.info(
                "United: %s→%s on %s — %d offers in %.1fs",
                req.origin, req.destination, req.date_from, len(offers), elapsed,
            )
            return FlightSearchResponse(
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
                search_id=f"united_{req.origin}_{req.destination}_{req.date_from}",
            )
        except Exception as e:
            logger.error("United search error: %s", e)
            return self._empty(req)

    async def _search_via_browser(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        # CRITICAL: do NOT set user_agent — native Chrome UA must match sec-ch-ua
        # or Akamai blocks the SSE request with 428.
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="America/Chicago",
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

            # CDP session for SSE capture
            cdp = await context.new_cdp_session(page)
            await cdp.send("Network.enable")

            sse_request_id = None
            sse_done = asyncio.Event()
            sse_data_received = False

            def on_request(params):
                nonlocal sse_request_id, sse_data_received
                url = params.get("request", {}).get("url", "")
                if "FetchSSENestedFlights" in url:
                    sse_request_id = params.get("requestId")
                    sse_data_received = False

            def on_data_received(params):
                nonlocal sse_data_received
                if params.get("requestId") == sse_request_id:
                    sse_data_received = True

            def on_loading_finished(params):
                if params.get("requestId") == sse_request_id:
                    sse_done.set()

            cdp.on("Network.requestWillBeSent", on_request)
            cdp.on("Network.dataReceived", on_data_received)
            cdp.on("Network.loadingFinished", on_loading_finished)

            # Step 1: Load homepage for Akamai session cookies
            logger.info("United: loading homepage for session cookies")
            await page.goto(
                "https://www.united.com/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3)

            # Step 2: Navigate to FSR deep-link URL
            date_str = req.date_from.strftime("%Y-%m-%d")
            fsr_url = (
                f"https://www.united.com/en/us/fsr/choose-flights"
                f"?f={req.origin}&t={req.destination}&d={date_str}"
                f"&tt=1&sc=7&px={req.adults}&taxng=1&newHP=True"
                f"&clm=7&st=bestmatches&tqp=R"
            )
            logger.info("United: navigating to FSR: %s", fsr_url)
            await page.goto(
                fsr_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            # Step 3: Wait for SSE to complete
            try:
                await asyncio.wait_for(sse_done.wait(), timeout=self.timeout)
            except asyncio.TimeoutError:
                if not sse_data_received:
                    logger.warning("United: SSE timed out with no data")
                    return []
                # Data received but stream didn't finish — try getting body anyway
                logger.warning("United: SSE timed out but data was received, trying body capture")

            # Step 4: Get SSE response body via CDP
            if not sse_request_id:
                logger.warning("United: no SSE request captured")
                return []

            try:
                resp = await cdp.send(
                    "Network.getResponseBody",
                    {"requestId": sse_request_id},
                )
                body = resp.get("body", "")
            except Exception as e:
                logger.error("United: failed to get SSE body: %s", e)
                return []

            return self._parse_sse(body, req)
        finally:
            await context.close()

    def _parse_sse(
        self, body: str, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Parse SSE event stream into FlightOffers."""
        events = body.split("\n\n")
        max_stops = req.max_stopovers
        date_str = req.date_from.strftime("%Y-%m-%d")
        booking_url = (
            f"https://www.united.com/en/us/fsr/choose-flights"
            f"?f={req.origin}&t={req.destination}&d={date_str}"
            f"&tt=1&sc=7&px={req.adults}"
        )
        offers: list[FlightOffer] = []

        for evt in events:
            if "data:" not in evt:
                continue
            data_line = evt.split("data:", 1)[1].strip()
            try:
                obj = json.loads(data_line)
            except (json.JSONDecodeError, ValueError):
                continue

            if obj.get("type") != "flightOption":
                continue

            flight = obj.get("flight")
            if not flight:
                continue

            try:
                offer = self._parse_flight(flight, req, booking_url, max_stops)
                if offer:
                    offers.append(offer)
            except Exception as e:
                logger.debug("United: skipping flight: %s", e)

        return offers

    def _parse_flight(
        self,
        flight: dict,
        req: FlightSearchRequest,
        booking_url: str,
        max_stops: int,
    ) -> Optional[FlightOffer]:
        """Parse a single flightOption event into a FlightOffer."""
        connections = flight.get("connections") or []
        stopovers = len(connections)
        if stopovers > max_stops:
            return None

        # Find cheapest economy product
        products = flight.get("products") or []
        best_price = None
        best_product = None

        for product in products:
            pt = product.get("productType", "")
            if pt in _ECONOMY_PRODUCT_TYPES:
                price = self._extract_fare_price(product)
                if price and (best_price is None or price < best_price):
                    best_price = price
                    best_product = product
            # Check nested products
            for nested in product.get("nestedProducts") or []:
                npt = nested.get("productType", "")
                if npt in _ECONOMY_PRODUCT_TYPES:
                    nprice = self._extract_fare_price(nested)
                    if nprice and (best_price is None or nprice < best_price):
                        best_price = nprice
                        best_product = nested

        if not best_price or best_price <= 0:
            return None

        cabin_class = _CABIN_MAP.get(
            (best_product or {}).get("productType", "ECONOMY"), "economy"
        )

        # Build segments
        segments: list[FlightSegment] = []

        # First segment (origin → first stop or destination)
        seg = self._build_segment(flight, cabin_class)
        if seg:
            segments.append(seg)

        # Connection segments
        for conn in connections:
            cseg = self._build_segment(conn, cabin_class)
            if cseg:
                segments.append(cseg)

        if not segments:
            return None

        total_duration = flight.get("travelMinutesTotal", 0) * 60
        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_duration,
            stopovers=stopovers,
        )

        # Unique ID
        carrier = flight.get("marketingCarrier", "UA")
        fnum = flight.get("flightNumber", "")
        key = (
            f"{req.origin}{req.destination}"
            f"{flight.get('departDateTime', '')}"
            f"{best_price}"
        )
        offer_id = f"ua_{hashlib.md5(key.encode()).hexdigest()[:12]}"

        airlines = list({s.airline_name for s in segments})

        return FlightOffer(
            id=offer_id,
            price=round(best_price, 2),
            currency="USD",
            price_formatted=f"${best_price:.2f}",
            outbound=route,
            inbound=None,
            airlines=airlines,
            owner_airline="UA",
            booking_url=booking_url,
            is_locked=False,
            source="united_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_fare_price(product: dict) -> Optional[float]:
        """Get fare total from a product's prices array."""
        for p in product.get("prices") or []:
            if p.get("pricingType") == "Fare":
                amount = p.get("amount")
                if isinstance(amount, (int, float)) and amount > 0:
                    return float(amount)
        return None

    @staticmethod
    def _build_segment(seg_data: dict, cabin_class: str) -> Optional[FlightSegment]:
        """Build a FlightSegment from a flight or connection dict."""
        dep_str = seg_data.get("departDateTime", "")
        arr_str = seg_data.get("destinationDateTime", "")
        if not dep_str or not arr_str:
            return None

        try:
            dep_dt = datetime.strptime(dep_str, "%Y-%m-%d %H:%M")
            arr_dt = datetime.strptime(arr_str, "%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            return None

        carrier = seg_data.get("marketingCarrier", "UA")
        carrier_name = seg_data.get(
            "marketingCarrierDescription",
            seg_data.get("operatingCarrierDescription", "United Airlines"),
        )
        fnum = seg_data.get("flightNumber", "")
        origin = seg_data.get("origin", "")
        destination = seg_data.get("destination", "")
        duration_min = seg_data.get("travelMinutes", 0)

        return FlightSegment(
            airline=carrier,
            airline_name=carrier_name,
            flight_no=f"{carrier}{fnum}",
            origin=origin,
            destination=destination,
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=duration_min * 60,
            cabin_class=cabin_class,
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
            search_id=f"united_{req.origin}_{req.destination}_{req.date_from}",
        )
