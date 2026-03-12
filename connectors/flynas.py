"""
Flynas hybrid scraper — CDP Chrome + persistent page + page.evaluate(fetch).

Flynas (IATA: XY) is a Saudi low-cost carrier.
Website: booking.flynas.com — custom booking engine protected by Akamai WAF.

Strategy (CDP Chrome + in-browser fetch):
1. Launch REAL system Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP. Keep ONE persistent page on booking.flynas.com.
3. page.evaluate(fetch('/api/SessionCreate')) → establishes API session
4. page.evaluate(fetch('/api/FlightSearch', body)) → real-time flight data
5. Parse JSON → FlightOffer objects

Real Chrome bypasses Akamai fingerprinting where bundled Chromium fails.
Searches complete in ~0.4-0.7s after initial page load (~6-10s).
The page is kept warm and reused across multiple searches.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

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
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_DEBUG_PORT = 9449
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".flynas_chrome_data"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
# Warm page: keeps one page with Akamai cookies alive for fast reuse
_warm_page = None
_warm_page_lock: Optional[asyncio.Lock] = None
_warm_ready = False


def _find_chrome() -> Optional[str]:
    """Find Chrome executable on the system."""
    candidates = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _get_page_lock() -> asyncio.Lock:
    global _warm_page_lock
    if _warm_page_lock is None:
        _warm_page_lock = asyncio.Lock()
    return _warm_page_lock


async def _get_browser():
    """Launch real Chrome via subprocess + connect via CDP."""
    global _pw_instance, _browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass

        from playwright.async_api import async_playwright

        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
        _pw_instance = await async_playwright().start()

        chrome_path = _find_chrome()
        if chrome_path:
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            # Try connecting to already-running Chrome
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Flynas: connected to existing Chrome via CDP")
                return _browser
            except Exception:
                pass

            # Launch new Chrome
            vp = random.choice(_VIEWPORTS)
            _chrome_proc = subprocess.Popen(
                [
                    chrome_path,
                    f"--remote-debugging-port={_DEBUG_PORT}",
                    f"--user-data-dir={_USER_DATA_DIR}",
                    f"--window-size={vp['width']},{vp['height']}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-background-networking",
                    "about:blank",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            await asyncio.sleep(2.5)
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Flynas: CDP Chrome connected (port %d)", _DEBUG_PORT)
                return _browser
            except Exception as e:
                logger.warning("Flynas: CDP connect failed: %s, falling back", e)
                if _chrome_proc:
                    _chrome_proc.terminate()
                    _chrome_proc = None

        # Fallback: regular Playwright headed
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
        logger.info("Flynas: Playwright browser launched (headed fallback)")
        return _browser


async def _ensure_warm_page():
    """Ensure a warm page exists with valid Akamai session."""
    global _warm_page, _warm_ready
    lock = _get_page_lock()
    async with lock:
        # Check if existing page is still usable
        if _warm_ready and _warm_page and not _warm_page.is_closed():
            try:
                await _warm_page.evaluate("1")
                return _warm_page
            except Exception:
                _warm_ready = False

        # Get browser and use first context's first page, or create one
        browser = await _get_browser()
        contexts = browser.contexts
        if contexts and contexts[0].pages:
            _warm_page = contexts[0].pages[0]
        else:
            ctx = contexts[0] if contexts else await browser.new_context()
            _warm_page = await ctx.new_page()

        logger.info("Flynas: loading booking page (Akamai warm-up)...")
        await _warm_page.goto(
            "https://booking.flynas.com/",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await asyncio.sleep(5)

        # Create API session
        sess = await _warm_page.evaluate("""async () => {
            const r = await fetch('/api/SessionCreate', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({session: {channel: 'web'}})
            });
            return {status: r.status};
        }""")
        logger.info("Flynas: SessionCreate status=%s", sess.get("status"))

        _warm_ready = True
        return _warm_page


class FlynasConnectorClient:
    """Flynas hybrid scraper — Playwright warm page + in-browser fetch API."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            page = await _ensure_warm_page()
        except Exception as e:
            logger.error("Flynas: warm page setup failed: %s", e)
            return self._empty(req)

        date_str = req.date_from.strftime("%Y-%m-%d")

        flights_data = [{"origin": req.origin, "destination": req.destination, "date": date_str}]
        if req.return_from:
            ret_str = req.return_from.strftime("%Y-%m-%d")
            flights_data.append({"origin": req.destination, "destination": req.origin, "date": ret_str})

        search_body = {
            "flightSearch": {
                "flights": flights_data,
                "adultCount": str(req.adults),
                "childCount": str(req.children or 0),
                "infantCount": str(req.infants or 0),
                "selectedCurrencyCode": req.currency or "SAR",
                "flightMode": "return" if req.return_from else "oneway",
                "clickId": "",
                "reqSource": "",
                "custID": "",
                "isStopoverbooking": False,
            }
        }

        # Execute FlightSearch via in-browser fetch (inherits Akamai cookies)
        try:
            result = await page.evaluate(
                """async (body) => {
                    const r = await fetch('/api/FlightSearch', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(body)
                    });
                    return {status: r.status, body: await r.text()};
                }""",
                search_body,
            )
        except Exception as e:
            logger.warning("Flynas: page.evaluate failed: %s — resetting warm page", e)
            global _warm_ready
            _warm_ready = False
            return self._empty(req)

        status = result.get("status", 0)
        elapsed = time.monotonic() - t0

        if status == 429:
            # Akamai challenge — wait and retry once
            logger.info("Flynas: 429 challenge, waiting 3s and retrying...")
            await asyncio.sleep(3)
            try:
                result = await page.evaluate(
                    """async (body) => {
                        const r = await fetch('/api/FlightSearch', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify(body)
                        });
                        return {status: r.status, body: await r.text()};
                    }""",
                    search_body,
                )
                status = result.get("status", 0)
                elapsed = time.monotonic() - t0
            except Exception:
                _warm_ready = False
                return self._empty(req)

        if status not in (200, 201):
            logger.warning("Flynas API returned %d: %s", status, result.get("body", "")[:300])
            if status in (403, 429):
                _warm_ready = False
            return self._empty(req)

        try:
            data = json.loads(result["body"])
        except Exception:
            logger.warning("Flynas: non-JSON response")
            return self._empty(req)

        # Parse response
        trips = data.get("flightsAvailability", {}).get("trips", [])
        outbound_flights = []
        return_flights = []

        for i, trip in enumerate(trips):
            for flight in trip.get("flights", []):
                parsed = self._parse_flight(flight)
                if parsed:
                    if i == 0:
                        outbound_flights.append(parsed)
                    else:
                        return_flights.append(parsed)

        offers = []
        booking_url = self._build_booking_url(req)
        currency = req.currency or "SAR"

        if req.return_from and return_flights:
            outbound_flights.sort(key=lambda x: x["price"])
            return_flights.sort(key=lambda x: x["price"])

            for ob in outbound_flights[:15]:
                for rt in return_flights[:10]:
                    total = ob["price"] + rt["price"]
                    offer = FlightOffer(
                        id=f"xy_{hashlib.md5((ob['key'] + rt['key']).encode()).hexdigest()[:12]}",
                        price=round(total, 2),
                        currency=currency,
                        price_formatted=f"{total:.2f} {currency}",
                        outbound=ob["route"],
                        inbound=rt["route"],
                        airlines=["flynas"],
                        owner_airline="XY",
                        booking_url=booking_url,
                        is_locked=False,
                        source="flynas_direct",
                        source_tier="free",
                    )
                    offers.append(offer)
        else:
            for ob in outbound_flights:
                offer = FlightOffer(
                    id=f"xy_{hashlib.md5(ob['key'].encode()).hexdigest()[:12]}",
                    price=round(ob["price"], 2),
                    currency=currency,
                    price_formatted=f"{ob['price']:.2f} {currency}",
                    outbound=ob["route"],
                    inbound=None,
                    airlines=["flynas"],
                    owner_airline="XY",
                    booking_url=booking_url,
                    is_locked=False,
                    source="flynas_direct",
                    source_tier="free",
                )
                offers.append(offer)

        offers.sort(key=lambda o: o.price)

        logger.info(
            "Flynas %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"flynas{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=offers,
            total_results=len(offers),
        )

    def _parse_flight(self, flight: dict) -> Optional[dict]:
        """Parse a single flight from the flynas FlightSearch response."""
        best_price = None
        fares = flight.get("fares", [])
        for fare in fares:
            p = fare.get("price")
            if p is not None and p > 0:
                if best_price is None or p < best_price:
                    best_price = p
        if best_price is None or best_price <= 0:
            return None

        legs = flight.get("legs", [])
        segments = []
        for leg in legs:
            flight_no = str(leg.get("flightNumber", "")).strip()
            carrier = leg.get("carrierCode") or leg.get("operatedBy") or "XY"
            if flight_no and not flight_no.startswith(("XY", "xy")):
                flight_no = f"XY{flight_no}"
            segments.append(FlightSegment(
                airline=carrier,
                airline_name="flynas",
                flight_no=flight_no,
                origin=leg.get("origin", ""),
                destination=leg.get("destination", ""),
                departure=self._parse_dt(leg.get("departureDate", "")),
                arrival=self._parse_dt(leg.get("arrivalDate", "")),
                cabin_class="M",
            ))

        if not segments:
            # Fallback: build from flight-level data
            flight_no = str(flight.get("flightNumber", "")).strip()
            if flight_no and not flight_no.startswith(("XY", "xy")):
                flight_no = f"XY{flight_no}"
            segments.append(FlightSegment(
                airline="XY",
                airline_name="flynas",
                flight_no=flight_no,
                origin=flight.get("origin", ""),
                destination=flight.get("destination", ""),
                departure=self._parse_dt(flight.get("departureDate", "")),
                arrival=self._parse_dt(flight.get("arrivalDate", "")),
                cabin_class="M",
            ))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = max(
                int((segments[-1].arrival - segments[0].departure).total_seconds()), 0
            )

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=flight.get("numberOfStops", max(len(segments) - 1, 0)),
        )

        flight_key = str(
            flight.get("journeyKey")
            or flight.get("id")
            or f"{segments[0].flight_no}_{segments[0].departure.isoformat()}"
        )

        return {
            "price": float(best_price),
            "key": flight_key,
            "route": route,
        }

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://booking.flynas.com/#/booking/search-redirect?"
            f"origin={req.origin}&destination={req.destination}"
            f"&departureDate={dep}&flightMode=oneway&adultCount={req.adults}"
            f"&childCount={req.children or 0}&infantCount={req.infants or 0}&culture=en-US"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"flynas{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
