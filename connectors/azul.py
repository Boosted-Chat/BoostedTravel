"""
Azul Brazilian Airlines scraper — warm CDP Chrome + navigate + API intercept.

Azul (IATA: AD) is Brazil's third-largest airline with the widest domestic network.
Website: www.voeazul.com.br — English version at /us/en/home.

Architecture:
- React SPA frontend with Navitaire/New Skies backend
- Akamai Bot Manager protects the availability API
- Booking URL triggers SPA to fire availability API automatically
- No form automation needed — direct URL navigation

Strategy:
1. Launch real Chrome via CDP (Akamai blocks bundled Chromium)
2. Warm context: load homepage to establish Akamai _abck cookie
3. Per search: navigate to booking URL → SPA fires availability API → intercept response
4. Parse Navitaire availability format → FlightOffer objects

Performance: ~5-8s first search (Chrome + Akamai), ~3-5s subsequent searches.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
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
from connectors.browser import stealth_args, stealth_popen_kwargs

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────

_AZUL_HOME = "https://www.voeazul.com.br/us/en/home"
_AVAIL_API = "reservationavailability/api/reservation/availability"
_MAX_ATTEMPTS = 3
_API_WAIT = 20  # seconds to wait for availability API per attempt

_CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]
_CDP_PORT = 9467
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", ".azul_chrome_data"
)

# ── Module-level warm state ──────────────────────────────────────────────

_chrome_proc: Optional[subprocess.Popen] = None
_warm_ctx = None          # BrowserContext
_ctx_ready = False
_ctx_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _ctx_lock
    if _ctx_lock is None:
        _ctx_lock = asyncio.Lock()
    return _ctx_lock


def _find_chrome() -> Optional[str]:
    for p in _CHROME_PATHS:
        if os.path.isfile(p):
            return p
    return None


async def _launch_chrome() -> subprocess.Popen:
    """Launch real Chrome with remote debugging."""
    global _chrome_proc
    if _chrome_proc and _chrome_proc.poll() is None:
        return _chrome_proc

    chrome = _find_chrome()
    if not chrome:
        raise RuntimeError("Chrome not found — Azul requires real Chrome for Akamai bypass")

    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    _chrome_proc = subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--window-size=1366,768",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            *stealth_args(),
            "about:blank",
        ],
        **stealth_popen_kwargs(),
    )
    await asyncio.sleep(3)
    logger.info("Azul: Chrome launched on CDP port %d", _CDP_PORT)
    return _chrome_proc


async def _wait_akamai(page, timeout: float = 15) -> bool:
    """Wait for Akamai teapot challenge to clear."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            text = await page.evaluate("() => document.body.innerText.substring(0, 300)")
            if "teapot" not in text.lower():
                return True
        except Exception:
            pass
        await asyncio.sleep(1.5)
    return False


async def _ensure_warm_ctx(force: bool = False):
    """Ensure a warm browser context with Akamai cookies established."""
    global _warm_ctx, _ctx_ready

    lock = _get_lock()
    async with lock:
        if _ctx_ready and _warm_ctx and not force:
            try:
                # Quick health check
                pages = _warm_ctx.pages
                if pages is not None:
                    return _warm_ctx
            except Exception:
                _ctx_ready = False

        # Launch Chrome & connect via CDP
        await _launch_chrome()

        from playwright.async_api import async_playwright
        pw = await async_playwright().start()
        try:
            browser = await pw.chromium.connect_over_cdp(f"http://localhost:{_CDP_PORT}")
        except Exception as e:
            logger.error("Azul: CDP connect failed: %s", e)
            raise RuntimeError(f"CDP connect failed: {e}")

        # Create fresh context
        ctx = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/Sao_Paulo",
            service_workers="block",
        )
        _warm_ctx = ctx

        # Warm up: load homepage to get Akamai _abck cookie
        page = await ctx.new_page()
        try:
            from playwright_stealth import stealth_async
            await stealth_async(page)
        except ImportError:
            pass

        logger.info("Azul: warming context (homepage for Akamai cookies)...")
        await page.goto(_AZUL_HOME, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(5)

        # Check for Akamai teapot
        akamai_ok = await _wait_akamai(page, timeout=15)
        if not akamai_ok:
            logger.warning("Azul: Akamai teapot on homepage, retrying...")
            await asyncio.sleep(8)
            await page.reload(wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
            akamai_ok = await _wait_akamai(page, timeout=15)
            if not akamai_ok:
                logger.error("Azul: Akamai blocked after retry")
                _ctx_ready = False
                await page.close()
                raise RuntimeError("Akamai blocked")

        # Dismiss cookie/LGPD banners
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"],[id*="cookie"],[class*="consent"],[id*="consent"],'
                    + '[class*="onetrust"],[id*="onetrust"],[class*="lgpd"],[class*="privacy"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                const btns = [...document.querySelectorAll('button')];
                const accept = btns.find(b =>
                    /accept|aceitar|got it|ok|agree/i.test(b.textContent));
                if (accept) accept.click();
            }""")
        except Exception:
            pass

        await page.close()
        _ctx_ready = True
        logger.info("Azul: warm context ready (Akamai cookies established)")
        return _warm_ctx


class AzulConnectorClient:
    """Azul scraper — warm CDP Chrome + navigate + Navitaire API intercept.

    Uses real Chrome (Akamai blocks bundled Chromium). Browser launched once,
    reused across searches. ~3-5s per search after warm-up.
    """

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._attempt_search(req, t0)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning("Azul: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)
                if "closed" in str(e).lower() or "disconnected" in str(e).lower():
                    global _ctx_ready
                    _ctx_ready = False

        return self._empty(req)

    async def _attempt_search(
        self, req: FlightSearchRequest, t0: float
    ) -> Optional[FlightSearchResponse]:
        """Single attempt: fresh page in warm context → navigate → intercept API."""
        ctx = await _ensure_warm_ctx()

        booking_url = self._build_booking_url(req)

        # Fresh page per search (inherits Akamai cookies, clean SPA state)
        page = await ctx.new_page()
        try:
            from playwright_stealth import stealth_async
            await stealth_async(page)
        except ImportError:
            pass

        # Set up API response interception
        captured: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                if _AVAIL_API not in response.url:
                    return
                if response.status != 200:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = await response.json()
                if isinstance(body, dict) and body:
                    captured["avail"] = body
                    api_event.set()
            except Exception:
                pass

        page.on("response", on_response)

        dep = req.date_from.strftime("%Y-%m-%d")
        logger.info("Azul: searching %s→%s on %s (fresh page)", req.origin, req.destination, dep)

        try:
            await page.goto(
                booking_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            # Check for Akamai teapot (usually passes immediately with warm cookies)
            akamai_ok = await _wait_akamai(page, timeout=10)
            if not akamai_ok:
                logger.warning("Azul: Akamai teapot on search page")
                return None

            # Wait for availability API response
            await asyncio.wait_for(api_event.wait(), timeout=_API_WAIT)

        except asyncio.TimeoutError:
            logger.warning("Azul: availability API timed out")
            return None
        except Exception as e:
            logger.warning("Azul: navigation error: %s", e)
            return None
        finally:
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

        data = captured.get("avail")
        if data is None:
            return None

        elapsed = time.monotonic() - t0
        offers = self._parse_availability(data, req)
        return self._build_response(offers, req, elapsed)

    # ── Navitaire availability parsing ───────────────────────────────────

    def _parse_availability(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = data.get("trips") or data.get("data", {}).get("trips") or []
        for trip in trips:
            journeys = trip.get("journeysAvailable") or trip.get("journeys") or []
            for journey in journeys:
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)

        if offers:
            return offers

        # Fallback: flatter structures
        journeys = (
            data.get("journeysAvailable") or data.get("journeys")
            or data.get("flights") or data.get("outboundFlights")
            or data.get("availability", {}).get("trips", [])
            or data.get("data", {}).get("journeys", [])
            or data.get("flightList", []) or []
        )
        if isinstance(journeys, dict):
            journeys = journeys.get("outbound", []) or list(journeys.values())
        if not isinstance(journeys, list):
            journeys = []

        for journey in journeys:
            if not isinstance(journey, dict):
                continue
            offer = self._parse_journey(journey, req, booking_url)
            if offer:
                offers.append(offer)

        return offers

    def _parse_journey(
        self, journey: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single Navitaire journey into a FlightOffer."""
        best_price = self._extract_journey_price(journey)
        if best_price is None or best_price <= 0:
            return None

        currency = self._extract_currency(journey) or "BRL"

        designator = journey.get("designator", {})
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []

        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._parse_segment(seg, req))
        else:
            dep_str = (
                designator.get("departure") or journey.get("departureDateTime")
                or journey.get("departure") or ""
            )
            arr_str = (
                designator.get("arrival") or journey.get("arrivalDateTime")
                or journey.get("arrival") or ""
            )
            origin = designator.get("origin") or journey.get("origin") or req.origin
            dest = designator.get("destination") or journey.get("destination") or req.destination
            flight_no = str(journey.get("flightNumber") or journey.get("flight_no") or "")
            segments.append(FlightSegment(
                airline="AD", airline_name="Azul",
                flight_no=f"AD{flight_no}" if flight_no else "",
                origin=origin, destination=dest,
                departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
                cabin_class="M",
            ))

        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            diff = (segments[-1].arrival - segments[0].departure).total_seconds()
            total_dur = int(diff) if diff > 0 else 0

        stops = journey.get("stops", max(len(segments) - 1, 0))
        route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=stops)

        journey_key = journey.get("journeyKey") or journey.get("id") or ""
        if not journey_key and segments:
            journey_key = f"{segments[0].departure.isoformat()}_{segments[0].flight_no}"

        return FlightOffer(
            id=f"ad_{hashlib.md5(journey_key.encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=list(set(s.airline for s in segments)) or ["AD"],
            owner_airline="AD", booking_url=booking_url,
            is_locked=False, source="azul_direct", source_tier="free",
        )

    def _parse_segment(self, seg: dict, req: FlightSearchRequest) -> FlightSegment:
        """Parse a Navitaire segment (with nested designator/flightDesignator)."""
        designator = seg.get("designator", {})
        flight_des = seg.get("flightDesignator", {})

        dep_str = (
            designator.get("departure") or seg.get("departureDateTime")
            or seg.get("departure") or seg.get("std") or ""
        )
        arr_str = (
            designator.get("arrival") or seg.get("arrivalDateTime")
            or seg.get("arrival") or seg.get("sta") or ""
        )
        origin = designator.get("origin") or seg.get("origin") or seg.get("departureStation") or req.origin
        dest = designator.get("destination") or seg.get("destination") or seg.get("arrivalStation") or req.destination
        carrier = flight_des.get("carrierCode") or seg.get("carrierCode") or seg.get("airline") or "AD"
        flight_num = str(flight_des.get("flightNumber") or seg.get("flightNumber") or seg.get("number") or "")

        dep = self._parse_dt(dep_str)
        arr = self._parse_dt(arr_str)

        return FlightSegment(
            airline=carrier, airline_name="Azul",
            flight_no=f"{carrier}{flight_num}" if flight_num else "",
            origin=origin, destination=dest,
            departure=dep, arrival=arr,
            cabin_class="M",
        )

    @staticmethod
    def _extract_journey_price(journey: dict) -> Optional[float]:
        """Extract the cheapest fare price from a Navitaire journey."""
        best = float("inf")

        fares = journey.get("fares", [])
        for fare in fares:
            if not isinstance(fare, dict):
                continue
            for pf in fare.get("passengerFares", []):
                fa = pf.get("fareAmount")
                if fa is not None:
                    try:
                        v = float(fa)
                        if 0 < v < best:
                            best = v
                    except (TypeError, ValueError):
                        pass
                charges = pf.get("serviceCharges", [])
                total_charge = 0.0
                for charge in charges:
                    try:
                        total_charge += float(charge.get("amount", 0))
                    except (TypeError, ValueError):
                        pass
                if total_charge > 0 and total_charge < best:
                    best = total_charge
                pf_val = pf.get("publishedFare")
                if pf_val is not None:
                    try:
                        v = float(pf_val)
                        if 0 < v < best:
                            best = v
                    except (TypeError, ValueError):
                        pass

        # Flat fare structures
        if best == float("inf"):
            for fare in fares:
                if not isinstance(fare, dict):
                    continue
                for key in ["price", "amount", "totalPrice", "total", "fareAmount", "totalAmount"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("total") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass

        # Journey-level price fields
        for key in ["price", "lowestFare", "totalPrice", "amount", "lowestPrice", "farePrice"]:
            p = journey.get(key)
            if p is not None:
                try:
                    v = float(p) if not isinstance(p, dict) else float(p.get("amount") or p.get("total", 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass

        return best if best < float("inf") else None

    @staticmethod
    def _extract_currency(journey: dict) -> Optional[str]:
        for fare in journey.get("fares", []):
            if not isinstance(fare, dict):
                continue
            for pf in fare.get("passengerFares", []):
                for charge in pf.get("serviceCharges", []):
                    cc = charge.get("currencyCode")
                    if cc:
                        return cc
        return None

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.voeazul.com.br/us/en/home/selecao-voo"
            f"?c0={req.origin}&c1={req.destination}&d1={dep}"
            f"&dt=ow&p1=ADT{req.adults}&px={req.adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"azul{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Azul %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(
            f"azul{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers, total_results=len(offers),
        )
