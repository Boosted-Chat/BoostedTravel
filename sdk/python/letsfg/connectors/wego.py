"""
<<<<<<< Updated upstream
Wego connector — CDP Chrome + API response interception.

Wego is a major metasearch engine popular in Middle East, South Asia,
and SE Asia.  Aggregates results from 700+ airlines and OTAs (Almosafer,
ClearTrip, Traveloka, etc.).

Strategy (CDP Chrome — Cloudflare protection):
1.  Launch real Chrome via CDP (--remote-debugging-port).
2.  Navigate to Wego search results page.
3.  Intercept XHR responses containing aggregated flight results.
4.  Parse into FlightOffers.
=======
Wego connector — patchright CDP browser + RSC parsing.

Wego is a major metasearch engine popular in Middle East, South Asia,
and SE Asia. Aggregates results from 700+ airlines and OTAs.

Strategy (rewritten Jul 2026 — RSC parsing model):
1. Each search launches fresh patchright browser with residential proxy.
2. Navigate to Wego search results URL.
3. Handle Cloudflare Turnstile challenges automatically.
4. Parse React Server Components (RSC) streaming data from page HTML.
5. Extract flight segments and itineraries from RSC chunks.
6. Close browser + cleanup.
>>>>>>> Stashed changes
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
<<<<<<< Updated upstream
import os
import re
import shutil
import subprocess
=======
import math
import os
import random
import re
import shutil
import sys
import tempfile
>>>>>>> Stashed changes
import time
from datetime import datetime, date as date_type
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
<<<<<<< Updated upstream
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_CDP_PORT = 9481
_USER_DATA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".wego_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


async def _get_context():
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            _pw_instance = pw
            logger.info("WEGO: connected to existing Chrome on port %d", _CDP_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

            chrome = find_chrome()
            os.makedirs(_USER_DATA, exist_ok=True)
            args = [
                chrome,
                f"--remote-debugging-port={_CDP_PORT}",
                f"--user-data-dir={_USER_DATA}",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
                "--lang=en-US",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.5)

            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            logger.info("WEGO: Chrome launched CDP port %d pid %d", _CDP_PORT, _chrome_proc.pid)

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    global _browser, _context, _pw_instance, _chrome_proc
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = _context = _pw_instance = _chrome_proc = None
    if os.path.isdir(_USER_DATA):
        try:
            shutil.rmtree(_USER_DATA)
        except Exception:
            pass


def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


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
=======
from .browser import (
    inject_stealth_js,
    get_default_proxy,
    proxy_is_configured,
    acquire_browser_slot,
    release_browser_slot,
    block_all_heavy_resources,
)

logger = logging.getLogger(__name__)

# ── IATA → Wego city slug mapping ──
# Wego URLs use format: /flights/{city}-{IATA}/{city}-{IATA}/{date}
_WEGO_SLUGS: dict[str, str] = {
    "LON": "london", "LHR": "london", "LGW": "london", "STN": "london",
    "LTN": "london", "LCY": "london", "SEN": "london",
    "BCN": "barcelona", "MAD": "madrid", "AGP": "malaga", "ALC": "alicante",
    "PMI": "palma-de-mallorca", "IBZ": "ibiza", "VLC": "valencia",
    "NYC": "new-york", "JFK": "new-york", "EWR": "new-york", "LGA": "new-york",
    "PAR": "paris", "CDG": "paris", "ORY": "paris",
    "BER": "berlin", "TXL": "berlin", "FRA": "frankfurt", "MUC": "munich",
    "ROM": "rome", "FCO": "rome", "MIL": "milan", "MXP": "milan", "LIN": "milan", "VCE": "venice",
    "IST": "istanbul", "SAW": "istanbul", "AYT": "antalya",
    "DXB": "dubai", "AUH": "abu-dhabi", "DOH": "doha",
    "SIN": "singapore", "BKK": "bangkok", "KUL": "kuala-lumpur",
    "DEL": "delhi", "BOM": "mumbai", "BLR": "bangalore",
    "HKG": "hong-kong", "TYO": "tokyo", "NRT": "tokyo", "HND": "tokyo",
    "SEL": "seoul", "ICN": "seoul", "GMP": "seoul",
    "BJS": "beijing", "PEK": "beijing", "PKX": "beijing",
    "SHA": "shanghai", "PVG": "shanghai",
    "SYD": "sydney", "MEL": "melbourne", "AKL": "auckland",
    "LIS": "lisbon", "OPO": "porto", "ATH": "athens",
    "AMS": "amsterdam", "BRU": "brussels", "DUB": "dublin",
    "ZRH": "zurich", "GVA": "geneva", "VIE": "vienna",
    "OSL": "oslo", "STO": "stockholm", "ARN": "stockholm", "BMA": "stockholm", "NYO": "stockholm",
    "CPH": "copenhagen", "HEL": "helsinki",
    "WAW": "warsaw", "PRG": "prague", "BUD": "budapest",
    "MOW": "moscow", "SVO": "moscow", "DME": "moscow", "VKO": "moscow",
    "CAI": "cairo", "JNB": "johannesburg", "NBO": "nairobi",
    "CHI": "chicago", "LAX": "los-angeles", "SFO": "san-francisco", "ORD": "chicago", "MDW": "chicago",
    "MIA": "miami", "DFW": "dallas", "ATL": "atlanta",
    "WAS": "washington", "IAD": "washington", "DCA": "washington",
    "YYZ": "toronto", "YVR": "vancouver", "MEX": "mexico-city",
    "SAO": "sao-paulo", "GRU": "sao-paulo", "CGH": "sao-paulo",
    "BUE": "buenos-aires", "EZE": "buenos-aires", "AEP": "buenos-aires",
    "BOG": "bogota", "SCL": "santiago", "LIM": "lima",
}

# Airport IATA → City IATA for multi-airport cities.
# Wego URLs must use the city code, not individual airport codes.
_AIRPORT_TO_CITY: dict[str, str] = {
    "LHR": "LON", "LGW": "LON", "STN": "LON", "LTN": "LON", "LCY": "LON", "SEN": "LON",
    "JFK": "NYC", "EWR": "NYC", "LGA": "NYC",
    "CDG": "PAR", "ORY": "PAR",
    "NRT": "TYO", "HND": "TYO",
    "FCO": "ROM", "CIA": "ROM",
    "MXP": "MIL", "LIN": "MIL",
    "TXL": "BER", "SXF": "BER",
    "SAW": "IST",
    "PVG": "SHA",
    "PKX": "BJS", "PEK": "BJS",
    "ICN": "SEL", "GMP": "SEL",
    "ARN": "STO", "BMA": "STO", "NYO": "STO",
    "SVO": "MOW", "DME": "MOW", "VKO": "MOW",
    "EZE": "BUE", "AEP": "BUE",
    "GRU": "SAO", "CGH": "SAO", "VCP": "SAO",
    "ORD": "CHI", "MDW": "CHI",
    "IAD": "WAS", "DCA": "WAS", "BWI": "WAS",
}
>>>>>>> Stashed changes


# ── Bezier curve helpers for human-like movements ──
def _bezier_curve(p0: tuple, p1: tuple, p2: tuple, p3: tuple, steps: int = 30) -> list:
    """Generate points along cubic bezier curve."""
    pts = []
    for i in range(steps + 1):
        t = i / steps
        s = 1 - t
        x = s**3 * p0[0] + 3*s**2*t * p1[0] + 3*s*t**2 * p2[0] + t**3 * p3[0]
        y = s**3 * p0[1] + 3*s**2*t * p1[1] + 3*s*t**2 * p2[1] + t**3 * p3[1]
        pts.append((x, y))
    return pts


async def _human_mouse_move(page, start_x: float, start_y: float, end_x: float, end_y: float):
    """Move mouse from start to end using bezier curve with micro-variations."""
    dx = end_x - start_x
    dy = end_y - start_y
    ctrl1 = (start_x + dx * random.uniform(0.2, 0.4), start_y + dy * random.uniform(-0.3, 0.3))
    ctrl2 = (start_x + dx * random.uniform(0.6, 0.8), end_y + random.uniform(-15, 15))
    pts = _bezier_curve((start_x, start_y), ctrl1, ctrl2, (end_x, end_y), steps=random.randint(25, 40))
    for px, py in pts:
        px += random.uniform(-1.5, 1.5)
        py += random.uniform(-1.5, 1.5)
        await page.mouse.move(px, py)
        await asyncio.sleep(random.uniform(0.004, 0.012))


async def _solve_cf_turnstile(page) -> bool:
    """Handle Cloudflare Turnstile challenge if present."""
    try:
        # Check for Turnstile iframe or checkbox
        cf_frame = page.frame_locator('iframe[src*="challenges.cloudflare.com"]')
        checkbox = cf_frame.locator('input[type="checkbox"]')
        
        if await checkbox.count() > 0:
            logger.info("WEGO: clicking Turnstile checkbox")
            box = await checkbox.bounding_box()
            if box:
                cx = box['x'] + box['width'] / 2
                cy = box['y'] + box['height'] / 2
                await _human_mouse_move(page, 100, 100, cx, cy)
                await asyncio.sleep(random.uniform(0.2, 0.4))
                await page.mouse.click(cx, cy)
                await asyncio.sleep(3)
            return True
    except Exception:
        pass
    
    # Check for turnstile div (patchright usually auto-handles this)
    try:
        turnstile = page.locator('[class*="turnstile"], [id*="turnstile"]')
        if await turnstile.count() > 0:
            await asyncio.sleep(2)  # patchright auto-solves
            return True
    except Exception:
        pass
    
    return False


def _wego_slug(iata: str) -> str:
    """Convert IATA code to Wego URL slug: bare city IATA code.

    Wego URLs use format: /flights/LON/BCN/2026-06-15
    Map airport → city first (LHR → LON, JFK → NYC).
    """
    code = iata.upper()
    city_code = _AIRPORT_TO_CITY.get(code, code)
    return city_code


def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


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


class WegoConnectorClient:
    """Wego — ME/Asia metasearch, CDP Chrome + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        dt = _to_datetime(req.date_from)
        date_str = dt.strftime("%Y-%m-%d")

        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0

        cabin_map = {"economy": "economy", "premium_economy": "premium_economy",
                     "business": "business", "first": "first"}
        cabin = cabin_map.get(
            getattr(req, "cabin_class", "economy") or "economy", "economy",
        )

<<<<<<< Updated upstream
        # Wego URL format: /flights/{origin}/{dest}/{date}
        search_url = (
            f"https://www.wego.com/flights/{req.origin}/{req.destination}"
=======
        # Wego URL format: /flights/{city-IATA}/{city-IATA}/{date}
        origin_slug = _wego_slug(req.origin)
        dest_slug = _wego_slug(req.destination)
        search_url = (
            f"https://www.wego.com/flights/{origin_slug}/{dest_slug}"
>>>>>>> Stashed changes
            f"/{date_str}"
            f"?adults={adults}&children={children}&infants={infants}"
            f"&cabin={cabin}&sort=price"
        )

<<<<<<< Updated upstream
        for attempt in range(2):
            try:
                offers = await self._do_search(search_url, req, dt)
=======
        for attempt in range(3):
            try:
                offers = await self._do_search(search_url, req, dt, attempt)
>>>>>>> Stashed changes
                if offers is not None:
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "WEGO %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"wego{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=offers[0].currency if offers else "USD",
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("WEGO attempt %d failed: %s", attempt, e)
<<<<<<< Updated upstream
                if attempt == 0:
                    await _reset_profile()
=======
>>>>>>> Stashed changes

        return self._empty(req)

    async def _do_search(
<<<<<<< Updated upstream
        self, search_url: str, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer] | None:
        context = await _get_context()
        page = await context.new_page()

        captured_data: list[dict] = []

        async def on_response(response):
            url = response.url
            # Wego makes API calls to srv.wego.com and/or internal APIs
            interesting = (
                ("srv.wego.com" in url and ("search" in url or "result" in url or "fares" in url))
                or ("wego.com/api" in url and "flight" in url)
                or ("wego.com" in url and "graphql" in url)
            )
            if not interesting:
                return
            try:
                ct = response.headers.get("content-type", "")
                if ("json" in ct or "graphql" in url) and response.status == 200:
                    body = await response.text()
                    data = json.loads(body)
                    if isinstance(data, dict):
                        captured_data.append(data)
                        logger.debug("WEGO: captured response from %s (%d bytes)", url, len(body))
            except Exception:
                pass

        page.on("response", on_response)

        try:
            logger.info("WEGO: navigating to %s", search_url)
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Wego is a metasearch — results trickle in over time
            deadline = time.monotonic() + 40
            last_count = 0
            stable_ticks = 0
            while time.monotonic() < deadline:
                await asyncio.sleep(3)
                if len(captured_data) > last_count:
                    last_count = len(captured_data)
                    stable_ticks = 0
                else:
                    stable_ticks += 1
                    if stable_ticks >= 3 and captured_data:
                        break  # no new data for ~9s

            if not captured_data:
                logger.warning("WEGO: no API responses intercepted, trying DOM")
                return await self._extract_from_dom(page, req, dt)

            offers: list[FlightOffer] = []
            seen: set[str] = set()
            for data in captured_data:
                parsed = self._parse_response(data, req, dt, seen)
                offers.extend(parsed)

            return offers

        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Response parsing
=======
        self, search_url: str, req: FlightSearchRequest, dt: datetime, attempt: int = 0,
    ) -> list[FlightOffer] | None:
        """Search using patchright with DOM text parsing."""
        from patchright.async_api import async_playwright

        browser = None
        context = None
        pw_instance = None

        try:
            await acquire_browser_slot()
            
            pw_instance = await async_playwright().start()
            
            # Build proxy config with session ID for different IP on retry
            launch_kwargs = {"headless": False}
            if proxy_is_configured():
                session_id = f"wego{int(time.time())}{attempt}"
                launch_kwargs["proxy"] = {
                    "server": "http://gate.decodo.com:10001",
                    "username": f"{os.environ.get('DECODO_USER', '')}-session-{session_id}",
                    "password": os.environ.get("DECODO_PASS", ""),
                }
            else:
                proxy = get_default_proxy()
                if proxy:
                    launch_kwargs["proxy"] = proxy

            browser = await pw_instance.chromium.launch(**launch_kwargs)
            context = await browser.new_context(
                viewport={"width": 1366, "height": 800},
                locale="en-US",
            )
            page = await context.new_page()

            logger.info("WEGO: navigating to %s", search_url)
            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
            except Exception as nav_err:
                err_str = str(nav_err)
                if "ERR_HTTP_RESPONSE_CODE_FAILURE" in err_str or "ERR_TUNNEL" in err_str:
                    logger.warning("WEGO: proxy may be blocked, retrying with different session")
                    raise  # Will trigger retry with different proxy session
                raise

            # Handle Cloudflare challenge - patchright usually auto-solves
            # but sometimes needs to wait for completion
            for attempt in range(15):
                title = await page.title()
                if "Just a moment" in title or "Checking" in title.lower() or "challenge" in title.lower():
                    if attempt == 0:
                        logger.info("WEGO: Cloudflare challenge detected, waiting...")
                    await asyncio.sleep(2)
                else:
                    if attempt > 0:
                        logger.info("WEGO: Cloudflare passed after %d attempts", attempt)
                    break
            
            # Wait for page to fully render and results to load
            logger.info("WEGO: waiting for flight results")
            await asyncio.sleep(5)
            
            # Wait for network to settle
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass  # OK if times out - we'll work with what we have
            
            await asyncio.sleep(3)
            
            # Scroll to trigger lazy loading
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 400)")
                await asyncio.sleep(1)

            # Get page HTML and try multiple extraction methods
            html = await page.content()
            
            # Method 1: Try DOM text parsing (most reliable for Wego)
            offers = await self._parse_dom_text(page, req, dt)
            if offers:
                return offers
            
            # Method 2: Fall back to RSC parsing
            offers = self._parse_rsc_data(html, req, dt)
            if offers:
                return offers
            
            # Method 3: Legacy DOM extraction
            offers = await self._extract_from_dom(page, req, dt)
            return offers

        finally:
            try:
                if context:
                    await context.close()
            except Exception:
                pass
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass
            try:
                if pw_instance:
                    await pw_instance.stop()
            except Exception:
                pass
            release_browser_slot()

    # ------------------------------------------------------------------
    # DOM Text Parsing (primary method for Wego)
    # ------------------------------------------------------------------

    async def _parse_dom_text(
        self, page, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Parse flight schedules from visible page text.
        
        Wego renders flight schedules as visible text with this pattern:
        - Times: LHR 06:00, ZRH 12:30 (departure/arrival pairs)
        - Duration: 12h 5m
        - Stops: 1 Stop, ZRH · 3h 50m
        - Airlines: Swiss, Finnair, Emirates
        - Fare Guide prices: US$ 334, US$ 340
        """
        try:
            # Get visible text from main element
            text = await page.evaluate("""() => {
                const main = document.querySelector('main');
                return main ? main.innerText : document.body.innerText;
            }""")
            
            if not text or len(text) < 500:
                logger.debug("WEGO: insufficient page text")
                return []
            
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            
            # Extract fare guide prices (US$ XXX)
            prices_found = []
            for line in lines:
                m = re.search(r'US\$\s*([\d,]+)', line)
                if m:
                    try:
                        price = float(m.group(1).replace(',', ''))
                        if 50 < price < 10000:
                            prices_found.append(price)
                    except ValueError:
                        pass
            
            # Remove obvious low prices (usually partial fares)
            if prices_found:
                min_price = min(prices_found)
                prices_found = [p for p in prices_found if p >= min_price]
            
            logger.info("WEGO: found %d fare guide prices", len(prices_found))
            
            # Extract flight schedules
            # Pattern: airport HH:MM lines followed by duration and airline
            schedules = []
            i = 0
            while i < len(lines) - 5:
                line = lines[i]
                
                # Look for departure pattern: LHR 06:00
                dep_match = re.match(r'^([A-Z]{3})\s+(\d{1,2}:\d{2})$', line)
                if dep_match:
                    dep_airport = dep_match.group(1)
                    dep_time = dep_match.group(2)
                    
                    # Look ahead for more info (arrival, duration, airline)
                    schedule = {
                        'dep_airport': dep_airport,
                        'dep_time': dep_time,
                        'arr_airport': req.destination,
                        'arr_time': None,
                        'duration': None,
                        'stops': 0,
                        'airlines': [],
                    }
                    
                    # Parse next few lines
                    for j in range(1, min(12, len(lines) - i)):
                        next_line = lines[i + j]
                        
                        # Arrival airport/time: DXB 21:05
                        arr_match = re.match(r'^([A-Z]{3})\s+(\d{1,2}:\d{2})$', next_line)
                        if arr_match:
                            schedule['arr_airport'] = arr_match.group(1)
                            schedule['arr_time'] = arr_match.group(2)
                            continue
                        
                        # Duration: 12h 5m or 12h 05m
                        dur_match = re.match(r'^(\d+)h\s*(\d+)m$', next_line)
                        if dur_match:
                            hours = int(dur_match.group(1))
                            mins = int(dur_match.group(2))
                            schedule['duration'] = hours * 60 + mins
                            continue
                        
                        # Stops: 1 Stop, 2 Stops, Direct
                        if 'Stop' in next_line:
                            stop_match = re.search(r'(\d+)\s*Stop', next_line)
                            if stop_match:
                                schedule['stops'] = int(stop_match.group(1))
                            continue
                        if next_line.lower() == 'direct':
                            schedule['stops'] = 0
                            continue
                        
                        # Airline names (common carriers)
                        airline_names = [
                            'Emirates', 'Etihad', 'Qatar', 'Swiss', 'Lufthansa',
                            'British Airways', 'KLM', 'Air France', 'Finnair',
                            'Turkish', 'Ryanair', 'EasyJet', 'Wizz', 'Vueling',
                            'Norwegian', 'SAS', 'Aeroflot', 'Saudia', 'Gulf Air',
                            'Kuwait Airways', 'Oman Air', 'Flydubai', 'Air India',
                            'Singapore Airlines', 'Cathay', 'Thai', 'Malaysia',
                        ]
                        for airline in airline_names:
                            if airline.lower() in next_line.lower():
                                if airline not in schedule['airlines']:
                                    schedule['airlines'].append(airline)
                    
                    if schedule['duration'] or schedule['arr_time']:
                        schedules.append(schedule)
                
                i += 1
            
            logger.info("WEGO: found %d flight schedules", len(schedules))
            
            # Build offers from schedules + prices
            offers: list[FlightOffer] = []
            seen: set[str] = set()
            
            # Get unique price list
            unique_prices = sorted(set(prices_found))[:20]
            
            for i, schedule in enumerate(schedules[:len(unique_prices)]):
                # Assign price from fare guide (lower prices to shorter durations)
                price_idx = min(i, len(unique_prices) - 1)
                if price_idx >= len(unique_prices):
                    continue
                price_f = unique_prices[price_idx]
                
                airline = schedule['airlines'][0] if schedule['airlines'] else 'Unknown'
                
                # Deduplicate
                dedup = f"{schedule['dep_airport']}_{schedule['arr_airport']}_{schedule['dep_time']}_{price_f}"
                if dedup in seen:
                    continue
                seen.add(dedup)
                
                # Parse times
                dep_time_str = f"{dt:%Y-%m-%d} {schedule['dep_time']}"
                try:
                    departure = datetime.strptime(dep_time_str, "%Y-%m-%d %H:%M")
                except ValueError:
                    departure = dt
                
                duration_s = (schedule['duration'] or 0) * 60
                arrival = departure
                if duration_s > 0:
                    from datetime import timedelta
                    arrival = departure + timedelta(seconds=duration_s)
                
                seg = FlightSegment(
                    airline=airline,
                    airline_name=airline,
                    flight_no="",
                    origin=schedule['dep_airport'],
                    destination=schedule['arr_airport'],
                    departure=departure,
                    arrival=arrival,
                    duration_seconds=duration_s,
                    cabin_class="economy",
                )
                
                route = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=duration_s,
                    stopovers=schedule['stops'],
                )
                
                fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"wego_{fid}",
                    price=price_f,
                    currency="USD",
                    price_formatted=f"${price_f:.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=schedule['airlines'] or [airline],
                    owner_airline=airline,
                    booking_url=(
                        f"https://www.wego.com/flights/{req.origin}/{req.destination}"
                        f"/{dt:%Y-%m-%d}?adults={req.adults or 1}"
                    ),
                    is_locked=False,
                    source="wego_meta",
                    source_tier="free",
                ))
            
            return offers
            
        except Exception as e:
            logger.debug("WEGO: DOM text parsing failed: %s", e)
            return []

    # ------------------------------------------------------------------
    # RSC Parsing (React Server Components)
    # ------------------------------------------------------------------

    def _parse_rsc_data(
        self, html: str, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Parse React Server Components streaming data from page HTML.
        
        Wego uses Next.js App Router with RSC streaming. Flight data is
        embedded in <script>self.__next_f.push([1,"..."])</script> tags.
        """
        # Extract all RSC chunks
        pattern = r'<script>self\.__next_f\.push\(\[1,"([^"]+)"\]\)</script>'
        chunks = re.findall(pattern, html, re.DOTALL)
        
        if not chunks:
            logger.warning("WEGO: no RSC chunks found in HTML")
            return []
        
        # Concatenate and unescape
        all_data = ''
        for chunk in chunks:
            unescaped = chunk.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
            all_data += unescaped
        
        logger.debug("WEGO: parsed %d RSC chunks, %d bytes total", len(chunks), len(all_data))
        
        offers: list[FlightOffer] = []
        seen: set[str] = set()
        
        # Extract flight segments
        # Pattern: "arrivalTime":"HH:MM","departureTime":"HH:MM","flightNumber":"NNN",...
        segment_pattern = (
            r'"arrivalTime":"(\d{2}:\d{2})",'
            r'"departureTime":"(\d{2}:\d{2})",'
            r'"flightNumber":"([^"]+)",'
            r'"designatorCode":"([^"]+)",'
            r'"airlineCode":"([A-Z0-9]{2})"'
        )
        segments_found = []
        for m in re.finditer(segment_pattern, all_data):
            segments_found.append({
                'arrival': m.group(1),
                'departure': m.group(2),
                'flight_number': m.group(3),
                'designator': m.group(4),
                'airline': m.group(5),
            })
        
        # Extract itinerary data with duration and airports
        itin_pattern = (
            r'"durationTimeMinutes":(\d+).*?'
            r'"departureAirportCode":"([A-Z]{3})".*?'
            r'"arrivalAirportCode":"([A-Z]{3})".*?'
            r'"stopoversCount":(\d+)'
        )
        itineraries = []
        for m in re.finditer(itin_pattern, all_data[:500000]):  # Limit for performance
            itineraries.append({
                'duration_min': int(m.group(1)),
                'origin': m.group(2),
                'destination': m.group(3),
                'stops': int(m.group(4)),
            })
        
        # Extract price data
        # Format: "priceUsd":333.70,"price":333.7,"outboundAirlineCodes":["VF","W9"]
        price_pattern = (
            r'"priceUsd":(\d+(?:\.\d+)?),?"price":(\d+(?:\.\d+)?).*?'
            r'"outboundAirlineCodes":\["([^"]+)"'
        )
        prices = []
        for m in re.finditer(price_pattern, all_data):
            prices.append({
                'price_usd': float(m.group(1)),
                'price': float(m.group(2)),
                'airline': m.group(3),
            })
        
        logger.info("WEGO: found %d segments, %d itineraries, %d prices",
                   len(segments_found), len(itineraries), len(prices))
        
        # Build offers from price data
        for i, price_data in enumerate(prices):
            price_f = round(price_data['price_usd'], 2)
            if price_f <= 0:
                continue
            
            airline = price_data.get('airline', 'Unknown')
            
            # Deduplicate
            dedup = f"{req.origin}_{req.destination}_{dt:%Y%m%d}_{price_f}_{airline}"
            if dedup in seen:
                continue
            seen.add(dedup)
            
            # Find matching itinerary
            matching_itin = None
            for itin in itineraries:
                # Match by similar airports (city codes may differ from airport codes)
                if (itin['origin'][:2] == req.origin[:2] or itin['origin'] in req.origin
                    or req.origin in itin['origin']):
                    if (itin['destination'][:2] == req.destination[:2] 
                        or itin['destination'] in req.destination
                        or req.destination in itin['destination']):
                        matching_itin = itin
                        break
            
            # Build segment
            duration_s = 0
            stops = 0
            if matching_itin:
                duration_s = matching_itin['duration_min'] * 60
                stops = matching_itin['stops']
            
            seg = FlightSegment(
                airline=airline,
                airline_name=airline,
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                departure=dt,
                arrival=dt,
                duration_seconds=duration_s,
                cabin_class="economy",
            )
            
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=duration_s,
                stopovers=stops,
            )
            
            fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"wego_{fid}",
                price=price_f,
                currency="USD",
                price_formatted=f"${price_f:.0f}",
                outbound=route,
                inbound=None,
                airlines=[airline],
                owner_airline=airline,
                booking_url=(
                    f"https://www.wego.com/flights/{req.origin}/{req.destination}"
                    f"/{dt:%Y-%m-%d}?adults={req.adults or 1}"
                ),
                is_locked=False,
                source="wego_meta",
                source_tier="free",
            ))
        
        # If no price data found, try to extract from segments + visible prices
        if not offers and segments_found:
            logger.info("WEGO: no price objects, building from segments")
            # Look for dollar amounts in the data
            dollar_pattern = r'\$(\d+(?:,\d{3})*(?:\.\d{2})?)'
            dollar_amounts = re.findall(dollar_pattern, all_data)
            dollar_values = [float(d.replace(',', '')) for d in dollar_amounts]
            dollar_values = sorted(set(v for v in dollar_values if 50 < v < 5000))
            
            # Dedupe segments
            seen_segs = set()
            unique_segs = []
            for s in segments_found:
                key = (s['designator'], s['departure'])
                if key not in seen_segs:
                    seen_segs.add(key)
                    unique_segs.append(s)
            
            # Match segments to prices heuristically
            for i, seg_data in enumerate(unique_segs[:len(dollar_values)]):
                if i >= len(dollar_values):
                    break
                    
                price_f = dollar_values[i] if i < len(dollar_values) else 0
                if price_f <= 0:
                    continue
                
                airline = seg_data['airline']
                dedup = f"{req.origin}_{req.destination}_{seg_data['designator']}_{price_f}"
                if dedup in seen:
                    continue
                seen.add(dedup)
                
                # Parse times
                dep_time = datetime.strptime(f"{dt:%Y-%m-%d} {seg_data['departure']}", "%Y-%m-%d %H:%M")
                arr_time = datetime.strptime(f"{dt:%Y-%m-%d} {seg_data['arrival']}", "%Y-%m-%d %H:%M")
                if arr_time < dep_time:
                    arr_time = arr_time.replace(day=arr_time.day + 1)
                
                duration_s = int((arr_time - dep_time).total_seconds())
                
                seg = FlightSegment(
                    airline=airline,
                    airline_name=airline,
                    flight_no=seg_data['designator'],
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_time,
                    arrival=arr_time,
                    duration_seconds=duration_s,
                    cabin_class="economy",
                )
                
                route = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=duration_s,
                    stopovers=0,
                )
                
                fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"wego_{fid}",
                    price=price_f,
                    currency="USD",
                    price_formatted=f"${price_f:.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=[airline],
                    owner_airline=airline,
                    booking_url=(
                        f"https://www.wego.com/flights/{req.origin}/{req.destination}"
                        f"/{dt:%Y-%m-%d}?adults={req.adults or 1}"
                    ),
                    is_locked=False,
                    source="wego_meta",
                    source_tier="free",
                ))
        
        return offers

    # ------------------------------------------------------------------
    # Legacy Response parsing (kept for backwards compatibility)
>>>>>>> Stashed changes
    # ------------------------------------------------------------------

    def _parse_response(
        self, data: dict, req: FlightSearchRequest, dt: datetime, seen: set,
    ) -> list[FlightOffer]:
        """Parse Wego metasearch API response data."""
        offers: list[FlightOffer] = []

        # Wego responses can nest results under various keys
        fares = (
            data.get("fares") or data.get("trips") or data.get("results")
            or data.get("itineraries") or data.get("flights") or []
        )

        # GraphQL responses
        if "data" in data and isinstance(data["data"], dict):
            gql = data["data"]
            fares = fares or (
                gql.get("flightSearch", {}).get("fares")
                or gql.get("flightSearch", {}).get("results")
                or gql.get("flights", {}).get("results")
                or []
            )

        # Lookup tables (Wego often sends airlines/airports separately)
        airlines_map = {}
        for a in data.get("airlines", []):
            if isinstance(a, dict):
                code = a.get("code") or a.get("iata") or ""
                airlines_map[code] = a.get("name") or code

        for fare in fares:
            try:
                offer = self._parse_fare(fare, req, dt, seen, airlines_map)
                if offer:
                    offers.append(offer)
            except Exception as e:
                logger.debug("WEGO: parse fare error: %s", e)

        return offers

    def _parse_fare(
        self, fare: dict, req: FlightSearchRequest, dt: datetime,
        seen: set, airlines_map: dict,
    ) -> FlightOffer | None:
        # Price
        price_obj = fare.get("price") or fare
        if isinstance(price_obj, dict):
            price = (
                price_obj.get("amount") or price_obj.get("totalAmount")
                or price_obj.get("price") or 0
            )
            currency = (
                price_obj.get("currencyCode") or price_obj.get("currency") or "USD"
            )
        else:
            try:
                price = float(price_obj)
            except (ValueError, TypeError):
                return None
            currency = "USD"

        try:
            price_f = round(float(price), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        # Segments / legs
        legs = fare.get("legs") or fare.get("segments") or fare.get("slices") or []
        if not legs:
            # Flat fare structure
            legs = [fare]

        segments: list[FlightSegment] = []
        for leg in legs:
            seg_items = leg.get("segments") or [leg]
            for sd in seg_items:
                airline_code = (
                    sd.get("airlineCode") or sd.get("operatingCarrier")
                    or sd.get("marketingCarrier") or sd.get("airline") or ""
                )
                airline_name = (
                    sd.get("airlineName") or airlines_map.get(airline_code, "")
                    or airline_code
                )
                fno = sd.get("flightNumber") or sd.get("flightNo") or ""
                if airline_code and fno and not fno.startswith(airline_code):
                    fno = f"{airline_code}{fno}"

                dep_time = (
                    sd.get("departureTime") or sd.get("departure")
                    or sd.get("departureDateTime") or ""
                )
                arr_time = (
                    sd.get("arrivalTime") or sd.get("arrival")
                    or sd.get("arrivalDateTime") or ""
                )
                dep_apt = (
                    sd.get("departureAirportCode") or sd.get("departureCode")
                    or sd.get("origin") or req.origin
                )
                arr_apt = (
                    sd.get("arrivalAirportCode") or sd.get("arrivalCode")
                    or sd.get("destination") or req.destination
                )
                dur = sd.get("durationMinutes") or sd.get("duration") or 0
                dur_s = int(dur) * 60 if isinstance(dur, (int, float)) and dur > 0 else 0

                segments.append(FlightSegment(
                    airline=airline_code or airline_name,
                    airline_name=airline_name,
                    flight_no=fno,
                    origin=dep_apt,
                    destination=arr_apt,
                    departure=_parse_dt(dep_time) if dep_time else dt,
                    arrival=_parse_dt(arr_time) if arr_time else dt,
                    duration_seconds=dur_s,
                    cabin_class="economy",
                ))

        if not segments:
            return None

        total_dur = sum(s.duration_seconds for s in segments)
        if not total_dur and segments[0].departure != segments[-1].arrival:
            diff = (segments[-1].arrival - segments[0].departure).total_seconds()
            if 0 < diff < 86400 * 3:
                total_dur = int(diff)

        fno_key = "_".join(s.flight_no for s in segments)
        dedup = f"{req.origin}_{req.destination}_{dt:%Y%m%d}_{price_f}_{fno_key}"
        if dedup in seen:
            return None
        seen.add(dedup)

        airlines_set = list(dict.fromkeys(s.airline for s in segments if s.airline))
        names_set = list(dict.fromkeys(
            s.airline_name for s in segments if s.airline_name
        ))

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(0, len(segments) - 1),
        )

        fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"wego_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=names_set or airlines_set,
            owner_airline=airlines_set[0] if airlines_set else "",
            booking_url=(
                f"https://www.wego.com/flights/{req.origin}/{req.destination}"
                f"/{dt:%Y-%m-%d}?adults={req.adults or 1}"
            ),
            is_locked=False,
            source="wego_meta",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # DOM fallback
    # ------------------------------------------------------------------

    async def _extract_from_dom(
        self, page, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Fallback: scrape visible fare cards from the Wego results page."""
        try:
            data = await page.evaluate("""() => {
                const cards = document.querySelectorAll(
                    '[class*="FareCard"], [class*="fare-card"], '
                  + '[class*="ResultCard"], [class*="result-card"], '
                  + '[data-testid*="fare"], [data-testid*="result"]'
                );
                const out = [];
                cards.forEach(c => {
                    const p = c.querySelector(
                        '[class*="price"], [class*="Price"], [data-testid*="price"]'
                    );
                    const a = c.querySelector(
                        '[class*="airline"], [class*="Airline"], [data-testid*="airline"]'
                    );
                    const d = c.querySelector(
                        '[class*="duration"], [class*="Duration"]'
                    );
                    const stops = c.querySelector(
                        '[class*="stop"], [class*="Stop"]'
                    );
                    if (p) out.push({
                        price: p.textContent.trim(),
                        airline: a ? a.textContent.trim() : '',
                        duration: d ? d.textContent.trim() : '',
                        stops: stops ? stops.textContent.trim() : '',
                    });
                });
                return out;
            }""")

            offers: list[FlightOffer] = []
            seen: set[str] = set()
            for item in data or []:
                nums = re.findall(r"[\d]+", item.get("price", "").replace(",", ""))
                if not nums:
                    continue
                try:
                    price_f = round(float(nums[-1]), 2)
                except (ValueError, IndexError):
                    continue
                if price_f <= 0:
                    continue

                airline = item.get("airline") or "Unknown"
                dedup = f"{req.origin}_{req.destination}_{price_f}_{airline}"
                if dedup in seen:
                    continue
                seen.add(dedup)

                seg = FlightSegment(
                    airline=airline, flight_no="",
                    origin=req.origin, destination=req.destination,
                    departure=dt, arrival=dt, duration_seconds=0,
                )
                route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
                fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"wego_{fid}",
                    price=price_f,
                    currency="USD",
                    price_formatted=f"{price_f:.2f} USD",
                    outbound=route,
                    inbound=None,
                    airlines=[airline],
                    owner_airline="",
                    booking_url=(
                        f"https://www.wego.com/flights/{req.origin}"
                        f"/{req.destination}/{dt:%Y-%m-%d}"
                    ),
                    is_locked=False,
                    source="wego_meta",
                    source_tier="free",
                ))
            return offers
        except Exception as e:
            logger.debug("WEGO: DOM extraction failed: %s", e)
            return []

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"wego{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
