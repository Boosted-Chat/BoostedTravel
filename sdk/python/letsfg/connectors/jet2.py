"""
Jet2 Playwright scraper — per-search browser with headless support.

Jet2 (IATA: LS) is a British low-cost leisure airline operating from
14 UK airports to 75+ destinations across Europe and beyond.

Strategy (per-search browser, Cloud Run compatible):
  1. For each search: Launch headless Chrome with unique temp directory
  2. Navigate to Jet2 homepage → accept cookies → establish session
  3. Navigate to cheap-flights search URL with calendar
  4. Intercept flight-schedules API OR parse £ prices from HTML calendar
  5. Close browser and cleanup temp directory

  URL pattern: /en/cheap-flights/{origin-slug}-{origin-iata}-to-{dest-slug}-{dest-iata}?from=YYYY-MM-DD&...
  Cookie banner: OneTrust ("Accept All Cookies")
  Anti-bot: Akamai Bot Manager — bypassed via proxy + real Chrome + stealth patches

PROXY REQUIREMENT:
  Jet2 is protected by Akamai Bot Manager which blocks requests from datacenter IPs
  and detects headless browsers. To reliably bypass:
  1. Set LETSFG_PROXY env var (datacenter proxy works, residential is better)
  2. On Cloud Run: Use xvfb for headed mode (DISPLAY=:99 xvfb-run ...)
     Or set DISPLAY env var if virtual display is available

CURRENT LIMITATIONS:
  - Returns "from £XX" promotional prices, not actual flight-specific prices
  - Flight times are midnight (00:00) placeholders since the landing page
    doesn't show specific flight schedules
  - To get actual flight times, would need to fill/submit the booking form

ENVIRONMENT VARIABLES:
  LETSFG_PROXY - Proxy URL (e.g., http://user:pass@host:port). Required for bypass.
  DISPLAY - Virtual display for headed mode on Linux (e.g., :99)
  K_SERVICE or CLOUD_RUN - Auto-detected for Cloud Run optimization
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import shutil
import sys
import tempfile
import time
from datetime import date, datetime
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .airline_routes import get_city_airports
from .browser import (
    auto_block_if_proxied,
    inject_stealth_js,
    stealth_args,
    get_default_proxy,
    proxy_is_configured,
    acquire_browser_slot,
    release_browser_slot,
)

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]
_LOCALES = ["en-GB", "en-US"]
_TIMEZONES = ["Europe/London", "Europe/Dublin"]

# ── Hardcoded IATA → Jet2 URL slug mapping ────────────────────────────────
_STATIC_SLUGS: dict[str, str] = {
    # UK departure airports
    "MAN": "manchester", "LBA": "leeds-bradford", "EMA": "east-midlands",
    "BHX": "birmingham", "NCL": "newcastle", "EDI": "edinburgh",
    "GLA": "glasgow", "BFS": "belfast-international", "BRS": "bristol",
    "STN": "london-stansted", "LGW": "london-gatwick", "EXT": "exeter",
    # Popular holiday destinations
    "BCN": "barcelona", "PMI": "majorca", "TFS": "tenerife", "LPA": "gran-canaria",
    "AGP": "malaga", "ALC": "alicante", "FAO": "faro",
    "IBZ": "ibiza", "MAH": "menorca", "FUE": "fuerteventura",
    "ACE": "lanzarote", "HER": "crete-heraklion", "RHO": "rhodes",
    "CFU": "corfu", "ZTH": "zante", "DLM": "dalaman",
    "AYT": "antalya", "BJV": "bodrum", "PFO": "paphos",
    "LCA": "larnaca", "SKG": "thessaloniki", "CHQ": "crete-chania",
    "KGS": "kos", "JSI": "skiathos", "SPU": "split",
    "DBV": "dubrovnik", "MJT": "lesvos", "JMK": "mykonos",
    "JTR": "santorini", "SPC": "la-palma", "VRN": "verona",
    "NAP": "naples", "PSA": "pisa", "BRI": "bari",
    "OLB": "sardinia", "CTA": "catania", "GRO": "girona",
    "REU": "reus", "BUD": "budapest", "KRK": "krakow",
    "GDN": "gdansk", "PRG": "prague", "RAK": "marrakech",
    "SSH": "sharm-el-sheikh", "HRG": "hurghada", "TIV": "tivat",
}

# ── Dynamic slug cache (populated from airport API) ─────────────────────────
_airport_slug_cache: dict[str, str] = {}


class Jet2ConnectorClient:
    """Jet2 per-search browser scraper — headless Chrome with stealth patches."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Jet2 flights via Playwright.

        Each search launches a fresh headless Chrome with a unique temp directory,
        performs the search, and cleans up. Safe for Cloud Run and parallel execution.
        """
        # ── City code expansion (LON → STN, LGW, etc.) ──
        origins = [a for a in get_city_airports(req.origin) if a in _STATIC_SLUGS]
        destinations = [a for a in get_city_airports(req.destination) if a in _STATIC_SLUGS]

        if not origins:
            origins = [req.origin] if req.origin in _STATIC_SLUGS else []
        if not destinations:
            destinations = [req.destination] if req.destination in _STATIC_SLUGS else []

        if not origins or not destinations:
            logger.info("Jet2: no valid routes for %s→%s", req.origin, req.destination)
            return self._empty(req)

        # Multiple airports → run sub-searches
        if len(origins) > 1 or len(destinations) > 1:
            return await self._multi_search(req, origins, destinations)

        return await self._search_single(req)

    async def _multi_search(
        self, req: FlightSearchRequest, origins: list[str], destinations: list[str]
    ) -> FlightSearchResponse:
        """Run multiple sub-searches for expanded city codes."""
        tasks = []
        for o in origins:
            for d in destinations:
                if o == d:
                    continue
                sub_req = FlightSearchRequest(
                    origin=o, destination=d,
                    date_from=req.date_from, return_from=req.return_from,
                    adults=req.adults, children=req.children, infants=req.infants,
                    cabin_class=req.cabin_class, currency=req.currency,
                    max_stopovers=req.max_stopovers,
                )
                tasks.append(self._search_single(sub_req))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_offers: list[FlightOffer] = []
        for r in results:
            if isinstance(r, FlightSearchResponse):
                all_offers.extend(r.offers)
        all_offers.sort(key=lambda o: o.price)

        search_hash = hashlib.md5(
            f"jet2{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=all_offers[0].currency if all_offers else "GBP",
            offers=all_offers, total_results=len(all_offers),
        )

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search a single origin→destination pair with a fresh browser."""
        t0 = time.monotonic()
        temp_dir = None
        pw_instance = None
        browser = None
        context = None

        # Resolve slugs before launching browser
        origin_slug = _STATIC_SLUGS.get(req.origin.upper()) or _airport_slug_cache.get(req.origin.upper())
        dest_slug = _STATIC_SLUGS.get(req.destination.upper()) or _airport_slug_cache.get(req.destination.upper())
        if not origin_slug or not dest_slug:
            logger.warning("Jet2: no slug for %s→%s", req.origin, req.destination)
            return self._empty(req)

        # Jet2 requires proxy to bypass Akamai on Cloud Run
        if not proxy_is_configured():
            logger.warning(
                "Jet2: LETSFG_PROXY not set — Akamai may block. Set proxy for reliable results."
            )

        try:
            # Acquire browser slot (respects max concurrent browsers)
            await acquire_browser_slot()

            # Create unique temp directory for this search
            temp_dir = tempfile.mkdtemp(prefix="jet2_")
            logger.debug("Jet2: using temp dir %s", temp_dir)

            # Launch Playwright with Chrome
            from playwright.async_api import async_playwright
            pw_instance = await async_playwright().start()

            viewport = random.choice(_VIEWPORTS)
            locale = random.choice(_LOCALES)
            tz = random.choice(_TIMEZONES)

            # Launch browser - use headless on Cloud Run (Linux), headed off-screen on Windows/dev
            visible = os.environ.get("BOOSTED_BROWSER_VISIBLE", "").strip() in ("1", "true")
            is_cloud_run = bool(os.environ.get("K_SERVICE") or os.environ.get("CLOUD_RUN"))
            is_linux = sys.platform.startswith("linux")
            
            # Cloud Run args for Chrome stability
            args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",  # Jet2 servers return ERR_HTTP2_PROTOCOL_ERROR
                "--disable-features=IsolateOrigins,site-per-process",  # Stability
                "--disable-web-security",  # Cross-origin requests
            ]
            if is_linux or is_cloud_run:
                args.extend([
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--single-process",  # Better memory usage on Cloud Run
                ])
            
            # Jet2's Akamai detects headless browsers. Options:
            # 1. On Cloud Run: Use xvfb to provide virtual display (DISPLAY=:99)
            # 2. With proxy: Datacenter IP helps but Akamai still checks browser
            # 3. Use headed mode everywhere (safest for Akamai bypass)
            #
            # Check if xvfb is available (DISPLAY set = xvfb or real display)
            has_display = bool(os.environ.get("DISPLAY"))
            
            if is_cloud_run or is_linux:
                if has_display:
                    # xvfb available, run headed for best Akamai bypass
                    use_headless = False
                    logger.info("Jet2: using headed mode with xvfb (DISPLAY=%s)", os.environ.get("DISPLAY"))
                else:
                    # No xvfb — try headless but Akamai may block
                    use_headless = True
                    args.append("--headless=new")  # Newer stealth headless mode
                    logger.warning("Jet2: no DISPLAY available, using headless mode — Akamai may block")
            else:
                # Windows/Mac dev - use headed
                use_headless = False
            
            logger.info("Jet2: launching Chrome (headless=%s, visible=%s, proxy=%s)", 
                        use_headless, visible, proxy_is_configured())
            browser = await pw_instance.chromium.launch(
                headless=use_headless,
                args=args,
            )
            context = await browser.new_context(
                viewport=viewport,
                locale=locale,
                timezone_id=tz,
                proxy=get_default_proxy(),
            )

            # Perform the search
            offers = await self._do_browser_search(context, req, origin_slug, dest_slug)

            elapsed = time.monotonic() - t0
            if offers:
                offers.sort(key=lambda o: o.price)
                logger.info("Jet2 %s→%s returned %d offers in %.1fs",
                            req.origin, req.destination, len(offers), elapsed)
            else:
                logger.info("Jet2 %s→%s returned 0 offers in %.1fs",
                            req.origin, req.destination, elapsed)

            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Jet2 %s→%s error: %s", req.origin, req.destination, e)
            return self._empty(req)

        finally:
            # Cleanup: close context, browser, stop Playwright, delete temp dir
            if context:
                try:
                    await context.close()
                except Exception:
                    pass
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
            if pw_instance:
                try:
                    await pw_instance.stop()
                except Exception:
                    pass
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception:
                    pass
            release_browser_slot()

    async def _do_browser_search(
        self, context, req: FlightSearchRequest, origin_slug: str, dest_slug: str
    ) -> list[FlightOffer]:
        """Run the browser search flow."""
        page = await context.new_page()
        await inject_stealth_js(page)
        await auto_block_if_proxied(page)

        try:
            # Set up response interception
            captured: dict[str, Any] = {}

            async def on_response(response):
                try:
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    if response.status != 200 or "json" not in ct:
                        return
                    
                    if "allairportinformation" in url:
                        data = await response.json()
                        self._update_slug_cache(data)
                    elif "flight-schedules" in url:
                        captured["schedules"] = await response.json()
                    elif "flightsearchresults" in url:
                        captured["results"] = await response.json()
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Visit homepage first to establish session (bypass anti-bot)
            logger.info("Jet2: loading homepage to establish session")
            try:
                await page.goto(
                    "https://www.jet2.com/",
                    wait_until="domcontentloaded",
                    timeout=45000,
                )
                logger.info("Jet2: homepage loaded successfully")
                await asyncio.sleep(2.0)
                await self._dismiss_overlays(page)
            except Exception as e:
                err_str = str(e)
                if "closed" in err_str.lower() or "crash" in err_str.lower():
                    logger.error("Jet2: browser closed during homepage load: %s", e)
                    return []
                logger.warning("Jet2: homepage load failed: %s", e)
                # Page might still be usable, try search anyway

            # Check if page is still alive
            try:
                _ = page.url
            except Exception:
                logger.error("Jet2: page is no longer available after homepage")
                return []

            # Step 2: Navigate to calendar page (WITHOUT date params - date-specific URLs 
            # return ERR_HTTP2_PROTOCOL_ERROR even with --disable-http2)
            # The calendar page shows prices for all available dates.
            # URL format: /cheap-flights/{origin-slug}-{origin-iata}-to-{dest-slug}-{dest-iata}
            origin_iata = req.origin.lower()
            dest_iata = req.destination.lower()
            calendar_url = (
                f"https://www.jet2.com/en/cheap-flights/"
                f"{origin_slug}-{origin_iata}-to-{dest_slug}-{dest_iata}"
            )
            
            for attempt in range(2):
                try:
                    logger.info("Jet2: navigating to calendar URL (attempt %d)", attempt + 1)
                    await page.goto(
                        calendar_url,
                        wait_until="domcontentloaded",
                        timeout=int(self.timeout * 1000),
                    )
                    logger.info("Jet2: calendar page loaded")
                    break
                except Exception as e:
                    err_str = str(e)
                    if "closed" in err_str.lower():
                        logger.error("Jet2: browser closed during calendar navigation")
                        return []
                    if attempt == 1:
                        raise
                    logger.warning("Jet2: calendar navigation failed, retrying: %s", e)
                    await asyncio.sleep(2.0)
            
            await asyncio.sleep(2.0)
            await self._dismiss_overlays(page)

            # Step 3: Wait for prices to load
            try:
                await page.wait_for_selector(
                    "[class*='price'], [class*='calendar'], table td",
                    timeout=10000,
                )
            except Exception:
                pass
            await asyncio.sleep(1.5)

            # Step 4: Parse offers from captured API or DOM
            offers: list[FlightOffer] = []

            if "schedules" in captured:
                offers = self._parse_schedule_data(captured["schedules"], req)

            if not offers:
                offers = await self._parse_dom_prices(page, req)

            return offers

        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _dismiss_overlays(self, page) -> None:
        """Dismiss OneTrust cookie banner and any other overlays."""
        for selector in ["#onetrust-accept-btn-handler", "button[id*='accept']"]:
            try:
                btn = page.locator(selector).first
                if await btn.count() > 0:
                    await btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    return
            except Exception:
                continue

        # Force-remove overlays
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '#onetrust-consent-sdk, [class*="onetrust"], [class*="cookie-consent"]'
                ).forEach(el => el.remove());
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    def _update_slug_cache(self, data: Any) -> None:
        """Update airport slug cache from API response."""
        global _airport_slug_cache
        if not data:
            return
        airports = data if isinstance(data, list) else data.get("airports", [])
        if not isinstance(airports, list):
            return
        for airport in airports:
            if not isinstance(airport, dict):
                continue
            iata = (airport.get("iataCode") or airport.get("code") or "").upper().strip()
            slug = (airport.get("seoUrl") or airport.get("slug") or "").strip().lower()
            if iata and slug:
                slug = slug.strip("/").rsplit("/", 1)[-1]
                _airport_slug_cache[iata] = slug

    def _parse_schedule_data(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight-schedules API response."""
        if not isinstance(data, dict):
            return []

        year_str = str(req.date_from.year)
        month_str = str(req.date_from.month)
        day_str = str(req.date_from.day)

        year_data = data.get(year_str, {})
        month_data = year_data.get(month_str, {})
        days = month_data.get("days", {})

        if day_str not in days:
            logger.debug("Jet2: schedule API says no flight on %s", req.date_from)
            return []

        day_info = days.get(day_str, {})
        price = day_info.get("price") or day_info.get("lowestPrice") or day_info.get("p")
        if not price:
            return []

        try:
            price = float(price)
        except (ValueError, TypeError):
            return []

        return [self._make_offer(price, req)]

    async def _parse_dom_prices(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse calendar prices from the page DOM.
        
        Jet2 calendar pages show prices per day. We try to:
        1. Find calendar cells with date and price data
        2. Match the requested date to get the exact price
        3. Fall back to finding any £ prices if calendar parsing fails
        """
        try:
            html = await page.content()
        except Exception:
            return []

        offers: list[FlightOffer] = []
        target_date = req.date_from
        target_day = str(target_date.day)
        target_month = target_date.month
        target_year = target_date.year
        
        # Strategy 1: Try to parse calendar table cells with data-date attributes
        try:
            # Get all td elements that might contain calendar data
            calendar_cells = await page.query_selector_all("td[data-date], td[data-day], .calendar-day")
            for cell in calendar_cells:
                try:
                    data_date = await cell.get_attribute("data-date")
                    if data_date and target_date.strftime("%Y-%m-%d") in data_date:
                        inner = await cell.inner_text()
                        prices = re.findall(r'£(\d+(?:\.\d{2})?)', inner)
                        if prices:
                            price = float(prices[0])
                            offers.append(self._make_offer(price, req))
                            logger.info("Jet2: found £%.2f for %s via calendar cell", price, target_date)
                            return offers
                except Exception:
                    continue
        except Exception:
            pass
        
        # Strategy 2: Find day number + adjacent price in HTML
        # Calendar cells often have format: <td>15<br>£60</td> or similar
        day_price_pattern = rf'>\s*{target_day}\s*(?:</?\w[^>]*>\s*)*£(\d+(?:\.\d{2})?)'
        matches = re.findall(day_price_pattern, html, re.IGNORECASE)
        if matches:
            try:
                price = float(matches[0])
                offers.append(self._make_offer(price, req))
                logger.info("Jet2: found £%.2f for day %s via regex", price, target_day)
                return offers
            except ValueError:
                pass

        # Strategy 3: Parse any visible flights for the date
        # Look for "Flights Available" indicators with prices
        flights_avail = re.findall(r'flights?\s+available[^£]*£(\d+(?:\.\d{2})?)', html, re.IGNORECASE)
        if flights_avail:
            try:
                price = float(flights_avail[0])
                offers.append(self._make_offer(price, req))
                logger.info("Jet2: found £%.2f via 'flights available' text", price)
                return offers
            except ValueError:
                pass

        # Strategy 4: Fallback - find all £ prices and take the lowest as "from" price
        all_prices = re.findall(r'£(\d+(?:\.\d{2})?)', html)
        if not all_prices:
            return []

        seen: set[float] = set()
        for price_str in all_prices:
            try:
                price = float(price_str)
            except ValueError:
                continue
            if price <= 0 or price >= 10000 or price in seen:
                continue
            seen.add(price)
            offers.append(self._make_offer(price, req))

        if offers:
            # Sort and take lowest prices as calendar "from" prices
            offers.sort(key=lambda o: o.price)
            logger.info("Jet2: found %d prices via fallback, lowest: £%.2f", len(offers), offers[0].price)
        
        return offers

    def _make_offer(self, price: float, req: FlightSearchRequest) -> FlightOffer:
        """Create a FlightOffer from a price."""
        dep_dt = datetime(req.date_from.year, req.date_from.month, req.date_from.day, 0, 0)
        segment = FlightSegment(
            airline="LS", airline_name="Jet2",
            flight_no="",
            origin=req.origin, destination=req.destination,
            departure=dep_dt, arrival=dep_dt,
            cabin_class="M",
        )
        route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
        offer_id = f"ls_{hashlib.md5(f'{req.date_from}_{price}'.encode()).hexdigest()[:12]}"
        return FlightOffer(
            id=offer_id,
            price=round(price, 2),
            currency="GBP",
            price_formatted=f"£{price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Jet2"],
            owner_airline="LS",
            booking_url=self._build_booking_url(req),
            is_locked=False,
            source="jet2_direct",
            source_tier="free",
        )

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        origin_slug = _STATIC_SLUGS.get(req.origin.upper(), req.origin.lower())
        dest_slug = _STATIC_SLUGS.get(req.destination.upper(), req.destination.lower())
        origin_iata = req.origin.lower()
        dest_iata = req.destination.lower()
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.jet2.com/en/cheap-flights/"
            f"{origin_slug}-{origin_iata}-to-{dest_slug}-{dest_iata}"
            f"?from={dep}&to={dep}&adults={req.adults}"
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"jet2{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "GBP",
            offers=offers,
            total_results=len(offers),
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"jet2{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="GBP",
            offers=[],
            total_results=0,
        )
