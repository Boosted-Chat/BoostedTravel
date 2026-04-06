"""
TransNusa CDP Chrome connector — Hitit Crane IBE form fill + response interception.

TransNusa (IATA: 8B) is an Indonesian airline based in Denpasar, Bali.
Uses Hitit Crane IBE booking engine at book-transnusa.crane.aero behind
Cloudflare turnstile — requires real Chrome (headed) to bypass.

Strategy (CDP Chrome + form fill + API response interception):
1. Launch REAL system Chrome (--remote-debugging-port) off-screen.
2. Connect via Playwright CDP. Persistent browser across searches.
3. Navigate to Crane IBE → wait for Cloudflare challenge to pass.
4. Fill search form (origin, destination, date, passengers).
5. Submit → intercept API responses for availability data.
6. Parse results from intercepted JSON or DOM scraping.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_IBE_URL = "https://book-transnusa.crane.aero/ibe/search"

# TransNusa route map — DPS (Bali) hub + international
_VALID_IATA: set[str] = {
    "BMU",  # Bima
    "CAN",  # Guangzhou
    "CGK",  # Jakarta (Soekarno-Hatta)
    "DPS",  # Denpasar (Ngurah Rai)
    "KUL",  # Kuala Lumpur
    "KWE",  # Guiyang
    "LOP",  # Lombok
    "MDC",  # Manado (Sam Ratulangi)
    "MWS",  # Mount Wilson (Sumbawa?)
    "PEN",  # Penang
    "SIN",  # Singapore
    "PER",  # Perth
    "YIA",  # Yogyakarta (YIA)
    "SUB",  # Surabaya
}

# CDP Chrome state — singleton shared across searches
_DEBUG_PORT = 9329
_USER_DATA_DIR = os.path.join(os.path.expanduser("~"), ".letsfg_transnusa_cdp")
_browser = None
_pw_instance = None
_chrome_proc: Optional[subprocess.Popen] = None
_context = None


async def _get_browser():
    """Get or launch persistent Chrome browser for TransNusa."""
    global _browser, _pw_instance, _chrome_proc, _context

    # Check existing connection
    if _browser is not None:
        try:
            if _browser.is_connected():
                return _browser
        except Exception:
            pass

    from playwright.async_api import async_playwright

    # Try connecting to existing Chrome on port
    pw = None
    try:
        pw = await async_playwright().start()
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        _pw_instance = pw
        logger.info("TransNusa: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch Chrome HEADED (Cloudflare blocks headless)
    chrome = find_chrome()
    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    args = [
        chrome,
        f"--remote-debugging-port={_DEBUG_PORT}",
        f"--user-data-dir={_USER_DATA_DIR}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",
        "--disable-http2",
        "--window-position=-2400,-2400",
        "--window-size=1366,768",
        "about:blank",
    ]
    _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
    _launched_procs.append(_chrome_proc)
    await asyncio.sleep(2)

    pw = await async_playwright().start()
    _pw_instance = pw
    _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
    logger.info("TransNusa: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
    return _browser


async def _get_context():
    """Get persistent browser context (carries cookies across searches)."""
    global _context
    if _context is not None:
        try:
            # Verify context is valid
            await _context.pages
            return _context
        except Exception:
            _context = None

    browser = await _get_browser()
    contexts = browser.contexts
    if contexts:
        _context = contexts[0]
    else:
        _context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
        )
    return _context


async def _reset_chrome_profile():
    """Kill Chrome and wipe user-data-dir on Cloudflare block."""
    global _browser, _chrome_proc, _context
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    _browser = None
    _context = None
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
        _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("TransNusa: deleted stale Chrome profile %s", _USER_DATA_DIR)
        except Exception as e:
            logger.warning("TransNusa: failed to delete Chrome profile: %s", e)


class TransNusaConnectorClient:
    """TransNusa — Hitit Crane IBE via CDP Chrome."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        if req.origin not in _VALID_IATA or req.destination not in _VALID_IATA:
            return self._empty(req)

        try:
            context = await _get_context()
            page = await context.new_page()

            # Response interception for Crane IBE API calls
            search_data: dict = {}
            cf_blocked = False

            async def _on_response(response):
                nonlocal cf_blocked
                url = response.url
                status = response.status
                ct = response.headers.get("content-type", "")

                if status == 403 and "cloudflare" in url.lower():
                    cf_blocked = True
                    return

                # Capture availability/search API responses
                if status == 200 and "json" in ct:
                    if any(kw in url.lower() for kw in [
                        "availability", "search", "flight", "fare", "offer", "schedule"
                    ]):
                        try:
                            data = await response.json()
                            if isinstance(data, (dict, list)):
                                search_data["api"] = data
                                logger.info("TransNusa: captured API response from %s", url[:80])
                        except Exception:
                            pass

                # Also capture HTML responses that might contain flight data
                if status == 200 and "html" in ct:
                    if any(kw in url.lower() for kw in ["availability", "select", "result"]):
                        try:
                            body = await response.text()
                            if len(body) > 1000:
                                search_data["html"] = body
                                logger.info("TransNusa: captured HTML response from %s", url[:80])
                        except Exception:
                            pass

            page.on("response", _on_response)

            try:
                logger.info("TransNusa: loading Crane IBE for %s->%s", req.origin, req.destination)
                await page.goto(_IBE_URL, wait_until="domcontentloaded", timeout=30000)

                # Wait for Cloudflare challenge to complete
                await self._wait_for_cf_pass(page)

                if cf_blocked:
                    logger.warning("TransNusa: Cloudflare blocked, resetting profile")
                    await _reset_chrome_profile()
                    return self._empty(req)

                # Wait for the Crane IBE app to load
                await asyncio.sleep(3)

                title = await page.title()
                logger.info("TransNusa: page title: %s, URL: %s", title, page.url)

                if "just a moment" in title.lower() or "challenge" in title.lower():
                    logger.warning("TransNusa: still on Cloudflare challenge page")
                    await _reset_chrome_profile()
                    return self._empty(req)

                # Fill the search form
                ok = await self._fill_search_form(page, req)
                if not ok:
                    logger.warning("TransNusa: form fill failed")
                    return self._empty(req)

                # Click search button
                clicked = await self._click_search(page)
                if not clicked:
                    logger.warning("TransNusa: could not click search")
                    return self._empty(req)

                # Wait for results
                remaining = max(self.timeout - (time.monotonic() - t0), 10)
                deadline = time.monotonic() + remaining
                while not search_data and not cf_blocked and time.monotonic() < deadline:
                    await asyncio.sleep(0.5)

                await asyncio.sleep(3)  # Extra time for DOM update

                # Parse results
                offers = []
                if "api" in search_data:
                    offers = self._parse_api_data(search_data["api"], req)
                if not offers and "html" in search_data:
                    offers = self._parse_html_results(search_data["html"], req)
                if not offers:
                    # Scrape current page DOM
                    html = await page.content()
                    offers = self._parse_html_results(html, req)

                offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                elapsed = time.monotonic() - t0
                logger.info(
                    "TransNusa %s->%s: %d offers in %.1fs (CDP Chrome)",
                    req.origin, req.destination, len(offers), elapsed,
                )

                h = hashlib.md5(
                    f"transnusa{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                ).hexdigest()[:12]
                return FlightSearchResponse(
                    search_id=f"fs_{h}",
                    origin=req.origin,
                    destination=req.destination,
                    currency="IDR",
                    offers=offers,
                    total_results=len(offers),
                )

            except Exception as e:
                logger.error("TransNusa CDP error: %s", e)
                return self._empty(req)
            finally:
                try:
                    await page.close()
                except Exception:
                    pass

        except Exception as e:
            logger.error("TransNusa browser launch error: %s", e)
            return self._empty(req)

    async def _wait_for_cf_pass(self, page, max_wait: float = 15.0) -> None:
        """Wait for Cloudflare turnstile challenge to complete."""
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            title = await page.title()
            if "just a moment" not in title.lower() and "challenge" not in title.lower():
                logger.info("TransNusa: Cloudflare challenge passed")
                return
            await asyncio.sleep(1)
        logger.warning("TransNusa: Cloudflare challenge did not pass within %.0fs", max_wait)

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill the Crane IBE search form."""
        try:
            # Dismiss any cookie banners
            for text in ["Accept", "Accept All", "I agree", "OK", "Got it"]:
                try:
                    btn = page.get_by_role("button", name=text)
                    if await btn.count() > 0:
                        await btn.first.click(timeout=2000)
                        await asyncio.sleep(0.5)
                        break
                except Exception:
                    continue

            # Try Hitit Crane IBE form patterns
            # Origin field
            origin_filled = False
            for selector in [
                'input[placeholder*="From"]',
                'input[placeholder*="Origin"]',
                'input[placeholder*="Departure"]',
                '#departureAirport', '#origin', '#from',
                'input[name*="origin"]', 'input[name*="from"]',
                'input[name*="departure"]',
                '.origin-input input', '.from-input input',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.3)
                        await el.fill(req.origin)
                        await asyncio.sleep(1)
                        # Try to select from autocomplete
                        try:
                            option = page.locator(f'text=/{req.origin}/i').first
                            if await option.count() > 0:
                                await option.click(timeout=2000)
                        except Exception:
                            # Press Enter to confirm
                            await el.press("Enter")
                        origin_filled = True
                        logger.info("TransNusa: filled origin with %s via %s", req.origin, selector)
                        break
                except Exception:
                    continue

            if not origin_filled:
                logger.warning("TransNusa: could not fill origin field")
                return False

            await asyncio.sleep(0.5)

            # Destination field
            dest_filled = False
            for selector in [
                'input[placeholder*="To"]',
                'input[placeholder*="Destination"]',
                'input[placeholder*="Arrival"]',
                '#arrivalAirport', '#destination', '#to',
                'input[name*="destination"]', 'input[name*="to"]',
                'input[name*="arrival"]',
                '.destination-input input', '.to-input input',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.3)
                        await el.fill(req.destination)
                        await asyncio.sleep(1)
                        try:
                            option = page.locator(f'text=/{req.destination}/i').first
                            if await option.count() > 0:
                                await option.click(timeout=2000)
                        except Exception:
                            await el.press("Enter")
                        dest_filled = True
                        logger.info("TransNusa: filled destination with %s via %s", req.destination, selector)
                        break
                except Exception:
                    continue

            if not dest_filled:
                logger.warning("TransNusa: could not fill destination field")
                return False

            await asyncio.sleep(0.5)

            # One-way toggle
            for selector in [
                'text=/one.?way/i',
                'input[value*="one"]',
                'label:has-text("One Way")',
                '.trip-type input[value="OW"]',
                '#oneWay',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        logger.info("TransNusa: selected one-way")
                        break
                except Exception:
                    continue

            await asyncio.sleep(0.5)

            # Date selection
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_filled = False

            for selector in [
                'input[placeholder*="Date"]',
                'input[placeholder*="Depart"]',
                '#departureDate', '#depDate',
                'input[name*="date"]', 'input[name*="depart"]',
                '.date-input input', '.departure-date input',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.5)

                        # Try different date formats
                        for fmt in [
                            dep_date.strftime("%d/%m/%Y"),
                            dep_date.strftime("%Y-%m-%d"),
                            dep_date.strftime("%d %b %Y"),
                            dep_date.strftime("%m/%d/%Y"),
                        ]:
                            try:
                                await el.fill(fmt)
                                await asyncio.sleep(0.3)
                                await el.press("Enter")
                                date_filled = True
                                logger.info("TransNusa: filled date with %s", fmt)
                                break
                            except Exception:
                                continue
                        if date_filled:
                            break
                except Exception:
                    continue

            # Fallback: try calendar navigation
            if not date_filled:
                date_filled = await self._navigate_calendar(page, dep_date)

            return origin_filled and dest_filled

        except Exception as e:
            logger.error("TransNusa form fill error: %s", e)
            return False

    async def _navigate_calendar(self, page, dep_date: datetime) -> bool:
        """Navigate a datepicker calendar to select the date."""
        try:
            # Click on calendar / date area
            for selector in ['.calendar', '.datepicker', '[class*="calendar"]', '[class*="date"]']:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    break

            await asyncio.sleep(1)

            # Navigate to correct month
            target_month = dep_date.strftime("%B %Y")
            for _ in range(12):
                heading = await page.locator('.calendar-header, .datepicker-header, [class*="month"]').first.text_content()
                if heading and target_month.lower() in heading.lower():
                    break
                # Click next month
                try:
                    await page.locator('button:has-text(">"), .next, [class*="next"]').first.click(timeout=1000)
                    await asyncio.sleep(0.3)
                except Exception:
                    break

            # Click the day
            day_str = str(dep_date.day)
            try:
                await page.locator(f'td:has-text("{day_str}"), button:has-text("{day_str}")').first.click(timeout=2000)
                logger.info("TransNusa: selected date %s via calendar", dep_date.strftime("%Y-%m-%d"))
                return True
            except Exception:
                return False
        except Exception:
            return False

    async def _click_search(self, page) -> bool:
        """Click the search/submit button."""
        for selector in [
            'button:has-text("Search")',
            'button:has-text("Find")',
            'button[type="submit"]',
            '#searchButton', '#btnSearch',
            '.search-btn', '.submit-btn',
            'input[type="submit"]',
        ]:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    logger.info("TransNusa: clicked search button via %s", selector)
                    return True
            except Exception:
                continue
        return False

    def _parse_api_data(self, data, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Crane IBE API JSON response."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        if isinstance(data, dict):
            # Hitit Crane IBE typically returns availability in various formats
            flights = (
                data.get("flights", []) or
                data.get("availability", []) or
                data.get("journeys", []) or
                data.get("data", {}).get("flights", []) or
                data.get("result", [])
            )
            if isinstance(flights, list):
                for flight in flights:
                    offer = self._parse_single_flight(flight, req, dep_date)
                    if offer:
                        offers.append(offer)

        elif isinstance(data, list):
            for item in data:
                offer = self._parse_single_flight(item, req, dep_date)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_single_flight(
        self, flight: dict, req: FlightSearchRequest, dep_date: datetime
    ) -> Optional[FlightOffer]:
        """Parse a single flight from Crane IBE API."""
        # Try various price keys
        price = None
        for key in ["totalPrice", "price", "fare", "amount", "total", "adultFare"]:
            val = flight.get(key)
            if val is not None:
                try:
                    price = float(val)
                    break
                except (ValueError, TypeError):
                    continue

        if not price or price <= 0:
            return None

        currency = flight.get("currency", "IDR")
        flight_no = flight.get("flightNumber", flight.get("flightNo", ""))

        # Parse departure/arrival
        dep_str = flight.get("departureTime", flight.get("departure", ""))
        arr_str = flight.get("arrivalTime", flight.get("arrival", ""))

        dep_dt = dep_date
        arr_dt = dep_date
        for dt_str, is_dep in [(dep_str, True), (arr_str, False)]:
            if dt_str:
                parsed = self._parse_datetime(dt_str, dep_date)
                if parsed:
                    if is_dep:
                        dep_dt = parsed
                    else:
                        arr_dt = parsed

        if arr_dt < dep_dt:
            arr_dt += timedelta(days=1)

        duration = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0
        stops = flight.get("stops", flight.get("stopovers", 0))

        segment = FlightSegment(
            airline="8B",
            airline_name="TransNusa",
            flight_no=str(flight_no),
            origin=req.origin,
            destination=req.destination,
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=duration,
            cabin_class="economy",
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=duration,
            stopovers=int(stops) if stops else 0,
        )

        fid = hashlib.md5(
            f"8b_{flight_no}_{price}_{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"8b_{fid}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{currency} {price:,.0f}",
            outbound=route,
            inbound=None,
            airlines=["TransNusa"],
            owner_airline="8B",
            booking_url=f"{_IBE_URL}?from={req.origin}&to={req.destination}&date={req.date_from}&adults={req.adults or 1}",
            is_locked=False,
            source="transnusa_direct",
            source_tier="free",
        )

    def _parse_html_results(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight results from HTML DOM."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        # Hitit Crane IBE common patterns
        # Look for fare/price containers
        price_blocks = re.findall(
            r'(?:class="[^"]*(?:fare|price|amount)[^"]*"[^>]*>.*?)([\d,.]+(?:\s*(?:IDR|USD|EUR))?)',
            html, re.S | re.I,
        )

        # Look for flight card/row patterns
        cards = re.findall(
            r'<(?:div|tr)[^>]*class="[^"]*(?:flight|journey|itinerary|bound)[^"]*"[^>]*>(.*?)</(?:div|tr)>',
            html, re.S | re.I,
        )

        for card_html in cards:
            # Extract price
            price_m = re.search(r'([\d,]+(?:\.\d{2})?)\s*(?:IDR|Rp|USD)?', card_html)
            if not price_m:
                continue
            try:
                price_str = price_m.group(1).replace(",", "")
                price = float(price_str)
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue

            # Extract times
            times = re.findall(r'(\d{1,2}:\d{2})', card_html)
            dep_dt = dep_date
            arr_dt = dep_date
            if len(times) >= 2:
                try:
                    dep_dt = datetime.strptime(f"{dep_date.strftime('%Y-%m-%d')} {times[0]}", "%Y-%m-%d %H:%M")
                    arr_dt = datetime.strptime(f"{dep_date.strftime('%Y-%m-%d')} {times[1]}", "%Y-%m-%d %H:%M")
                    if arr_dt < dep_dt:
                        arr_dt += timedelta(days=1)
                except ValueError:
                    pass

            duration = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            # Flight number
            fn_m = re.search(r'\b(8B\s*\d+|QG\s*\d+)\b', card_html)
            flight_no = fn_m.group(1).replace(" ", "") if fn_m else ""

            segment = FlightSegment(
                airline="8B",
                airline_name="TransNusa",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=duration,
                cabin_class="economy",
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=duration,
                stopovers=0,
            )

            fid = hashlib.md5(
                f"8b_{flight_no}_{price}_{req.date_from}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"8b_{fid}",
                price=round(price, 2),
                currency="IDR",
                price_formatted=f"IDR {price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=["TransNusa"],
                owner_airline="8B",
                booking_url=f"{_IBE_URL}?from={req.origin}&to={req.destination}&date={req.date_from}&adults={req.adults or 1}",
                is_locked=False,
                source="transnusa_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _parse_datetime(dt_str: str, fallback_date: datetime) -> Optional[datetime]:
        """Try to parse datetime from various formats."""
        for fmt in [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M",
            "%H:%M",
        ]:
            try:
                if fmt == "%H:%M":
                    t = datetime.strptime(dt_str.strip(), fmt)
                    return fallback_date.replace(hour=t.hour, minute=t.minute, second=0)
                return datetime.strptime(dt_str.strip(), fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"transnusa{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="IDR",
            offers=[],
            total_results=0,
        )
