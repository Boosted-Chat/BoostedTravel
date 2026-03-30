"""
Volotea hybrid scraper — static schedule JSON API + Playwright fallback.

Volotea (V7) is a Spanish LCC based in Asturias, operating point-to-point
routes across Southern Europe (France, Italy, Spain, Greece, North Africa).
German routes are co-branded as Eurowings Discover (eurowings.volotea.com).

Strategy (hybrid, updated Mar 2026):
  1. PRIMARY (no auth needed, ~0.5-1s):
     GET https://json.volotea.com/dist/schedule/{ORIGIN}-{DEST}_schedule.json
     Returns full flight data with prices, fares, seats, times.
     Response: {"{ORIGIN}-{DEST}": [{Departure, Arrival, FlightNumber, CarrierCode,
                Prices: [{Price, PriceWithFee, FareType, Currency}], AvailableSeats, ...}]}
     Datetime format: YYYYMMDDTHHMM (e.g. 202603120810)
  2. FALLBACK: Full Playwright homepage form-fill + API interception (old path).

  Note: The schedule JSON URL uses {ORIGIN}-{DEST} or {DEST}-{ORIGIN} naming.
  Both files are tried — the reverse-direction file often has fresher data.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from curl_cffi import requests as cffi_requests

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, get_curl_cffi_proxies, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_SCHEDULE_URL = "https://json.volotea.com/dist/schedule/{route}_schedule.json"
_IMPERSONATE = "chrome124"

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]

_DEBUG_PORT = 9461
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", ".volotea_chrome_profile"
)
_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real headed Chrome via CDP (Incapsula blocks headless & new contexts).

    Incapsula on volotea.com blocks pages opened in Playwright-created contexts
    (browser.new_context()) but allows pages in Chrome's default context — the
    one Chrome creates for the initial URL.  So we launch Chrome with
    ``https://www.volotea.com/en/`` as the startup URL, then attach via CDP and
    grab ``browser.contexts[0]``.
    """
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

        # Try connecting to existing Chrome on the port first
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
            _pw_instance = pw
            logger.info("Volotea: connected to existing Chrome on port %d", _DEBUG_PORT)
            return _browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        # Launch Chrome HEADED with the Volotea homepage so the default
        # context passes Incapsula.
        chrome = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)
        args = [
            chrome,
            f"--remote-debugging-port={_DEBUG_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--no-first-run",
            *proxy_chrome_args(),
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            "https://www.volotea.com/en/",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(4.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        logger.info("Volotea: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
        return _browser


class VoloteaConnectorClient:
    """Volotea hybrid scraper — static schedule JSON + Playwright fallback."""

    def __init__(self, timeout: float = 50.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Volotea flights via static schedule JSON API.

        Fast path (~0.5-1s): curl_cffi GET to json.volotea.com schedule endpoint.
        Fallback: Full Playwright homepage form-fill + interception.
        """
        t0 = time.monotonic()

        # Try direct API first (no auth needed)
        offers = await self._api_search(req)

        if offers:
            elapsed = time.monotonic() - t0
            offers.sort(key=lambda o: o.price)
            logger.info(
                "Volotea %s→%s returned %d offers in %.1fs (schedule API)",
                req.origin, req.destination, len(offers), elapsed,
            )
            search_hash = hashlib.md5(
                f"volotea{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else (req.currency or "EUR"),
                offers=offers, total_results=len(offers),
            )

        # Fallback to Playwright
        logger.info("Volotea: schedule API failed, falling back to Playwright")
        return await self._playwright_fallback(req, t0)

    # ------------------------------------------------------------------
    # Direct API via curl_cffi (static schedule JSON)
    # ------------------------------------------------------------------

    async def _api_search(
        self, req: FlightSearchRequest,
    ) -> Optional[list[FlightOffer]]:
        """Fetch Volotea schedule JSON via curl_cffi."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._api_search_sync, req)

    def _api_search_sync(
        self, req: FlightSearchRequest,
    ) -> Optional[list[FlightOffer]]:
        """Synchronous curl_cffi schedule fetch."""
        route_key = f"{req.origin}-{req.destination}"
        reverse_key = f"{req.destination}-{req.origin}"

        sess = cffi_requests.Session(impersonate=_IMPERSONATE, proxies=get_curl_cffi_proxies())
        target_date = req.date_from.strftime("%Y%m%d")
        booking_url = self._build_booking_url(req)

        # Try both file directions — the "reverse" file often has fresher data
        # containing both route directions, while the primary may be stale.
        for try_key in (route_key, reverse_key):
            data = self._fetch_schedule(sess, try_key)
            if data is None:
                continue

            # The JSON is a dict like {"ATH-BCN": [...], "BCN-ATH": [...]}
            flights = None
            if isinstance(data, dict):
                flights = data.get(route_key)
                if not flights:
                    for key in data:
                        if key.upper() == route_key.upper():
                            flights = data[key]
                            break
            elif isinstance(data, list):
                flights = data

            if not flights or not isinstance(flights, list):
                continue

            offers: list[FlightOffer] = []
            for flight in flights:
                if not isinstance(flight, dict):
                    continue
                dep_str = flight.get("Departure", "")
                if not dep_str.startswith(target_date):
                    continue
                parsed = self._parse_schedule_flight(flight, req, booking_url)
                if parsed:
                    offers.extend(parsed)

            if offers:
                return offers

        return []

    def _fetch_schedule(self, sess, route_key: str) -> Optional[dict]:
        """Fetch a single schedule JSON file."""
        url = _SCHEDULE_URL.format(route=route_key)
        try:
            r = sess.get(url, timeout=10)
        except Exception as e:
            logger.error("Volotea: schedule fetch error: %s", e)
            return None

        if r.status_code == 404:
            return None
        if r.status_code != 200:
            return None

        ct = r.headers.get("content-type", "")
        if "json" not in ct:
            return None

        try:
            return r.json()
        except Exception:
            return None

    def _parse_schedule_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str,
    ) -> list[FlightOffer]:
        """Parse a single flight from the schedule JSON.

        Each flight has multiple Prices (fare bundles).
        Format: {Departure: "202603120810", Arrival: "202603121025",
                 FlightNumber: "5004", CarrierCode: "V7",
                 Prices: [{Price: 214.45, PriceWithFee: 214.45, FareType: "AE", Currency: "EUR"}],
                 AvailableSeats: 180, BookingClass: "K"}
        """
        prices_raw = flight.get("Prices", [])
        if not prices_raw:
            return []

        carrier = flight.get("CarrierCode") or "V7"
        flight_no = f"{carrier}{flight.get('FlightNumber') or ''}"
        dep_dt = self._parse_schedule_dt(flight.get("Departure", ""))
        arr_dt = self._parse_schedule_dt(flight.get("Arrival", ""))

        segment = FlightSegment(
            airline=carrier,
            airline_name="Volotea",
            flight_no=flight_no,
            origin=req.origin,
            destination=req.destination,
            departure=dep_dt,
            arrival=arr_dt,
            cabin_class="M",
        )

        total_dur = 0
        if dep_dt and arr_dt and arr_dt > dep_dt:
            total_dur = int((arr_dt - dep_dt).total_seconds())

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=max(total_dur, 0),
            stopovers=0,
        )

        # Check for connection flights
        conn = flight.get("ConnectionInformation")
        if conn and isinstance(conn, dict):
            # Has a connection — build multi-segment route
            conn_dep = self._parse_schedule_dt(conn.get("Departure", ""))
            conn_arr = self._parse_schedule_dt(conn.get("Arrival", ""))
            conn_origin = conn.get("DepartureStation", req.destination)
            conn_dest = conn.get("ArrivalStation", req.destination)
            conn_carrier = conn.get("CarrierCode", carrier)
            conn_flight_no = f"{conn_carrier}{conn.get('FlightNumber', '')}"

            segment2 = FlightSegment(
                airline=conn_carrier, airline_name="Volotea",
                flight_no=conn_flight_no,
                origin=conn_origin, destination=conn_dest,
                departure=conn_dep, arrival=conn_arr,
                cabin_class="M",
            )
            route = FlightRoute(
                segments=[segment, segment2],
                total_duration_seconds=max(int((conn_arr - dep_dt).total_seconds()), 0) if conn_arr > dep_dt else 0,
                stopovers=1,
            )

        offers: list[FlightOffer] = []
        for price_entry in prices_raw:
            if not isinstance(price_entry, dict):
                continue
            price = price_entry.get("PriceWithFee") or price_entry.get("Price")
            if price is None or price <= 0:
                continue
            currency = price_entry.get("Currency") or "EUR"
            fare_type = price_entry.get("FareType") or ""
            fare_basis = price_entry.get("FareBasis") or ""

            offer_key = f"{flight_no}_{fare_type}_{fare_basis}_{price}"
            offers.append(FlightOffer(
                id=f"v7_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                price=round(float(price), 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Volotea"],
                owner_airline=carrier,
                booking_url=booking_url,
                is_locked=False,
                source="volotea_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _parse_schedule_dt(s: str) -> datetime:
        """Parse Volotea schedule datetime: YYYYMMDDTHHMM or YYYYMMDDHHMM."""
        if not s:
            return datetime(2000, 1, 1)
        s = s.replace("T", "")  # normalize
        m = re.match(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})", s)
        if m:
            return datetime(int(m[1]), int(m[2]), int(m[3]), int(m[4]), int(m[5]))
        return datetime(2000, 1, 1)

    # ------------------------------------------------------------------
    # Playwright fallback
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full Playwright form-fill + API interception as fallback.

        MUST use the default browser context (contexts[0]) — Incapsula blocks
        pages opened in Playwright-created contexts.
        """
        browser = await _get_browser()

        # Use the default context — Incapsula passes only in this context.
        contexts = browser.contexts
        if not contexts:
            logger.error("Volotea: no default context available")
            return self._empty(req)
        context = contexts[0]

        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)

            # API response interception
            captured: dict[str, Any] = {}
            api_event = asyncio.Event()

            async def on_response(response):
                url = response.url
                ct = response.headers.get("content-type", "")
                if response.status == 200 and "json" in ct:
                    if "SearchFlights" in url or "searchflights" in url.lower():
                        try:
                            data = await response.json()
                            if data and isinstance(data, dict):
                                captured["flights"] = data
                                api_event.set()
                        except Exception:
                            pass

            page.on("response", on_response)

            # Step 1: Load homepage
            logger.info("Volotea: loading homepage")
            await page.goto(
                "https://www.volotea.com/en/",
                wait_until="domcontentloaded",
                timeout=25000,
            )
            # Wait for Incapsula challenge to pass and Angular to render
            for _wait in range(10):
                await asyncio.sleep(1.5)
                if await page.locator("input").count() > 3:
                    break
            await asyncio.sleep(1)

            # Step 2: Dismiss cookies
            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)

            # Step 3: Fill origin
            if not await self._fill_origin(page, req.origin):
                logger.warning("Volotea: origin fill failed for %s", req.origin)
                return self._empty(req)

            # Step 4: Fill destination
            if not await self._fill_destination(page, req.destination):
                logger.warning("Volotea: destination fill failed for %s", req.destination)
                return self._empty(req)

            # Step 5: Select one-way
            await self._select_one_way(page)

            # Step 6: Select date
            if not await self._select_date(page, req):
                logger.warning("Volotea: date selection failed")
                return self._empty(req)

            # Step 7: Click search
            await self._click_search(page)

            # Step 8: Wait for API response
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Volotea: API timeout after %.0fs", time.monotonic() - t0)

            # Parse API response
            if captured.get("flights"):
                offers = self._parse_api_response(captured["flights"], req)
                if offers:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)

            # DOM fallback: wait for cards on the booking page
            await asyncio.sleep(3)
            if "booking" in page.url.lower():
                offers = await self._extract_from_dom(page, req)
                if offers:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)

            logger.warning("Volotea: no flights found for %s→%s", req.origin, req.destination)
            return self._empty(req)

        except Exception as e:
            logger.error("Volotea error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Form interaction
    # ------------------------------------------------------------------

    async def _fill_origin(self, page, iata: str) -> bool:
        """Fill the origin airport field via the full-page city overlay.

        Volotea has TWO sets of inputs:
        - #input-text_sf-origin (readonly, in the form bar) — click to open overlay
        - #origin (editable, in the overlay) — type the IATA code here
        """
        try:
            # Click the form-bar input to open the overlay
            form_input = page.locator('#input-text_sf-origin')
            if await form_input.count() == 0:
                form_input = page.locator('input[placeholder="You are travelling from:"]').first
            await form_input.click(timeout=8000)
            await asyncio.sleep(2)

            # Type IATA into the overlay input.
            overlay_input = page.locator('#origin')
            if await overlay_input.count() == 0:
                logger.debug("Volotea: #origin overlay input not found")
                return False
            await overlay_input.fill(iata)
            await asyncio.sleep(2.5)

            # Click the city heading in the overlay
            return await self._pick_city_option(page, iata)
        except Exception as e:
            logger.debug("Volotea: origin fill error: %s", e)
            return False

    async def _fill_destination(self, page, iata: str) -> bool:
        """Fill the destination airport field via the city overlay.

        After origin selection, the destination overlay usually opens automatically.
        """
        try:
            await asyncio.sleep(1)
            # Check if overlay destination input is already available
            overlay_input = page.locator('#destination')
            available = await overlay_input.count() > 0
            disabled = await overlay_input.is_disabled() if available else True

            if not available or disabled:
                # Click the form-bar destination to open overlay
                form_input = page.locator('#input-text_sf-destination')
                if await form_input.count() == 0:
                    form_input = page.locator('input[placeholder="Where do you want to go?"]').first
                if await form_input.count() > 0:
                    await form_input.click(timeout=5000)
                    await asyncio.sleep(1)

            await overlay_input.click(timeout=3000)
            await overlay_input.press_sequentially(iata, delay=80)
            await asyncio.sleep(2.5)

            return await self._pick_city_option(page, iata)
        except Exception as e:
            logger.debug("Volotea: destination fill error: %s", e)
            return False

    async def _pick_city_option(self, page, iata: str) -> bool:
        """Pick a city from the Volotea city-selection overlay.

        The overlay shows cities grouped by country, each as a li element
        containing an h3 heading with the city name. Clicking the li
        selects the city and advances the form.
        """
        try:
            # Find city headings in the overlay — the IATA code appears in
            # a nested <li> and the city name in an <h3>
            # First try clicking the parent li of any h3 matching a city
            # that corresponds to this IATA code
            headings = page.locator('h3')
            count = await headings.count()
            for i in range(count):
                h3 = headings.nth(i)
                if not await h3.is_visible():
                    continue
                # Check if the sibling list contains the IATA code
                parent_li = h3.locator("xpath=ancestor::li[1]")
                if await parent_li.count() == 0:
                    continue
                text = await parent_li.text_content()
                if text and iata in text:
                    await parent_li.click(timeout=5000)
                    await asyncio.sleep(1.5)
                    return True

            # Fallback: click any visible text matching IATA
            option = page.locator(f'text="{iata}"').first
            if await option.count() > 0 and await option.is_visible():
                await option.click(timeout=3000)
                await asyncio.sleep(1.5)
                return True

            return False
        except Exception as e:
            logger.debug("Volotea: city pick error: %s", e)
            return False

    async def _select_one_way(self, page) -> None:
        """Select one-way trip in the calendar popup."""
        try:
            one_way = page.locator('text="One way"').first
            if await one_way.count() > 0:
                await one_way.click(timeout=3000)
                await asyncio.sleep(0.5)
                cont = page.locator('text="continue"').first
                if await cont.count() > 0:
                    await cont.click(timeout=2000)
                    await asyncio.sleep(0.5)
        except Exception:
            pass

    async def _select_date(self, page, req: FlightSearchRequest) -> bool:
        """Select the departure date from the calendar.

        The calendar shows 8 months as ``SF-CALENDAR-MONTH`` sections, each
        containing a ``.v7-cal`` grid.  Month labels sit in ``<p>`` elements
        with class ``v7-body-text`` inside each section (e.g. "June 2026.").
        We locate the section for the target month via JS, then use a
        Playwright click on the matching cell to properly trigger Angular.
        """
        try:
            target_day = req.date_from.day
            target_month = req.date_from.strftime("%B %Y")  # e.g. "March 2026"

            # Find the index of the correct calendar month section
            section_idx = await page.evaluate("""({targetMonth}) => {
                const sections = document.querySelectorAll('sf-calendar-month, .v7-calendar__section');
                for (let i = 0; i < sections.length; i++) {
                    const label = sections[i].querySelector('p.v7-body-text');
                    if (!label) continue;
                    const labelText = label.textContent.trim().replace('.', '');
                    if (labelText === targetMonth) return i;
                }
                return -1;
            }""", {"targetMonth": target_month})

            if section_idx >= 0:
                # Use Playwright locator scoped to the correct month section
                section = page.locator(
                    'sf-calendar-month, .v7-calendar__section'
                ).nth(section_idx)
                cells = section.locator('.v7-cal > *')
                count = await cells.count()
                for i in range(count):
                    cell = cells.nth(i)
                    if not await cell.is_visible():
                        continue
                    text = (await cell.text_content() or "").strip()
                    match = re.match(r'^(\d+)', text)
                    if match and int(match.group(1)) == target_day:
                        await cell.click(timeout=3000)
                        await asyncio.sleep(1.5)
                        return True

            # Fallback: click any visible day with target number and a price
            day_cells = page.locator('.v7-cal > *')
            count = await day_cells.count()
            for i in range(count):
                cell = day_cells.nth(i)
                if not await cell.is_visible():
                    continue
                text = (await cell.text_content() or "").strip()
                match = re.match(r'^(\d+)', text)
                if match and int(match.group(1)) == target_day and '\u20ac' in text:
                    await cell.click(timeout=3000)
                    await asyncio.sleep(1)
                    return True

            return False
        except Exception as e:
            logger.debug("Volotea: date select error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        """Click the 'Search flights' link/button.

        On Volotea, the search trigger is an ``<a>`` tag with class
        ``v7-sf__btn``.  It starts disabled (``v7-is-disabled``) and
        becomes enabled once origin, destination and date are set.
        There may be two such elements (desktop + mobile) — pick the
        first visible one that is not disabled.
        """
        try:
            links = page.locator('a.v7-sf__btn')
            count = await links.count()
            for idx in range(count):
                link = links.nth(idx)
                if not await link.is_visible():
                    continue
                # Wait up to 5s for it to lose the disabled class
                for _ in range(10):
                    cls = await link.get_attribute("class") or ""
                    if "v7-is-disabled" not in cls:
                        await link.click(timeout=5000)
                        return
                    await asyncio.sleep(0.5)
                # Try force-clicking anyway
                await link.click(timeout=5000, force=True)
                return
        except Exception:
            pass
        # Fallback: text match
        for label in ["Search flights", "Search", "Buscar vuelos"]:
            try:
                btn = page.locator(f'text="{label}"').first
                if await btn.count() > 0:
                    await btn.click(timeout=5000)
                    return
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Cookie dismissal
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        # Click accept button (various languages)
        for label in [
            "Accept cookies", "Aceptar cookies", "Accept all",
            "Aceptar todas", "Accetta tutto", "Tout accepter",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=3000)
                    await asyncio.sleep(0.3)
                    return
            except Exception:
                continue

        # JS fallback: remove OneTrust overlay entirely
        try:
            await page.evaluate("""() => {
                const sdk = document.getElementById('onetrust-consent-sdk');
                if (sdk) sdk.remove();
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # DOM extraction from flight results page
    # ------------------------------------------------------------------

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flights from .v7-c-flight-card DOM elements."""
        try:
            flights_data = await page.evaluate("""(reqDate) => {
                const cards = document.querySelectorAll('.v7-c-flight-card');
                if (!cards.length) return [];
                const results = [];
                cards.forEach(card => {
                    const schedule = card.querySelector('.v7-c-schedule');
                    if (!schedule) return;

                    const topSides = schedule.querySelectorAll(
                        '.v7-c-schedule__section.v7-is-top .v7-c-schedule__side'
                    );
                    const dep = topSides[0] ? topSides[0].textContent.trim() : '';
                    const arr = topSides[1] ? topSides[1].textContent.trim() : '';
                    const dur = schedule.querySelector('.v7-c-schedule__duration p');
                    const duration = dur ? dur.textContent.trim() : '';

                    const bottomSides = schedule.querySelectorAll(
                        '.v7-c-schedule__section.v7-is-bottom .v7-c-schedule__side'
                    );
                    const origin = bottomSides[0] ? bottomSides[0].textContent.trim() : '';
                    const dest = bottomSides[1] ? bottomSides[1].textContent.trim() : '';

                    // Price from CTA button
                    const priceBtn = card.querySelector('.v7-c-flight-card__ctas button');
                    let priceText = priceBtn ? priceBtn.textContent.trim() : '';
                    // Also try strong inside the button
                    if (!priceText) {
                        const strong = card.querySelector('.v7-c-flight-card__ctas strong');
                        priceText = strong ? strong.textContent.trim() : '';
                    }

                    if (dep && arr && priceText) {
                        results.push({ dep, arr, duration, origin, dest, priceText, reqDate });
                    }
                });
                return results;
            }""", req.date_from.isoformat())

            if not flights_data:
                return []

            offers: list[FlightOffer] = []
            booking_url = self._build_booking_url(req)
            for i, fd in enumerate(flights_data):
                offer = self._parse_dom_flight(fd, req, booking_url, i)
                if offer:
                    offers.append(offer)
            return offers
        except Exception as e:
            logger.debug("Volotea DOM extract error: %s", e)
            return []

    def _parse_dom_flight(
        self, fd: dict, req: FlightSearchRequest, booking_url: str, idx: int,
    ) -> Optional[FlightOffer]:
        # Parse price (e.g. "€249.60", "€44.99")
        price_match = re.search(r"[\d,.]+", fd.get("priceText", "").replace(",", ""))
        if not price_match:
            return None
        try:
            price = float(price_match.group())
        except ValueError:
            return None
        if price <= 0:
            return None

        # Parse times (format "01.55" or "12:15")
        dep_time = fd.get("dep", "").replace(".", ":")
        arr_time = fd.get("arr", "").replace(".", ":")
        date_str = req.date_from.strftime("%Y-%m-%d")

        dep_dt = self._parse_dt(f"{date_str}T{dep_time}")
        arr_dt = self._parse_dt(f"{date_str}T{arr_time}")
        # Handle overnight flights
        if arr_dt <= dep_dt:
            from datetime import timedelta
            arr_dt += timedelta(days=1)

        # Parse duration (e.g. "02:50h")
        dur_match = re.search(r"(\d+):(\d+)", fd.get("duration", ""))
        total_dur = 0
        if dur_match:
            total_dur = int(dur_match.group(1)) * 3600 + int(dur_match.group(2)) * 60
        elif dep_dt and arr_dt:
            total_dur = max(int((arr_dt - dep_dt).total_seconds()), 0)

        segment = FlightSegment(
            airline="V7",
            airline_name="Volotea",
            flight_no=f"V7{idx + 1:03d}",
            origin=req.origin,
            destination=req.destination,
            departure=dep_dt,
            arrival=arr_dt,
            cabin_class="M",
        )
        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=total_dur,
            stopovers=0,
        )
        flight_key = f"v7_{req.origin}{req.destination}_{date_str}_{dep_time}"
        return FlightOffer(
            id=f"v7_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency="EUR",
            price_formatted=f"{price:.2f} EUR",
            outbound=route,
            inbound=None,
            airlines=["Volotea"],
            owner_airline="V7",
            booking_url=booking_url,
            is_locked=False,
            source="volotea_direct",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # API response parsing
    # ------------------------------------------------------------------

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the SearchFlights API response.

        Structure: { header: {...}, payload: { trips: [{ schedules: [{ journeys: [...] }] }] } }
        Each journey has: departureDate, arrivalDate, travelTime, segments[], fares[]
        """
        if not data or not isinstance(data, dict):
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        payload = data.get("payload", data)
        trips = payload.get("trips", [])

        for trip in trips:
            if not isinstance(trip, dict):
                continue
            for schedule in trip.get("schedules", []):
                if not isinstance(schedule, dict):
                    continue
                for journey in schedule.get("journeys", []):
                    if not isinstance(journey, dict):
                        continue
                    offer = self._parse_journey(journey, req, booking_url)
                    if offer:
                        offers.append(offer)

        return offers

    def _parse_journey(
        self, journey: dict, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        """Parse a single journey from the API response."""
        price = self._cheapest_fare_price(journey.get("fares", []))
        if price is None or price <= 0:
            return None

        currency = "EUR"
        fares = journey.get("fares", [])
        if fares:
            fare_prices = fares[0].get("farePrices", [])
            if fare_prices:
                curr_obj = fare_prices[0].get("price", {}).get("currency", {})
                currency = curr_obj.get("code", "EUR")

        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []
        for seg in segments_raw:
            segments.append(self._build_segment(seg, req))

        if not segments:
            return None

        total_dur = self._parse_travel_time(journey.get("travelTime", ""))
        if total_dur == 0 and segments[0].departure and segments[-1].arrival:
            total_dur = max(int((segments[-1].arrival - segments[0].departure).total_seconds()), 0)

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(len(segments) - 1, 0),
        )

        sell_key = ""
        if fares:
            sell_key = fares[0].get("sellKey", "")
        if not sell_key:
            sell_key = f"{segments[0].flight_no}_{journey.get('departureDate', '')}"

        return FlightOffer(
            id=f"v7_{hashlib.md5(sell_key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=[segments[0].airline_name],
            owner_airline=segments[0].airline,
            booking_url=booking_url,
            is_locked=False,
            source="volotea_direct",
            source_tier="free",
        )

    def _build_segment(self, seg: dict, req: FlightSearchRequest) -> FlightSegment:
        """Build a FlightSegment from an API segment object."""
        operator = seg.get("operator", {})
        airline_code = operator.get("airlineCode") or "V7"
        airline_name = operator.get("airlineName") or "Volotea"
        flight_no = f"{airline_code}{operator.get('flightNumber', '')}"
        origin = seg.get("departureStationCode") or req.origin
        destination = seg.get("arrivalStationCode") or req.destination

        return FlightSegment(
            airline=airline_code,
            airline_name=airline_name,
            flight_no=flight_no,
            origin=origin,
            destination=destination,
            departure=self._parse_dt(seg.get("departureDate", "")),
            arrival=self._parse_dt(seg.get("arrivalDate", "")),
            cabin_class="M",
        )

    @staticmethod
    def _cheapest_fare_price(fares: list) -> Optional[float]:
        """Extract the cheapest adult fare price from fares list."""
        prices: list[float] = []
        for fare in fares:
            if not isinstance(fare, dict):
                continue
            for fp in fare.get("farePrices", []):
                if not isinstance(fp, dict):
                    continue
                amount = fp.get("price", {}).get("amount")
                if amount is not None:
                    try:
                        prices.append(float(amount))
                    except (TypeError, ValueError):
                        continue
        return min(prices) if prices else None

    @staticmethod
    def _parse_travel_time(tt: str) -> int:
        """Parse travelTime like '01:50:00' to seconds."""
        if not tt:
            return 0
        m = re.match(r"(\d+):(\d+):(\d+)", tt)
        if m:
            return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
        m = re.match(r"(\d+):(\d+)", tt)
        if m:
            return int(m.group(1)) * 3600 + int(m.group(2)) * 60
        return 0

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://book.volotea.com/booking/flights"
            f"?culture=en-GB&from={req.origin}&to={req.destination}"
            f"&departuredate={dep}&triptype=OneWay"
            f"&adults={req.adults}&children={req.children}&infants={req.infants}"
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float,
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Volotea %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        search_hash = hashlib.md5(f"volotea{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers, total_results=len(offers),
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"volotea{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[: len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)
