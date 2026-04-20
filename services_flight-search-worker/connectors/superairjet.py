"""
Super Air Jet CDP Chrome connector — IBE form fill via Playwright.

Super Air Jet (IATA: IU) is an Indonesian low-cost carrier.
Booking engine lives at secure.superairjet.com/SuperAirJetIBE/OnlineBooking.aspx —
page returns 0 bytes without JS, so real browser rendering is required.

Strategy (CDP Chrome + form fill + DOM/API interception):
1. Navigate to www.superairjet.com booking form.
2. Fill origin/destination/date/pax via the main site form.
3. Form submits via GET to secure.superairjet.com/SuperAirJetIBE/OnlineBooking.aspx.
4. Wait for JS-rendered results page.
5. Intercept XHR JSON or scrape DOM for flight cards.
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
from .browser import (
    find_chrome,
    stealth_popen_kwargs,
    _launched_procs,
    acquire_browser_slot,
    release_browser_slot,
    bandwidth_saving_args,
    disable_background_networking_args,
    apply_cdp_url_blocking,
)

logger = logging.getLogger(__name__)

_MAIN_URL = "https://www.superairjet.com/"
_BOOKING_URL = "https://secure.superairjet.com/SuperAirJetIBE/OnlineBooking.aspx"

# Known Super Air Jet domestic + short-haul routes (Indonesian domestic)
_VALID_IATA: set[str] = {
    "CGK",  # Jakarta (Soekarno-Hatta)
    "HLP",  # Jakarta (Halim)
    "SUB",  # Surabaya
    "DPS",  # Denpasar (Bali)
    "JOG",  # Yogyakarta
    "SOC",  # Solo
    "SRG",  # Semarang
    "BDO",  # Bandung
    "BPN",  # Balikpapan
    "UPG",  # Makassar
    "MDC",  # Manado
    "PDG",  # Padang
    "KNO",  # Medan (Kualanamu)
    "MES",  # Medan (Polonia)
    "PLM",  # Palembang
    "PKU",  # Pekanbaru
    "BTH",  # Batam
    "PNK",  # Pontianak
    "TKG",  # Bandar Lampung
    "BDJ",  # Banjarmasin
    "LOP",  # Lombok
    "AMQ",  # Ambon
    "DJB",  # Jambi
    "DJJ",  # Jayapura
    "LBJ",  # Labuan Bajo  
    "KOE",  # Kupang
    "YIA",  # Yogyakarta (YIA)
}

# CDP Chrome state
_DEBUG_PORT = 9331
_USER_DATA_DIR = os.path.join(os.path.expanduser("~"), ".letsfg_saj_cdp")
_browser = None
_pw_instance = None
_chrome_proc: Optional[subprocess.Popen] = None
_context = None


async def _get_browser():
    """Get or launch persistent Chrome browser for Super Air Jet."""
    global _browser, _pw_instance, _chrome_proc, _context

    if _browser is not None:
        try:
            if _browser.is_connected():
                return _browser
        except Exception:
            pass

    from playwright.async_api import async_playwright

    # Try connecting to existing Chrome
    pw = None
    try:
        pw = await async_playwright().start()
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        _pw_instance = pw
        logger.info("SuperAirJet: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch headed Chrome (secure.superairjet.com may need full browser)
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
        *bandwidth_saving_args(),
        *disable_background_networking_args(),
        "about:blank",
    ]
    _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
    _launched_procs.append(_chrome_proc)
    await asyncio.sleep(2)

    pw = await async_playwright().start()
    _pw_instance = pw
    _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
    logger.info("SuperAirJet: Chrome launched on port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
    return _browser


async def _get_context():
    global _context
    if _context is not None:
        try:
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


class SuperAirJetConnectorClient:
    """Super Air Jet — IBE booking via CDP Chrome."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        if req.origin not in _VALID_IATA or req.destination not in _VALID_IATA:
            return self._empty(req)

        from .browser import acquire_browser_slot, release_browser_slot

        await acquire_browser_slot()
        try:
            return await self._search_with_browser(req, t0)
        finally:
            release_browser_slot()

    async def _search_with_browser(
        self, req: FlightSearchRequest, t0: float
    ) -> FlightSearchResponse:
        context = await _get_context()
        page = await context.new_page()
        await apply_cdp_url_blocking(page)

        search_data: dict = {}

        async def _on_response(response):
            url = response.url
            status = response.status
            ct = response.headers.get("content-type", "")

            if status == 200 and "json" in ct:
                if any(kw in url.lower() for kw in [
                    "flight", "search", "avail", "fare", "schedule", "offer", "result"
                ]):
                    try:
                        data = await response.json()
                        if isinstance(data, (dict, list)):
                            search_data["api"] = data
                            logger.info("SuperAirJet: captured API from %s", url[:80])
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            # Approach 1: Navigate to main site, fill form, submit
            logger.info("SuperAirJet: loading main site for %s->%s", req.origin, req.destination)
            await page.goto(_MAIN_URL, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            # Dismiss cookies
            for text in ["Accept", "Accept All", "I agree", "OK", "Got it"]:
                try:
                    btn = page.get_by_role("button", name=text)
                    if await btn.count() > 0:
                        await btn.first.click(timeout=2000)
                        await asyncio.sleep(0.5)
                        break
                except Exception:
                    continue

            # Fill main site booking form
            # Form fields from probe: trip_type (radio), depCity2/arrCity2 (text autocomplete),
            # depart/dest.1 (hidden IATA), date.0/date.1, persons.0/1/2
            ok = await self._fill_main_form(page, req)

            if ok:
                # Wait for navigation to secure.superairjet.com
                await self._wait_for_results(page, search_data, t0)
            else:
                # Approach 2: Direct navigation to booking URL with params
                dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
                trip = "round+trip" if req.return_from else "one+way"
                direct_url = (
                    f"{_BOOKING_URL}"
                    f"?trip_type={trip}"
                    f"&depart={req.origin}"
                    f"&dest.1={req.destination}"
                    f"&date.0={dep_date.strftime('%d/%m/%Y')}"
                    f"&persons.0={req.adults or 1}"
                    f"&persons.1=0&persons.2=0"
                )
                if req.return_from:
                    ret_date = datetime.strptime(str(req.return_from), "%Y-%m-%d")
                    direct_url += f"&date.1={ret_date.strftime('%d/%m/%Y')}"
                logger.info("SuperAirJet: direct nav to booking URL")
                await page.goto(direct_url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(3)
                await self._wait_for_results(page, search_data, t0)

            # Parse results
            offers = []
            if "api" in search_data:
                offers = self._parse_api_data(search_data["api"], req)

            if not offers:
                html = await page.content()
                offers = self._parse_html_results(html, req)

            # RT pairing: if return_from and we have offers, build inbound route
            if req.return_from and offers:
                try:
                    ret_dt = datetime.strptime(str(req.return_from), "%Y-%m-%d")
                except (ValueError, TypeError):
                    ret_dt = None
                if ret_dt:
                    _iu_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                    cheapest = min(offers, key=lambda o: o.price)
                    ib_seg = FlightSegment(
                        airline="IU",
                        airline_name="Super Air Jet",
                        flight_no="IU",
                        origin=req.destination,
                        destination=req.origin,
                        departure=ret_dt,
                        arrival=ret_dt,
                        duration_seconds=0,
                        cabin_class=_iu_cabin,
                    )
                    ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)
                    ib_price = cheapest.price  # estimate inbound ≈ cheapest outbound

                    for o in offers:
                        o.inbound = ib_route
                        o.price = round(o.price + ib_price, 2)
                        o.price_formatted = f"{o.currency} {o.price:,.0f}"
                        o.id = o.id.replace("iu_", "iu_rt_")

            offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
            elapsed = time.monotonic() - t0
            logger.info(
                "SuperAirJet %s->%s: %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            h = hashlib.md5(
                f"saj{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
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
            logger.error("SuperAirJet CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_main_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill the booking form on www.superairjet.com."""
        try:
            # Select trip type
            if req.return_from:
                for selector in [
                    'input[value="round trip"]',
                    'input[name="trip_type"][value="round trip"]',
                    'label:has-text("Round Trip")',
                    'label:has-text("Pulang Pergi")',  # Indonesian
                ]:
                    try:
                        el = page.locator(selector).first
                        if await el.count() > 0:
                            await el.click(timeout=2000)
                            logger.info("SuperAirJet: selected round-trip")
                            break
                    except Exception:
                        continue
            else:
                for selector in [
                    'input[value="one way"]',
                    'input[name="trip_type"][value="one way"]',
                    'label:has-text("One Way")',
                    'label:has-text("Sekali Jalan")',  # Indonesian
                ]:
                    try:
                        el = page.locator(selector).first
                        if await el.count() > 0:
                            await el.click(timeout=2000)
                            logger.info("SuperAirJet: selected one-way")
                            break
                    except Exception:
                        continue

            await asyncio.sleep(0.5)

            # Fill departure city (depCity2 = text autocomplete, depart = hidden IATA)
            origin_filled = False
            for selector in [
                '#depCity2', 'input[name="depCity2"]',
                'input[placeholder*="From"]', 'input[placeholder*="Departure"]',
                'input[placeholder*="Origin"]',
                'input[placeholder*="Asal"]',  # Indonesian
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.3)
                        await el.fill(req.origin)
                        await asyncio.sleep(1)
                        # Select from autocomplete dropdown
                        try:
                            opt = page.locator(f'li:has-text("{req.origin}"), .autocomplete-item:has-text("{req.origin}"), [data-code="{req.origin}"]').first
                            if await opt.count() > 0:
                                await opt.click(timeout=2000)
                            else:
                                await el.press("Enter")
                        except Exception:
                            await el.press("Enter")
                        origin_filled = True
                        logger.info("SuperAirJet: filled origin %s", req.origin)
                        break
                except Exception:
                    continue

            # Also try setting the hidden field directly
            if not origin_filled:
                try:
                    await page.evaluate(f'''
                        var el = document.querySelector('input[name="depart"]');
                        if (el) {{ el.value = "{req.origin}"; }}
                    ''')
                    origin_filled = True
                except Exception:
                    pass

            if not origin_filled:
                return False

            await asyncio.sleep(0.5)

            # Fill arrival city
            dest_filled = False
            for selector in [
                '#arrCity2', 'input[name="arrCity2"]',
                'input[placeholder*="To"]', 'input[placeholder*="Arrival"]',
                'input[placeholder*="Destination"]',
                'input[placeholder*="Tujuan"]',  # Indonesian
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.3)
                        await el.fill(req.destination)
                        await asyncio.sleep(1)
                        try:
                            opt = page.locator(f'li:has-text("{req.destination}"), .autocomplete-item:has-text("{req.destination}"), [data-code="{req.destination}"]').first
                            if await opt.count() > 0:
                                await opt.click(timeout=2000)
                            else:
                                await el.press("Enter")
                        except Exception:
                            await el.press("Enter")
                        dest_filled = True
                        logger.info("SuperAirJet: filled destination %s", req.destination)
                        break
                except Exception:
                    continue

            if not dest_filled:
                try:
                    await page.evaluate(f'''
                        var el = document.querySelector('input[name="dest.1"]');
                        if (el) {{ el.value = "{req.destination}"; }}
                    ''')
                    dest_filled = True
                except Exception:
                    pass

            if not dest_filled:
                return False

            await asyncio.sleep(0.5)

            # Fill date
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_filled = False
            for selector in [
                '#date0', 'input[name="date.0"]',
                'input[placeholder*="Date"]', 'input[placeholder*="Tanggal"]',
                'input[type="date"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.5)
                        for fmt in [
                            dep_date.strftime("%d/%m/%Y"),
                            dep_date.strftime("%Y-%m-%d"),
                            dep_date.strftime("%d %b %Y"),
                        ]:
                            try:
                                await el.fill(fmt)
                                date_filled = True
                                logger.info("SuperAirJet: filled date %s", fmt)
                                break
                            except Exception:
                                continue
                        if date_filled:
                            break
                except Exception:
                    continue

            # Set hidden date field
            if not date_filled:
                try:
                    await page.evaluate(f'''
                        var el = document.querySelector('input[name="date.0"]');
                        if (el) {{ el.value = "{dep_date.strftime("%d/%m/%Y")}"; }}
                    ''')
                    date_filled = True
                except Exception:
                    pass

            # Set passengers
            adults = req.adults or 1

            # Fill return date if round-trip
            if req.return_from:
                ret_date = datetime.strptime(str(req.return_from), "%Y-%m-%d")
                ret_filled = False
                for selector in [
                    '#date1', 'input[name="date.1"]',
                    'input[placeholder*="Return"]', 'input[placeholder*="Kembali"]',
                ]:
                    try:
                        el = page.locator(selector).first
                        if await el.count() > 0:
                            await el.click(timeout=2000)
                            await asyncio.sleep(0.5)
                            for fmt in [
                                ret_date.strftime("%d/%m/%Y"),
                                ret_date.strftime("%Y-%m-%d"),
                                ret_date.strftime("%d %b %Y"),
                            ]:
                                try:
                                    await el.fill(fmt)
                                    ret_filled = True
                                    logger.info("SuperAirJet: filled return date %s", fmt)
                                    break
                                except Exception:
                                    continue
                            if ret_filled:
                                break
                    except Exception:
                        continue
                if not ret_filled:
                    try:
                        await page.evaluate(f'''
                            var el = document.querySelector('input[name="date.1"]');
                            if (el) {{ el.value = "{ret_date.strftime("%d/%m/%Y")}"; }}
                        ''')
                    except Exception:
                        pass

            try:
                await page.evaluate(f'''
                    var el = document.querySelector('select[name="persons.0"], input[name="persons.0"]');
                    if (el) {{ el.value = "{adults}"; }}
                ''')
            except Exception:
                pass

            # Click search button
            for selector in [
                'button:has-text("Search")',
                'button:has-text("Cari")',  # Indonesian
                'input[type="submit"]',
                'button[type="submit"]',
                '#btnSearch', '.search-btn',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("SuperAirJet: clicked search via %s", selector)
                        return True
                except Exception:
                    continue

            # Try form submission via JS
            try:
                await page.evaluate('''
                    var form = document.querySelector('form#bookingSection');
                    if (form) form.submit();
                ''')
                logger.info("SuperAirJet: submitted form via JS")
                return True
            except Exception:
                pass

            return origin_filled and dest_filled

        except Exception as e:
            logger.error("SuperAirJet form fill error: %s", e)
            return False

    async def _wait_for_results(self, page, search_data: dict, t0: float):
        """Wait for results page to load."""
        remaining = max(self.timeout - (time.monotonic() - t0), 10)
        deadline = time.monotonic() + remaining

        # Wait for URL change or content change
        while time.monotonic() < deadline:
            if search_data:
                return
            await asyncio.sleep(1)

            # Check if results are in the page
            try:
                html_len = await page.evaluate("document.body.innerHTML.length")
                if html_len > 2000:
                    # Check for flight-related content
                    has_flights = await page.evaluate('''
                        !!document.querySelector('.flight, .fare, [class*="flight"], [class*="result"], [class*="itinerary"]')
                    ''')
                    if has_flights:
                        return
            except Exception:
                pass

        await asyncio.sleep(2)

    def _parse_api_data(self, data, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse intercepted API JSON data."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        if isinstance(data, dict):
            flights = (
                data.get("flights", []) or
                data.get("data", {}).get("flights", []) or
                data.get("results", []) or
                data.get("journeys", []) or
                data.get("schedules", [])
            )
        elif isinstance(data, list):
            flights = data
        else:
            return []

        if isinstance(flights, list):
            for flight in flights:
                offer = self._parse_single_flight(flight, req, dep_date)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_single_flight(
        self, flight: dict, req: FlightSearchRequest, dep_date: datetime
    ) -> Optional[FlightOffer]:
        price = None
        for key in ["totalPrice", "price", "fare", "amount", "total", "adultFare", "displayPrice"]:
            val = flight.get(key)
            if val is not None:
                try:
                    price = float(str(val).replace(",", ""))
                    break
                except (ValueError, TypeError):
                    continue
        if not price or price <= 0:
            return None

        currency = flight.get("currency", "IDR")
        flight_no = flight.get("flightNumber", flight.get("flightNo", ""))

        dep_str = flight.get("departureTime", flight.get("departure", ""))
        arr_str = flight.get("arrivalTime", flight.get("arrival", ""))

        dep_dt = dep_date
        arr_dt = dep_date
        for dt_str, is_dep in [(dep_str, True), (arr_str, False)]:
            if dt_str:
                parsed = self._parse_dt(dt_str, dep_date)
                if parsed:
                    if is_dep:
                        dep_dt = parsed
                    else:
                        arr_dt = parsed

        if arr_dt < dep_dt:
            arr_dt += timedelta(days=1)
        duration = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

        _iu_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segment = FlightSegment(
            airline="IU",
            airline_name="Super Air Jet",
            flight_no=str(flight_no),
            origin=req.origin,
            destination=req.destination,
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=duration,
            cabin_class=_iu_cabin,
        )
        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=duration,
            stopovers=0,
        )

        fid = hashlib.md5(
            f"iu_{flight_no}_{price}_{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightOffer(
            id=f"iu_{fid}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{currency} {price:,.0f}",
            outbound=route,
            inbound=None,
            airlines=["Super Air Jet"],
            owner_airline="IU",
            booking_url=self._booking_url(req),
            is_locked=False,
            source="superairjet_direct",
            source_tier="free",
        )

    def _parse_html_results(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight results from rendered HTML."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        # Look for flight cards/rows in the DOM
        cards = re.findall(
            r'<(?:div|tr)[^>]*class="[^"]*(?:flight|fare|schedule|result|itinerary|journey)[^"]*"[^>]*>(.*?)</(?:div|tr)>',
            html, re.S | re.I,
        )

        for card_html in cards:
            # Extract price (IDR format: Rp 500.000 or IDR 500,000 or 500000)
            price_m = re.search(
                r'(?:Rp\.?\s*|IDR\s*)?(\d[\d.,]+)\s*(?:IDR|Rp)?',
                card_html, re.I,
            )
            if not price_m:
                continue
            try:
                price_str = price_m.group(1).replace(".", "").replace(",", "")
                price = float(price_str)
            except (ValueError, TypeError):
                continue
            if price <= 0 or price < 50000:  # Min IDR 50k for a flight
                continue

            # Times
            times = re.findall(r'(\d{1,2}:\d{2})', card_html)
            dep_dt = dep_date
            arr_dt = dep_date
            if len(times) >= 2:
                try:
                    dep_dt = datetime.strptime(
                        f"{dep_date.strftime('%Y-%m-%d')} {times[0]}", "%Y-%m-%d %H:%M"
                    )
                    arr_dt = datetime.strptime(
                        f"{dep_date.strftime('%Y-%m-%d')} {times[1]}", "%Y-%m-%d %H:%M"
                    )
                    if arr_dt < dep_dt:
                        arr_dt += timedelta(days=1)
                except ValueError:
                    pass

            duration = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            fn_m = re.search(r'\b(IU\s*\d+)\b', card_html)
            flight_no = fn_m.group(1).replace(" ", "") if fn_m else ""

            _iu_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            segment = FlightSegment(
                airline="IU",
                airline_name="Super Air Jet",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=duration,
                cabin_class=_iu_cabin,
            )
            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=duration,
                stopovers=0,
            )
            fid = hashlib.md5(
                f"iu_{flight_no}_{price}_{req.date_from}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"iu_{fid}",
                price=round(price, 2),
                currency="IDR",
                price_formatted=f"IDR {price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=["Super Air Jet"],
                owner_airline="IU",
                booking_url=self._booking_url(req),
                is_locked=False,
                source="superairjet_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _parse_dt(dt_str: str, fallback: datetime) -> Optional[datetime]:
        for fmt in [
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M", "%H:%M",
        ]:
            try:
                if fmt == "%H:%M":
                    t = datetime.strptime(dt_str.strip(), fmt)
                    return fallback.replace(hour=t.hour, minute=t.minute, second=0)
                return datetime.strptime(dt_str.strip(), fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        try:
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            dep_date = datetime.now()
        trip = "round+trip" if req.return_from else "one+way"
        url = (
            f"{_BOOKING_URL}"
            f"?trip_type={trip}"
            f"&depart={req.origin}"
            f"&dest.1={req.destination}"
            f"&date.0={dep_date.strftime('%d/%m/%Y')}"
            f"&persons.0={req.adults or 1}"
            f"&persons.1=0&persons.2=0"
        )
        if req.return_from:
            try:
                ret_date = datetime.strptime(str(req.return_from), "%Y-%m-%d")
                url += f"&date.1={ret_date.strftime('%d/%m/%Y')}"
            except (ValueError, TypeError):
                pass
        return url


    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req,
    ) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_supe_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"saj{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="IDR",
            offers=[],
            total_results=0,
        )
