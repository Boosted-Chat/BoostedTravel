"""
Citilink CDP Chrome connector — Navitaire IBE form fill.

Citilink (IATA: QG) is an Indonesian low-cost carrier (Garuda subsidiary).
Booking engine at book.citilink.co.id is Navitaire behind Cloudflare WAF —
all HTTP requests return 403. Real Chrome required.

Strategy (CDP Chrome + form fill + response/DOM scraping):
1. Launch real system Chrome headed + off-screen.
2. Navigate to book.citilink.co.id or www.citilink.co.id.
3. Fill search form (origin, destination, date, passengers).
4. Submit → wait for availability page.
5. Intercept Navitaire API responses or scrape rendered DOM.
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
    proxy_chrome_args,
    auto_block_if_proxied,
)

logger = logging.getLogger(__name__)

_BOOK_URL = "https://book.citilink.co.id/Search.aspx"
_HOME_URL = "https://www.citilink.co.id/"

# Indonesian domestic + short-haul international routes
_VALID_IATA: set[str] = {
    "CGK",  # Jakarta (Soekarno-Hatta)
    "HLP",  # Jakarta (Halim)
    "SUB",  # Surabaya
    "DPS",  # Denpasar (Bali)
    "JOG",  # Yogyakarta (Adisucipto)
    "YIA",  # Yogyakarta (YIA)
    "SOC",  # Solo
    "SRG",  # Semarang
    "BDO",  # Bandung
    "BPN",  # Balikpapan
    "UPG",  # Makassar
    "MDC",  # Manado
    "PDG",  # Padang
    "KNO",  # Medan (Kualanamu)
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
    "KUL",  # Kuala Lumpur
    "SIN",  # Singapore
    "PEN",  # Penang
    "BKK",  # Bangkok
    "JED",  # Jeddah
    "MED",  # Medina
}

# CDP Chrome state
_DEBUG_PORT = 9335
_USER_DATA_DIR = os.path.join(os.path.expanduser("~"), ".letsfg_citilink_cdp")
_browser = None
_pw_instance = None
_chrome_proc: Optional[subprocess.Popen] = None
_context = None


async def _get_browser():
    """Get or launch persistent Chrome browser for Citilink."""
    global _browser, _pw_instance, _chrome_proc, _context

    if _browser is not None:
        try:
            if _browser.is_connected():
                return _browser
        except Exception:
            pass

    from playwright.async_api import async_playwright

    pw = None
    try:
        pw = await async_playwright().start()
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        _pw_instance = pw
        logger.info("Citilink: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

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
        *proxy_chrome_args(),
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
    logger.info("Citilink: Chrome launched on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
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


async def _reset_profile():
    """Kill Chrome and wipe profile on Cloudflare block."""
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
        except Exception:
            pass


class CitilinkConnectorClient:
    """Citilink — Navitaire booking via CDP Chrome."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        if req.origin not in _VALID_IATA or req.destination not in _VALID_IATA:
            return self._empty(req)

        await acquire_browser_slot()
        try:
            ob_result = await self._search_cdp(req, t0)
        finally:
            release_browser_slot()

        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            if ib_req.origin in _VALID_IATA and ib_req.destination in _VALID_IATA:
                await acquire_browser_slot()
                try:
                    ib_result = await self._search_cdp(ib_req, t0)
                finally:
                    release_browser_slot()
                if ib_result.total_results > 0:
                    ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                    ob_result.total_results = len(ob_result.offers)

        return ob_result

    async def _search_cdp(
        self, req: FlightSearchRequest, t0: float
    ) -> FlightSearchResponse:
        context = await _get_context()
        page = await context.new_page()

        search_data: dict = {}
        cf_blocked = False

        async def _on_response(response):
            nonlocal cf_blocked
            url = response.url
            status = response.status
            ct = response.headers.get("content-type", "")

            if status == 403:
                if "challenge" in url.lower() or "cloudflare" in url.lower():
                    cf_blocked = True
                return

            if status == 200 and "json" in ct:
                lurl = url.lower()
                if any(kw in lurl for kw in [
                    "avail", "search", "flight", "fare", "schedule",
                    "price", "journey", "offer",
                ]):
                    try:
                        data = await response.json()
                        if isinstance(data, (dict, list)):
                            search_data["api"] = data
                            logger.info("Citilink: captured API from %s", url[:80])
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            # Try Navitaire booking engine first (less aggressive WAF than main site)
            logger.info("Citilink: loading booking site for %s->%s", req.origin, req.destination)
            await page.goto(_BOOK_URL, wait_until="domcontentloaded", timeout=25000)
            await asyncio.sleep(3)

            title = await page.title()
            content = await page.content()
            current_url = page.url
            logger.info("Citilink: booking page — url=%s title=%r content_len=%d", current_url, title[:80], len(content))

            booking_blocked = "403" in title or "forbidden" in title.lower() or "you have been blocked" in content.lower()

            if booking_blocked:
                # Fallback to main website
                logger.info("Citilink: booking site blocked, trying main site")
                await page.goto(_HOME_URL, wait_until="domcontentloaded", timeout=25000)
                await asyncio.sleep(3)

                title = await page.title()
                content = await page.content()
                current_url = page.url
                logger.info("Citilink: main page — url=%s title=%r content_len=%d", current_url, title[:80], len(content))
                if "403" in title or "forbidden" in title.lower() or "you have been blocked" in content.lower():
                    logger.warning("Citilink: WAF blocked (403 Forbidden) — title=%r url=%s", title, current_url)
                    raise RuntimeError("403 Forbidden - WAF blocked")

            # Wait for possible Cloudflare challenge
            await self._wait_for_cf(page)

            title = await page.title()
            if "just a moment" in title.lower():
                logger.warning("Citilink: stuck on Cloudflare challenge")
                await _reset_profile()
                raise RuntimeError("Cloudflare challenge blocked")

            # Dismiss popups
            await self._dismiss_popups(page)

            # Check if we got redirected away from booking page (e.g. F5 challenge → homepage)
            on_booking = "book.citilink.co.id" in page.url.lower()

            if not booking_blocked and not on_booking:
                # F5 resolved but redirected us to homepage — navigate back (cookies are set now)
                logger.info("Citilink: WAF redirected to %s, re-navigating to booking page", page.url[:80])
                await page.goto(_BOOK_URL, wait_until="domcontentloaded", timeout=25000)
                await asyncio.sleep(5)
                on_booking = "book.citilink.co.id" in page.url.lower()
                title = await page.title()
                content = await page.content()
                logger.info("Citilink: re-nav booking — url=%s title=%r content_len=%d", page.url, title[:80], len(content))
                if "403" in title or "forbidden" in title.lower() or (len(content) < 2000 and not title):
                    on_booking = False

            # Fill search form based on which page we're actually on
            if on_booking:
                ok = await self._fill_navitaire_form(page, req)
            else:
                # On homepage or other page — try homepage form
                ok = await self._fill_form(page, req)
                if not ok:
                    # Last resort: try booking page directly
                    logger.info("Citilink: homepage form failed, trying direct Navitaire URL")
                    logger.info("Citilink: trying direct Navitaire URL")
                    await page.goto(_BOOK_URL, wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(3)
                    
                    # Check for WAF block on booking site
                    title = await page.title()
                    content = await page.content()
                    if "403" in title or "forbidden" in title.lower() or "you have been blocked" in content.lower():
                        logger.warning("Citilink: WAF blocked on booking site (403 Forbidden)")
                        raise RuntimeError("403 Forbidden - WAF blocked")
                    
                    await self._wait_for_cf(page)
                    ok = await self._fill_navitaire_form(page, req)

            if ok:
                # Wait for results
                remaining = max(self.timeout - (time.monotonic() - t0), 10)
                deadline = time.monotonic() + remaining
                while time.monotonic() < deadline:
                    if search_data or cf_blocked:
                        break
                    await asyncio.sleep(1)

                await asyncio.sleep(3)

            # Parse results
            offers = []
            if "api" in search_data:
                offers = self._parse_api(search_data["api"], req)
            if not offers:
                html = await page.content()
                offers = self._parse_html(html, req)

            offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
            elapsed = time.monotonic() - t0
            logger.info("Citilink %s->%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            h = hashlib.md5(
                f"citilink{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency="IDR",
                offers=offers,
                total_results=len(offers),
            )

        except RuntimeError:
            raise  # Let block-detection errors propagate for retry logic
        except Exception as e:
            logger.error("Citilink CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _wait_for_cf(self, page, max_wait: float = 15.0):
        """Wait for Cloudflare challenge to pass."""
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            title = await page.title()
            if "just a moment" not in title.lower() and "challenge" not in title.lower():
                return
            await asyncio.sleep(1)

    async def _dismiss_popups(self, page):
        for label in ["Accept", "Accept All", "I agree", "OK", "Got it", "Close"]:
            try:
                btn = page.get_by_role("button", name=label)
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    break
            except Exception:
                continue

    async def _fill_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill Citilink main site search form."""
        try:
            # One-way toggle
            for selector in [
                'text=/one.?way/i', 'label:has-text("One Way")',
                'label:has-text("Sekali Jalan")',
                'input[value="OW"]', '#oneWay',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        break
                except Exception:
                    continue

            await asyncio.sleep(0.5)

            # Origin
            origin_ok = await self._fill_city(
                page, req.origin,
                ['#origin', 'input[placeholder*="From"]', 'input[placeholder*="Asal"]',
                 'input[name*="origin"]', '.origin-field input'],
                "origin",
            )
            if not origin_ok:
                return False

            await asyncio.sleep(0.5)

            # Destination
            dest_ok = await self._fill_city(
                page, req.destination,
                ['#destination', 'input[placeholder*="To"]', 'input[placeholder*="Tujuan"]',
                 'input[name*="destination"]', '.destination-field input'],
                "destination",
            )
            if not dest_ok:
                return False

            await asyncio.sleep(0.5)

            # Date
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
            for selector in [
                '#departureDate', 'input[placeholder*="Date"]',
                'input[placeholder*="Tanggal"]', 'input[name*="date"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.5)
                        for fmt in [dep_date.strftime("%d/%m/%Y"), dep_date.strftime("%Y-%m-%d")]:
                            try:
                                await el.fill(fmt)
                                break
                            except Exception:
                                continue
                        break
                except Exception:
                    continue

            # Click search
            for selector in [
                'button:has-text("Search")', 'button:has-text("Cari")',
                'button[type="submit"]', '#btnSearch', '.search-button',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("Citilink: clicked search")
                        return True
                except Exception:
                    continue

            return False

        except Exception as e:
            logger.error("Citilink form fill error: %s", e)
            return False

    async def _fill_navitaire_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill Navitaire booking form at book.citilink.co.id."""
        try:
            # Navitaire has specific field patterns
            # Origin station
            for selector in [
                '#TextBoxMarketOrigin1', '#TextBoxMarketOrigin0',
                'input[id*="Origin"]', 'input[name*="origin"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await el.fill(req.origin)
                        await asyncio.sleep(1)
                        # Navitaire autocomplete
                        try:
                            opt = page.locator(f'li:has-text("{req.origin}")').first
                            if await opt.count() > 0:
                                await opt.click(timeout=2000)
                        except Exception:
                            await el.press("Tab")
                        break
                except Exception:
                    continue

            # Destination station
            for selector in [
                '#TextBoxMarketDestination1', '#TextBoxMarketDestination0',
                'input[id*="Destination"]', 'input[name*="destination"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await el.fill(req.destination)
                        await asyncio.sleep(1)
                        try:
                            opt = page.locator(f'li:has-text("{req.destination}")').first
                            if await opt.count() > 0:
                                await opt.click(timeout=2000)
                        except Exception:
                            await el.press("Tab")
                        break
                except Exception:
                    continue

            # Date
            dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")
            for selector in ['#TextBoxMarketDepartDate1', 'input[id*="DepartDate"]']:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.fill(dep_date.strftime("%d/%m/%Y"))
                        break
                except Exception:
                    continue

            # One-way radio
            try:
                await page.locator('#RadioButtonMarketStructureOneWay, input[value="OneWay"]').first.click(timeout=2000)
            except Exception:
                pass

            # Search button
            for selector in [
                '#buttonSubmit', '#ControlGroupSearchView_ButtonSubmit',
                'button:has-text("Search")', 'input[type="submit"]',
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("Citilink: clicked Navitaire search")
                        return True
                except Exception:
                    continue

            return False

        except Exception as e:
            logger.error("Citilink Navitaire form error: %s", e)
            return False

    async def _fill_city(
        self, page, iata: str, selectors: list[str], label: str
    ) -> bool:
        for selector in selectors:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    await el.fill(iata)
                    await asyncio.sleep(1)
                    # Autocomplete
                    for opt_sel in [
                        f'li:has-text("{iata}")',
                        f'[data-code="{iata}"]',
                        f'.autocomplete-item:has-text("{iata}")',
                    ]:
                        try:
                            opt = page.locator(opt_sel).first
                            if await opt.count() > 0:
                                await opt.click(timeout=2000)
                                logger.info("Citilink: filled %s = %s", label, iata)
                                return True
                        except Exception:
                            continue
                    await el.press("Enter")
                    return True
            except Exception:
                continue
        return False

    def _parse_api(self, data, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Navitaire API JSON."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        if isinstance(data, dict):
            # Navitaire-style: data.journeys[].segments[]
            journeys = (
                data.get("journeys", []) or
                data.get("data", {}).get("journeys", []) or
                data.get("schedules", []) or
                data.get("flights", [])
            )
        elif isinstance(data, list):
            journeys = data
        else:
            return []

        for journey in journeys:
            if not isinstance(journey, dict):
                continue

            fares = journey.get("fares", [journey])
            for fare in fares:
                price = None
                for key in [
                    "totalPrice", "price", "fare", "amount",
                    "adultFare", "displayPrice",
                ]:
                    val = fare.get(key)
                    if val is not None:
                        try:
                            price = float(str(val).replace(",", ""))
                            break
                        except (ValueError, TypeError):
                            continue

                if not price or price <= 0:
                    continue

                segs = journey.get("segments", fare.get("segments", []))
                flight_no = ""
                dep_dt = dep_date
                arr_dt = dep_date

                if segs:
                    seg0 = segs[0]
                    flight_no = seg0.get("flightNumber", seg0.get("flightNo", ""))
                    dep_str = seg0.get("departureTime", seg0.get("departure", ""))
                    arr_str = seg0.get("arrivalTime", seg0.get("arrival", ""))
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
                dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

                _qg_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                segment = FlightSegment(
                    airline="QG",
                    airline_name="Citilink",
                    flight_no=str(flight_no),
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=dur,
                    cabin_class=_qg_cabin,
                )
                route = FlightRoute(
                    segments=[segment],
                    total_duration_seconds=dur,
                    stopovers=0,
                )
                fid = hashlib.md5(
                    f"qg_{flight_no}_{price}_{req.date_from}".encode()
                ).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"qg_{fid}",
                    price=round(price, 2),
                    currency="IDR",
                    price_formatted=f"IDR {price:,.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=["Citilink"],
                    owner_airline="QG",
                    booking_url=_BOOK_URL,
                    is_locked=False,
                    source="citilink_direct",
                    source_tier="free",
                ))

        return offers

    def _parse_html(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Navitaire rendered HTML for flight results."""
        offers = []
        dep_date = datetime.strptime(str(req.date_from), "%Y-%m-%d")

        # Navitaire HTML patterns: fare-row, flight-strip, availability row
        cards = re.findall(
            r'<(?:div|tr|li)[^>]*class="[^"]*(?:fare|flight|avail|journey|schedule)[^"]*"[^>]*>(.*?)</(?:div|tr|li)>',
            html, re.S | re.I,
        )

        for card in cards:
            # Price (IDR format)
            price_m = re.search(
                r'(?:Rp\.?\s*|IDR\s*)?(\d[\d.,]+)',
                card, re.I,
            )
            if not price_m:
                continue
            try:
                ps = price_m.group(1).replace(".", "").replace(",", "")
                price = float(ps)
            except (ValueError, TypeError):
                continue
            if price < 50000:  # Minimum IDR for a flight
                continue

            times = re.findall(r'(\d{1,2}:\d{2})', card)
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

            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0
            fn_m = re.search(r'\b(QG\s*\d+)\b', card)
            flight_no = fn_m.group(1).replace(" ", "") if fn_m else ""

            _qg_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            segment = FlightSegment(
                airline="QG",
                airline_name="Citilink",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur,
                cabin_class=_qg_cabin,
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=dur, stopovers=0)
            fid = hashlib.md5(f"qg_{flight_no}_{price}_{req.date_from}".encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"qg_{fid}",
                price=round(price, 2),
                currency="IDR",
                price_formatted=f"IDR {price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=["Citilink"],
                owner_airline="QG",
                booking_url=_BOOK_URL,
                is_locked=False,
                source="citilink_direct",
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
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"qg_rt_{o.id}_{i.id}",
                    price=round(o.price + i.price, 2),
                    currency=o.currency,
                    outbound=o.outbound,
                    inbound=i.outbound,
                    owner_airline=o.owner_airline,
                    airlines=list(set(o.airlines + i.airlines)),
                    source=o.source,
                    booking_url=o.booking_url,
                    conditions=o.conditions,
                ))
        combos.sort(key=lambda x: x.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"citilink{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="IDR",
            offers=[],
            total_results=0,
        )
