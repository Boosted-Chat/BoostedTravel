"""
PLAY CDP Chrome scraper — navigates to PLAY homepage and searches flights.

The direct API is behind WAF — requires browser session.
Real Chrome via CDP passes WAF better than Playwright's bundled Chromium.

PLAY (OG) is an Icelandic LCC flying from Keflavík (KEF) to European and
North American destinations.

Strategy (converted Mar 2026):
1. Launch real system Chrome via CDP (persistent, passes WAF better)
2. Navigate to flyplay.com/en/ homepage
3. Fill search form (origin, destination, date, one-way)
4. Click search button
5. Wait for API response interception (flights/search or availability)
6. Parse results → FlightOffers
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
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US"]
_TIMEZONES = ["Atlantic/Reykjavik", "Europe/London", "Europe/Berlin", "Europe/Paris"]

_ICELAND_AIRPORTS = {"KEF", "RKV", "AEY", "EGS", "IFJ", "VPN", "HFN"}

_CDP_PORT = 9456
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "play_cdp_data")
_CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]

_chrome_proc: subprocess.Popen | None = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _find_chrome() -> str:
    for p in _CHROME_PATHS:
        if os.path.isfile(p):
            return p
    raise FileNotFoundError("Chrome not found")


def _launch_chrome():
    global _chrome_proc
    if _chrome_proc and _chrome_proc.poll() is None:
        return
    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    chrome = _find_chrome()
    _chrome_proc = subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--disable-blink-features=AutomationControlled",
            "--no-first-run", "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    logger.info("PLAY: Chrome launched on CDP port %d (pid=%d)", _CDP_PORT, _chrome_proc.pid)


async def _get_browser():
    """Shared real Chrome via CDP (launched once, reused across searches)."""
    global _pw_instance, _cdp_browser
    lock = _get_lock()
    async with lock:
        if _cdp_browser and _cdp_browser.is_connected():
            return _cdp_browser
        _launch_chrome()
        await asyncio.sleep(2)
        from playwright.async_api import async_playwright
        if not _pw_instance:
            _pw_instance = await async_playwright().start()
        for attempt in range(5):
            try:
                _cdp_browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{_CDP_PORT}"
                )
                logger.info("PLAY: connected to Chrome via CDP")
                return _cdp_browser
            except Exception:
                if attempt < 4:
                    await asyncio.sleep(1)
        raise RuntimeError(f"PLAY: cannot connect to Chrome CDP on port {_CDP_PORT}")


class PlayConnectorClient:
    """PLAY Playwright scraper — homepage form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            page = await context.new_page()

            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and (
                        "flights/search" in url
                        or "availability" in url
                        or "search/flights" in url
                        or ("api" in url and "offer" in url)
                        or "lowfare" in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, dict):
                                captured_data["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("PLAY: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.flyplay.com/en/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("PLAY: form fill failed")
                return self._empty(req)

            await self._click_search(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("PLAY: timed out waiting for API response")
                offers = await self._extract_from_dom(page, req)
                if offers:
                    return self._build_response(offers, req, time.monotonic() - t0)
                return self._empty(req)

            data = captured_data.get("json", {})
            if not data:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_response(data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("PLAY Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Cookie dismissal
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept all", "Accept All", "Accept", "Accept cookies",
            "Accept all cookies", "Got it", "OK", "I agree",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue

        try:
            accept = page.locator(
                "[class*='cookie'] button, [id*='cookie'] button, "
                "[class*='consent'] button, [id*='consent'] button"
            ).first
            if await accept.count() > 0:
                await accept.click(timeout=2000)
                await asyncio.sleep(0.5)
                return
        except Exception:
            pass

        try:
            await page.evaluate("""() => {
                const ids = ['onetrust-consent-sdk', 'CybotCookiebotDialog', 'cookiebanner'];
                ids.forEach(id => { const el = document.getElementById(id); if (el) el.remove(); });
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="modal-backdrop"], [class*="overlay"][style*="z-index"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Form filling
    # ------------------------------------------------------------------

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        # Set one-way
        await self._set_one_way(page)
        await asyncio.sleep(0.3)

        ok = await self._fill_airport_field(page, "From", req.origin)
        if not ok:
            ok = await self._fill_airport_field(page, "Origin", req.origin)
        if not ok:
            ok = await self._fill_airport_field(page, "Departure", req.origin)
        if not ok:
            ok = await self._fill_airport_field_fallback(page, 0, req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        ok = await self._fill_airport_field(page, "To", req.destination)
        if not ok:
            ok = await self._fill_airport_field(page, "Destination", req.destination)
        if not ok:
            ok = await self._fill_airport_field(page, "Arrival", req.destination)
        if not ok:
            ok = await self._fill_airport_field_fallback(page, 1, req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        ok = await self._fill_date(page, req)
        return ok

    async def _fill_airport_field(self, page, label: str, iata: str) -> bool:
        try:
            field = page.get_by_role("combobox", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE))
            if await field.count() == 0:
                field = page.get_by_role("textbox", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE))
            if await field.count() == 0:
                field = page.get_by_placeholder(re.compile(rf"{re.escape(label)}", re.IGNORECASE))
            if await field.count() == 0:
                return False

            await field.first.click(timeout=3000)
            await asyncio.sleep(0.3)
            await field.first.fill("")
            await asyncio.sleep(0.2)
            await field.first.fill(iata)
            await asyncio.sleep(1.5)

            for role in ["option", "button", "listitem", "link", "menuitem"]:
                option = page.get_by_role(
                    role, name=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)
                ).first
                try:
                    if await option.count() > 0:
                        await option.click(timeout=3000)
                        logger.info("PLAY: selected %s for %s", iata, label)
                        return True
                except Exception:
                    continue

            item = page.locator(
                "[class*='suggestion'], [class*='option'], [class*='result'], [class*='airport'], li"
            ).filter(has_text=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)).first
            if await item.count() > 0:
                await item.click(timeout=3000)
                return True

            await page.keyboard.press("Enter")
            return True
        except Exception as e:
            logger.debug("PLAY: %s field error: %s", label, e)
            return False

    async def _fill_airport_field_fallback(self, page, index: int, iata: str) -> bool:
        try:
            inputs = page.locator("input[type='text'], input[type='search']")
            if await inputs.count() > index:
                field = inputs.nth(index)
                await field.click(timeout=3000)
                await field.fill("")
                await asyncio.sleep(0.2)
                await field.fill(iata)
                await asyncio.sleep(1.5)
                await page.keyboard.press("Enter")
                return True
        except Exception:
            pass
        return False

    async def _set_one_way(self, page) -> None:
        for label in ["One way", "One-way", "one way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue

        try:
            ow = page.get_by_role("radio", name=re.compile(r"one.?way", re.IGNORECASE)).first
            if await ow.count() > 0:
                await ow.click(timeout=2000)
                return
        except Exception:
            pass
        try:
            ow = page.get_by_role("tab", name=re.compile(r"one.?way", re.IGNORECASE)).first
            if await ow.count() > 0:
                await ow.click(timeout=2000)
        except Exception:
            pass

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        try:
            for name in ["Departure", "Outbound", "Depart", "Date", "When"]:
                field = page.get_by_role(
                    "textbox", name=re.compile(rf"{name}", re.IGNORECASE)
                )
                if await field.count() > 0:
                    await field.first.click(timeout=3000)
                    break
            else:
                date_input = page.locator(
                    "[class*='date'], [data-testid*='date'], input[name*='date']"
                ).first
                await date_input.click(timeout=3000)
            await asyncio.sleep(0.8)

            target_month_year = target.strftime("%B %Y")
            for _ in range(12):
                for variant in [target_month_year, target_month_year.upper(), target_month_year.lower()]:
                    heading = page.locator(f"text={variant}").first
                    if await heading.count() > 0:
                        break
                else:
                    try:
                        fwd = page.get_by_role(
                            "button", name=re.compile(r"(next|forward|›|→|>)", re.IGNORECASE)
                        )
                        if await fwd.count() > 0:
                            await fwd.first.click(timeout=2000)
                            await asyncio.sleep(0.4)
                            continue
                    except Exception:
                        pass
                    try:
                        fwd = page.locator("[class*='next'], [aria-label*='next'], [aria-label*='Next']").first
                        await fwd.click(timeout=2000)
                        await asyncio.sleep(0.4)
                        continue
                    except Exception:
                        break
                break

            day = target.day
            date_iso = target.isoformat()
            try:
                day_btn = page.locator(f"[data-date='{date_iso}']").first
                if await day_btn.count() > 0:
                    await day_btn.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    return True
            except Exception:
                pass

            # Try "Month Day, Year" format (US style)
            day_label = f"{target.strftime('%B')} {day}, {target.year}"
            try:
                day_btn = page.get_by_role("button", name=day_label)
                if await day_btn.count() > 0:
                    await day_btn.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    return True
            except Exception:
                pass

            # Try "Day Month Year" format (EU style)
            day_label_eu = f"{day} {target.strftime('%B')} {target.year}"
            try:
                day_btn = page.locator(f"[aria-label*='{day_label_eu}']").first
                if await day_btn.count() > 0:
                    await day_btn.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    return True
            except Exception:
                pass

            day_btn = page.locator(
                "table button, .calendar button, [class*='calendar'] button, [class*='day']"
            ).filter(has_text=re.compile(rf"^{day}$")).first
            if await day_btn.count() > 0:
                await day_btn.click(timeout=3000)
                await asyncio.sleep(0.5)
                return True

            day_btn = page.get_by_role("button", name=re.compile(rf"^{day}$")).first
            await day_btn.click(timeout=3000)
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("PLAY: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        for label in ["Search", "search", "Search flights", "SEARCH", "Find flights"]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    logger.info("PLAY: clicked search")
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # ------------------------------------------------------------------
    # DOM fallback
    # ------------------------------------------------------------------

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                if (window.appData) return window.appData;
                if (window.__INITIAL_STATE__) return window.__INITIAL_STATE__;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.outbound || d.journeys || d.fares || d.offers)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        # Determine default currency (ISK for Iceland domestic, else req.currency)
        default_currency = req.currency or "EUR"
        if req.origin in _ICELAND_AIRPORTS and req.destination in _ICELAND_AIRPORTS:
            default_currency = "ISK"
        currency = data.get("currency", default_currency)
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        flights_raw = (
            data.get("outboundFlights") or data.get("outbound")
            or data.get("flights") or data.get("offers")
            or (data.get("journeys", {}).get("outbound") if isinstance(data.get("journeys"), dict) else None)
            or []
        )
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)

        # Try "availability" wrapper
        if not offers and "availability" in data:
            avail = data["availability"]
            if isinstance(avail, list):
                for flight in avail:
                    offer = self._parse_single_flight(flight, currency, req, booking_url)
                    if offer:
                        offers.append(offer)

        return offers

    def _parse_single_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        price = (
            flight.get("price") or flight.get("totalPrice") or flight.get("lowestPrice")
            or flight.get("farePrice") or self._extract_cheapest_fare(flight)
        )
        if price is None:
            pd = flight.get("priceDetails") or flight.get("pricing") or {}
            price = pd.get("totalPrice") or pd.get("price") or pd.get("amount")
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        cur = flight.get("currency") or currency

        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))
        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments, total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("flightKey") or flight.get("id") or flight.get("flightId")
            or flight.get("flightNumber", "") + "_" + segments[0].departure.isoformat()
        )
        return FlightOffer(
            id=f"og_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=cur,
            price_formatted=f"{price:.2f} {cur}",
            outbound=route, inbound=None,
            airlines=["PLAY"], owner_airline="OG",
            booking_url=booking_url, is_locked=False,
            source="play_direct", source_tier="free",
        )

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departure") or seg.get("departureDate") or seg.get("departureDateTime") or seg.get("departureTime") or seg.get("std") or ""
        arr_str = seg.get("arrival") or seg.get("arrivalDate") or seg.get("arrivalDateTime") or seg.get("arrivalTime") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("flightNo") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or seg.get("departureCode") or default_origin
        destination = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or seg.get("arrivalCode") or default_dest
        return FlightSegment(
            airline="OG", airline_name="PLAY", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    @staticmethod
    def _extract_cheapest_fare(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareBundles") or flight.get("fareOptions") or []
        prices: list[float] = []
        for f in fares:
            p = f.get("price") or f.get("amount") or f.get("totalPrice")
            if p is not None:
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    continue
        return min(prices) if prices else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("PLAY %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"play{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
            offers=offers, total_results=len(offers),
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

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.flyplay.com/en/flights"
            f"?from={req.origin}&to={req.destination}"
            f"&departure={dep}&adults={req.adults}"
            f"&children={req.children}&infants={req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"play{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
        )
