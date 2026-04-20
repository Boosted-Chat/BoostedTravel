"""
Norwegian Air hybrid scraper — cookie-farm + curl_cffi direct API.

Norwegian's booking engine (booking.norwegian.com) is an Angular 18 SPA that
calls api-des.norwegian.com (Amadeus Digital Experience Suite). The search API
is behind Incapsula, which blocks raw HTTP clients. The token API is NOT
behind Incapsula.

Strategy (hybrid cookie-farm):
1. ONCE per ~25 min: Playwright opens homepage, fills search form, submits.
   This generates valid Incapsula cookies (reese84, visid_incap, etc.).
   Extract all cookies via context.cookies().
2. For each search: curl_cffi uses farmed cookies to:
   a) POST token/initialization → get Bearer access_token (~0.4s)
   b) POST airlines/DY/v2/search/air-bounds → get flight data (~0.6s)
3. Parse airBoundGroups → FlightOffers

Result: ~1s per search instead of ~45s with full Playwright.

API details (discovered Mar 2026):
  Token: POST api-des.norwegian.com/v1/security/oauth2/token/initialization
    Body: client_id, client_secret, grant_type=client_credentials, fact (JSON)
  Search: POST api-des.norwegian.com/airlines/DY/v2/search/air-bounds
    Body: {commercialFareFamilies, itineraries, travelers, searchPreferences}
  Response: {data: {airBoundGroups: [{boundDetails, airBounds: [{prices, ...}]}]}}
  Prices are in CENTS (divide by 100)
  flightId format: SEG-DY1303-LGWOSL-2026-04-15-0920
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from typing import Optional

from curl_cffi import requests as cffi_requests

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .airline_routes import get_city_airports
from .browser import stealth_popen_kwargs, find_chrome, _launched_procs, get_curl_cffi_proxies, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE"]
_TIMEZONES = ["Europe/London", "Europe/Berlin", "Europe/Oslo", "Europe/Paris"]

_CLIENT_ID = "YnF1uDBnJMWsGEmAndoGljO0DgkBeWaE"
_CLIENT_SECRET = "mrYaim0FdBrNRRZf"
_TOKEN_URL = "https://api-des.norwegian.com/v1/security/oauth2/token/initialization"
_SEARCH_URL = "https://api-des.norwegian.com/airlines/DY/v2/search/air-bounds"
_IMPERSONATE = "chrome131"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
_COOKIE_MAX_AGE = 25 * 60  # Re-farm cookies after 25 minutes
_DEBUG_PORT = 9460
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".norwegian_chrome_profile"
)

# Shared cookie farm state
_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0.0
_pw_instance = None
_browser = None
_chrome_proc = None


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Launch real headed Chrome via CDP for cookie farming (Incapsula blocks headless)."""
    global _pw_instance, _browser, _chrome_proc
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
        logger.info("Norwegian: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch Chrome HEADED (no --headless) — Incapsula blocks headless Chrome.
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
        "about:blank",
    ]
    _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
    _launched_procs.append(_chrome_proc)
    await asyncio.sleep(2.0)

    pw = await async_playwright().start()
    _pw_instance = pw
    _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
    logger.info("Norwegian: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
    return _browser


async def _reset_chrome_profile():
    """Kill Chrome and wipe the profile so next farm gets a fresh start."""
    global _browser, _pw_instance, _chrome_proc, _farmed_cookies, _farm_timestamp
    logger.info("Norwegian: resetting Chrome profile for fresh Incapsula farm")
    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None
    if _pw_instance:
        try:
            await _pw_instance.stop()
        except Exception:
            pass
        _pw_instance = None
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
            _chrome_proc.wait(timeout=5)
        except Exception:
            try:
                _chrome_proc.kill()
            except Exception:
                pass
        _chrome_proc = None
    _farmed_cookies = []
    _farm_timestamp = 0.0
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("Norwegian: deleted stale Chrome profile %s", _USER_DATA_DIR)
        except Exception as e:
            logger.warning("Norwegian: failed to delete Chrome profile: %s", e)


class NorwegianConnectorClient:
    """Norwegian hybrid scraper — cookie-farm + curl_cffi direct API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

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
        """
        Search Norwegian flights via cookie-farm + curl_cffi direct API.

        Norwegian requires airport codes (LGW, LTN), not city codes (LON).
        Expands city codes and merges results.
        """
        origins = get_city_airports(req.origin)
        destinations = get_city_airports(req.destination)

        if len(origins) > 1 or len(destinations) > 1:
            all_offers: list[FlightOffer] = []
            for o in origins:
                for d in destinations:
                    if o == d:
                        continue
                    sub_req = FlightSearchRequest(
                        origin=o,
                        destination=d,
                        date_from=req.date_from,
                        return_from=req.return_from,
                        adults=req.adults,
                        children=req.children,
                        infants=req.infants,
                        cabin_class=req.cabin_class,
                        currency=req.currency,
                        max_stopovers=req.max_stopovers,
                    )
                    try:
                        resp = await self._search_single(sub_req)
                        all_offers.extend(resp.offers)
                    except Exception:
                        pass
            all_offers.sort(key=lambda o: o.price)
            search_hash = hashlib.md5(
                f"norwegian{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=all_offers[0].currency if all_offers else req.currency,
                offers=all_offers,
                total_results=len(all_offers),
            )
        return await self._search_single(req)

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search a single origin→destination pair (airport-level codes).

        Fast path (~1s): curl_cffi with farmed Incapsula cookies.
        Slow path (~18s): Playwright farms cookies first, then curl_cffi.
        """
        t0 = time.monotonic()

        try:
            cookies = await self._ensure_cookies(req)
            if not cookies:
                logger.warning("Norwegian: cookie farm failed, no cookies")
                return self._empty(req)

            data = await self._api_search(req, cookies)

            # If search failed (expired cookies), re-farm once and retry
            if data is None:
                logger.warning("Norwegian: API search failed, re-farming cookies")
                cookies = await self._farm_cookies(req)
                if cookies:
                    data = await self._api_search(req, cookies)

            # If still failing, reset the Chrome profile (Incapsula may have
            # flagged the browser fingerprint) and try from scratch
            if data is None:
                logger.warning("Norwegian: persistent failure, resetting Chrome profile")
                await _reset_chrome_profile()
                cookies = await self._farm_cookies(req)
                if cookies:
                    data = await self._api_search(req, cookies)

            if not data:
                logger.warning("Norwegian: no data after search")
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_air_bounds(data, req)
            offers.sort(key=lambda o: o.price)

            logger.info(
                "Norwegian %s→%s returned %d offers in %.1fs (hybrid API)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"norwegian{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else req.currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Norwegian hybrid error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------
    # Cookie farm — Playwright generates Incapsula cookies
    # ------------------------------------------------------------------

    async def _ensure_cookies(self, req: FlightSearchRequest) -> list[dict]:
        """Return valid farmed cookies, farming new ones if needed."""
        global _farmed_cookies, _farm_timestamp
        lock = _get_farm_lock()
        async with lock:
            age = time.monotonic() - _farm_timestamp
            if _farmed_cookies and age < _COOKIE_MAX_AGE:
                return _farmed_cookies
            return await self._farm_cookies(req)

    async def _farm_cookies(self, req: FlightSearchRequest) -> list[dict]:
        """Visit booking.norwegian.com to get valid Incapsula cookies."""
        global _farmed_cookies, _farm_timestamp

        browser = await _get_browser()
        context = browser.contexts[0] if browser.contexts else await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
        )

        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)

            logger.info("Norwegian: farming Incapsula cookies from booking.norwegian.com")
            await page.goto(
                "https://booking.norwegian.com/booking/",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            # Incapsula JS challenge needs ~4-5s to generate reese84 + visid cookies
            await asyncio.sleep(5)

            cookies = await context.cookies()
            if cookies:
                _farmed_cookies = cookies
                _farm_timestamp = time.monotonic()
                incap = [c for c in cookies if "incap" in c["name"].lower() or "reese84" in c["name"].lower()]
                logger.info("Norwegian: farmed %d cookies (%d Incapsula)", len(cookies), len(incap))
            return cookies

        except Exception as e:
            logger.error("Norwegian: cookie farm error: %s", e)
            return []
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Direct API via curl_cffi
    # ------------------------------------------------------------------

    async def _api_search(
        self, req: FlightSearchRequest, cookies: list[dict]
    ) -> Optional[dict]:
        """Get token + search via curl_cffi with farmed cookies."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._api_search_sync, req, cookies)

    def _api_search_sync(
        self, req: FlightSearchRequest, cookies: list[dict]
    ) -> Optional[dict]:
        """Synchronous curl_cffi token + search."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE, proxies=get_curl_cffi_proxies())

        # Load farmed cookies into session
        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        # Step 1: Get OAuth2 token
        date_str = req.date_from.strftime("%Y-%m-%dT00:00:00")
        fact = json.dumps({
            "keyValuePairs": [
                {"key": "originLocationCode1", "value": req.origin},
                {"key": "destinationLocationCode1", "value": req.destination},
                {"key": "departureDateTime1", "value": date_str},
                {"key": "market", "value": "EN"},
                {"key": "channel", "value": "B2C"},
            ]
        })

        try:
            r_token = sess.post(
                _TOKEN_URL,
                data={
                    "client_id": _CLIENT_ID,
                    "client_secret": _CLIENT_SECRET,
                    "grant_type": "client_credentials",
                    "fact": fact,
                },
                headers={
                    "User-Agent": _UA,
                    "Origin": "https://booking.norwegian.com",
                    "Referer": "https://booking.norwegian.com/",
                },
                timeout=15,
            )
        except Exception as e:
            logger.error("Norwegian: token request failed: %s", e)
            return None

        if r_token.status_code != 200:
            logger.warning("Norwegian: token returned %d", r_token.status_code)
            return None

        access_token = r_token.json().get("access_token")
        if not access_token:
            logger.warning("Norwegian: no access_token in response")
            return None

        # Step 2: Search flights
        itineraries = [{
            "originLocationCode": req.origin,
            "destinationLocationCode": req.destination,
            "departureDateTime": f"{date_str}.000",
            "directFlights": False,
            "originLocationType": "airport",
            "destinationLocationType": "airport",
            "isRequestedBound": True,
        }]
        if req.return_from:
            ret_str = req.return_from.strftime("%Y-%m-%dT00:00:00")
            itineraries.append({
                "originLocationCode": req.destination,
                "destinationLocationCode": req.origin,
                "departureDateTime": f"{ret_str}.000",
                "directFlights": False,
                "originLocationType": "airport",
                "destinationLocationType": "airport",
                "isRequestedBound": True,
            })
        search_body = {
            "commercialFareFamilies": ["DYSTD"],
            "itineraries": itineraries,
            "travelers": self._build_travelers(req),
            "searchPreferences": {"showSoldOut": True, "showMilesPrice": False},
        }

        try:
            r_search = sess.post(
                _SEARCH_URL,
                json=search_body,
                headers={
                    "User-Agent": _UA,
                    "Authorization": f"Bearer {access_token}",
                    "Origin": "https://booking.norwegian.com",
                    "Referer": "https://booking.norwegian.com/",
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                },
                timeout=30,
            )
        except Exception as e:
            logger.error("Norwegian: search request failed: %s", e)
            return None

        if r_search.status_code != 200:
            logger.warning("Norwegian: search returned %d", r_search.status_code)
            return None

        return r_search.json()

    @staticmethod
    def _build_travelers(req: FlightSearchRequest) -> list[dict]:
        travelers = []
        for _ in range(req.adults):
            travelers.append({"passengerTypeCode": "ADT"})
        for _ in range(req.children or 0):
            travelers.append({"passengerTypeCode": "CHD"})
        for _ in range(req.infants or 0):
            travelers.append({"passengerTypeCode": "INF"})
        return travelers or [{"passengerTypeCode": "ADT"}]

    # ------------------------------------------------------------------
    # Form interaction for cookie farming (selectors verified Mar 2026)
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        """Remove OneTrust cookie banner — click accept first, then JS cleanup."""
        try:
            for label in ["Accept All Cookies", "Accept all", "Accept"]:
                btn = page.get_by_role("button", name=label)
                if await btn.count() > 0:
                    await btn.first.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    break
        except Exception:
            pass
        try:
            await page.evaluate("""() => {
                const ot = document.getElementById('onetrust-consent-sdk');
                if (ot) ot.remove();
                document.querySelectorAll('[class*="cookie"], [id*="cookie"], [class*="consent"]')
                    .forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            }""")
        except Exception:
            pass

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> None:
        """Fill the Norwegian homepage search form (one-way, airports, date)."""
        # Wait for the search form to be interactive
        try:
            await page.get_by_role("combobox", name="From").wait_for(
                state="visible", timeout=10000
            )
        except Exception:
            logger.debug("Norwegian: From combobox not found, trying anyway")

        # Select one-way — click the text label (radio input is covered by label)
        try:
            await page.get_by_text("One-way").click(timeout=3000)
            await asyncio.sleep(0.3)
        except Exception:
            logger.debug("Norwegian: could not click One-way")

        # Fill 'From' airport
        await self._fill_airport_field(page, "From", req.origin)
        await asyncio.sleep(0.5)

        # Fill 'To' airport
        await self._fill_airport_field(page, "To", req.destination)
        await asyncio.sleep(0.5)

        # Fill departure date via calendar picker
        await self._fill_date(page, req)

    async def _fill_airport_field(self, page, label: str, iata: str) -> None:
        """Fill an airport combobox and pick the matching option.

        The Norwegian form exposes ``combobox "From"`` / ``combobox "To"``.
        Typing the IATA code filters the listbox; each option renders as
        ``button "CityName (IATA) Country"`` inside the listbox.
        """
        try:
            combo = page.get_by_role("combobox", name=label)
            await combo.click(timeout=3000)
            await asyncio.sleep(0.3)
            await combo.fill(iata)
            await asyncio.sleep(1.5)

            # Click the first option button whose name contains "(IATA)"
            option_btn = page.get_by_role("button", name=re.compile(
                rf"\({re.escape(iata)}\)", re.IGNORECASE
            )).first
            await option_btn.click(timeout=5000)
        except Exception as e:
            logger.debug("Norwegian: %s field error: %s", label, e)

    async def _fill_date(self, page, req: FlightSearchRequest) -> None:
        """Open the calendar picker, navigate to the correct month, click the day."""
        target_year = req.date_from.year
        target_month = req.date_from.month
        target_day = req.date_from.day

        try:
            # Click the "Outbound flight" textbox to open the calendar
            date_box = page.get_by_role("textbox", name="Outbound flight")
            await date_box.click(timeout=3000)
            await asyncio.sleep(0.5)

            # Navigate months using the <select> inside the datepicker.
            # Option values follow the pattern "YYYY-MM-01Txx:xx:xx.xxxZ".
            target_prefix = f"{target_year}-{target_month:02d}-01T"
            changed = await page.evaluate(f"""() => {{
                const sel = document.querySelector('.nas-datepicker select');
                if (!sel) return 'no select';
                for (const opt of sel.options) {{
                    if (opt.value.startsWith('{target_prefix}')) {{
                        sel.value = opt.value;
                        sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                        return 'ok';
                    }}
                }}
                return 'month not found';
            }}""")
            if changed != "ok":
                logger.debug("Norwegian: month select result: %s", changed)
            await asyncio.sleep(0.5)

            # Click the day button inside the calendar table
            # The calendar renders buttons with just the day number as name.
            # Use a narrow locator: table cell button with exact day text.
            day_btn = page.locator(
                f".nas-datepicker table button"
            ).filter(has_text=re.compile(rf"^{target_day}$")).first
            await day_btn.click(timeout=3000)
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug("Norwegian: Date error: %s", e)

    async def _click_search(self, page) -> None:
        """Click 'Search and book' (enabled only after form is filled)."""
        try:
            btn = page.get_by_role("button", name="Search and book")
            await btn.click(timeout=5000)
        except Exception:
            # Fallback: try any submit button
            try:
                await page.locator("button[type='submit']").first.click(timeout=3000)
            except Exception:
                await page.keyboard.press("Enter")

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_air_bounds(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Amadeus DES air-bounds response into FlightOffers."""
        groups = data.get("data", {}).get("airBoundGroups", [])
        booking_url = self._build_booking_url(req)
        is_rt = bool(req.return_from)

        # Classify each group as outbound or inbound by first segment origin
        outbound_groups: list[tuple[FlightRoute, float, str]] = []
        inbound_groups: list[tuple[FlightRoute, float, str]] = []

        for group in groups:
            bound_details = group.get("boundDetails", {})
            segments_raw = bound_details.get("segments", [])
            duration = bound_details.get("duration", 0)

            segments = self._parse_segments(segments_raw)
            if not segments:
                continue

            self._fix_arrival_times(segments, duration)

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=max(duration, 0),
                stopovers=max(len(segments) - 1, 0),
            )

            # Get cheapest fare (LOWFARE)
            for air_bound in group.get("airBounds", []):
                if air_bound.get("fareFamilyCode", "") != "LOWFARE":
                    continue
                total_prices = air_bound.get("prices", {}).get("totalPrices", [])
                if not total_prices:
                    continue
                price_obj = total_prices[0]
                total_cents = price_obj.get("total", 0)
                currency = price_obj.get("currencyCode", "EUR")
                price = total_cents / 100.0
                if price <= 0:
                    continue

                # Classify direction by first segment origin
                first_origin = segments[0].origin if segments else ""
                if is_rt and first_origin == req.destination:
                    inbound_groups.append((route, price, currency))
                else:
                    outbound_groups.append((route, price, currency))
                break

        # Build offers
        offers: list[FlightOffer] = []

        if is_rt and outbound_groups and inbound_groups:
            # Pair outbound × cheapest inbound, and vice versa
            inbound_groups.sort(key=lambda x: x[1])
            for ob_route, ob_price, ob_cur in outbound_groups:
                ib_route, ib_price, ib_cur = inbound_groups[0]
                total = round(ob_price + ib_price, 2)
                currency = ob_cur
                key = f"{ob_route.segments[0].flight_no}_{ib_route.segments[0].flight_no}_{total}"
                offers.append(FlightOffer(
                    id=f"dy_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                    price=total,
                    currency=currency,
                    price_formatted=f"{total:.2f} {currency}",
                    outbound=ob_route,
                    inbound=ib_route,
                    airlines=["Norwegian"],
                    owner_airline="DY",
                    booking_url=booking_url,
                    is_locked=False,
                    source="norwegian_api",
                    source_tier="free",
                ))
            # Also emit one-way outbound offers for combo engine
            for ob_route, ob_price, ob_cur in outbound_groups:
                key = f"ow_{ob_route.segments[0].flight_no}_{ob_price}"
                offers.append(FlightOffer(
                    id=f"dy_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                    price=round(ob_price, 2),
                    currency=ob_cur,
                    price_formatted=f"{ob_price:.2f} {ob_cur}",
                    outbound=ob_route,
                    inbound=None,
                    airlines=["Norwegian"],
                    owner_airline="DY",
                    booking_url=booking_url,
                    is_locked=False,
                    source="norwegian_api",
                    source_tier="free",
                ))
        else:
            # One-way search or no inbound results
            for route, price, currency in outbound_groups:
                key = f"{route.segments[0].flight_no}_{price}"
                offers.append(FlightOffer(
                    id=f"dy_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                    price=round(price, 2),
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=["Norwegian"],
                    owner_airline="DY",
                    booking_url=booking_url,
                    is_locked=False,
                    source="norwegian_api",
                    source_tier="free",
                ))

        return offers

    def _parse_segments(self, segments_raw: list) -> list[FlightSegment]:
        """Parse segments from flightId strings.

        flightId format: SEG-DY1303-LGWOSL-2026-04-15-0920
        → carrier=DY, number=1303, origin=LGW, dest=OSL, date=2026-04-15, time=09:20
        """
        segments: list[FlightSegment] = []

        for seg_info in segments_raw:
            flight_id = seg_info.get("flightId", "")
            match = re.match(
                r"SEG-([A-Z0-9]{2})(\d+)-([A-Z]{3})([A-Z]{3})-(\d{4}-\d{2}-\d{2})-(\d{4})",
                flight_id,
            )
            if not match:
                logger.debug("Norwegian: could not parse flightId: %s", flight_id)
                continue

            carrier = match.group(1)
            number = match.group(2)
            origin = match.group(3)
            dest = match.group(4)
            date_str = match.group(5)
            time_str = match.group(6)

            dep_dt = datetime.strptime(
                f"{date_str} {time_str[:2]}:{time_str[2:]}", "%Y-%m-%d %H:%M"
            )

            _dy_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Norwegian",
                flight_no=f"{carrier}{number}",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=dep_dt,  # Placeholder — fixed by _fix_arrival_times
                cabin_class=_dy_cabin,
            ))

        return segments

    def _fix_arrival_times(self, segments: list[FlightSegment], duration_seconds: int) -> None:
        """Fix placeholder arrival times using total bound duration."""
        if len(segments) == 1 and duration_seconds > 0:
            segments[0] = FlightSegment(
                airline=segments[0].airline,
                airline_name=segments[0].airline_name,
                flight_no=segments[0].flight_no,
                origin=segments[0].origin,
                destination=segments[0].destination,
                departure=segments[0].departure,
                arrival=segments[0].departure + timedelta(seconds=duration_seconds),
                cabin_class=segments[0].cabin_class,
            )
        elif len(segments) > 1 and duration_seconds > 0:
            # For multi-segment: set last segment's arrival from total duration
            segments[-1] = FlightSegment(
                airline=segments[-1].airline,
                airline_name=segments[-1].airline_name,
                flight_no=segments[-1].flight_no,
                origin=segments[-1].origin,
                destination=segments[-1].destination,
                departure=segments[-1].departure,
                arrival=segments[0].departure + timedelta(seconds=duration_seconds),
                cabin_class=segments[-1].cabin_class,
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_str = req.date_from.strftime("%d/%m/%Y")
        is_rt = bool(req.return_from)
        trip = "2" if is_rt else "1"
        url = (
            f"https://www.norwegian.com/en/"
            f"?D_City={req.origin}&A_City={req.destination}"
            f"&TripType={trip}&D_Day={date_str}"
            f"&AdultCount={req.adults}"
            f"&ChildCount={req.children or 0}"
            f"&InfantCount={req.infants or 0}"
        )
        if is_rt:
            url += f"&R_Day={req.return_from.strftime('%d/%m/%Y')}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"norwegian{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


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
                    id=f"rt_norw_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
