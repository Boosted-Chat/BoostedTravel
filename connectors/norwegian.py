"""
Norwegian Air hybrid connector — direct Amadeus DES API first, Playwright fallback.

Norwegian's booking engine (booking.norwegian.com) is an Angular 18 SPA that
calls api-des.norwegian.com (Amadeus Digital Experience Suite).

Hybrid strategy (Mar 2026):
1. Try direct HTTP via curl_cffi (Chrome TLS fingerprint):
   a. Token: POST api-des.norwegian.com/v1/security/oauth2/token/initialization
   b. Search: POST api-des.norwegian.com/airlines/DY/v2/search/air-bounds
   - Token is cached and reused until close to expiry
   - ~1–2s total (token + search), no browser needed
2. If direct API fails → fall back to full Playwright browser flow
   - Navigate to norwegian.com/en/ homepage, fill the search form, submit
   - Intercept the Amadeus DES air-bounds API response

API details (discovered Mar 2026):
  Token: POST api-des.norwegian.com/v1/security/oauth2/token/initialization
  Search: POST api-des.norwegian.com/airlines/DY/v2/search/air-bounds
  Payload: {commercialFareFamilies, itineraries, travelers, searchPreferences}
  Response: {data: {airBoundGroups: [{boundDetails, airBounds: [{prices, ...}]}]}}
  Prices are in CENTS (divide by 100)
  flightId format: SEG-DY1303-LGWOSL-2026-04-15-0920
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
import time
from datetime import datetime, timedelta
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# ── Direct API constants ─────────────────────────────────────────────────
_TOKEN_URL = "https://api-des.norwegian.com/v1/security/oauth2/token/initialization"
_SEARCH_URL = "https://api-des.norwegian.com/airlines/DY/v2/search/air-bounds"
_TOKEN_MARGIN = 60  # refresh token 60s before expiry

# Cached token state (guarded by _dy_token_lock)
_dy_token: str | None = None
_dy_token_expiry: float = 0.0  # monotonic timestamp
_dy_token_lock: Optional[asyncio.Lock] = None


def _get_dy_token_lock() -> asyncio.Lock:
    global _dy_token_lock
    if _dy_token_lock is None:
        _dy_token_lock = asyncio.Lock()
    return _dy_token_lock

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE"]
_TIMEZONES = ["Europe/London", "Europe/Berlin", "Europe/Oslo", "Europe/Paris"]

_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Shared headed Chromium (launched once, reused across searches)."""
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        _pw_instance = await async_playwright().start()
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
        logger.info("Norwegian: Playwright browser launched (headed Chrome)")
        return _browser


async def _ensure_dy_token() -> str | None:
    """Get a valid Bearer token for Amadeus DES, refreshing if needed."""
    global _dy_token, _dy_token_expiry
    lock = _get_dy_token_lock()
    async with lock:
        # Re-check inside the lock in case another coroutine already refreshed
        if _dy_token and time.monotonic() < _dy_token_expiry:
            return _dy_token
        try:
            from curl_cffi import requests as cffi_requests

            ses = cffi_requests.Session(impersonate="chrome131")
            r = ses.post(
                _TOKEN_URL,
                json={},
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Origin": "https://www.norwegian.com",
                    "Referer": "https://www.norwegian.com/",
                },
                timeout=10,
            )
            if r.status_code != 200:
                logger.warning("Norwegian token init failed: HTTP %s", r.status_code)
                return None
            data = r.json()
            tok = (
                data.get("accessToken")
                or data.get("access_token")
                or data.get("token")
            )
            # Fallback: look for a JWT-shaped value (three base64url segments)
            if not tok:
                for v in data.values():
                    if isinstance(v, str) and v.count(".") == 2 and len(v) > 50:
                        tok = v
                        break
            if tok:
                ttl = data.get("expiresIn", data.get("expires_in", 1799))
                _dy_token = tok
                _dy_token_expiry = time.monotonic() + int(ttl) - _TOKEN_MARGIN
                logger.info("Norwegian: token acquired (TTL=%ss)", ttl)
                return tok
            logger.warning("Norwegian: no token found in init response")
            return None
        except Exception as e:
            logger.warning("Norwegian: token init error: %s", e)
            return None


class NorwegianConnectorClient:
    """Norwegian hybrid connector — direct Amadeus DES API first, Playwright fallback."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Try direct API first (no browser)
        try:
            result = await self._search_via_api(req)
            if result and result.total_results > 0:
                return result
            logger.info("Norwegian: direct API returned 0 results, falling back to Playwright")
        except Exception as e:
            logger.info("Norwegian: direct API failed (%s), falling back to Playwright", e)

        # Fallback: full Playwright browser flow
        return await self._search_via_playwright(req)

    # ── Direct API path (curl_cffi) ──────────────────────────────────────

    async def _search_via_api(self, req: FlightSearchRequest) -> FlightSearchResponse | None:
        """Try direct HTTP to Amadeus DES air-bounds API using curl_cffi."""
        global _dy_token, _dy_token_expiry
        t0 = time.monotonic()
        token = await _ensure_dy_token()
        if not token:
            return None

        try:
            from curl_cffi import requests as cffi_requests

            # Build traveler list
            travelers = []
            traveler_id = 1
            for _ in range(req.adults):
                travelers.append({"id": str(traveler_id), "type": "ADULT"})
                traveler_id += 1
            for _ in range(req.children or 0):
                travelers.append({"id": str(traveler_id), "type": "CHILD"})
                traveler_id += 1
            for _ in range(req.infants or 0):
                travelers.append({"id": str(traveler_id), "type": "HELD_INFANT"})
                traveler_id += 1

            payload = {
                "commercialFareFamilies": ["LOWFARE", "LOWPLUS", "FLEX"],
                "itineraries": [
                    {
                        "originLocationCode": req.origin,
                        "destinationLocationCode": req.destination,
                        "departureDateTime": req.date_from.strftime("%Y-%m-%dT00:00:00.000"),
                    }
                ],
                "travelers": travelers,
                "searchPreferences": {
                    "cabinPreferences": [
                        {"cabin": "ECONOMY", "preferenceLevel": "PREFERRED"}
                    ]
                },
            }

            ses = cffi_requests.Session(impersonate="chrome131")
            r = ses.post(
                _SEARCH_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Origin": "https://www.norwegian.com",
                    "Referer": "https://booking.norwegian.com/",
                },
                timeout=20,
            )

            if r.status_code == 401:
                # Token may have expired mid-session; clear cache and bail
                lock = _get_dy_token_lock()
                async with lock:
                    _dy_token = None
                    _dy_token_expiry = 0.0
                logger.warning("Norwegian: 401 from air-bounds, token cleared")
                return None

            if r.status_code != 200:
                logger.warning("Norwegian: air-bounds HTTP %s", r.status_code)
                return None

            data = r.json()
            elapsed = time.monotonic() - t0
            offers = self._parse_air_bounds(data, req)
            offers.sort(key=lambda o: o.price)
            for o in offers:
                o.source = "norwegian_api"

            logger.info(
                "Norwegian %s→%s: %d offers in %.1fs (direct API)",
                req.origin, req.destination, len(offers), elapsed,
            )
            return self._build_response(offers, req)

        except Exception as e:
            logger.warning("Norwegian: direct API error: %s", e)
            return None

    # ── Playwright fallback ──────────────────────────────────────────────

    async def _search_via_playwright(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Norwegian flights via Playwright.

        Strategy: Navigate to norwegian.com/en/ homepage, fill the search form,
        submit, and intercept the Amadeus DES air-bounds API response from the
        booking.norwegian.com Angular SPA.
        """
        t0 = time.monotonic()

        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            # Set up response interception for air-bounds API
            captured_data: dict = {}
            api_response_event = asyncio.Event()

            async def on_response(response):
                try:
                    if "air-bounds" in response.url and response.status == 200:
                        captured_data["json"] = await response.json()
                        api_response_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Load homepage
            logger.info("Norwegian: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.norwegian.com/en/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            # Allow page JS to initialize
            await asyncio.sleep(2)

            # Dismiss cookie banners (OneTrust + any overlays)
            await self._dismiss_cookies(page)

            # Step 2: Fill search form
            await self._fill_search_form(page, req)

            # Step 3: Dismiss cookies again (may reappear after interaction)
            await self._dismiss_cookies(page)

            # Step 4: Submit search and wait for API response
            await self._click_search(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_response_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Norwegian: timed out waiting for air-bounds response")
                return self._empty(req)

            data = captured_data.get("json", {})
            if not data:
                logger.warning("Norwegian: captured empty response")
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_air_bounds(data, req)
            offers.sort(key=lambda o: o.price)

            logger.info(
                "Norwegian %s→%s returned %d offers in %.1fs (Playwright)",
                req.origin, req.destination, len(offers), elapsed,
            )

            return self._build_response(offers, req)

        except Exception as e:
            logger.error("Norwegian Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Form interaction (selectors verified Mar 2026)
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        """Remove OneTrust cookie banner via JS (avoids click-interception)."""
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
        offers: list[FlightOffer] = []
        groups = data.get("data", {}).get("airBoundGroups", [])
        booking_url = self._build_booking_url(req)

        for group in groups:
            bound_details = group.get("boundDetails", {})
            segments_raw = bound_details.get("segments", [])
            duration = bound_details.get("duration", 0)  # seconds

            # Parse segments from flightIds
            segments = self._parse_segments(segments_raw)
            if not segments:
                continue

            # Fix arrival times using bound duration
            self._fix_arrival_times(segments, duration)

            stopovers = max(len(segments) - 1, 0)

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=max(duration, 0),
                stopovers=stopovers,
            )

            # Get cheapest fare from airBounds (LOWFARE < LOWPLUS < FLEX)
            for air_bound in group.get("airBounds", []):
                fare_family = air_bound.get("fareFamilyCode", "")
                if fare_family != "LOWFARE":
                    continue  # Only take cheapest fare family

                total_prices = air_bound.get("prices", {}).get("totalPrices", [])
                if not total_prices:
                    continue

                price_obj = total_prices[0]
                total_cents = price_obj.get("total", 0)
                currency = price_obj.get("currencyCode", "EUR")
                price = total_cents / 100.0  # Prices are in cents

                if price <= 0:
                    continue

                flight_ids = "_".join(s.get("flightId", "") for s in segments_raw)
                key = f"{flight_ids}_{fare_family}_{total_cents}"

                offer = FlightOffer(
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
                    source="norwegian_direct",
                    source_tier="free",
                )
                offers.append(offer)
                break  # Only one offer per group (cheapest)

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

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Norwegian",
                flight_no=f"{carrier}{number}",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=dep_dt,  # Placeholder — fixed by _fix_arrival_times
                cabin_class="M",
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
        return (
            f"https://www.norwegian.com/en/"
            f"?D_City={req.origin}&A_City={req.destination}"
            f"&TripType=1&D_Day={date_str}"
            f"&AdultCount={req.adults}"
            f"&ChildCount={req.children or 0}"
            f"&InfantCount={req.infants or 0}"
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest,
    ) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"norwegian{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
            offers=offers,
            total_results=len(offers),
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return self._build_response([], req)
