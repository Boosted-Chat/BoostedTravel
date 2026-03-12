"""
Akasa Air hybrid connector — direct Navitaire NSK API first, Playwright fallback.

Akasa Air (IATA: QP) is an Indian low-cost carrier (launched 2022).
Website: www.akasaair.com — React (Next.js) SPA booking engine.

Backend: Navitaire New Skies (NSK) via prod-bl.qp.akasaair.com.

Hybrid strategy (Mar 2026):
1. Try direct HTTP via curl_cffi (Chrome TLS fingerprint):
   a. Token: POST prod-bl.qp.akasaair.com/api/ibe/token/generateToken
   b. Search: POST prod-bl.qp.akasaair.com/api/ibe/availability/search
   - Token is cached and reused until close to expiry
   - ~1–2s total (token + search), no browser needed
2. If direct API fails → fall back to full Playwright browser flow
   - Navigate to akasaair.com homepage, fill form, intercept API response

API details (discovered Mar 2026):
  Token: POST prod-bl.qp.akasaair.com/api/ibe/token/generateToken
  Search: POST prod-bl.qp.akasaair.com/api/ibe/availability/search
  LowFare: POST prod-bl.qp.akasaair.com/api/ibe/availability/search/lowFare
  Response: {data: {results[0].trips[0].journeysAvailableByMarket[0].value: [journey, ...]}}
  Fare lookup: {data: {faresAvailable: [{key, value: {totals: {fareTotal}}}]}}
  Prices are in whole currency units (INR), fareTotal includes taxes+fees.

Form selectors (verified Mar 2026):
  One-way: input#oneway (value="ONE_WAY")
  Origin: input#From → type IATA → click li#IATA in ul#destinations dropdown
  Destination: input#To → same pattern
  Date: input[name='DepartureDate'] → react-datepicker calendar
  Search: button with text "Search"
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
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

# ── Direct API constants ─────────────────────────────────────────────────
_QP_BASE = "https://prod-bl.qp.akasaair.com"
_TOKEN_URL = f"{_QP_BASE}/api/ibe/token/generateToken"
_SEARCH_URL = f"{_QP_BASE}/api/ibe/availability/search"
_TOKEN_MARGIN = 60  # refresh token 60s before expiry

# Cached token state (guarded by _qp_token_lock)
_qp_token: str | None = None
_qp_token_expiry: float = 0.0  # monotonic timestamp
_qp_token_lock: Optional[asyncio.Lock] = None


def _get_qp_token_lock() -> asyncio.Lock:
    global _qp_token_lock
    if _qp_token_lock is None:
        _qp_token_lock = asyncio.Lock()
    return _qp_token_lock

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-IN", "en-US", "en-GB"]
_TIMEZONES = ["Asia/Kolkata", "Asia/Dubai", "Asia/Singapore"]

_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Shared headed Chromium (launched once, reused)."""
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
        logger.info("Akasa: Playwright browser launched (headed Chrome)")
        return _browser


async def _ensure_qp_token() -> str | None:
    """Get a valid Navitaire NSK token for Akasa, refreshing if needed."""
    global _qp_token, _qp_token_expiry
    lock = _get_qp_token_lock()
    async with lock:
        # Re-check inside the lock in case another coroutine already refreshed
        if _qp_token and time.monotonic() < _qp_token_expiry:
            return _qp_token
        try:
            from curl_cffi import requests as cffi_requests

            ses = cffi_requests.Session(impersonate="chrome131")
            r = ses.post(
                _TOKEN_URL,
                json={"LanguageCode": "en", "market": "en-IN"},
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Origin": "https://www.akasaair.com",
                    "Referer": "https://www.akasaair.com/",
                },
                timeout=10,
            )
            if r.status_code != 200:
                logger.warning("Akasa token init failed: HTTP %s", r.status_code)
                return None
            data = r.json()
            tok = (
                data.get("data", {}).get("token")
                or data.get("token")
                or data.get("accessToken")
                or data.get("access_token")
            )
            # Fallback: look for a JWT-shaped value (three base64url segments)
            if not tok:
                for v in data.values():
                    if isinstance(v, str) and v.count(".") == 2 and len(v) > 50:
                        tok = v
                        break
            if tok:
                ttl = (
                    data.get("data", {}).get("expiresIn")
                    or data.get("expiresIn")
                    or data.get("expires_in")
                    or 3600
                )
                _qp_token = tok
                _qp_token_expiry = time.monotonic() + int(ttl) - _TOKEN_MARGIN
                logger.info("Akasa: token acquired (TTL=%ss)", ttl)
                return tok
            logger.warning("Akasa: no token found in generateToken response")
            return None
        except Exception as e:
            logger.warning("Akasa: token init error: %s", e)
            return None


class AkasaConnectorClient:
    """Akasa Air hybrid connector — direct Navitaire NSK API first, Playwright fallback."""

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
            logger.info("Akasa: direct API returned 0 results, falling back to Playwright")
        except Exception as e:
            logger.info("Akasa: direct API failed (%s), falling back to Playwright", e)

        # Fallback: full Playwright browser flow
        return await self._search_via_playwright(req)

    # ── Direct API path (curl_cffi) ──────────────────────────────────────

    async def _search_via_api(self, req: FlightSearchRequest) -> FlightSearchResponse | None:
        """Try direct HTTP to Navitaire NSK availability/search API via curl_cffi."""
        global _qp_token, _qp_token_expiry
        t0 = time.monotonic()
        token = await _ensure_qp_token()
        if not token:
            return None

        try:
            from curl_cffi import requests as cffi_requests

            # Build pax info
            pax = {"ADT": req.adults, "CHD": req.children or 0, "INF": req.infants or 0}

            payload = {
                "model": {
                    "contentType": "json",
                    "fareFilters": {"type": "Cheapest"},
                    "loyaltyFilter": {"accountNumber": None, "programCode": None},
                    "paxInfo": pax,
                    "promotionCode": "",
                    "tripDetails": {
                        "beginDate": req.date_from.strftime("%Y-%m-%d"),
                        "endDate": req.date_from.strftime("%Y-%m-%d"),
                        "destStation": req.destination,
                        "originStation": req.origin,
                        "tripType": "OneWay",
                        "channel": "Web",
                    },
                }
            }

            ses = cffi_requests.Session(impersonate="chrome131")
            r = ses.post(
                _SEARCH_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Origin": "https://www.akasaair.com",
                    "Referer": "https://www.akasaair.com/",
                },
                timeout=20,
            )

            if r.status_code == 401:
                # Token expired; clear cache under lock
                lock = _get_qp_token_lock()
                async with lock:
                    _qp_token = None
                    _qp_token_expiry = 0.0
                logger.warning("Akasa: 401 from availability/search, token cleared")
                return None

            if r.status_code != 200:
                logger.warning("Akasa: availability/search HTTP %s", r.status_code)
                return None

            data = r.json()
            elapsed = time.monotonic() - t0
            offers = self._parse_navitaire_response(data, req)
            offers.sort(key=lambda o: o.price)
            for o in offers:
                o.source = "akasa_api"

            logger.info(
                "Akasa %s→%s: %d offers in %.1fs (direct API)",
                req.origin, req.destination, len(offers), elapsed,
            )
            return self._build_response(offers, req)

        except Exception as e:
            logger.warning("Akasa: direct API error: %s", e)
            return None

    # ── Playwright fallback ──────────────────────────────────────────────

    async def _search_via_playwright(self, req: FlightSearchRequest) -> FlightSearchResponse:
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

            # Set up response interception for availability/search API
            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url
                    # Match the main search endpoint (not lowFare or v2/search)
                    if (
                        response.status == 200
                        and "/api/ibe/availability/search" in url
                        and "/lowFare" not in url
                        and "/v2/" not in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, dict):
                                results = data.get("data", {}).get("results", [])
                                if results:
                                    captured_data["json"] = data
                                    api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Load homepage
            logger.info("Akasa: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.akasaair.com/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(4.0)

            # Dismiss cookie banner
            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)

            # Step 2: Fill search form
            await self._fill_search_form(page, req)

            # Dismiss cookies again (may reappear after interaction)
            await self._dismiss_cookies(page)

            # Step 3: Submit search and wait for API response
            await self._click_search(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Akasa: timed out waiting for availability/search response")
                return self._empty(req)

            data = captured_data.get("json", {})
            if not data:
                logger.warning("Akasa: captured empty response")
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_navitaire_response(data, req)
            offers.sort(key=lambda o: o.price)

            logger.info(
                "Akasa %s→%s returned %d offers in %.1fs (Playwright)",
                req.origin, req.destination, len(offers), elapsed,
            )

            return self._build_response(offers, req)

        except Exception as e:
            logger.error("Akasa Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Form interaction (selectors verified Mar 2026)
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        """Dismiss cookie banner and remove overlays via JS."""
        for sel in ["button:has-text('Accept')", "button:has-text('Got it')"]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.is_visible():
                    await btn.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> None:
        """Fill the Akasa homepage search form (one-way, airports, date)."""
        # Set one-way — click the #oneway radio input
        try:
            oneway = page.locator("input#oneway")
            if await oneway.count() > 0:
                await oneway.click(force=True, timeout=3000)
                await asyncio.sleep(0.3)
        except Exception:
            try:
                await page.locator("label[for='oneway']").click(timeout=3000)
            except Exception:
                logger.debug("Akasa: could not set one-way")

        await asyncio.sleep(0.5)

        # Fill origin airport
        await self._fill_airport_field(page, "From", req.origin)
        await asyncio.sleep(0.8)

        # Fill destination airport
        await self._fill_airport_field(page, "To", req.destination)
        await asyncio.sleep(0.8)

        # Fill departure date
        await self._fill_date(page, req)

    async def _fill_airport_field(self, page, field_id: str, iata: str) -> None:
        """Fill an airport field and pick the matching suggestion.

        Akasa form: input#From / input#To. Typing the IATA code opens a dropdown
        (ul#destinations). Each airport is rendered as li#IATA with the airport name.
        """
        try:
            field = page.locator(f"input#{field_id}")
            await field.click(timeout=3000)
            await asyncio.sleep(0.5)
            await field.fill("")
            await asyncio.sleep(0.2)

            # Type slowly to trigger suggestion dropdown
            for ch in iata:
                await field.type(ch, delay=80)
            await asyncio.sleep(2.0)

            # Click the suggestion — try li#IATA first (exact ID match)
            suggestion = page.locator(f"li#{iata}")
            if await suggestion.count() > 0:
                await suggestion.click(timeout=3000)
                return

            # Fallback: li with text matching the IATA code
            suggestion = page.locator("ul#destinations li").filter(
                has_text=re.compile(rf"\b{re.escape(iata)}\b", re.IGNORECASE)
            ).first
            if await suggestion.count() > 0:
                await suggestion.click(timeout=3000)
                return

            # Fallback: any visible li containing the IATA code
            suggestion = page.locator("li").filter(
                has_text=re.compile(rf"\b{re.escape(iata)}\b")
            ).first
            if await suggestion.count() > 0 and await suggestion.is_visible():
                await suggestion.click(timeout=3000)
                return

            logger.warning("Akasa: no suggestion found for %s in %s", iata, field_id)
        except Exception as e:
            logger.debug("Akasa: %s field error: %s", field_id, e)

    async def _fill_date(self, page, req: FlightSearchRequest) -> None:
        """Open the react-datepicker calendar, navigate to the target month,
        and click the target day."""
        target = req.date_from

        try:
            # Click the departure date input to open the calendar
            date_field = page.locator("input[name='DepartureDate']")
            if await date_field.count() > 0:
                await date_field.click(timeout=3000)
            else:
                # Fallback — the calendar may auto-open after destination fill
                date_field = page.locator(
                    "input[placeholder*='Departure'], input[placeholder*='date']"
                ).first
                if await date_field.count() > 0:
                    await date_field.click(timeout=3000)
            await asyncio.sleep(1.0)

            # Navigate to the correct month using the react-datepicker nav buttons
            target_month_year = target.strftime("%B %Y")  # e.g. "April 2026"
            for _ in range(12):
                # Check if target month is visible in the page
                content = await page.content()
                if target_month_year.lower() in content.lower():
                    break
                # Click the next-month navigation button
                nav = page.locator(
                    "button.react-datepicker__navigation--next, "
                    "[class*='react-datepicker'] [class*='navigation--next'], "
                    "button[aria-label*='Next']"
                ).first
                if await nav.count() > 0:
                    await nav.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    break

            # Click the target day — react-datepicker renders days as div.react-datepicker__day
            day_str = str(target.day)
            # Try aria-label first (format varies: "Choose <day> <Month> <Year>")
            for aria_fmt in [
                f"Choose {target.strftime('%A, %B')} {target.day}, {target.year}",
                f"{target.day} {target.strftime('%B')} {target.year}",
                f"{target.strftime('%B')} {target.day}",
            ]:
                day_btn = page.locator(f"[aria-label*='{aria_fmt}']").first
                if await day_btn.count() > 0:
                    await day_btn.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    return

            # Fallback: find day element by class and text
            day_btn = page.locator(
                "[class*='react-datepicker__day']:not([class*='outside-month'])"
            ).filter(has_text=re.compile(rf"^{target.day}$")).first
            if await day_btn.count() > 0:
                await day_btn.click(timeout=3000)
                await asyncio.sleep(0.5)
                return

            # Last resort: any button/div with the exact day number
            day_btn = page.locator(
                ".react-datepicker button, .react-datepicker div[role='option']"
            ).filter(has_text=re.compile(rf"^{day_str}$")).first
            if await day_btn.count() > 0:
                await day_btn.click(timeout=3000)
                return

            logger.warning("Akasa: could not click day %d", target.day)
        except Exception as e:
            logger.debug("Akasa: date error: %s", e)

    async def _click_search(self, page) -> None:
        """Click the search/submit button."""
        try:
            btn = page.locator("button:has-text('Search')").first
            if await btn.count() > 0:
                await btn.click(timeout=5000)
                logger.info("Akasa: clicked search button")
                return
        except Exception:
            pass
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # ------------------------------------------------------------------
    # Navitaire response parsing
    # ------------------------------------------------------------------

    def _parse_navitaire_response(
        self, data: dict, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse Navitaire NSK availability/search response into FlightOffers.

        Response structure:
          data.results[0].trips[0].journeysAvailableByMarket[0].value → [journey]
          data.faresAvailable → [{key, value: {totals: {fareTotal}, fares: [...]}}]

        Each journey has:
          - designator: {origin, destination, departure, arrival}
          - segments[].identifier: {carrierCode, identifier (flight number)}
          - segments[].designator: {origin, dest, departure, arrival}
          - segments[].legs[].legInfo: {departureTerminal, arrivalTerminal, equipmentType}
          - fares[].fareAvailabilityKey → links to faresAvailable lookup
          - journeyKey, stops, flightType
        """
        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)

        inner = data.get("data", {})
        results = inner.get("results", [])
        if not results:
            return offers

        # Build fare lookup: fareAvailabilityKey → {totals, fares, productClass}
        fare_lookup: dict[str, dict] = {}
        for fare_entry in inner.get("faresAvailable", []):
            key = fare_entry.get("key", "")
            value = fare_entry.get("value", {})
            if key and value:
                fare_lookup[key] = value

        # Walk results → trips → journeysAvailableByMarket → journeys
        for result_block in results:
            for trip in result_block.get("trips", []):
                for market in trip.get("journeysAvailableByMarket", []):
                    journeys = market.get("value", [])
                    for journey in journeys:
                        offer = self._parse_journey(
                            journey, fare_lookup, req, booking_url
                        )
                        if offer:
                            offers.append(offer)

        return offers

    def _parse_journey(
        self,
        journey: dict,
        fare_lookup: dict[str, dict],
        req: FlightSearchRequest,
        booking_url: str,
    ) -> Optional[FlightOffer]:
        """Convert a single Navitaire journey into a FlightOffer."""
        # Find the cheapest fare for this journey
        best_price = float("inf")
        best_currency = req.currency
        best_product = ""

        for fare_ref in journey.get("fares", []):
            fare_key = fare_ref.get("fareAvailabilityKey", "")
            fare_data = fare_lookup.get(fare_key)
            if not fare_data:
                continue
            totals = fare_data.get("totals", {})
            fare_total = totals.get("fareTotal", 0)  # Includes taxes+fees
            if 0 < fare_total < best_price:
                best_price = fare_total
                # Determine currency from service charges
                for fare in fare_data.get("fares", []):
                    for pf in fare.get("passengerFares", []):
                        for sc in pf.get("serviceCharges", []):
                            if sc.get("currencyCode"):
                                best_currency = sc["currencyCode"]
                                break
                best_product = fare_data.get("fares", [{}])[0].get("productClass", "")

        if best_price == float("inf") or best_price <= 0:
            return None

        # Parse segments
        segments = self._parse_nsk_segments(journey.get("segments", []))
        if not segments:
            return None

        designator = journey.get("designator", {})
        dep_str = designator.get("departure", "")
        arr_str = designator.get("arrival", "")
        dep_dt = self._parse_dt(dep_str)
        arr_dt = self._parse_dt(arr_str)
        total_dur = int((arr_dt - dep_dt).total_seconds()) if dep_dt and arr_dt else 0

        stops = journey.get("stops", max(len(segments) - 1, 0))
        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=stops,
        )

        journey_key = journey.get("journeyKey", "")
        offer_key = f"{journey_key}_{best_price}"

        # Map productClass to cabin: EC=Economy, AV=Akasa Value (premium economy equivalent)
        cabin_map = {"EC": "M", "AV": "W", "NB": "M", "LB": "M"}
        cabin = cabin_map.get(best_product, "M")
        for seg in segments:
            seg.cabin_class = cabin

        return FlightOffer(
            id=f"qp_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=best_currency,
            price_formatted=f"{best_price:.2f} {best_currency}",
            outbound=route,
            inbound=None,
            airlines=["Akasa Air"],
            owner_airline="QP",
            booking_url=booking_url,
            is_locked=False,
            source="akasa_direct",
            source_tier="free",
        )

    def _parse_nsk_segments(self, segments_raw: list) -> list[FlightSegment]:
        """Parse Navitaire segments.

        Each segment has:
          identifier: {carrierCode: "QP", identifier: "1819"}
          designator: {origin, destination, departure, arrival}
          legs[0].legInfo: {departureTerminal, arrivalTerminal, equipmentType}
        """
        segments: list[FlightSegment] = []

        for seg_info in segments_raw:
            ident = seg_info.get("identifier", {})
            design = seg_info.get("designator", {})
            carrier = ident.get("carrierCode", "QP")
            number = ident.get("identifier", "")

            origin = design.get("origin", "")
            dest = design.get("destination", "")
            dep_dt = self._parse_dt(design.get("departure", ""))
            arr_dt = self._parse_dt(design.get("arrival", ""))

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Akasa Air",
                flight_no=f"{carrier}{number}",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=arr_dt,
                cabin_class="M",
            ))

        return segments

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
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

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.akasaair.com/booking?origin={req.origin}"
            f"&destination={req.destination}&date={dep}"
            f"&adults={req.adults}&tripType=O"
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest,
    ) -> FlightSearchResponse:
        h = hashlib.md5(
            f"akasa{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "INR"),
            offers=offers,
            total_results=len(offers),
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return self._build_response([], req)
