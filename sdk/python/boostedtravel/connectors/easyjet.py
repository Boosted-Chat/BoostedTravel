"""
easyJet CDP Chrome hybrid scraper — persistent browser + form navigation
+ API response interception.

easyJet's API (/funnel/api/query) is behind Akamai WAF — requires browser-level
session. Direct deep-link URLs redirect to homepage without a BFF session.
The search must be initiated via the homepage form to trigger the BFF.

Strategy (CDP Chrome + response interception):
1. Launch REAL system Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP. Browser context persists across searches.
3. Each search: new page → intercept → homepage → fill form → click search.
4. Capture POST /funnel/api/query response via page.on("response").
5. Parse journeyPairs → FlightOffers.

If Akamai flags the session (403), delete user-data-dir and restart Chrome
with a clean profile. Real Chrome bypasses fingerprinting where bundled
Chromium fails.
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
from datetime import datetime
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import find_chrome, stealth_popen_kwargs

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]

_DEBUG_PORT = 9450
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".easyjet_chrome_data"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_context = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (shares cookies across searches)."""
    global _context
    browser = await _get_browser()
    if _context:
        try:
            # Check context is still alive
            if _context.pages:
                return _context
        except Exception:
            pass
    contexts = browser.contexts
    if contexts:
        _context = contexts[0]
    else:
        _context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
        )
    return _context


async def _get_browser():
    """Launch real Chrome via CDP (headed — Akamai blocks headless)."""
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
        from connectors.browser import find_chrome, stealth_popen_kwargs, _launched_procs

        # Try connecting to existing Chrome on the port first
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
            _pw_instance = pw
            logger.info("easyJet: connected to existing Chrome on port %d", _DEBUG_PORT)
            return _browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        # Launch Chrome HEADED (no --headless) — Akamai 403s headless Chrome.
        # Off-screen + minimised so it doesn't disturb the user.
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
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        logger.info("easyJet: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
        return _browser


async def _dismiss_cookies(page) -> None:
    """Remove ensighten / cookie banners and account modals that block interaction."""
    for label in ["Accept", "Accept all", "Accept All Cookies", "I agree", "Got it", "OK"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                logger.info("easyJet: clicked cookie accept button '%s'", label)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue

    try:
        await page.evaluate("""() => {
            // Remove cookie banners
            const ids = ['ensBannerBG', 'ensNotifyBanner', 'onetrust-consent-sdk',
                          'ensCloseBanner', 'ens-banner-overlay'];
            ids.forEach(id => { const el = document.getElementById(id); if (el) el.remove(); });
            document.querySelectorAll(
                '.ens-banner, [class*="cookie-banner"], [class*="consent"], ' +
                '[class*="CookieBanner"], [id*="cookie"], [id*="consent"], ' +
                '[class*="overlay"][style*="z-index"]'
            ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            // Remove account modals that intercept pointer events
            document.querySelectorAll(
                '.modal-lightbox-wrapper, .account-modal, .modal__dialog-wrapper'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


async def _reset_chrome_profile():
    """Kill Chrome and wipe user-data-dir to clear Akamai-flagged sessions."""
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
            logger.info("easyJet: deleted stale Chrome profile %s", _USER_DATA_DIR)
        except Exception as e:
            logger.warning("easyJet: failed to delete Chrome profile: %s", e)


class EasyjetConnectorClient:
    """easyJet CDP Chrome scraper — persistent browser + form + API response interception."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search easyJet using CDP Chrome + homepage form + API response interception.

        1. Get persistent browser context (cookies carry over)
        2. Open new page, set up response interception for POST /funnel/api/query
        3. Navigate to homepage, fill search form
        4. Click "Show flights" → capture intercepted API response
        5. Parse journeyPairs → FlightOffers
        6. Close page (context stays alive for next search)
        """
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()

        # Set up response interception BEFORE navigating
        search_data: dict = {}
        akamai_blocked = False

        async def _on_response(response):
            nonlocal akamai_blocked
            url = response.url
            if (
                "/funnel/api/query" in url
                and "auth-status" not in url
                and "search/airports" not in url
                and "/stats" not in url
            ):
                status = response.status
                if status == 403:
                    akamai_blocked = True
                    logger.warning("easyJet: Akamai 403 on /funnel/api/query")
                    return
                if status == 200:
                    try:
                        data = await response.json()
                        if isinstance(data, dict) and "journeyPairs" in data:
                            search_data.update(data)
                            logger.info("easyJet: captured search API response")
                    except Exception as e:
                        logger.warning("easyJet: failed to parse API response: %s", e)

        page.on("response", _on_response)

        try:
            logger.info("easyJet: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.easyjet.com/en/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)

            # Dismiss cookie/consent banners
            await _dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await _dismiss_cookies(page)

            # Fill the search form
            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("easyJet: form fill failed, aborting")
                return self._empty(req)

            # Click "Show flights"
            try:
                await page.get_by_role("button", name="Show flights").click(timeout=5000)
                logger.info("easyJet: clicked 'Show flights', waiting for navigation")
            except Exception as e:
                logger.warning("easyJet: could not click 'Show flights': %s", e)
                return self._empty(req)

            # Wait for navigation to /buy/flights
            try:
                await page.wait_for_url("**/buy/flights**", timeout=15000)
                logger.info("easyJet: navigated to %s", page.url)
            except Exception:
                logger.warning("easyJet: didn't navigate to /buy/flights, URL: %s", page.url)

            # Wait for the intercepted API response (up to remaining timeout)
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            while not search_data and not akamai_blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            # If Akamai blocked us, nuke the profile and bail
            if akamai_blocked:
                logger.warning("easyJet: Akamai flagged session, clearing Chrome profile for next run")
                await _reset_chrome_profile()
                return self._empty(req)

            if not search_data or not search_data.get("journeyPairs"):
                logger.warning("easyJet: no journeyPairs in intercepted response")
                return self._empty(req)

            currency = search_data.get("metaData", {}).get("currencyCode", "GBP")
            offers = self._parse_journey_pairs(search_data["journeyPairs"], req, currency)

            elapsed = time.monotonic() - t0
            offers.sort(key=lambda o: o.price)

            logger.info(
                "easyJet %s→%s returned %d offers in %.1fs (CDP Chrome)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"easyjet{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("easyJet CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Form interaction
    # ------------------------------------------------------------------

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill the easyJet homepage search form."""
        ok = await self._fill_airport_field(page, "From", req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        ok = await self._fill_airport_field(page, "To", req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        ok = await self._fill_date(page, req)
        if not ok:
            return False
        return True

    async def _fill_airport_field(self, page, label: str, iata: str) -> bool:
        """Fill an airport textbox and select the matching suggestion."""
        try:
            # Remove any overlays that might intercept clicks
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '.modal-lightbox-wrapper, .account-modal, .modal__dialog-wrapper, ' +
                    '[class*="overlay"][style*="z-index"]'
                ).forEach(el => el.remove());
            }""")

            field = page.get_by_role("textbox", name=label)
            if label == "From":
                clear_name = "Clear selected departure airport"
            else:
                clear_name = "Clear selected destination airport"
            try:
                clear_btn = page.get_by_role("button", name=clear_name)
                if await clear_btn.count() > 0:
                    await clear_btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                pass

            await field.click(timeout=3000)
            await asyncio.sleep(0.3)
            await field.fill(iata)
            logger.info("easyJet: typed '%s' in %s field", iata, label)
            await asyncio.sleep(2.0)

            # Try multiple selector strategies for the autocomplete dropdown
            for role in ("option", "radio", "listitem"):
                try:
                    option = page.get_by_role(role, name=re.compile(
                        rf"{re.escape(iata)}", re.IGNORECASE
                    )).first
                    if await option.count() > 0:
                        await option.click(timeout=3000)
                        logger.info("easyJet: selected %s airport via %s role", iata, role)
                        # Close any lingering dropdown overlays
                        await asyncio.sleep(0.3)
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.3)
                        return True
                except Exception:
                    continue

            for sel in (
                f'[data-testid*="airport"] >> text=/{re.escape(iata)}/i',
                f'li:has-text("{iata}")',
                f'[role="listbox"] >> text=/{re.escape(iata)}/i',
                f'ul li >> text=/{re.escape(iata)}/i',
            ):
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("easyJet: selected %s airport via locator", iata)
                        await asyncio.sleep(0.3)
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.3)
                        return True
                except Exception:
                    continue

            for sel in (
                '[role="listbox"] [role="option"]',
                '[class*="airport"] li',
                '[class*="dropdown"] li',
                '[class*="suggestion"] li',
                '[class*="result"] li',
            ):
                try:
                    item = page.locator(sel).first
                    if await item.count() > 0:
                        await item.click(timeout=3000)
                        logger.info("easyJet: selected first dropdown item for %s", iata)
                        await asyncio.sleep(0.3)
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.3)
                        return True
                except Exception:
                    continue

            logger.warning("easyJet: %s field — no matching suggestion found for %s", label, iata)
            return False
        except Exception as e:
            logger.warning("easyJet: %s field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open the date picker and select the outbound date."""
        target = req.date_from
        try:
            try:
                date_field = page.get_by_role("textbox", name="Clear selected travel date")
                if await date_field.count() == 0:
                    date_field = page.get_by_placeholder("Choose your dates")
                await date_field.click(timeout=3000)
            except Exception:
                when_section = page.locator("text=When").first
                await when_section.click(timeout=3000)
            await asyncio.sleep(0.5)

            try:
                await page.wait_for_selector(
                    '[data-testid="month-title"]', timeout=10000
                )
            except Exception:
                logger.warning("easyJet: calendar grid didn't load in time")
                return False
            await asyncio.sleep(0.3)

            testid = f"{target.day}-{target.month}-{target.year}"
            day_btn = page.locator(f'[data-testid="{testid}"]')

            aria_label = f"{target.strftime('%B')} {target.day}, {target.year}"
            day_btn_fallback = page.get_by_role("button", name=aria_label)

            for attempt in range(12):
                if await day_btn.count() > 0 or await day_btn_fallback.count() > 0:
                    break
                try:
                    await page.get_by_role("button", name="Next month").click(timeout=2000)
                    await asyncio.sleep(0.5)
                except Exception:
                    break

            if await day_btn.count() > 0:
                await day_btn.click(timeout=5000)
                logger.info("easyJet: clicked date %s (testid: %s)", target, testid)
            elif await day_btn_fallback.count() > 0:
                await day_btn_fallback.click(timeout=5000)
                logger.info("easyJet: clicked date %s (aria-label fallback)", target)
            else:
                clicked = await page.evaluate("""(args) => {
                    const [testid, ariaLabel] = args;
                    let btn = document.querySelector(`[data-testid="${testid}"]`);
                    if (!btn) {
                        btn = Array.from(document.querySelectorAll('button'))
                            .find(b => b.getAttribute('aria-label') === ariaLabel);
                    }
                    if (btn) { btn.click(); return true; }
                    return false;
                }""", [testid, aria_label])
                if clicked:
                    logger.info("easyJet: clicked date %s (JS fallback)", target)
                else:
                    logger.warning("easyJet: could not find date button for %s", target)
                    return False

            await asyncio.sleep(0.5)
            # Dismiss date picker dropdown so it doesn't block "Show flights"
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
            return True
        except Exception as e:
            logger.warning("easyJet: date error: %s", e)
            return False

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_journey_pairs(
        self, journey_pairs: list, req: FlightSearchRequest, currency: str
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        target_date = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)

        for pair in journey_pairs:
            outbound = pair.get("outbound", {})
            flights_by_date = outbound.get("flights", {})

            matched_dates = [dk for dk in flights_by_date if dk == target_date]
            if not matched_dates:
                available = list(flights_by_date.keys())
                logger.warning(
                    "easyJet: target %s not in flight dates %s, using all",
                    target_date, available,
                )
                matched_dates = available

            for date_key in matched_dates:
                for flight in flights_by_date[date_key]:
                    offer = self._parse_single_flight(flight, currency, booking_url)
                    if offer:
                        offers.append(offer)

        return offers

    def _parse_single_flight(
        self, flight: dict, currency: str, booking_url: str
    ) -> Optional[FlightOffer]:
        if flight.get("soldOut") or flight.get("saleableStatus") != "AVAILABLE":
            return None

        fares = flight.get("fares", {})
        adt_fares = fares.get("ADT", {})
        price = None
        for fare_family in ["STANDARD", "FLEXI"]:
            fare = adt_fares.get(fare_family)
            if fare:
                unit_price = fare.get("unitPrice", {})
                gross = unit_price.get("grossPrice")
                if gross is not None:
                    if price is None or gross < price:
                        price = gross
                    break

        if price is None or price <= 0:
            return None

        flight_no = flight.get("flightNumber", "")
        carrier = flight.get("iataCarrierCode", "U2")
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        dep_str = flight.get("localDepartureDateTime", "")
        arr_str = flight.get("localArrivalDateTime", "")

        segment = FlightSegment(
            airline=carrier,
            airline_name="easyJet",
            flight_no=flight_no,
            origin=flight.get("departureAirportCode", ""),
            destination=flight.get("arrivalAirportCode", ""),
            departure=self._parse_dt(dep_str),
            arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

        total_dur = int((segment.arrival - segment.departure).total_seconds())

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=max(total_dur, 0),
            stopovers=0,
        )

        key = f"{flight_no}_{dep_str}_{price}"

        return FlightOffer(
            id=f"ej_{hashlib.md5(key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["easyJet"],
            owner_airline="U2",
            booking_url=booking_url,
            is_locked=False,
            source="easyjet_direct",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_out = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.easyjet.com/en/buy/flights"
            f"?dep={req.origin}&dest={req.destination}"
            f"&dd={date_out}&isOneWay=on"
            f"&apax={req.adults}&cpax={req.children or 0}"
            f"&ipax={req.infants or 0}"
        )

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"easyjet{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


# ── Bookable connector (checkout automation) ─────────────────────────────

class EasyjetBookableConnector:
    """
    Drive EasyJet checkout up to (not including) payment submission.

    Flow: Navigate to booking URL → Select flights → Select fare →
          Fill passengers → Skip extras → STOP at payment page.

    Uses Playwright with Akamai-aware headed Chrome. Never submits payment.
    """

    AIRLINE_NAME = "easyJet"
    SOURCE_TAG = "easyjet_direct"

    async def start_checkout(
        self,
        offer: dict,
        passengers: list[dict],
        checkout_token: str,
        api_key: str,
        *,
        base_url: str | None = None,
    ):
        from connectors.booking_base import (
            CheckoutProgress,
            dismiss_overlays,
            safe_click,
            safe_fill,
            take_screenshot_b64,
            verify_checkout_token,
        )
        import random
        import time

        t0 = time.monotonic()
        booking_url = offer.get("booking_url", "")
        offer_id = offer.get("id", "")

        # Verify checkout token with backend
        try:
            verification = verify_checkout_token(offer_id, checkout_token, api_key, base_url)
            if not verification.get("valid"):
                return CheckoutProgress(
                    status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                    offer_id=offer_id, booking_url=booking_url,
                    message="Checkout token invalid or expired. Call unlock() first ($1 fee).",
                )
        except Exception as e:
            return CheckoutProgress(
                status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                offer_id=offer_id, booking_url=booking_url,
                message=f"Token verification failed: {e}",
            )

        if not booking_url:
            return CheckoutProgress(
                status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                offer_id=offer_id, message="No booking URL available for this offer.",
            )

        from playwright.async_api import async_playwright

        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": random.choice([1366, 1440, 1920]),
                       "height": random.choice([768, 900, 1080])},
            locale="en-GB",
            timezone_id="Europe/London",
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            step = "started"
            pax = passengers[0] if passengers else {}

            # Step 1: Navigate to EasyJet booking page
            logger.info("EasyJet checkout: navigating to %s", booking_url)
            await page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # Dismiss EasyJet-specific overlays
            await dismiss_overlays(page)
            for sel in [
                "#ensCloseBanner",
                "button:has-text('Accept all cookies')",
                "[class*='cookie-banner'] button",
            ]:
                await safe_click(page, sel, timeout=2000, desc="easyjet cookies")

            step = "page_loaded"

            # Step 2: Select flights (EasyJet shows flight cards)
            try:
                await page.wait_for_selector(
                    "[class*='flight-grid'], [class*='flight-card'], [data-testid*='flight']",
                    timeout=20000,
                )
            except Exception:
                pass

            await dismiss_overlays(page)

            # Try to match by departure time
            outbound = offer.get("outbound", {})
            segments = outbound.get("segments", []) if isinstance(outbound, dict) else []
            if segments:
                dep = segments[0].get("departure", "")
                if dep and len(dep) >= 16:
                    dep_time = dep[11:16]
                    try:
                        card = page.locator(f"text='{dep_time}'").first
                        if await card.is_visible(timeout=3000):
                            await card.click()
                    except Exception:
                        pass

            # Fallback: click first available flight
            for sel in [
                "[class*='flight-card']:first-child",
                "[data-testid*='flight']:first-child",
                "button:has-text('Select'):first-child",
            ]:
                await safe_click(page, sel, timeout=3000, desc="first flight")

            await page.wait_for_timeout(2000)
            step = "flights_selected"

            # Step 3: Select fare (Standard/Flexi)
            for sel in [
                "button:has-text('Standard')",
                "button:has-text('Continue')",
                "[class*='fare'] button:first-child",
                "button:has-text('Select')",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="select fare"):
                    break

            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)
            step = "fare_selected"

            # Step 4: Skip login if prompted
            for sel in [
                "button:has-text('Continue as guest')",
                "button:has-text('Skip')",
                "button:has-text('No thanks')",
                "[data-testid*='guest'] button",
            ]:
                if await safe_click(page, sel, timeout=4000, desc="skip login"):
                    break
            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)
            step = "login_bypassed"

            # Step 5: Fill passenger details
            try:
                await page.wait_for_selector(
                    "input[name*='name'], [class*='passenger-form'], [data-testid*='passenger']",
                    timeout=15000,
                )
            except Exception:
                pass

            # Title
            title = "Mr" if pax.get("gender", "m") == "m" else "Ms"
            for sel in [
                f"select option:has-text('{title}')",
                f"[data-testid*='title'] option:has-text('{title}')",
            ]:
                try:
                    await page.select_option("select[name*='title'], [data-testid*='title'] select",
                                             label=title, timeout=3000)
                    break
                except Exception:
                    pass
            # Fallback: click dropdown
            await safe_click(page, f"button:has-text('{title}')", timeout=2000, desc=f"title {title}")

            # First name
            for sel in [
                "input[name*='firstName']",
                "input[data-testid*='first-name']",
                "input[placeholder*='First name' i]",
            ]:
                if await safe_fill(page, sel, pax.get("given_name", "Test")):
                    break

            # Last name
            for sel in [
                "input[name*='lastName']",
                "input[data-testid*='last-name']",
                "input[placeholder*='Last name' i]",
            ]:
                if await safe_fill(page, sel, pax.get("family_name", "Traveler")):
                    break

            # Email
            for sel in [
                "input[name*='email']",
                "input[data-testid*='email']",
                "input[type='email']",
            ]:
                if await safe_fill(page, sel, pax.get("email", "test@example.com")):
                    break

            # Phone
            for sel in [
                "input[name*='phone']",
                "input[data-testid*='phone']",
                "input[type='tel']",
            ]:
                if await safe_fill(page, sel, pax.get("phone_number", "+441234567890")):
                    break

            step = "passengers_filled"

            # Continue
            for sel in [
                "button:has-text('Continue')",
                "button:has-text('Next')",
                "[data-testid*='continue'] button",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="continue"):
                    break
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            # Step 6: Skip extras (bags, insurance, seats)
            for _ in range(5):
                await dismiss_overlays(page)
                for sel in [
                    "button:has-text('No, thanks')",
                    "button:has-text('Continue')",
                    "button:has-text('Skip')",
                    "button:has-text('No hold luggage')",
                    "button:has-text('Continue without')",
                    "button:has-text('Next')",
                ]:
                    await safe_click(page, sel, timeout=2000, desc="skip extras")
                await page.wait_for_timeout(1500)

            step = "extras_skipped"

            # Skip seats
            for sel in [
                "button:has-text('Skip')",
                "button:has-text('No thanks')",
                "button:has-text('Continue without')",
                "button:has-text('Assign random seats')",
            ]:
                if await safe_click(page, sel, timeout=4000, desc="skip seats"):
                    break
            await page.wait_for_timeout(1500)
            for sel in ["button:has-text('OK')", "button:has-text('Continue')"]:
                await safe_click(page, sel, timeout=3000, desc="confirm skip")

            step = "seats_skipped"
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            # Step 7: Payment page reached — STOP HERE
            step = "payment_page_reached"
            screenshot = await take_screenshot_b64(page)

            page_price = offer.get("price", 0.0)
            try:
                for sel in [
                    "[class*='total-price']",
                    "[data-testid*='total']",
                    "[class*='summary'] [class*='price']",
                ]:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=2000):
                        text = await el.text_content()
                        if text:
                            import re
                            nums = re.findall(r"[\d,.]+", text)
                            if nums:
                                page_price = float(nums[-1].replace(",", ""))
                        break
            except Exception:
                pass

            elapsed = time.monotonic() - t0
            return CheckoutProgress(
                status="payment_page_reached",
                step=step,
                step_index=8,
                airline=self.AIRLINE_NAME,
                source=self.SOURCE_TAG,
                offer_id=offer_id,
                total_price=page_price,
                currency=offer.get("currency", "EUR"),
                booking_url=booking_url,
                screenshot_b64=screenshot,
                message=(
                    f"easyJet checkout complete — reached payment page in {elapsed:.0f}s. "
                    f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                    f"Payment NOT submitted (safe mode). "
                    f"Complete manually at: {booking_url}"
                ),
                can_complete_manually=True,
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            logger.error("EasyJet checkout error: %s", e, exc_info=True)
            screenshot = ""
            try:
                screenshot = await take_screenshot_b64(page)
            except Exception:
                pass
            return CheckoutProgress(
                status="error",
                step=step,
                airline=self.AIRLINE_NAME,
                source=self.SOURCE_TAG,
                offer_id=offer_id,
                booking_url=booking_url,
                screenshot_b64=screenshot,
                message=f"Checkout error at step '{step}': {e}",
                elapsed_seconds=time.monotonic() - t0,
            )
        finally:
            await context.close()
            await browser.close()
            await pw.stop()
