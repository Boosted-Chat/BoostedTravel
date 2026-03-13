"""
Smartwings hybrid scraper — curl_cffi direct + cookie-farm + stealth Playwright.

Smartwings (IATA: QS) is the Czech Republic's largest airline group,
operating from Prague (PRG), Brno (BRQ), Ostrava (OSR) and Pardubice (PED)
to Mediterranean, Middle East and North Africa destinations.

Cloudflare WAF on www.smartwings.com — bypassed via curl_cffi Chrome TLS
fingerprint or stealth Playwright with persistent Cloudflare clearance.

Hybrid strategy (Mar 2026):
1. (Primary) curl_cffi with Chrome TLS fingerprint (impersonate="chrome124"):
   - Deep-link to book.smartwings.com Amadeus FPOW (bypasses www Cloudflare)
   - Fallback: fetch smartwings.com with Chrome TLS
   - Parse FlexPricer HTML for availability data
   - ~2-5s, no browser overhead
2. (Secondary) Cookie-farm + curl_cffi:
   - Farm Cloudflare clearance cookies via stealth Playwright
   - Replay cookies with curl_cffi for fast subsequent searches
3. (Fallback) Playwright with stealth improvements:
   - Real Chrome via CDP with persistent user-data-dir (Cloudflare clearance)
   - playwright_stealth for navigator.webdriver patching
   - CDP markers removal via JS injection
   - Navigate homepage → fill search form → parse FPOW results
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL = True
except ImportError:
    HAS_CURL = False

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import get_or_launch_cdp

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE", "en-AU"]
_TIMEZONES = [
    "Europe/Prague", "Europe/London", "Europe/Berlin",
    "Europe/Paris", "Europe/Vienna", "Europe/Warsaw",
]

_MAX_ATTEMPTS = 2
_IMPERSONATE = "chrome124"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_COOKIE_MAX_AGE = 25 * 60  # 25 minutes

# Amadeus FPOW deep-link base (may bypass Cloudflare on www)
_FPOW_BASE = "https://book.smartwings.com/plnext/smartwings/Override.action"

# ── CDP Chrome singleton ──────────────────────────────────────────────
_CDP_PORT = 9452
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "smartwings_cdp_data")

_chrome_proc = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None

# ── Cookie farm state ─────────────────────────────────────────────────
_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0.0


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Shared real Chrome via CDP (launched once, reused across searches)."""
    global _pw_instance, _cdp_browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _cdp_browser and _cdp_browser.is_connected():
            return _cdp_browser
        _cdp_browser, _chrome_proc = await get_or_launch_cdp(_CDP_PORT, _USER_DATA_DIR)
        logger.info("Smartwings: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _cdp_browser


# ── Stealth script — removes CDP / webdriver markers ──────────────────

_STEALTH_JS = """\
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
window.navigator.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}};
const _origQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (p) => (
    p.name === 'notifications'
        ? Promise.resolve({state: Notification.permission})
        : _origQuery(p)
);
"""


class SmartwingsConnectorClient:
    """Smartwings hybrid scraper — curl_cffi direct + stealth Playwright."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ── Main entry point ──────────────────────────────────────────────

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # ── Primary: curl_cffi (no browser) ──
        if HAS_CURL:
            try:
                offers = await self._search_via_api(req)
                if offers:
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "Smartwings API: %d offers %s→%s in %.1fs",
                        len(offers), req.origin, req.destination, elapsed,
                    )
                    return self._build_response(offers, req, elapsed)
            except Exception as e:
                logger.warning("Smartwings: API path error: %s", e)

            # ── Secondary: cookie-farm + curl_cffi ──
            try:
                cookies = await self._ensure_cookies(req)
                if cookies:
                    offers = await self._search_via_api(req, cookies=cookies)
                    if offers:
                        elapsed = time.monotonic() - t0
                        logger.info(
                            "Smartwings cookie+API: %d offers in %.1fs",
                            len(offers), elapsed,
                        )
                        return self._build_response(offers, req, elapsed)
            except Exception as e:
                logger.warning("Smartwings: cookie-farm path error: %s", e)

        # ── Fallback: improved Playwright with stealth ──
        logger.info(
            "Smartwings: falling back to browser for %s→%s",
            req.origin, req.destination,
        )
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._search_via_browser(req, t0)
                if result.total_results > 0 or attempt == _MAX_ATTEMPTS:
                    return result
                logger.warning(
                    "Smartwings: browser attempt %d got 0 offers, retrying",
                    attempt,
                )
            except Exception as e:
                logger.error("Smartwings: browser attempt %d error: %s", attempt, e)
                if attempt == _MAX_ATTEMPTS:
                    return self._empty(req)
        return self._empty(req)

    # ------------------------------------------------------------------
    # curl_cffi direct API path
    # ------------------------------------------------------------------

    async def _search_via_api(
        self,
        req: FlightSearchRequest,
        *,
        cookies: list[dict] | None = None,
    ) -> list[FlightOffer] | None:
        """Fetch availability via curl_cffi (Chrome TLS fingerprint)."""
        return await asyncio.to_thread(self._api_search_sync, req, cookies or [])

    def _api_search_sync(
        self,
        req: FlightSearchRequest,
        cookies: list[dict],
    ) -> list[FlightOffer] | None:
        """Synchronous curl_cffi search — tries FPOW deep-link then homepage."""
        sess = curl_requests.Session(impersonate=_IMPERSONATE)
        for c in cookies:
            sess.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

        headers = {
            "User-Agent": _UA,
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "en-GB,en;q=0.9,cs;q=0.8",
            "Referer": "https://www.smartwings.com/en",
        }

        # Strategy 1: FPOW deep-link (bypasses www Cloudflare entirely)
        offers = self._try_fpow_deeplink(sess, req, headers)
        if offers:
            return offers

        # Strategy 2: smartwings.com search URL with Chrome TLS
        offers = self._try_homepage_search(sess, req, headers)
        if offers:
            return offers

        return None

    def _try_fpow_deeplink(
        self,
        sess: Any,
        req: FlightSearchRequest,
        headers: dict,
    ) -> list[FlightOffer] | None:
        """Try direct FPOW deep-link to Amadeus booking engine."""
        date_param = req.date_from.strftime("%Y%m%d")
        params: dict[str, str] = {
            "TRIP_TYPE": "O",
            "EXTERNAL_ID": "homepage",
            "B_LOCATION_1": req.origin,
            "E_LOCATION_1": req.destination,
            "B_DATE_1": date_param,
            "TRAVELLER_TYPE_1": "ADT",
            "TRAVELLER_COUNT_1": str(req.adults),
        }
        if req.children:
            params["TRAVELLER_TYPE_2"] = "CNN"
            params["TRAVELLER_COUNT_2"] = str(req.children)
        if req.infants:
            k = "3" if req.children else "2"
            params[f"TRAVELLER_TYPE_{k}"] = "INF"
            params[f"TRAVELLER_COUNT_{k}"] = str(req.infants)

        try:
            r = sess.get(
                _FPOW_BASE,
                params=params,
                headers=headers,
                timeout=20,
                allow_redirects=True,
            )
            if r.status_code == 200 and len(r.text) > 5000:
                offers = self._parse_fpow_html(r.text, req)
                if offers:
                    logger.info(
                        "Smartwings: FPOW deep-link returned %d offers",
                        len(offers),
                    )
                    return offers
        except Exception as e:
            logger.debug("Smartwings: FPOW deep-link error: %s", e)
        return None

    def _try_homepage_search(
        self,
        sess: Any,
        req: FlightSearchRequest,
        headers: dict,
    ) -> list[FlightOffer] | None:
        """Fetch smartwings.com search URL with Chrome TLS fingerprint."""
        dep = req.date_from.strftime("%Y-%m-%d")
        url = (
            f"https://www.smartwings.com/en/flights?from={req.origin}"
            f"&to={req.destination}&departure={dep}"
            f"&adults={req.adults}"
            f"&children={req.children or 0}"
            f"&infants={req.infants or 0}"
        )
        try:
            r = sess.get(
                url,
                headers=headers,
                timeout=20,
                allow_redirects=True,
            )
            if r.status_code != 200:
                return None
            # Check for Cloudflare challenge page
            if "just a moment" in r.text[:2000].lower():
                logger.debug("Smartwings: Cloudflare challenge in curl response")
                return None
            if len(r.text) > 5000:
                offers = self._parse_fpow_html(r.text, req)
                if offers:
                    return offers
        except Exception as e:
            logger.debug("Smartwings: homepage search error: %s", e)
        return None

    def _parse_fpow_html(
        self, html: str, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Parse Amadeus FPOW HTML for flight availability data."""
        offers: list[FlightOffer] = []
        date_str = req.date_from.isoformat()
        booking_url = self._build_booking_url(req)

        # Extract fare headers
        fare_headers: list[str] = []
        for m in re.finditer(
            r'farefamily-header-content[^>]*>([^<]+)<', html,
        ):
            fare_headers.append(m.group(1).strip())

        # Split HTML by flight-line blocks
        flight_blocks = re.split(r'bound-table-flightline', html)
        for block in flight_blocks[1:]:
            # Extract times
            times = re.findall(r'<time[^>]*>([^<]+)</time>', block)
            dep_time = times[0].strip() if len(times) > 0 else ""
            arr_time = times[1].strip() if len(times) > 1 else ""

            # Duration
            dur_m = re.search(
                r'flight-duration-info[^>]*>.*?<strong>([^<]+)</strong>',
                block,
                re.DOTALL,
            )
            duration_str = dur_m.group(1).strip() if dur_m else ""

            # Flight number
            fn_m = re.search(r'QS\d+', block)
            flight_no = fn_m.group(0) if fn_m else ""

            is_direct = "Direct" in block or "direct" in block

            # Prices
            prices: list[float] = []
            for pm in re.finditer(
                r'cell-reco-bestprice-integer[^>]*>([^<]+)<', block,
            ):
                try:
                    prices.append(float(pm.group(1).strip()))
                except (ValueError, TypeError):
                    pass

            if not dep_time or not prices:
                continue

            dep_dt = self._parse_time(date_str, dep_time)
            arr_dt = self._parse_time(date_str, arr_time)
            if arr_dt < dep_dt:
                from datetime import timedelta
                arr_dt += timedelta(days=1)

            dur_secs = self._parse_duration(duration_str)
            if dur_secs == 0 and dep_dt and arr_dt:
                dur_secs = int((arr_dt - dep_dt).total_seconds())

            segment = FlightSegment(
                airline="QS",
                airline_name="Smartwings",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur_secs,
                cabin_class="economy",
            )
            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=max(dur_secs, 0),
                stopovers=0 if is_direct else 1,
            )

            for i, price in enumerate(prices):
                fare_name = fare_headers[i] if i < len(fare_headers) else ""
                suffix = fare_name.lower() if fare_name else str(i)
                offer_key = f"{flight_no}_{date_str}_{suffix}"
                offer_id = (
                    f"qs_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}"
                )
                offers.append(FlightOffer(
                    id=offer_id,
                    price=round(price, 2),
                    currency=req.currency or "EUR",
                    price_formatted=(
                        f"{price:.2f} EUR"
                        + (f" ({fare_name})" if fare_name else "")
                    ),
                    outbound=route,
                    inbound=None,
                    airlines=["Smartwings"],
                    owner_airline="QS",
                    booking_url=booking_url,
                    is_locked=False,
                    source="smartwings_direct",
                    source_tier="free",
                ))

        return offers

    # ------------------------------------------------------------------
    # Cookie farm — Playwright generates Cloudflare clearance cookies
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
        """Open Playwright, load homepage, extract Cloudflare clearance cookies."""
        global _farmed_cookies, _farm_timestamp

        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            page = await context.new_page()
            await self._apply_stealth(page)

            logger.info("Smartwings: farming Cloudflare cookies via homepage")
            await page.goto(
                "https://www.smartwings.com/en",
                wait_until="domcontentloaded",
                timeout=30000,
            )

            # Extended Cloudflare wait (up to ~25s)
            for _ in range(5):
                try:
                    await page.wait_for_selector(
                        "input.route-from-text", timeout=5000,
                    )
                    break
                except Exception:
                    title = await page.title()
                    if "just a moment" not in title.lower():
                        break
                    await asyncio.sleep(2)

            cookies = await context.cookies()
            _farmed_cookies = cookies
            _farm_timestamp = time.monotonic()
            logger.info("Smartwings: farmed %d cookies", len(cookies))
            return cookies

        except Exception as e:
            logger.error("Smartwings: cookie farm error: %s", e)
            return []
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Improved Playwright with stealth
    # ------------------------------------------------------------------

    async def _search_via_browser(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full Playwright search with stealth improvements."""
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )
        page = None
        try:
            page = await context.new_page()
            await self._apply_stealth(page)

            # ── Step 1: Load homepage & wait for Cloudflare ──
            logger.info(
                "Smartwings: browser loading homepage for %s→%s on %s",
                req.origin, req.destination, req.date_from,
            )
            await page.goto(
                "https://www.smartwings.com/en",
                wait_until="domcontentloaded",
                timeout=30000,
            )

            # Extended Cloudflare wait with retries (up to ~30s)
            cf_passed = False
            for _ in range(6):
                try:
                    await page.wait_for_selector(
                        "input.route-from-text", timeout=5000,
                    )
                    cf_passed = True
                    break
                except Exception:
                    title = await page.title()
                    if "just a moment" not in title.lower():
                        cf_passed = True
                        break
                    logger.debug("Smartwings: Cloudflare still active, waiting…")
                    await asyncio.sleep(3)

            if not cf_passed:
                logger.warning(
                    "Smartwings: stuck on Cloudflare after extended wait"
                )
                return self._empty(req)

            # ── Step 2: Cookie consent ──
            await self._dismiss_cookies(page)
            await asyncio.sleep(0.3)

            # ── Step 3: One-way flight ──
            try:
                ow_btn = page.locator("button").filter(
                    has_text="One-way flight",
                ).first
                await ow_btn.click(timeout=3000)
            except Exception:
                pass
            await asyncio.sleep(0.3)

            # ── Step 4: Select airports via data-iata JS click ──
            origin_ok = await self._select_airport(page, "from", req.origin)
            if not origin_ok:
                logger.warning(
                    "Smartwings: failed to select origin %s", req.origin,
                )
                return self._empty(req)
            await asyncio.sleep(0.5)

            dest_ok = await self._select_airport(page, "to", req.destination)
            if not dest_ok:
                logger.warning(
                    "Smartwings: failed to select destination %s",
                    req.destination,
                )
                return self._empty(req)
            await asyncio.sleep(0.5)

            # ── Step 5: Set date via jQuery datepicker ──
            date_str = req.date_from.strftime("%d.%m.%Y")
            await page.evaluate(
                """(dateStr) => {
                    const dp = document.getElementById('datepicker-from');
                    if (dp && window.jQuery) {
                        jQuery('#datepicker-from').datepicker('setDate', dateStr);
                        jQuery('#datepicker-from').datepicker('hide');
                    } else if (dp) {
                        const nativeSetter = Object.getOwnPropertyDescriptor(
                            window.HTMLInputElement.prototype, 'value'
                        ).set;
                        nativeSetter.call(dp, dateStr);
                        dp.dispatchEvent(new Event('change', { bubbles: true }));
                    }
                    document.querySelectorAll('.ui-datepicker').forEach(
                        el => el.style.display = 'none'
                    );
                }""",
                date_str,
            )
            await asyncio.sleep(0.5)

            # ── Step 6: Click Search → book.smartwings.com ──
            search_btn = page.locator(".search-flight").first
            await search_btn.click(timeout=10000, force=True)
            logger.info(
                "Smartwings: clicked Search, waiting for Amadeus redirect",
            )

            try:
                await page.wait_for_url(
                    "**/book.smartwings.com/**", timeout=20000,
                )
            except Exception:
                await asyncio.sleep(5)
                if "book.smartwings.com" not in page.url:
                    logger.warning(
                        "Smartwings: did not redirect to booking page",
                    )
                    return self._empty(req)

            # ── Step 7: Calendar page → click continue ──
            try:
                await page.wait_for_selector(
                    "button:has-text('continue'), [class*='continue']",
                    timeout=15000,
                )
            except Exception:
                await asyncio.sleep(5)

            try:
                continue_btn = page.locator("button").filter(
                    has_text="continue",
                ).first
                await continue_btn.click(timeout=5000)
            except Exception:
                logger.warning(
                    "Smartwings: could not click continue on calendar",
                )
                return self._empty(req)

            # ── Step 8: Wait for flights page (FPOW) ──
            try:
                await page.wait_for_selector(
                    ".bound-table-flightline", timeout=20000,
                )
            except Exception:
                await asyncio.sleep(5)

            # ── Step 9: Parse flight results from DOM ──
            offers = await self._parse_flights_page(page, req)

            # Harvest cookies for future curl_cffi searches
            try:
                global _farmed_cookies, _farm_timestamp
                _farmed_cookies = await context.cookies()
                _farm_timestamp = time.monotonic()
            except Exception:
                pass

            elapsed = time.monotonic() - t0
            if offers:
                return self._build_response(offers, req, elapsed)
            return self._empty(req)

        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
            try:
                await context.close()
            except Exception:
                pass

    # ── Stealth injection ──────────────────────────────────────────────

    async def _apply_stealth(self, page) -> None:
        """Apply stealth patches to avoid Cloudflare bot detection."""
        try:
            from playwright_stealth import stealth_async
            await stealth_async(page)
        except ImportError:
            pass
        try:
            await page.add_init_script(_STEALTH_JS)
        except Exception:
            pass

    # ── Airport selection via data-iata click ──────────────────────────

    async def _select_airport(self, page, direction: str, iata: str) -> bool:
        """Click the airport with matching data-iata in the from/to dropdown."""
        selector_map = {
            "from": ".route-from-select",
            "to": ".route-to-select",
        }
        container = selector_map.get(direction, ".route-from-select")

        # Click the textbox to open the dropdown
        textbox_cls = f"input.route-{direction}-text"
        try:
            await page.click(textbox_cls, timeout=3000)
            await asyncio.sleep(0.5)
        except Exception:
            pass

        # JS-click the airport element with matching data-iata
        clicked = await page.evaluate(
            """([container, iata]) => {
                const el = document.querySelector(container + ' [data-iata="' + iata + '"]');
                if (el) { el.click(); return true; }
                return false;
            }""",
            [container, iata],
        )
        if clicked:
            logger.info("Smartwings: selected %s airport %s", direction, iata)
            return True

        # Fallback: try clicking the textbox, type the IATA code, press Enter
        try:
            await page.click(textbox_cls, timeout=2000)
            await page.fill(textbox_cls, iata)
            await asyncio.sleep(1.0)
            # Try to find matching option in dropdown
            option = await page.evaluate(
                """([container, iata]) => {
                    const items = document.querySelectorAll(container + ' [data-iata]');
                    for (const item of items) {
                        if (item.getAttribute('data-iata') === iata) {
                            item.click();
                            return true;
                        }
                    }
                    return false;
                }""",
                [container, iata],
            )
            if option:
                return True
        except Exception:
            pass

        logger.warning("Smartwings: airport %s not found in %s dropdown", iata, direction)
        return False

    # ── Cookie dismissal ───────────────────────────────────────────────

    async def _dismiss_cookies(self, page) -> None:
        try:
            await page.evaluate("""() => {
                const btns = document.querySelectorAll(
                    '[class*="cookie"] button, [id*="cookie"] button, ' +
                    '[class*="consent"] button, [class*="cc-"] button'
                );
                for (const b of btns) {
                    const t = b.textContent.toLowerCase();
                    if (t.includes('accept') || t.includes('agree') || t.includes('souhlas')) {
                        b.click(); return;
                    }
                }
                // Fallback: remove cookie overlays
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── Parse Amadeus FPOW flights page ────────────────────────────────

    async def _parse_flights_page(
        self, page, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Extract flights from the Amadeus FlexPricer results page."""
        try:
            raw = await page.evaluate(r"""() => {
                const flightlines = document.querySelectorAll('.bound-table-flightline');
                const fareHeaders = [];
                document.querySelectorAll('.farefamily-header-cell').forEach(hdr => {
                    const name = hdr.querySelector('.farefamily-header-content');
                    fareHeaders.push(name ? name.textContent.trim() : '');
                });

                const flights = [];
                flightlines.forEach(fl => {
                    const times = fl.querySelectorAll('time');
                    const durEl = fl.querySelector('.flight-duration-info strong');
                    const text = fl.textContent || '';
                    const flightNoMatch = text.match(/QS\d+/);
                    const isDirect = text.includes('Direct');

                    // Get all fare prices
                    const priceEls = fl.querySelectorAll('.cell-reco-bestprice-integer');
                    const prices = [];
                    priceEls.forEach(p => {
                        const v = parseFloat(p.textContent.trim());
                        if (!isNaN(v)) prices.push(v);
                    });

                    flights.push({
                        depTime: times[0] ? times[0].textContent.trim() : '',
                        arrTime: times[1] ? times[1].textContent.trim() : '',
                        duration: durEl ? durEl.textContent.trim() : '',
                        flightNo: flightNoMatch ? flightNoMatch[0] : '',
                        direct: isDirect,
                        prices: prices,
                        fareHeaders: fareHeaders,
                    });
                });
                return flights;
            }""")
        except Exception as e:
            logger.error("Smartwings: DOM parse error: %s", e)
            return []

        if not raw:
            return []

        date_str = req.date_from.isoformat()
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for flight in raw:
            dep_time = flight.get("depTime", "")
            arr_time = flight.get("arrTime", "")
            duration_str = flight.get("duration", "")
            flight_no = flight.get("flightNo", "")
            is_direct = flight.get("direct", True)
            prices = flight.get("prices", [])
            fare_headers = flight.get("fareHeaders", [])

            if not dep_time or not prices:
                continue

            dep_dt = self._parse_time(date_str, dep_time)
            arr_dt = self._parse_time(date_str, arr_time)

            # Handle overnight flights
            if arr_dt < dep_dt:
                from datetime import timedelta
                arr_dt += timedelta(days=1)

            dur_secs = self._parse_duration(duration_str)
            if dur_secs == 0 and dep_dt and arr_dt:
                dur_secs = int((arr_dt - dep_dt).total_seconds())

            segment = FlightSegment(
                airline="QS",
                airline_name="Smartwings",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur_secs,
                cabin_class="economy",
            )
            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=max(dur_secs, 0),
                stopovers=0 if is_direct else 1,
            )

            # Create one offer per fare class (LITE/PLUS/FLEX)
            for i, price in enumerate(prices):
                fare_name = fare_headers[i] if i < len(fare_headers) else ""
                suffix = fare_name.lower() if fare_name else str(i)
                offer_key = f"{flight_no}_{date_str}_{suffix}"
                offer_id = f"qs_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}"

                offers.append(FlightOffer(
                    id=offer_id,
                    price=round(price, 2),
                    currency=req.currency or "EUR",
                    price_formatted=f"{price:.2f} EUR" + (f" ({fare_name})" if fare_name else ""),
                    outbound=route,
                    inbound=None,
                    airlines=["Smartwings"],
                    owner_airline="QS",
                    booking_url=booking_url,
                    is_locked=False,
                    source="smartwings_direct",
                    source_tier="free",
                ))

        return offers

    # ── Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _parse_time(date_str: str, time_str: str) -> datetime:
        """Combine date ISO string with HH:MM time."""
        try:
            return datetime.fromisoformat(f"{date_str}T{time_str}:00")
        except (ValueError, TypeError):
            return datetime(2000, 1, 1)

    @staticmethod
    def _parse_duration(dur_str: str) -> int:
        """Parse '02h35m' into seconds."""
        m = re.match(r"(\d+)h\s*(\d+)m", dur_str)
        if m:
            return int(m.group(1)) * 3600 + int(m.group(2)) * 60
        return 0

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float,
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Smartwings %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        search_hash = hashlib.md5(
            f"smartwings{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.smartwings.com/en/flights?from={req.origin}"
            f"&to={req.destination}&departure={dep}"
            f"&adults={req.adults}&children={req.children}&infants={req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"smartwings{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "EUR",
            offers=[],
            total_results=0,
        )
