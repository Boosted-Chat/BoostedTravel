"""
Smartwings CDP Chrome scraper — navigates smartwings.com homepage form
then parses the Amadeus FlexPricer results on book.smartwings.com.

Smartwings (IATA: QS) is the Czech Republic's largest airline group,
operating from Prague (PRG), Brno (BRQ), Ostrava (OSR) and Pardubice (PED)
to Mediterranean, Middle East and North Africa destinations.

Cloudflare WAF — requires browser session (real Chrome passes better).

Strategy (converted Mar 2026):
1. Launch real system Chrome via CDP (persistent, avoids launch overhead)
2. Navigate to smartwings.com/en homepage, wait for Cloudflare
3. Dismiss cookie consent banner
4. Click "One-way flight" button
5. Select origin/destination via JS click on [data-iata] elements
6. Set date via jQuery datepicker
7. Click Search -> redirects to book.smartwings.com Amadeus FlexPricer
8. Calendar page → click continue
9. Flights page → parse .bound-table-flightline elements
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

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min

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

# ── CDP Chrome singleton (headed, no --headless) ──────────────────────
_CDP_PORT = 9507
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".smartwings_chrome_profile"
)

_chrome_proc: subprocess.Popen | None = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch or reuse headed Chrome via CDP.

    Cloudflare on smartwings.com blocks headless Chrome and pages in
    Playwright-created contexts.  We launch Chrome HEADED (no --headless)
    off-screen and use browser.contexts[0] (the default context).
    """
    global _pw_instance, _cdp_browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _cdp_browser:
            try:
                if _cdp_browser.is_connected():
                    return _cdp_browser
            except Exception:
                pass
            _cdp_browser = None

        from playwright.async_api import async_playwright

        # Try connecting to existing Chrome on the port
        pw = None
        try:
            pw = await async_playwright().start()
            _cdp_browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            _pw_instance = pw
            logger.info("Smartwings: connected to existing Chrome on port %d", _CDP_PORT)
            return _cdp_browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        # Launch Chrome HEADED — no --headless (Cloudflare blocks headless)
        chrome = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)
        _chrome_proc = subprocess.Popen(
            [
                chrome,
                f"--remote-debugging-port={_CDP_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                *proxy_chrome_args(),
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
                "--window-position=-2400,-2400",
                "--window-size=1440,900",
                "https://www.smartwings.com/en",
            ],
            **stealth_popen_kwargs(),
        )
        _launched_procs.append(_chrome_proc)
        # Cloudflare Turnstile resolves when Chrome loads the page natively
        # BEFORE CDP/Playwright attaches — give it time.
        await asyncio.sleep(5.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _cdp_browser = await pw.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_CDP_PORT}"
        )
        logger.info("Smartwings: Chrome launched headed on CDP port %d (pid %d)",
                    _CDP_PORT, _chrome_proc.pid)
        return _cdp_browser


class SmartwingsConnectorClient:
    """Smartwings Playwright scraper — homepage form + Amadeus FPOW parsing."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        result = self._empty(req)
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._attempt_search(req, t0)
                if result.total_results > 0 or attempt == _MAX_ATTEMPTS:
                    break
                logger.warning("Smartwings: attempt %d got 0 offers, retrying", attempt)
            except Exception as e:
                logger.error("Smartwings: attempt %d error: %s", attempt, e)
                if attempt == _MAX_ATTEMPTS:
                    return self._empty(req)

        # --- RT: second search for inbound ---
        if req.return_from and result.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "return_from": None,
            })
            for attempt in range(1, _MAX_ATTEMPTS + 1):
                try:
                    ib_result = await self._attempt_search(ib_req, t0)
                    if ib_result.total_results > 0:
                        result.offers = self._combine_rt(
                            result.offers, ib_result.offers, req,
                        )
                        result.total_results = len(result.offers)
                        break
                except Exception:
                    pass

        if result.offers:
            segs = result.offers[0].outbound.segments if result.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, result.currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("QS")
        if not ref:
            return None
        cur = ref.get("currency", "EUR")
        carry_on = ref.get("carry_on")
        checked_bag = ref.get("checked_bag")
        seat = ref.get("seat")
        carry_on_note = ref.get("carry_on_note") or (
            "1 cabin bag included" if carry_on == 0.0
            else f"Carry-on add-on from ~{cur} {carry_on:.0f}" if carry_on is not None
            else None
        )
        checked_note = ref.get("checked_bag_note") or (
            "1 checked bag included" if checked_bag == 0.0
            else f"First checked bag from ~{cur} {checked_bag:.0f}" if checked_bag is not None
            else None
        )
        seat_note = (
            "Seat selection included" if seat == 0.0
            else f"Seat selection from ~{cur} {seat:.0f}" if seat is not None
            else None
        )
        return {
            "bags_from": carry_on,
            "bags_note": carry_on_note,
            "checked_bag_from": checked_bag,
            "checked_bag_note": checked_note,
            "seat_from": seat,
            "seat_note": seat_note,
            "currency": cur,
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_bag_note = ancillary.get("checked_bag_note")
        checked_bag_from = ancillary.get("checked_bag_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["cabin_bag"] = bags_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if checked_bag_note:
                offer.conditions["checked_bag"] = checked_bag_note
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["cabin_bag"] = bags_from
            if checked_bag_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["checked_bag"] = checked_bag_from
    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> FlightSearchResponse:
        browser = await _get_browser()

        # Use default context to keep Cloudflare clearance cookies warm
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        # Clean up extra pages from previous searches
        for p in context.pages[1:]:
            try:
                await p.close()
            except Exception:
                pass
        page = None
        try:
            page = context.pages[0] if context.pages else await context.new_page()

            # ── Step 1: Load homepage & pass Cloudflare ──
            logger.info("Smartwings: loading homepage for %s->%s on %s",
                        req.origin, req.destination, req.date_from)
            await auto_block_if_proxied(page)
            await page.goto(
                "https://www.smartwings.com/en",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            # Wait for Cloudflare challenge to resolve (cookies from first
            # launch should make subsequent passes very fast)
            cf_passed = False
            for _ in range(15):  # up to ~30 s
                title = await page.title()
                if "just a moment" not in title.lower():
                    cf_passed = True
                    break
                await asyncio.sleep(2)
            if not cf_passed:
                logger.warning("Smartwings: stuck on Cloudflare challenge")
                return self._empty(req)
            # Ensure form controls are present
            try:
                await page.wait_for_selector(
                    "input.route-from-text", timeout=10000,
                )
            except Exception:
                pass
            # Wait for dropdown airports to be populated by page JS
            try:
                await page.wait_for_selector(
                    ".route-from-select [data-iata]", timeout=8000,
                )
            except Exception:
                pass

            # ── Step 2: Cookie consent ──
            await self._dismiss_cookies(page)
            await asyncio.sleep(0.3)

            # ── Step 3: One-way flight ──
            try:
                ow_btn = page.locator("button").filter(has_text="One-way flight").first
                await ow_btn.click(timeout=3000)
            except Exception:
                pass
            await asyncio.sleep(0.3)

            # ── Step 4: Select airports via data-iata JS click ──
            origin_ok = await self._select_airport(page, "from", req.origin)
            if not origin_ok:
                logger.warning("Smartwings: failed to select origin %s", req.origin)
                return self._empty(req)
            await asyncio.sleep(0.5)

            dest_ok = await self._select_airport(page, "to", req.destination)
            if not dest_ok:
                logger.warning("Smartwings: failed to select destination %s", req.destination)
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
                    // Close any open datepicker overlay
                    document.querySelectorAll('.ui-datepicker').forEach(el => el.style.display = 'none');
                }""",
                date_str,
            )
            await asyncio.sleep(0.5)

            # ── Step 6: Click Search → book.smartwings.com ──
            search_btn = page.locator(".search-flight").first
            await search_btn.click(timeout=10000, force=True)
            logger.info("Smartwings: clicked Search, waiting for Amadeus redirect")

            # Wait for book.smartwings.com to load
            try:
                await page.wait_for_url("**/book.smartwings.com/**", timeout=20000)
            except Exception:
                await asyncio.sleep(3)
                if "book.smartwings.com" not in page.url:
                    logger.warning("Smartwings: did not redirect to booking page")
                    return self._empty(req)

            # ── Step 7: Calendar page → click continue ──
            try:
                await page.wait_for_selector(
                    "button:has-text('continue'), [class*='continue']",
                    timeout=15000,
                )
            except Exception:
                await asyncio.sleep(3)

            # The date should already be selected; click continue
            try:
                continue_btn = page.locator("button").filter(has_text="continue").first
                await continue_btn.click(timeout=5000)
            except Exception:
                logger.warning("Smartwings: could not click continue on calendar")
                return self._empty(req)

            # ── Step 8: Wait for flights page (FPOW) ──
            try:
                await page.wait_for_selector(
                    ".bound-table-flightline", timeout=20000,
                )
            except Exception:
                await asyncio.sleep(3)

            # ── Step 9: Parse flight results from DOM ──
            offers = await self._parse_flights_page(page, req)
            elapsed = time.monotonic() - t0
            if offers:
                return self._build_response(offers, req, elapsed)
            return self._empty(req)

        finally:
            # Keep default context alive for cookie persistence,
            # but navigate to blank to free page memory.
            if page:
                try:
                    await page.goto("about:blank", wait_until="commit", timeout=5000)
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
            # Smartwings uses CookieConsent lib with <a> tags, not <button>
            await page.evaluate("""() => {
                const accept = document.querySelector('.cc-allowall, .cc-btn.cc-allowall');
                if (accept) { accept.click(); return; }
                // Broader fallback: any element with agree/accept text
                const els = document.querySelectorAll(
                    '[class*="cc-"] a, [class*="cc-"] button, ' +
                    '[class*="cookie"] a, [class*="cookie"] button, ' +
                    '[class*="consent"] a, [class*="consent"] button'
                );
                for (const b of els) {
                    const t = (b.textContent || '').toLowerCase();
                    if (t.includes('accept') || t.includes('agree') || t.includes('souhlas')) {
                        b.click(); return;
                    }
                }
                // Last resort: remove consent overlays
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
            _qs_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")

            segment = FlightSegment(
                airline="QS",
                airline_name="Smartwings",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur_secs,
                cabin_class=_qs_cabin,
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
            f"smartwings{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
            offers=offers,
            total_results=len(offers),
        )

    def _combine_rt(
        self,
        ob_offers: list[FlightOffer],
        ib_offers: list[FlightOffer],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Cross-multiply OB x IB one-way offers into RT combos."""
        ob_sorted = sorted(ob_offers, key=lambda o: o.price)[:15]
        ib_sorted = sorted(ib_offers, key=lambda o: o.price)[:10]
        dep = req.date_from.strftime("%Y-%m-%d")
        ret = req.return_from.strftime("%Y-%m-%d")
        bk_url = (
            f"https://www.smartwings.com/en/flights?from={req.origin}"
            f"&to={req.destination}&departure={dep}&return={ret}"
            f"&adults={req.adults}&children={req.children}&infants={req.infants}"
        )
        combined: list[FlightOffer] = []
        for ob in ob_sorted:
            for ib in ib_sorted:
                total = round(ob.price + ib.price, 2)
                cur = ob.currency or ib.currency or "EUR"
                key = f"{ob.id}_{ib.id}"
                cid = f"qs_rt_{hashlib.md5(key.encode()).hexdigest()[:12]}"
                combined.append(FlightOffer(
                    id=cid,
                    price=total,
                    currency=cur,
                    price_formatted=f"{total:.2f} EUR",
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    airlines=["Smartwings"],
                    owner_airline="QS",
                    booking_url=bk_url,
                    is_locked=False,
                    source="smartwings_direct",
                    source_tier="free",
                ))
        combined.sort(key=lambda o: o.price)
        return combined

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
            f"smartwings{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "EUR",
            offers=[],
            total_results=0,
        )
