"""
Etihad (EY) CDP Chrome connector — form fill + calendar pricing API interception.

Etihad's homepage search widget calls ada-services/bff-calendar-pricing/
service/instant-search/v2/fetch-prices — a POST endpoint behind Akamai WAF.
Direct API calls (even from browser JS context) get 403 on replay.
The ONLY reliable path is form-triggered requests.

Strategy (CDP Chrome + response interception):
1. Launch REAL Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP. Context persists across searches.
3. Each search: new page → intercept → homepage → dismiss OneTrust
   cookie banner → fill form → click search.
4. Capture POST fetch-prices response via page.on("response").
5. Parse pricePerDay → FlightOffer for requested departure date.
6. Build booking deep-link URL.

Calendar pricing returns the cheapest round-trip price per day for ~7
months. Since we do one-way searches, we divide by 2 as estimate
(Etihad's homepage doesn't have a one-way toggle).

API details (discovered Mar 2026):
  POST /ada-services/bff-calendar-pricing/service/instant-search/v2/fetch-prices
  Body: {originAirportCode, destinationAirportCode, cabinClass, tripType,
         passengerTypeCode, departureDate, tripDuration, ...}
  Response: {currency, pricePerDay: [{YYYYMM: [{DD: {price, miles, flags}}]}],
             monthAggregatePrice: [{YYYYMM: {lowestPrice, highestPrice}}]}
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
from datetime import datetime
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied, inject_stealth_js

# Map IATA airport/city codes to Etihad locale codes.
# The calendar pricing API validates origin country against the site locale.
_ORIGIN_TO_LOCALE: dict[str, str] = {
    # UAE
    "AUH": "en-ae", "DXB": "en-ae", "SHJ": "en-ae",
    # Saudi Arabia
    "RUH": "en-sa", "JED": "en-sa", "DMM": "en-sa",
    # India
    "DEL": "en-in", "BOM": "en-in", "BLR": "en-in", "HYD": "en-in",
    "MAA": "en-in", "CCU": "en-in", "AMD": "en-in", "COK": "en-in",
    # UK
    "LHR": "en-gb", "LGW": "en-gb", "MAN": "en-gb", "EDI": "en-gb", "LON": "en-gb",
    # US
    "JFK": "en-us", "IAD": "en-us", "ORD": "en-us", "LAX": "en-us", "SFO": "en-us",
    "EWR": "en-us", "DFW": "en-us", "NYC": "en-us",
    # Australia
    "SYD": "en-au", "MEL": "en-au", "BNE": "en-au", "PER": "en-au",
    # France
    "CDG": "en-fr", "ORY": "en-fr", "PAR": "en-fr",
    # Germany
    "FRA": "en-de", "MUC": "en-de", "BER": "en-de", "DUS": "en-de",
    # Egypt
    "CAI": "en-eg",
    # Pakistan
    "ISB": "en-pk", "KHI": "en-pk", "LHE": "en-pk",
    # Japan
    "NRT": "en-jp", "HND": "en-jp", "TYO": "en-jp",
    # South Korea
    "ICN": "en-kr", "SEL": "en-kr",
    # Thailand
    "BKK": "en-th",
    # Singapore
    "SIN": "en-sg",
    # Malaysia
    "KUL": "en-my",
    # Indonesia
    "CGK": "en-id",
    # Philippines
    "MNL": "en-ph",
    # Qatar
    "DOH": "en-qa",
    # Kuwait
    "KWI": "en-kw",
    # Bahrain
    "BAH": "en-bh",
    # Oman
    "MCT": "en-om",
    # Jordan
    "AMM": "en-jo",
    # Kenya
    "NBO": "en-ke",
    # South Africa
    "JNB": "en-za", "CPT": "en-za",
}

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
_DEBUG_PORT = 9505
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".etihad_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (headed — Akamai blocks headless)."""
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        # Try connecting to existing Chrome on the port
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("Etihad: connected to existing Chrome on port %d", _DEBUG_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

            # Launch Chrome HEADED (no --headless) — Akamai blocks headless.
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
                "--window-position=-2400,-2400",
                "--window-size=1400,900",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.0)

            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            logger.info(
                "Etihad: Chrome launched headed on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _dismiss_overlays(page) -> None:
    """Remove OneTrust cookie banner and any blocking overlays."""
    # Click standard accept buttons
    for selector in [
        "#onetrust-accept-btn-handler",
        "button#accept-recommended-btn-handler",
    ]:
        try:
            btn = page.locator(selector)
            if await btn.count() > 0 and await btn.first.is_visible(timeout=1000):
                await btn.first.click(timeout=3000)
                logger.info("Etihad: clicked cookie accept %s", selector)
                await asyncio.sleep(0.5)
                return
        except Exception:
            continue

    # Fallback: try by text
    for text in ["Accept", "Accept all", "Accept All Cookies", "I agree", "OK"]:
        try:
            btn = page.get_by_role("button", name=text)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                logger.info("Etihad: clicked cookie button '%s'", text)
                await asyncio.sleep(0.5)
                return
        except Exception:
            continue

    # Force-remove OneTrust elements via JS
    try:
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter, #onetrust-banner-sdk'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


async def _reset_profile():
    """Wipe Chrome profile when Akamai flags the session."""
    global _browser, _context, _pw_instance, _chrome_proc
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        pid = _chrome_proc.pid
        try:
            _chrome_proc.kill()
        except Exception:
            pass
        try:
            _chrome_proc.wait(timeout=5)
        except Exception:
            try:
                import signal
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    await asyncio.sleep(0.5)
    if os.path.isdir(_USER_DATA_DIR):
        for retry in range(3):
            try:
                shutil.rmtree(_USER_DATA_DIR)
                logger.info("Etihad: deleted stale Chrome profile")
                break
            except Exception:
                if retry < 2:
                    await asyncio.sleep(1.0)


class EtihadConnectorClient:
    """Etihad CDP Chrome connector — form fill + calendar pricing interception."""

    def __init__(self, timeout: float = 35.0):
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
        if ob_result.offers:
            segs = ob_result.offers[0].outbound.segments if ob_result.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, ob_result.currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        # Etihad includes checked bag on Economy fares.
        return {
            "bags_note": "1 checked bag included (23 kg Economy). Carry-on 7 kg included.",
            "seat_note": "Seat selection: standard free at check-in. Preferred/extra-legroom from USD 18.",
            "bags_from": None,
            "currency": currency,
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_price")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["carry_on"] = bags_from
            if checked_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["checked_bag"] = checked_from

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        # NOTE: Do NOT inject stealth JS here — Akamai detects the navigator/canvas
        # patches and returns 403.  Real Chrome via CDP already passes bot checks.
        await auto_block_if_proxied(page)

        try:
            # Determine locale from origin for correct market context
            locale = _ORIGIN_TO_LOCALE.get(req.origin, "en-ae")
            logger.info("Etihad: loading homepage for %s→%s (locale=%s)", req.origin, req.destination, locale)
            await page.goto(
                f"https://www.etihad.com/{locale}/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            # Wait for Akamai bot management JS to complete its challenge
            # (fresh sessions need 5+ seconds for _abck cookie to be set)
            await asyncio.sleep(5.0)

            # Dismiss cookie overlay
            await _dismiss_overlays(page)
            await asyncio.sleep(0.5)

            # Etihad API needs real airport IATA codes, not city codes like LON
            from .airline_routes import get_city_airports
            origins = get_city_airports(req.origin)
            destinations = get_city_airports(req.destination)
            # Use first airport from each city (e.g. LON → LHR, NYC → JFK)
            api_origin = origins[0] if origins else req.origin
            api_dest = destinations[0] if destinations else req.destination

            # Fill form to establish Akamai trust, then intercept calendar pricing API response
            # (Direct fetch() from fresh sessions gets 403 — Akamai requires natural page interaction)
            fill_req = req.model_copy(update={"origin": api_origin, "destination": api_dest})
            ok = await self._fill_form(page, fill_req)
            if not ok:
                logger.warning("Etihad: form fill failed for %s→%s, proceeding anyway", api_origin, api_dest)

            # Set up response interception for the calendar pricing API
            api_result_future: asyncio.Future = asyncio.get_event_loop().create_future()

            async def _on_calendar_response(response):
                if "fetch-prices" in response.url and not api_result_future.done():
                    try:
                        if response.status == 200:
                            data = await response.json()
                            api_result_future.set_result(data)
                        else:
                            text = await response.text()
                            api_result_future.set_result({"_error": response.status, "_text": text[:300]})
                    except Exception as e:
                        if not api_result_future.done():
                            api_result_future.set_result({"_error": -1, "_msg": str(e)})

            page.on("response", _on_calendar_response)

            # Click the "Travelling when?" date card to trigger the calendar pricing API
            try:
                await page.locator("div.ey-fsp-stat--guest-and-date[role='tab']").last.click(timeout=5000)
                logger.info("Etihad: clicked date card, waiting for calendar pricing API...")
            except Exception as e:
                logger.warning("Etihad: date card click failed: %s, trying fallback", e)
                # Fallback: try any date-related element
                try:
                    await page.evaluate("""() => {
                        const els = document.querySelectorAll('[class*="guest-and-date"], [class*="date"]');
                        for (const el of els) {
                            if ((el.textContent || '').includes('Travelling') && el.offsetHeight > 0) {
                                el.click(); return true;
                            }
                        }
                        return false;
                    }""")
                except Exception:
                    pass

            # Wait for the intercepted response (up to 10 seconds)
            try:
                result = await asyncio.wait_for(api_result_future, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Etihad: calendar pricing API did not fire within 10s")
                result = None

            page.remove_listener("response", _on_calendar_response)

            if not result or result.get("_error"):
                err = result.get("_error", "?") if result else "null"
                txt = result.get("_text", "") if result else ""
                logger.warning("Etihad: API returned error %s (using %s→%s): %s", err, api_origin, api_dest, txt[:200])
                if err == 403:
                    logger.warning("Etihad: Akamai flagged, resetting profile")
                    await _reset_profile()
                return self._empty(req)

            has_ppd = bool(result.get("pricePerDay"))
            has_agg = bool(result.get("monthAggregatePrice"))
            if not has_ppd and not has_agg:
                logger.warning("Etihad: no pricing data in response (keys=%s)", list(result.keys())[:10])
                return self._empty(req)

            currency = result.get("currency", "AED")
            if has_ppd:
                offers = self._parse_calendar(result, req, currency)
                if not offers and has_agg:
                    offers = self._parse_month_aggregate(result, req, currency)
            else:
                offers = self._parse_month_aggregate(result, req, currency)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "Etihad %s→%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"etihad{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
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
            logger.error("Etihad CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Form fill
    # ------------------------------------------------------------------

    async def _fill_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill Etihad search form: origin + destination (no date — date card click triggers API separately)."""
        # Switch to One-way tab (Etihad defaults to Round trip)
        try:
            ow_tab = page.locator("li[role='tab']").filter(has_text="One-way")
            if await ow_tab.count() > 0:
                await ow_tab.click(timeout=3000)
                await asyncio.sleep(0.5)
        except Exception:
            pass

        # Origin
        ok = await self._fill_airport(page, "#fsporigin", req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        # Destination
        ok = await self._fill_airport(page, "#fspdestination", req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        return True

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill departure date in Etihad's date picker."""
        try:
            dt = req.date_from if hasattr(req.date_from, 'strftime') else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False

        target_day = str(dt.day)
        target_month = dt.strftime("%B")   # e.g. "June"
        target_year = str(dt.year)

        try:
            # Click departure date field to open calendar
            # Etihad uses a styled div[role="tab"] with text "Travelling when?"
            date_click = await page.evaluate("""() => {
                // Primary: Etihad's date card is a div with role=tab containing "Travelling when?"
                const tabs = document.querySelectorAll('div[role="tab"], [role="tab"]');
                for (const t of tabs) {
                    const text = (t.textContent || '').toLowerCase();
                    if (text.includes('travelling when') || text.includes('when') || text.includes('date')) {
                        if (t.offsetHeight > 0) { t.click(); return 'tab:' + text.trim().substring(0, 40); }
                    }
                }
                // Fallback: div with dates class
                const dateCards = document.querySelectorAll('.ey-fsp-stat--guest-and-date, .ey-fsp-stat--field-icon.dates, [class*="fsp"][class*="date"]');
                for (const el of dateCards) {
                    const text = (el.textContent || '').toLowerCase();
                    if ((text.includes('when') || text.includes('date') || el.classList.contains('dates')) && el.offsetHeight > 0) {
                        el.click();
                        return 'class:' + (el.className || '').substring(0, 50);
                    }
                }
                // Previous selectors as last resort
                const selectors = [
                    '#fspdate', '#departure-date', '#fspdeparture',
                    'input[placeholder*="Departure"]', 'input[placeholder*="departure"]',
                    '[data-testid*="departure-date"]', '[data-testid*="date"]',
                    '.fsp-date-picker', '.departure-date',
                ];
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el && el.offsetHeight > 0) { el.click(); return sel; }
                }
                // Try clicking elements with "Departure" label
                const labels = document.querySelectorAll('label, span, div');
                for (const l of labels) {
                    const t = (l.textContent || '').toLowerCase().trim();
                    if ((t === 'departure' || t === 'departing' || t.includes('depart'))
                        && l.offsetHeight > 0) {
                        l.click(); return 'label:' + t;
                    }
                }
                return null;
            }""")

            if date_click:
                logger.info("Etihad: opened date picker via %s", date_click)
                await asyncio.sleep(1.5)
            else:
                logger.warning("Etihad: could not find departure date field")
                return False

            # Navigate calendar to target month — click "next" until visible
            for _ in range(14):
                found = await page.evaluate("""(args) => {
                    const [targetMonth, targetYear] = args;
                    const check = targetMonth.toLowerCase() + ' ' + targetYear;
                    // Check calendar headers
                    const headers = document.querySelectorAll(
                        '.calendar-header, .month-name, [class*="calendar"] [class*="month"], ' +
                        '[class*="datepicker"] [class*="header"], th[class*="month"], ' +
                        '.DayPicker-Caption, [class*="CalendarMonth"]'
                    );
                    for (const h of headers) {
                        const t = (h.textContent || '').toLowerCase();
                        if (t.includes(targetMonth.toLowerCase()) && t.includes(targetYear)) {
                            return true;
                        }
                    }
                    // Check any element containing month+year text
                    const all = document.body.innerText.toLowerCase();
                    return all.includes(check);
                }""", [target_month, target_year])

                if found:
                    break

                # Click next month button
                clicked = await page.evaluate("""() => {
                    const btns = document.querySelectorAll(
                        'button[aria-label*="next"], button[aria-label*="Next"], ' +
                        '.next-month, [class*="next"], .calendar-forward, ' +
                        '[data-testid*="next"], .DayPicker-NavButton--next'
                    );
                    for (const b of btns) {
                        if (b.offsetHeight > 0) { b.click(); return true; }
                    }
                    return false;
                }""")
                if not clicked:
                    break
                await asyncio.sleep(0.5)

            # Click the target day
            clicked = await page.evaluate("""(args) => {
                const [targetMonth, targetYear, day] = args;
                // Try finding day cells with the target number
                const cells = document.querySelectorAll(
                    'td[data-day], button[data-day], .calendar-day, ' +
                    '[class*="CalendarDay"], [class*="day"], ' +
                    '[role="gridcell"], td button, .DayPicker-Day'
                );
                for (const c of cells) {
                    const dayText = (c.getAttribute('data-day') || c.textContent || '').trim();
                    if (dayText === day && c.offsetHeight > 0 && !c.classList.contains('disabled')) {
                        c.click();
                        return true;
                    }
                }
                return false;
            }""", [target_month, target_year, target_day])

            if clicked:
                logger.info("Etihad: selected departure date %s", dt.strftime("%Y-%m-%d"))
                await asyncio.sleep(1.0)
                return True
            else:
                logger.warning("Etihad: could not click day %s in calendar", target_day)
                return False

        except Exception as e:
            logger.warning("Etihad: date fill error: %s", e)
            return False

    async def _fill_airport(self, page, selector: str, iata: str) -> bool:
        """Fill an airport typeahead field and select first match (React Bootstrap Typeahead)."""
        try:
            field = page.locator(selector)
            await field.click(timeout=5000)
            await asyncio.sleep(0.3)
            # Clear existing value (triple-click to select all, then clear)
            await field.click(click_count=3)
            await asyncio.sleep(0.1)
            await field.fill("")
            await asyncio.sleep(0.3)
            await field.type(iata, delay=80)
            await asyncio.sleep(2.0)

            # Try clicking the first dropdown item (React Bootstrap Typeahead)
            for sel in [
                ".rbt-menu .dropdown-item",
                "[role='option']",
                ".rbt-menu li",
            ]:
                try:
                    opt = page.locator(sel).first
                    if await opt.count() > 0 and await opt.is_visible(timeout=1000):
                        await opt.click(timeout=2000)
                        logger.info("Etihad: selected %s via %s", iata, sel)
                        await asyncio.sleep(0.5)
                        return True
                except Exception:
                    continue

            # Fallback: keyboard navigation
            await field.press("ArrowDown")
            await asyncio.sleep(0.2)
            await field.press("Enter")
            await asyncio.sleep(0.5)

            value = await field.input_value()
            if value and len(value) > 2:
                logger.info("Etihad: filled %s → %s", selector, value)
                return True

            logger.warning("Etihad: could not fill airport %s for %s", selector, iata)
            return False

        except Exception as e:
            logger.warning("Etihad: airport fill error %s: %s", selector, e)
            return False

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_calendar(
        self, data: dict, req: FlightSearchRequest, currency: str,
    ) -> list[FlightOffer]:
        """Parse pricePerDay into FlightOffers for the requested date."""
        offers: list[FlightOffer] = []
        ppd_list = data.get("pricePerDay", [])
        if not ppd_list:
            return offers

        ppd = ppd_list[0] if isinstance(ppd_list, list) else ppd_list

        # Target month/day from request
        try:
            dt = req.date_from if hasattr(req.date_from, 'strftime') else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            logger.warning("Etihad: invalid date_from: %s", req.date_from)
            return offers

        month_key = dt.strftime("%Y%m")  # e.g. "202604"
        day_key = str(dt.day)  # e.g. "15" (no leading zero)

        month_data = ppd.get(month_key)
        if not month_data:
            logger.info("Etihad: no price data for month %s", month_key)
            return offers

        # Each month_data entry is a dict with one key (day number)
        for day_entry in month_data:
            if not isinstance(day_entry, dict):
                continue
            for d, info in day_entry.items():
                if d != day_key:
                    continue

                price = self._parse_price(info.get("price", "0"))
                if price <= 0:
                    continue

                if req.return_from:
                    use_price = price  # full round-trip price
                else:
                    use_price = round(price * 0.55, 2)  # estimate one-way

                offer = self._build_offer(
                    req, use_price, currency, dt, info
                )
                if offer:
                    offers.append(offer)
                return offers  # found our date

        logger.info("Etihad: no price for day %s in month %s", day_key, month_key)
        return offers

    def _parse_month_aggregate(
        self, data: dict, req: FlightSearchRequest, currency: str,
    ) -> list[FlightOffer]:
        """Parse monthAggregatePrice into a FlightOffer for the requested date's month."""
        offers: list[FlightOffer] = []
        agg_list = data.get("monthAggregatePrice", [])
        if not agg_list:
            return offers

        try:
            dt = req.date_from if hasattr(req.date_from, 'strftime') else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            logger.warning("Etihad: invalid date_from: %s", req.date_from)
            return offers

        month_key = dt.strftime("%Y%m")  # e.g. "202604"

        # Find the target month in the aggregate list
        for entry in agg_list:
            if not isinstance(entry, dict):
                continue
            month_data = entry.get(month_key)
            if not month_data:
                continue

            price = self._parse_price(month_data.get("lowestPrice", "0"))
            if price <= 0:
                continue

            if not req.return_from:
                price = round(price * 0.55, 2)  # estimate one-way from RT aggregate

            offer = self._build_offer(req, price, currency, dt, month_data)
            if offer:
                offers.append(offer)
            return offers

        # Target month not found — use any available month's lowest as fallback
        for entry in agg_list:
            if not isinstance(entry, dict):
                continue
            for mk, month_data in entry.items():
                if not isinstance(month_data, dict):
                    continue
                price = self._parse_price(month_data.get("lowestPrice", "0"))
                if price <= 0:
                    continue
                if not req.return_from:
                    price = round(price * 0.55, 2)
                offer = self._build_offer(req, price, currency, dt, month_data)
                if offer:
                    offers.append(offer)
                return offers

        logger.info("Etihad: no aggregate price for month %s", month_key)
        return offers

    def _build_offer(
        self,
        req: FlightSearchRequest,
        price: float,
        currency: str,
        dep_dt,
        info: dict,
    ) -> Optional[FlightOffer]:
        """Build a FlightOffer from calendar pricing data."""
        dep_date = dep_dt if not hasattr(dep_dt, 'date') else dep_dt.date()
        offer_id = hashlib.md5(
            f"ey_{req.origin}_{req.destination}_{dep_date}_{price}".encode()
        ).hexdigest()[:12]

        # Ensure dep_dt is a datetime (not just date)
        if not isinstance(dep_dt, datetime):
            dep_dt = datetime(dep_dt.year, dep_dt.month, dep_dt.day)

        segment = FlightSegment(
            airline="EY",
            airline_name="Etihad Airways",
            flight_no="EY",
            origin=req.origin,
            destination=req.destination,
            departure=dep_dt,
            arrival=dep_dt,  # time unknown from calendar API
            duration_seconds=0,
            cabin_class="economy",
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=0,
            stopovers=0,
        )

        booking_url = self._booking_url(req)

        inbound = None
        if req.return_from:
            try:
                rdt = req.return_from if isinstance(req.return_from, datetime) else datetime.strptime(str(req.return_from), "%Y-%m-%d")
            except (ValueError, TypeError):
                rdt = dep_dt
            ib_seg = FlightSegment(
                airline="EY",
                airline_name="Etihad Airways",
                flight_no="EY",
                origin=req.destination,
                destination=req.origin,
                departure=rdt if isinstance(rdt, datetime) else datetime(rdt.year, rdt.month, rdt.day),
                arrival=rdt if isinstance(rdt, datetime) else datetime(rdt.year, rdt.month, rdt.day),
                duration_seconds=0,
                cabin_class="economy",
            )
            inbound = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)

        return FlightOffer(
            id=f"ey_{'rt_' if req.return_from else ''}{offer_id}",
            price=price,
            currency=currency,
            price_formatted=f"{price:,.0f} {currency}",
            outbound=route,
            inbound=inbound,
            airlines=["Etihad Airways"],
            owner_airline="EY",
            booking_url=booking_url,
            is_locked=False,
            source="etihad_direct",
            source_tier="free",
        )

    @staticmethod
    def _parse_price(price_str: str) -> float:
        """Parse comma-formatted price string like '3,310' → 3310.0."""
        try:
            return float(price_str.replace(",", ""))
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        """Build Etihad booking deep-link."""
        try:
            dt = req.date_from if hasattr(req.date_from, 'strftime') else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_str = dt.strftime("%d-%m-%Y")
        except (ValueError, TypeError):
            date_str = ""
        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0
        trip = "return" if req.return_from else "oneway"
        _ey_booking_cabin = {"M": "Economy", "W": "Economy", "C": "Business", "F": "First"}.get(req.cabin_class or "M", "Economy")
        url = (
            f"https://www.etihad.com/en/book/flights"
            f"?from={req.origin}&to={req.destination}"
            f"&departdate={date_str}"
            f"&adult={adults}&child={children}&infant={infants}"
            f"&class={_ey_booking_cabin}&trip={trip}"
        )
        if req.return_from:
            try:
                rdt = req.return_from if hasattr(req.return_from, 'strftime') else datetime.strptime(str(req.return_from), "%Y-%m-%d")
                url += f"&returndate={rdt.strftime('%d-%m-%Y')}"
            except (ValueError, TypeError):
                pass
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"etihad{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
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
                    id=f"rt_etih_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
