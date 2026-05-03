"""
SunExpress CDP Chrome scraper — homepage form fill + DOM extraction.

SunExpress (IATA: XQ) is a Turkish-German low-cost carrier, a joint venture
of Turkish Airlines and Lufthansa, operating from Turkey and Germany to
leisure destinations across Europe, Middle East and North Africa.

Strategy (converted Mar 2026):
1. Launch real system Chrome via CDP (persistent, avoids launch overhead)
2. Navigate to sunexpress.com/en-gb homepage (establishes session + cookies)
3. Dismiss OneTrust cookie banner ("Accept All Cookies" button)
4. Click "One-way" toggle
5. Fill origin/destination via combobox
6. Navigate calendar to target month, click day
7. Click "Search flights" → wait for /booking/select/
8. Extract flights from Angular SPA DOM
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

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import auto_block_if_proxied, get_default_proxy

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE", "en-AU"]
_TIMEZONES = [
    "Europe/Istanbul", "Europe/Berlin", "Europe/London",
    "Europe/Paris", "Europe/Vienna", "Europe/Zurich",
]

_TURKEY_AIRPORTS = {
    "IST", "SAW", "AYT", "ESB", "ADB", "DLM", "BJV", "TZX", "GZT",
    "VAN", "ERZ", "DIY", "SZF", "KYA", "MLX", "ASR", "EZS", "MQM",
    "HTY", "NAV", "KCM", "EDO", "ONQ", "BAL", "CKZ", "MSR", "IGD",
    "NKT", "GNY", "USQ", "DNZ", "ERC", "AOE", "KSY", "ISE", "YEI",
    "TEQ", "OGU", "BZI", "SFQ",
}

# ── Persistent browser context (headed to bypass Radware) ─────────────
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "sunexpress_pw_data")
_pw_instance = None
_pw_context = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """
    Persistent headed Chrome context — cookies survive across searches
    so the Radware challenge only needs to pass once.
    """
    global _pw_instance, _pw_context
    lock = _get_lock()
    async with lock:
        if _pw_context:
            try:
                # Check if still alive
                _pw_context.pages
                return _pw_context
            except Exception:
                _pw_context = None

        from playwright.async_api import async_playwright

        os.makedirs(_USER_DATA_DIR, exist_ok=True)
        _pw_instance = await async_playwright().start()

        _pw_context = await _pw_instance.chromium.launch_persistent_context(
            _USER_DATA_DIR,
            channel="chrome",
            headless=False,
            proxy=get_default_proxy(),
            args=[
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
            ],
            viewport={"width": 1366, "height": 768},
            locale="en-GB",
            timezone_id="Europe/Berlin",
            service_workers="block",
        )
        logger.info("SunExpress: persistent Chrome context ready")
        return _pw_context


class SunExpressConnectorClient:
    """SunExpress Playwright scraper — homepage form fill + DOM extraction."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        ob_offers = await self._search_ow(req)

        # --- RT: second OW search for inbound ---
        if req.return_from and ob_offers:
            ib_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "return_from": None,
            })
            ib_offers = await self._search_ow(ib_req)
            if ib_offers:
                ob_offers = self._combine_rt(ob_offers, ib_offers, req)

        elapsed = time.monotonic() - t0
        if ob_offers:
            segs = ob_offers[0].outbound.segments if ob_offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, ob_offers[0].currency or req.currency or "EUR"),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s→%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s→%s: %s", anc_origin, anc_dest, _anc_err)
            return self._build_response(ob_offers, req, elapsed)
        return self._empty(req)

    async def _search_ow(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Run one OW search via persistent Chrome form fill."""
        context = await _get_context()

        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)

            # ── Step 1: Load homepage (establishes session) ────────────
            logger.info("SunExpress: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.sunexpress.com/en-gb",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            # Handle Radware Bot Manager challenge (redirects to validate.perfdrive.com)
            if "perfdrive" in page.url or "validate" in page.url:
                logger.info("SunExpress: Radware challenge detected, waiting for auto-solve...")
                try:
                    await page.wait_for_url("**/sunexpress.com/**", timeout=20000)
                    await asyncio.sleep(2.0)
                    logger.info("SunExpress: Radware challenge passed, URL = %s", page.url)
                except Exception:
                    logger.warning("SunExpress: Radware challenge timeout (URL: %s)", page.url)
                    return []

            logger.info("SunExpress: page URL = %s", page.url)

            # ── Step 2: Dismiss cookie banner ──────────────────────────
            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)

            # ── Step 3: Fill search form ───────────────────────────────
            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("SunExpress: form fill failed")
                return []

            # ── Step 4: Navigate to results ─────────────────────────
            # Form may auto-submit after date + Escape; try waiting first
            try:
                await page.wait_for_url("**/booking/select/**", timeout=10000)
                logger.info("SunExpress: auto-navigated to results page")
            except Exception:
                # Handle Radware challenge if triggered by form submission
                if "perfdrive" in page.url or "validate" in page.url:
                    logger.info("SunExpress: Radware challenge on form submit, waiting...")
                    try:
                        await page.wait_for_url("**/sunexpress.com/**", timeout=20000)
                        await asyncio.sleep(2.0)
                        logger.info("SunExpress: Radware passed, URL = %s", page.url)
                    except Exception:
                        logger.warning("SunExpress: Radware challenge timeout on submit")
                        return []
                # If still not on results, try clicking search manually
                if "/booking/select" not in page.url:
                    logger.info("SunExpress: URL after form = %s", page.url)
                    try:
                        await self._click_search(page)
                        await page.wait_for_url("**/booking/select/**", timeout=30000)
                        logger.info("SunExpress: navigated to results page")
                    except Exception:
                        # Handle Radware again
                        if "perfdrive" in page.url or "validate" in page.url:
                            logger.info("SunExpress: Radware on search click, waiting...")
                            try:
                                await page.wait_for_url("**/booking/select/**", timeout=20000)
                            except Exception:
                                pass
                        if "/booking/select" not in page.url:
                            logger.warning("SunExpress: failed to reach results (URL: %s)", page.url)
                            return []

            # ── Step 5: Wait for DOM prices to load ────────────────────
            remaining = max(self.timeout - 15, 8)
            prices_loaded = False
            for _ in range(int(remaining)):
                await asyncio.sleep(1)
                has_prices = await page.evaluate("""() => {
                    const els = document.querySelectorAll('[class*="price"]');
                    return Array.from(els).some(e => /[1-9]/.test(e.textContent));
                }""")
                if has_prices:
                    prices_loaded = True
                    break

            if not prices_loaded:
                logger.warning("SunExpress: prices never loaded on results page")
                return []

            await asyncio.sleep(1.5)

            # ── Step 6: Extract flights from DOM ───────────────────────
            offers = await self._extract_from_dom(page, req)
            logger.info("SunExpress: extracted %d offers", len(offers))
            return offers if offers else []

        except Exception as e:
            logger.error("SunExpress Playwright error: %s", e)
            return []
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ── Cookie dismissal ───────────────────────────────────────────────

    async def _dismiss_cookies(self, page) -> None:
        try:
            btn = page.get_by_role(
                "button", name=re.compile(r"Accept All Cookies", re.IGNORECASE)
            )
            if await btn.count() > 0:
                await btn.first.click(timeout=3000)
                await asyncio.sleep(0.5)
                return
        except Exception:
            pass
        # JS fallback: remove OneTrust / cookie overlays
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="onetrust"], [id*="onetrust"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── Form filling ───────────────────────────────────────────────────

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        # 1. Set one-way
        await self._set_one_way(page)
        await asyncio.sleep(0.3)

        # 2. Fill origin — the destination picker opens automatically after selection
        ok = await self._fill_airport(page, "From", req.origin)
        if not ok:
            logger.warning("SunExpress: origin fill failed for %s", req.origin)
            return False
        await asyncio.sleep(1)

        # 3. Fill destination — picker should already be open from origin selection
        ok = await self._fill_airport(page, "To", req.destination)
        if not ok:
            logger.warning("SunExpress: destination fill failed for %s", req.destination)
            return False
        await asyncio.sleep(0.8)

        # 4. Fill date — calendar opens automatically after destination
        ok = await self._fill_date(page, req)
        if not ok:
            logger.warning("SunExpress: date fill failed")
            return False

        # 5. Dismiss passengers dialog if open (opens after date selection)
        await page.keyboard.press("Escape")
        await asyncio.sleep(1.0)

        return True

    async def _fill_airport(self, page, label: str, iata: str) -> bool:
        """Click the airport button, type IATA in the combobox, click the option.

        SunExpress uses a two-panel picker:
          - Left: country listbox (filters when typing)
          - Right: airport listbox with options like "Antalya ( Türkiye ) AYT"
        Key: must use press_sequentially (not fill) to keep the dropdown open.
        After selecting origin, the destination picker opens automatically.
        """
        try:
            # Check if the combobox is already visible (destination auto-opens after origin)
            cb = page.get_by_role("combobox").first
            cb_visible = False
            try:
                await cb.wait_for(state="visible", timeout=1500)
                cb_visible = True
            except Exception:
                pass

            if not cb_visible:
                # Click the From/To button to open the picker
                btn = page.get_by_role("button", name=re.compile(
                    rf"^{re.escape(label)}\b", re.IGNORECASE
                )).first
                if await btn.count() > 0:
                    await btn.click(timeout=5000)
                    logger.info("SunExpress: clicked %s button", label)
                else:
                    btn2 = page.locator(f'button:has-text("{label}")').first
                    if await btn2.count() > 0:
                        await btn2.click(timeout=5000)
                        logger.info("SunExpress: clicked %s button (fallback)", label)
                    else:
                        logger.warning("SunExpress: no %s button found", label)
                        return False
                await asyncio.sleep(1)

                # Re-acquire combobox after opening
                cb = page.get_by_role("combobox").first
                try:
                    await cb.wait_for(state="visible", timeout=3000)
                except Exception:
                    logger.warning("SunExpress: combobox not visible for %s/%s", label, iata)
                    return False

            # Type IATA code character-by-character in the combobox
            await cb.press_sequentially(iata, delay=80)
            await asyncio.sleep(1.5)

            # Click the matching airport option (format: "CityName ( Country ) IATA")
            opt = page.get_by_role("option", name=re.compile(
                rf"\b{re.escape(iata)}\b", re.IGNORECASE
            )).first
            if await opt.count() > 0:
                await opt.click(timeout=3000)
                logger.info("SunExpress: selected %s for %s", iata, label)
                return True

            logger.warning("SunExpress: no option found for %s/%s", label, iata)
        except Exception as e:
            logger.warning("SunExpress: airport fill error for %s/%s: %s", label, iata, e)
        return False

    async def _set_one_way(self, page) -> None:
        try:
            ow = page.get_by_text("One-way", exact=False).first
            if await ow.count() > 0:
                await ow.click(timeout=3000)
                return
        except Exception:
            pass

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        # Gridcell aria-labels use "D-M-YYYY" format (e.g. "15-4-2026")
        gridcell_label = f"{target.day}-{target.month}-{target.year}"
        # CSS attribute selector — avoids Playwright visibility checks on
        # gridcells that use visibility:hidden with visible children.
        selector = f'[role="gridcell"][aria-label="{gridcell_label}"]'

        try:
            await asyncio.sleep(1)

            # If calendar not open yet, try clicking the departure date field
            if await page.locator(selector).count() == 0:
                for fallback in [
                    page.get_by_role("textbox", name=re.compile(r"departure date", re.IGNORECASE)).first,
                    page.locator('[class*="date-control"], [class*="departure"]').first,
                ]:
                    if await fallback.count() > 0:
                        await fallback.click(timeout=3000)
                        logger.info("SunExpress: clicked departure date field")
                        await asyncio.sleep(1)
                        break

            # Navigate months until the target cell appears (max 12 clicks)
            for _ in range(12):
                if await page.locator(selector).count() > 0:
                    break
                next_btn = page.get_by_role("button", name="Next month")
                if await next_btn.count() > 0:
                    await next_btn.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    break

            # Click the target day cell — force=True bypasses visibility:hidden
            cell = page.locator(selector)
            if await cell.count() > 0:
                await cell.first.click(force=True, timeout=3000)
                logger.info("SunExpress: selected date %s", target.strftime("%Y-%m-%d"))
                await asyncio.sleep(0.5)
                return True

            logger.warning("SunExpress: gridcell %s not found in calendar", gridcell_label)
            return False
        except Exception as e:
            logger.warning("SunExpress: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        btn = page.get_by_role("button", name="Search flights")
        if await btn.count() > 0:
            await btn.first.click(timeout=5000)
            logger.info("SunExpress: clicked Search flights")
            return
        await page.locator("button[type='submit']").first.click(timeout=3000)

    # ── DOM extraction (results page) ──────────────────────────────────

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight cards from the SunExpress booking/select results page.

        Each flight is an <article> with a button whose accessible name contains:
        "Select flight ... Departure: HH:MM CityName IATA to Return: HH:MM CityName IATA
         Duration: Xh Ym Nonstop|N stop ..."
        Price is in a sibling element with "£ XX .YY" or "€ XX .YY" format.
        """
        raw = await page.evaluate(r"""() => {
            const results = [];
            const articles = document.querySelectorAll('article');
            for (const art of articles) {
                // Use the full article text — the button aria-label only has
                // departure info; arrival, duration, price are in sibling elements.
                const text = art.textContent || '';
                if (!text.includes('Departure:')) continue;

                // Normalize non-breaking spaces (\xa0) to regular spaces
                const t = text.replace(/\u00a0/g, ' ');

                // Extract departure/arrival times
                const depMatch = t.match(/Departure:\s*(\d{1,2}:\d{2})/);
                const arrMatch = t.match(/(?:Return|Arrival):\s*(\d{1,2}:\d{2})/);
                if (!depMatch || !arrMatch) continue;

                // Extract duration
                const durMatch = t.match(/Duration:\s*([\dhm\s]+)/);
                let durationSec = 0;
                if (durMatch) {
                    const h = durMatch[1].match(/(\d+)\s*h/);
                    const m = durMatch[1].match(/(\d+)\s*m/);
                    durationSec = (h ? parseInt(h[1]) * 3600 : 0) + (m ? parseInt(m[1]) * 60 : 0);
                }

                // Extract stops from button aria-label (more reliable)
                const btn = art.querySelector('button[class*="trigger"]') || art.querySelector('button');
                const label = btn ? (btn.getAttribute('aria-label') || '') : '';
                const stopMatch = label.match(/(\d+)\s*stop/);
                const nonstop = label.toLowerCase().includes('nonstop') || t.toLowerCase().includes('nonstop');
                const stops = stopMatch ? parseInt(stopMatch[1]) : (nonstop ? 0 : -1);

                // Extract IATA codes from text (3-letter uppercase near airport names)
                const iataMatches = t.match(/\b([A-Z]{3})\b/g) || [];
                // Filter to likely IATA codes (appear after city names)
                const depIata = iataMatches[0] || '';
                const arrIata = iataMatches.length > 1 ? iataMatches[1] : '';

                // Extract flight number (XQ followed by digits)
                const flightNo = (t.match(/\b(XQ\s*\d{3,4})\b/) || ['', ''])[1].replace(/\s/g, '');

                // Extract price — look for currency symbol/code followed by amount
                const priceText = art.textContent || '';
                const currSymbol = priceText.match(/[£€$]/);
                const currCode = priceText.match(/(?:GBP|EUR|USD|TRY)/);
                let currency = currCode ? currCode[0]
                    : currSymbol ? (currSymbol[0] === '£' ? 'GBP' : currSymbol[0] === '€' ? 'EUR' : 'USD')
                    : 'EUR';

                // Price with possible space before decimal: "96 .00", "82.69", "82 .69"
                const pMatch = priceText.match(/(?:[£€$]|GBP|EUR|USD|TRY)\s*([\d,]+)\s*\.?\s*(\d{2})/);
                if (!pMatch) continue;
                const price = parseFloat(pMatch[1].replace(/,/g, '') + '.' + pMatch[2]);
                if (isNaN(price) || price <= 0) continue;

                results.push({
                    depTime: depMatch[1], arrTime: arrMatch[1],
                    depIata, arrIata, flightNo,
                    duration: durationSec, stops, price, currency
                });
            }
            return results;
        }""")

        if not raw:
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        date_str = req.date_from.strftime("%Y-%m-%d")

        for i, flight in enumerate(raw):
            price = flight.get("price", 0)
            if not price or price <= 0:
                continue

            dep_time = flight.get("depTime", "00:00")
            arr_time = flight.get("arrTime", "00:00")
            dep_iata = flight.get("depIata") or req.origin
            arr_iata = flight.get("arrIata") or req.destination
            duration_sec = flight.get("duration", 0)
            stops = max(flight.get("stops", 0), 0)
            currency = flight.get("currency", req.currency or "EUR")
            flight_no = flight.get("flightNo", "")

            dep_dt = self._parse_dt(f"{date_str}T{dep_time}")
            arr_dt = self._parse_dt(f"{date_str}T{arr_time}")
            if arr_dt <= dep_dt:
                from datetime import timedelta
                arr_dt = arr_dt + timedelta(days=1)

            if duration_sec == 0 and dep_dt and arr_dt:
                duration_sec = max(int((arr_dt - dep_dt).total_seconds()), 0)

            _xq_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            segment = FlightSegment(
                airline="XQ", airline_name="SunExpress",
                flight_no=flight_no,
                origin=dep_iata, destination=arr_iata,
                departure=dep_dt, arrival=arr_dt,
                cabin_class=_xq_cabin,
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=duration_sec,
                stopovers=stops,
            )

            flight_key = f"XQ_{dep_iata}_{arr_iata}_{dep_time}_{price}"
            offers.append(FlightOffer(
                id=f"xq_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["SunExpress"],
                owner_airline="XQ",
                booking_url=booking_url,
                is_locked=False,
                source="sunexpress_direct",
                source_tier="free",
            ))

        return offers

    # ── Helpers ────────────────────────────────────────────────────────

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("SunExpress %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"sunexpress{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.sunexpress.com/en-gb/booking/select/"
            f"?origin1={req.origin}&destination1={req.destination}"
            f"&departure1={dep}&adult={getattr(req, 'adults', 1) or 1}"
            f"&child={getattr(req, 'children', 0) or 0}"
            f"&infant={getattr(req, 'infants', 0) or 0}"
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
            f"https://www.sunexpress.com/en-gb/booking/select/"
            f"?origin1={req.origin}&destination1={req.destination}"
            f"&departure1={dep}&origin2={req.destination}&destination2={req.origin}"
            f"&departure2={ret}&adult={getattr(req, 'adults', 1) or 1}"
        )
        combined: list[FlightOffer] = []
        for ob in ob_sorted:
            for ib in ib_sorted:
                total = round(ob.price + ib.price, 2)
                cur = ob.currency or ib.currency or "EUR"
                key = f"{ob.id}_{ib.id}"
                cid = f"xq_rt_{hashlib.md5(key.encode()).hexdigest()[:12]}"
                combined.append(FlightOffer(
                    id=cid,
                    price=total,
                    currency=cur,
                    price_formatted=f"{total:.2f} {cur}",
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    airlines=["SunExpress"],
                    owner_airline="XQ",
                    booking_url=bk_url,
                    is_locked=False,
                    source="sunexpress_direct",
                    source_tier="free",
                ))
        combined.sort(key=lambda o: o.price)
        return combined

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        # SunExpress XQ — Essential fare: cabin bag 8 kg free, checked bag add-on from 10 EUR
        return {
            "checked_bag_note": "checked bag 20 kg not included – add-on from ~10 EUR",
            "bags_note": "cabin bag 8 kg included free (Essential fare)",
            "seat_note": "seat selection add-on from ~5 EUR",
            "checked_bag_from": 10.0,
            "currency": "EUR",
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        checked_bag_note = ancillary.get("checked_bag_note")
        bags_note = ancillary.get("bags_note")
        seat_note = ancillary.get("seat_note")
        checked_bag_from = ancillary.get("checked_bag_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if checked_bag_note:
                offer.conditions["checked_bag"] = checked_bag_note
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if checked_bag_from is not None:
                offer.bags_price["checked_bag"] = checked_bag_from

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"sunexpress{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
        )
