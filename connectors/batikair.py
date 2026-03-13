"""
Batik Air Playwright scraper — browser form-fill + DOM extraction.

Batik Air (IATA: ID) is an Indonesian full-service carrier (Lion Air Group).
Website: www.batikair.com.my (Malaysia hub).

Strategy:
1. Navigate to batikair.com.my homepage (Cloudflare challenge auto-resolves)
2. Remove popup overlays (#__st_overlay_popup, ant-modal-wrap)
3. Click "One-way" radio
4. Fill Ant Design combobox airports (type IATA → JS click [title] option)
5. Pick date from Ant Design calendar table
6. Click search → navigates to /book/flight-search
7. Extract flight cards from results DOM

API note: search.batikair.com.my/flightrr_api/api/get/Flights uses
encrypted payloads {"payload":"<encrypted>"} — not replayable without
reverse-engineering the JS. DOM extraction is the reliable approach.

Discovered via MCP Playwright probing, Mar 2026.
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
]

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9458
_chrome_proc = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Connect to a real Chrome instance via CDP (launched once, reused)."""
    global _chrome_proc, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from connectors.browser import get_or_launch_cdp
        _user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-cdp-batikair")
        _browser, _chrome_proc = await get_or_launch_cdp(_CDP_PORT, _user_data)
        logger.info("BatikAir: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _browser


class BatikAirConnectorClient:
    """Batik Air scraper — Playwright form-fill + DOM card extraction."""

    def __init__(self, timeout: float = 50.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="Asia/Jakarta",
            service_workers="block",
        )
        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            # --- API response interception (in case response is unencrypted) ---
            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and (
                        "flightrr_api/api/get/flights" in url
                        or "availability" in url
                        or "/api/flights" in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, (dict, list)):
                                captured_data["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("BatikAir: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto("https://www.batikair.com.my/",
                            wait_until="domcontentloaded", timeout=30000)

            # Wait for Cloudflare challenge + page load
            try:
                await page.wait_for_selector(
                    "input[role='combobox']",
                    timeout=25000,
                )
            except Exception:
                await asyncio.sleep(8)

            # Remove overlay popups
            await self._remove_overlays(page)
            await asyncio.sleep(0.5)

            # One-way
            await self._set_one_way(page)
            await asyncio.sleep(0.3)
            await self._remove_overlays(page)

            # Wait for the full form to render (need 3+ comboboxes: misc + origin + destination)
            for _ in range(15):
                count = await page.locator("input[role='combobox']").count()
                if count >= 3:
                    break
                await asyncio.sleep(0.5)
            # Origin
            ok = await self._fill_airport(page, req.origin, is_origin=True)
            if not ok:
                logger.warning("BatikAir: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Destination
            ok = await self._fill_airport(page, req.destination, is_origin=False)
            if not ok:
                logger.warning("BatikAir: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("BatikAir: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)
            await self._remove_overlays(page)

            # Search
            await self._click_search(page)
            # Wait for results page
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await page.wait_for_url("**/book/flight-search**", timeout=remaining * 1000)
            except Exception:
                pass
            await asyncio.sleep(2)

            # Try API capture first
            try:
                await asyncio.wait_for(api_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

            if captured_data.get("json"):
                offers = self._parse_api_response(captured_data["json"], req)
                if offers:
                    return self._build_response(offers, req, time.monotonic() - t0)

            # Fallback: DOM extraction from results page
            offers = await self._extract_from_dom(page, req)
            return self._build_response(offers, req, time.monotonic() - t0)

        except Exception as e:
            logger.error("BatikAir error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ── Overlay removal ──

    async def _remove_overlays(self, page) -> None:
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '#__st_overlay_popup, [smtmsgid], .ant-modal-wrap, .ant-modal-root, '
                    + '.ant-modal-mask, [class*="cookie"], [id*="cookie"], '
                    + '[class*="consent"], [id*="consent"], [class*="onetrust"]'
                ).forEach(el => el.remove());
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── One-way toggle ──

    async def _set_one_way(self, page) -> None:
        try:
            radio = page.get_by_role("radio", name=re.compile(r"one.?way", re.IGNORECASE))
            if await radio.count() > 0:
                parent = page.locator("[class*='ant-radio-wrapper']").filter(has_text=re.compile(r"one.?way", re.IGNORECASE)).first
                if await parent.count() > 0:
                    await parent.click(timeout=3000)
                    return
                await radio.first.click(timeout=3000)
                return
        except Exception:
            pass
        # Fallback: click text
        for label in ["One-way", "One Way", "One way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    return
            except Exception:
                continue

    # ── Airport fill (Ant Design Select combobox) ──

    async def _fill_airport(self, page, iata: str, is_origin: bool) -> bool:
        """Fill Ant Design Select combobox — force-click, keyboard type, JS-click option."""
        try:
            # The form has 3+ comboboxes: index 0 is a non-airport control,
            # index 1 = origin, index 2 = destination (discovered empirically)
            all_combos = page.locator("input[role='combobox']")
            count = await all_combos.count()

            # Find the right combobox by checking parent context
            target_idx = None
            if count >= 3:
                # index 1 = origin, index 2 = destination
                target_idx = 1 if is_origin else 2
            elif count >= 2:
                target_idx = 0 if is_origin else 1
            else:
                logger.debug("BatikAir: only %d comboboxes found", count)
                return False

            cb_input = all_combos.nth(target_idx)

            # Force-click to open dropdown (bypasses Ant Design overlay interception)
            await cb_input.click(force=True, timeout=5000)
            await asyncio.sleep(0.5)

            # Clear existing text with keyboard shortcuts
            await page.keyboard.press("Control+a")
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.3)

            # Type IATA character by character (triggers React keyboard events)
            await cb_input.type(iata, delay=100)
            await asyncio.sleep(1.5)

            # JS-click the dropdown option by title attribute
            clicked = await page.evaluate("""(iata) => {
                const opts = document.querySelectorAll('[title]');
                for (const opt of opts) {
                    const t = opt.title || '';
                    if (t.includes('(' + iata + ')')) {
                        opt.click();
                        return t;
                    }
                }
                // Try option role elements
                const options = document.querySelectorAll('[role="option"]');
                for (const opt of options) {
                    if (opt.textContent.includes(iata)) {
                        opt.click();
                        return opt.textContent.substring(0, 80);
                    }
                }
                return null;
            }""", iata)

            if clicked:
                logger.debug("BatikAir: selected airport %s → %s", iata, clicked)
                return True

            # Last resort: Enter key to accept first suggestion
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.3)
            logger.debug("BatikAir: airport %s — pressed Enter fallback", iata)
            return True

        except Exception as e:
            logger.debug("BatikAir: airport fill error for %s: %s", iata, e)
            return False

    # ── Date picker (Ant Design Calendar) ──

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        try:
            # Wait for UI to stabilize after airport selection
            await asyncio.sleep(1.5)

            # Click the departure date field (Ant Design DatePicker - readonly input)
            # Use JS click to bypass stability checks on animated elements
            clicked_cal = await page.evaluate("""() => {
                const inputs = document.querySelectorAll('input[readonly]');
                for (const inp of inputs) {
                    if (inp.placeholder === 'Select date' || inp.value.match(/\\w{3},\\s/)) {
                        inp.click();
                        return true;
                    }
                }
                // Try textbox role with calendar name
                const cb = document.querySelector('[placeholder*="date" i], [aria-label*="calendar" i]');
                if (cb) { cb.click(); return true; }
                return false;
            }""")

            if not clicked_cal:
                # Playwright fallback with force
                date_field = page.locator("input[readonly][placeholder='Select date']").first
                if await date_field.count() > 0:
                    await date_field.click(force=True, timeout=5000)
                else:
                    return False

            await asyncio.sleep(1.0)

            # Navigate to target month using Next button
            target_month = target.strftime("%b")  # "Mar", "Apr" etc.
            target_year = str(target.year)

            for _ in range(12):
                # Check current calendar header text
                header_text = await page.evaluate("""() => {
                    // Ant Design picker panel header contains month+year buttons
                    const panel = document.querySelector('.ant-picker-dropdown, .ant-picker-panel');
                    if (panel) return panel.textContent || '';
                    return document.body.textContent.substring(0, 5000);
                }""")

                if target_month in header_text and target_year in header_text:
                    break

                # Click Next month button (Ant Design uses aria-label "Next month (PageDown)")
                next_clicked = await page.evaluate("""() => {
                    const btns = document.querySelectorAll('button');
                    for (const b of btns) {
                        const label = b.getAttribute('aria-label') || b.title || '';
                        if (label.toLowerCase().includes('next month')) {
                            b.click();
                            return true;
                        }
                    }
                    // Fallback: super-next or right arrow in picker header
                    const next = document.querySelector('.ant-picker-header-next-btn, [class*="next"]');
                    if (next) { next.click(); return true; }
                    return false;
                }""")

                if not next_clicked:
                    break
                await asyncio.sleep(0.5)

            # Click the target day cell
            day = str(target.day)

            clicked = await page.evaluate("""(day) => {
                // Ant Design renders days in td.ant-picker-cell elements
                // Each cell has a .ant-picker-cell-inner div with the day text
                const cells = document.querySelectorAll('td[class*="picker-cell"]');
                for (const cell of cells) {
                    if (cell.classList.contains('ant-picker-cell-disabled')) continue;
                    // Skip cells from other months
                    if (cell.classList.contains('ant-picker-cell-in-view') === false &&
                        cell.className.includes('in-view') === false) {
                        // Only skip if there's an explicit out-of-view class
                        if (cell.classList.contains('ant-picker-cell-start') ||
                            cell.classList.contains('ant-picker-cell-end')) continue;
                    }
                    const inner = cell.querySelector('[class*="inner"], div, span');
                    if (!inner) continue;
                    // The day text may include price info like "15 1.2M"
                    const text = inner.textContent.trim();
                    if (text === day || text.startsWith(day + ' ') || text.startsWith(day + '\\n')) {
                        cell.click();
                        return true;
                    }
                }
                // Broader fallback: any td in a calendar table
                const allCells = document.querySelectorAll('table td');
                for (const cell of allCells) {
                    const text = cell.textContent.trim();
                    if ((text === day || text.startsWith(day + ' ')) &&
                        !cell.classList.contains('disabled')) {
                        cell.click();
                        return true;
                    }
                }
                return false;
            }""", day)

            if clicked:
                logger.debug("BatikAir: selected date day %s", day)
                return True

            return False

        except Exception as e:
            logger.warning("BatikAir: date error: %s", e)
            return False

    # ── Search button ──

    async def _click_search(self, page) -> None:
        await self._remove_overlays(page)
        # The search button has id="search_btn" or contains a search icon
        try:
            btn = page.locator("#search_btn")
            if await btn.count() > 0:
                await btn.click(timeout=5000)
                return
        except Exception:
            pass

        # Fallback: enabled button in the form
        for label in ["Search", "SEARCH", "Search Flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    return
            except Exception:
                continue

        # Last fallback: button with arrow/search img inside the form area
        try:
            btn = page.locator("button:not([disabled])").filter(
                has=page.locator("img")
            ).last
            if await btn.count() > 0:
                await btn.click(timeout=5000)
        except Exception:
            pass

    # ── DOM extraction (results page) ──

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight cards from /book/flight-search results page."""
        try:
            await asyncio.sleep(2)

            # Wait for flight cards to appear
            try:
                await page.wait_for_selector("[class*='flight'], [class*='card'], [class*='border']",
                                             timeout=10000)
            except Exception:
                pass

            flights = await page.evaluate("""(params) => {
                // Strategy: extract ordered sequences from page text
                // Flight data appears linearly: airline (flightNo) depTime arrTime route duration price
                const body = document.body.innerText || document.body.textContent || '';

                // Find all flight number occurrences with surrounding context
                const results = [];
                const flightPattern = /(Lion Air|Batik Air|Super Air Jet|Wings Air|Malindo Air)\\s*\\(([A-Z]{2}\\d{1,5})\\)/g;
                let match;
                const matches = [];
                while ((match = flightPattern.exec(body)) !== null) {
                    matches.push({
                        airline: match[1],
                        flightNo: match[2],
                        carrier: match[2].substring(0, 2),
                        index: match.index,
                    });
                }

                for (let i = 0; i < matches.length; i++) {
                    const m = matches[i];
                    const nextIdx = (i + 1 < matches.length) ? matches[i + 1].index : body.length;
                    const block = body.substring(m.index, Math.min(nextIdx, m.index + 500));

                    // Extract times from block (HH:MM pattern)
                    const times = block.match(/(\\d{2}:\\d{2})/g) || [];
                    const depTime = times[0] || '';
                    const arrTime = times[1] || '';

                    // Extract duration
                    const durMatch = block.match(/(\\d+)\\s*hr\\s*(\\d+)\\s*m/i);
                    let durSec = 0;
                    if (durMatch) durSec = parseInt(durMatch[1]) * 3600 + parseInt(durMatch[2]) * 60;

                    // Is non-stop?
                    const isNonstop = /non.?stop/i.test(block);
                    const stops = isNonstop ? 0 : (block.match(/(\\d+)\\s*stop/i)?.[1] || 0);

                    // Extract price - look for "Rp X,XXX,XXX" or "IDR X,XXX"
                    const priceMatches = block.match(/Rp\\s*([\\d,]+)/g) || [];
                    let price = 0;
                    if (priceMatches.length > 0) {
                        // First price is usually economy
                        const pStr = priceMatches[0].replace(/Rp\\s*/, '').replace(/,/g, '');
                        price = parseInt(pStr) || 0;
                    }

                    if (price <= 0) continue;

                    results.push({
                        flightNo: m.flightNo,
                        carrier: m.carrier,
                        airline: m.airline,
                        depTime: depTime,
                        arrTime: arrTime,
                        durationSec: durSec,
                        stops: parseInt(stops) || 0,
                        price: price,
                        origin: params.origin,
                        destination: params.destination,
                    });
                }

                return results;
            }""", {"origin": req.origin, "destination": req.destination})

            if not flights:
                return []

            booking_url = self._build_booking_url(req)
            offers: list[FlightOffer] = []
            date_str = req.date_from.strftime("%Y-%m-%d")

            for f in flights:
                dep_dt = self._parse_time(date_str, f.get("depTime", ""))
                arr_dt = self._parse_time(date_str, f.get("arrTime", ""))
                dur = f.get("durationSec", 0)
                if not dur and dep_dt and arr_dt:
                    delta = arr_dt - dep_dt
                    dur = max(int(delta.total_seconds()), 0)
                    if dur < 0:
                        dur += 86400  # crosses midnight

                seg = FlightSegment(
                    airline=f.get("carrier", "ID"),
                    airline_name=f.get("airline", "Batik Air"),
                    flight_no=f.get("flightNo", ""),
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_dt or datetime(2000, 1, 1),
                    arrival=arr_dt or datetime(2000, 1, 1),
                    duration_seconds=dur,
                    cabin_class="economy",
                    aircraft="",
                )

                route = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=dur,
                    stopovers=f.get("stops", 0),
                )

                price = f.get("price", 0)
                fid = hashlib.md5(
                    f"id_{req.origin}{req.destination}{f.get('flightNo','')}{price}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"id_{fid}",
                    price=float(price),
                    currency="IDR",
                    price_formatted=f"Rp {price:,.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=[f.get("airline", "Batik Air")],
                    owner_airline=f.get("carrier", "ID"),
                    booking_url=booking_url,
                    is_locked=False,
                    source="batikair_direct",
                    source_tier="free",
                ))

            return offers

        except Exception as e:
            logger.warning("BatikAir DOM extraction error: %s", e)
            return []

    # ── API response parser (if interception works) ──

    def _parse_api_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if not isinstance(data, (dict, list)):
            return []
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Inspect common JSON shapes
        flights_raw = []
        if isinstance(data, dict):
            for key in ("flights", "outboundFlights", "data", "journeys", "availability"):
                v = data.get(key)
                if isinstance(v, list):
                    flights_raw = v
                    break
                if isinstance(v, dict):
                    for k2 in ("flights", "journeys", "outbound"):
                        v2 = v.get(k2)
                        if isinstance(v2, list):
                            flights_raw = v2
                            break
                    if flights_raw:
                        break
        elif isinstance(data, list):
            flights_raw = data

        for flight in flights_raw:
            if not isinstance(flight, dict):
                continue
            price = None
            for pk in ("price", "totalPrice", "lowestFare", "farePrice", "amount"):
                pv = flight.get(pk)
                if pv is not None:
                    try:
                        price = float(pv if not isinstance(pv, dict) else pv.get("amount", 0))
                        break
                    except (TypeError, ValueError):
                        pass
            if not price or price <= 0:
                continue

            fno = str(flight.get("flightNumber", "") or flight.get("flightNo", ""))
            carrier = flight.get("carrierCode", "") or flight.get("carrier", "") or "ID"

            dep_str = flight.get("departureDateTime", "") or flight.get("departure", "")
            arr_str = flight.get("arrivalDateTime", "") or flight.get("arrival", "")
            dep_dt = self._parse_dt(dep_str)
            arr_dt = self._parse_dt(arr_str)
            dur = max(int((arr_dt - dep_dt).total_seconds()), 0) if dep_dt.year > 2000 and arr_dt.year > 2000 else 0

            seg = FlightSegment(
                airline=carrier, airline_name="Batik Air", flight_no=fno,
                origin=flight.get("origin", req.origin),
                destination=flight.get("destination", req.destination),
                departure=dep_dt, arrival=arr_dt, duration_seconds=dur,
                cabin_class="economy", aircraft="",
            )

            route = FlightRoute(segments=[seg], total_duration_seconds=dur, stopovers=0)
            fid = hashlib.md5(f"id_{req.origin}{req.destination}{fno}{price}".encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"id_{fid}", price=round(price, 2), currency="IDR",
                price_formatted=f"Rp {price:,.0f}", outbound=route, inbound=None,
                airlines=["Batik Air"], owner_airline=carrier,
                booking_url=booking_url, is_locked=False,
                source="batikair_direct", source_tier="free",
            ))

        return offers

    # ── Helpers ──

    @staticmethod
    def _parse_time(date_str: str, time_str: str) -> Optional[datetime]:
        if not time_str or not date_str:
            return None
        try:
            return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            return None

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

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("BatikAir %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"batikair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="IDR", offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return f"https://www.batikair.com.my/book/flight-search"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"batikair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="IDR", offers=[], total_results=0,
        )
