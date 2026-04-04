"""
Air Europa (UX) — CDP Chrome connector — form fill + API intercept.

Air Europa's website at www.aireuropa.com uses a search widget with autocomplete
airport fields and calendar date picker. Direct API calls are blocked;
headed CDP Chrome with form fill + API interception is required.

Strategy (CDP Chrome + API interception):
1. Launch headed Chrome via CDP (off-screen, stealth).
2. Navigate to airchina.com → SPA loads with search widget.
3. Accept cookies → set one-way → fill origin/dest → select date → search.
4. Intercept the search API response (flight availability JSON).
5. If API not captured, fall back to DOM scraping on results page.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, date, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied, inject_stealth_js
from .airline_routes import get_city_airports

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9498
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".aireuropa_chrome_data"
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

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("AirEuropa: connected to existing Chrome on port %d", _DEBUG_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass
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
            logger.info("AirEuropa: Chrome launched on CDP port %d", _DEBUG_PORT)

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
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
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = _context = _pw_instance = _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
        except Exception:
            pass


async def _dismiss_overlays(page) -> None:
    try:
        await page.evaluate("""() => {
            const accept = document.querySelector('#onetrust-accept-btn-handler');
            if (accept && accept.offsetHeight > 0) { accept.click(); return; }
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = b.textContent.trim().toLowerCase();
                if ((t.includes('accept') || t.includes('agree') || t.includes('got it'))
                    && b.offsetHeight > 0) { b.click(); return; }
            }
        }""")
        await asyncio.sleep(1.0)
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter, ' +
                '[class*="cookie"], [class*="consent"], ' +
                '.cdk-overlay-backdrop, .cdk-overlay-dark-backdrop'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class AirEuropaConnectorClient:
    """Air Europa (UX) CDP Chrome connector."""

    IATA = "UX"
    AIRLINE_NAME = "Air Europa"
    SOURCE = "aireuropa_direct"
    HOMEPAGE = "https://www.aireuropa.com/en/flights"
    DEFAULT_CURRENCY = "EUR"

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # ── City code expansion (LON → LHR, BCN stays BCN) ──
        origins = get_city_airports(req.origin)
        destinations = get_city_airports(req.destination)
        api_origin = origins[0] if origins else req.origin
        api_dest = destinations[0] if destinations else req.destination

        context = await _get_context()
        page = await context.new_page()
        await inject_stealth_js(page)
        await auto_block_if_proxied(page)

        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            nonlocal search_data
            url = response.url.lower()
            if response.status not in (200, 201):
                if response.status in (403, 429) and any(k in url for k in ("air-bounds", "availability", "flight", "search", "offer")):
                    logger.warning("AirEuropa: blocked %d on %s", response.status, url[:120])
                return
            try:
                # Only capture Amadeus DES flight API responses — exclude CMS/masterdata/channel URLs
                is_flight_api = any(k in url for k in (
                    "air-bounds", "air-shopping", "availability",
                    "/offers", "flight-search", "low-fare",
                ))
                # Exclude known non-flight API paths
                is_excluded = any(k in url for k in (
                    "channel-cms", "masterdata", "config", "tracking",
                    "analytics", "tag-manager", "i18n", "assets",
                    "login", "session", "payment",
                ))
                if not is_flight_api or is_excluded:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = await response.text()
                if len(body) < 200:
                    return
                data = json.loads(body)
                if isinstance(data, dict):
                    keys = list(data.keys())[:8]
                    logger.info("AirEuropa: API response keys=%s from %s (%dB)", keys, url[:100], len(body))
                    # Primary: Amadeus DES format with data.airBoundGroups
                    inner = data.get("data", {})
                    if isinstance(inner, dict) and ("airBoundGroups" in inner or "airBounds" in inner):
                        search_data.update(data)
                        api_event.set()
                        logger.info("AirEuropa: captured air-bounds API (%d keys, %dB)", len(data), len(body))
                    # Alternate: direct flight results with flight-specific keys
                    elif any(k in data for k in ("flights", "offers", "itineraries", "airBounds", "airBoundGroups")):
                        search_data.update(data)
                        api_event.set()
                        logger.info("AirEuropa: captured alternate flight API (%d keys)", len(data))
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            logger.info("AirEuropa: loading search for %s→%s", req.origin, req.destination)

            # Direct URL to digital.aireuropa.com/booking/availability no longer works
            # (Amadeus DES Angular app requires session context from form submission).
            # Always use the homepage form fill approach.
            logger.info("AirEuropa: using form fill on homepage")
            await page.goto(self.HOMEPAGE, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(4.0)
            await _dismiss_overlays(page)
            # Remove any lingering CDK/Angular overlays that block interaction
            await page.evaluate("""() => {
                document.querySelectorAll('.cdk-overlay-backdrop, .cdk-overlay-dark-backdrop').forEach(el => el.remove());
            }""")
            await asyncio.sleep(0.5)

            # One-way toggle — use force click to bypass any remaining overlay
            await page.evaluate("""() => {
                // Try mat-radio for one-way
                const radios = document.querySelectorAll('mat-radio-button, input[type="radio"]');
                for (const r of radios) {
                    const t = (r.textContent || r.parentElement?.textContent || '').trim().toLowerCase();
                    if (t.includes('one way') || t.includes('one-way') || t.includes('solo ida')) {
                        r.click(); return;
                    }
                }
                // Try mat-select dropdown
                const ms = document.querySelector('common-select.way-trip mat-select, [class*="trip-type"] mat-select');
                if (ms) ms.click();
            }""")
            await asyncio.sleep(0.8)
            await page.evaluate("""() => {
                const opts = document.querySelectorAll('mat-option, [role="option"]');
                for (const o of opts) {
                    const t = (o.textContent || '').trim().toLowerCase();
                    if (t.includes('one way') || t.includes('one-way') || t.includes('solo ida')) { o.click(); return; }
                }
            }""")
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, "input#departure", api_origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, "input#arrival", api_dest)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Click search — Air Europa uses Angular SPA routing, so no full navigation.
            # Just click and let the API interception capture the response.
            await page.evaluate("""() => {
                const btn = document.querySelector('button.ae-btn-block.ae-btn-primary');
                if (btn && btn.offsetHeight > 0) { btn.click(); return; }
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    const t = (b.textContent || '').trim().toLowerCase();
                    if ((t === 'search' || t.includes('search')) && b.offsetHeight > 0) { b.click(); return; }
                }
            }""")
            logger.info("AirEuropa: search button clicked, waiting for SPA navigation")
            await asyncio.sleep(8.0)

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                if any(k in url.lower() for k in ["result", "booking", "digital.aireuropa", "availability", "select"]):
                    await asyncio.sleep(6.0)
                    break
                await asyncio.sleep(1.0)

            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=8.0)
                except asyncio.TimeoutError:
                    pass

            offers = []
            if search_data:
                offers = self._parse_api_response(search_data, req)
            if not offers:
                offers = await self._scrape_dom(page, req)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("AirEuropa %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"aireuropa{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("AirEuropa error: %s", e)
            return self._empty(req)
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_airport(self, page, selector: str, iata: str) -> bool:
        try:
            field = page.locator(selector).first
            await field.click(timeout=5000, force=True)
            await asyncio.sleep(0.5)
            await page.keyboard.press("Control+a")
            await page.keyboard.press("Backspace")
            await field.type(iata, delay=120)
            await asyncio.sleep(2.0)

            # Check if Air Europa serves this destination
            no_service = await page.evaluate("""() => {
                const opts = document.querySelectorAll('mat-option, [role="option"]');
                for (const o of opts) {
                    if (o.offsetHeight > 0 && (o.textContent || '').toLowerCase().includes('do not fly')) return true;
                }
                return false;
            }""")
            if no_service:
                logger.warning("AirEuropa: airline does not fly to %s", iata)
                return False

            # Angular mat-autocomplete options
            selected = await page.evaluate("""(iata) => {
                const opts = document.querySelectorAll(
                    'mat-option, [role="option"], .mat-option, ' +
                    '.cdk-overlay-pane mat-option, [class*="autocomplete"] li'
                );
                for (const o of opts) {
                    if (o.textContent.includes(iata) && o.offsetHeight > 0) {
                        o.click(); return true;
                    }
                }
                // Click first valid option if available
                for (const o of opts) {
                    if (o.offsetHeight > 0 && !(o.textContent || '').toLowerCase().includes('do not fly')) {
                        o.click(); return true;
                    }
                }
                return false;
            }""", iata)
            if not selected:
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.2)
                await page.keyboard.press("Enter")

            await asyncio.sleep(0.5)
            logger.info("AirEuropa: airport %s → %s", selector, iata)
            return True
        except Exception as e:
            logger.warning("AirEuropa: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        target_day = str(dt.day)
        target_month = dt.strftime("%B").lower()  # e.g. "april"
        target_year = str(dt.year)
        try:
            # Click the departure date input to open calendar
            date_field = page.locator("input#mat-input-1, input[placeholder*='DD/MM']").first
            await date_field.click(timeout=5000, force=True)
            await asyncio.sleep(1.0)

            # Air Europa calendar shows 2 months at a time with .month sections.
            # Each .month section contains the month name + all day cells.
            # Navigate forward until target month is visible.
            for _ in range(12):
                found = await page.evaluate("""(args) => {
                    const [targetMonth, targetYear] = args;
                    const months = document.querySelectorAll('.month');
                    for (const m of months) {
                        const text = m.textContent.toLowerCase();
                        if (text.includes(targetMonth) && text.includes(targetYear) && m.offsetHeight > 0) {
                            return true;
                        }
                    }
                    return false;
                }""", [target_month, target_year])
                if found:
                    break
                await page.evaluate("""() => {
                    const btns = document.querySelectorAll('button');
                    for (const b of btns) {
                        if ((b.getAttribute('aria-label') || '').toLowerCase().includes('next month') && b.offsetHeight > 0) {
                            b.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(0.5)

            # Click the target day WITHIN the correct month section (scoped)
            clicked = await page.evaluate("""(args) => {
                const [targetMonth, targetYear, day] = args;
                const months = document.querySelectorAll('.month');
                for (const m of months) {
                    const text = m.textContent.toLowerCase();
                    if (text.includes(targetMonth) && text.includes(targetYear) && m.offsetHeight > 0) {
                        // Found the target month section — click .day-content within it
                        const spans = m.querySelectorAll('span.day-content');
                        for (const s of spans) {
                            if ((s.textContent || '').trim() === day && s.offsetHeight > 0) {
                                s.click();
                                return true;
                            }
                        }
                    }
                }
                return false;
            }""", [target_month, target_year, target_day])
            if clicked:
                logger.info("AirEuropa: date selected %s", dt.strftime("%Y-%m-%d"))
            else:
                logger.warning("AirEuropa: could not click day %s in %s", target_day, target_month)
            await asyncio.sleep(1.0)
            return clicked
        except Exception as e:
            logger.warning("AirEuropa: date error: %s", e)
            return False

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Amadeus DES air-bounds format."""
        offers = []
        air_data = data.get("data", {})
        if isinstance(air_data, dict):
            groups = air_data.get("airBoundGroups", [])
        else:
            groups = []
        dicts = data.get("dictionaries", {})
        flight_dict = dicts.get("flight", {}) if isinstance(dicts, dict) else {}

        for group in (groups if isinstance(groups, list) else []):
            bound_details = group.get("boundDetails", {})
            seg_refs = bound_details.get("segments", [])
            duration_secs = bound_details.get("duration", 0)

            # Build segments from dictionaries
            segments = []
            for seg_ref in seg_refs:
                fid = seg_ref.get("flightId", "") if isinstance(seg_ref, dict) else str(seg_ref)
                seg_data = flight_dict.get(fid, {})
                dep_info = seg_data.get("departure", {})
                arr_info = seg_data.get("arrival", {})
                airline_code = seg_data.get("marketingAirlineCode") or self.IATA
                flight_num = seg_data.get("marketingFlightNumber", "")
                flight_no = f"{airline_code}{flight_num}" if flight_num else airline_code

                dep_dt = self._parse_dt(dep_info.get("dateTime", ""), req.date_from)
                arr_dt = self._parse_dt(arr_info.get("dateTime", ""), req.date_from)

                segments.append(FlightSegment(
                    airline=airline_code[:2],
                    airline_name=self.AIRLINE_NAME if airline_code == self.IATA else airline_code,
                    flight_no=flight_no,
                    origin=dep_info.get("locationCode", req.origin),
                    destination=arr_info.get("locationCode", req.destination),
                    departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                ))

            if not segments:
                continue

            route = FlightRoute(segments=segments, total_duration_seconds=duration_secs,
                                stopovers=max(0, len(segments) - 1))

            # Each airBound is a fare option for this flight group
            for ab in group.get("airBounds", []):
                prices_block = ab.get("prices", {})
                total_prices = prices_block.get("totalPrices", [{}])
                tp = total_prices[0] if total_prices else {}
                # Prices are in cents
                total_cents = tp.get("total", 0)
                currency = tp.get("currencyCode", self.DEFAULT_CURRENCY)
                price = total_cents / 100.0
                if price <= 0:
                    continue

                fare_family = ab.get("fareFamilyCode", "")
                offer_id = hashlib.md5(
                    f"{self.IATA.lower()}_{req.origin}_{req.destination}_{req.date_from}_{price}_{fare_family}_{segments[0].flight_no}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
                    price_formatted=f"{currency} {price:,.2f}", outbound=route, inbound=None,
                    airlines=list({s.airline for s in segments}), owner_airline=self.IATA,
                    booking_url=self._booking_url(req), is_locked=False,
                    source=self.SOURCE, source_tier="free",
                ))
        return offers

    def _extract_currency(self, d: dict) -> str:
        for key in ("currency", "currencyCode"):
            val = d.get(key)
            if isinstance(val, str) and len(val) == 3:
                return val.upper()
        if isinstance(d.get("price"), dict):
            return d["price"].get("currency", self.DEFAULT_CURRENCY)
        return self.DEFAULT_CURRENCY

    @staticmethod
    def _parse_dt(s, fallback_date) -> datetime:
        if not s:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                return datetime(dt.year, dt.month, dt.day) if isinstance(dt, date) and not isinstance(dt, datetime) else dt
            except Exception:
                return datetime.now()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(s[:19], fmt)
            except (ValueError, TypeError):
                continue
        m = re.search(r"(\d{1,2}):(\d{2})", str(s))
        if m:
            try:
                dt = fallback_date if isinstance(fallback_date, (datetime, date)) else datetime.strptime(str(fallback_date), "%Y-%m-%d")
                d = dt if isinstance(dt, date) and not isinstance(dt, datetime) else dt.date() if isinstance(dt, datetime) else dt
                return datetime(d.year, d.month, d.day, int(m.group(1)), int(m.group(2)))
            except Exception:
                pass
        return datetime.now()

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        await asyncio.sleep(3)
        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];
            const cards = document.querySelectorAll(
                '[class*="flight-card"], [class*="flight-row"], [class*="itinerary"], ' +
                '[class*="result-card"], [class*="bound"], [class*="flight-item"], ' +
                '[class*="flightInfo"], [class*="flight_item"]'
            );
            for (const card of cards) {
                const text = card.innerText || '';
                if (text.length < 20) continue;
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;
                const priceMatch = text.match(/(EUR|USD|GBP|€|\$|£)\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(EUR|USD|GBP|€|\$|£)/i);
                if (!priceMatch) continue;
                // Handle European price format: "496,62" or "1.496,62"
                let priceStr = priceMatch[0].replace(/[^0-9.,]/g, '');
                // If comma is present and followed by 1-2 digits at end → European decimal
                if (/,\d{1,2}$/.test(priceStr)) {
                    priceStr = priceStr.replace(/\./g, '').replace(',', '.');
                } else {
                    priceStr = priceStr.replace(/,/g, '');
                }
                const price = parseFloat(priceStr);
                if (!price || price <= 0) continue;
                let currency = 'EUR';
                if (/USD|\$/.test(priceMatch[0])) currency = 'USD';
                else if (/GBP|£/.test(priceMatch[0])) currency = 'GBP';
                const fnMatch = text.match(/\b(UX\s*\d{2,4})\b/i);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'UX',
                });
            }
            return results;
        }""", [req.origin, req.destination])

        offers = []
        for f in (flights or []):
            offer = self._build_dom_offer(f, req)
            if offer:
                offers.append(offer)
        return offers

    def _build_dom_offer(self, f: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        price = f.get("price", 0)
        if price <= 0:
            return None
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            dep_date = dt.date() if isinstance(dt, datetime) else dt if isinstance(dt, date) else date.today()
        except (ValueError, TypeError):
            dep_date = date.today()

        dep_time = f.get("depTime", "00:00")
        arr_time = f.get("arrTime", "00:00")
        try:
            h, m = dep_time.split(":")
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day, int(h), int(m))
        except (ValueError, IndexError):
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day)
        try:
            h, m = arr_time.split(":")
            arr_dt = datetime(dep_date.year, dep_date.month, dep_date.day, int(h), int(m))
            if arr_dt <= dep_dt:
                arr_dt += timedelta(days=1)
        except (ValueError, IndexError):
            arr_dt = dep_dt

        flight_no = f.get("flightNo", self.IATA)
        currency = f.get("currency", self.DEFAULT_CURRENCY)
        offer_id = hashlib.md5(f"{self.IATA.lower()}_{req.origin}_{req.destination}_{dep_date}_{flight_no}_{price}".encode()).hexdigest()[:12]

        segment = FlightSegment(
            airline=self.IATA, airline_name=self.AIRLINE_NAME, flight_no=flight_no,
            origin=req.origin, destination=req.destination, departure=dep_dt, arrival=arr_dt, cabin_class="economy",
        )
        route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
        return FlightOffer(
            id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
            price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
            airlines=[self.AIRLINE_NAME], owner_airline=self.IATA,
            booking_url=self._booking_url(req), is_locked=False, source=self.SOURCE, source_tier="free",
        )

    def _booking_url(self, req: FlightSearchRequest) -> str:
        try:
            date_str = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
        except Exception:
            date_str = ""
        return f"https://www.aireuropa.com/en/flights?from={req.origin}&to={req.destination}&date={date_str}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"aireuropa{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
