"""
Hainan Airlines (HU) — CDP Chrome connector — form fill + clientSideData parse.

Hainan Airlines's website at www.hainanairlines.com uses HUPortal (Amadeus IBE).
Direct API calls are blocked; headed CDP Chrome with form fill is required.

Strategy (CDP Chrome + clientSideData.AVAI):
1. Launch headed Chrome via CDP (off-screen, stealth).
2. Navigate to /CN/GB/Search → loads booking form.
3. Click one-way → fill origin/dest autocomplete → set date via huCalendar → submit.
4. Wait for /availability page → parse clientSideData.AVAI JSON.
5. If prices needed, select first flight + Continue → FARE page.
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
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9500
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".hainan_chrome_data"
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
            logger.info("Hainan: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("Hainan: Chrome launched on CDP port %d", _DEBUG_PORT)

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
            document.querySelectorAll(
                '#onetrust-consent-sdk, .cookie-banner, [class*="cookie"], [class*="consent"]'
            ).forEach(el => el.remove());
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = b.textContent.trim().toLowerCase();
                if (t.includes('accept') || t.includes('agree') || t.includes('got it') || t.includes('ok')) {
                    if (b.offsetHeight > 0) { b.click(); break; }
                }
            }
        }""")
    except Exception:
        pass


class HainanConnectorClient:
    """Hainan Airlines (HU) CDP Chrome connector."""

    IATA = "HU"
    AIRLINE_NAME = "Hainan Airlines"
    SOURCE = "hainan_direct"
    SEARCH_URL = "https://www.hainanairlines.com/CN/GB/Search"
    DEFAULT_CURRENCY = "CNY"

    def __init__(self, timeout: float = 55.0):
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
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        avai_data: dict = {}

        try:
            logger.info("Hainan: loading Search page for %s→%s", req.origin, req.destination)
            await page.goto(self.SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(6.0)
            await _dismiss_overlays(page)

            # Click One-way button + force TRIP_TYPE hidden field
            await page.evaluate(r"""() => {
                const form = document.querySelector('#formREVENUE');
                if (form) {
                    const ow = form.querySelector('button.oneWay');
                    if (ow) ow.click();
                }
                var tt = document.querySelector('input[name="TRIP_TYPE"]');
                if (tt) tt.value = 'O';
            }""")
            await asyncio.sleep(1.0)

            # Fill origin via autocomplete
            ok = await self._fill_airport(page, "origin", req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            # Fill destination via autocomplete
            ok = await self._fill_airport(page, "destination", req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            # Set date via huCalendar + hidden B_DATE_1 field
            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Set adults to 1 via selectBoxIt API
            await page.evaluate(r"""() => {
                const sel = document.querySelector('#adult_selectbox_REVENUE');
                if (sel) {
                    try {
                        var sbi = $(sel).data('selectBoxIt') || $(sel).data('selectboxitObj');
                        if (sbi) { sbi.selectOption('1'); return; }
                    } catch(e) {}
                    sel.value = '1';
                    sel.dispatchEvent(new Event('change', { bubbles: true }));
                }
            }""")
            await asyncio.sleep(0.5)

            # Force TRIP_TYPE again right before submit (oneWay click can be unreliable)
            await page.evaluate(r"""() => {
                var tt = document.querySelector('input[name="TRIP_TYPE"]');
                if (tt) tt.value = 'O';
            }""")

            # Click search submit button
            await page.evaluate(r"""() => {
                const form = document.querySelector('#formREVENUE');
                const btn = form?.querySelector('button.submitFlightSearchButton');
                if (btn) btn.click();
            }""")
            logger.info("Hainan: search submitted")

            # Wait for availability page
            try:
                await page.wait_for_url("**/availability**", timeout=30000)
            except Exception:
                pass
            await asyncio.sleep(10.0)

            # Extract clientSideData.AVAI
            avai_data = await page.evaluate(r"""() => {
                return window.clientSideData?.AVAI || null;
            }""")
            logger.info("Hainan: AVAI data present=%s, url=%s", bool(avai_data), page.url)

            offers = []
            if avai_data:
                offers = self._parse_avai_data(avai_data, req)
                logger.info("Hainan: AVAI parsed %d flight offers", len(offers))

            if not offers:
                offers = await self._scrape_dom(page, req)

            # ---- FARE PAGE PRICING ----
            # Select first direct flight on availability page, navigate to FARE page
            # to get the actual ticket price. Apply price to matching offer.
            if offers and "availability" in (page.url or ""):
                fare_price = await self._get_fare_price(page)
                if fare_price and fare_price > 0:
                    # Apply price to first offer (the one we selected on availability page)
                    offers[0].price = fare_price
                    offers[0].price_formatted = f"CNY {fare_price:,.0f}"
                    logger.info("Hainan: got fare price CNY %.0f for first flight", fare_price)

            # Filter out price=0 offers (unpriced flights)
            priced = [o for o in offers if o.price > 0]
            unpriced = [o for o in offers if o.price <= 0]
            # Keep priced offers first; include unpriced only if no priced results
            final_offers = priced if priced else unpriced
            final_offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info("Hainan %s→%s: %d offers (%d priced) in %.1fs", req.origin, req.destination, len(final_offers), len(priced), elapsed)

            search_hash = hashlib.md5(
                f"hainan{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = final_offers[0].currency if final_offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=final_offers, total_results=len(final_offers),
            )
        except Exception as e:
            logger.error("Hainan error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _get_fare_price(self, page) -> float:
        """Select first flight radio on availability page, click Continue,
        and extract total price from the FARE page."""
        try:
            # Click first flight radio (b0_0RadioEl / SchedDrivenAvailButton_1)
            clicked = await page.evaluate(r"""() => {
                var radios = document.querySelectorAll('input[name="SchedDrivenAvailButton_1"]');
                if (radios.length > 0) {
                    radios[0].click();
                    radios[0].checked = true;
                    radios[0].dispatchEvent(new Event('change', {bubbles: true}));
                    return true;
                }
                // Fallback: any non-TRIP_TYPE radio
                var all = document.querySelectorAll('input[type="radio"]');
                for (var r of all) {
                    if (r.name !== 'TRIP_TYPE') { r.click(); r.checked = true; return true; }
                }
                return false;
            }""")
            if not clicked:
                logger.debug("Hainan: no flight radio found")
                return 0

            await asyncio.sleep(1.0)

            # Click Continue / nextStep button
            await page.evaluate(r"""() => {
                var btn = document.querySelector('button.nextStep');
                if (btn) btn.click();
            }""")

            # Wait for FARE page
            try:
                await page.wait_for_url("**/fare**", timeout=20000)
            except Exception:
                logger.debug("Hainan: FARE page not reached")
                return 0

            await asyncio.sleep(5.0)

            # Extract total price from FARE page
            price_data = await page.evaluate(r"""() => {
                // Method 1: .currency-price element (most reliable)
                var cp = document.querySelectorAll('.currency-price');
                for (var el of cp) {
                    var text = el.textContent.trim().replace(/[^0-9.]/g, '');
                    var val = parseFloat(text);
                    if (val > 0) return {price: val, source: 'currency-price'};
                }
                // Method 2: .group-total-price
                var gtp = document.querySelector('.group-total-price');
                if (gtp) {
                    var m = gtp.textContent.match(/[\d,]+\.\d{2}/);
                    if (m) return {price: parseFloat(m[0].replace(/,/g, '')), source: 'group-total-price'};
                }
                // Method 3: scan for CNY pattern in page text
                var body = document.body.innerText;
                var matches = body.match(/CNY\s*([\d,]+\.\d{2})/g) || [];
                for (var mt of matches) {
                    var num = mt.replace(/[^0-9.]/g, '');
                    var val = parseFloat(num);
                    if (val > 0) return {price: val, source: 'body-text'};
                }
                // Method 4: look for Total row
                var totalMatch = body.match(/Total\s+([\d,]+\.\d{2})\s*CNY/);
                if (totalMatch) return {price: parseFloat(totalMatch[1].replace(/,/g, '')), source: 'total-row'};
                return null;
            }""")

            if price_data and price_data.get("price", 0) > 0:
                logger.debug("Hainan: fare price %.2f from %s", price_data["price"], price_data["source"])
                return price_data["price"]

            return 0
        except Exception as e:
            logger.debug("Hainan: fare price extraction error: %s", e)
            return 0

    async def _fill_airport(self, page, direction: str, iata: str) -> bool:
        try:
            # Hainan uses #departureLocREVENUE and #ReturnLocREVENUE
            sel = "#departureLocREVENUE" if direction == "origin" else "#ReturnLocREVENUE"

            field = page.locator(sel)
            await field.click(force=True, timeout=5000)
            await field.fill("")
            await asyncio.sleep(0.3)
            await field.type(iata, delay=50)
            await asyncio.sleep(2.0)

            # Click first autocomplete item
            try:
                first_item = page.locator(".ui-autocomplete .ui-menu-item:visible").first
                await first_item.click(timeout=3000)
            except Exception:
                # Fallback: ArrowDown + Enter
                await field.press("ArrowDown")
                await asyncio.sleep(0.3)
                await field.press("Enter")

            await asyncio.sleep(1.0)
            logger.info("Hainan: airport %s → %s", direction, iata)
            return True
        except Exception as e:
            logger.warning("Hainan: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        try:
            # Use huCalendar API to set date (month is 0-indexed in JS)
            year = dt.year
            month = dt.month - 1  # JS months are 0-indexed
            day = dt.day
            # Also set hidden B_DATE_1 field (format YYYYMMDD0000) — required for form submit
            b_date_str = dt.strftime("%Y%m%d") + "0000"
            await page.evaluate(f"""() => {{
                var d = new Date({year}, {month}, {day});
                $('#formREVENUE .inputOnlineFrameFormDepartureDate').huCalendar('setDate', d);
                var bd = document.querySelector('#B_DATE_1') || document.querySelector('input[name="B_DATE_1"]');
                if (bd) bd.value = '{b_date_str}';
            }}""")
            await asyncio.sleep(1.0)
            logger.info("Hainan: date set to %s via huCalendar", dt.strftime("%Y-%m-%d"))
            return True
        except Exception as e:
            logger.warning("Hainan: date error: %s", e)
            return False



    def _parse_avai_data(self, avai: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse clientSideData.AVAI from the availability page."""
        offers = []
        try:
            # AVAI structure: ORIGINAL_LIST_BOUND[0].LIST_FLIGHT[]
            bounds = avai.get("ORIGINAL_LIST_BOUND") or []
            for bound in bounds:
                flights = bound.get("LIST_FLIGHT") or []
                for flight in flights:
                    offer = self._build_avai_offer(flight, req)
                    if offer:
                        offers.append(offer)
        except Exception as e:
            logger.debug("Hainan: AVAI parse error: %s", e)
        return offers

    def _build_avai_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build offer from AVAI.ORIGINAL_LIST_BOUND[].LIST_FLIGHT[] item."""
        try:
            # Availability page doesn't show prices - we'd need to select + continue
            # For now, return 0 price to indicate flight exists (prices fetched separately)
            segments_data = flight.get("LIST_SEGMENT") or []
            if not segments_data:
                return None

            segments = []
            total_duration_ms = 0

            for seg in segments_data:
                airline_info = seg.get("AIRLINE") or {}
                airline_code = airline_info.get("CODE") or self.IATA
                airline_name = airline_info.get("NAME") or self.AIRLINE_NAME
                flight_no = seg.get("FLIGHT_NUMBER") or ""
                if flight_no and not flight_no.startswith(airline_code):
                    flight_no = f"{airline_code}{flight_no}"

                # B_DATE and E_DATE are epoch milliseconds
                b_date_ms = seg.get("B_DATE") or 0
                e_date_ms = seg.get("E_DATE") or 0
                dep_dt = datetime.fromtimestamp(b_date_ms / 1000) if b_date_ms else datetime.now()
                arr_dt = datetime.fromtimestamp(e_date_ms / 1000) if e_date_ms else datetime.now()

                b_loc = seg.get("B_LOCATION") or {}
                e_loc = seg.get("E_LOCATION") or {}
                origin = b_loc.get("LOCATION_CODE") or req.origin
                destination = e_loc.get("LOCATION_CODE") or req.destination

                flight_time_ms = seg.get("FLIGHT_TIME") or 0
                total_duration_ms += flight_time_ms

                _hu_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                segments.append(FlightSegment(
                    airline=airline_code[:2],
                    airline_name=airline_name,
                    flight_no=flight_no or self.IATA,
                    origin=origin,
                    destination=destination,
                    departure=dep_dt,
                    arrival=arr_dt,
                    cabin_class=_hu_cabin,
                ))

            if not segments:
                return None

            total_duration_sec = int(total_duration_ms / 1000) if total_duration_ms else 0
            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_duration_sec,
                stopovers=max(0, len(segments) - 1)
            )

            # Generate offer ID from flight info
            first_flight_no = segments[0].flight_no if segments else "HU"
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{req.origin}_{req.destination}_{req.date_from}_{first_flight_no}".encode()
            ).hexdigest()[:12]

            # Price is 0 from availability page - actual prices require FARE page
            # We'll set a placeholder and note this is from availability
            return FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}",
                price=0,  # Actual price requires FARE page navigation
                currency=self.DEFAULT_CURRENCY,
                price_formatted="Check airline",
                outbound=route,
                inbound=None,
                airlines=list({s.airline for s in segments}),
                owner_airline=self.IATA,
                booking_url=self._booking_url(req),
                is_locked=False,
                source=self.SOURCE,
                source_tier="free",
            )
        except Exception as e:
            logger.debug("Hainan: AVAI offer parse error: %s", e)
            return None

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
                const priceMatch = text.match(/(CNY|USD|EUR|¥|\$|€)\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(CNY|USD|EUR|¥|\$|€)/i);
                if (!priceMatch) continue;
                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0) continue;
                let currency = 'CNY';
                if (/USD|\$/.test(priceMatch[0])) currency = 'USD';
                else if (/EUR|€/.test(priceMatch[0])) currency = 'EUR';
                const fnMatch = text.match(/\b(HU\s*\d{2,4})\b/i);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'HU',
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

        _hu_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segment = FlightSegment(
            airline=self.IATA, airline_name=self.AIRLINE_NAME, flight_no=flight_no,
            origin=req.origin, destination=req.destination, departure=dep_dt, arrival=arr_dt, cabin_class=_hu_cabin,
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
        return f"https://www.hainanairlines.com/en?from={req.origin}&to={req.destination}&date={date_str}"

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"hu_rt_{o.id}_{i.id}",
                    price=round(o.price + i.price, 2),
                    currency=o.currency,
                    outbound=o.outbound,
                    inbound=i.outbound,
                    owner_airline=o.owner_airline,
                    airlines=list(set(o.airlines + i.airlines)),
                    source=o.source,
                    booking_url=o.booking_url,
                    conditions=o.conditions,
                ))
        combos.sort(key=lambda x: x.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"hainan{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )


# Module-level client instance and search function (required by engine.py)
_client = HainanConnectorClient()


async def search(req: FlightSearchRequest) -> FlightSearchResponse:
    """Search Hainan Airlines for flights."""
    return await _client.search_flights(req)
