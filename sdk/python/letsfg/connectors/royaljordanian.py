"""
Royal Jordanian (RJ) — CDP Chrome connector — form fill + API intercept.

Royal Jordanian's website at www.rj.com uses a search widget with autocomplete
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
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9501
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".royaljordanian_chrome_data"
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
            logger.info("RoyalJordanian: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("RoyalJordanian: Chrome launched on CDP port %d", _DEBUG_PORT)

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


class RoyalJordanianConnectorClient:
    """Royal Jordanian (RJ) CDP Chrome connector."""

    IATA = "RJ"
    AIRLINE_NAME = "Royal Jordanian"
    SOURCE = "royaljordanian_direct"
    HOMEPAGE = "https://www.rj.com/en"
    DEFAULT_CURRENCY = "JOD"

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            url = response.url.lower()
            if response.status not in (200, 201):
                return
            try:
                if any(k in url for k in ["/search", "/availability", "/flight",
                                           "/offer", "/fare", "/lowprice", "/schedule",
                                           "/plnext", "/airavail", "/upsell",
                                           "/airrevalidate", "/traveloption",
                                           "/getflightresult", "/displayresult",
                                           "action"]):
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct and "text" not in ct and "html" not in ct:
                        return
                    body = await response.text()
                    if len(body) < 50:
                        return
                    # Try JSON first
                    try:
                        data = json.loads(body)
                        if isinstance(data, dict):
                            keys_str = " ".join(str(k).lower() for k in data.keys())
                            if any(k in keys_str for k in ["flight", "itiner", "offer", "fare",
                                                             "bound", "trip", "result", "segment",
                                                             "avail", "journey", "price",
                                                             "recommendation", "proposal"]):
                                search_data.update(data)
                                api_event.set()
                                logger.info("RoyalJordanian: captured JSON API → %s (%d keys)", url[:80], len(data))
                    except json.JSONDecodeError:
                        pass
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            logger.info("RoyalJordanian: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(self.HOMEPAGE, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5.0)
            await _dismiss_overlays(page)

            # Compute date in YYYYMMDD format (RJ booking engine format)
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_yyyymmdd = dt.strftime("%Y%m%d")

            # Bypass the form UI: directly call the booking AJAX endpoint.
            # The submit handler POSTs to "redirect/to/Bookings/BookRestFlight"
            # with CSRF token and search params, gets back a redirect form HTML.
            ajax_result = await page.evaluate("""(args) => {
                return new Promise((resolve) => {
                    const [origin, dest, date1, adults, children, infants] = args;
                    const $ = window.jQuery;
                    if (!$) { resolve({error: 'no-jquery'}); return; }

                    const form = $('#onlineBookingSearchForm');
                    const url = form.data('online-booking-url') || 'redirect/to/Bookings/BookRestFlight';
                    const csrf = (window.AntiForgery?.CookieToken || '') + ':' + (window.AntiForgery?.FormToken || '');
                    const dsId = $("input[name='datasourceID']").val() || '';

                    $.ajax({
                        url: url,
                        type: 'post',
                        data: {
                            Org: origin,
                            Des: dest,
                            Date1: date1,
                            Date2: '',
                            Adult: String(adults),
                            Child: String(children),
                            Infant: String(infants),
                            Youth: '0',
                            Cabin: 'E',
                            Direct: false,
                            Flex: false,
                            PromoCode: '',
                            datasourceID: dsId,
                            Language: 'EN'
                        },
                        headers: { RequestVerificationToken: csrf },
                        success: function(resp) {
                            try {
                                // Response contains a redirect <form>
                                const div = $('<div>').html(resp);
                                const redirectForm = div.find('form');
                                if (redirectForm.length) {
                                    const action = redirectForm.attr('action') || '';
                                    const fields = {};
                                    redirectForm.find('input').each(function() {
                                        fields[$(this).attr('name')] = $(this).val();
                                    });
                                    resolve({ok: true, action: action, fields: fields});
                                } else {
                                    resolve({ok: false, error: 'no-form-in-response', resp: String(resp).slice(0, 500)});
                                }
                            } catch(e) {
                                resolve({ok: false, error: e.message, resp: String(resp).slice(0, 500)});
                            }
                        },
                        error: function(xhr, status, err) {
                            resolve({ok: false, error: status + ':' + err, status: xhr.status});
                        }
                    });

                    // Timeout after 15 seconds
                    setTimeout(() => resolve({ok: false, error: 'timeout'}), 15000);
                });
            }""", [req.origin, req.destination, date_yyyymmdd, req.adults, req.children or 0, req.infants or 0])

            logger.info("RoyalJordanian: AJAX result → ok=%s, action=%s",
                        ajax_result.get('ok'), str(ajax_result.get('action', ''))[:100])

            if ajax_result.get('ok') and ajax_result.get('action'):
                # Navigate to the booking engine via the redirect form
                action_url = ajax_result['action']
                fields = ajax_result.get('fields', {})

                # Submit the redirect form by navigating
                async with page.expect_navigation(timeout=30000, wait_until="domcontentloaded"):
                    await page.evaluate("""(args) => {
                        const [action, fields] = args;
                        const form = document.createElement('form');
                        form.method = 'POST';
                        form.action = action;
                        for (const [name, value] of Object.entries(fields)) {
                            const input = document.createElement('input');
                            input.type = 'hidden';
                            input.name = name;
                            input.value = value || '';
                            form.appendChild(input);
                        }
                        document.body.appendChild(form);
                        form.submit();
                    }""", [action_url, fields])
                logger.info("RoyalJordanian: redirected to booking engine → %s", page.url[:150])
            elif ajax_result.get('error'):
                logger.warning("RoyalJordanian: AJAX error → %s", ajax_result.get('error'))
                logger.info("RoyalJordanian: resp → %s", str(ajax_result.get('resp', ''))[:200])

            # Wait for booking engine page to load and render results
            await asyncio.sleep(8.0)
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                # Check if page has results loaded
                has_results = await page.evaluate("""() => {
                    return document.querySelectorAll(
                        '[class*="bound"], [class*="flight"], [class*="recommendation"], [class*="avail"], [class*="itinerary"], [class*="offer"], [class*="result"], [class*="fare-row"], [class*="price"]'
                    ).length > 3;
                }""")
                if has_results:
                    logger.info("RoyalJordanian: results detected in DOM")
                    await asyncio.sleep(2.0)
                    break
                await asyncio.sleep(2.0)

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
            logger.info("RoyalJordanian %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"royaljordanian{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("RoyalJordanian error: %s", e)
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
        """Fill Royal Jordanian flexselect autocomplete field."""
        try:
            field = page.locator(selector)
            await field.click(timeout=5000)
            await asyncio.sleep(0.5)
            await field.fill("")
            await field.type(iata, delay=100)
            await asyncio.sleep(2.0)

            # Click matching autocomplete suggestion
            selected = await page.evaluate("""(iata) => {
                const opts = document.querySelectorAll(
                    '.flexselect_dropdown li, .ui-autocomplete li, ' +
                    '[class*="dropdown"] li, [class*="suggest"] li, ' +
                    '[role="option"]'
                );
                for (const o of opts) {
                    const text = (o.textContent || '').trim();
                    if (text.toUpperCase().includes(iata) && o.offsetHeight > 0) {
                        o.click(); return text.slice(0, 80);
                    }
                }
                return null;
            }""", iata)
            if not selected:
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.3)
                await page.keyboard.press("Enter")

            await asyncio.sleep(0.5)
            logger.info("RoyalJordanian: airport %s → %s", selector[-30:], selected or iata)
            return True
        except Exception as e:
            logger.warning("RoyalJordanian: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill Royal Jordanian date — try JS value set, then calendar click."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        try:
            iso = dt.strftime("%Y-%m-%d")
            dd_mm_yyyy = dt.strftime("%d/%m/%Y")
            target_day = str(dt.day)
            target_month = dt.strftime("%B %Y")  # e.g. "April 2026"

            # Try setting value directly via JS (readonly input — can't use fill())
            ok = await page.evaluate("""(args) => {
                const [iso, formatted] = args;
                const el = document.getElementById('range-date-picker');
                if (!el) return 'no-element';
                // Try flatpickr instance (may be stored various ways)
                const fp = el._flatpickr || (window.jQuery && jQuery(el).data('flatpickr'))
                         || (window.flatpickr && window.flatpickr.instanceById && window.flatpickr.instanceById(el.id));
                if (fp && typeof fp.setDate === 'function') {
                    fp.setDate(iso, true);
                    return 'flatpickr';
                }
                // Force-set via native setter (bypasses readonly)
                const nativeSetter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value'
                ).set;
                nativeSetter.call(el, formatted);
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                return 'native-setter';
            }""", [iso, dd_mm_yyyy])
            logger.info("RoyalJordanian: date JS set result: %s", ok)

            if ok == 'flatpickr':
                await asyncio.sleep(0.5)
                return True

            # Click input to open calendar, then navigate and click day
            await page.locator('#range-date-picker').click(timeout=5000)
            await asyncio.sleep(1.5)

            # Check if calendar opened
            cal_open = await page.evaluate("""() => {
                return document.querySelector(
                    '.flatpickr-calendar.open, .datepicker.active, ' +
                    '[class*="calendar"][class*="open"], [class*="calendar"][class*="visible"]'
                ) !== null;
            }""")

            if cal_open:
                # Navigate to target month
                for _ in range(12):
                    month_text = await page.evaluate("""() => {
                        const h = document.querySelectorAll(
                            '.flatpickr-month .flatpickr-current-month, ' +
                            '.datepicker-month, .month-title, ' +
                            '[class*="calendar"] [class*="month"], th[colspan]'
                        );
                        return [...h].map(e => e.textContent.trim()).join('|');
                    }""")
                    if target_month.lower() in month_text.lower():
                        break
                    await page.evaluate("""() => {
                        const n = document.querySelector(
                            '.flatpickr-next-month, .next-month, ' +
                            '[class*="next"], [aria-label*="Next"]'
                        );
                        if (n && n.offsetHeight > 0) n.click();
                    }""")
                    await asyncio.sleep(0.5)

                # Click the target day
                clicked = await page.evaluate("""(day) => {
                    const cells = document.querySelectorAll(
                        '.flatpickr-day, td.day, td[role="gridcell"], ' +
                        '[class*="calendar"] [class*="day"]'
                    );
                    for (const c of cells) {
                        const text = c.textContent.trim();
                        const disabled = c.classList.contains('flatpickr-disabled') ||
                                         c.classList.contains('disabled') ||
                                         c.classList.contains('prevMonthDay') ||
                                         c.classList.contains('nextMonthDay') ||
                                         c.getAttribute('aria-disabled') === 'true';
                        if (text === day && !disabled && c.offsetHeight > 0) {
                            c.click(); return true;
                        }
                    }
                    return false;
                }""", target_day)
                if clicked:
                    await asyncio.sleep(1.0)
                    logger.info("RoyalJordanian: date clicked via calendar → %s", iso)
                    return True

            # Final fallback: native setter already ran, log and proceed
            logger.info("RoyalJordanian: date set via native setter → %s", dd_mm_yyyy)
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("RoyalJordanian: date error: %s", e)
            return False

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []
        flights = (
            data.get("flights") or data.get("results") or data.get("itineraries") or
            data.get("flightInfos") or data.get("offers") or data.get("journeys") or
            data.get("routeList") or data.get("flightList") or []
        )
        if isinstance(flights, dict):
            for key in ("flights", "results", "itineraries", "options", "list"):
                if key in flights:
                    flights = flights[key]
                    break
            else:
                flights = [flights]
        if not isinstance(flights, list):
            flights = self._find_flights(data)
        for flight in flights:
            offer = self._build_offer(flight, req)
            if offer:
                offers.append(offer)
        return offers

    def _find_flights(self, data, depth=0) -> list:
        if depth > 4 or not isinstance(data, dict):
            return []
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                sample_keys = {str(k).lower() for k in val[0].keys()}
                if sample_keys & {"price", "fare", "flight", "departure", "segment", "leg"}:
                    return val
            elif isinstance(val, dict):
                result = self._find_flights(val, depth + 1)
                if result:
                    return result
        return []

    def _build_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        try:
            price = (
                flight.get("price") or flight.get("totalPrice") or
                flight.get("fare") or flight.get("amount") or
                flight.get("adultPrice") or 0
            )
            if isinstance(price, dict):
                price = price.get("amount") or price.get("total") or price.get("value") or 0
            price = float(price) if price else 0
            if price <= 0:
                return None

            currency = self._extract_currency(flight)

            segments_data = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
            if not isinstance(segments_data, list):
                segments_data = [flight]

            segments = []
            for seg in segments_data:
                dep_str = seg.get("departure") or seg.get("departureTime") or seg.get("depTime") or ""
                arr_str = seg.get("arrival") or seg.get("arrivalTime") or seg.get("arrTime") or ""
                dep_dt = self._parse_dt(dep_str, req.date_from)
                arr_dt = self._parse_dt(arr_str, req.date_from)
                airline_code = seg.get("airline") or seg.get("carrierCode") or seg.get("operatingCarrier") or self.IATA
                flight_no = seg.get("flightNumber") or seg.get("flightNo") or ""
                if flight_no and not flight_no.startswith(airline_code):
                    flight_no = f"{airline_code}{flight_no}"

                segments.append(FlightSegment(
                    airline=airline_code[:2], airline_name=self.AIRLINE_NAME if airline_code == self.IATA else airline_code,
                    flight_no=flight_no or self.IATA, origin=seg.get("origin") or seg.get("departureAirport") or req.origin,
                    destination=seg.get("destination") or seg.get("arrivalAirport") or req.destination,
                    departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                ))

            if not segments:
                return None

            route = FlightRoute(segments=segments, total_duration_seconds=0, stopovers=max(0, len(segments) - 1))
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{req.origin}_{req.destination}_{req.date_from}_{price}_{segments[0].flight_no}".encode()
            ).hexdigest()[:12]

            return FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=currency,
                price_formatted=f"{currency} {price:,.0f}", outbound=route, inbound=None,
                airlines=list({s.airline for s in segments}), owner_airline=self.IATA,
                booking_url=self._booking_url(req), is_locked=False,
                source=self.SOURCE, source_tier="free",
            )
        except Exception as e:
            logger.debug("RoyalJordanian: offer parse error: %s", e)
            return None

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
        """Scrape Amadeus booking engine results page."""
        await asyncio.sleep(3)
        flights = await page.evaluate(r"""(params) => {
            const [origin, destination] = params;
            const results = [];
            const seen = new Set();
            const cards = document.querySelectorAll(
                '.bound-table-flightline, [class*="bound-inner"], [class*="recommendation"], [class*="flight-row"], ' +
                '[class*="result-item"], [class*="flight-card"], [class*="itinerary"], ' +
                '[class*="avail-row"], [class*="fare-row"], [class*="offer-row"], ' +
                'tr.bound, tr.result, div.result, .c-flight-result, .c-offer, ' +
                '[class*="flight-item"], [class*="flightInfo"]'
            );
            for (const card of cards) {
                const text = card.innerText || '';
                if (text.length < 15 || text.length > 600) continue;
                const times = text.match(/\b(\d{1,2}:\d{2})\b/g) || [];
                if (times.length < 2) continue;
                const priceMatch = text.match(/(JOD|USD|EUR|GBP|[\$€£¥])\s*[\d,]+\.?\d*/i) ||
                                   text.match(/[\d,]+\.?\d*\s*(JOD|USD|EUR|GBP|[\$€£¥])/i) ||
                                   text.match(/[\d,]+\.?\d{0,2}/);
                if (!priceMatch) continue;
                const priceStr = priceMatch[0].replace(/[^0-9.]/g, '');
                const price = parseFloat(priceStr);
                if (!price || price <= 0 || price > 50000) continue;
                const key = times[0] + '-' + times[1];
                if (seen.has(key)) continue;
                seen.add(key);
                let currency = 'JOD';
                if (/USD|\$/.test(priceMatch[0])) currency = 'USD';
                else if (/EUR|€/.test(priceMatch[0])) currency = 'EUR';
                else if (/GBP|£/.test(priceMatch[0])) currency = 'GBP';
                const fnMatch = text.match(/\b(RJ\s*\d{2,4})\b/i) || text.match(/\b([A-Z]{2}\s*\d{2,4})\b/);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'RJ',
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
        return f"https://www.rj.com/en?from={req.origin}&to={req.destination}&date={date_str}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"royaljordanian{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
