"""
China Southern Airlines (CZ) — CDP Chrome connector — form fill + API intercept.

China Southern Airlines's website at www.csair.com uses a search widget with autocomplete
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

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9493
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".chinasouthern_chrome_data"
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
            logger.info("ChinaSouthern: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("ChinaSouthern: Chrome launched on CDP port %d", _DEBUG_PORT)

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
                '[class*="cookie"], [class*="consent"], [class*="overlay"]'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class ChinaSouthernConnectorClient:
    """China Southern Airlines (CZ) CDP Chrome connector."""

    IATA = "CZ"
    AIRLINE_NAME = "China Southern Airlines"
    SOURCE = "chinasouthern_direct"
    HOMEPAGE = "https://www.csair.com/en"
    DEFAULT_CURRENCY = "CNY"

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _get_context()
        page = await context.new_page()

        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            url = response.url.lower()
            if response.status not in (200, 201):
                return
            try:
                if any(k in url for k in ["/search", "/availability", "/flight",
                                           "/offer", "/fare", "/lowprice", "/schedule",
                                           "/queryinterflight", "/ita/rest", "/aoa/"]):
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct and "text" not in ct:
                        return
                    body = await response.text()
                    if len(body) < 50:
                        return
                    data = json.loads(body)
                    if not isinstance(data, dict):
                        return
                    keys_str = " ".join(str(k).lower() for k in data.keys())
                    if any(k in keys_str for k in ["flight", "itiner", "offer", "fare",
                                                     "bound", "trip", "result", "segment",
                                                     "avail", "journey", "price",
                                                     "dateflight", "success"]):
                        # Only update if this response has real data — don't let error responses overwrite good data
                        has_real_data = data.get("data") is not None
                        if has_real_data or not search_data:
                            search_data.update(data)
                            api_event.set()
                            logger.info("ChinaSouthern: captured API → %s (%d keys)", url[:80], len(data))
                        else:
                            logger.info("ChinaSouthern: skipping error response (no data key) → %s", url[:80])
            except Exception:
                pass

        page.on("response", _on_response)

        # Also listen on any new pages opened by search (China Southern often opens b2c.csair.com in new tab)
        def _attach_listener(new_page):
            new_page.on("response", _on_response)
        context.on("page", _attach_listener)

        try:
            logger.info("ChinaSouthern: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(self.HOMEPAGE, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5.0)
            await _dismiss_overlays(page)

            # One-way toggle — China Southern uses hidden .segtype field
            # segtype=1 is one-way (default), segtype=2 is round-trip
            await page.evaluate("""() => {
                const seg = document.querySelector('input.segtype');
                if (seg) {
                    const nativeSetter = Object.getOwnPropertyDescriptor(
                        window.HTMLInputElement.prototype, 'value'
                    ).set;
                    nativeSetter.call(seg, '1');
                    seg.dispatchEvent(new Event('change', {bubbles: true}));
                }
                // Also try clicking one-way text if visible
                const els = document.querySelectorAll('a, span, div, li');
                for (const el of els) {
                    const t = (el.textContent || '').trim().toLowerCase();
                    if ((t === 'one-way' || t === 'one way') && el.offsetHeight > 0) {
                        el.click(); break;
                    }
                }
            }""")
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, '#fDepCity', '#city1_code', req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_airport(page, '#fArrCity', '#city2_code', req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(1.0)

            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)

            # Click search — China Southern: search div/link/button
            await page.evaluate("""() => {
                const all = document.querySelectorAll('a, div, span, button, input[type="submit"], input[type="button"]');
                for (const el of all) {
                    const t = (el.textContent || el.value || '').trim().toLowerCase();
                    if ((t === 'search' || t === 'search flights' || t.includes('查询') || t.includes('搜索'))
                        && el.offsetHeight > 0 && el.offsetWidth > 30) {
                        el.click(); return;
                    }
                }
                // Fallback: submit the form directly
                const form = document.querySelector('form[name*="search"], form[class*="search"], form[action*="search"]');
                if (form) form.submit();
            }""")
            logger.info("ChinaSouthern: search clicked")

            # China Southern opens results in new tab at b2c.csair.com
            # Wait for the new page and attach response listener
            await asyncio.sleep(3.0)
            # Check all contexts and pages in the browser
            for ctx in _browser.contexts:
                for p in ctx.pages:
                    if p != page and "b2c.csair" in p.url:
                        p.on("response", _on_response)
                        logger.info("ChinaSouthern: attached listener to results page: %s", p.url[:80])
            # Also check direct context pages
            for p in context.pages:
                if p != page and "b2c.csair" in p.url:
                    try:
                        p.on("response", _on_response)
                    except Exception:
                        pass

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                url = page.url
                if any(k in url.lower() for k in ["result", "search", "flight", "availability", "booking", "ita"]):
                    await asyncio.sleep(8.0)
                    break
                await asyncio.sleep(1.0)

            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=8.0)
                except asyncio.TimeoutError:
                    pass

            offers = []
            if search_data:
                logger.info("ChinaSouthern: parsing %d keys: %s", len(search_data), list(search_data.keys())[:8])
                offers = self._parse_api_response(search_data, req)
            if not offers:
                offers = await self._scrape_dom(page, req)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("ChinaSouthern %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"chinasouthern{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("ChinaSouthern error: %s", e)
            return self._empty(req)
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass
            try:
                context.remove_listener("page", _attach_listener)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_airport(self, page, input_sel: str, code_sel: str, iata: str) -> bool:
        """Fill China Southern airport field with custom ui-city-input autocomplete."""
        try:
            field = page.locator(input_sel).first
            await field.click(timeout=5000)
            await asyncio.sleep(0.5)
            await page.keyboard.press("Control+a")
            await page.keyboard.press("Backspace")
            await field.type(iata, delay=120)
            await asyncio.sleep(2.0)

            # Click matching autocomplete suggestion
            selected = await page.evaluate("""(iata) => {
                const opts = document.querySelectorAll(
                    '.ui-city-panel li, [class*="suggest"] li, [class*="dropdown"] li, ' +
                    '[class*="autocomplete"] li, [role="option"], .search-result-item'
                );
                for (const o of opts) {
                    if (o.textContent.includes(iata) && o.offsetHeight > 0) {
                        o.click(); return true;
                    }
                }
                return false;
            }""", iata)
            if not selected:
                await page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.2)
                await page.keyboard.press("Enter")

            # Also set the hidden IATA code field
            await page.evaluate("""(args) => {
                const [sel, code] = args;
                const el = document.querySelector(sel);
                if (el) { el.value = code; }
            }""", [code_sel, iata])

            await asyncio.sleep(0.5)
            logger.info("ChinaSouthern: airport %s → %s", input_sel, iata)
            return True
        except Exception as e:
            logger.warning("ChinaSouthern: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill China Southern date field — direct YYYY-MM-DD value set."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        iso = dt.strftime("%Y-%m-%d")
        try:
            # China Southern #fDepDate accepts YYYY-MM-DD directly
            await page.evaluate("""(iso) => {
                const el = document.getElementById('fDepDate');
                if (!el) return;
                const nativeSetter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value'
                ).set;
                nativeSetter.call(el, iso);
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                // Also try jQuery trigger
                if (window.jQuery) { jQuery(el).val(iso).trigger('change'); }
            }""", iso)
            logger.info("ChinaSouthern: date set %s", iso)
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("ChinaSouthern: date error: %s", e)
            return False

    def _parse_api_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []

        # China Southern queryInterFlight structure: data.data.dateFlights[]
        inner = data.get("data")
        if isinstance(inner, dict):
            inner2 = inner.get("data")
            if isinstance(inner2, dict):
                date_flights = inner2.get("dateFlights")
                if isinstance(date_flights, list) and date_flights:
                    logger.info("ChinaSouthern: found %d dateFlights in queryInterFlight", len(date_flights))
                    return self._parse_query_inter_flight(date_flights, data, req)
                else:
                    logger.info("ChinaSouthern: inner2 keys: %s", list(inner2.keys())[:10])
            else:
                logger.info("ChinaSouthern: inner keys: %s, type(inner.data)=%s", list(inner.keys())[:10], type(inner.get("data")))

        # Generic fallback
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

    def _parse_query_inter_flight(self, date_flights: list, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse China Southern queryInterFlight API response."""
        offers = []
        # Get currency from first price entry or default
        currency = "CNY"

        for flight in date_flights:
            segments_raw = flight.get("segments") or []
            if not segments_raw:
                continue

            # Extract price from lowEconomyPrices (cheapest economy), fall back to prices
            price = 0.0
            price_currency = currency
            for price_key in ("lowEconomyPrices", "prices", "lowBfPrices"):
                price_list = flight.get(price_key)
                if isinstance(price_list, list) and price_list:
                    entry = price_list[0]
                    # displayPrice = fare + tax (total); adultSalePrice also works
                    p = entry.get("adultSalePrice") or entry.get("displayPrice") or entry.get("salePrice") or 0
                    price_currency = entry.get("saleCurrency") or entry.get("displayCurrency") or currency
                    if p and float(p) > 0:
                        price = float(p)
                        break

            if price <= 0:
                continue

            # Parse segments
            segments = []
            for seg in segments_raw:
                dep_date = seg.get("depDate") or str(req.date_from)
                dep_time = seg.get("depTime") or "00:00"
                arr_date = seg.get("arrDate") or dep_date
                arr_time = seg.get("arrTime") or "00:00"

                dep_dt = self._parse_dt(f"{dep_date}T{dep_time}" if "T" not in str(dep_time) else dep_time, req.date_from)
                arr_dt = self._parse_dt(f"{arr_date}T{arr_time}" if "T" not in str(arr_time) else arr_time, req.date_from)

                carrier = seg.get("carrier") or self.IATA
                fno = seg.get("flightNo") or ""
                full_fno = f"{carrier}{fno}" if fno and not fno.startswith(carrier) else (fno or f"{self.IATA}???")

                segments.append(FlightSegment(
                    airline=carrier[:2],
                    airline_name=seg.get("airlineName") or self.AIRLINE_NAME,
                    flight_no=full_fno,
                    origin=seg.get("depPort") or req.origin,
                    destination=seg.get("arrPort") or req.destination,
                    departure=dep_dt, arrival=arr_dt, cabin_class="economy",
                ))

            if not segments:
                continue

            # Duration from flyTime string like "4h5m"
            total_dur = 0
            fly_time = flight.get("flyTime") or ""
            m_h = re.search(r"(\d+)h", fly_time)
            m_m = re.search(r"(\d+)m", fly_time)
            if m_h:
                total_dur += int(m_h.group(1)) * 3600
            if m_m:
                total_dur += int(m_m.group(1)) * 60

            stopovers = flight.get("stopNumber") or flight.get("zzCount") or max(0, len(segments) - 1)
            route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=stopovers)
            offer_id = hashlib.md5(
                f"{self.IATA.lower()}_{segments[0].origin}_{segments[-1].destination}_{segments[0].departure}_{price}_{segments[0].flight_no}".encode()
            ).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"{self.IATA.lower()}_{offer_id}", price=round(price, 2), currency=price_currency,
                price_formatted=f"{price_currency} {price:,.0f}", outbound=route, inbound=None,
                airlines=list({s.airline for s in segments}), owner_airline=self.IATA,
                booking_url=self._booking_url(req), is_locked=False,
                source=self.SOURCE, source_tier="free",
            ))
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
            logger.debug("ChinaSouthern: offer parse error: %s", e)
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
                const fnMatch = text.match(/\b(CZ\s*\d{2,4})\b/i) || text.match(/\b([A-Z]{2}\s*\d{2,4})\b/);
                results.push({
                    depTime: times[0], arrTime: times[1], price, currency,
                    flightNo: fnMatch ? fnMatch[1].replace(/\s/g, '') : 'CZ',
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
        return f"https://www.csair.com/en?from={req.origin}&to={req.destination}&date={date_str}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"chinasouthern{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
