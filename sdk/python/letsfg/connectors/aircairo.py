"""
Air Cairo connector — CDP Chrome + form fill + DOM scraping.

Air Cairo (IATA: SM) is an Egyptian low-cost carrier headquartered at
Cairo International Airport. Operates domestic Egyptian routes plus
regional flights to Saudi Arabia, UAE, Kuwait, Jordan, Turkey, and
European destinations (Germany, Italy, France, UK).

Strategy (CDP Chrome — form fill + search results scraping):
  1. Launch real Chrome via CDP (no headless — Laravel site may fingerprint).
  2. Navigate to aircairo.com/en-gl/book-flight.
  3. Wait for JS to populate the _csrf token (empty in static HTML).
  4. Fill booking form: departureFrom, departureTo, date, adult, tripType.
  5. Submit form → page navigates to /en-gl/search-results.
  6. Scrape flight result cards from DOM (price, times, flight number).

Discovered via probing (Jun 2026):
  - No reCAPTCHA on booking form (only on newsletter form).
  - CSRF token is populated by JavaScript after page load.
  - Form fields: departureFrom, departureTo, date, adult, child, infant, tripType.
  - Form action: /en-gl/search-results (or /{locale}/search-results).
  - jQuery 3.6.0 + app.bundle.04b9ee8.min.js frontend.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import subprocess
import time
from datetime import date, datetime, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_BASE = "https://www.aircairo.com"
_BOOK_PATH = "/en-gl/book-flight"
_CDP_PORT = 9487
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".aircairo_chrome_data"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
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

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
            _pw_instance = pw
            return _browser
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
            f"--remote-debugging-port={_CDP_PORT}",
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
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
        logger.info("AirCairo: Chrome on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


class AirCairoConnectorClient:
    """Air Cairo (SM) — Egyptian LCC, CDP Chrome + form fill + DOM scraping."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination, "destination": req.origin,
                "date_from": req.return_from, "return_from": None,
            })
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            dt = (
                req.date_from
                if isinstance(req.date_from, (datetime, date))
                else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            )
            if isinstance(dt, datetime):
                dt = dt.date()
        except (ValueError, TypeError):
            dt = date.today() + timedelta(days=30)

        date_str = dt.strftime("%Y-%m-%d")

        for attempt in range(2):
            try:
                offers = await self._do_search(req, date_str)
                if offers:
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info("AirCairo %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
                    return self._build_response(offers, req, date_str)
            except Exception as e:
                logger.warning("AirCairo attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        browser = await _get_browser()
        contexts = browser.contexts
        context = contexts[0] if contexts else await browser.new_context(
            viewport={"width": 1366, "height": 768}
        )
        page = await context.new_page()

        # Capture any JSON API responses the page makes during search
        api_data: list[dict] = []

        async def _on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            if response.status == 200 and "json" in ct:
                if any(k in url.lower() for k in ["/search", "/flight", "/result", "/availab", "/fare"]):
                    try:
                        body = await response.text()
                        if len(body) > 100:
                            data = json.loads(body)
                            api_data.append(data)
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            logger.info("AirCairo: loading booking page for %s→%s on %s", req.origin, req.destination, date_str)
            await page.goto(f"{_BASE}{_BOOK_PATH}", wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            # Dismiss cookie consent + terms modals + header overlay via JS
            await page.evaluate("""() => {
                // Remove cookie consent
                document.querySelectorAll('.cookie-banner, .cookie-consent, #cookieConsent')
                    .forEach(el => el.remove());
                // Remove terms modal and backdrop
                const m = document.querySelector('#termsModal');
                if (m) m.remove();
                document.querySelectorAll('.modal-backdrop').forEach(b => b.remove());
                document.body.classList.remove('modal-open');
                document.body.style.overflow = '';
            }""")
            await asyncio.sleep(0.5)

            # Fill form entirely via JS to avoid overlay/click issues
            filled = await page.evaluate("""([origin, dest, dateStr, adults]) => {
                // Set departure
                const depInput = document.querySelector('input[name="departureFrom"], #departureFrom');
                if (depInput) {
                    depInput.value = origin;
                    depInput.dispatchEvent(new Event('input', {bubbles: true}));
                    depInput.dispatchEvent(new Event('change', {bubbles: true}));
                }
                // Set arrival
                const arrInput = document.querySelector('input[name="departureTo"], #departureTo');
                if (arrInput) {
                    arrInput.value = dest;
                    arrInput.dispatchEvent(new Event('input', {bubbles: true}));
                    arrInput.dispatchEvent(new Event('change', {bubbles: true}));
                }
                // Set trip type
                const tripSel = document.querySelector('select[name="tripType"]');
                if (tripSel) {
                    tripSel.value = 'oneWay';
                    tripSel.dispatchEvent(new Event('change', {bubbles: true}));
                }
                // Set date
                const dateInput = document.querySelector('input[name="date"]');
                if (dateInput) {
                    dateInput.value = dateStr;
                    dateInput.dispatchEvent(new Event('input', {bubbles: true}));
                    dateInput.dispatchEvent(new Event('change', {bubbles: true}));
                }
                // Set adults
                const adultInput = document.querySelector('input[name="adult"], #adult, select[name="adult"]');
                if (adultInput) {
                    adultInput.value = String(adults);
                    adultInput.dispatchEvent(new Event('change', {bubbles: true}));
                }
                return {
                    dep: depInput ? depInput.value : null,
                    arr: arrInput ? arrInput.value : null,
                    date: dateInput ? dateInput.value : null,
                };
            }""", [req.origin, req.destination, date_str, req.adults or 1])
            logger.info("AirCairo: form filled via JS: %s", filled)
            await asyncio.sleep(0.5)

            # Submit the form via JS
            submitted = await page.evaluate("""() => {
                // Remove any remaining modals/overlays
                document.querySelectorAll('#termsModal, .modal-backdrop').forEach(el => el.remove());
                document.body.classList.remove('modal-open');
                // Submit form
                const form = document.querySelector('form');
                if (form) { form.submit(); return true; }
                const btn = document.querySelector('button[type="submit"], input[type="submit"]');
                if (btn) { btn.click(); return true; }
                return false;
            }""")
            logger.info("AirCairo: form submitted via JS: %s", submitted)

            # Wait for results page
            await page.wait_for_load_state("domcontentloaded", timeout=15000)
            await asyncio.sleep(3)

            # Try API data first
            if api_data:
                for data in api_data:
                    offers = self._parse_api_data(data, req, date_str)
                    if offers:
                        return offers

            # DOM scraping for flight results
            offers = await self._extract_from_dom(page, req, date_str)
            return offers

        except Exception as e:
            logger.error("AirCairo browser error: %s", e)
            return []
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _extract_from_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Scrape flight result cards from the search results page."""
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        # Get page HTML for parsing
        html = await page.content()

        # Look for price elements — Air Cairo uses various selectors
        price_patterns = [
            r'(?:price|fare|cost|amount)["\s:]*?(\d[\d,]*\.?\d*)\s*(?:EGP|USD|EUR|GBP|SAR|AED)',
            r'(?:EGP|USD|EUR|GBP|SAR|AED)\s*(\d[\d,]*\.?\d*)',
            r'class="[^"]*price[^"]*"[^>]*>[\s\S]*?(\d[\d,]*\.?\d*)',
        ]

        # Try structured extraction via JS
        try:
            flight_data = await page.evaluate("""() => {
                const results = [];
                // Look for flight cards/rows
                const cards = document.querySelectorAll(
                    '.flight-card, .flight-row, .flight-result, .result-item, ' +
                    '[class*="flight"], [class*="result"], .search-result, ' +
                    'tr[class*="flight"], .booking-result'
                );
                for (const card of cards) {
                    const text = card.textContent || '';
                    // Extract price
                    const priceMatch = text.match(/(\\d[\\d,]*\\.?\\d*)\\s*(?:EGP|USD|EUR|GBP|SAR|AED)/i)
                        || text.match(/(?:EGP|USD|EUR|GBP|SAR|AED)\\s*(\\d[\\d,]*\\.?\\d*)/i);
                    if (!priceMatch) continue;

                    // Extract currency
                    const curMatch = text.match(/\\b(EGP|USD|EUR|GBP|SAR|AED)\\b/i);

                    // Extract flight number (SM followed by digits)
                    const fnMatch = text.match(/\\b(SM\\s*\\d{3,4})\\b/i);

                    // Extract times (HH:MM format)
                    const times = text.match(/\\b(\\d{1,2}:\\d{2})\\b/g) || [];

                    results.push({
                        price: priceMatch[1].replace(/,/g, ''),
                        currency: curMatch ? curMatch[1].toUpperCase() : 'EGP',
                        flightNo: fnMatch ? fnMatch[1].replace(/\\s/g, '') : '',
                        depTime: times[0] || '',
                        arrTime: times[1] || '',
                        text: text.substring(0, 500)
                    });
                }
                return results;
            }""")
        except Exception:
            flight_data = []

        if not flight_data:
            # Broader fallback: look for ANY price on page
            try:
                flight_data = await page.evaluate("""() => {
                    const text = document.body.textContent || '';
                    const results = [];
                    const priceMatches = text.matchAll(/(\\d[\\d,]*\\.?\\d*)\\s*(EGP|USD|EUR|GBP|SAR|AED)/gi);
                    for (const m of priceMatches) {
                        const price = parseFloat(m[1].replace(/,/g, ''));
                        if (price > 10 && price < 100000) {
                            results.push({
                                price: m[1].replace(/,/g, ''),
                                currency: m[2].toUpperCase(),
                                flightNo: '',
                                depTime: '',
                                arrTime: ''
                            });
                        }
                    }
                    return results;
                }""")
            except Exception:
                flight_data = []

        for fd in flight_data:
            try:
                price_f = round(float(fd["price"]), 2)
            except (ValueError, TypeError):
                continue
            if price_f <= 0:
                continue

            currency = fd.get("currency", "EGP")
            flight_no = fd.get("flightNo", "")
            dep_time_str = fd.get("depTime", "")
            arr_time_str = fd.get("arrTime", "")

            dedup_key = f"{req.origin}_{req.destination}_{date_str}_{price_f}_{flight_no}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            dep_dt = datetime.strptime(date_str, "%Y-%m-%d")
            arr_dt = dep_dt
            dur_sec = 0

            if dep_time_str and arr_time_str:
                try:
                    h, m = map(int, dep_time_str.split(":"))
                    dep_dt = dep_dt.replace(hour=h, minute=m)
                    h2, m2 = map(int, arr_time_str.split(":"))
                    arr_dt = arr_dt.replace(hour=h2, minute=m2)
                    dur_sec = max(0, int((arr_dt - dep_dt).total_seconds()))
                    if dur_sec < 0:
                        dur_sec += 86400
                        arr_dt = arr_dt + timedelta(days=1)
                except (ValueError, TypeError):
                    pass

            _sm_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="SM",
                airline_name="Air Cairo",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur_sec,
                cabin_class=_sm_cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=dur_sec, stopovers=0)

            fid = hashlib.md5(
                f"sm_{req.origin}{req.destination}{date_str}{price_f}{flight_no}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"sm_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Air Cairo"],
                owner_airline="SM",
                booking_url=(
                    f"https://www.aircairo.com/en-gl/book-flight?"
                    f"departureFrom={req.origin}&departureTo={req.destination}"
                    f"&date={date_str}&adult={req.adults or 1}"
                ),
                is_locked=False,
                source="aircairo_direct",
                source_tier="free",
            ))

        return offers

    def _parse_api_data(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse intercepted API JSON responses."""
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        flights = (
            data.get("flights", data.get("availability", data.get("fares", data.get("data", []))))
        )
        if isinstance(flights, dict):
            flights = flights.get("items", flights.get("journeys", flights.get("flights", [])))
        if not isinstance(flights, list):
            return offers

        for flight in flights:
            if not isinstance(flight, dict):
                continue
            price = (
                flight.get("price") or flight.get("totalPrice")
                or flight.get("fareAmount") or flight.get("amount")
            )
            if isinstance(price, dict):
                price = price.get("amount", price.get("value"))
            if not price:
                continue
            try:
                price_f = round(float(price), 2)
            except (ValueError, TypeError):
                continue
            if price_f <= 0:
                continue

            currency = flight.get("currency", "EGP")
            flight_no = str(flight.get("flightNumber", flight.get("number", "")))
            dep_time = str(flight.get("departureTime", flight.get("departure", date_str)))
            arr_time = str(flight.get("arrivalTime", flight.get("arrival", date_str)))

            dep_dt = self._parse_dt(dep_time, date_str)
            arr_dt = self._parse_dt(arr_time, date_str)
            dur_sec = max(0, int((arr_dt - dep_dt).total_seconds())) if arr_dt > dep_dt else 0

            dedup_key = f"{req.origin}_{req.destination}_{date_str}_{price_f}_{flight_no}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            _sm_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="SM", airline_name="Air Cairo", flight_no=flight_no,
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=arr_dt, duration_seconds=dur_sec,
                cabin_class=_sm_cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=dur_sec, stopovers=0)
            fid = hashlib.md5(f"sm_{dedup_key}".encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"sm_{fid}", price=price_f, currency=currency,
                price_formatted=f"{price_f:.2f} {currency}",
                outbound=route, inbound=None,
                airlines=["Air Cairo"], owner_airline="SM",
                booking_url=f"{_BASE}/en-gl/book-flight",
                is_locked=False, source="aircairo_direct", source_tier="free",
            ))

        return offers

    @staticmethod
    def _parse_dt(s: str, fallback_date: str) -> datetime:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(s[:19], fmt)
            except (ValueError, IndexError):
                continue
        try:
            return datetime.strptime(fallback_date, "%Y-%m-%d")
        except ValueError:
            return datetime(2000, 1, 1)

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, date_str: str) -> FlightSearchResponse:
        h = hashlib.md5(
            f"aircairo{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "EGP",
            offers=offers,
            total_results=len(offers),
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
                    id=f"rt_sm_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"aircairo{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EGP",
            offers=[],
            total_results=0,
        )
