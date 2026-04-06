"""
ITA Airways connector — CDP Chrome booking page + API interception.

ITA Airways (IATA: AZ) — FCO/MXP hubs, SkyTeam member.

Strategy:
  ITA uses the Lufthansa Group booking widget with Shadow DOM web components.
  We fill the form (origin, destination, date) and click "Find flights" to
  trigger the search. API responses are intercepted for offers.

  Correct booking URL: /gb/en/book-and-prepare/book-flights.html
  (NOT /en_gb/book-a-flight.html which returns 404)

  Form elements:
  - Trip type: dropdown button "Round trip" → select "One-way"  
  - Origin: input[name="flightQuery.flightSegments[0].originCode"]
  - Destination: input[name="flightQuery.flightSegments[0].destinationCode"]
  - Date: maui-input (shadow DOM) → opens date-picker-with-prices
  - Search: "Find flights" button
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
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
from .browser import (
    _launched_procs,
    _launched_pw_instances,
    auto_block_if_proxied,
    find_chrome,
    proxy_chrome_args,
    stealth_popen_kwargs,
)

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9470
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", ".ita_chrome_profile"
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
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            _launched_pw_instances.append(pw)
            logger.info("ITA: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            f"--remote-debugging-port={_DEBUG_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--no-first-run",
            *proxy_chrome_args(),
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.5)

        pw = await async_playwright().start()
        _pw_instance = pw
        _launched_pw_instances.append(pw)
        _browser = await pw.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_DEBUG_PORT}"
        )
        logger.info(
            "ITA: Chrome launched on CDP port %d (pid %d)",
            _DEBUG_PORT,
            _chrome_proc.pid,
        )
        return _browser


class ITAAirwaysConnectorClient:
    """ITA Airways — CDP Chrome booking page + API interception."""

    def __init__(self, timeout: float = 120.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            browser = await _get_browser()
            context = (
                browser.contexts[0]
                if browser.contexts
                else await browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    locale="en-GB",
                )
            )
            page = await context.new_page()
            await auto_block_if_proxied(page)

            captured: dict = {}
            api_event = asyncio.Event()

            async def _on_response(response):
                url = response.url
                ct = response.headers.get("content-type", "")
                # Log ALL responses from api.shop domain regardless of status/content
                if "api.shop" in url:
                    logger.info(
                        "ITA: api.shop response: %s status=%d ct=%s",
                        url.split("?")[0], response.status, ct[:40],
                    )
                if response.status != 200:
                    return
                if "json" not in ct:
                    return
                try:
                    body = await response.body()
                    if len(body) < 200:
                        return
                    import json as _json
                    data = _json.loads(body)
                    if not isinstance(data, dict):
                        return
                    # Log all JSON responses from shop domain for debugging
                    if "shop.ita-airways.com" in url or "api.shop" in url:
                        logger.debug(
                            "ITA: shop response %s (%d bytes, keys=%s)",
                            url.split("/")[-1][:40], len(body),
                            list(data.keys())[:3],
                        )
                    # Primary: api.shop.ita-airways.com/one-booking/v2/search
                    # returns { data: { airBoundGroups: [...] }, dictionaries: {...} }
                    data_obj = data.get("data")
                    if isinstance(data_obj, dict) and "airBoundGroups" in data_obj:
                        captured["search"] = data
                        api_event.set()
                        logger.info(
                            "ITA: captured search API (%d bytes, %d groups)",
                            len(body),
                            len(data_obj["airBoundGroups"]),
                        )
                    # Secondary: fare families / fare teaser
                    elif any(k in data for k in (
                        "offers", "flights", "itineraries",
                        "outboundFlights", "lowfares", "boundList", "journeys",
                    )):
                        captured.setdefault("offers", data)
                        api_event.set()
                        logger.info("ITA: captured offers API (%d bytes)", len(body))
                except Exception:
                    pass

            page.on("response", _on_response)

            # Also listen on context level — captures responses from new pages
            # (ITA may open shop.ita-airways.com in the same tab via full navigation,
            # but the CDP page object stays the same; however, if a new page opens
            # we need to catch it too)
            _tracked_pages = {id(page)}

            def _on_new_page(new_page):
                if id(new_page) not in _tracked_pages:
                    _tracked_pages.add(id(new_page))
                    new_page.on("response", _on_response)
                    logger.info("ITA: attached response listener to new page: %s", new_page.url[:80])

            context.on("page", _on_new_page)

            # Navigate to correct booking page (NOT /en_gb/ which is 404!)
            logger.info("ITA: navigating to booking page")
            await page.goto(
                "https://www.ita-airways.com/gb/en/book-and-prepare/book-flights.html",
                wait_until="networkidle", timeout=35000,
            )
            await asyncio.sleep(4.0)

            # Accept cookies
            await page.evaluate("""() => {
                const onetrust = document.querySelector('#onetrust-accept-btn-handler');
                if (onetrust) { onetrust.click(); return true; }
                const btns = document.querySelectorAll('button, a');
                for (const b of btns) {
                    const t = (b.innerText || '').toLowerCase().trim();
                    if (t.includes('accept all') || t === 'accept') {
                        b.click(); return true;
                    }
                }
                return false;
            }""")
            await asyncio.sleep(1.5)

            # --- Step 1: Select "One-way" trip type ---
            await page.evaluate("""() => {
                // Click "Round trip" dropdown button
                for (const b of document.querySelectorAll('button')) {
                    if ((b.innerText || '').trim() === 'Round trip') { b.click(); return; }
                }
            }""")
            await asyncio.sleep(0.8)
            await page.evaluate("""() => {
                // Select "One-way" — may be li.sel-item or div[role="option"]
                for (const el of document.querySelectorAll('li.sel-item, [role="option"]')) {
                    if ((el.innerText || '').trim() === 'One-way') { el.click(); return; }
                }
            }""")
            await asyncio.sleep(0.5)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
            # Force-remove any remaining dropdown overlays
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '.flight-tab2, [class*="dropdown-menu"], [role="listbox"]'
                ).forEach(el => { el.style.display = 'none'; });
                document.body.click();
            }""")
            await asyncio.sleep(0.5)

            # --- Step 2: Fill origin airport ---
            # Use evaluate() to click and type — avoids Playwright overlay checks
            await page.evaluate("""() => {
                const inp = document.querySelector('input[placeholder="From"]');
                if (inp) { inp.focus(); inp.select(); }
            }""")
            await asyncio.sleep(0.2)
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.2)
            await page.keyboard.type(req.origin, delay=100)
            await asyncio.sleep(2.5)
            await page.evaluate("""(code) => {
                for (const li of document.querySelectorAll('li.sel-item, [role="option"]')) {
                    const t = (li.innerText || '').toLowerCase();
                    if (t.includes(code.toLowerCase())) { li.click(); return; }
                }
                document.querySelector('li.sel-item')?.click();
            }""", req.origin)
            await asyncio.sleep(1.0)

            # --- Step 3: Fill destination airport ---
            await page.evaluate("""() => {
                const inp = document.querySelector('input[placeholder="To"]');
                if (inp) { inp.focus(); inp.select(); }
            }""")
            await asyncio.sleep(0.2)
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.2)
            await page.keyboard.type(req.destination, delay=100)
            await asyncio.sleep(2.5)
            await page.evaluate("""(code) => {
                for (const li of document.querySelectorAll('li.sel-item, [role="option"]')) {
                    const t = (li.innerText || '').toLowerCase();
                    if (t.includes(code.toLowerCase())) { li.click(); return; }
                }
                document.querySelector('li.sel-item')?.click();
            }""", req.destination)
            await asyncio.sleep(1.0)

            # --- Step 4: Set departure date via calendar ---
            target_day = req.date_from.day
            target_month = req.date_from.strftime("%B")  # e.g., "June"

            # Helper JS to detect calendar open state by looking for calendar-specific elements
            CAL_CHECK_JS = """() => {
                // Look for calendar container: ReactModal, DayPicker, date-picker, etc.
                const calContainers = document.querySelectorAll(
                    '.ReactModal__Content, .DayPicker, [class*="calendar"], [class*="datepicker"], [class*="date-picker"], [role="dialog"]'
                );
                // Also check for td elements within a month grid (calendars have many tds)
                const tds = document.querySelectorAll('td');
                const calTds = [...tds].filter(td => {
                    const t = (td.innerText || '').trim();
                    return /^\\d{1,2}$/.test(t) && parseInt(t) >= 1 && parseInt(t) <= 31;
                });
                // Check for month names in calendar containers or near calendar elements
                const months = ['January','February','March','April','May','June',
                    'July','August','September','October','November','December'];
                let calText = '';
                for (const c of calContainers) {
                    calText += ' ' + (c.innerText || '');
                }
                // If no calendar container found, check if there are many day-number tds
                // (which indicates the calendar is open)
                if (!calText.trim() && calTds.length > 10) {
                    // Use nearby text from the day-picker area
                    const parent = calTds[0]?.closest('table')?.parentElement?.parentElement;
                    if (parent) calText = parent.innerText || '';
                    if (!calText) calText = document.body.innerText;
                }
                const visMonths = months.filter(m => calText.includes(m));
                return { months: visMonths, calTds: calTds.length, containers: calContainers.length };
            }"""

            # Try evaluate() to find and click the "Departure" element
            await page.evaluate("""() => {
                for (const el of document.querySelectorAll('*')) {
                    const text = (el.innerText || el.textContent || '').trim();
                    const rect = el.getBoundingClientRect();
                    if (text === 'Departure' && rect.y > 300 && rect.y < 600 && rect.width > 50) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }""")
            await asyncio.sleep(2.0)

            # Check if calendar opened
            cal_info = await page.evaluate(CAL_CHECK_JS)

            if cal_info.get("calTds", 0) < 10:
                # Calendar not open — try mouse click on the Departure field area
                logger.info("ITA: calendar not detected (tds=%d), trying mouse fallback", cal_info.get("calTds", 0))
                await page.mouse.click(830, 438)
                await asyncio.sleep(2.0)
                cal_info = await page.evaluate(CAL_CHECK_JS)

            if cal_info.get("calTds", 0) < 10:
                # Last resort: try clicking various date-related selectors
                for sel in ['.date-input', 'maui-input[type*="date"]', 'maui-input', '[class*="date"]', '[class*="calendar"]']:
                    try:
                        els = await page.query_selector_all(sel)
                        for el in els:
                            try:
                                await el.click()
                                await asyncio.sleep(1.5)
                                cal_info = await page.evaluate(CAL_CHECK_JS)
                                if cal_info.get("calTds", 0) >= 10:
                                    break
                            except Exception:
                                pass
                        if cal_info.get("calTds", 0) >= 10:
                            break
                    except Exception:
                        pass

            logger.info("ITA: calendar state: %s", cal_info)

            # Navigate to target month using the forward arrow button
            cal_open = cal_info.get("calTds", 0) >= 10
            if cal_open:
                for nav_attempt in range(8):
                    vis_months = await page.evaluate(CAL_CHECK_JS)
                    if target_month in vis_months.get("months", []):
                        logger.info("ITA: found %s after %d nav clicks (visible: %s)",
                                    target_month, nav_attempt, vis_months.get("months"))
                        break

                    # Primary: click the arrow by aria-label
                    arrow_clicked = await page.evaluate("""() => {
                        const btn = document.querySelector(
                            'button[aria-label="Move forward to switch to the next month"]'
                        );
                        if (btn) { btn.click(); return true; }
                        for (const b of document.querySelectorAll('button, [role="button"]')) {
                            const al = (b.getAttribute('aria-label') || '').toLowerCase();
                            if (al.includes('next') || al.includes('forward')) {
                                b.click(); return true;
                            }
                        }
                        return false;
                    }""")
                    if not arrow_clicked:
                        await page.mouse.click(1020, 385)
                    await asyncio.sleep(0.7)
            else:
                logger.warning("ITA: calendar not open (calTds=%d), skipping date selection",
                               cal_info.get("calTds", 0))

            # Click the target day — pick rightmost td matching (target month side)
            await asyncio.sleep(0.5)
            day_clicked = await page.evaluate("""(dayNum) => {
                const tds = document.querySelectorAll('td');
                const matches = [];
                for (const d of tds) {
                    const text = (d.innerText || '').trim();
                    if (text === String(dayNum)) {
                        const rect = d.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0) {
                            matches.push({el: d, left: rect.x});
                        }
                    }
                }
                matches.sort((a, b) => b.left - a.left);
                if (matches.length > 0) { matches[0].el.click(); return true; }
                return false;
            }""", target_day)
            logger.info("ITA: day click result: %s", day_clicked)
            await asyncio.sleep(0.5)

            # Close calendar
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.5)
            # Force-remove ReactModal overlay that blocks clicks
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '.ReactModal__Overlay, .modal-backdrop, .modal-calendar'
                ).forEach(el => el.remove());
            }""")
            await asyncio.sleep(0.5)

            # --- Step 5: Click "Find flights" button ---
            # ITA uses <maui-button> custom web component, not standard <button>.
            # Use evaluate() which bypasses overlay interception checks.
            searched = await page.evaluate("""() => {
                for (const el of document.querySelectorAll('maui-button')) {
                    const t = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    if (t.includes('find flight')) { el.click(); return t; }
                }
                // Fallback: standard buttons or any element with matching text
                for (const el of document.querySelectorAll('button, [role="button"]')) {
                    const t = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    if (t.includes('find flight') || t.includes('search flight')) {
                        el.click(); return t;
                    }
                }
                return null;
            }""")
            logger.info("ITA: search button clicked: %s", searched)

            # Wait for navigation to shop.ita-airways.com first
            for _nav_i in range(25):
                await asyncio.sleep(1)
                try:
                    cur_url = page.url
                except Exception:
                    cur_url = ""
                if "shop.ita-airways.com" in cur_url or "availability" in cur_url:
                    logger.info("ITA: navigated to results: %s", cur_url[:80])
                    break
                # Also check if a new page opened with results
                for p in context.pages:
                    try:
                        pu = p.url
                    except Exception:
                        continue
                    if "shop.ita-airways.com" in pu:
                        logger.info("ITA: results in new page: %s", pu[:80])
                        if id(p) not in _tracked_pages:
                            _tracked_pages.add(id(p))
                            p.on("response", _on_response)
                        page = p  # switch to results page
                        break
                if _nav_i % 5 == 0:
                    logger.debug("ITA: waiting for navigation (%d)... url=%s", _nav_i, cur_url[:60])

            # Now wait for the search API response (arrives 5-20s after page load)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=40)
            except asyncio.TimeoutError:
                # Try waiting a bit more — the search API can be slow
                logger.info("ITA: API not yet captured, waiting 15s more...")
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=15)
                except asyncio.TimeoutError:
                    logger.warning("ITA: API interception timed out after 55s total")

            offers: list[FlightOffer] = []
            if "search" in captured:
                offers = self._parse_search(captured["search"], req)
            elif "offers" in captured:
                offers = self._parse_offers(captured["offers"], req)

            # Fallback: scrape DOM for prices on results page
            if not offers:
                offers = await self._scrape_dom(page, req)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info(
                "ITA %s→%s: %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            sh = hashlib.md5(
                f"ita{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else "EUR",
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.warning("ITA search error: %s", e)
            return self._empty(req)
        finally:
            try:
                context.remove_listener("page", _on_new_page)
                page.remove_listener("response", _on_response)
                await page.close()
            except Exception:
                pass

    def _parse_search(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse api.shop.ita-airways.com/one-booking/v2/search response.

        Structure:
          data.airBoundGroups[]: each is a route option (direct or connecting)
            boundDetails: { originLocationCode, destinationLocationCode, duration, segments: [{flightId}] }
            airBounds[]: fare options per route (economy, classic, flex, business)
              airOffer.totalPrice: { value (cents!), currencyCode }
              fareInfos, availabilityDetails, fareFamilyCode
          dictionaries.flight: { flightId: { departure, arrival, marketingAirlineCode, ... } }
          dictionaries.airline: { code: name }
        """
        offers: list[FlightOffer] = []
        booking_url = self._booking_url(req)

        search_data = data.get("data", {})
        dicts = data.get("dictionaries", {})
        flight_dict = dicts.get("flight", {})
        airline_dict = dicts.get("airline", {})
        groups = search_data.get("airBoundGroups", [])

        if not groups:
            return offers

        seen_keys: set[str] = set()

        for group in groups:
            bd = group.get("boundDetails", {})
            origin_code = bd.get("originLocationCode", req.origin)
            dest_code = bd.get("destinationLocationCode", req.destination)
            route_dur = bd.get("duration", 0)
            seg_refs = bd.get("segments", [])

            # Build segments from flight dictionary
            segments: list[FlightSegment] = []
            airlines_set: set[str] = set()
            for sref in seg_refs:
                fid = sref.get("flightId", "")
                fl = flight_dict.get(fid, {})
                dep = fl.get("departure", {})
                arr = fl.get("arrival", {})
                mkt_code = fl.get("marketingAirlineCode", "AZ")
                mkt_num = fl.get("marketingFlightNumber", "")
                airline_name = airline_dict.get(mkt_code, "ITA Airways")
                airlines_set.add(airline_name)

                segments.append(FlightSegment(
                    airline=mkt_code,
                    airline_name=airline_name,
                    flight_no=f"{mkt_code}{mkt_num}",
                    origin=dep.get("locationCode", origin_code),
                    destination=arr.get("locationCode", dest_code),
                    departure=self._parse_dt(dep.get("dateTime")),
                    arrival=self._parse_dt(arr.get("dateTime")),
                ))

            stopovers = max(len(segments) - 1, 0)
            route = FlightRoute(
                segments=segments or [FlightSegment(
                    airline="AZ", airline_name="ITA Airways", flight_no="AZ",
                    origin=req.origin, destination=req.destination,
                    departure=datetime.combine(req.date_from, datetime.min.time().replace(hour=8)),
                    arrival=datetime.combine(req.date_from, datetime.min.time().replace(hour=8)),
                )],
                total_duration_seconds=route_dur,
                stopovers=stopovers,
            )

            # Take cheapest fare per group (first airBound — usually isCheapestOffer)
            for ab in group.get("airBounds", []):
                tp = ab.get("airOffer", {}).get("totalPrice", {})
                price_cents = tp.get("value")
                currency = tp.get("currencyCode", "EUR")

                if price_cents is None:
                    # Fallback to prices.totalPrices
                    total_prices = ab.get("prices", {}).get("totalPrices", [])
                    if total_prices:
                        price_cents = total_prices[0].get("total")
                        currency = total_prices[0].get("currencyCode", "EUR")

                if price_cents is None or price_cents <= 0:
                    continue

                # Prices are in cents
                price = round(price_cents / 100, 2)

                ff_code = ab.get("fareFamilyCode", "")
                dedup_key = f"az_{origin_code}{dest_code}{price}{ff_code}"
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)

                oid = hashlib.md5(dedup_key.encode()).hexdigest()[:12]
                airlines_list = sorted(airlines_set) if airlines_set else ["ITA Airways"]
                offers.append(FlightOffer(
                    id=f"az_{oid}",
                    price=price,
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=airlines_list,
                    owner_airline="AZ",
                    booking_url=booking_url,
                    is_locked=False,
                    source="itaairways_direct",
                    source_tier="free",
                ))
                # Only keep cheapest fare per group to avoid duplicating routes
                break

        return offers

    def _parse_offers(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse ITA availability/offers API response (legacy fallback)."""
        offers: list[FlightOffer] = []
        booking_url = self._booking_url(req)

        raw = (
            data.get("offers") or data.get("flights")
            or data.get("itineraries") or data.get("recommendations")
            or data.get("outboundFlights") or data.get("boundList")
            or data.get("journeys") or []
        )
        if isinstance(raw, dict):
            raw = list(raw.values())
        if not isinstance(raw, list):
            return offers

        for item in raw:
            if not isinstance(item, dict):
                continue
            price = (
                item.get("price") or item.get("totalPrice")
                or item.get("amount") or item.get("cheapestPrice")
                or item.get("lowestPrice")
            )
            if isinstance(price, dict):
                currency = price.get("currency") or price.get("currencyCode") or "EUR"
                price = price.get("amount") or price.get("value") or price.get("total")
            else:
                currency = item.get("currency") or item.get("currencyCode") or "EUR"
            if price is None:
                continue
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue

            segments = self._extract_segments(item, req)
            total_dur = 0
            if segments and segments[0].departure and segments[-1].arrival:
                total_dur = max(
                    int((segments[-1].arrival - segments[0].departure).total_seconds()), 0
                )

            route = FlightRoute(
                segments=segments or [FlightSegment(
                    airline="AZ", airline_name="ITA Airways",
                    flight_no="AZ", origin=req.origin, destination=req.destination,
                    departure=datetime.combine(req.date_from, datetime.min.time().replace(hour=8)),
                    arrival=datetime.combine(req.date_from, datetime.min.time().replace(hour=8)),
                )],
                total_duration_seconds=total_dur,
                stopovers=max(len(segments) - 1, 0) if segments else 0,
            )

            key = f"az_{req.origin}{req.destination}{price}{currency}"
            oid = hashlib.md5(key.encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"az_{oid}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["ITA Airways"],
                owner_airline="AZ",
                booking_url=booking_url,
                is_locked=False,
                source="itaairways_direct",
                source_tier="free",
            ))
        return offers

    def _parse_fares(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse fare teaser / lowest fares response."""
        offers: list[FlightOffer] = []
        booking_url = self._booking_url(req)

        # Fare teaser responses can have various structures
        fares = (
            data.get("lowfares") or data.get("fares")
            or data.get("offers") or data.get("results") or []
        )
        if isinstance(fares, dict):
            fares = list(fares.values())
        if not isinstance(fares, list):
            return offers

        for fare in fares:
            if not isinstance(fare, dict):
                continue
            price = fare.get("price") or fare.get("amount") or fare.get("totalPrice")
            if isinstance(price, dict):
                currency = price.get("currency") or "EUR"
                price = price.get("amount") or price.get("value")
            else:
                currency = fare.get("currency") or fare.get("currencyCode") or "EUR"
            if price is None:
                continue
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue

            dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
            seg = FlightSegment(
                airline="AZ", airline_name="ITA Airways", flight_no="AZ",
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=dep_dt,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
            key = f"az_fare_{req.origin}{req.destination}{price}"
            oid = hashlib.md5(key.encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"az_{oid}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["ITA Airways"],
                owner_airline="AZ",
                conditions={"price_type": "starting_from"},
                booking_url=booking_url,
                is_locked=False,
                source="itaairways_direct",
                source_tier="free",
            ))
        return offers

    def _extract_segments(self, item: dict, req: FlightSearchRequest) -> list[FlightSegment]:
        segments: list[FlightSegment] = []
        raw = (
            item.get("segments") or item.get("legs") or item.get("flights") or []
        )
        if isinstance(raw, dict):
            # Might be nested under bounds
            raw = raw.get("outbound", raw.get("segments", []))
        if not isinstance(raw, list):
            return segments
        for seg in raw:
            if not isinstance(seg, dict):
                continue
            dep_str = seg.get("departure") or seg.get("departureTime") or seg.get("departureDateTime") or ""
            arr_str = seg.get("arrival") or seg.get("arrivalTime") or seg.get("arrivalDateTime") or ""
            fn = str(seg.get("flightNumber") or seg.get("flightNo") or "AZ").strip()
            origin = seg.get("origin") or seg.get("departureAirport") or req.origin
            if isinstance(origin, dict):
                origin = origin.get("code") or origin.get("iata") or req.origin
            dest = seg.get("destination") or seg.get("arrivalAirport") or req.destination
            if isinstance(dest, dict):
                dest = dest.get("code") or dest.get("iata") or req.destination
            segments.append(FlightSegment(
                airline="AZ", airline_name="ITA Airways", flight_no=fn,
                origin=origin, destination=dest,
                departure=self._parse_dt(dep_str),
                arrival=self._parse_dt(arr_str),
            ))
        return segments

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: scrape pricing from page DOM."""
        booking_url = self._booking_url(req)
        try:
            data = await page.evaluate(r"""() => {
                const results = [];
                const body = document.body?.innerText || '';
                const priceRe = /(?:EUR|€)\s*([\d.,]+)|(\d{1,4}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:EUR|€)/gi;
                let m;
                while ((m = priceRe.exec(body)) !== null) {
                    const raw = (m[1] || m[2] || '').replace(/\./g, '').replace(',', '.');
                    const p = parseFloat(raw);
                    if (p > 15 && p < 10000) results.push(p);
                }
                return [...new Set(results)].sort((a, b) => a - b).slice(0, 10);
            }""")
            offers = []
            for price in (data or []):
                dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
                seg = FlightSegment(
                    airline="AZ", airline_name="ITA Airways", flight_no="AZ",
                    origin=req.origin, destination=req.destination,
                    departure=dep_dt, arrival=dep_dt,
                )
                route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
                key = f"az_dom_{req.origin}{req.destination}{price}"
                oid = hashlib.md5(key.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"az_{oid}",
                    price=round(price, 2),
                    currency="EUR",
                    price_formatted=f"{price:.2f} EUR",
                    outbound=route,
                    inbound=None,
                    airlines=["ITA Airways"],
                    owner_airline="AZ",
                    conditions={"price_type": "starting_from"},
                    booking_url=booking_url,
                    is_locked=False,
                    source="itaairways_direct",
                    source_tier="free",
                ))
            return offers
        except Exception as e:
            logger.warning("ITA DOM scrape failed: %s", e)
            return []

    async def _navigate_calendar(self, page, target_date) -> None:
        """Navigate ITA's calendar modal and click target date.

        ITA uses a modal calendar that shows 2 months at a time (March/April style).
        We need to click the ">" arrow to navigate forward until we reach the target month,
        then click the day cell.
        """
        from datetime import date as _date
        target = target_date if isinstance(target_date, _date) else target_date
        target_month_name = target.strftime("%B")  # e.g. "June"
        target_day = str(target.day)

        logger.info("ITA: navigating calendar to %s %d", target_month_name, target.day)

        # Wait for calendar modal to fully render
        await asyncio.sleep(1.0)

        # Navigate to target month by clicking the forward arrow (">")
        for attempt in range(18):  # max 18 months ahead
            # Get currently visible month names
            visible_months = await page.evaluate("""() => {
                const months = [];
                // Look for month headers in the calendar (they have dropdowns with month names)
                document.querySelectorAll('button, span, div').forEach(el => {
                    const t = (el.innerText || '').trim();
                    // Match month names like "March", "April", "June"
                    if (/^(January|February|March|April|May|June|July|August|September|October|November|December)$/.test(t)) {
                        months.push(t);
                    }
                });
                return [...new Set(months)];
            }""")
            logger.debug("ITA calendar visible months: %s", visible_months)

            # Check if target month is visible
            if target_month_name in visible_months:
                logger.info("ITA: target month %s is visible", target_month_name)
                break

            # Click the forward arrow (looks like ">")
            clicked = await page.evaluate("""() => {
                // Find the ">" navigation button (usually an SVG or button)
                const arrows = document.querySelectorAll('svg, button, [role="button"]');
                for (const el of arrows) {
                    // Check if it's a right arrow by looking at SVG path or text
                    const html = el.outerHTML || '';
                    const text = (el.innerText || '').trim();
                    // Common patterns for forward/right arrow
                    if ((html.includes('chevron') && html.includes('right')) ||
                        (html.includes('arrow') && html.includes('right')) ||
                        text === '>' || text === '›' || text === '→') {
                        if (el.offsetParent !== null) {
                            el.click();
                            return 'arrow-text';
                        }
                    }
                }
                // Look for SVG with rightward-facing path (positive x direction)
                const svgs = document.querySelectorAll('svg');
                for (const svg of svgs) {
                    const rect = svg.getBoundingClientRect();
                    if (rect.width > 0 && rect.width < 40 && rect.height < 40) {
                        // Small SVG that might be a navigation arrow
                        const paths = svg.querySelectorAll('path');
                        for (const p of paths) {
                            const d = p.getAttribute('d') || '';
                            // Chevron right patterns often have positive x movements
                            if (d.includes('l') || d.includes('L')) {
                                // Click parent if it's a button
                                const parent = svg.closest('button, [role="button"]');
                                if (parent && parent.offsetParent !== null) {
                                    parent.click();
                                    return 'svg-parent';
                                }
                                svg.click();
                                return 'svg-direct';
                            }
                        }
                    }
                }
                return null;
            }""")
            
            if not clicked:
                # Fallback: try clicking by position (the ">" is usually on the right side of month header)
                clicked = await page.evaluate("""() => {
                    // Find the April month (or the rightmost month) and look for arrow after it
                    const monthHeaders = [];
                    document.querySelectorAll('button, span').forEach(el => {
                        const t = (el.innerText || '').trim();
                        if (/^(January|February|March|April|May|June|July|August|September|October|November|December)$/.test(t)) {
                            monthHeaders.push({el, rect: el.getBoundingClientRect()});
                        }
                    });
                    // Sort by x position (rightmost first)
                    monthHeaders.sort((a, b) => b.rect.x - a.rect.x);
                    if (monthHeaders.length > 0) {
                        // Look for clickable element to the right of the rightmost month
                        const rightmost = monthHeaders[0];
                        const rightX = rightmost.rect.right;
                        const y = rightmost.rect.top + rightmost.rect.height / 2;
                        // Find element at position slightly to the right
                        const arrows = document.querySelectorAll('svg, button, [role="button"]');
                        for (const el of arrows) {
                            const rect = el.getBoundingClientRect();
                            if (rect.left >= rightX - 20 && rect.left <= rightX + 60 && 
                                Math.abs(rect.top + rect.height/2 - y) < 30) {
                                el.click();
                                return 'positional';
                            }
                        }
                    }
                    return null;
                }""")

            if clicked:
                logger.debug("ITA: clicked forward arrow (%s)", clicked)
            else:
                logger.warning("ITA: could not find forward arrow (attempt %d)", attempt)
                
            await asyncio.sleep(0.5)

        # Now click the target day
        # First find which column the target month is in (could be left or right)
        day_clicked = await page.evaluate("""(args) => {
            const [monthName, dayNum] = args;
            
            // Find the month header for our target month
            let targetMonthHeader = null;
            document.querySelectorAll('button, span, div').forEach(el => {
                const t = (el.innerText || '').trim();
                if (t === monthName) {
                    targetMonthHeader = el;
                }
            });
            
            if (!targetMonthHeader) {
                return 'month-not-found:' + monthName;
            }
            
            const monthRect = targetMonthHeader.getBoundingClientRect();
            const monthCenterX = monthRect.left + monthRect.width / 2;
            
            // Now find day cells that are under this month column
            // Look for cells with just the day number
            const dayCells = document.querySelectorAll('td, div, span, button');
            const candidates = [];
            
            for (const c of dayCells) {
                const t = (c.innerText || '').trim();
                if (t === dayNum) {
                    const rect = c.getBoundingClientRect();
                    // Check if this cell is roughly under our month (within same column)
                    const cellCenterX = rect.left + rect.width / 2;
                    const xDiff = Math.abs(cellCenterX - monthCenterX);
                    // Calendar column width is typically 30-50px, month header spans ~200px
                    // So a cell is "under" a month if within ~150px
                    if (xDiff < 150 && rect.width < 60 && rect.width > 15) {
                        candidates.push({
                            el: c,
                            xDiff,
                            rect,
                            disabled: c.classList.contains('disabled') || c.hasAttribute('disabled')
                        });
                    }
                }
            }
            
            if (candidates.length === 0) {
                return 'no-candidates';
            }
            
            // Sort by x distance (closest to month center first)
            candidates.sort((a, b) => a.xDiff - b.xDiff);
            
            // Click the first non-disabled candidate
            for (const cand of candidates) {
                if (!cand.disabled) {
                    cand.el.click();
                    return 'clicked-day:' + dayNum;
                }
            }
            
            return 'all-disabled';
        }""", [target_month_name, target_day])

        logger.info("ITA: day click result: %s", day_clicked)
        await asyncio.sleep(0.5)

    async def _fill_airport(self, page, field: str, code: str) -> None:
        """Fill a departure or arrival airport via typeahead.

        ITA's booking form uses input fields inside containers. We locate
        them by placeholder text or data attributes, type the IATA code,
        then pick the first suggestion.
        """
        try:
            # Try common selectors for ITA's booking form inputs
            js = """(args) => {
                const [field, code] = args;
                const inputs = document.querySelectorAll('input[type="text"], input:not([type])');
                let target = null;
                for (const inp of inputs) {
                    const ph = (inp.placeholder || '').toLowerCase();
                    const lbl = (inp.getAttribute('aria-label') || '').toLowerCase();
                    const name = (inp.name || '').toLowerCase();
                    if (field === 'departure') {
                        if (ph.includes('depart') || ph.includes('from') || ph.includes('origin')
                            || lbl.includes('depart') || lbl.includes('from') || lbl.includes('origin')
                            || name.includes('depart') || name.includes('from') || name.includes('origin')) {
                            target = inp;
                            break;
                        }
                    } else {
                        if (ph.includes('arriv') || ph.includes('to') || ph.includes('dest')
                            || lbl.includes('arriv') || lbl.includes('to') || lbl.includes('dest')
                            || name.includes('arriv') || name.includes('to') || name.includes('dest')) {
                            target = inp;
                            break;
                        }
                    }
                }
                if (!target) {
                    // Fallback: departure = first, arrival = second text input
                    const all = [...inputs].filter(i => i.offsetParent !== null);
                    target = field === 'departure' ? all[0] : all[1];
                }
                if (!target) return null;
                // Clear and focus
                target.focus();
                target.value = '';
                target.dispatchEvent(new Event('input', {bubbles: true}));
                return target.getBoundingClientRect().toJSON();
            }"""
            rect = await page.evaluate(js, [field, code])
            if not rect:
                logger.warning("ITA: could not find %s input", field)
                return

            # Type the code slowly to trigger typeahead
            x = rect["x"] + rect["width"] / 2
            y = rect["y"] + rect["height"] / 2
            await page.mouse.click(x, y)
            await asyncio.sleep(0.3)
            await page.keyboard.type(code, delay=120)
            await asyncio.sleep(1.5)

            # Pick first suggestion
            picked = await page.evaluate("""() => {
                const items = document.querySelectorAll(
                    '[class*="suggestion"], [class*="autocomplete"] li, '
                    + '[class*="dropdown"] li, [role="option"], [role="listbox"] li, '
                    + '[class*="result"] li, [class*="search-list"] li'
                );
                if (items.length > 0) {
                    items[0].click();
                    return items[0].innerText;
                }
                return null;
            }""")
            if picked:
                logger.info("ITA: %s → %s (picked: %s)", field, code, picked.strip()[:30])
            else:
                # Try pressing Enter to accept
                await page.keyboard.press("Enter")
                logger.info("ITA: %s → %s (pressed Enter)", field, code)

        except Exception as e:
            logger.warning("ITA: fill %s failed: %s", field, e)

    async def _set_date(self, page, target_date) -> None:
        """Set the departure date via calendar widget.

        ITA uses a DayPicker calendar. We:
        1. Click the date input to open the calendar
        2. Navigate month-by-month using the next arrow
        3. Click the target date's cell by aria-label
        """
        from datetime import date as _date
        target = target_date if isinstance(target_date, _date) else target_date

        try:
            # Click the date input div to open calendar
            opened = await page.evaluate("""() => {
                const dateInputs = document.querySelectorAll(
                    '.date-input, [class*="date-picker"], input[type="date"], '
                    + '[class*="datepicker"], [data-testid*="date"]'
                );
                for (const el of dateInputs) {
                    if (el.offsetParent !== null) {
                        el.click();
                        return true;
                    }
                }
                // Fallback: look for calendar icon
                const icons = document.querySelectorAll(
                    '[class*="calendar"], [class*="icon-date"]'
                );
                for (const ic of icons) {
                    if (ic.offsetParent !== null) {
                        ic.click();
                        return true;
                    }
                }
                return false;
            }""")
            if not opened:
                logger.warning("ITA: could not open date picker")
                return
            await asyncio.sleep(1.0)

            # Navigate to target month — click next arrow repeatedly
            target_month_year = target.strftime("%B %Y")  # e.g. "June 2026"
            for _ in range(18):  # max 18 months ahead
                current = await page.evaluate("""() => {
                    // DayPicker captions
                    const captions = document.querySelectorAll(
                        '.DayPicker-Caption, [class*="month-caption"], '
                        + '[class*="calendar-header"], [class*="CalendarMonth_caption"]'
                    );
                    const texts = [];
                    for (const c of captions) texts.push(c.innerText.trim());
                    return texts;
                }""")
                if any(target_month_year.lower() in t.lower() for t in (current or [])):
                    break
                # Check if any caption contains just month name (no year)
                mon_name = target.strftime("%B")  # "June"
                yr = str(target.year)
                if any(mon_name.lower() in t.lower() and yr in t for t in (current or [])):
                    break
                # Also check abbreviated format captions
                if any(mon_name.lower() in t.lower() for t in (current or [])):
                    # If year appears anywhere on page near this caption
                    break

                # Click "next month" navigation
                clicked = await page.evaluate("""() => {
                    const nexts = document.querySelectorAll(
                        '[class*="next"], [aria-label*="next"], [aria-label*="Next"], '
                        + '[class*="forward"], button.DayPicker-NavButton--next, '
                        + '[class*="nav-right"], [class*="arrow-right"]'
                    );
                    for (const b of nexts) {
                        if (b.offsetParent !== null) {
                            b.click();
                            return true;
                        }
                    }
                    return false;
                }""")
                if not clicked:
                    break
                await asyncio.sleep(0.5)

            # Click the target date cell
            day_label = target.strftime("%B %-d, %Y") if os.name != "nt" else target.strftime("%B {}, %Y").format(target.day)
            clicked = await page.evaluate("""(label) => {
                // Try aria-label first (DayPicker style)
                let cell = document.querySelector('[aria-label*="' + label + '"]');
                if (cell) { cell.click(); return 'aria:' + label; }

                // Try data-day attribute
                const dayNum = label.split(' ')[1].replace(',', '');
                const cells = document.querySelectorAll(
                    '.DayPicker-Day, [class*="calendar-day"], td[role="gridcell"], '
                    + '[class*="day-cell"]'
                );
                for (const c of cells) {
                    const t = c.innerText.trim();
                    if (t === dayNum && !c.classList.contains('disabled')
                        && !c.getAttribute('aria-disabled')) {
                        c.click();
                        return 'text:' + t;
                    }
                }
                return null;
            }""", day_label)
            if clicked:
                logger.info("ITA: date set to %s (%s)", target, clicked)
            else:
                logger.warning("ITA: could not click date %s", target)

            await asyncio.sleep(0.5)

            # Close calendar if still open (click outside)
            await page.evaluate("""() => {
                const overlay = document.querySelector(
                    '.DayPicker-Overlay, [class*="calendar-overlay"], '
                    + '[class*="datepicker-backdrop"]'
                );
                if (overlay) overlay.click();
            }""")
            await asyncio.sleep(0.3)

        except Exception as e:
            logger.warning("ITA: set date failed: %s", e)

    @staticmethod
    def _parse_dt(s) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%d/%m/%Y")
        return (
            f"https://www.ita-airways.com/gb/en/book-and-prepare/book-flights.html"
            f"?from={req.origin}&to={req.destination}"
            f"&departureDate={dep}&adults={req.adults or 1}&tripType={'RT' if req.return_from else 'OW'}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        sh = hashlib.md5(
            f"ita{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )
