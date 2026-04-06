"""
ITA Airways connector — CDP Chrome booking page + API interception.

ITA Airways (IATA: AZ) — FCO/MXP hubs, SkyTeam member.

Strategy:
  Launch Chrome with CDP, navigate to ITA booking form, fill origin/dest/date,
  click search, intercept the api.shop.ita-airways.com/one-booking/v2/search/air-bounds
  response which contains structured flight data.

  Key findings:
  - Booking URL: /gb/en/book-and-prepare/book-flights.html
  - Form fields are in regular DOM (not Shadow DOM): input[placeholder="From"], input[placeholder="To"]
  - Date field: "Departure - return" combined field
  - Button: "Search flights" (maui-button custom element)
  - After form submit, navigates to shop.ita-airways.com/booking/availability/0
  - Search API: api.shop.ita-airways.com/one-booking/v2/search/air-bounds
  - Prices in response are in CENTS (divide by 100)
  - Flight details in dictionaries.flight (keyed by flightId)
  - Must use fresh Chrome profile to avoid stale overlays
  - Must use Playwright locators (not evaluate) for form filling to trigger React state
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
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
    """Launch or connect to a Chrome instance on the CDP debug port."""
    global _pw_instance, _browser, _chrome_proc

    lock = _get_lock()
    async with lock:
        # Clean up previous Chrome process (fresh profile each time)
        if _chrome_proc:
            try:
                _chrome_proc.kill()
                _chrome_proc.wait(timeout=3)
            except Exception:
                pass
            _chrome_proc = None
        if _browser:
            try:
                await _browser.close()
            except Exception:
                pass
            _browser = None
        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
            _pw_instance = None

        # Fresh profile to avoid stale state / pre-filled values / overlay issues
        if os.path.exists(_USER_DATA_DIR):
            shutil.rmtree(_USER_DATA_DIR, ignore_errors=True)
        os.makedirs(_USER_DATA_DIR, exist_ok=True)

        chrome = find_chrome()
        args = [
            chrome,
            f"--remote-debugging-port={_DEBUG_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--no-first-run",
            *proxy_chrome_args(),
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-size=1400,900",
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.5)

        from playwright.async_api import async_playwright

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
                    viewport={"width": 1400, "height": 900},
                    locale="en-GB",
                )
            )
            # Use existing page (from about:blank) rather than creating new one
            page = context.pages[0] if context.pages else await context.new_page()
            await auto_block_if_proxied(page)

            # --- API response interception ---
            captured: dict = {}
            api_event = asyncio.Event()

            async def _on_response(response):
                url = response.url
                ct = response.headers.get("content-type", "")
                if response.status != 200 or "json" not in ct:
                    return
                try:
                    body = await response.body()
                    if len(body) < 200:
                        return
                    import json as _json
                    data = _json.loads(body)
                    if not isinstance(data, dict):
                        return
                    # Primary: api.shop.ita-airways.com/one-booking/v2/search/air-bounds
                    data_obj = data.get("data")
                    if isinstance(data_obj, dict) and "airBoundGroups" in data_obj:
                        captured["search"] = data
                        api_event.set()
                        logger.info(
                            "ITA: captured air-bounds API (%d bytes, %d groups)",
                            len(body),
                            len(data_obj["airBoundGroups"]),
                        )
                except Exception:
                    pass

            page.on("response", _on_response)

            # --- Navigate to booking page ---
            logger.info("ITA: navigating to booking page")
            await page.goto(
                "https://www.ita-airways.com/gb/en/book-and-prepare/book-flights.html",
                wait_until="networkidle",
                timeout=40000,
            )
            await asyncio.sleep(5)

            # --- Accept cookies ---
            try:
                btn = page.locator("#onetrust-accept-btn-handler")
                if await btn.count() > 0:
                    await btn.click(timeout=3000)
                else:
                    accept = page.locator("button", has_text="Accept all")
                    if await accept.count() > 0:
                        await accept.first.click(timeout=3000)
            except Exception:
                pass
            await asyncio.sleep(1.5)

            # --- Step 1: Select trip type ---
            if not req.return_from:
                logger.info("ITA: selecting One-way")
                try:
                    rt_btn = page.locator("button", has_text="Round trip")
                    if await rt_btn.count() > 0:
                        await rt_btn.first.click(timeout=5000)
                        await asyncio.sleep(0.8)
                        ow = page.locator('[role="option"]', has_text="One-way")
                        if await ow.count() > 0:
                            await ow.first.click(timeout=3000)
                        else:
                            ow2 = page.locator("li.sel-item", has_text="One-way")
                            if await ow2.count() > 0:
                                await ow2.first.click(timeout=3000)
                        await asyncio.sleep(0.3)
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.3)
                except Exception as e:
                    logger.debug("ITA: one-way selection: %s", e)
            else:
                logger.info("ITA: keeping Round trip (default)")

            # Dismiss any lingering dropdown overlays
            await page.evaluate("""() => {
                document.querySelectorAll('.flight-tab2, [role="listbox"]')
                    .forEach(el => { el.style.display = 'none'; });
                document.body.click();
            }""")
            await asyncio.sleep(0.5)

            # --- Step 2: Fill origin airport ---
            logger.info("ITA: filling origin %s", req.origin)
            origin_inp = page.locator('input[placeholder="From"]')
            await origin_inp.click(click_count=3, force=True, timeout=8000)
            await asyncio.sleep(0.2)
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.2)
            await page.keyboard.type(req.origin, delay=100)
            await asyncio.sleep(2.5)
            # Pick suggestion
            await page.evaluate("""(code) => {
                for (const li of document.querySelectorAll('li.sel-item, [role="option"]')) {
                    const t = (li.innerText || '').toLowerCase();
                    if (t.includes(code.toLowerCase())) { li.click(); return; }
                }
                document.querySelector('li.sel-item')?.click();
            }""", req.origin)
            await asyncio.sleep(1.0)

            # --- Step 3: Fill destination airport ---
            logger.info("ITA: filling destination %s", req.destination)
            dest_inp = page.locator('input[placeholder="To"]')
            await dest_inp.click(click_count=3, force=True, timeout=8000)
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

            # Verify form state
            form_vals = await page.evaluate("""() => ({
                origin: document.querySelector('input[placeholder="From"]')?.value || '',
                dest: document.querySelector('input[placeholder="To"]')?.value || '',
            })""")
            logger.info("ITA: form state: %s", form_vals)

            # --- Step 4: Set departure date ---
            target_month = req.date_from.strftime("%B")  # e.g. "June"
            target_day = req.date_from.day
            logger.info("ITA: setting date %s %d", target_month, target_day)

            # Click the date field to open calendar
            # Try the "Departure" text/field area
            date_opened = False
            # Method 1: Click "Departure" label/field via evaluate (finds text anywhere)
            await page.evaluate("""() => {
                for (const el of document.querySelectorAll('*')) {
                    const text = (el.innerText || '').trim();
                    const rect = el.getBoundingClientRect();
                    if ((text === 'Departure' || text === 'Departure - return' || text.startsWith('Departure'))
                        && rect.y > 300 && rect.y < 600 && rect.width > 50) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }""")
            await asyncio.sleep(2)

            cal_tds = await page.evaluate("""() => {
                return [...document.querySelectorAll('td')].filter(
                    td => /^\\d{1,2}$/.test((td.innerText||'').trim())
                ).length;
            }""")
            if cal_tds >= 10:
                date_opened = True
                logger.info("ITA: calendar opened via Departure text (%d day tds)", cal_tds)

            if not date_opened:
                # Method 2: Click the date input area by finding it near the form
                # The date field is the 3rd input area, after From and To
                date_box = await page.evaluate("""() => {
                    // Find elements containing "Departure" or date-related text
                    const candidates = document.querySelectorAll(
                        '[class*="date"], [class*="calendar"], [placeholder*="date"], [placeholder*="Departure"]'
                    );
                    for (const el of candidates) {
                        const rect = el.getBoundingClientRect();
                        if (rect.width > 50 && rect.height > 20 && rect.y > 300) {
                            return {x: rect.x + rect.width/2, y: rect.y + rect.height/2};
                        }
                    }
                    // Fallback: find the area between To input and Search button
                    const toInp = document.querySelector('input[placeholder="To"]');
                    if (toInp) {
                        const toRect = toInp.getBoundingClientRect();
                        // Date field is to the right of To field
                        return {x: toRect.right + 150, y: toRect.top + toRect.height/2};
                    }
                    return null;
                }""")
                if date_box:
                    logger.info("ITA: clicking date field at (%.0f, %.0f)", date_box["x"], date_box["y"])
                    await page.mouse.click(date_box["x"], date_box["y"])
                    await asyncio.sleep(2)
                    cal_tds = await page.evaluate("""() => {
                        return [...document.querySelectorAll('td')].filter(
                            td => /^\\d{1,2}$/.test((td.innerText||'').trim())
                        ).length;
                    }""")
                    if cal_tds >= 10:
                        date_opened = True
                        logger.info("ITA: calendar opened via position click (%d tds)", cal_tds)

            if not date_opened:
                # Method 3: Try clicking each maui-input
                count = await page.evaluate("""() => document.querySelectorAll('maui-input').length""")
                for idx in range(count):
                    await page.evaluate(f"() => document.querySelectorAll('maui-input')[{idx}]?.click()")
                    await asyncio.sleep(1.5)
                    cal_tds = await page.evaluate("""() => {
                        return [...document.querySelectorAll('td')].filter(
                            td => /^\\d{1,2}$/.test((td.innerText||'').trim())
                        ).length;
                    }""")
                    if cal_tds >= 10:
                        date_opened = True
                        logger.info("ITA: calendar opened via maui-input[%d]", idx)
                        break

            if date_opened:
                # Navigate to target month
                for nav_i in range(12):
                    vis_months = await page.evaluate("""() => {
                        const months = ['January','February','March','April','May','June',
                            'July','August','September','October','November','December'];
                        return months.filter(m => document.body.innerText.includes(m));
                    }""")
                    if target_month in vis_months:
                        logger.info("ITA: target month %s visible after %d clicks", target_month, nav_i)
                        break
                    # Click forward arrow
                    await page.evaluate("""() => {
                        const btn = document.querySelector(
                            'button[aria-label="Move forward to switch to the next month"]'
                        );
                        if (btn) { btn.click(); return; }
                        for (const b of document.querySelectorAll('button, [role="button"]')) {
                            const al = (b.getAttribute('aria-label') || '').toLowerCase();
                            if (al.includes('next') || al.includes('forward')) {
                                b.click(); return;
                            }
                        }
                    }""")
                    await asyncio.sleep(0.7)

                # Click target day — pick rightmost td matching (target month column)
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
                logger.info("ITA: day %d click: %s", target_day, day_clicked)
                await asyncio.sleep(0.5)

                # --- RT: Click return date in range picker ---
                if req.return_from and day_clicked:
                    ret_month = req.return_from.strftime("%B")
                    ret_day = req.return_from.day
                    logger.info("ITA: setting return date %s %d", ret_month, ret_day)
                    # Navigate to return month
                    for nav_j in range(12):
                        vis_months = await page.evaluate("""() => {
                            const months = ['January','February','March','April','May','June',
                                'July','August','September','October','November','December'];
                            return months.filter(m => document.body.innerText.includes(m));
                        }""")
                        if ret_month in vis_months:
                            break
                        await page.evaluate("""() => {
                            const btn = document.querySelector(
                                'button[aria-label="Move forward to switch to the next month"]'
                            );
                            if (btn) { btn.click(); return; }
                            for (const b of document.querySelectorAll('button, [role="button"]')) {
                                const al = (b.getAttribute('aria-label') || '').toLowerCase();
                                if (al.includes('next') || al.includes('forward')) {
                                    b.click(); return;
                                }
                            }
                        }""")
                        await asyncio.sleep(0.7)
                    # Click return day
                    await asyncio.sleep(0.5)
                    ret_clicked = await page.evaluate("""(dayNum) => {
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
                    }""", ret_day)
                    logger.info("ITA: return day %d click: %s", ret_day, ret_clicked)
                    await asyncio.sleep(0.5)
            else:
                logger.warning("ITA: could not open calendar")

            # Close calendar / remove overlays
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.5)
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '.ReactModal__Overlay, .modal-backdrop, .modal-calendar'
                ).forEach(el => el.remove());
            }""")
            await asyncio.sleep(0.5)

            # --- Step 5: Click search button ---
            # ITA uses <maui-button> custom element. Text is "Search flights" or "Find flights".
            searched = await page.evaluate("""() => {
                for (const el of document.querySelectorAll('maui-button')) {
                    const t = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    if (t.includes('search flight') || t.includes('find flight')) {
                        el.click();
                        return t;
                    }
                }
                for (const el of document.querySelectorAll('button, [role="button"]')) {
                    const t = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    if (t.includes('search flight') || t.includes('find flight')) {
                        el.click();
                        return t;
                    }
                }
                return null;
            }""")
            logger.info("ITA: search button clicked: %s", searched)

            # --- Wait for navigation to shop.ita-airways.com ---
            for _nav_i in range(30):
                await asyncio.sleep(1)
                try:
                    cur_url = page.url
                except Exception:
                    cur_url = ""
                if "shop.ita-airways.com" in cur_url or "availability" in cur_url:
                    logger.info("ITA: navigated to results: %s", cur_url[:80])
                    break
                if _nav_i % 10 == 0 and _nav_i > 0:
                    logger.debug("ITA: waiting for navigation (%ds)... url=%s", _nav_i, cur_url[:60])

            # --- Wait for the air-bounds API response ---
            try:
                await asyncio.wait_for(api_event.wait(), timeout=45)
            except asyncio.TimeoutError:
                logger.info("ITA: API not captured after 45s, waiting 15s more...")
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=15)
                except asyncio.TimeoutError:
                    logger.warning("ITA: API interception timed out")

            # --- Parse results ---
            offers: list[FlightOffer] = []
            if "search" in captured:
                offers = self._parse_search(captured["search"], req)

            # Fallback: scrape DOM for prices
            if not offers:
                offers = await self._scrape_dom(page, req)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("ITA %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

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
                page.remove_listener("response", _on_response)
                await page.close()
            except Exception:
                pass

    # ---- Parsers ----

    def _parse_search(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse api.shop.ita-airways.com/one-booking/v2/search/air-bounds response.

        Structure:
          data.airBoundGroups[]: each is a route option (direct or connecting)
            boundDetails: { originLocationCode, destinationLocationCode, duration, segments: [{flightId}] }
            airBounds[]: fare options per route (economy, classic, flex, business)
              airOffer.totalPrice: { value (cents!), currencyCode }
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

        # ── Classify groups into OB vs IB for RT ──
        ob_groups = []
        ib_groups = []
        for group in groups:
            bd = group.get("boundDetails", {})
            orig = bd.get("originLocationCode", "")
            if req.return_from and orig.upper() == req.destination.upper():
                ib_groups.append(group)
            else:
                ob_groups.append(group)

        # Find cheapest IB offer
        ib_route = None
        ib_price = 0.0
        if ib_groups and req.return_from:
            best_ib_price = float("inf")
            for grp in ib_groups:
                bd = grp.get("boundDetails", {})
                seg_refs = bd.get("segments", [])
                for ab in grp.get("airBounds", []):
                    tp = ab.get("airOffer", {}).get("totalPrice", {})
                    pc = tp.get("value")
                    if pc is None:
                        total_prices = ab.get("prices", {}).get("totalPrices", [])
                        if total_prices:
                            pc = total_prices[0].get("total")
                    if pc and 0 < pc / 100 < best_ib_price:
                        best_ib_price = pc / 100
                        # Build IB route from this group
                        ib_segs: list[FlightSegment] = []
                        for sref in seg_refs:
                            fid = sref.get("flightId", "")
                            fl = flight_dict.get(fid, {})
                            dep = fl.get("departure", {})
                            arr = fl.get("arrival", {})
                            mkt_code = fl.get("marketingAirlineCode", "AZ")
                            mkt_num = fl.get("marketingFlightNumber", "")
                            aname = airline_dict.get(mkt_code, "ITA Airways")
                            ib_segs.append(FlightSegment(
                                airline=mkt_code, airline_name=aname,
                                flight_no=f"{mkt_code}{mkt_num}",
                                origin=dep.get("locationCode", ""),
                                destination=arr.get("locationCode", ""),
                                departure=self._parse_dt(dep.get("dateTime")),
                                arrival=self._parse_dt(arr.get("dateTime")),
                            ))
                        if ib_segs:
                            ib_route = FlightRoute(
                                segments=ib_segs,
                                total_duration_seconds=bd.get("duration", 0),
                                stopovers=max(len(ib_segs) - 1, 0),
                            )
                            ib_price = best_ib_price
                    break  # cheapest per group

        for group in ob_groups:
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

            # Take cheapest fare per group (first airBound is usually isCheapestOffer)
            for ab in group.get("airBounds", []):
                tp = ab.get("airOffer", {}).get("totalPrice", {})
                price_cents = tp.get("value")
                currency = tp.get("currencyCode", "EUR")

                if price_cents is None:
                    total_prices = ab.get("prices", {}).get("totalPrices", [])
                    if total_prices:
                        price_cents = total_prices[0].get("total")
                        currency = total_prices[0].get("currencyCode", "EUR")

                if price_cents is None or price_cents <= 0:
                    continue

                price = round(price_cents / 100, 2)

                ff_code = ab.get("fareFamilyCode", "")
                dedup_key = f"az_{origin_code}{dest_code}{price}{ff_code}"
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)

                combined = round(price + ib_price, 2) if ib_route else price
                oid = hashlib.md5(dedup_key.encode()).hexdigest()[:12]
                airlines_list = sorted(airlines_set) if airlines_set else ["ITA Airways"]
                offers.append(FlightOffer(
                    id=f"az_rt_{oid}" if ib_route else f"az_{oid}",
                    price=combined,
                    currency=currency,
                    price_formatted=f"{combined:.2f} {currency}",
                    outbound=route,
                    inbound=ib_route,
                    airlines=airlines_list,
                    owner_airline="AZ",
                    booking_url=booking_url,
                    is_locked=False,
                    source="itaairways_direct",
                    source_tier="free",
                ))
                break  # Only cheapest fare per group

        return offers

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

    # ---- Helpers ----

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
        url = (
            f"https://www.ita-airways.com/gb/en/book-and-prepare/book-flights.html"
            f"?from={req.origin}&to={req.destination}"
            f"&departureDate={dep}&adults={req.adults or 1}&tripType={'RT' if req.return_from else 'OW'}"
        )
        if req.return_from:
            url += f"&returnDate={req.return_from.strftime('%d/%m/%Y')}"
        return url

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
