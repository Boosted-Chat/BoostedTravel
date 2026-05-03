"""
VietJet Air Playwright scraper -- deep-link URL + API response interception.

VietJet (IATA: VJ) is Vietnam's largest low-cost carrier operating domestic
and regional flights across Southeast Asia, India, and beyond.

Strategy (Deep-Link URL + API Interception, Form-Fill Fallback):
1. Primary: Navigate to vietjetair.com/en?departAirport=X&arrivalAirport=Y&departDate=D&tripType=oneway
   → The SPA auto-triggers encrypted PATCH search-flight API calls
2. Fallback: If deep-link yields nothing, do a form-fill on the homepage
3. Intercept PATCH vietjet-api.vietjetair.com/booking/api/v1/search-flight responses
4. Parse travelOption[cityPair] → flights[] + fareOptions[] → FlightOffer objects

API details (discovered Mar 2026):
- Endpoint: PATCH vietjet-api.vietjetair.com/booking/api/v1/search-flight
- Request body: {"encrypted":"..."} (client-side encrypted, no replay possible)
- Response: {status, message, travelOption: {"ORIG-DEST": [options]},
             lowestFares, sessionId, sessionExpIn}
- Intelisys/Navitaire backend
- reCAPTCHA v3 auto-verified (score 0.7), no manual solve needed
- AWS WAF (ap-southeast-1) token auto-managed by browser

Response structure:
  travelOption["SGN-HAN"] = [
    {departureDate, enRouteHours, numberOfStops,
     flights: [{airlineCode, flightNumber, departure{localScheduledTime, airport{code}},
                arrival{...}, availability, aircraftModel{identifier}}],
     fareOptions: [{fareValidity{valid, soldOut}, fareType{identifier},
       cabinClass{code, description},
       fareCharges: [{chargeType{code:"FA"}, passengerApplicability{adult:true},
                      currencyAmounts: [{totalAmount, currency{code}}]}]}]
    }
  ]
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

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import auto_block_if_proxied, get_or_launch_cdp

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-US", "en-GB", "en-VN", "en-SG"]
_TIMEZONES = [
    "Asia/Ho_Chi_Minh", "Asia/Bangkok", "Asia/Singapore",
    "Asia/Kuala_Lumpur", "Asia/Jakarta",
]

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9515
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
        from .browser import get_or_launch_cdp
        _user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-cdp-vietjet")
        _browser, _chrome_proc = await get_or_launch_cdp(_CDP_PORT, _user_data)
        logger.info("VietJet: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _browser


class VietJetConnectorClient:
    """VietJet scraper -- deep-link URL + PATCH /booking/api/v1/search-flight interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ==================================================================
    # Main entry point
    # ==================================================================

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result, ob_anc = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result, _ = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        if ob_result.offers:
            if ob_anc:
                self._apply_ancillaries(ob_result.offers, ob_anc)
            else:
                segs = ob_result.offers[0].outbound.segments if ob_result.offers[0].outbound else []
                anc_origin = segs[0].origin if segs else req.origin
                anc_dest = segs[-1].destination if segs else req.destination
                try:
                    ancillary = await asyncio.wait_for(
                        self._fetch_ancillaries(
                            anc_origin, anc_dest,
                            req.date_from.isoformat() if hasattr(req.date_from, 'isoformat') else str(req.date_from),
                            req.adults, ob_result.currency or req.currency,
                        ),
                        timeout=45.0,
                    )
                    if ancillary:
                        self._apply_ancillaries(ob_result.offers, ancillary)
                except (asyncio.TimeoutError, TimeoutError):
                    logger.debug("Ancillary fetch timed out for %s->%s", anc_origin, anc_dest)
                except Exception as _anc_err:
                    logger.debug("Ancillary fetch error: %s", _anc_err)
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> tuple[FlightSearchResponse, dict | None]:
        # 2026: VietJet deep-link works for origin/dest but ignores date param
        # Strategy: deep-link first (sets route), then navigate date slider
        probe_anc: dict | None = None
        for attempt in range(2):
            # First attempt: deep-link + date navigation
            # Second attempt: form-fill fallback
            use_form = attempt == 1
            result, probe_anc = await self._try_search(req, use_form=use_form)
            if result.total_results > 0:
                return result, probe_anc
            if attempt == 0:
                logger.info("VietJet: deep-link yielded 0 results, trying form-fill fallback")
                await asyncio.sleep(1.5)
        return result, probe_anc

    async def _try_search(self, req: FlightSearchRequest, use_form: bool = False) -> tuple[FlightSearchResponse, dict | None]:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await auto_block_if_proxied(page)
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()
                await auto_block_if_proxied(page)

            route_key = f"{req.origin}-{req.destination}"
            return_route_key = f"{req.destination}-{req.origin}"
            target_date = req.date_from.strftime("%Y-%m-%d")
            is_rt = req.return_from is not None
            currency = req.currency if req.currency != "EUR" else "USD"

            all_travel_options: list[dict] = []
            all_return_options: list[dict] = []
            lowest_fares_captured: dict = {}
            api_event = asyncio.Event()
            ssr_captured: dict = {}
            ssr_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url
                    if response.status == 200 and "vietjet-api" in url:
                        ct = response.headers.get("content-type", "")
                        if "json" not in ct:
                            return
                        if "search-flight" in url:
                            data = await response.json()
                            if not isinstance(data, dict) or not data.get("status"):
                                return
                            travel = data.get("travelOption", {})
                            # Outbound direction
                            options = travel.get(route_key, [])
                            if not options:
                                for key in travel:
                                    if req.origin in key and req.destination in key:
                                        options = travel[key]
                                        break
                            if options:
                                if isinstance(options, list):
                                    all_travel_options.extend(options)
                                else:
                                    all_travel_options.append(options)
                            # Return direction (for RT)
                            if is_rt:
                                ret_options = travel.get(return_route_key, [])
                                if not ret_options:
                                    for key in travel:
                                        if req.destination in key and req.origin in key and key != route_key:
                                            ret_options = travel[key]
                                            break
                                if ret_options:
                                    if isinstance(ret_options, list):
                                        all_return_options.extend(ret_options)
                                    else:
                                        all_return_options.append(ret_options)
                            lf = data.get("lowestFares")
                            if lf:
                                lowest_fares_captured.update(lf if isinstance(lf, dict) else {})
                            if all_travel_options or all_return_options:
                                api_event.set()
                        elif any(k in url for k in ("addon", "ancillar", "/ssr", "bundle")):
                            data = await response.json()
                            if isinstance(data, dict) and data:
                                ssr_captured.update(data)
                                ssr_event.set()
                                logger.debug("VietJet: captured SSR/addon response from %s", url)
                except Exception:
                    pass

            page.on("response", on_response)

            if use_form:
                # Form-fill approach
                await self._do_form_search(page, req, t0)
            else:
                # Deep-link URL approach (2026: site ignores date param, need to navigate)
                dep_str = req.date_from.strftime("%Y-%m-%d")
                trip = "roundtrip" if is_rt else "oneway"
                deep_url = (
                    f"https://www.vietjetair.com/en?"
                    f"departAirport={req.origin}&arrivalAirport={req.destination}"
                    f"&departDate={dep_str}&tripType={trip}"
                    f"&currency={currency}&languageCode=en"
                )
                if is_rt:
                    ret_str = req.return_from.strftime("%Y-%m-%d")
                    deep_url += f"&returnDate={ret_str}"
                logger.info("VietJet: deep-link search %s->%s on %s (%s)", req.origin, req.destination, dep_str, trip)
                await page.goto(deep_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
                await self._nuke_overlays(page)
                # VietJet's new SPA ignores the date param - click a date cell to trigger search
                await self._navigate_and_click_date(page, req.date_from)

            # Wait for search API responses
            remaining = max(self.timeout - (time.monotonic() - t0), 12)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
                # Wait a bit more for parallel API calls to arrive
                await asyncio.sleep(3.0)
            except asyncio.TimeoutError:
                logger.warning("VietJet: timed out waiting for search-flight API (%s)", "form" if use_form else "deep-link")

            if not all_travel_options:
                return self._empty(req), None

            elapsed = time.monotonic() - t0
            outbound_offers = self._parse_travel_options(all_travel_options, req, currency, lowest_fares_captured)

            # Build RT combos from outbound + return options
            if is_rt and all_return_options:
                from copy import copy
                ret_req = copy(req)
                ret_req.origin = req.destination
                ret_req.destination = req.origin
                ret_req.date_from = req.return_from
                inbound_offers = self._parse_travel_options(all_return_options, ret_req, currency, {})

                if inbound_offers:
                    booking_url = self._build_booking_url(req)
                    rt_offers: list[FlightOffer] = []
                    for ob in outbound_offers[:15]:
                        for ib in inbound_offers[:10]:
                            combined = round(ob.price + ib.price, 2)
                            rt_id = hashlib.md5(
                                f"vj_rt_{ob.id}_{ib.id}".encode()
                            ).hexdigest()[:12]
                            rt_offers.append(FlightOffer(
                                id=f"vj_{rt_id}",
                                price=combined,
                                currency=ob.currency,
                                price_formatted=f"{combined:,.0f} {ob.currency}",
                                outbound=ob.outbound,
                                inbound=ib.outbound,
                                airlines=["VietJet"],
                                owner_airline="VJ",
                                booking_url=booking_url,
                                is_locked=False,
                                source="vietjet_direct",
                                source_tier="free",
                            ))
                    rt_offers.sort(key=lambda o: o.price)
                    outbound_offers = rt_offers[:50] if rt_offers else outbound_offers

            # ── Live ancillary checkout probe ──────────────────────────
            probe_anc: dict | None = None
            if outbound_offers and not ssr_event.is_set():
                try:
                    probe_anc = await asyncio.wait_for(
                        self._probe_vietjet_ancillary(page, ssr_captured, ssr_event),
                        timeout=18.0,
                    )
                except Exception as _pe:
                    logger.debug("VietJet probe skipped: %s", _pe)
            elif ssr_event.is_set():
                probe_anc = self._parse_vietjet_ssr(ssr_captured)

            return self._build_response(outbound_offers, req, elapsed), probe_anc

        except Exception as e:
            logger.error("VietJet Playwright error: %s", e)
            return self._empty(req), None
        finally:
            await context.close()

    # ==================================================================
    # Live ancillary checkout probe
    # ==================================================================

    async def _probe_vietjet_ancillary(
        self, page, ssr_captured: dict, ssr_event: asyncio.Event
    ) -> dict | None:
        """After search results load, click first 'Select' button and capture
        live VietJet SSR/addon API response for bag/seat prices."""
        try:
            # Try common VietJet English + Vietnamese select button selectors
            select_btn = None
            for selector in [
                "button:has-text('Select')",
                "button:has-text('Choose')",
                "button:has-text('Book')",
                "button:has-text('Chọn')",
                "[data-testid*='select']",
                ".btn-select",
                "button[class*='select']",
                "button[class*='book']",
            ]:
                try:
                    btn = page.locator(selector).first
                    await btn.wait_for(state="visible", timeout=4000)
                    select_btn = btn
                    logger.debug("VietJet probe: found button via %s", selector)
                    break
                except Exception:
                    pass

            if select_btn is None:
                logger.debug("VietJet probe: no Select button found on search page")
                return None

            await select_btn.click()

            # Wait for SSR/addon API response (ssr_event set by on_response handler)
            try:
                await asyncio.wait_for(ssr_event.wait(), timeout=14.0)
            except asyncio.TimeoutError:
                logger.debug("VietJet probe: SSR event timed out after click")
                return None

            if not ssr_captured:
                return None

            result = self._parse_vietjet_ssr(ssr_captured)
            if result:
                logger.info(
                    "VietJet probe: cabin=%s checked=%s seat=%s %s",
                    result.get("bags_from"), result.get("checked_bag_from"),
                    result.get("seat_from"), result.get("currency", ""),
                )
            return result
        except Exception as e:
            logger.debug("VietJet checkout probe failed: %s", e)
            return None

    def _parse_vietjet_ssr(self, data: dict) -> dict | None:
        """Parse VietJet SSR/addon API response for live bag/seat prices.

        VietJet uses Navitaire Intelisys backend. SSR response typically:
        {
          "data": {
            "addonList": [
              {"type": "BAGGAGE", "code": "VB20", "name": "20kg bag",
               "price": {"amount": 15.0, "currency": "USD"}, ...},
              {"type": "SEAT", "code": "STSS", "price": {"amount": 5.0, ...}},
            ]
          }
        }
        Or nested Navitaire SSR structure.
        """
        currency = "USD"
        cabin_price: float | None = None
        checked_price: float | None = None
        seat_price: float | None = None

        def _walk(node, depth=0):
            nonlocal cabin_price, checked_price, seat_price, currency
            if depth > 10 or not isinstance(node, (dict, list)):
                return
            if isinstance(node, list):
                for item in node:
                    _walk(item, depth + 1)
                return
            # Extract currency
            for k in ("currency", "currencyCode", "currCode"):
                v = node.get(k)
                if isinstance(v, str) and len(v) == 3 and v.isupper():
                    currency = v
                    break

            # Identify item type
            code = str(
                node.get("ssrCode") or node.get("code") or
                node.get("type") or node.get("serviceCode") or ""
            ).upper()
            name = str(node.get("name") or node.get("description") or "").lower()

            is_cabin = (
                any(k in code for k in ("CABIN", "HAND", "CARRY")) or
                any(k in name for k in ("cabin bag", "hand bag", "carry-on", "7kg"))
            )
            is_checked = (
                any(k in code for k in ("BAG", "LUGGAGE", "HOLD", "VB", "CBAG")) or
                any(k in name for k in ("checked", "hold bag", "luggage", "kg"))
            ) and not is_cabin
            is_seat = "SEAT" in code or "seat" in name

            # Extract price
            price_obj = node.get("price") or node.get("amount") or node.get("fee") or {}
            price_amt: float | None = None
            if isinstance(price_obj, dict):
                price_amt = price_obj.get("amount") or price_obj.get("value")
                if isinstance(price_obj.get("currency"), str):
                    currency = price_obj["currency"]
            elif isinstance(price_obj, (int, float)):
                price_amt = float(price_obj)

            if price_amt is not None:
                p = float(price_amt)
                if p == 0.0:
                    if is_cabin:
                        cabin_price = 0.0
                elif p > 0.0:
                    if is_cabin and (cabin_price is None or p < cabin_price):
                        cabin_price = p
                    elif is_checked and (checked_price is None or p < checked_price):
                        checked_price = p
                    elif is_seat and (seat_price is None or p < seat_price):
                        seat_price = p

            for val in node.values():
                if isinstance(val, (dict, list)):
                    _walk(val, depth + 1)

        _walk(data)

        if checked_price is None and seat_price is None and cabin_price is None:
            return None

        result: dict = {"currency": currency, "source": "checkout_probe"}

        if cabin_price is not None and cabin_price == 0.0:
            result["bags_from"] = 0.0
            result["bags_note"] = "7 kg cabin bag included"
        elif cabin_price is not None:
            result["bags_from"] = cabin_price
            result["bags_note"] = f"cabin bag add-on from {currency} {cabin_price:.0f}"
        else:
            result["bags_from"] = None
            result["bags_note"] = "cabin bag available at checkout"

        if checked_price is not None:
            result["checked_bag_from"] = checked_price
            result["checked_bag_note"] = f"checked bag from {currency} {checked_price:.0f}"
        else:
            result["checked_bag_from"] = None
            result["checked_bag_note"] = "checked bag add-on — see checkout"

        if seat_price is not None:
            result["seat_from"] = seat_price
            result["seat_note"] = f"seat selection from {currency} {seat_price:.0f}"
        else:
            result["seat_from"] = None
            result["seat_note"] = "seat selection available at checkout"

        return result

    # ==================================================================
    # Form-fill fallback (homepage → fill form → click "Let's go")
    # ==================================================================

    async def _do_form_search(self, page, req: FlightSearchRequest, t0: float) -> None:
        """Load homepage, fill form, click search. API interception is already set up."""
        logger.info("VietJet: form-fill search %s->%s", req.origin, req.destination)
        await page.goto(
            "https://www.vietjetair.com/en",
            wait_until="domcontentloaded",
            timeout=int(self.timeout * 1000),
        )
        await asyncio.sleep(4.0)
        await self._nuke_overlays(page)
        await asyncio.sleep(0.5)

        # Set trip type
        if req.return_from:
            await self._set_round_trip(page)
        else:
            await self._set_one_way(page)
        await asyncio.sleep(0.5)

        # Fill origin
        ok = await self._fill_airport(page, "origin", req.origin)
        if not ok:
            logger.warning("VietJet: origin fill failed for %s", req.origin)
            return
        await asyncio.sleep(1.0)

        # Fill destination (dropdown may auto-open after origin)
        ok = await self._fill_airport(page, "destination", req.destination)
        if not ok:
            logger.warning("VietJet: destination fill failed for %s", req.destination)
            return
        await asyncio.sleep(1.0)

        # Fill date
        await self._fill_date(page, req)
        await asyncio.sleep(0.5)

        # Click "Let's go"
        await self._nuke_overlays(page)
        await self._click_search(page)

    # ------------------------------------------------------------------
    # Navigate date slider and click target date (for select-flight page)
    # ------------------------------------------------------------------

    async def _navigate_and_click_date(self, page, target_date) -> bool:
        """Navigate the horizontal date slider on select-flight page and click target date.
        
        The select-flight page (2026) has:
        - A horizontal date strip starting from today
        - A forward arrow button before the date cells (left side of viewport)
        - Each date cell has two paragraphs: day name + "Month Dayth" (e.g., "August 15th")
        - Clicking a date cell triggers the search-flight API
        """
        from datetime import date
        
        await asyncio.sleep(2.0)  # Wait for page to fully load
        
        today = date.today()
        target = target_date if isinstance(target_date, date) else datetime.strptime(str(target_date), "%Y-%m-%d").date()
        days_diff = (target - today).days
        
        if days_diff <= 0:
            logger.warning("VietJet: target date %s is in the past", target)
            return False
        
        logger.info("VietJet: navigating date slider %d days forward to %s", days_diff, target)
        
        # Target date text formats (e.g., "August 15th", "August 1st")
        day = target.day
        if day in (1, 21, 31):
            suffix = "st"
        elif day in (2, 22):
            suffix = "nd"
        elif day in (3, 23):
            suffix = "rd"
        else:
            suffix = "th"
        target_text = f"{target.strftime('%B')} {day}{suffix}"  # "August 15th"
        
        # Scroll the date slider forward until target date is visible
        # Each scroll moves ~7 days, so calculate clicks needed
        max_clicks = max((days_diff // 5), 2) + 10  # Extra buffer
        
        for click_num in range(min(max_clicks, 120)):
            # Check if target date is already visible
            try:
                found = await page.evaluate("""(targetText) => {
                    const paragraphs = document.querySelectorAll('p, [class*="MuiTypography"]');
                    for (const p of paragraphs) {
                        if (p.textContent && p.textContent.trim() === targetText) {
                            const rect = p.getBoundingClientRect();
                            if (rect.width > 0 && rect.left >= 0 && rect.right <= window.innerWidth) {
                                return true;
                            }
                        }
                    }
                    return false;
                }""", target_text)
                
                if found:
                    logger.debug("VietJet: target date '%s' is now visible after %d clicks", target_text, click_num)
                    break
            except Exception:
                pass
            
            # Click forward arrow to scroll dates
            clicked_arrow = False
            try:
                # The forward arrow is a button with an img child at the left of the date slider
                arrow = await page.evaluate("""() => {
                    // Find all buttons with img children in the date area
                    const buttons = document.querySelectorAll('button');
                    for (const btn of buttons) {
                        const img = btn.querySelector('img');
                        if (img && btn.offsetHeight > 0) {
                            const rect = btn.getBoundingClientRect();
                            // Arrow buttons are typically on left/right edges
                            if (rect.width > 0 && rect.width < 80) {
                                // Click the right arrow (second button in the date nav area)
                                const parent = btn.closest('[class*="jss"], [class*="MuiBox"]');
                                if (parent) {
                                    const btns = parent.querySelectorAll('button:has(img)');
                                    if (btns.length >= 2) {
                                        // Right arrow is the second one
                                        btns[1].click();
                                        return 'right';
                                    } else if (btns.length === 1) {
                                        btns[0].click();
                                        return 'single';
                                    }
                                }
                            }
                        }
                    }
                    return null;
                }""")
                if arrow:
                    clicked_arrow = True
            except Exception:
                pass
            
            if not clicked_arrow:
                # Fallback: try various arrow selectors
                for sel in [
                    "[class*='jss'] button:has(img):last-of-type",
                    "button:has(img[src*='arrow'])",
                    "button:has(img[alt*='right'])",
                    "[class*='MuiBox'] > div > button:last-child",
                ]:
                    try:
                        arrow = page.locator(sel).first
                        if await arrow.count() > 0:
                            await arrow.click(timeout=500)
                            clicked_arrow = True
                            break
                    except Exception:
                        continue
            
            await asyncio.sleep(0.1)  # Small delay between arrow clicks
        
        await asyncio.sleep(0.3)
        
        # Now click the target date cell
        try:
            clicked = await page.evaluate("""(targetText) => {
                // Find the paragraph with the exact date text
                const paragraphs = document.querySelectorAll('p, [class*="MuiTypography"]');
                for (const p of paragraphs) {
                    if (p.textContent && p.textContent.trim() === targetText) {
                        const rect = p.getBoundingClientRect();
                        if (rect.width > 0 && rect.left >= 0) {
                            // Click the parent cell (the clickable container)
                            let cell = p.closest('[class*="jss"], [class*="MuiBox"]');
                            if (cell && cell.classList.length > 0) {
                                cell.click();
                                return 'cell';
                            }
                            // Fallback: click the paragraph itself
                            p.click();
                            return 'paragraph';
                        }
                    }
                }
                return null;
            }""", target_text)
            
            if clicked:
                logger.info("VietJet: clicked target date %s (%s)", target_text, clicked)
                await asyncio.sleep(3.0)  # Wait for search API to trigger
                return True
        except Exception as e:
            logger.debug("VietJet: date click error: %s", e)
        
        logger.warning("VietJet: could not click target date %s", target_text)
        return False

    # ------------------------------------------------------------------
    # Overlay / popup removal
    # ------------------------------------------------------------------

    async def _nuke_overlays(self, page) -> None:
        """Aggressively remove all overlays, popups, cookie banners."""
        for txt in ["Accept", "Agree", "Close", "Later", "Got it", "OK", "Dismiss"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(txt)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=1500)
                    await asyncio.sleep(0.3)
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                const sels = '[class*="cookie"],[id*="cookie"],[class*="popup"],[id*="popup"],' +
                    '[class*="modal"],[class*="notification"],[class*="smartech"],[id*="smartech"],' +
                    '[class*="Overlay"],[class*="overlay"],.MuiBackdrop-root,' +
                    '[class*="consent"],[id*="consent"]';
                document.querySelectorAll(sels).forEach(el => {
                    if (el.offsetHeight > 0) el.remove();
                });
                document.body.style.overflow = 'auto';
                document.body.style.pointerEvents = 'auto';
            }""")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # One-way / Round-trip toggle
    # ------------------------------------------------------------------

    async def _set_round_trip(self, page) -> None:
        for sel in [
            "input#roundtrip", "input[value='roundtrip']", "input[value='RT']",
            "label[for='roundtrip']", "label[for='round-trip']",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue
        for label in ["Round-trip", "Round Trip", "Round trip", "Roundtrip"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if el and await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue

    async def _set_one_way(self, page) -> None:
        for sel in [
            "input#oneway", "input[value='oneway']", "input[value='OW']",
            "label[for='oneway']", "label[for='one-way']",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue
        for label in ["One-way", "One Way", "One way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if el and await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Airport field fill (MUI ExpansionPanel dropdown)
    # ------------------------------------------------------------------

    async def _fill_airport(self, page, field_type: str, iata: str) -> bool:
        """Fill origin or destination airport in the MUI booking widget.

        The dropdown is an MUI ExpansionPanel with airports grouped by country.
        Each airport shows the IATA code as a separate text element.
        """
        is_origin = field_type == "origin"

        # Step 1: Click the input to open the dropdown
        clicked = False
        if is_origin:
            for sel in [
                "[class*='departurePlaceDesktop'] input",
                "input#departurePlaceDesktop",
                ".booking-widget input:first-of-type",
                "[class*='MuiInputBase'] input",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        clicked = True
                        break
                except Exception:
                    continue
            if not clicked:
                try:
                    inputs = page.locator("[class*='booking'] input, [class*='search-form'] input, [class*='flight-search'] input")
                    if await inputs.count() > 0:
                        await inputs.first.click(timeout=3000)
                        clicked = True
                except Exception:
                    pass
        else:
            for sel in [
                "input#arrivalPlaceDesktop",
                "[class*='arrivalPlaceDesktop'] input",
                "[id*='arrival'] input",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        clicked = True
                        break
                except Exception:
                    continue

        if not clicked:
            logger.debug("VietJet: %s input not found", field_type)
            return False

        await asyncio.sleep(1.0)

        # Step 2: Find and click the IATA code in the dropdown
        for _ in range(3):
            # Try text match for the IATA code
            try:
                iata_text = page.get_by_text(iata, exact=True)
                count = await iata_text.count()
                if count > 0:
                    for i in range(min(count, 5)):
                        el = iata_text.nth(i)
                        try:
                            box = await el.bounding_box()
                            if box and box["height"] > 0:
                                await el.click(timeout=2000)
                                logger.debug("VietJet: selected %s via text (idx %d)", iata, i)
                                return True
                        except Exception:
                            continue
            except Exception:
                pass

            # Try typing to filter, then recheck
            try:
                active = page.locator("input:focus")
                if await active.count() > 0:
                    await active.fill(iata)
                    await asyncio.sleep(1.5)
            except Exception:
                pass

        # JS fallback: find and click leaf text node matching IATA
        try:
            clicked_js = await page.evaluate("""(iata) => {
                const els = document.querySelectorAll('*');
                for (const el of els) {
                    if (el.children.length === 0 &&
                        el.textContent.trim() === iata &&
                        el.offsetHeight > 0 &&
                        el.getBoundingClientRect().top > 0) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }""", iata)
            if clicked_js:
                logger.debug("VietJet: selected %s via JS", iata)
                return True
        except Exception:
            pass

        logger.warning("VietJet: could not select %s in %s dropdown", iata, field_type)
        return False

    # ------------------------------------------------------------------
    # Date picker
    # ------------------------------------------------------------------

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        day = target.day
        day_str = str(day)

        # Open date picker if not already visible
        try:
            cal = page.locator("[class*='calendar'], [class*='Calendar'], [class*='datepicker']")
            if await cal.count() == 0:
                for sel in [
                    "input[name='DepartureDate']",
                    "[class*='departure-date'] input",
                    "[class*='depart'] input",
                ]:
                    try:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.click(timeout=3000)
                            break
                    except Exception:
                        continue
            await asyncio.sleep(1.0)
        except Exception:
            pass

        # Navigate to correct month
        target_month_year = target.strftime("%B %Y")
        for _ in range(14):
            try:
                if await page.get_by_text(target_month_year).count() > 0:
                    break
            except Exception:
                pass
            for arrow_sel in [
                "button[aria-label*='next' i]",
                "button[aria-label*='Next']",
                "[class*='next']",
                "[class*='forward']",
            ]:
                try:
                    arrow = page.locator(arrow_sel).first
                    if await arrow.count() > 0:
                        await arrow.click(timeout=2000)
                        await asyncio.sleep(0.4)
                        break
                except Exception:
                    continue

        # Click the target day
        for fmt in [
            f"{day} {target.strftime('%B')} {target.year}",
            f"{target.strftime('%B')} {day}, {target.year}",
            target.strftime("%Y-%m-%d"),
        ]:
            try:
                btn = page.locator(f"[aria-label*='{fmt}']").first
                if await btn.count() > 0:
                    await btn.click(timeout=3000)
                    return True
            except Exception:
                continue

        # Calendar cell buttons
        for sel in [
            "td button", "table button", "[class*='calendar'] button",
            "[class*='Calendar'] button",
        ]:
            try:
                btns = page.locator(sel).filter(has_text=re.compile(rf"^{day_str}$"))
                if await btns.count() > 0:
                    await btns.first.click(timeout=3000)
                    return True
            except Exception:
                continue

        # JS fallback
        try:
            clicked = await page.evaluate("""(day) => {
                const cals = document.querySelectorAll(
                    '[class*="calendar"], [class*="Calendar"], [class*="datepicker"], table'
                );
                for (const cal of cals) {
                    const cells = cal.querySelectorAll('td, button, [class*="day"]');
                    for (const cell of cells) {
                        if (cell.textContent.trim() === String(day) && cell.offsetHeight > 0) {
                            cell.click();
                            return true;
                        }
                    }
                }
                return false;
            }""", day)
            if clicked:
                return True
        except Exception:
            pass

        logger.warning("VietJet: failed to click day %d", day)
        return False

    # ------------------------------------------------------------------
    # Search button ("Let's go")
    # ------------------------------------------------------------------

    async def _click_search(self, page) -> None:
        for label in ["Let's go", "Let's Go", "LETS GO", "Search", "Search flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(force=True, timeout=5000)
                    logger.info("VietJet: clicked '%s'", label)
                    await asyncio.sleep(3.0)
                    return
            except Exception:
                continue

        for sel in [
            "button:has-text(\"Let's go\")",
            "button:has-text('Let')",
            "button[type='submit']",
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(force=True, timeout=5000)
                    await asyncio.sleep(3.0)
                    return
            except Exception:
                continue

        # JS fallback
        try:
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    const txt = b.textContent.toLowerCase();
                    if (txt.includes("let") && txt.includes("go")) {
                        b.click();
                        return true;
                    }
                }
                const submit = document.querySelector('button[type="submit"]');
                if (submit) { submit.click(); return true; }
                return false;
            }""")
            await asyncio.sleep(3.0)
        except Exception:
            logger.warning("VietJet: could not click search button")

    # ==================================================================
    # Response parsing
    # ==================================================================

    def _parse_travel_options(
        self, options: list[dict], req: FlightSearchRequest, currency: str, lowest_fares: dict
    ) -> list[FlightOffer]:
        """Parse travelOption entries into FlightOffer objects."""
        target_date = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        seen_keys: set[str] = set()

        for opt in options:
            # Filter to target date
            dep_date = opt.get("departureDate", "")
            if dep_date and target_date not in dep_date:
                continue

            flights_raw = opt.get("flights", [])
            fare_options = opt.get("fareOptions", [])
            if not flights_raw:
                continue

            # Build segments
            segments: list[FlightSegment] = []
            for flt in flights_raw:
                seg = self._build_segment(flt, req)
                if seg:
                    segments.append(seg)
            if not segments:
                continue

            # Deduplicate by flight numbers
            seg_key = "-".join(f"{s.airline}{s.flight_no}" for s in segments)
            if seg_key in seen_keys:
                continue
            seen_keys.add(seg_key)

            # Duration
            en_route = opt.get("enRouteHours", 0)
            if en_route:
                total_dur = int(float(en_route) * 3600)
            elif segments[0].departure and segments[-1].arrival:
                total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
            else:
                total_dur = 0

            stopovers = max(len(segments) - 1, 0)
            route = FlightRoute(
                segments=segments,
                total_duration_seconds=max(total_dur, 0),
                stopovers=stopovers,
            )

            # Extract pricing from fareOptions
            if fare_options:
                best_price, best_currency, _, fare_type = self._cheapest_fare(fare_options, currency)
                if best_price is not None and best_price > 0:
                    # Map fareType identifier → live bag conditions
                    bag_conds = self._fare_type_to_conditions(fare_type)
                    offer_key = opt.get("key", seg_key)
                    offers.append(self._make_offer(
                        offer_key, best_price, best_currency, route, booking_url,
                        conditions=bag_conds,
                    ))
                    continue

            # Fallback: try lowestFares
            if lowest_fares:
                lf_price = self._extract_lowest_fare(lowest_fares, target_date)
                if lf_price and lf_price > 0:
                    offer_key = opt.get("key", seg_key)
                    offers.append(self._make_offer(
                        offer_key, lf_price, currency, route, booking_url,
                    ))
                    continue

            # Fallback: emit offer with price=0 (flights known, price unknown)
            offer_key = opt.get("key", seg_key)
            offers.append(self._make_offer(
                offer_key, 0, currency, route, booking_url,
            ))
            logger.warning("VietJet: flights found but no pricing for %s", seg_key)

        return offers

    # ------------------------------------------------------------------
    # Fare extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _cheapest_fare(fare_options: list[dict], default_currency: str) -> tuple:
        """Extract cheapest adult fare from fareOptions[].fareCharges[].currencyAmounts[].

        Returns (price, currency, cabin_class, fare_type_identifier) or (None, default_currency, "Economy", "").
        """
        best_price = None
        best_currency = default_currency
        best_cabin = "Economy"
        best_fare_type = ""

        for fo in fare_options:
            # Skip invalid / sold out fares
            validity = fo.get("fareValidity", {})
            if isinstance(validity, dict):
                if not validity.get("valid", True) or validity.get("soldOut", False):
                    continue

            cabin = fo.get("cabinClass", {})
            cabin_desc = cabin.get("description", "Economy") if isinstance(cabin, dict) else "Economy"

            # Capture fareType identifier for this option
            fare_type_raw = fo.get("fareType", {})
            fare_type_id = (
                fare_type_raw.get("identifier", "") if isinstance(fare_type_raw, dict)
                else str(fare_type_raw or "")
            ).upper()

            # VietJet uses fareCharges[].currencyAmounts[].totalAmount
            for charge in fo.get("fareCharges", []):
                charge_type = charge.get("chargeType", {})
                code = charge_type.get("code", "") if isinstance(charge_type, dict) else str(charge_type)
                if code != "FA":
                    continue
                pax = charge.get("passengerApplicability", {})
                if isinstance(pax, dict) and not pax.get("adult", True):
                    continue
                for ca in charge.get("currencyAmounts", []):
                    total = ca.get("totalAmount")
                    if total is not None:
                        try:
                            v = float(total)
                            if v > 0 and (best_price is None or v < best_price):
                                best_price = v
                                cur = ca.get("currency", {})
                                best_currency = (cur.get("code", default_currency)
                                                 if isinstance(cur, dict) else str(cur or default_currency))
                                best_cabin = cabin_desc
                                best_fare_type = fare_type_id
                        except (TypeError, ValueError):
                            pass

            # Fallback: try flat price fields on the fareOption itself
            if best_price is None:
                for key in ["price", "amount", "totalPrice", "totalAmount", "displayPrice"]:
                    val = fo.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if v > 0 and (best_price is None or v < best_price):
                                best_price = v
                                best_cabin = cabin_desc
                                best_fare_type = fare_type_id
                        except (TypeError, ValueError):
                            pass

        return best_price, best_currency, best_cabin, best_fare_type

    @staticmethod
    def _extract_lowest_fare(lowest_fares: dict, target_date: str) -> Optional[float]:
        """Extract price from the lowestFares section keyed by date."""
        if not isinstance(lowest_fares, dict):
            return None
        # Try date-keyed lookup
        for key, val in lowest_fares.items():
            if target_date in str(key):
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)
                if isinstance(val, dict):
                    for pkey in ["price", "amount", "fare", "total", "totalAmount"]:
                        p = val.get(pkey)
                        if p is not None:
                            try:
                                v = float(p)
                                if v > 0:
                                    return v
                            except (TypeError, ValueError):
                                pass
        return None

    # ------------------------------------------------------------------
    # Segment / offer builders
    # ------------------------------------------------------------------

    def _build_segment(self, flt: dict, req: FlightSearchRequest) -> Optional[FlightSegment]:
        """Parse a single flight from travelOption.flights[]."""
        dep_info = flt.get("departure", {})
        arr_info = flt.get("arrival", {})
        dep_time = dep_info.get("localScheduledTime") or dep_info.get("scheduledTime") or ""
        arr_time = arr_info.get("localScheduledTime") or arr_info.get("scheduledTime") or ""

        dep_airport = dep_info.get("airport", {})
        arr_airport = arr_info.get("airport", {})

        # airlineCode can be a string "VJ" or a dict {"code": "VJ"}
        airline_raw = flt.get("airlineCode", "VJ")
        if isinstance(airline_raw, dict):
            airline = airline_raw.get("code", "VJ")
        else:
            airline = str(airline_raw) if airline_raw else "VJ"

        flight_no_raw = str(flt.get("flightNumber", ""))
        flight_no = f"{airline}{flight_no_raw}" if flight_no_raw and not flight_no_raw.startswith(airline) else flight_no_raw
        _vj_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")

        return FlightSegment(
            airline=airline,
            airline_name="VietJet Air",
            flight_no=flight_no,
            origin=dep_airport.get("code", req.origin) if isinstance(dep_airport, dict) else req.origin,
            destination=arr_airport.get("code", req.destination) if isinstance(arr_airport, dict) else req.destination,
            departure=self._parse_dt(dep_time),
            arrival=self._parse_dt(arr_time),
            cabin_class=_vj_cabin,
        )

    @staticmethod
    def _fare_type_to_conditions(fare_type: str) -> dict:
        """Map VietJet fareType identifier to live bag conditions.

        VietJet fare families (from API fareType.identifier):
          ECO / ECONOMY  — no carry-on overhead; personal item only; all bags extra
          STANDARD / SMART  — 7 kg carry-on + 20 kg checked bag included
          FLEX / FLEXI / COMBO  — 7 kg carry-on + 20 kg checked bag + seat selection included
        """
        ft = (fare_type or "").upper()
        if ft in {"ECO", "ECONOMY", "LITE"}:
            return {
                "carry_on": "no free carry-on (ECO fare — add from ~VND 200,000/7 kg)",
                "checked_bag": "no checked bag (ECO fare — add from ~VND 400,000/20 kg)",
            }
        if ft in {"STANDARD", "SMART"}:
            return {
                "carry_on": "7 kg carry-on included",
                "checked_bag": "20 kg checked bag included (SMART fare)",
            }
        if ft in {"FLEX", "FLEXI", "COMBO"}:
            return {
                "carry_on": "7 kg carry-on included",
                "checked_bag": "20 kg checked bag included",
                "seat": "seat selection included (FLEXI fare)",
            }
        return {}

    @staticmethod
    def _make_offer(
        key: str, price: float, currency: str, route: FlightRoute, booking_url: str,
        *, conditions: dict | None = None,
    ) -> FlightOffer:
        offer_id = hashlib.md5(str(key).encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"vj_{offer_id}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:,.0f} {currency}" if price > 0 else f"0 {currency}",
            outbound=route,
            inbound=None,
            airlines=["VietJet"],
            owner_airline="VJ",
            booking_url=booking_url,
            is_locked=False,
            source="vietjet_direct",
            source_tier="free",
            conditions=dict(conditions) if conditions else {},
        )

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: (o.price if o.price > 0 else float("inf")))
        logger.info("VietJet %s->%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"vietjet{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%d/%m/%Y")
        trip = "RT" if req.return_from else "OW"
        url = (
            f"https://www.vietjetair.com/en/booking?origin={req.origin}"
            f"&destination={req.destination}&departDate={dep}&adults={req.adults}&type={trip}"
        )
        if req.return_from:
            ret = req.return_from.strftime("%d/%m/%Y")
            url += f"&returnDate={ret}"
        return url

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
    ) -> dict | None:
        """Return VietJet (VJ) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("VJ")
        if not ref:
            return None
        carry_on = ref.get("carry_on")
        checked = ref.get("checked_bag")
        seat = ref.get("seat")
        ref_cur = ref.get("currency", "USD")

        # Carry-on note
        if note := ref.get("carry_on_note"):
            bags_note = note
        elif carry_on == 0.0:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"1 cabin bag included ({kg} kg)"
        elif carry_on is not None:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"cabin bag from ~{ref_cur} {carry_on:.0f} ({kg} kg)"
        else:
            bags_note = None

        # Checked bag note
        if note := ref.get("checked_bag_note"):
            checked_note = note
        elif checked == 0.0:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"1 checked bag included ({kg} kg)"
        elif checked is not None:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"checked bag from ~{ref_cur} {checked:.0f} ({kg} kg)"
        else:
            checked_note = None

        # Seat note
        if seat == 0.0:
            seat_note = "seat selection included"
        elif seat is not None:
            seat_note = f"seat selection from ~{ref_cur} {seat:.0f}"
        else:
            seat_note = "seat selection available at checkout"

        return {
            "bags_from": carry_on,
            "bags_note": bags_note,
            "checked_bag_from": checked,
            "checked_bag_note": checked_note,
            "seat_from": seat,
            "seat_note": seat_note,
            "currency": ref_cur,
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag_note")
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_from")
        seat_from = ancillary.get("seat_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            # Use setdefault so live fare-type conditions (set in _parse_travel_options) are not overwritten
            if bags_note:
                offer.conditions.setdefault("carry_on", bags_note)
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions.setdefault("seat", seat_note)
            if bags_from is not None:
                offer.bags_price.setdefault("carry_on", bags_from)
            if checked_from is not None:
                offer.bags_price.setdefault("checked_bag", checked_from)
            if seat_from is not None:
                offer.bags_price.setdefault("seat", seat_from)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"vietjet{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
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
                    id=f"rt_viet_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
