"""
Level (IB) — CDP Chrome connector — pricing calendar API.

Level's website at www.flylevel.com is a Remix SPA whose "Find Flights"
button redirects to Booking.com.  However selecting a destination triggers
internal REST calls to /nwe/api/pricing/calendar/ which returns per-day
minimum prices, and /nwe/api/operability/dates for flight dates.

Strategy:
1. Open the homepage in CDP Chrome to establish a session.
2. Fetch the pricing calendar API directly via page.evaluate(fetch).
3. Parse dayPrices for the requested date.
4. Fall back to form-fill → intercept if the direct call fails.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
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
from .browser import get_or_launch_cdp, auto_block_if_proxied

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9503
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".level_chrome_data"
)

_browser = None
_context = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    global _browser, _context
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

        # Use shared CDP launch utility (handles Chrome discovery, retries, Cloud Run)
        _browser, _ = await get_or_launch_cdp(
            port=_DEBUG_PORT,
            user_data_dir=_USER_DATA_DIR,
            startup_wait=3.5,  # Longer wait for Cloud Run reliability
        )
        logger.info("Level: CDP Chrome ready on port %d", _DEBUG_PORT)

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _dismiss_overlays(page) -> None:
    try:
        await page.evaluate("""() => {
            // Dismiss newsletter modal
            const backdrop = document.querySelector('.newsletter-modal-backdrop');
            if (backdrop) backdrop.remove();
            const newsletter = document.querySelector('[class*="newsletter-modal"]');
            if (newsletter) newsletter.remove();
            // Close any visible close buttons on modals
            const closeBtns = document.querySelectorAll('.newsletter-modal button.close, [class*="newsletter"] button[aria-label="Close"]');
            for (const b of closeBtns) { if (b.offsetHeight > 0) b.click(); }

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
                '[class*="cookie"], [class*="consent"], [class*="overlay"], ' +
                '.newsletter-modal-backdrop, [class*="newsletter-modal"]'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class LevelConnectorClient:
    """Level (IB) CDP Chrome connector."""

    IATA = "IB"
    AIRLINE_NAME = "Level"
    SOURCE = "level_direct"
    HOMEPAGE = "https://www.flylevel.com/en"
    DEFAULT_CURRENCY = "EUR"

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

        try:
            dt = (
                req.date_from
                if isinstance(req.date_from, (datetime, date))
                else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            )
            date_str = dt.strftime("%Y-%m-%d")
            month_str = f"{dt.month:02d}"
            year_str = str(dt.year)

            logger.info("Level: loading homepage for %s→%s on %s", req.origin, req.destination, date_str)
            await page.goto(self.HOMEPAGE, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3.0)
            await _dismiss_overlays(page)

            # ── Direct API call via page context ──
            calendar_url = (
                f"/nwe/api/pricing/calendar/?triptype={'RT' if req.return_from else 'OW'}"
                f"&origin={req.origin}&destination={req.destination}"
                f"&month={month_str}&year={year_str}&version=1&currency=EUR"
            )
            result = await page.evaluate("""async (url) => {
                try {
                    const r = await fetch(url);
                    if (!r.ok) return {error: r.status};
                    return await r.json();
                } catch(e) { return {error: e.message}; }
            }""", calendar_url)

            offers: list[FlightOffer] = []
            if isinstance(result, dict) and "dayPrices" in result:
                offers = self._parse_calendar(result["dayPrices"], date_str, req)
                logger.info("Level: calendar API returned %d prices for %s", len(offers), date_str)

            # ── Fallback: form-fill → intercept ──
            if not offers:
                offers = await self._form_fill_search(page, req, t0)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("Level %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(
                f"level{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            currency = offers[0].currency if offers else self.DEFAULT_CURRENCY
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
                currency=currency, offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("Level error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ── Calendar API parser ──

    def _parse_calendar(self, day_prices: list, date_str: str, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return offers

        for dp in day_prices:
            if dp.get("date") != date_str:
                continue
            price = dp.get("price", 0)
            if not price or price <= 0:
                continue
            price = float(price)
            group = dp.get("minimumPriceGroup", 0)
            tags = dp.get("tags", [])
            cabin = "premium" if group >= 3 else "economy"
            tag_label = tags[0] if tags else "economy"

            offer_id = hashlib.md5(
                f"ib_{req.origin}_{req.destination}_{date_str}_{price}_{group}".encode()
            ).hexdigest()[:12]

            segment = FlightSegment(
                airline=self.IATA, airline_name=self.AIRLINE_NAME,
                flight_no=self.IATA,
                origin=req.origin, destination=req.destination,
                departure=datetime(dt.year, dt.month, dt.day),
                arrival=datetime(dt.year, dt.month, dt.day),
                cabin_class=cabin,
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
            offers.append(FlightOffer(
                id=f"ib_{offer_id}", price=round(price, 2), currency=self.DEFAULT_CURRENCY,
                price_formatted=f"EUR {price:,.0f}",
                outbound=route, inbound=None,
                airlines=[self.AIRLINE_NAME], owner_airline=self.IATA,
                booking_url=self._booking_url(req),
                is_locked=False, source=self.SOURCE, source_tier="free",
            ))
        return offers

    # ── Fallback: form-fill → API intercept / DOM scrape ──

    async def _form_fill_search(self, page, req: FlightSearchRequest, t0: float) -> list[FlightOffer]:
        """Fallback: fill the search form, trigger calendar API, capture prices."""
        search_data: dict = {}
        api_event = asyncio.Event()

        async def _on_response(response):
            url = response.url
            if response.status != 200:
                return
            try:
                if "/nwe/api/pricing/calendar/" in url:
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    body = await response.text()
                    data = json.loads(body)
                    if isinstance(data, dict) and "dayPrices" in data:
                        search_data["dayPrices"] = data["dayPrices"]
                        api_event.set()
                        logger.info("Level: intercepted calendar API (%d prices)", len(data["dayPrices"]))
            except Exception:
                pass

        page.on("response", _on_response)
        try:
            # One-way toggle
            await page.click('[data-testid="sb-triptype-trigger"]', force=True, timeout=5000)
            await asyncio.sleep(0.5)
            await page.evaluate("""() => {
                for (const i of document.querySelectorAll('.trip-type_dropdown__item'))
                    if (i.textContent.trim() === 'One Way') { i.click(); return; }
            }""")
            await asyncio.sleep(0.5)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)

            # Destination — the city list panel
            await page.click('[data-testid="sb-destination-trigger"]', force=True, timeout=5000)
            await asyncio.sleep(1.0)

            # Try clicking city by IATA match in the destination-wrapper
            dest_iata = req.destination
            await page.evaluate("""(iata) => {
                const w = document.querySelector('.destination-wrapper');
                if (!w) return;
                // Look through all text nodes for IATA code
                const walker = document.createTreeWalker(w, NodeFilter.SHOW_TEXT, null);
                let node;
                while (node = walker.nextNode()) {
                    if (node.textContent.trim() === iata) {
                        // Click the closest .city ancestor or parent
                        let el = node.parentElement;
                        while (el && !el.classList.contains('city') && el !== w) el = el.parentElement;
                        if (el && el !== w) { el.click(); return; }
                    }
                }
                // Fallback: click any .city whose text includes the IATA
                for (const c of w.querySelectorAll('.city'))
                    if (c.textContent.includes(iata)) { c.click(); return; }
            }""", dest_iata)
            await asyncio.sleep(2.0)

            # Wait for calendar API to fire (it triggers when destination is selected)
            try:
                dt = (
                    req.date_from
                    if isinstance(req.date_from, (datetime, date))
                    else datetime.strptime(str(req.date_from), "%Y-%m-%d")
                )
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_str = str(req.date_from)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=min(remaining, 15))
            except asyncio.TimeoutError:
                pass

            if search_data.get("dayPrices"):
                return self._parse_calendar(search_data["dayPrices"], date_str, req)
            return []
        except Exception as e:
            logger.warning("Level: form-fill fallback error: %s", e)
            return []
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass

    def _booking_url(self, req: FlightSearchRequest) -> str:
        try:
            date_str = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
        except Exception:
            date_str = ""
        return f"https://www.flylevel.com/en?from={req.origin}&to={req.destination}&date={date_str}"

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"lv_rt_{o.id}_{i.id}",
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
        search_hash = hashlib.md5(f"level{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=self.DEFAULT_CURRENCY, offers=[], total_results=0,
        )
