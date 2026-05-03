"""
On the Beach connector — CDP Chrome + form fill.

On the Beach (onthebeach.co.uk) is a UK-based **package holiday** OTA focused
on Mediterranean beach destinations.  Their search form combines flights with
hotels/nights — they do NOT offer standalone flight-only bookings.

**STATUS (2026-03): PERMANENTLY INCOMPATIBLE**
OnTheBeach is package-holiday only. There is no flight-only search endpoint.
This connector is REMOVED from engine.py and airline_routes.py.
Keep the file for reference.

NOTE: OnTheBeach uses Cloudflare + aggressive bot detection, so real Chrome
via CDP is required just to load the page.
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
from datetime import datetime
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_BASE = "https://www.onthebeach.co.uk"
_FLIGHTS_URL = f"{_BASE}/flights"
_CDP_PORT = 9522
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".onthebeach_chrome_data"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_context = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


async def _get_context():
    global _context
    browser = await _get_browser()
    if _context:
        try:
            if _context.pages:
                return _context
        except Exception:
            pass
    contexts = browser.contexts
    if contexts:
        _context = contexts[0]
    else:
        _context = await browser.new_context(viewport={"width": 1366, "height": 768})
    return _context


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
        logger.info("OnTheBeach: Chrome on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


class OnthebeachConnectorClient:
    """On the Beach — UK OTA (Mediterranean focus), CDP Chrome + form fill."""

    def __init__(self, timeout: float = 60.0):
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
        date_str = req.date_from.strftime("%Y-%m-%d")

        context = await _get_context()
        page = await context.new_page()

        api_data: list[dict] = []

        async def _on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            if response.status == 200 and "json" in ct:
                if any(k in url.lower() for k in [
                    "/search", "/flight", "/result", "/api/", "/fare",
                    "/graphql", "/availability", "/offers", "/quote",
                ]):
                    try:
                        body = await response.text()
                        if len(body) > 200:
                            data = json.loads(body)
                            api_data.append(data)
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            logger.info("OnTheBeach: searching %s→%s on %s", req.origin, req.destination, date_str)

            # Navigate to flights page and fill form (deep-links don't work)
            await page.goto(_FLIGHTS_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
            await self._fill_form(page, req)

            # Wait for results
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining

            offers: list[FlightOffer] = []
            while time.monotonic() < deadline:
                await asyncio.sleep(2)

                if api_data:
                    for data in api_data:
                        parsed = self._parse_api(data, req, date_str)
                        offers.extend(parsed)
                    if offers:
                        break

                try:
                    dom_offers = await self._scrape_dom(page, req, date_str)
                    if dom_offers:
                        offers = dom_offers
                        break
                except Exception:
                    pass

                try:
                    text = await page.evaluate("() => document.body?.innerText?.substring(0, 500) || ''")
                    if "no result" in text.lower() or "no flight" in text.lower() or "sorry" in text.lower():
                        break
                except Exception:
                    pass

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("OnTheBeach %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"otb{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_otb_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "GBP",
                offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("OnTheBeach error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_form(self, page, req: FlightSearchRequest):
        """Fill the On the Beach search form (button-based UI with GraphQL backend)."""
        # Accept cookies first
        for sel in ['#ccc-notify-accept', 'button:has-text("I Accept Cookies")',
                   'button:has-text("Accept")', '#onetrust-accept-btn-handler']:
            try:
                btn = page.locator(sel)
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                continue

        # Departure Airport button (id='search-field-departureAirport')
        try:
            dep_btn = page.locator('#search-field-departureAirport')
            if await dep_btn.count() > 0:
                await dep_btn.click(timeout=3000)
                await asyncio.sleep(1)
                # Type in revealed input
                search_input = page.locator(
                    'input[type="text"]:visible, input[placeholder*="airport"], '
                    'input[placeholder*="search"], input[role="searchbox"]'
                ).first
                if await search_input.count() > 0:
                    await search_input.fill(req.origin)
                    await asyncio.sleep(2)
                    sug = page.locator('[role="option"], [class*="suggestion"], [class*="airport"] li, label:has-text("' + req.origin + '")').first
                    if await sug.count() > 0:
                        await sug.click(timeout=3000)
                    else:
                        await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("OnTheBeach: departure airport failed: %s", e)

        # Destination button (id='search-field-destinations')
        try:
            dest_btn = page.locator('#search-field-destinations')
            if await dest_btn.count() > 0:
                await dest_btn.click(timeout=3000)
                await asyncio.sleep(1)
                search_input = page.locator(
                    'input[type="text"]:visible, input[placeholder*="search"], '
                    'input[placeholder*="destination"], input[role="searchbox"]'
                ).first
                if await search_input.count() > 0:
                    await search_input.fill(req.destination)
                    await asyncio.sleep(2)
                    sug = page.locator('[role="option"], [class*="suggestion"], [class*="destination"] li').first
                    if await sug.count() > 0:
                        await sug.click(timeout=3000)
                    else:
                        await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("OnTheBeach: destination failed: %s", e)

        # Date button (id='search-field-departureDate')
        try:
            date_btn = page.locator('#search-field-departureDate')
            if await date_btn.count() > 0:
                await date_btn.click(timeout=3000)
                await asyncio.sleep(1)
                # Navigate calendar to target month
                target_month = req.date_from.strftime("%B %Y")
                for _ in range(12):
                    try:
                        cal_text = await page.evaluate("""() => {
                            const h = document.querySelectorAll('[class*="month"], [class*="calendar"] h2, [class*="Calendar"] th');
                            return Array.from(h).map(e => e.innerText).join(' ');
                        }""")
                        if target_month.lower() in cal_text.lower():
                            break
                    except Exception:
                        pass
                    try:
                        nxt = page.locator('[aria-label="Next month"], button:has-text("›"), button:has-text("Next")')
                        if await nxt.count() > 0:
                            await nxt.first.click(timeout=2000)
                            await asyncio.sleep(0.3)
                    except Exception:
                        break
                day_str = str(req.date_from.day)
                day_cells = page.locator('button[class*="day"], td[class*="day"]')
                dc = await day_cells.count()
                for idx in range(dc):
                    cell = day_cells.nth(idx)
                    txt = (await cell.inner_text()).strip()
                    if txt == day_str:
                        await cell.click(timeout=2000)
                        break
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("OnTheBeach: date failed: %s", e)

        # Submit — click search button
        try:
            search_btn = page.locator('button:has-text("Search"), [data-testid*="search"], .search-button')
            if await search_btn.count() > 0:
                await search_btn.first.click(timeout=5000)
        except Exception:
            pass

    def _parse_api(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        items = (
            data.get("flights") or data.get("results") or data.get("offers") or
            data.get("data", {}).get("flights") if isinstance(data.get("data"), dict) else [] or []
        )
        if isinstance(items, list):
            for item in items[:50]:
                try:
                    price = float(item.get("price") or item.get("totalPrice") or item.get("total") or 0)
                    if price <= 0:
                        continue
                    currency = item.get("currency", "GBP")
                    airline = item.get("airline") or item.get("carrier") or item.get("airlineName") or "On the Beach"
                    oid = hashlib.md5(f"otb_{req.origin}{req.destination}{price}_{id(item)}".encode()).hexdigest()[:12]
                    segments = [FlightSegment(
                        airline=str(airline), flight_no="",
                        origin=req.origin, destination=req.destination,
                        departure=datetime(2000, 1, 1), arrival=datetime(2000, 1, 1),
                        duration_seconds=0,
                    )]
                    route = FlightRoute(segments=segments, total_duration_seconds=0, stopovers=0)
                    offers.append(FlightOffer(
                        id=f"otb_{oid}", price=round(price, 2), currency=currency,
                        price_formatted=f"£{price:.2f}",
                        outbound=route, inbound=None,
                        airlines=[str(airline)], owner_airline=str(airline),
                        booking_url=_BASE,
                        is_locked=False, source="onthebeach_ota", source_tier="free",
                    ))
                except Exception:
                    continue
        return offers

    async def _scrape_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        try:
            cards = page.locator('.flight-result, .result-card, .flight-card, .search-result, [data-testid*="flight"], .offer-card')
            count = await cards.count()
            if count == 0:
                return offers

            for i in range(min(count, 50)):
                try:
                    card = cards.nth(i)
                    text = await card.inner_text()

                    price_m = re.search(r'£\s*([\d,.]+)', text)
                    if not price_m:
                        continue

                    price = float(price_m.group(1).replace(",", ""))
                    if price <= 0 or price > 50000:
                        continue

                    times = re.findall(r'(\d{1,2}:\d{2})', text)
                    dep_time = times[0] if times else "00:00"
                    arr_time = times[1] if len(times) > 1 else "00:00"

                    airline = "On the Beach"
                    for known in ["Ryanair", "easyJet", "TUI", "Jet2", "British Airways", "Wizz Air", "Vueling", "Iberia"]:
                        if known.lower() in text.lower():
                            airline = known
                            break

                    dep_dt = _parse_dt(f"{date_str}T{dep_time}:00")
                    arr_dt = _parse_dt(f"{date_str}T{arr_time}:00")

                    dur = 0
                    if dep_dt.hour and arr_dt.hour:
                        dur = int((arr_dt - dep_dt).total_seconds())
                        if dur < 0:
                            dur += 86400

                    segments = [FlightSegment(
                        airline=airline, flight_no="",
                        origin=req.origin, destination=req.destination,
                        departure=dep_dt, arrival=arr_dt, duration_seconds=max(dur, 0),
                    )]
                    route = FlightRoute(segments=segments, total_duration_seconds=max(dur, 0), stopovers=0)
                    oid = hashlib.md5(f"otb_{i}_{price}".encode()).hexdigest()[:12]

                    offers.append(FlightOffer(
                        id=f"otb_{oid}", price=round(price, 2), currency="GBP",
                        price_formatted=f"£{price:.2f}",
                        outbound=route, inbound=None,
                        airlines=[airline], owner_airline=airline,
                        booking_url=_BASE,
                        is_locked=False, source="onthebeach_ota", source_tier="free",
                    ))
                except Exception:
                    continue
        except Exception:
            pass
        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"otb{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="GBP", offers=[], total_results=0,
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
                    id=f"rt_otb_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        return {
            "checked_bag_note": "baggage allowance varies by package – often included; check at checkout",
            "bags_note": "hand baggage typically included; checked bag may be included in beach packages",
            "seat_note": "seat selection add-on at checkout; skip for free random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        pass  # conditions + bags_price set per-offer by engine's apply_ref_ancillaries via owner_airline
