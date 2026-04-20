"""
Peach Aviation CDP Chrome scraper — direct booking URL + DOM extraction.

Peach Aviation (IATA: MM) is a Japanese LCC (ANA group).
Booking site: booking.flypeach.com

Strategy (converted Mar 2026):
1. Launch real system Chrome via CDP (persistent, survives across searches)
2. Build direct search URL with JSON params (bypasses homepage form)
3. Navigate to booking.flypeach.com/en/getsearch?s=[params]
4. Click "Search by One-way" to submit
5. Extract flight data from server-rendered DOM
6. Parse → FlightOffer objects

Real Chrome passes reCAPTCHA better than Playwright's bundled Chromium.
Persistent browser avoids ~5s launch overhead per search.
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
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, auto_block_if_proxied

logger = logging.getLogger(__name__)

# ── CDP Chrome (headed — headless causes page-load timeout on Peach) ─────
_CDP_PORT = 9468
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "peach_cdp_data")

_chrome_proc: subprocess.Popen | None = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _launch_chrome():
    """Launch headed Chrome with CDP.

    MUST be headed (no --headless=new): headless Chrome times out loading
    Peach pages.  --disable-http2 prevents ERR_HTTP2_PROTOCOL_ERROR.
    The window is pushed off-screen so it doesn't disturb the user.
    """
    global _chrome_proc
    if _chrome_proc and _chrome_proc.poll() is None:
        return
    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    chrome = find_chrome()
    _chrome_proc = subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--no-first-run", "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            "about:blank",
        ],
        **stealth_popen_kwargs(),
    )
    logger.info("Peach: Chrome launched on CDP port %d (pid=%d)", _CDP_PORT, _chrome_proc.pid)


async def _get_browser():
    """Connect to (or launch) Chrome via CDP. Reused across searches."""
    global _pw_instance, _cdp_browser
    lock = _get_lock()
    async with lock:
        if _cdp_browser and _cdp_browser.is_connected():
            return _cdp_browser
        _launch_chrome()
        await asyncio.sleep(2)
        from playwright.async_api import async_playwright
        if not _pw_instance:
            _pw_instance = await async_playwright().start()
        for attempt in range(5):
            try:
                _cdp_browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{_CDP_PORT}"
                )
                logger.info("Peach: connected to Chrome via CDP")
                return _cdp_browser
            except Exception:
                if attempt < 4:
                    await asyncio.sleep(1)
        raise RuntimeError(f"Peach: cannot connect to Chrome CDP on port {_CDP_PORT}")


class PeachConnectorClient:
    """Peach Aviation scraper — direct booking URL + DOM extraction."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = browser.contexts[0] if browser.contexts else await browser.new_context()

        ob_offers = await self._search_ow_page(context, req)

        # --- RT: second OW search for inbound ---
        if req.return_from and ob_offers:
            ib_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "return_from": None,
            })
            ib_offers = await self._search_ow_page(context, ib_req)
            if ib_offers:
                ob_offers = self._combine_rt(ob_offers, ib_offers, req)

        elapsed = time.monotonic() - t0
        if ob_offers:
            return self._build_response(ob_offers, req, elapsed)
        return self._empty(req)

    async def _search_ow_page(self, context, req: FlightSearchRequest) -> list[FlightOffer]:
        """Run one OW search on a fresh page — getsearch URL auto-redirects to results."""
        page = None
        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)

            search_url = self._build_search_url(req)
            logger.info("Peach: navigating to booking URL for %s→%s on %s",
                        req.origin, req.destination, req.date_from.strftime("%Y/%m/%d"))

            # getsearch URL now auto-redirects to /en/flight_search with results
            await page.goto(search_url, wait_until="domcontentloaded",
                            timeout=int(self.timeout * 1000))

            # Wait for flight results page
            try:
                await page.wait_for_url("**/flight_search**", timeout=25000)
            except Exception:
                if "flight_search" not in page.url:
                    challenge = await page.evaluate("""() => {
                        const f = document.querySelector('iframe[title*="recaptcha"]');
                        return f ? f.offsetHeight > 100 : false;
                    }""")
                    if challenge:
                        logger.warning("Peach: reCAPTCHA image challenge appeared — cannot auto-solve")
                    else:
                        logger.warning("Peach: did not reach flight_search (at %s)", page.url)
                    return []

            # Let the page settle (avoids "execution context destroyed" from JS redirects)
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(3)

            flights_data = await self._extract_flights_from_dom(page)
            if not flights_data:
                logger.warning("Peach: no flights extracted from DOM")
                return []

            return self._build_offers(flights_data, req)

        except Exception as e:
            logger.error("Peach error: %s", e)
            return []
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # URL building
    # ------------------------------------------------------------------

    @staticmethod
    def _build_search_url(req: FlightSearchRequest) -> str:
        """Build direct booking search URL with JSON params."""
        params = [{
            "departure_date": req.date_from.strftime("%Y/%m/%d"),
            "departure_airport_code": req.origin,
            "arrival_airport_code": req.destination,
            "is_return": False,
        }]
        json_str = json.dumps(params, separators=(",", ":"))
        encoded = urllib.parse.quote(json_str)
        return f"https://booking.flypeach.com/en/getsearch?s={encoded}"

    # ------------------------------------------------------------------
    # DOM extraction
    # ------------------------------------------------------------------

    async def _extract_flights_from_dom(self, page) -> list[dict]:
        """Extract flight data from Peach's server-rendered results page.

        DOM structure per flight row (observed from live site):
        - paragraph with flight number (MM307) and aircraft type (A320)
        - time elements: departure HH:MM, arrow, arrival HH:MM  
        - duration text (1Hour30Min(s))
        - fare cells with prices (￥3,990) and seats info (e.g. "4 seats left at this price")
        - Three fare tiers: Minimum, Standard, Standard Plus
        """
        return await page.evaluate(r"""() => {
            const results = [];
            const body = document.body.innerText || '';

            // Find all flight number occurrences (MM + 2-4 digits)
            const flightNoRegex = /MM\d{2,4}/g;
            const allMatches = [...body.matchAll(flightNoRegex)];
            const uniqueFlights = [...new Set(allMatches.map(m => m[0]))];

            // For each unique flight, find its containing element and extract data
            for (const flightNo of uniqueFlights) {
                // Find ANY element (div, span, p) containing exactly this flight number
                const fnEls = [];
                document.querySelectorAll('p, div, span').forEach(el => {
                    if (el.textContent.trim() === flightNo && el.children.length === 0) fnEls.push(el);
                });
                if (fnEls.length === 0) continue;
                const fnEl = fnEls[0];

                // Walk up to find the flight row container (has >=4 children for columns)
                let row = fnEl;
                for (let i = 0; i < 10; i++) {
                    if (!row.parentElement) break;
                    row = row.parentElement;
                    if (row.children.length >= 4) break;
                }

                const text = row.innerText || '';

                // Aircraft type: nearby leaf element with A3XX/B7XX pattern
                let aircraft = '';
                const nearbyEls = row.querySelectorAll('p, div, span');
                nearbyEls.forEach(el => {
                    const t = el.textContent.trim();
                    if (/^[AB]\d{3}/.test(t) && el.children.length === 0) aircraft = t;
                });

                // Times (HH:MM pattern)
                const timeMatches = text.match(/(\d{2}:\d{2})/g) || [];

                // Prices: ￥ followed by digits with commas
                const priceMatches = text.match(/￥([\d,]+)/g) || [];
                const prices = priceMatches.map(
                    p => parseInt(p.replace(/[￥,]/g, ''))
                );

                // Seats remaining ("N seats left")
                const seatMatches = [...text.matchAll(/(\d+)\s*seats?\s*left/gi)];
                const seats = seatMatches.map(m => parseInt(m[1]));

                // Duration (e.g. "1Hour30Min(s)")
                let durationMins = 0;
                const durMatch = text.match(/(\d+)\s*Hour\s*(\d+)\s*Min/i);
                if (durMatch) {
                    durationMins = parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]);
                }

                if (flightNo && timeMatches.length >= 2) {
                    results.push({
                        flight_no: flightNo,
                        aircraft: aircraft,
                        dep_time: timeMatches[0],
                        arr_time: timeMatches[1],
                        duration_mins: durationMins,
                        prices: prices,
                        seats: seats,
                    });
                }
            }

            return results;
        }""")

    # ------------------------------------------------------------------
    # Offer building
    # ------------------------------------------------------------------

    def _build_offers(self, flights_data: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)

        for flight in flights_data:
            prices = [p for p in flight.get("prices", []) if p > 0]
            if not prices:
                continue
            best_price = min(prices)

            flight_no = flight.get("flight_no", "")
            dep_time = flight.get("dep_time", "")
            arr_time = flight.get("arr_time", "")
            duration_mins = flight.get("duration_mins", 0)

            dep_dt = self._time_on_date(dep_time, req.date_from)
            arr_dt = self._time_on_date(arr_time, req.date_from)

            if arr_dt < dep_dt:
                arr_dt += timedelta(days=1)

            total_dur = (
                duration_mins * 60
                if duration_mins
                else max(int((arr_dt - dep_dt).total_seconds()), 0)
            )

            _mm_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="MM",
                airline_name="Peach Aviation",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                cabin_class=_mm_cabin,
            )

            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=max(total_dur, 0),
                stopovers=0,
            )

            fkey = f"{flight_no}_{dep_dt.isoformat()}"
            offers.append(FlightOffer(
                id=f"mm_{hashlib.md5(fkey.encode()).hexdigest()[:12]}",
                price=round(best_price, 2),
                currency="JPY",
                price_formatted=f"¥{best_price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=["Peach Aviation"],
                owner_airline="MM",
                booking_url=booking_url,
                is_locked=False,
                source="peach_direct",
                source_tier="free",
            ))

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _time_on_date(time_str: str, date) -> datetime:
        """Combine HH:MM string with a date into a datetime."""
        if not time_str:
            return datetime(2000, 1, 1)
        try:
            h, m = time_str.split(":")
            return datetime(date.year, date.month, date.day, int(h), int(m))
        except (ValueError, IndexError):
            return datetime(2000, 1, 1)

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Peach %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"peach{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="JPY", offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        params = json.dumps([{
            "departure_date": req.date_from.strftime("%Y/%m/%d"),
            "departure_airport_code": req.origin,
            "arrival_airport_code": req.destination,
            "is_return": False,
        }], separators=(",", ":"))
        return f"https://booking.flypeach.com/en/getsearch?s={urllib.parse.quote(params)}"

    def _combine_rt(
        self,
        ob_offers: list[FlightOffer],
        ib_offers: list[FlightOffer],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Cross-multiply OB x IB one-way offers into RT combos."""
        ob_sorted = sorted(ob_offers, key=lambda o: o.price)[:15]
        ib_sorted = sorted(ib_offers, key=lambda o: o.price)[:10]
        params = json.dumps([{
            "departure_date": req.date_from.strftime("%Y/%m/%d"),
            "departure_airport_code": req.origin,
            "arrival_airport_code": req.destination,
            "is_return": True,
        }], separators=(",", ":"))
        bk_url = f"https://booking.flypeach.com/en/getsearch?s={urllib.parse.quote(params)}"
        combined: list[FlightOffer] = []
        for ob in ob_sorted:
            for ib in ib_sorted:
                total = round(ob.price + ib.price, 2)
                key = f"{ob.id}_{ib.id}"
                cid = f"mm_rt_{hashlib.md5(key.encode()).hexdigest()[:12]}"
                combined.append(FlightOffer(
                    id=cid,
                    price=total,
                    currency="JPY",
                    price_formatted=f"¥{total:,.0f}",
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    airlines=["Peach Aviation"],
                    owner_airline="MM",
                    booking_url=bk_url,
                    is_locked=False,
                    source="peach_direct",
                    source_tier="free",
                ))
        combined.sort(key=lambda o: o.price)
        return combined

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"peach{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="JPY", offers=[], total_results=0,
        )
