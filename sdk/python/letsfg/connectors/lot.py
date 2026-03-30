"""
LOT Polish Airlines (LO) CDP Chrome connector — direct API via same-origin fetch.

LOT Polish Airlines is Poland's flag carrier (Star Alliance), hub at Warsaw
Chopin (WAW).  The booking SPA calls ``POST /api/v1/ibe/search/air-bounds``
for flight availability.

Strategy (CDP Chrome + same-origin fetch):
1.  Launch real Chrome via CDP (--remote-debugging-port).
2.  Navigate to the homepage once to establish AWS WAF / Akamai cookies.
3.  Call ``POST /api/v1/ibe/search/air-bounds`` from the page context via
    ``page.evaluate(fetch(…))`` with required custom headers.
4.  Parse the JSON response into FlightOffers.

Required custom headers (set by Angular HTTP interceptor):
  language, market, channel, action, step, x-xsrf-token
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
from datetime import datetime, date as date_type, timedelta
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

_DEBUG_PORT = 9459
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".lo_chrome_data"
)

_AIR_BOUNDS_URL = "https://www.lot.com/api/v1/ibe/search/air-bounds"

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_homepage_warmed = False


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
            logger.info("LO: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info(
                "LO: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    global _browser, _context, _pw_instance, _chrome_proc, _homepage_warmed
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
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    _homepage_warmed = False
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("LO: deleted stale Chrome profile")
        except Exception:
            pass


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


# Segment ID format: SEG-LO26-WAWJFK-2026-04-14-1650
_SEG_RE = re.compile(
    r"^SEG-([A-Z0-9]+)-([A-Z]{3})([A-Z]{3})-(\d{4}-\d{2}-\d{2})-(\d{4})$"
)


def _parse_segment_id(seg_id: str) -> dict | None:
    m = _SEG_RE.match(seg_id)
    if not m:
        return None
    flight_no, origin, dest, date_str, hhmm = m.groups()
    dep_dt = datetime.strptime(f"{date_str} {hhmm[:2]}:{hhmm[2:]}", "%Y-%m-%d %H:%M")
    return {
        "flight_no": flight_no,
        "origin": origin,
        "destination": dest,
        "departure": dep_dt,
    }


class LotConnectorClient:
    """LOT Polish Airlines CDP Chrome connector — direct API via same-origin fetch."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ------------------------------------------------------------------
    # Main search entry-point
    # ------------------------------------------------------------------

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        global _homepage_warmed
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        try:
            # Warm cookies by visiting homepage
            if not _homepage_warmed:
                logger.info("LO: warming cookies via homepage")
                await page.goto(
                    "https://www.lot.com/us/en",
                    wait_until="domcontentloaded",
                    timeout=25000,
                )
                await asyncio.sleep(5)
                _homepage_warmed = True
            else:
                await page.goto(
                    "https://www.lot.com/us/en",
                    wait_until="domcontentloaded",
                    timeout=20000,
                )
                await asyncio.sleep(2)

            # Build request payload
            dt = _to_datetime(req.date_from)
            date_str = dt.strftime("%Y-%m-%d")
            adults = req.adults or 1
            children = req.children or 0
            infants = req.infants or 0

            travelers = ["ADT"] * adults + ["CHD"] * children + ["INF"] * infants

            payload = {
                "travelers": travelers,
                "compartment": "ECONOMY",
                "itinerary": [
                    {
                        "originLocationCode": req.origin,
                        "destinationLocationCode": req.destination,
                        "departureDate": date_str,
                        "isRequestedBound": True,
                    }
                ],
                "searchPreferences": {},
                "promotion": None,
            }

            # Call the air-bounds API from page context
            api_result = await page.evaluate(
                """async ([url, payload]) => {
                    // Extract XSRF token from cookies
                    let xsrf = '';
                    for (const c of document.cookie.split(';')) {
                        const t = c.trim();
                        if (t.startsWith('__HOST-XSRF-TOKEN=') ||
                            t.startsWith('__Host-XSRF-TOKEN=')) {
                            xsrf = t.split('=').slice(1).join('=');
                        }
                    }
                    try {
                        const resp = await fetch(url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'language': 'en',
                                'market': 'us',
                                'channel': '1',
                                'action': 'DO_SEARCH',
                                'step': 'SEARCH',
                                'x-xsrf-token': xsrf,
                            },
                            credentials: 'include',
                            body: JSON.stringify(payload),
                        });
                        const text = await resp.text();
                        return {status: resp.status, body: text};
                    } catch(e) {
                        return {error: e.message};
                    }
                }""",
                [_AIR_BOUNDS_URL, payload],
            )

            status = api_result.get("status", 0)
            body_text = api_result.get("body", "")

            if api_result.get("error"):
                logger.error("LO: fetch error: %s", api_result["error"])
                return self._empty(req)

            if status == 403 or status == 429:
                logger.warning("LO: blocked (%d), resetting profile", status)
                await _reset_profile()
                return self._empty(req)

            if status != 200:
                logger.warning("LO: API returned %d: %s", status, body_text[:200])
                _homepage_warmed = False
                return self._empty(req)

            data = json.loads(body_text)
            offers = self._parse_offers(data, req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "LO %s->%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"lo{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            currency = "USD"
            if offers:
                currency = offers[0].currency

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("LO CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_offers(
        self, data: dict, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse /api/v1/ibe/search/air-bounds response."""
        offers: list[FlightOffer] = []
        air_bound_flights = (data.get("data") or {}).get("airBoundFlights", [])
        if not isinstance(air_bound_flights, list):
            return offers

        for abf in air_bound_flights:
            flight = abf.get("flight", {})
            ab_offers = abf.get("airBoundOffers", [])
            if not flight or not ab_offers:
                continue

            origin = flight.get("originLocationCode", req.origin)
            dest = flight.get("destinationLocationCode", req.destination)
            total_dur = flight.get("duration", 0)
            raw_segments = flight.get("segments", [])

            # Parse segments from segment IDs
            segments: list[FlightSegment] = []
            for raw_seg in raw_segments:
                seg_id = raw_seg.get("segmentId", "")
                parsed = _parse_segment_id(seg_id)
                if not parsed:
                    continue
                segments.append(
                    FlightSegment(
                        airline="LO",
                        airline_name="LOT Polish Airlines",
                        flight_no=parsed["flight_no"],
                        origin=parsed["origin"],
                        destination=parsed["destination"],
                        departure=parsed["departure"],
                        arrival=parsed["departure"],  # placeholder
                        duration_seconds=0,
                        cabin_class="economy",
                    )
                )

            if not segments:
                continue

            # Compute arrival for last segment using total duration
            if total_dur > 0 and len(segments) == 1:
                segments[0] = FlightSegment(
                    airline=segments[0].airline,
                    airline_name=segments[0].airline_name,
                    flight_no=segments[0].flight_no,
                    origin=segments[0].origin,
                    destination=segments[0].destination,
                    departure=segments[0].departure,
                    arrival=segments[0].departure + timedelta(seconds=total_dur),
                    duration_seconds=total_dur,
                    cabin_class="economy",
                )
            elif total_dur > 0 and len(segments) > 1:
                # Multi-segment: compute per-segment durations from gap between departures
                for i in range(len(segments) - 1):
                    gap = int((segments[i + 1].departure - segments[i].departure).total_seconds())
                    seg_dur = max(gap, 0)
                    segments[i] = FlightSegment(
                        airline=segments[i].airline,
                        airline_name=segments[i].airline_name,
                        flight_no=segments[i].flight_no,
                        origin=segments[i].origin,
                        destination=segments[i].destination,
                        departure=segments[i].departure,
                        arrival=segments[i].departure + timedelta(seconds=seg_dur),
                        duration_seconds=seg_dur,
                        cabin_class="economy",
                    )
                # Last segment: remaining duration
                elapsed = int((segments[-1].departure - segments[0].departure).total_seconds())
                last_dur = max(total_dur - elapsed, 0)
                segments[-1] = FlightSegment(
                    airline=segments[-1].airline,
                    airline_name=segments[-1].airline_name,
                    flight_no=segments[-1].flight_no,
                    origin=segments[-1].origin,
                    destination=segments[-1].destination,
                    departure=segments[-1].departure,
                    arrival=segments[-1].departure + timedelta(seconds=last_dur),
                    duration_seconds=last_dur,
                    cabin_class="economy",
                )

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=max(len(segments) - 1, 0),
            )

            # Take cheapest economy offer (marked isCheapestOffer or lowest total)
            best_price = None
            best_currency = "USD"
            best_cabin = "economy"
            for offer in ab_offers:
                avail = offer.get("availabilityDetails", [{}])
                compartment = avail[0].get("compartment", "ECONOMY") if avail else "ECONOMY"
                if compartment not in ("ECONOMY", "PREMIUM_ECONOMY"):
                    continue
                total_prices = (offer.get("prices") or {}).get("totalPrices", [])
                if not total_prices:
                    continue
                total_cents = total_prices[0].get("total", 0)
                cur = total_prices[0].get("currencyCode", "USD")
                price = total_cents / 100.0
                if price > 0 and (best_price is None or price < best_price):
                    best_price = price
                    best_currency = cur
                    best_cabin = "premium_economy" if compartment == "PREMIUM_ECONOMY" else "economy"

            # Fallback: take cheapest across all cabins
            if best_price is None:
                for offer in ab_offers:
                    total_prices = (offer.get("prices") or {}).get("totalPrices", [])
                    if not total_prices:
                        continue
                    total_cents = total_prices[0].get("total", 0)
                    cur = total_prices[0].get("currencyCode", "USD")
                    price = total_cents / 100.0
                    if price > 0 and (best_price is None or price < best_price):
                        best_price = price
                        best_currency = cur

            if not best_price or best_price <= 0:
                continue

            # Update cabin class on segments
            for i, seg in enumerate(segments):
                segments[i] = FlightSegment(
                    airline=seg.airline,
                    airline_name=seg.airline_name,
                    flight_no=seg.flight_no,
                    origin=seg.origin,
                    destination=seg.destination,
                    departure=seg.departure,
                    arrival=seg.arrival,
                    duration_seconds=seg.duration_seconds,
                    cabin_class=best_cabin,
                )

            offer_key = (
                f"lo_{req.origin}_{req.destination}"
                f"_{segments[0].departure.isoformat()}_{best_price}"
            )
            offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]
            all_airlines = list({s.airline for s in segments})

            offers.append(
                FlightOffer(
                    id=f"lo_{offer_id}",
                    price=round(best_price, 2),
                    currency=best_currency,
                    outbound=route,
                    airlines=[
                        ("LOT Polish Airlines" if a == "LO" else a)
                        for a in all_airlines
                    ],
                    owner_airline="LO",
                    booking_url=self._user_booking_url(req),
                    is_locked=False,
                    source="lot_direct",
                    source_tier="free",
                )
            )

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _user_booking_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        return (
            f"https://www.lot.com/us/en/offer/flights"
            f"?departureAirport={req.origin}&arrivalAirport={req.destination}"
            f"&departureDate={dt.strftime('%d.%m.%Y')}&adults={req.adults or 1}"
            f"&cabinClass=ECONOMY&tripType=ONE_WAY"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"lo{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
            offers=[],
            total_results=0,
        )
