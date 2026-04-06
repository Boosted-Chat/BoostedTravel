"""
WestJet (WS) CDP Chrome connector — /shop/ SPA + API response interception.

WestJet's booking flow is a Vue SPA at westjet.com/shop/.  We navigate there
with query-string params (same URL the homepage widget builds) and intercept
the POST to ``ecomm/booktrip/flight-search-api/v1`` that the SPA fires,
protected by Akamai Bot Manager.

Strategy:
1.  Launch real Chrome via CDP (Akamai bot protection).
2.  Navigate to ``/shop/?origin=…&destination=…&departure=…`` .
3.  Intercept the ``flight-search-api/v1`` JSON response.
4.  Parse ``flights[].flightOptions[]`` into FlightOffers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, date as date_type, timedelta
from typing import Optional
from urllib.parse import urlencode

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9455
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".ws_chrome_data"
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
    """Get or create a persistent browser context via CDP."""
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
            logger.info("WS: connected to existing Chrome on port %d", _DEBUG_PORT)
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
                "WS: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    """Wipe Chrome profile on persistent bot-detection blocks."""
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
            logger.info("WS: deleted stale Chrome profile")
        except Exception:
            pass


# ── Helpers ──────────────────────────────────────────────────────────────────

_FLIGHT_API = "flight-search-api/v1"


def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_dt(s: str) -> datetime:
    """Parse ISO-ish datetime strings from the WestJet API."""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M",
    ):
        try:
            return datetime.strptime(s[:len(fmt) + 3], fmt)
        except (ValueError, IndexError):
            continue
    return datetime.strptime(s[:10], "%Y-%m-%d")


def _cabin_label(codes: list) -> str:
    """Map WestJet cabin codes to readable labels."""
    if not codes:
        return "economy"
    c = codes[0]
    if c == "W":
        return "premium_economy"
    if c in ("C", "J"):
        return "business"
    return "economy"


class WestjetConnectorClient:
    """WestJet CDP Chrome connector — /shop/ SPA + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        api_data: dict = {}
        blocked = False

        async def _on_response(response):
            nonlocal blocked
            url = response.url
            if _FLIGHT_API not in url:
                return
            status = response.status
            if status in (403, 429):
                blocked = True
                logger.warning("WS: %d on %s", status, url[:120])
                return
            if status != 200:
                return
            try:
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = await response.text()
                data = json.loads(body)
                if isinstance(data, dict) and data.get("flights"):
                    api_data.update(data)
                    logger.info(
                        "WS: captured %d bytes from %s",
                        len(body), url[:100],
                    )
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            shop_url = self._build_shop_url(req)
            logger.info("WS: loading %s->%s via /shop/", req.origin, req.destination)
            await page.goto(shop_url, wait_until="domcontentloaded", timeout=30000)

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while not api_data and not blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            if blocked:
                logger.warning("WS: Akamai blocked, resetting profile")
                await _reset_profile()
                return self._empty(req)

            if not api_data:
                logger.warning("WS: no flight API data captured")
                return self._empty(req)

            offers = self._parse_flights(api_data, req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "WS %s->%s: %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"ws{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]

            flights = api_data.get("flights", [{}])
            currency = flights[0].get("currency", "CAD") if flights else "CAD"

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("WS CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # URL construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_shop_url(req: FlightSearchRequest) -> str:
        """Build the /shop/ URL that the booking widget would navigate to."""
        dt = _to_datetime(req.date_from)
        dep = dt.strftime("%Y-%m-%d")
        params = {
            "origin": req.origin,
            "destination": req.destination,
            "departure": dep,
            "outboundDate": dep,
            "lang": "en-CA",
            "adults": str(req.adults or 1),
            "children": str(req.children or 0),
            "infants": str(req.infants or 0),
            "companionvoucher": "false",
            "currency": "CAD",
            "appSource": "widgetone-way",
        }
        return f"https://www.westjet.com/shop/?{urlencode(params)}"

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_flights(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        for flight_block in data.get("flights", []):
            currency = flight_block.get("currency", "CAD")

            for opt in flight_block.get("flightOptions", []):
                details = opt.get("flightDetails", {})
                raw_segs = details.get("flightSegments", [])
                if not raw_segs:
                    continue

                # Cheapest fare = first priceDetails (sorted by fareSortOrder)
                adult_fare = opt.get("adultFare", {})
                price_list = adult_fare.get("priceDetails", [])
                if not price_list:
                    continue
                cheapest = min(price_list, key=lambda p: p.get("fareSortOrder", 999))
                try:
                    price = float(cheapest["totalFareAmount"])
                except (KeyError, ValueError, TypeError):
                    continue
                if price <= 0:
                    continue

                cabin = _cabin_label(cheapest.get("cabinCodes", []))

                # Build segments
                segments: list[FlightSegment] = []
                for seg in raw_segs:
                    dep_raw = seg.get("departureDateRaw", "")
                    arr_raw = seg.get("arrivalDateRaw", "")
                    dep_dt = _parse_dt(dep_raw) if dep_raw else _to_datetime(req.date_from)
                    arr_dt = _parse_dt(arr_raw) if arr_raw else dep_dt + timedelta(hours=2)
                    dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

                    carrier = seg.get("operatingAirline", "WS")
                    fno = seg.get("flightNumber", "")
                    flight_code = f"{carrier}{fno}" if fno else f"{carrier}?"

                    segments.append(FlightSegment(
                        airline=carrier,
                        airline_name=seg.get("operatingAirlineName", "WestJet").title(),
                        flight_no=flight_code,
                        origin=seg.get("originCode", req.origin),
                        destination=seg.get("destinationCode", req.destination),
                        departure=dep_dt,
                        arrival=arr_dt,
                        duration_seconds=dur,
                        cabin_class=cabin,
                    ))

                if not segments:
                    continue

                total_dur_obj = details.get("totalTravelDuration", {})
                total_dur = (
                    total_dur_obj.get("hrs", 0) * 3600
                    + total_dur_obj.get("mins", 0) * 60
                )
                if not total_dur:
                    total_dur = int(
                        (segments[-1].arrival - segments[0].departure).total_seconds()
                    )

                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=max(total_dur, 0),
                    stopovers=max(len(segments) - 1, 0),
                )

                offer_key = f"ws_{segments[0].flight_no}_{segments[0].departure.isoformat()}_{price}"
                offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]
                all_airlines = list({s.airline for s in segments})

                offers.append(FlightOffer(
                    id=f"ws_{offer_id}",
                    price=round(price, 2),
                    currency=currency,
                    outbound=route,
                    airlines=[("WestJet" if a == "WS" else a) for a in all_airlines],
                    owner_airline="WS",
                    booking_url=self._user_url(req),
                    is_locked=False,
                    source="westjet_direct",
                    source_tier="free",
                ))

        return offers

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _user_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        dep = dt.strftime("%Y-%m-%d")
        return (
            f"https://www.westjet.com/shop/"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departure={dep}&outboundDate={dep}"
            f"&lang=en-CA&adults={req.adults or 1}"
            f"&appSource=widgetone-way"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"ws{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CAD",
            offers=[],
            total_results=0,
        )
