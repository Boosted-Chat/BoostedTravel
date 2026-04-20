"""
Aunt Betty connector — Australian OTA (Flight Centre group, CDP Chrome).

Aunt Betty (auntbetty.com.au) is an Australian OTA brand of Flight Centre
Travel Group. Shares the same backend as BYOjet — booking.auntbetty.com
with ``/Api/search`` JSON responses.

Strategy (CDP Chrome + deep-link + API interception):
1. Launch real Chrome via --remote-debugging-port.
2. Navigate to booking.auntbetty.com deep-link (no form fill needed).
3. Intercept JSON ``/Api/search`` responses with flight data.
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
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, bandwidth_saving_args, disable_background_networking_args, apply_cdp_url_blocking

logger = logging.getLogger(__name__)

_BASE = "https://www.auntbetty.com.au"
_BOOKING = "https://booking.auntbetty.com"
_CDP_PORT = 9517
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".auntbetty_chrome_data"
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
            *proxy_chrome_args(),
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            *bandwidth_saving_args(),
            *disable_background_networking_args(),
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
        logger.info("AuntBetty: Chrome launched on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


async def _dismiss_cookies(page) -> None:
    try:
        btn = page.locator('#onetrust-accept-btn-handler')
        if await btn.count() > 0:
            await btn.click(timeout=3000)
            await asyncio.sleep(0.5)
            return
    except Exception:
        pass
    for label in ["Accept All Cookies", "Accept All", "Accept", "OK", "Got it", "I agree", "Allow all"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue


class AuntbettyConnectorClient:
    """Aunt Betty — Australian OTA (Flight Centre group), CDP Chrome + deep-link."""

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
        date_str = req.date_from.strftime("%Y-%m-%d")

        context = await _get_context()
        page = await context.new_page()
        await apply_cdp_url_blocking(page)

        search_result = None

        async def _on_response(response):
            nonlocal search_result
            url = response.url
            if response.status == 200 and "/Api/search" in url:
                try:
                    data = await response.json()
                    if isinstance(data, dict) and data.get("success") and data.get("resultSets"):
                        search_result = data
                except Exception:
                    pass

        page.on("response", _on_response)

        try:
            month_param = f"{req.date_from.month:02d}%2F{req.date_from.year}"
            day_param = req.date_from.day
            adults = req.adults if hasattr(req, 'adults') and req.adults else 1
            children = req.children if hasattr(req, 'children') and req.children else 0
            infants = req.infants if hasattr(req, 'infants') and req.infants else 0
            deep_url = (
                f"{_BOOKING}/#/au/search?"
                f"Class=E&TotalAdults={adults}&TotalChildren={children}&TotalInfants={infants}"
                f"&Sort=0&OriginCode={req.origin}&DestinationCode={req.destination}"
                f"&DepartMonth={month_param}&DepartDay={day_param}"
            )

            logger.info("AuntBetty: navigating to deep-link %s→%s on %s", req.origin, req.destination, date_str)
            await page.goto(deep_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))

            await _dismiss_cookies(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining

            while time.monotonic() < deadline and not search_result:
                await asyncio.sleep(1.5)
                try:
                    text = await page.evaluate("() => document.body?.innerText?.substring(0, 300) || ''")
                    if "no results" in text.lower() or "no flights" in text.lower():
                        logger.info("AuntBetty: no results found")
                        break
                except Exception:
                    pass

            offers: list[FlightOffer] = []
            if search_result:
                offers = self._parse(search_result, req, date_str)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("AuntBetty %s→%s: %d offers in %.1fs (CDP Chrome)", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"auntbetty{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "AUD",
                offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("AuntBetty CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    _AIRLINE_NAMES = {
        "VA": "Virgin Australia", "QF": "Qantas Airways", "JQ": "Jetstar Airways",
        "ZL": "Regional Express", "NZ": "Air New Zealand",
        "SQ": "Singapore Airlines", "CX": "Cathay Pacific", "MH": "Malaysia Airlines",
        "EK": "Emirates", "QR": "Qatar Airways", "TG": "Thai Airways",
        "EY": "Etihad Airways", "BA": "British Airways", "LH": "Lufthansa",
    }

    def _parse(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        result_sets = data.get("resultSets") or []
        if not isinstance(result_sets, list) or not result_sets:
            return offers

        results = result_sets[0].get("results") or []
        if not isinstance(results, list):
            return offers

        currency = "AUD"

        for item in results[:50]:
            try:
                price = float(item.get("totalPrice") or 0)
                if price <= 0:
                    continue

                journeys = item.get("journeys") or []
                if not journeys:
                    continue

                journey = journeys[0]
                journey_segs = journey.get("segments") or []
                if not journey_segs:
                    continue

                j_origin = journey.get("originCode") or req.origin
                j_dest = journey.get("destinationCode") or req.destination
                j_stops = journey.get("stops") or 0

                segments: list[FlightSegment] = []
                for si, seg in enumerate(journey_segs):
                    flight_no_raw = seg.get("flightNumber") or ""
                    operator = seg.get("operatorCode") or seg.get("vendorCode") or ""
                    airline_name = self._AIRLINE_NAMES.get(operator, operator)

                    dep_dt = _parse_dt(seg.get("departureDateTime"))
                    arr_dt = _parse_dt(seg.get("arrivalDateTime"))
                    dur_min = seg.get("durationMinutes") or 0

                    if len(journey_segs) == 1:
                        dep_code, arr_code = j_origin, j_dest
                    elif si == 0:
                        dep_code, arr_code = j_origin, ""
                    elif si == len(journey_segs) - 1:
                        dep_code, arr_code = "", j_dest
                    else:
                        dep_code, arr_code = "", ""

                    segments.append(FlightSegment(
                        airline=airline_name or "Aunt Betty",
                        flight_no=flight_no_raw,
                        origin=dep_code or req.origin,
                        destination=arr_code or req.destination,
                        departure=dep_dt, arrival=arr_dt,
                        duration_seconds=int(dur_min) * 60 if dur_min else 0,
                    ))

                if not segments:
                    continue

                total_dur = journey.get("durationMinutes")
                total_dur_secs = int(total_dur) * 60 if total_dur else sum(s.duration_seconds for s in segments)

                route = FlightRoute(
                    segments=segments, total_duration_seconds=total_dur_secs,
                    stopovers=max(0, int(j_stops)),
                )
                oid = hashlib.md5(f"ab_{req.origin}{req.destination}{date_str}{price}{segments[0].flight_no}".encode()).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"ab_{oid}", price=round(price, 2), currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route, inbound=None,
                    airlines=list({s.airline for s in segments if s.airline}),
                    owner_airline=segments[0].airline if segments else "Aunt Betty",
                    booking_url=f"{_BOOKING}/#/au/search?OriginCode={req.origin}&DestinationCode={req.destination}",
                    is_locked=False, source="auntbetty_ota", source_tier="free",
                ))
            except Exception as e:
                logger.debug("AuntBetty parse error: %s", e)

        return offers

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
                    id=f"rt_ab_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"auntbetty{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="AUD", offers=[], total_results=0,
        )
