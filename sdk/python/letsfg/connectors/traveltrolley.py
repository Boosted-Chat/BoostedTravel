"""
Travel Trolley connector — CDP Chrome + Cloudflare bypass.

Travel Trolley (traveltrolley.co.uk) is a UK-based OTA specialising in
flights to India, Africa, and the Middle East. Uses Cloudflare protection
so requires real Chrome via CDP.

Strategy (CDP Chrome + form fill + DOM scraping):
1.  Launch real Chrome via --remote-debugging-port to bypass Cloudflare.
2.  Navigate to traveltrolley.co.uk and fill the search form.
3.  Intercept API responses or scrape result cards from DOM.
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

_BASE = "https://www.traveltrolley.co.uk"
_CDP_PORT = 9497
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".traveltrolley_chrome_data"
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
        logger.info("TravelTrolley: Chrome on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


class TraveltrolleyConnectorClient:
    """Travel Trolley — UK OTA (India/Africa routes), CDP Chrome + form fill."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")

        context = await _get_context()
        page = await context.new_page()

        try:
            logger.info("TravelTrolley: searching %s→%s on %s", req.origin, req.destination, date_str)

            # Direct URL navigation — bypasses Blazor form fill entirely
            search_url = (
                f"{_BASE}/flight-results"
                f"?dcity={req.origin}&acity={req.destination}"
                f"&ddate={date_str}"
                f"&dairport={req.origin}&aairport={req.destination}"
                f"&triptype=ow&class=y&aqty={req.adults}&nonstop=false"
            )
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for Cloudflare to pass
            for _ in range(10):
                await asyncio.sleep(2)
                try:
                    body_text = await page.evaluate("() => document.body?.innerText?.substring(0, 300) || ''")
                    if "cloudflare" not in body_text.lower() and "challenge" not in body_text.lower() and len(body_text) > 50:
                        break
                except Exception:
                    pass

            # Wait for results to render (Blazor SignalR)
            deadline = time.monotonic() + min(self.timeout - (time.monotonic() - t0), 40)
            offers: list[FlightOffer] = []

            while time.monotonic() < deadline:
                await asyncio.sleep(3)

                try:
                    dom_offers = await self._scrape_dom(page, req, date_str)
                    if dom_offers:
                        offers = dom_offers
                        break
                except Exception:
                    pass

                try:
                    text = await page.evaluate("() => document.body?.innerText?.substring(0, 500) || ''")
                    if "no result" in text.lower() or "no flight" in text.lower():
                        break
                except Exception:
                    pass

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("TravelTrolley %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"tt{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_tt_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "GBP",
                offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("TravelTrolley error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _scrape_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Scrape flight-list-card elements from TravelTrolley results page.

        Each ``.flight-list-card`` contains departure/arrival info on the left
        and price on the right.  Text pattern:
            LHR 10:00 \\n 13h 35min \\n 1 Stop \\n DEL 04:05 +1 \\n ... \\n £296.29
        """
        offers: list[FlightOffer] = []
        results_url = page.url  # current page is the results page with search params
        try:
            cards = page.locator('.flight-list-card')
            count = await cards.count()
            if count == 0:
                return offers

            for i in range(min(count, 50)):
                try:
                    card = cards.nth(i)
                    text = await card.inner_text()

                    # Price — £NNN.NN
                    price_m = re.search(r'£\s*([\d,.]+)', text)
                    if not price_m:
                        continue
                    price = float(price_m.group(1).replace(",", ""))
                    if price <= 0 or price > 50000:
                        continue

                    # Times — HH:MM patterns
                    times = re.findall(r'(\d{1,2}:\d{2})', text)
                    dep_time = times[0] if times else "00:00"
                    arr_time = times[1] if len(times) > 1 else "00:00"

                    # Duration — e.g. "13h 35min"
                    dur_m = re.search(r'(\d+)h\s*(\d+)\s*min', text)
                    dur_sec = 0
                    if dur_m:
                        dur_sec = int(dur_m.group(1)) * 3600 + int(dur_m.group(2)) * 60

                    # Stops
                    stops = 0
                    stops_m = re.search(r'(\d+)\s*Stop', text)
                    if stops_m:
                        stops = int(stops_m.group(1))
                    elif "Direct" in text or "Non-Stop" in text or "Nonstop" in text:
                        stops = 0

                    # Airline from img alt or text
                    airline = "Travel Trolley"

                    dep_dt = _parse_dt(f"{date_str}T{dep_time}:00")
                    arr_dt = _parse_dt(f"{date_str}T{arr_time}:00")

                    segments = [FlightSegment(
                        airline=airline, flight_no="",
                        origin=req.origin, destination=req.destination,
                        departure=dep_dt, arrival=arr_dt, duration_seconds=dur_sec,
                    )]
                    route = FlightRoute(segments=segments, total_duration_seconds=dur_sec, stopovers=stops)
                    oid = hashlib.md5(f"tt_{i}_{price}_{dep_time}".encode()).hexdigest()[:12]

                    offers.append(FlightOffer(
                        id=f"tt_{oid}", price=round(price, 2), currency="GBP",
                        price_formatted=f"£{price:.2f}",
                        outbound=route, inbound=None,
                        airlines=[airline], owner_airline=airline,
                        booking_url=results_url,
                        is_locked=False, source="traveltrolley_ota", source_tier="free",
                    ))
                except Exception:
                    continue
        except Exception:
            pass
        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"tt{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="GBP", offers=[], total_results=0,
        )
