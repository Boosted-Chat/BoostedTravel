"""
Travelgenio connector — CDP Chrome form fill + API interception.

Travelgenio (travelgenio.com) is a Spanish OTA with global coverage,
also operating under the Travel2be brand. Part of eTraveli group (Booking Holdings).

**STATUS (2026-03): PERMANENTLY BROKEN / DECOMMISSIONED**
All backend APIs return HTTP 404:
- FindAirportCity (autocomplete) -> 404
- FindMultiCities (Search() pre-flight) -> 404
- Air/Get/en-GB (English locale) -> 404
- Direct search URLs (/vuelos/, /Air/Search, /Air/Results) -> 404
- Form POST to /Air/Get/es-ES/201 returns the homepage (ignores input)

The homepage still renders statically but no search functionality works.
The airline dropdown lists defunct carriers (Air Berlin, Malev, Kingfisher),
suggesting the site hasn't been maintained in years.

This connector is REMOVED from engine.py and airline_routes.py.
Keep the file for reference in case the backend is restored.

Strategy (CDP Chrome + form fill):
1.  Launch real Chrome via --remote-debugging-port.
2.  Navigate to travelgenio.com, fill the search form via JS injection.
3.  Intercept XHR responses or scrape the results page.
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

_BASE = "https://www.travelgenio.com"
_CDP_PORT = 9495
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".travelgenio_chrome_data"
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
        logger.info("Travelgenio: Chrome launched on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


class TravelgenioConnectorClient:
    """Travelgenio — Spanish OTA, CDP Chrome + form fill."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")

        context = await _get_context()
        page = await context.new_page()

        flight_data: list[dict] = []

        async def _on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            if response.status == 200 and ("json" in ct or "xml" in ct):
                if any(k in url.lower() for k in ["/air/", "/flight", "/search", "/result", "/get/"]):
                    try:
                        body = await response.text()
                        if len(body) > 200:
                            try:
                                data = json.loads(body)
                                flight_data.append(data)
                            except json.JSONDecodeError:
                                pass
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            logger.info("Travelgenio: searching %s→%s on %s", req.origin, req.destination, date_str)
            await page.goto(_BASE, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            # Dismiss cookies
            for label in ["Aceptar", "Accept", "Accept All", "OK", "Acepto"]:
                try:
                    btn = page.get_by_role("button", name=label)
                    if await btn.count() > 0:
                        await btn.first.click(timeout=2000)
                        await asyncio.sleep(0.5)
                        break
                except Exception:
                    continue

            # Set one-way trip
            try:
                oneway = page.locator('#OneWay')
                if await oneway.count() > 0:
                    await oneway.first.click(timeout=3000)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

            # Fill form via JS injection — bypasses broken autocomplete API
            day_str = str(req.date_from.day).zfill(2)
            month_year = req.date_from.strftime("%m-%Y")  # e.g. "06-2026"
            await page.evaluate(f"""() => {{
                const orig = document.querySelector('#Orig');
                const dest = document.querySelector('#Dest');
                if (orig) orig.value = '{req.origin} ({req.origin})';
                if (dest) dest.value = '{req.destination} ({req.destination})';

                const oneWay = document.querySelector('#OneWay');
                if (oneWay) {{ oneWay.checked = true; oneWay.dispatchEvent(new Event('change', {{bubbles: true}})); }}

                const daySelect = document.querySelector('#OutboundDate');
                if (daySelect) {{ daySelect.value = '{day_str}'; daySelect.dispatchEvent(new Event('change', {{bubbles: true}})); }}

                const monthSelect = document.querySelector('#OutboundMonthYear');
                if (monthSelect) {{ monthSelect.value = '{month_year}'; monthSelect.dispatchEvent(new Event('change', {{bubbles: true}})); }}
            }}""")
            await asyncio.sleep(0.5)

            # Also try typing in the origin/dest (for autocomplete if alive)
            try:
                origin_input = page.locator('#Orig')
                if await origin_input.count() > 0:
                    await origin_input.first.click()
                    await origin_input.first.fill("")
                    await origin_input.first.type(req.origin, delay=100)
                    await asyncio.sleep(1.5)
                    suggestion = page.locator('.ui-menu-item, .ui-autocomplete li').first
                    if await suggestion.count() > 0:
                        await suggestion.click(timeout=3000)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

            try:
                dest_input = page.locator('#Dest')
                if await dest_input.count() > 0:
                    await dest_input.first.click()
                    await dest_input.first.fill("")
                    await dest_input.first.type(req.destination, delay=100)
                    await asyncio.sleep(1.5)
                    suggestion = page.locator('.ui-menu-item, .ui-autocomplete li').first
                    if await suggestion.count() > 0:
                        await suggestion.click(timeout=3000)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

            # Submit search — try button click first, then JS form submit
            try:
                search_btn = page.locator('#searchButton')
                if await search_btn.count() > 0:
                    await search_btn.first.click(timeout=5000)
                    await asyncio.sleep(2)
            except Exception:
                pass
            # If button click didn't navigate, try direct form submit
            if page.url == _BASE or page.url == f"{_BASE}/":
                try:
                    await page.evaluate("() => { const f = document.querySelector('#searchButton')?.closest('form'); if (f) f.submit(); }")
                except Exception:
                    pass

            # Wait for results
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining

            offers: list[FlightOffer] = []
            while time.monotonic() < deadline:
                await asyncio.sleep(2)

                # Check for API data
                if flight_data:
                    for data in flight_data:
                        parsed = self._parse_api(data, req, date_str)
                        offers.extend(parsed)
                    if offers:
                        break

                # Fallback: DOM scraping
                try:
                    dom_offers = await self._scrape_dom(page, req, date_str)
                    if dom_offers:
                        offers = dom_offers
                        break
                except Exception:
                    pass

                # Check for no results
                try:
                    text = await page.evaluate("() => document.body?.innerText?.substring(0, 500) || ''")
                    if any(x in text.lower() for x in ["no result", "no flight", "sin resultado", "no hay vuelos"]):
                        break
                except Exception:
                    pass

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0

            # Set booking URL to the actual results page
            results_url = page.url
            for offer in offers:
                offer.booking_url = results_url

            logger.info("Travelgenio %s->%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"tg{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_tg_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "EUR",
                offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("Travelgenio error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    def _parse_api(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse JSON API response if intercepted."""
        offers: list[FlightOffer] = []
        # Generic JSON parser — adapt to actual response structure
        items = data.get("flights") or data.get("results") or data.get("itineraries") or []
        if isinstance(items, list):
            for item in items[:50]:
                try:
                    price = float(item.get("price") or item.get("totalPrice") or item.get("amount") or 0)
                    if price <= 0:
                        continue
                    currency = item.get("currency", "EUR")
                    airline = item.get("airline") or item.get("carrier") or item.get("operatingCarrier") or "Travelgenio"

                    segments = []
                    segs = item.get("segments") or item.get("legs") or [item]
                    for seg in segs:
                        dep_str = seg.get("departure") or seg.get("departureTime") or ""
                        arr_str = seg.get("arrival") or seg.get("arrivalTime") or ""
                        segments.append(FlightSegment(
                            airline=str(airline),
                            flight_no=seg.get("flightNumber") or seg.get("flight_no") or "",
                            origin=seg.get("origin") or req.origin,
                            destination=seg.get("destination") or req.destination,
                            departure=_parse_dt(dep_str),
                            arrival=_parse_dt(arr_str),
                            duration_seconds=int(seg.get("duration") or seg.get("durationMinutes", 0)) * 60,
                        ))

                    if not segments:
                        continue

                    route = FlightRoute(
                        segments=segments,
                        total_duration_seconds=sum(s.duration_seconds for s in segments),
                        stopovers=max(0, len(segments) - 1),
                    )

                    oid = hashlib.md5(f"tg_{req.origin}{req.destination}{date_str}{price}".encode()).hexdigest()[:12]
                    offers.append(FlightOffer(
                        id=f"tg_{oid}", price=round(price, 2), currency=currency,
                        price_formatted=f"{price:.2f} {currency}",
                        outbound=route, inbound=None,
                        airlines=[str(airline)],
                        owner_airline=str(airline),
                        booking_url=f"{_BASE}/Air/Get/en-GB/201/{req.origin}/{req.destination}/{date_str}",
                        is_locked=False, source="travelgenio_ota", source_tier="free",
                    ))
                except Exception as e:
                    logger.debug("Travelgenio parse error: %s", e)
        return offers

    async def _scrape_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Fallback: scrape results from the DOM."""
        offers: list[FlightOffer] = []
        try:
            cards = page.locator('.flight-result, .result-card, .flight-item, .itinerary, tr.flight')
            count = await cards.count()
            if count == 0:
                return offers

            for i in range(min(count, 50)):
                try:
                    card = cards.nth(i)
                    text = await card.inner_text()

                    # Extract price
                    price_m = re.search(r'[€£$]\s*([\d,.]+)', text)
                    if not price_m:
                        price_m = re.search(r'([\d,.]+)\s*(?:EUR|GBP|USD)', text)
                    if not price_m:
                        continue

                    price_str = price_m.group(1).replace(",", ".")
                    if price_str.count(".") > 1:
                        price_str = price_str.replace(".", "", price_str.count(".") - 1)
                    price = float(price_str)
                    if price <= 0 or price > 50000:
                        continue

                    # Determine currency
                    currency = "EUR"
                    if "£" in text:
                        currency = "GBP"
                    elif "$" in text:
                        currency = "USD"

                    # Extract times
                    times = re.findall(r'(\d{1,2}:\d{2})', text)
                    dep_time = times[0] if times else "00:00"
                    arr_time = times[1] if len(times) > 1 else "00:00"

                    # Extract airline
                    airline = "Travelgenio"
                    airline_patterns = re.findall(r'(?:operated by|airline[:\s]+)([A-Za-z\s]+)', text, re.I)
                    if airline_patterns:
                        airline = airline_patterns[0].strip()

                    dep_dt = _parse_dt(f"{date_str}T{dep_time}:00")
                    arr_dt = _parse_dt(f"{date_str}T{arr_time}:00")

                    segments = [FlightSegment(
                        airline=airline, flight_no="",
                        origin=req.origin, destination=req.destination,
                        departure=dep_dt, arrival=arr_dt,
                        duration_seconds=0,
                    )]

                    route = FlightRoute(segments=segments, total_duration_seconds=0, stopovers=0)
                    oid = hashlib.md5(f"tg_{i}_{price}_{dep_time}".encode()).hexdigest()[:12]

                    offers.append(FlightOffer(
                        id=f"tg_{oid}", price=round(price, 2), currency=currency,
                        price_formatted=f"{price:.2f} {currency}",
                        outbound=route, inbound=None,
                        airlines=[airline], owner_airline=airline,
                        booking_url=f"{_BASE}/Air/Get/en-GB/201/{req.origin}/{req.destination}/{date_str}",
                        is_locked=False, source="travelgenio_ota", source_tier="free",
                    ))
                except Exception:
                    continue
        except Exception:
            pass
        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"travelgenio{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="EUR", offers=[], total_results=0,
        )
