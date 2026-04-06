"""
Flightcatchers connector — CDP Chrome + vibe.travel white-label.

Flightcatchers (flightcatchers.com) is a UK-based OTA powered by the
vibe.travel white-label flight search platform. Provides consolidated
fares for UK→Asia/Africa/Middle East routes.

Strategy (CDP Chrome + form fill + DOM scraping):
1.  Launch real Chrome via --remote-debugging-port.
2.  Navigate to flightcatchers.com search page.
3.  Fill the search form and submit.
4.  Intercept vibe.travel API responses or scrape result cards.
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

_BASE = "https://www.flightcatchers.com"
_CDP_PORT = 9496
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".flightcatchers_chrome_data"
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
        logger.info("Flightcatchers: Chrome on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


class FlightcatchersConnectorClient:
    """Flightcatchers — UK OTA (vibe.travel), CDP Chrome + form fill."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")

        context = await _get_context()
        page = await context.new_page()

        api_data: list[dict] = []

        async def _on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            if response.status == 200 and "json" in ct:
                if any(k in url.lower() for k in ["vibe.travel", "/search", "/flight", "/result", "/offer", "/price"]):
                    try:
                        body = await response.text()
                        if len(body) > 200:
                            data = json.loads(body)
                            api_data.append(data)
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            logger.info("Flightcatchers: searching %s→%s on %s", req.origin, req.destination, date_str)

            # Navigate to homepage and fill form (deep-link doesn't exist)
            await page.goto(_BASE, wait_until="domcontentloaded", timeout=25000)
            await asyncio.sleep(3)

            # Dismiss cookie banner
            for sel in ['#onetrust-accept-btn-handler', 'button:has-text("Accept")',
                       'button:has-text("I Accept")', '.cookie-accept']:
                try:
                    btn = page.locator(sel)
                    if await btn.count() > 0:
                        await btn.first.click(timeout=2000)
                        await asyncio.sleep(0.5)
                        break
                except Exception:
                    continue

            await self._fill_form(page, req)

            # Wait for results
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining

            offers: list[FlightOffer] = []
            while time.monotonic() < deadline:
                await asyncio.sleep(2)

                # Check for API data
                if api_data:
                    for data in api_data:
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
                    if "no result" in text.lower() or "no flight" in text.lower():
                        break
                except Exception:
                    pass

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0

            # Set booking URL to the actual results page
            results_url = page.url
            for offer in offers:
                offer.booking_url = results_url

            logger.info("Flightcatchers %s->%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"fc{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_fc_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "GBP",
                offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("Flightcatchers error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_form(self, page, req: FlightSearchRequest):
        """Fill the vibe.travel white-label search form with correct selectors."""
        # One-way toggle — the radio is hidden (styled via CSS), use JS to check it
        try:
            await page.evaluate("""() => {
                const radio = document.querySelector('#searchbox_sb3_cc_flightonly_sb3_flight_datereturnformat_oneway');
                if (radio) { radio.checked = true; radio.dispatchEvent(new Event('change', {bubbles: true})); }
            }""")
            await asyncio.sleep(0.5)
            # Also click the label if it exists
            label = page.locator('label[for="searchbox_sb3_cc_flightonly_sb3_flight_datereturnformat_oneway"]')
            if await label.count() > 0:
                await label.first.click(force=True, timeout=2000)
                await asyncio.sleep(0.3)
        except Exception:
            pass

        # Origin — iataFrom input with autocomplete
        try:
            origin_input = page.locator('#searchbox_sb3_cc_flightonly_sb3_flight_iataFrom')
            if await origin_input.count() > 0:
                await origin_input.click(timeout=3000)
                await origin_input.fill("")
                await origin_input.type(req.origin, delay=80)
                await asyncio.sleep(2)
                # vibe.travel uses jQuery UI autocomplete
                sug = page.locator('.ui-autocomplete li, .ui-menu-item, .autocomplete-suggestion').first
                if await sug.count() > 0:
                    await sug.click(timeout=3000)
                else:
                    await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
        except Exception:
            pass

        # Destination — iataTo input
        try:
            dest_input = page.locator('#searchbox_sb3_cc_flightonly_sb3_flight_iataTo')
            if await dest_input.count() > 0:
                await dest_input.click(timeout=3000)
                await dest_input.fill("")
                await dest_input.type(req.destination, delay=80)
                await asyncio.sleep(2)
                sug = page.locator('.ui-autocomplete li, .ui-menu-item, .autocomplete-suggestion').first
                if await sug.count() > 0:
                    await sug.click(timeout=3000)
                else:
                    await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
        except Exception:
            pass

        # Date — set via JS on both visible and hidden inputs
        try:
            formatted_visible = req.date_from.strftime("%d/%m/%Y")
            formatted_iso = req.date_from.strftime("%Y-%m-%d")
            await page.evaluate(f"""() => {{
                const hidden = document.querySelector('#searchbox_sb3_cc_flightonly_sb3_flight_outboundDate_dateinput');
                if (hidden) {{ hidden.value = '{formatted_iso}'; hidden.dispatchEvent(new Event('change', {{bubbles: true}})); }}
                const visible = document.querySelector('#searchbox_sb3_cc_flightonly_sb3_flight_outboundDate');
                if (visible) {{ visible.value = '{formatted_visible}'; }}
            }}""")
            await asyncio.sleep(0.3)
        except Exception:
            pass

        # Submit — try multiple selectors for the search button
        try:
            submit = page.locator(
                'form button[type="submit"], .sb3_submit, .btn-search, '
                'button:has-text("Search"), input[type="submit"]'
            )
            if await submit.count() > 0:
                await submit.first.click(timeout=5000)
            else:
                await page.evaluate("() => { const f = document.querySelector('form'); if (f) f.submit(); }")
        except Exception:
            pass

    def _parse_api(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse vibe.travel or other API response."""
        offers: list[FlightOffer] = []
        items = data.get("offers") or data.get("flights") or data.get("results") or []
        if isinstance(items, list):
            for item in items[:50]:
                try:
                    price = float(item.get("price") or item.get("totalPrice") or 0)
                    if price <= 0:
                        continue
                    currency = item.get("currency", "GBP")
                    airline = item.get("airline") or item.get("carrier") or "Flightcatchers"

                    segments = []
                    segs = item.get("segments") or item.get("flights") or [item]
                    for seg in (segs if isinstance(segs, list) else []):
                        segments.append(FlightSegment(
                            airline=str(airline),
                            flight_no=seg.get("flightNumber") or "",
                            origin=seg.get("origin") or req.origin,
                            destination=seg.get("destination") or req.destination,
                            departure=_parse_dt(seg.get("departure") or ""),
                            arrival=_parse_dt(seg.get("arrival") or ""),
                            duration_seconds=int(seg.get("duration", 0)) * 60,
                        ))

                    if not segments:
                        segments = [FlightSegment(
                            airline=str(airline), flight_no="",
                            origin=req.origin, destination=req.destination,
                            departure=datetime(2000, 1, 1), arrival=datetime(2000, 1, 1),
                            duration_seconds=0,
                        )]

                    route = FlightRoute(segments=segments, total_duration_seconds=sum(s.duration_seconds for s in segments), stopovers=max(0, len(segments) - 1))
                    oid = hashlib.md5(f"fc_{req.origin}{req.destination}{price}".encode()).hexdigest()[:12]
                    offers.append(FlightOffer(
                        id=f"fc_{oid}", price=round(price, 2), currency=currency,
                        price_formatted=f"{price:.2f} {currency}",
                        outbound=route, inbound=None,
                        airlines=[str(airline)], owner_airline=str(airline),
                        booking_url=f"{_BASE}/search.php?from={req.origin}&to={req.destination}&depart={req.date_from.strftime('%Y-%m-%d')}",
                        is_locked=False, source="flightcatchers_ota", source_tier="free",
                    ))
                except Exception:
                    continue
        return offers

    async def _scrape_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Scrape results from DOM (vibe.travel bf_ result cards)."""
        offers: list[FlightOffer] = []
        try:
            # vibe.travel uses .bf_rsitem or bf_frslt for result cards inside section.bf_results
            cards = page.locator('.bf_frslt')
            count = await cards.count()
            if count == 0:
                cards = page.locator('.bf_rsitem')
                count = await cards.count()
            if count == 0:
                return offers

            results_url = page.url  # booking URL = results page where user clicks Continue
            for i in range(min(count, 50)):
                try:
                    card = cards.nth(i)
                    text = await card.inner_text()

                    # Extract discounted price first, then regular price
                    prices = re.findall(r'[\xA3\u00A3$]\s*([\d,.]+)', text)
                    if not prices:
                        prices = re.findall(r'([\d,.]+)\s*(?:per|pp)', text.lower())
                    if not prices:
                        continue

                    # Use last price (discounted) if multiple, else first
                    price_str = prices[-1].replace(",", "") if len(prices) > 1 else prices[0].replace(",", "")
                    price = float(price_str)
                    if price <= 0 or price > 50000:
                        continue

                    currency = "GBP"

                    # Airline from logo alt text
                    airline = "Unknown"
                    try:
                        img = card.locator('img[alt]').first
                        if await img.count() > 0:
                            airline = await img.get_attribute("alt") or "Unknown"
                    except Exception:
                        pass

                    # Flight number
                    flight_no = ""
                    fn_match = re.search(r'Flight\s+([A-Z]{2}\d+)', text)
                    if fn_match:
                        flight_no = fn_match.group(1)

                    # Times: HH:MM patterns
                    times = re.findall(r'(\d{1,2}:\d{2})', text)
                    dep_time = times[0] if times else "00:00"
                    arr_time = times[1] if len(times) > 1 else dep_time

                    # Duration
                    dur_secs = 0
                    dur_m = re.search(r'(\d+)\s*hrs?\s*(\d+)\s*min', text, re.I)
                    if dur_m:
                        dur_secs = int(dur_m.group(1)) * 3600 + int(dur_m.group(2)) * 60

                    # Stops
                    stops = 0
                    if "direct" in text.lower():
                        stops = 0
                    elif "1 connection" in text.lower() or "1 stop" in text.lower():
                        stops = 1
                    elif "2 connection" in text.lower() or "2 stop" in text.lower():
                        stops = 2

                    dep_dt = _parse_dt(f"{date_str}T{dep_time}:00")
                    arr_dt = _parse_dt(f"{date_str}T{arr_time}:00")

                    segments = [FlightSegment(
                        airline=airline, flight_no=flight_no,
                        origin=req.origin, destination=req.destination,
                        departure=dep_dt, arrival=arr_dt, duration_seconds=dur_secs,
                    )]

                    route = FlightRoute(segments=segments, total_duration_seconds=dur_secs, stopovers=stops)
                    oid = hashlib.md5(f"fc_{i}_{price}_{flight_no}_{dep_time}".encode()).hexdigest()[:12]

                    offers.append(FlightOffer(
                        id=f"fc_{oid}", price=round(price, 2), currency=currency,
                        price_formatted=f"{currency} {price:.2f}",
                        outbound=route, inbound=None,
                        airlines=[airline], owner_airline=airline,
                        booking_url=results_url,
                        is_locked=False, source="flightcatchers_ota", source_tier="free",
                    ))
                except Exception:
                    continue
        except Exception:
            pass
        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"fc{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="GBP", offers=[], total_results=0,
        )
