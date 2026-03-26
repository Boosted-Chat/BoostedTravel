"""
Booking.com Flights connector — CDP Chrome + DataDome bypass.

Booking.com is the largest OTA by revenue (Booking Holdings). Their flights
vertical is at booking.com/flights and is protected by DataDome anti-bot.
Requires real Chrome via CDP for browser fingerprint integrity.

The flights backend may share the Kayak/Booking Holdings poll API or use
a separate GraphQL schema.

Strategy (CDP Chrome + deep-link + API interception):
1.  Launch System Chrome via --remote-debugging-port (DataDome bypass).
2.  Navigate to booking.com/flights search results URL.
3.  Intercept XHR/fetch responses with flight data (poll API or GraphQL).
4.  Parse offers; fall back to DOM scraping if API not captured.
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

_CDP_PORT = 9499
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".bookingcom_chrome_data"
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
        logger.info("BookingCom: Chrome on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


class BookingcomConnectorClient:
    """Booking.com Flights — world's largest OTA, CDP Chrome + DataDome bypass."""

    def __init__(self, timeout: float = 65.0):
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
            if response.status != 200:
                return
            if "json" not in ct:
                return
            # Booking.com flight data endpoints
            if any(k in url.lower() for k in [
                "/flights/poll", "/flights/results", "/graphql",
                "/api/flights", "/api/search", "/flights/search",
                "/dml/graphql", "/flights-api/",
                "flightsearch", "searchresults",
            ]):
                try:
                    body = await response.text()
                    if len(body) > 1000:
                        data = json.loads(body)
                        api_data.append(data)
                except Exception:
                    pass

        page.on("response", _on_response)

        try:
            logger.info("BookingCom: searching %s→%s on %s", req.origin, req.destination, date_str)

            # Navigate to flights.booking.com (NOT www.booking.com which geo-redirects)
            await page.goto(
                "https://flights.booking.com/",
                wait_until="domcontentloaded", timeout=35000,
            )

            # Handle DataDome challenge / cookie consent
            await self._handle_challenges(page)

            # Fill the search form (deep links are overridden by geo-locate)
            await self._fill_form(page, req, date_str)

            # Wait for results (progressive loading like Kayak/Momondo)
            remaining = max(self.timeout - (time.monotonic() - t0), 20)
            deadline = time.monotonic() + remaining

            offers: list[FlightOffer] = []
            check_count = 0
            while time.monotonic() < deadline:
                await asyncio.sleep(3)
                check_count += 1

                # Check API data
                if api_data:
                    for data in api_data:
                        parsed = self._parse_api(data, req, date_str)
                        offers.extend(parsed)

                        # Also try Booking Holdings poll format
                        bh_offers = self._parse_bh_poll(data, req, date_str)
                        offers.extend(bh_offers)

                    if offers:
                        # Wait a bit more for progressive results
                        if check_count < 5:
                            continue
                        break

                # DOM scraping fallback
                try:
                    dom_offers = await self._scrape_dom(page, req, date_str)
                    if dom_offers:
                        offers = dom_offers
                        break
                except Exception:
                    pass

                # Check for no-results states
                try:
                    text = await page.evaluate("() => document.body?.innerText?.substring(0, 500) || ''")
                    if any(s in text.lower() for s in ["no results", "no flights", "try different", "sorry"]):
                        break
                except Exception:
                    pass

            # Deduplicate
            seen: set[str] = set()
            unique: list[FlightOffer] = []
            for o in offers:
                key = f"{o.price}_{o.owner_airline}_{o.outbound.total_duration_seconds}"
                if key not in seen:
                    seen.add(key)
                    unique.append(o)
            offers = unique

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("BookingCom %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"bc{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_bc_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "EUR",
                offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("BookingCom error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _handle_challenges(self, page):
        """Handle DataDome captcha and cookie consent popups."""
        await asyncio.sleep(2)

        # Cookie consent
        for sel in [
            '#onetrust-accept-btn-handler',
            'button[id*="accept"]',
            'button:has-text("Accept")',
            'button:has-text("OK")',
            '[data-testid="accept-btn"]',
        ]:
            try:
                btn = page.locator(sel)
                if await btn.count() > 0:
                    await btn.first.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                continue

        # DataDome interstitial — wait for it to resolve
        for _ in range(5):
            try:
                body = await page.evaluate("() => document.body?.innerText?.substring(0, 300) || ''")
                if "captcha" in body.lower() or "datadome" in body.lower() or "verify" in body.lower():
                    logger.warning("BookingCom: DataDome challenge detected, waiting...")
                    await asyncio.sleep(5)
                else:
                    break
            except Exception:
                await asyncio.sleep(2)

    async def _fill_form(self, page, req: FlightSearchRequest, date_str: str):
        """Fill the Booking.com flights search form."""
        await asyncio.sleep(1)

        # Select one-way
        try:
            ow = page.locator('#search_type_option_ONEWAY')
            if await ow.count() > 0:
                await ow.click(timeout=3000)
                await asyncio.sleep(0.5)
        except Exception:
            pass

        # Origin — the field is a <button>, click to reveal input
        try:
            from_btn = page.locator('[data-ui-name="input_location_from_segment_0"]')
            if await from_btn.count() > 0:
                await from_btn.first.click(timeout=3000)
                await asyncio.sleep(1)
                # After clicking, an input field appears — find and fill it
                active_input = page.locator(
                    '[data-ui-name="input_text_autocomplete"], '
                    'input[placeholder*="airport"], input[placeholder*="city"], '
                    'input[role="searchbox"], input[type="text"]:visible'
                ).first
                if await active_input.count() > 0:
                    await active_input.fill(req.origin)
                    await asyncio.sleep(2)
                    sug = page.locator('[role="option"], [data-ui-name*="autocomplete"] li').first
                    if await sug.count() > 0:
                        await sug.click(timeout=3000)
                        logger.debug("BookingCom: origin selected")
                    else:
                        await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("BookingCom: origin fill failed: %s", e)

        # Destination
        try:
            to_btn = page.locator('[data-ui-name="input_location_to_segment_0"]')
            if await to_btn.count() > 0:
                await to_btn.first.click(timeout=3000)
                await asyncio.sleep(1)
                active_input = page.locator(
                    '[data-ui-name="input_text_autocomplete"], '
                    'input[placeholder*="airport"], input[placeholder*="city"], '
                    'input[role="searchbox"], input[type="text"]:visible'
                ).first
                if await active_input.count() > 0:
                    await active_input.fill(req.destination)
                    await asyncio.sleep(2)
                    sug = page.locator('[role="option"], [data-ui-name*="autocomplete"] li').first
                    if await sug.count() > 0:
                        await sug.click(timeout=3000)
                        logger.debug("BookingCom: destination selected")
                    else:
                        await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("BookingCom: dest fill failed: %s", e)

        # Date — click date button, navigate calendar, select day
        try:
            date_btn = page.locator('[data-ui-name="button_date_segment_0"]')
            if await date_btn.count() > 0:
                await date_btn.first.click(timeout=3000)
                await asyncio.sleep(1)
                # Navigate to target month
                target_month = req.date_from.strftime("%B %Y")
                for _ in range(12):
                    try:
                        header = page.locator('[class*="Calendar-module__monthName"], [data-ui-name*="calendar_month"]')
                        if await header.count() > 0:
                            text = await header.first.inner_text()
                            if target_month.lower() in text.lower():
                                break
                    except Exception:
                        pass
                    try:
                        nxt = page.locator('[data-ui-name="calendar_body_navigation_next"], [aria-label="Next month"], button:has-text("›")')
                        if await nxt.count() > 0:
                            await nxt.first.click(timeout=2000)
                            await asyncio.sleep(0.3)
                    except Exception:
                        break

                # Click the target day
                day_num = str(req.date_from.day)
                day_btn = page.locator(f'[data-date="{date_str}"], [aria-label*="{day_num}"]')
                if await day_btn.count() > 0:
                    await day_btn.first.click(timeout=3000)
                else:
                    # Fallback: find by text
                    day_cell = page.locator(f'td:has-text("{day_num}"), button:has-text("{day_num}")')
                    if await day_cell.count() > 0:
                        for idx in range(await day_cell.count()):
                            el = day_cell.nth(idx)
                            txt = (await el.inner_text()).strip()
                            if txt == day_num:
                                await el.click(timeout=2000)
                                break
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("BookingCom: date selection failed: %s", e)

        # Search button
        try:
            search_btn = page.locator(
                '[data-ui-name="button_search_submit"], '
                'button:has-text("Search"), button:has-text("Search flights")'
            )
            if await search_btn.count() > 0:
                await search_btn.first.click(timeout=5000)
                logger.debug("BookingCom: search submitted")
        except Exception as e:
            logger.debug("BookingCom: search button failed: %s", e)

    def _parse_api(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse Booking.com /api/flights response (flightOffers array)."""
        offers: list[FlightOffer] = []

        items = data.get("flightOffers")
        if not isinstance(items, list):
            # Try GraphQL format
            gql_data = data.get("data", {})
            for key in ["flightOffers", "flights", "searchResults", "results"]:
                if key in gql_data and isinstance(gql_data[key], list):
                    items = gql_data[key]
                    break
        if not isinstance(items, list):
            return offers

        for item in items[:50]:
            try:
                # Price: priceBreakdown.total = {currencyCode, units, nanos}
                pb = item.get("priceBreakdown", {})
                total = pb.get("total", {})
                units = total.get("units", 0)
                nanos = total.get("nanos", 0)
                price = float(units) + float(nanos) / 1_000_000_000
                if price <= 0:
                    continue
                currency = total.get("currencyCode", req.currency or "EUR")

                # Segments
                segments_data = item.get("segments", [])
                token = item.get("token", "")

                all_segments: list[FlightSegment] = []
                total_dur = 0
                total_stops = 0
                airlines_set: set[str] = set()

                for seg_data in segments_data:
                    seg_dur = int(seg_data.get("totalTime", 0))
                    total_dur += seg_dur
                    legs = seg_data.get("legs", [])
                    total_stops += max(0, len(legs) - 1)

                    dep_airport = seg_data.get("departureAirport", {})
                    arr_airport = seg_data.get("arrivalAirport", {})

                    for leg in legs:
                        carrier_data = (leg.get("carriersData") or [{}])[0] if leg.get("carriersData") else {}
                        airline_name = carrier_data.get("name", "Booking.com")
                        carrier_code = carrier_data.get("code", "")
                        airlines_set.add(airline_name)

                        fi = leg.get("flightInfo", {})
                        ci = fi.get("carrierInfo", {})
                        fn = fi.get("flightNumber", "")
                        mkt_carrier = ci.get("marketingCarrier", carrier_code)
                        flight_no = f"{mkt_carrier}{fn}" if fn else ""

                        leg_dep = leg.get("departureAirport", dep_airport)
                        leg_arr = leg.get("arrivalAirport", arr_airport)

                        all_segments.append(FlightSegment(
                            airline=airline_name,
                            flight_no=flight_no,
                            origin=leg_dep.get("code", req.origin),
                            destination=leg_arr.get("code", req.destination),
                            departure=_parse_dt(leg.get("departureTime", "")),
                            arrival=_parse_dt(leg.get("arrivalTime", "")),
                            duration_seconds=int(leg.get("totalTime", 0)),
                        ))

                if not all_segments:
                    continue

                airlines = sorted(airlines_set) if airlines_set else ["Booking.com"]
                route = FlightRoute(
                    segments=all_segments,
                    total_duration_seconds=total_dur,
                    stopovers=total_stops,
                )

                # Booking URL using token
                booking_url = f"https://flights.booking.com/checkout/pax?token={token}" if token else f"https://flights.booking.com/"

                oid = hashlib.md5(f"bc_{token[:20] if token else ''}_{price}".encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"bc_{oid}", price=round(price, 2), currency=str(currency),
                    price_formatted=f"{currency} {price:.2f}",
                    outbound=route, inbound=None,
                    airlines=airlines, owner_airline=airlines[0],
                    booking_url=booking_url,
                    is_locked=False, source="bookingcom_ota", source_tier="free",
                ))
            except Exception:
                continue
        return offers

    def _parse_bh_poll(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Try to parse as Booking Holdings poll format (like Kayak/Momondo)."""
        if not (data.get("results") and data.get("legs")):
            return []

        try:
            from .momondo import _parse_booking_holdings_poll
            return _parse_booking_holdings_poll(
                [data], req,
                source="bookingcom_ota",
                id_prefix="bc",
                booking_base_url=f"https://www.booking.com/flights",
            )
        except Exception:
            return []

    async def _scrape_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """DOM scraping fallback for Booking.com flights."""
        offers: list[FlightOffer] = []
        try:
            cards = page.locator(
                '[data-testid*="flight"], [data-testid*="result"], '
                '.flight-card, .result-card, [class*="FlightCard"], '
                '[class*="SearchResult"], [class*="flight-result"]'
            )
            count = await cards.count()
            if count == 0:
                return offers

            for i in range(min(count, 50)):
                try:
                    card = cards.nth(i)
                    text = await card.inner_text()

                    # Price in various currencies
                    price_m = re.search(r'(?:€|£|\$|US\$|EUR|GBP|USD)\s*([\d,.]+)', text)
                    if not price_m:
                        price_m = re.search(r'([\d,.]+)\s*(?:€|£|\$|EUR|GBP|USD)', text)
                    if not price_m:
                        continue

                    price = float(price_m.group(1).replace(",", ""))
                    if price <= 0 or price > 50000:
                        continue

                    # Currency detection
                    currency = "EUR"
                    if "£" in text or "GBP" in text:
                        currency = "GBP"
                    elif "$" in text or "USD" in text:
                        currency = "USD"

                    times = re.findall(r'(\d{1,2}:\d{2})', text)
                    dep_time = times[0] if times else "00:00"
                    arr_time = times[1] if len(times) > 1 else "00:00"

                    airline = "Booking.com"
                    for known in ["Ryanair", "easyJet", "British Airways", "Lufthansa", "Air France",
                                  "KLM", "Wizz Air", "Vueling", "Norwegian", "SAS", "Finnair",
                                  "TAP", "Iberia", "Turkish Airlines", "Emirates", "Qatar"]:
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

                    # Stops
                    stops = 0
                    if "1 stop" in text.lower():
                        stops = 1
                    elif "2 stop" in text.lower():
                        stops = 2
                    elif "direct" in text.lower() or "nonstop" in text.lower():
                        stops = 0

                    segments = [FlightSegment(
                        airline=airline, flight_no="",
                        origin=req.origin, destination=req.destination,
                        departure=dep_dt, arrival=arr_dt, duration_seconds=max(dur, 0),
                    )]
                    route = FlightRoute(segments=segments, total_duration_seconds=max(dur, 0), stopovers=stops)
                    oid = hashlib.md5(f"bc_{i}_{price}_{airline}".encode()).hexdigest()[:12]

                    offers.append(FlightOffer(
                        id=f"bc_{oid}", price=round(price, 2), currency=currency,
                        price_formatted=f"{currency} {price:.2f}",
                        outbound=route, inbound=None,
                        airlines=[airline], owner_airline=airline,
                        booking_url=f"https://www.booking.com/flights/{req.origin}-{req.destination}/",
                        is_locked=False, source="bookingcom_ota", source_tier="free",
                    ))
                except Exception:
                    continue
        except Exception:
            pass
        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"bc{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="EUR", offers=[], total_results=0,
        )
