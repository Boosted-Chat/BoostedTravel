"""
Yatra connector — India's OTA (CDP Chrome + deep-link + API interception).

Yatra.com is one of India's largest OTAs. Strong on domestic Indian routes
and India-international connections. Has competitive fares from consolidator
arrangements with Indian carriers.

Strategy (CDP Chrome + deep-link + API interception):
1. Launch real Chrome via --remote-debugging-port (bypasses PerimeterX).
2. Navigate directly to Yatra search results URL (deep-link).
3. Intercept flight.yatra.com/air-search-ui API responses.
4. Also try extracting window.Search_.Populator.resData global.
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
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args

logger = logging.getLogger(__name__)

_BASE = "https://www.yatra.com"
_FLIGHT_API = "https://flight.yatra.com"
_CDP_PORT = 9469
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".yatra_chrome_data"
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
        pass
    try:
        if "/" in s:
            return datetime.strptime(s[:16], "%d/%m/%Y %H:%M")
        if s.isdigit() and len(s) >= 10:
            return datetime.fromtimestamp(int(s[:10]))
    except (ValueError, TypeError):
        pass
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
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
        logger.info("Yatra: Chrome launched on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


async def _dismiss_cookies(page) -> None:
    for label in ["Accept", "OK", "Got it", "I agree", "Accept All", "Accept all"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue


class YatraConnectorClient:
    """Yatra — India's OTA, CDP Chrome + deep-link + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")
        date_yatra = req.date_from.strftime("%d/%m/%Y")

        context = await _get_context()
        page = await context.new_page()

        captured: list[dict] = []

        async def _on_response(response):
            url = response.url.lower()
            if response.status == 200 and any(kw in url for kw in ("air-search", "flight", "fbdom", "fbint", "result")):
                ct = response.headers.get("content-type", "")
                if "json" in ct or "javascript" in ct:
                    try:
                        data = await response.json()
                        if isinstance(data, (dict, list)):
                            captured.append(data if isinstance(data, dict) else {"results": data})
                    except Exception:
                        try:
                            text = await response.text()
                            jsonp_match = re.search(r'^\w+\((.*)\);?$', text.strip(), re.S)
                            if jsonp_match:
                                data = json.loads(jsonp_match.group(1))
                                captured.append(data if isinstance(data, dict) else {"results": data})
                        except Exception:
                            pass

        page.on("response", _on_response)

        try:
            # Deep-link to Yatra search results
            search_url = f"{_BASE}/flights/search?type=O&ADT={req.adults or 1}&CNN=0&INF=0&origin={req.origin}&destination={req.destination}&flight_depart_date={date_yatra}&class=Economy"
            logger.info("Yatra: navigating to %s", search_url)
            await page.goto(search_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            await asyncio.sleep(3.0)

            # Check for GDPR block (Yatra blocks EU traffic)
            if "/gdpr" in page.url.lower() or "temporarily unavailable" in (await page.title()).lower():
                logger.warning("Yatra: GDPR block — site unavailable from this region")
                return self._empty(req)

            await _dismiss_cookies(page)

            # Polling loop: wait for API or window data
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            offers: list[FlightOffer] = []

            while time.monotonic() < deadline and not offers:
                # Check captured API responses (largest first)
                for data in sorted(captured, key=lambda d: len(str(d)), reverse=True):
                    offers = self._parse(data, req, date_str)
                    if offers:
                        break

                # Try window globals
                if not offers:
                    try:
                        page_data = await page.evaluate("""() => {
                            if (window.Search_ && Search_.Populator && Search_.Populator.resData)
                                return JSON.stringify(Search_.Populator.resData);
                            if (window.__INITIAL_DATA__) return JSON.stringify(window.__INITIAL_DATA__);
                            if (window.flightSearchResult) return JSON.stringify(window.flightSearchResult);
                            if (window.__NEXT_DATA__) return JSON.stringify(window.__NEXT_DATA__);
                            return null;
                        }""")
                        if page_data:
                            data = json.loads(page_data)
                            if isinstance(data, dict):
                                props = data.get("props", {}).get("pageProps", data) if "props" in data else data
                                offers = self._parse(props, req, date_str)
                    except Exception:
                        pass

                if not offers:
                    await asyncio.sleep(1.5)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("Yatra %s→%s: %d offers in %.1fs (CDP Chrome)", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"yatra{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "INR",
                offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("Yatra CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    def _parse(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse Yatra flight results."""
        offers: list[FlightOffer] = []

        results = (
            data.get("flightList") or data.get("flights") or data.get("itineraries")
            or data.get("result") or data.get("results")
            or data.get("OB") or data.get("outbound")
            or (data.get("data", {}).get("flightList") if isinstance(data.get("data"), dict) else None)
            or []
        )

        if isinstance(results, dict):
            results = results.get("items") or results.get("list") or list(results.values())

        for item in (results if isinstance(results, list) else [])[:30]:
            try:
                price = 0.0
                for pk in ("totalFare", "fare", "price", "totalPrice", "amt", "amount", "netFare"):
                    v = item.get(pk)
                    if v and float(v) > 0:
                        price = float(v)
                        break
                if isinstance(item.get("pricingSummary"), dict):
                    price = float(item["pricingSummary"].get("totalFare") or item["pricingSummary"].get("total") or 0) or price

                currency = item.get("currency") or item.get("currCode") or "INR"
                if price <= 0:
                    continue

                seg_data = item.get("flightSegments") or item.get("segments") or item.get("legs") or item.get("flightLegs") or [item]
                segments: list[FlightSegment] = []

                for seg in (seg_data if isinstance(seg_data, list) else [seg_data]):
                    carrier = seg.get("airlineCode") or seg.get("airline") or seg.get("carrier") or seg.get("al") or ""
                    carrier_name = seg.get("airlineName") or seg.get("alName") or carrier
                    flight_no = seg.get("flightNo") or seg.get("flightNumber") or seg.get("fltNo") or ""

                    dep_airport = seg.get("origin") or seg.get("departureAirport") or seg.get("org") or req.origin
                    arr_airport = seg.get("destination") or seg.get("arrivalAirport") or seg.get("dest") or req.destination

                    dep_dt = _parse_dt(
                        seg.get("departureDateTime") or seg.get("departure") or seg.get("depTime")
                        or seg.get("departTime")
                    )
                    arr_dt = _parse_dt(
                        seg.get("arrivalDateTime") or seg.get("arrival") or seg.get("arrTime")
                        or seg.get("arrivalTime")
                    )
                    dur = seg.get("duration") or seg.get("eft") or seg.get("durationMinutes") or 0

                    segments.append(FlightSegment(
                        airline=carrier_name, flight_no=f"{carrier}{flight_no}",
                        origin=dep_airport, destination=arr_airport,
                        departure=dep_dt, arrival=arr_dt,
                        duration_seconds=int(dur) * 60 if dur else 0,
                    ))

                if not segments:
                    continue

                total_dur = sum(s.duration_seconds for s in segments)
                route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=max(0, len(segments) - 1))
                oid = hashlib.md5(f"ytra_{req.origin}{req.destination}{date_str}{price}{segments[0].flight_no}".encode()).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"ytra_{oid}", price=round(price, 2), currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route, inbound=None,
                    airlines=list({s.airline for s in segments if s.airline}),
                    owner_airline=segments[0].airline if segments else "Yatra",
                    booking_url=f"{_BASE}/flights/search?type=O&ADT={req.adults or 1}&origin={req.origin}&destination={req.destination}&flight_depart_date={req.date_from.strftime('%d/%m/%Y')}&class=Economy",
                    is_locked=False, source="yatra_ota", source_tier="free",
                ))
            except Exception as e:
                logger.debug("Yatra parse error: %s", e)

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"yatra{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="INR", offers=[], total_results=0,
        )
