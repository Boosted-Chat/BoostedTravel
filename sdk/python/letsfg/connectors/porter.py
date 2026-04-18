"""
Porter Airlines CDP scraper — real Chrome with MCP flags to bypass Cloudflare.

Porter (IATA: PD) is a Canadian airline based at Billy Bishop Toronto City Airport.

Strategy:
1. Launch Chrome with MCP-style flags (bypasses Cloudflare bot detection)
2. Navigate directly to www.flyporter.com/en/flight/tickets/Select_BAF?...
3. Wait ~6-10s for Cloudflare challenge to auto-resolve
4. Parse flight cards from DOM → FlightOffer objects

Currency: CAD
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import platform
import re
import subprocess
import time
from datetime import datetime
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, proxy_chrome_args, auto_block_if_proxied, inject_stealth_js

logger = logging.getLogger(__name__)

# ── Chrome launch flags that bypass Cloudflare bot detection ──────────────
_CHROME_FLAGS = [
    "--disable-field-trial-config",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-back-forward-cache",
    "--disable-breakpad",
    "--disable-client-side-phishing-detection",
    "--disable-component-extensions-with-background-pages",
    "--disable-component-update",
    "--no-default-browser-check",
    "--disable-default-apps",
    "--disable-dev-shm-usage",
    "--disable-features=AvoidUnnecessaryBeforeUnloadCheckSync,"
    "BoundaryEventDispatchTracksNodeRemoval,DestroyProfileOnBrowserClose,"
    "DialMediaRouteProvider,GlobalMediaControls,HttpsUpgrades,LensOverlay,"
    "MediaRouter,PaintHolding,ThirdPartyStoragePartitioning,Translate,"
    "AutoDeElevate,RenderDocument,OptimizationHints,AutomationControlled",
    "--enable-features=CDPScreenshotNewSurface",
    "--allow-pre-commit-input",
    "--disable-hang-monitor",
    "--disable-ipc-flooding-protection",
    "--disable-popup-blocking",
    "--disable-prompt-on-repost",
    "--disable-renderer-backgrounding",
    "--force-color-profile=srgb",
    "--metrics-recording-only",
    "--no-first-run",
    *proxy_chrome_args(),
    "--password-store=basic",
    "--no-service-autorun",
    "--disable-search-engine-choice-screen",
    "--disable-infobars",
    "--disable-sync",
    "--enable-unsafe-swiftshader",
    "--window-position=-2400,-2400",
    "--window-size=1366,768",
]

_CDP_PORT = 9512
_chrome_proc: Optional[subprocess.Popen] = None
_browser = None
_pw_instance = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch Chrome with Cloudflare-bypassing flags and connect via CDP."""
    global _chrome_proc, _browser, _pw_instance
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser

        from .browser import find_chrome

        chrome = find_chrome()
        user_data = os.path.join(
            os.environ.get("TEMP", "/tmp"), "chrome-cdp-porter"
        )
        os.makedirs(user_data, exist_ok=True)

        args = [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={user_data}",
            *_CHROME_FLAGS,
            "about:blank",
        ]

        popen_kw: dict = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if platform.system() == "Windows":
            si = subprocess.STARTUPINFO()
            si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            si.wShowWindow = 7  # SW_SHOWMINNOACTIVE
            popen_kw["startupinfo"] = si
            popen_kw["creationflags"] = subprocess.CREATE_NO_WINDOW

        _chrome_proc = subprocess.Popen(args, **popen_kw)
        await asyncio.sleep(2.5)

        from playwright.async_api import async_playwright

        _pw_instance = await async_playwright().start()
        _browser = await _pw_instance.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_CDP_PORT}"
        )
        logger.info(
            "Porter: Chrome ready via CDP (port %d, pid %d)",
            _CDP_PORT,
            _chrome_proc.pid,
        )
        return _browser


class PorterConnectorClient:
    """Porter Airlines scraper — direct URL + Cloudflare bypass via MCP Chrome flags."""

    _RESULTS_URL_TPL = (
        "https://www.flyporter.com/en/flight/tickets/Select_BAF"
        "?departStation={origin}&destination={dest}&depDate={date}"
        "&paxADT=1&paxCHD=0&paxINF=0&trpType={trip_type}&fareClass=R&bookWithPoints=0"
        "{ret_param}"
    )

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
        browser = await _get_browser()
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()

        try:
            trip_type = "RoundTrip" if req.return_from else "OneWay"
            ret_param = ""
            if req.return_from:
                ret_date = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, 'strftime') else str(req.return_from)
                ret_param = f"&retDate={ret_date}"
            results_url = self._RESULTS_URL_TPL.format(
                origin=req.origin,
                dest=req.destination,
                date=req.date_from.strftime("%Y-%m-%d"),
                trip_type=trip_type,
                ret_param=ret_param,
            )
            logger.info(
                "Porter: searching %s→%s on %s",
                req.origin, req.destination, req.date_from.strftime("%Y-%m-%d"),
            )

            await inject_stealth_js(page)
            await auto_block_if_proxied(page)
            try:
                await page.goto(results_url, wait_until="commit", timeout=15000)
            except Exception:
                pass  # Execution context destroyed during Cloudflare redirect

            # Wait for Cloudflare challenge to auto-resolve
            cf_ok = await self._wait_cloudflare(page, timeout=20)
            if not cf_ok:
                logger.warning("Porter: Cloudflare challenge did not resolve")
                return self._empty(req)

            # Wait for flight cards to render
            try:
                await page.wait_for_selector("h4:has-text('Departs')", timeout=15000)
            except Exception:
                await asyncio.sleep(3.0)

            offers = await self._extract_from_dom(page, req)
            elapsed = time.monotonic() - t0
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Porter error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.goto("about:blank", wait_until="commit", timeout=5000)
            except Exception:
                pass

    async def _wait_cloudflare(self, page, timeout: int = 20) -> bool:
        """Wait for Cloudflare challenge to resolve. Returns True if cleared."""
        for _ in range(timeout):
            try:
                title = await page.title()
            except Exception:
                # Execution context destroyed = page navigated = CF likely cleared
                await asyncio.sleep(1.5)
                continue
            if "moment" not in title.lower() and "security" not in title.lower():
                return True
            await asyncio.sleep(1.0)
        return False

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight cards from the www.flyporter.com results page DOM."""
        try:
            flight_data = await page.evaluate("""() => {
                const results = [];
                // Detect section headings to classify flights by direction
                let currentSection = 'outbound';
                const allElements = document.querySelectorAll('li, h2, h3, [class*="heading"], [class*="section"]');
                
                const items = document.querySelectorAll('li');
                for (const item of items) {
                    // Check for section labels in parent/preceding elements
                    const parentText = (item.closest('[class*="section"]') || item.parentElement || {}).textContent || '';
                    if (/return|inbound/i.test(parentText) && !/depart|outbound/i.test(parentText.substring(0, 50))) {
                        currentSection = 'inbound';
                    }
                    
                    const headings = item.querySelectorAll('h4');
                    let dep = null, arr = null;
                    for (const h of headings) {
                        const t = h.textContent.trim();
                        if (t.startsWith('Departs')) dep = t.replace('Departs', '').trim();
                        if (t.startsWith('Arrives')) arr = t.replace('Arrives', '').trim();
                    }
                    if (!dep || !arr) continue;

                    // Flight number
                    let flightNum = '';
                    const allText = item.innerText;
                    const fnMatch = allText.match(/PD\\s*\\d+/);
                    if (fnMatch) flightNum = fnMatch[0].replace(/\\s+/g, '');

                    // Duration
                    let duration = '';
                    const durMatch = allText.match(/(\\d+)\\s*min/);
                    if (durMatch) duration = durMatch[0];

                    // Stops
                    const isNonstop = /non.?stop/i.test(allText);

                    // Fares — buttons containing "Fare category:" text
                    const fares = [];
                    const fareButtons = item.querySelectorAll('button');
                    for (const btn of fareButtons) {
                        const bt = btn.textContent || '';
                        if (!bt.includes('Fare category')) continue;
                        const priceMatch = bt.match(/\\$(\\d+(?:,\\d{3})*(?:\\.\\d{2})?)/);
                        const catMatch = bt.match(/Fare category:\\s*([\\w]+)/);
                        if (priceMatch && catMatch) {
                            fares.push({
                                category: catMatch[1],
                                price: parseFloat(priceMatch[1].replace(',', '')),
                            });
                        }
                    }

                    if (fares.length > 0) {
                        results.push({ dep, arr, flightNum, duration, nonstop: isNonstop, fares, section: currentSection });
                    }
                }
                return results;
            }""")

            if not flight_data:
                logger.info("Porter: no flight cards found in DOM")
                return []

            logger.info("Porter: extracted %d flights from DOM", len(flight_data))

            booking_url = self._build_booking_url(req)
            dep_date = req.date_from.strftime("%Y-%m-%d")
            ret_date = None
            if req.return_from:
                ret_date = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, 'strftime') else str(req.return_from)
            offers: list[FlightOffer] = []
            ob_flights = []
            ib_flights = []

            for f in flight_data:
                section = f.get("section", "outbound")
                use_date = ret_date if (section == "inbound" and ret_date) else dep_date
                dep_time = self._parse_time(f.get("dep", ""), use_date)
                arr_time = self._parse_time(f.get("arr", ""), use_date)
                dur_min = 0
                dur_match = re.search(r"(\d+)\s*min", f.get("duration", ""))
                if dur_match:
                    dur_min = int(dur_match.group(1))
                hr_match = re.search(r"(\d+)\s*h", f.get("duration", ""))
                if hr_match:
                    dur_min += int(hr_match.group(1)) * 60

                flight_num = f.get("flightNum", "")
                nonstop = f.get("nonstop", True)
                dur_sec = dur_min * 60

                seg_origin = req.destination if section == "inbound" else req.origin
                seg_dest = req.origin if section == "inbound" else req.destination
                seg = FlightSegment(
                    airline="PD",
                    airline_name="Porter Airlines",
                    flight_no=flight_num,
                    origin=seg_origin,
                    destination=seg_dest,
                    departure=dep_time,
                    arrival=arr_time,
                    duration_seconds=dur_sec,
                )
                route = FlightRoute(
                    segments=[seg],
                    stopovers=0 if nonstop else 1,
                    total_duration_seconds=dur_sec,
                )

                for fare in f.get("fares", []):
                    price = fare.get("price", 0)
                    cat = fare.get("category", "Economy")
                    offer_id = hashlib.md5(
                        f"PD-{flight_num}-{dep_time}-{cat}-{price}".encode()
                    ).hexdigest()[:12]
                    entry = {"route": route, "price": price, "cat": cat, "offer_id": offer_id}
                    if section == "inbound":
                        ib_flights.append(entry)
                    else:
                        ob_flights.append(entry)

            # Build offers: pair OB x cheapest IB for RT, or emit OW
            if req.return_from and ob_flights and ib_flights:
                cheapest_ib = min(ib_flights, key=lambda x: x["price"])
                for ob in ob_flights:
                    offers.append(FlightOffer(
                        id=f"pd_rt_{ob['offer_id']}",
                        price=float(ob["price"] + cheapest_ib["price"]),
                        currency="CAD",
                        outbound=ob["route"],
                        inbound=cheapest_ib["route"],
                        airlines=["PD"],
                        owner_airline="PD",
                        source="porter_scraper",
                        source_tier="protocol",
                        is_locked=False,
                        booking_url=booking_url,
                    ))
            else:
                for entry in (ob_flights or ib_flights):
                    offers.append(FlightOffer(
                        id=entry["offer_id"],
                        price=float(entry["price"]),
                        currency="CAD",
                        outbound=entry["route"],
                        airlines=["PD"],
                        owner_airline="PD",
                        source="porter_scraper",
                        source_tier="protocol",
                        is_locked=False,
                        booking_url=booking_url,
                    ))

            return offers
        except Exception as e:
            logger.warning("Porter: DOM extraction error: %s", e)
        return []

    @staticmethod
    def _parse_time(time_str: str, date_str: str) -> datetime:
        """Parse '7:25AM' into a datetime object."""
        time_str = time_str.strip().upper()
        for fmt in ["%I:%M%p", "%I:%M %p"]:
            try:
                t = datetime.strptime(time_str, fmt)
                d = datetime.strptime(date_str, "%Y-%m-%d")
                return d.replace(hour=t.hour, minute=t.minute, second=0)
            except ValueError:
                continue
        return datetime.strptime(date_str, "%Y-%m-%d")

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Porter %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"porter{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        url = (
            f"https://www.flyporter.com/en/flight-results?from={req.origin}"
            f"&to={req.destination}&departure={dep}&adults={req.adults}&tripType={'roundtrip' if req.return_from else 'oneway'}"
        )
        if req.return_from:
            ret = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, 'strftime') else str(req.return_from)
            url += f"&return={ret}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"porter{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
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
                    id=f"rt_porter_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
