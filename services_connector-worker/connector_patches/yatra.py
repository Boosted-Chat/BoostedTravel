"""Yatra connector - India's OTA (CDP Chrome + Akamai warmup + fetch API).

Yatra.com is one of India's largest OTAs. Strong on domestic Indian routes
and India-international connections. Has competitive fares from consolidator
arrangements with Indian carriers.

Strategy:
1. Launch real Chrome via --remote-debugging-port (bypasses Akamai).
2. Warm up Akamai on flight.yatra.com (solve challenge once).
3. Navigate to fbdom_flight/trigger (domestic) or int_one_way/trigger (intl).
4. Call /air-service/dom|int/search via page.evaluate(fetch()) reusing Akamai cookies.
5. Parse merged resultData batches (fltSchedule + fareDetails join).
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

# Major Indian airports for domestic/international endpoint selection
_INDIA_AIRPORTS = {
    "DEL", "BOM", "BLR", "MAA", "CCU", "HYD", "AMD", "COK", "GOI", "PNQ",
    "JAI", "LKO", "PAT", "GAU", "IXC", "SXR", "VNS", "IXB", "BBI", "RPR",
    "NAG", "IDR", "VTZ", "IXR", "RAJ", "TRZ", "CJB", "UDR", "JDH", "IXA",
    "IMF", "DIB", "DED", "AIP", "IXE", "IXM", "KLH", "BDQ", "STV", "AGR",
}

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

        # Wait for Chrome to be ready on CDP port (cold start can take >2s)
        pw = await async_playwright().start()
        _pw_instance = pw
        for _attempt in range(6):
            await asyncio.sleep(2.0)
            try:
                _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
                logger.info("Yatra: Chrome launched on CDP port %d (pid %d, attempt %d)", _CDP_PORT, _chrome_proc.pid, _attempt)
                return _browser
            except Exception:
                if _attempt == 5:
                    raise
        raise RuntimeError(f"Chrome failed to start on port {_CDP_PORT}")


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
    """Yatra — India's OTA, CDP Chrome + Akamai warmup + fetch API."""

    def __init__(self, timeout: float = 70.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Yatra has native RT support — single search returns both OB and IB.
        # No need for the two-search wrapper (which caused 404s on reversed routes).
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")
        date_yatra = req.date_from.strftime("%d/%m/%Y")

        context = await _get_context()
        page = await context.new_page()

        captured: list[dict] = []

        async def _on_response(response):
            url = response.url.lower()
            ct = response.headers.get("content-type", "")
            is_search_api = any(kw in url for kw in (
                "air-search", "fbdom", "fbint", "getfares", "lowest-fare",
                "flightlist", "searchajax", "populat", "domresult", "intresult",
            ))
            is_flight_json = ("flight.yatra.com" in url and ("json" in ct or "javascript" in ct))
            if response.status == 200 and (is_search_api or is_flight_json):
                try:
                    data = await response.json()
                    if isinstance(data, (dict, list)):
                        captured.append(data if isinstance(data, dict) else {"results": data})
                except Exception:
                    pass

        page.on("response", _on_response)

        try:
            is_domestic = (req.origin in _INDIA_AIRPORTS and req.destination in _INDIA_AIRPORTS)
            date_encoded = date_yatra.replace("/", "%2F")  # URL-safe DD%2FMM%2FYYYY

            # Step 1: Warm up Akamai on flight.yatra.com
            logger.info("Yatra: warming up Akamai (%s->%s, domestic=%s)", req.origin, req.destination, is_domestic)
            await page.goto("https://flight.yatra.com/air-search-ui/", wait_until="domcontentloaded", timeout=30000)
            for w in range(15):
                await asyncio.sleep(2.0)
                try:
                    t = await page.title()
                    if "challenge" not in t.lower() and "validation" not in t.lower():
                        logger.info("Yatra: Akamai solved in %ds", (w + 1) * 2)
                        break
                except Exception:
                    pass
            await asyncio.sleep(1.0)

            # Step 2: Navigate to trigger page — try fbdom_flight (domestic) or int_one_way (intl)
            if is_domestic:
                trigger_url = (
                    f"https://flight.yatra.com/air-search-ui/fbdom_flight/trigger?"
                    f"ADT={req.adults or 1}&CNN=0&INF=0"
                    f"&origin={req.origin}&destination={req.destination}"
                    f"&flight_depart_date={date_encoded}&type=O"
                    f"&viewName=normal&flexi=N&class=Economy"
                )
            else:
                trigger_url = (
                    f"https://flight.yatra.com/air-search-ui/int_one_way/trigger?"
                    f"ADT={req.adults or 1}&CNN=0&INF=0"
                    f"&origin={req.origin}&destination={req.destination}"
                    f"&flight_depart_date={date_encoded}&type=O"
                    f"&viewName=normal&flexi=N&class=Economy"
                )

            logger.info("Yatra: navigating to trigger (domestic=%s)", is_domestic)
            await page.goto(trigger_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3.0)

            # Wait for any secondary Akamai challenge on trigger page
            try:
                title = await page.title()
            except Exception:
                title = ""
            if "challenge" in title.lower() or "validation" in title.lower():
                logger.info("Yatra: Akamai challenge on trigger page, waiting...")
                for w2 in range(20):
                    await asyncio.sleep(2.0)
                    try:
                        title = await page.title()
                        if "challenge" not in title.lower() and "validation" not in title.lower():
                            logger.info("Yatra: trigger challenge solved in %ds", (w2 + 1) * 2)
                            break
                    except Exception:
                        pass  # context destroyed during navigation — expected
                # After challenge, page may have navigated — wait for load
                await asyncio.sleep(2.0)
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass
                try:
                    title = await page.title()
                except Exception:
                    title = ""

            cur_url = page.url
            if "/gdpr" in cur_url.lower():
                logger.warning("Yatra: GDPR block")
                return self._empty(req)

            logger.info("Yatra: trigger loaded: %s (title=%s)", cur_url[:120], title[:60])

            await _dismiss_cookies(page)

            # Step 3: Call /air-service/dom/search API directly from the page context
            # (Akamai cookies from steps 1+2 make this work)
            api_scope = "dom" if is_domestic else "int"
            search_params = (
                f"ADT={req.adults or 1}&CHD=0&CNN=0&INF=0"
                f"&origin={req.origin}&destination={req.destination}"
                f"&flight_depart_date={date_yatra}&type=O"
                f"&viewName=normal&flexi=N&class=Economy"
            )

            fetch_js = f"""async () => {{
                const paths = ['/air-service/{api_scope}/search', '/air-service/{api_scope}/trigger'];
                let best = null;
                for (const path of paths) {{
                    try {{
                        const r = await fetch(path + '?{search_params}');
                        if (r.status === 200) {{
                            const text = await r.text();
                            if (text.length > 1000 && (!best || text.length > best.length)) {{
                                best = text;
                            }}
                        }}
                    }} catch(e) {{}}
                }}
                return best;
            }}"""

            try:
                raw = await page.evaluate(fetch_js)
                if raw:
                    search_data = json.loads(raw)
                    logger.info("Yatra: search API returned %d bytes", len(raw))
                    captured.append(search_data)
            except Exception as e:
                logger.info("Yatra: search API error: %s — retrying after 3s", str(e)[:80])
                await asyncio.sleep(3.0)
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                    raw = await page.evaluate(fetch_js)
                    if raw:
                        search_data = json.loads(raw)
                        logger.info("Yatra: search API retry returned %d bytes", len(raw))
                        captured.append(search_data)
                except Exception as e2:
                    logger.info("Yatra: search API retry also failed: %s", str(e2)[:80])

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

            sh = hashlib.md5(f"yatra{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()).hexdigest()[:12]
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
        """Parse Yatra flight results from /air-service/dom/search or /trigger response.

        Structure:
          resultData[0]:
            fltSchedule:
              {routeKey}: [{ID, OD: [{tdu, FS: [{ac, fl, dac, aac, ddt, adt, dd, ad, du, dum, ...}]}]}]
              airlineNames: {ac: name}
            fareDetails:
              {routeKey}:
                {flightID}:
                  O:
                    ADT: {bf: baseFare, tf: totalFare}
        """
        offers: list[FlightOffer] = []

        # Navigate to resultData — may have multiple batches
        rd_list = data.get("resultData")
        if not rd_list or not isinstance(rd_list, list):
            return offers

        # Merge fltSchedule and fareDetails across ALL resultData entries
        all_sched: dict = {}
        all_fares: dict = {}
        airline_names: dict = {}
        for rd in rd_list:
            if not isinstance(rd, dict):
                continue
            sched = rd.get("fltSchedule")
            if isinstance(sched, dict):
                if sched.get("airlineNames"):
                    airline_names.update(sched["airlineNames"])
                for k, v in sched.items():
                    if isinstance(v, list) and v and isinstance(v[0], dict) and "OD" in v[0]:
                        all_sched.setdefault(k, []).extend(v)
            for fk in ("fareDetails", "fareDetailsSR"):
                fd = rd.get(fk)
                if isinstance(fd, dict):
                    for rk, rv in fd.items():
                        if isinstance(rv, dict):
                            all_fares.setdefault(rk, {}).update(rv)

        if not all_sched:
            return offers

        logger.debug("Yatra parse: %d batches, %d flights, %d fare keys, %d airlines",
                      len(rd_list), sum(len(v) for v in all_sched.values()), len(all_fares), len(airline_names))

        # Iterate over all route keys and their flights
        for route_key, flights in all_sched.items():
            fare_map = all_fares.get(route_key, {})

            for item in flights[:80]:
                try:
                    fid = item.get("ID", "")
                    od_list = item.get("OD")
                    if not od_list or not isinstance(od_list, list):
                        continue
                    od = od_list[0]

                    # Get price from fareDetails using flight ID
                    price = 0.0
                    fare_entry = fare_map.get(fid, {})
                    if isinstance(fare_entry, dict):
                        ow = fare_entry.get("O", {})
                        adt = ow.get("ADT", {})
                        if isinstance(adt, dict):
                            tf = adt.get("tf") or adt.get("TF") or adt.get("totalFare")
                            if tf:
                                price = float(tf)

                    if price <= 0:
                        continue

                    # Parse segments from FS array
                    fs_list = od.get("FS", [])
                    if not isinstance(fs_list, list) or not fs_list:
                        continue

                    segments: list[FlightSegment] = []
                    for seg in fs_list:
                        ac = seg.get("ac", "")
                        carrier_name = airline_names.get(ac, ac)
                        fl = seg.get("fl", "")
                        dep_date = seg.get("ddt", "")
                        arr_date = seg.get("adt", "")
                        dep_time = seg.get("dd", "")  # "13:00"
                        arr_time = seg.get("ad", "")  # "15:20"
                        dep_dt = _parse_dt(f"{dep_date}T{dep_time}" if dep_date and dep_time else "")
                        arr_dt = _parse_dt(f"{arr_date}T{arr_time}" if arr_date and arr_time else "")
                        dur_min = int(seg.get("dum") or seg.get("du") or 0)

                        segments.append(FlightSegment(
                            airline=carrier_name, flight_no=f"{ac}{fl}",
                            origin=seg.get("dac", req.origin),
                            destination=seg.get("aac", req.destination),
                            departure=dep_dt, arrival=arr_dt,
                            duration_seconds=dur_min * 60,
                        ))

                    if not segments:
                        continue

                    total_dur = int(od.get("tdu", "0").replace(":", "")) if ":" in str(od.get("tdu", "")) else 0
                    if total_dur == 0:
                        total_dur = sum(s.duration_seconds for s in segments)
                    else:
                        # tdu = "02:20" → 2*3600 + 20*60
                        tdu_str = od.get("tdu", "0:0")
                        parts = tdu_str.split(":")
                        if len(parts) == 2:
                            total_dur = int(parts[0]) * 3600 + int(parts[1]) * 60

                    route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=max(0, len(segments) - 1))
                    oid = hashlib.md5(f"ytra_{req.origin}{req.destination}{date_str}{price}{segments[0].flight_no}".encode()).hexdigest()[:12]

                    bk_url = (
                        f"{_BASE}/air-search-ui/fbdom_flight/trigger?"
                        f"ADT={req.adults or 1}&CNN=0&INF=0&origin={req.origin}&destination={req.destination}"
                        f"&flight_depart_date={req.date_from.strftime('%d/%m/%Y')}&type=O&viewName=normal&flexi=N&class=Economy"
                    )

                    offers.append(FlightOffer(
                        id=f"ytra_{oid}", price=round(price, 2), currency="INR",
                        price_formatted=f"{price:.0f} INR",
                        outbound=route, inbound=None,
                        airlines=list({s.airline for s in segments if s.airline}),
                        owner_airline=segments[0].airline if segments else "Yatra",
                        booking_url=bk_url,
                        is_locked=False, source="yatra_ota", source_tier="free",
                    ))
                except Exception as e:
                    logger.debug("Yatra parse item error: %s", e)

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"yatra{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="INR", offers=[], total_results=0,
        )
