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
import shutil
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
from .airline_routes import get_country
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


def _decode_json_like(value: Any) -> Any:
    """Best-effort decode for JSON payloads that may be string-encoded 1-2 times."""
    decoded = value
    for _ in range(3):
        if not isinstance(decoded, str):
            break
        text = decoded.strip()
        if not text:
            break
        # Handle JSONP wrappers used by some Yatra endpoints.
        m = re.search(r"^\w+\((.*)\);?$", text, re.S)
        if m:
            text = m.group(1)
        try:
            decoded = json.loads(text)
        except Exception:
            break
    return decoded


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


async def _trigger_roundtrip_search(page) -> None:
    """Best-effort trigger for Yatra RT UI flows that require an explicit click."""
    candidates = [
        "Search",
        "Search Flights",
        "Modify Search",
        "Done",
        "Apply",
    ]
    for label in candidates:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await asyncio.sleep(0.8)
                return
        except Exception:
            continue

    # Fallback: pressing Enter can submit focused RT forms on some builds.
    try:
        await page.keyboard.press("Enter")
        await asyncio.sleep(0.5)
    except Exception:
        pass


async def _close_chrome() -> None:
    """Terminate the Yatra Chrome process and reset global state."""
    global _browser, _pw_instance, _chrome_proc, _context
    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None
    if _pw_instance:
        try:
            await _pw_instance.stop()
        except Exception:
            pass
        _pw_instance = None
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
            _chrome_proc.wait(timeout=5)
        except Exception:
            try:
                _chrome_proc.kill()
            except Exception:
                pass
        _chrome_proc = None
    _context = None


class YatraConnectorClient:
    """Yatra — India's OTA, CDP Chrome + deep-link + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout
        # True RT is the primary mode. Split fallback is opt-in so failures are visible.
        self.allow_split_fallback = os.getenv("LETSFG_YATRA_ALLOW_SPLIT_FALLBACK", "0") == "1"

    async def close(self):
        await _close_chrome()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Prefer true round-trip capture from Yatra's own type=R flow.
        # If no offers are exposed, fallback to synthetic one-way pairing.

        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")
        date_yatra = req.date_from.strftime("%d/%m/%Y")
        return_date_str = req.return_from.strftime("%Y-%m-%d") if req.return_from else None
        return_date_yatra = req.return_from.strftime("%d/%m/%Y") if req.return_from else None

        context = await _get_context()
        page = await context.new_page()

        captured: list[dict] = []
        all_json_urls: list[str] = []

        async def _on_response(response):
            url = response.url.lower()
            ct = response.headers.get("content-type", "")
            is_relevant = any(
                kw in url
                for kw in (
                    "air-search",
                    "air-service",
                    "seoint",
                    "dom2",
                    "flight",
                    "fbdom",
                    "fbint",
                    "result",
                    "search",
                    "itinerary",
                    "fare",
                    "yatra",
                )
            )
            if response.status == 200 and is_relevant:
                all_json_urls.append(response.url)
                try:
                    data = _decode_json_like(await response.json())
                    if isinstance(data, (dict, list)):
                        captured.append(data if isinstance(data, dict) else {"results": data})
                        return
                except Exception:
                    pass

                # Fallback: some endpoints mislabel JSON as text/plain or text/html.
                try:
                    text = await response.text()
                    if not text:
                        return
                    data = _decode_json_like(text)
                    if isinstance(data, (dict, list)):
                        captured.append(data if isinstance(data, dict) else {"results": data})
                except Exception:
                    pass

        page.on("response", _on_response)

        try:
            # Determine domestic vs international using shared IATA→country map.
            # Yatra expects ISO-2 country codes in originCountry/destinationCountry.
            origin_country = get_country(req.origin) or ""
            dest_country = get_country(req.destination) or ""
            is_domestic = bool(origin_country) and origin_country == dest_country

            trip_type = "R" if req.return_from else "O"
            api_host = "seodom" if is_domestic else "seoint"

            if is_domestic:
                # Domestic: modern dom2 flow (returns fltSchedule/fareDetails poll payloads).
                search_url = (
                    f"{_FLIGHT_API}/air-search-ui/dom2/trigger"
                    f"?flex=0&viewName=normal&source=fresco-flights&type={trip_type}"
                    f"&class=Economy&noOfSegments=1"
                    f"&ADT={req.adults or 1}&CHD={req.children or 0}&INF={req.infants or 0}"
                    f"&origin={req.origin}&originCountry={origin_country}"
                    f"&destination={req.destination}&destinationCountry={dest_country}"
                    f"&flight_depart_date={date_yatra}"
                )
                if req.return_from:
                    search_url += f"&arrivalDate={return_date_yatra}"
            else:
                # International: Yatra still uses seoint flow for many routes.
                seoint_segments = 2 if req.return_from else 1
                search_url = (
                    f"{_FLIGHT_API}/air-search-ui/{api_host}/trigger"
                    f"?type={trip_type}&viewName=normal&flexi=0&noOfSegments={seoint_segments}"
                    f"&specialfaretype=&originCountry={origin_country}&destinationCountry={dest_country}"
                    f"&ADT={req.adults or 1}&CHD={req.children or 0}&INF={req.infants or 0}"
                    f"&preferred=&class=Economy&source=seo&hb=0"
                    f"&origin={req.origin}&destination={req.destination}"
                    f"&flight_depart_date={date_yatra}"
                )
                if req.return_from:
                    search_url += f"&flight_return_date={return_date_yatra}&arrivalDate={return_date_yatra}"

            # Warm up with yatra.com to establish session cookies before hitting search API
            logger.info("Yatra: warming up session via yatra.com")
            try:
                await page.goto(_BASE, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(2.0)
                await _dismiss_cookies(page)
            except Exception as warmup_err:
                logger.debug("Yatra: warmup failed (continuing): %s", warmup_err)

            logger.info("Yatra: navigating to %s", search_url)
            await page.goto(search_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            await asyncio.sleep(3.0)

            # Check for GDPR block (Yatra blocks EU traffic)
            current_url = page.url.lower()
            page_title = (await page.title()).lower()
            if "/gdpr" in current_url or "temporarily unavailable" in page_title or "404" in page_title or "not found" in page_title:
                logger.warning("Yatra: page error or GDPR block — url=%s title=%s", page.url, await page.title())
                return self._empty(req, {
                    "yatra_mode": "one_way" if not req.return_from else "round_trip",
                    "yatra_roundtrip_mode": "true_rt" if req.return_from else "n/a",
                    "yatra_status": "blocked_or_error_page",
                })

            await _dismiss_cookies(page)

            if req.return_from:
                await _trigger_roundtrip_search(page)

            # Polling loop: wait for API or window data
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            offers: list[FlightOffer] = []
            # Track when we first got offers from dom2 polls; wait a short settle
            # window so parallel airline polls (SpiceJet, IndiGo, etc.) all arrive.
            _first_offers_at: float = 0.0
            _DOM2_SETTLE_SECS = 6.0

            while time.monotonic() < deadline:
                # Collect from ALL captured API responses (dom2 polls may be one-per-airline)
                seen_ids: set[str] = set()
                current_offers: list[FlightOffer] = []

                # First pass: merge all dom2 payload fragments so outbound/inbound legs
                # split across multiple poll responses can be parsed together.
                dom2_payloads: list[dict] = []
                for data in captured:
                    dom2_payloads.extend(self._extract_dom2_payloads(data))
                if dom2_payloads:
                    merged_dom2 = self._merge_dom2_payloads(dom2_payloads)
                    for o in self._parse(merged_dom2, req, date_str, return_date_str):
                        if o.id not in seen_ids:
                            seen_ids.add(o.id)
                            current_offers.append(o)

                for data in sorted(captured, key=lambda d: len(str(d)), reverse=True):
                    for o in self._parse(data, req, date_str, return_date_str):
                        if o.id not in seen_ids:
                            seen_ids.add(o.id)
                            current_offers.append(o)
                        # Skip malformed legacy-parser false-positives: must have airline name;
                        # RT requests must also have an inbound route.
                        current_offers = [
                            o for o in current_offers
                            if o.owner_airline and (not req.return_from or o.inbound is not None)
                        ]
                    # Legacy fallback: lowest-fare-service returns one big all-airline JSON,
                    # so a single successful parse is sufficient for that format.
                    if current_offers and "day" in data:
                        break

                if current_offers:
                    offers = current_offers
                    if _first_offers_at == 0.0:
                        _first_offers_at = time.monotonic()
                    # Settle window: let remaining parallel airline polls land
                    if time.monotonic() - _first_offers_at >= _DOM2_SETTLE_SECS:
                        break

                # Try window globals (only needed if no API responses yet)
                if not current_offers:
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
                                window_offers = self._parse(props, req, date_str, return_date_str)
                                if window_offers:
                                    offers = window_offers
                                    break
                    except Exception:
                        pass

                await asyncio.sleep(1.5)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("Yatra %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed)

            if req.return_from and not offers:
                # International RT pages can render offers while API payload shapes vary by route.
                # To avoid persistent 0-result runs, auto-fallback to one-way pairing for
                # international routes when true RT parsing yields no offers.
                if not is_domestic:
                    logger.info("Yatra international true RT yielded 0 offers; falling back to one-way pairing")
                    return await self._search_round_trip(req)

                # Keep true RT observable by default. Split fallback is opt-in.
                if self.allow_split_fallback:
                    split_offers = await self._fetch_split_rt(
                        page, req, api_host, date_str, return_date_str
                    )
                    if split_offers:
                        split_offers.sort(key=lambda o: o.price)
                        logger.info("Yatra split RT fallback (%s\u2192%s): %d offers", req.origin, req.destination, len(split_offers))
                        sh = hashlib.md5(f"yatra{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
                        return FlightSearchResponse(
                            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
                            currency="INR", offers=split_offers, total_results=len(split_offers),
                            search_params={
                                "yatra_mode": "round_trip",
                                "yatra_roundtrip_mode": "split_rt",
                                "yatra_status": "ok",
                            },
                        )
                    logger.info("Yatra true RT and split RT yielded 0 offers, falling back to one-way pairing")
                    return await self._search_round_trip(req)

                logger.info("Yatra true RT yielded 0 offers; split fallback disabled")
                return self._empty(req, {
                    "yatra_mode": "round_trip",
                    "yatra_roundtrip_mode": "true_rt",
                    "yatra_status": "true_rt_no_offers",
                })

            sh = hashlib.md5(f"yatra{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "INR",
                offers=offers, total_results=len(offers),
                search_params={
                    "yatra_mode": "one_way" if not req.return_from else "round_trip",
                    "yatra_roundtrip_mode": "true_rt" if req.return_from else "n/a",
                    "yatra_status": "ok",
                },
            )
        except Exception as e:
            logger.error("Yatra CDP error: %s", e)
            return self._empty(req, {
                "yatra_mode": "one_way" if not req.return_from else "round_trip",
                "yatra_roundtrip_mode": "true_rt" if req.return_from else "n/a",
                "yatra_status": f"error:{type(e).__name__}",
            })
        finally:
            try:
                await page.close()
            except Exception:
                pass

    def _extract_dom2_payloads(self, data: dict) -> list[dict]:
        """Extract dom2 payload dict(s) from direct or wrapped response shapes."""
        payloads: list[dict] = []
        if isinstance(data, dict) and "fltSchedule" in data and "fareDetails" in data:
            payloads.append(data)

        # Some seoint responses wrap payloads in nested objects.
        nested_data = data.get("data") if isinstance(data, dict) else None
        if isinstance(nested_data, dict) and "fltSchedule" in nested_data and "fareDetails" in nested_data:
            payloads.append(nested_data)

        nested_response = data.get("response") if isinstance(data, dict) else None
        if isinstance(nested_response, dict) and "fltSchedule" in nested_response and "fareDetails" in nested_response:
            payloads.append(nested_response)

        results = data.get("results") if isinstance(data, dict) else None
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict) and "fltSchedule" in item and "fareDetails" in item:
                    payloads.append(item)
                if isinstance(item, dict):
                    item_data = item.get("data")
                    if isinstance(item_data, dict) and "fltSchedule" in item_data and "fareDetails" in item_data:
                        payloads.append(item_data)
        return payloads

    def _merge_dom2_payloads(self, payloads: list[dict]) -> dict:
        """Merge multiple dom2 payload fragments into one parseable structure."""
        merged: dict[str, Any] = {
            "fltSchedule": {},
            "fareDetails": {},
        }

        for p in payloads:
            fs = p.get("fltSchedule") or {}
            fd = p.get("fareDetails") or {}

            if isinstance(fs, dict):
                for k, v in fs.items():
                    # Leg keys are lists; metadata keys are dict/scalars.
                    if isinstance(v, list):
                        existing = merged["fltSchedule"].get(k)
                        if not isinstance(existing, list):
                            merged["fltSchedule"][k] = list(v)
                        else:
                            seen_ids = {
                                it.get("ID") for it in existing
                                if isinstance(it, dict) and it.get("ID")
                            }
                            for it in v:
                                if isinstance(it, dict):
                                    iid = it.get("ID")
                                    if iid and iid in seen_ids:
                                        continue
                                    if iid:
                                        seen_ids.add(iid)
                                existing.append(it)
                    elif isinstance(v, dict):
                        existing = merged["fltSchedule"].get(k)
                        if isinstance(existing, dict):
                            existing.update(v)
                        else:
                            merged["fltSchedule"][k] = dict(v)
                    else:
                        if k not in merged["fltSchedule"]:
                            merged["fltSchedule"][k] = v

            if isinstance(fd, dict):
                for leg_key, leg_map in fd.items():
                    if not isinstance(leg_map, dict):
                        continue
                    existing_leg = merged["fareDetails"].get(leg_key)
                    if not isinstance(existing_leg, dict):
                        merged["fareDetails"][leg_key] = dict(leg_map)
                    else:
                        existing_leg.update(leg_map)

        return merged

    def _parse(self, data: dict, req: FlightSearchRequest, date_str: str, return_date_str: Optional[str] = None) -> list[FlightOffer]:
        """Parse Yatra flight results — handles dom2/poll, lowest-fare-service and legacy formats."""
        # Some international responses wrap the flight payload in nested containers.
        for nested_key in ("data", "response"):
            nested = data.get(nested_key) if isinstance(data, dict) else None
            if not isinstance(nested, dict) or nested is data:
                continue
            if not any(
                k in nested
                for k in (
                    "fltSchedule",
                    "fareDetails",
                    "day",
                    "flightList",
                    "flights",
                    "itineraries",
                    "result",
                    "results",
                    "OB",
                    "outbound",
                    "data",
                    "response",
                )
            ):
                continue
            nested_offers = self._parse(nested, req, date_str, return_date_str)
            if nested_offers:
                return nested_offers

        # Some responses arrive as wrapper objects like {"results": [...]} where
        # each list item is the real payload (including dom2 structures).
        if isinstance(data.get("results"), list):
            merged: list[FlightOffer] = []
            seen_ids: set[str] = set()
            for item in data.get("results", []):
                if not isinstance(item, dict):
                    continue
                for offer in self._parse(item, req, date_str, return_date_str):
                    if offer.id not in seen_ids:
                        seen_ids.add(offer.id)
                        merged.append(offer)
            if merged:
                return merged

        # ── Format 0: dom2/poll structure ─────────────────────────────────────
        # {"fltSchedule": {...}, "fareDetails": {...}, ...}
        if "fltSchedule" in data and "fareDetails" in data:
            return self._parse_dom2_poll(data, req, date_str, return_date_str)

        # ── Format 1: lowest-fare-service structure ──────────────────────────
        # {"day": {"2026-08-01": {"af": {"6E": {"tf":..., "ow":[...]}}}}}
        if "day" in data and isinstance(data.get("day"), dict):
            return self._parse_lowest_fare(data, req, date_str, return_date_str)

        # ── Format 2: legacy flightList / flights / itineraries ──────────────
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

                outbound_seg_data = item.get("flightSegments") or item.get("segments") or item.get("legs") or item.get("flightLegs") or item.get("outbound") or [item]
                outbound_segments = self._parse_segments_legacy(outbound_seg_data, req.origin, req.destination)
                if not outbound_segments:
                    continue

                inbound = None
                inbound_segments: list[FlightSegment] = []
                if req.return_from:
                    inbound_seg_data = item.get("returnSegments") or item.get("inbound") or item.get("return_legs")
                    if inbound_seg_data:
                        inbound_segments = self._parse_segments_legacy(inbound_seg_data, req.destination, req.origin)
                        if inbound_segments:
                            total_dur = sum(s.duration_seconds for s in inbound_segments)
                            inbound = FlightRoute(segments=inbound_segments, total_duration_seconds=total_dur, stopovers=max(0, len(inbound_segments) - 1))

                total_dur = sum(s.duration_seconds for s in outbound_segments)
                route = FlightRoute(segments=outbound_segments, total_duration_seconds=total_dur, stopovers=max(0, len(outbound_segments) - 1))
                oid = hashlib.md5(f"ytra_{req.origin}{req.destination}{date_str}{return_date_str or ''}{price}{outbound_segments[0].flight_no}".encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"ytra_{oid}", price=round(price, 2), currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route, inbound=inbound,
                    airlines=self._airlines_in_segment_order(outbound_segments + inbound_segments),
                    owner_airline=outbound_segments[0].airline if outbound_segments else "Yatra",
                    booking_url=self._booking_url(req),
                    is_locked=False, source="yatra_ota", source_tier="free",
                ))
            except Exception as e:
                logger.debug("Yatra legacy parse error: %s", e)
        return offers

    def _parse_dom2_poll(
        self,
        data: dict,
        req: FlightSearchRequest,
        date_str: str,
        return_date_str: Optional[str],
    ) -> list[FlightOffer]:
        """Parse Yatra dom2/poll payload.

        Structure:
          fltSchedule[leg_key] → list of itinerary items, each with:
            item["ID"] = fare_id string (links to fareDetails)
            item["OD"][0]["FS"] = list of flight segments
          fareDetails[leg_key][fare_id]["O"]["ADT"] → {ftf, tf, bf, ...}
          fltSchedule["airlineNames"] → {"6E": "IndiGo", ...}

        leg_key format: ORIGDESTYYYYMMDD  e.g. DELBOM20260801
        """
        try:
            flt_schedule = data.get("fltSchedule") or {}
            fare_details = data.get("fareDetails") or {}

            date_nodash = req.date_from.strftime("%Y%m%d")
            out_leg_key = self._resolve_dom2_leg_key(
                fare_details,
                origin=req.origin,
                destination=req.destination,
                date_yyyymmdd=date_nodash,
            )
            if not out_leg_key:
                return []

            airline_names: dict = flt_schedule.get("airlineNames") or {}
            if not isinstance(airline_names, dict):
                airline_names = {}

            out_sched_raw = flt_schedule.get(out_leg_key) or {}
            out_fares = fare_details.get(out_leg_key) or {}
            if not out_sched_raw or not isinstance(out_fares, dict):
                return []

            # fltSchedule leg value can be a list (old dom2) or a dict (aggregatedGds seoint).
            # In dict form, flight entries have "OD" key; metadata entries (scid, lang, etc.) do not.
            if isinstance(out_sched_raw, list):
                out_sched_idx = {
                    item["ID"]: item
                    for item in out_sched_raw
                    if isinstance(item, dict) and "ID" in item
                }
            else:
                out_sched_idx = {
                    (v.get("ID") or k): v
                    for k, v in out_sched_raw.items()
                    if isinstance(v, dict) and "OD" in v
                }

            # Detect combined RT: in aggregatedGds format, each itinerary has both
            # outbound (OD[0]) and inbound (OD[1]) packed into the same flight dict.
            sample_sched = next(iter(out_sched_idx.values()), {})
            is_combined_rt = req.return_from and return_date_str and len(sample_sched.get("OD") or []) >= 2

            booking_url = self._booking_url(req)
            offers: list[FlightOffer] = []

            if is_combined_rt:
                # Combined RT: both legs in one fare entry; total price already covers round-trip.
                seen_rt: dict[tuple, float] = {}
                for fare_id, fare_val in out_fares.items():
                    try:
                        adt_fare = (fare_val.get("O") or {}).get("ADT") or {}
                        price = float(adt_fare.get("ftf") or adt_fare.get("tf") or adt_fare.get("bf") or 0)
                        if price <= 0:
                            continue
                        sched = out_sched_idx.get(fare_id)
                        if not sched:
                            continue
                        out_segs = self._parse_dom2_fs_segments(sched, req.origin, req.destination, airline_names, od_index=0)
                        in_segs = self._parse_dom2_fs_segments(sched, req.destination, req.origin, airline_names, od_index=1)
                        if not out_segs:
                            continue
                        legs_key = (
                            tuple((s.flight_no, s.origin, s.destination) for s in out_segs)
                            + tuple((s.flight_no, s.origin, s.destination) for s in in_segs)
                        )
                        if legs_key in seen_rt and price >= seen_rt[legs_key]:
                            continue
                        seen_rt[legs_key] = price
                        out_route = FlightRoute(
                            segments=out_segs,
                            total_duration_seconds=sum(s.duration_seconds for s in out_segs),
                            stopovers=max(0, len(out_segs) - 1),
                        )
                        in_route = FlightRoute(
                            segments=in_segs,
                            total_duration_seconds=sum(s.duration_seconds for s in in_segs),
                            stopovers=max(0, len(in_segs) - 1),
                        ) if in_segs else None
                        all_airlines = self._airlines_in_segment_order(out_segs + in_segs)
                        oid = hashlib.md5(
                            f"ytra_d2rt_{req.origin}{req.destination}{date_str}{return_date_str}{price}{out_segs[0].flight_no}".encode()
                        ).hexdigest()[:12]
                        offers.append(FlightOffer(
                            id=f"ytra_{oid}", price=round(price, 2), currency="INR",
                            price_formatted=f"{price:.2f} INR",
                            outbound=out_route, inbound=in_route,
                            airlines=all_airlines,
                            owner_airline=out_segs[0].airline if out_segs else "Yatra",
                            booking_url=booking_url,
                            is_locked=False, source="yatra_ota", source_tier="free",
                        ))
                    except Exception as e:
                        logger.debug("Yatra dom2 combined-rt fare_id=%s: %s", fare_id, e)
                return offers

            # Parse outbound options — cheapest fare per unique itinerary (legs sequence)
            # Multiple fare classes (Saver/Flexi/etc.) exist per flight; keep only the cheapest.
            out_best: dict[tuple, tuple[float, list[FlightSegment]]] = {}
            for fare_id, fare_val in out_fares.items():
                try:
                    adt_fare = (fare_val.get("O") or {}).get("ADT") or {}
                    price = float(adt_fare.get("ftf") or adt_fare.get("tf") or adt_fare.get("bf") or 0)
                    if price <= 0:
                        continue
                    sched = out_sched_idx.get(fare_id)
                    if not sched:
                        continue
                    segs = self._parse_dom2_fs_segments(sched, req.origin, req.destination, airline_names)
                    if not segs:
                        continue
                    legs_key = tuple((s.flight_no, s.origin, s.destination) for s in segs)
                    if legs_key not in out_best or price < out_best[legs_key][0]:
                        out_best[legs_key] = (price, segs)
                except Exception as e:
                    logger.debug("Yatra dom2 out fare_id=%s: %s", fare_id, e)

            if not out_best:
                return []

            out_options: list[tuple[float, list[FlightSegment], str]] = [
                (price, segs, segs[0].flight_no[:2] if len(segs[0].flight_no) >= 2 else "")
                for price, segs in out_best.values()
            ]

            if not req.return_from or not return_date_str:
                # One-way
                for price, segs, _ in sorted(out_options, key=lambda x: x[0]):
                    total_dur = sum(s.duration_seconds for s in segs)
                    route = FlightRoute(segments=segs, total_duration_seconds=total_dur, stopovers=max(0, len(segs) - 1))
                    oid = hashlib.md5(f"ytra_d2_{req.origin}{req.destination}{date_str}{price}{segs[0].flight_no}".encode()).hexdigest()[:12]
                    offers.append(FlightOffer(
                        id=f"ytra_{oid}", price=round(price, 2), currency="INR",
                        price_formatted=f"{price:.2f} INR",
                        outbound=route, inbound=None,
                        airlines=self._airlines_in_segment_order(segs),
                        owner_airline=segs[0].airline if segs else "Yatra",
                        booking_url=booking_url,
                        is_locked=False, source="yatra_ota", source_tier="free",
                    ))
                return offers

            # Round-trip: parse inbound leg
            ret_nodash = req.return_from.strftime("%Y%m%d")
            in_leg_key = self._resolve_dom2_leg_key(
                fare_details,
                origin=req.destination,
                destination=req.origin,
                date_yyyymmdd=ret_nodash,
            )
            if not in_leg_key:
                return []
            in_sched_list = flt_schedule.get(in_leg_key) or []
            in_fares = fare_details.get(in_leg_key) or {}

            if not in_sched_list or not isinstance(in_fares, dict):
                return []

            in_sched_idx = {
                item["ID"]: item
                for item in in_sched_list
                if isinstance(item, dict) and "ID" in item
            }

            in_best: dict[tuple, tuple[float, list[FlightSegment]]] = {}
            in_by_airline: dict[str, tuple[float, list[FlightSegment]]] = {}
            for fare_id, fare_val in in_fares.items():
                try:
                    adt_fare = (fare_val.get("O") or {}).get("ADT") or {}
                    price = float(adt_fare.get("ftf") or adt_fare.get("tf") or adt_fare.get("bf") or 0)
                    if price <= 0:
                        continue
                    sched = in_sched_idx.get(fare_id)
                    if not sched:
                        continue
                    segs = self._parse_dom2_fs_segments(sched, req.destination, req.origin, airline_names)
                    if not segs:
                        continue
                    legs_key = tuple((s.flight_no, s.origin, s.destination) for s in segs)
                    if legs_key not in in_best or price < in_best[legs_key][0]:
                        in_best[legs_key] = (price, segs)
                except Exception as e:
                    logger.debug("Yatra dom2 in fare_id=%s: %s", fare_id, e)

            if not in_best:
                return []

            in_options: list[tuple[float, list[FlightSegment], str]] = [
                (price, segs, segs[0].flight_no[:2] if len(segs[0].flight_no) >= 2 else "")
                for price, segs in in_best.values()
            ]

            for price, segs, ac in in_options:
                if ac not in in_by_airline or price < in_by_airline[ac][0]:
                    in_by_airline[ac] = (price, segs)

            cheapest_in_price, cheapest_in_segs = min(in_options, key=lambda x: x[0])[:2]

            for out_price, out_segs, out_ac in sorted(out_options, key=lambda x: x[0]):
                in_price, in_segs = in_by_airline.get(out_ac, (cheapest_in_price, cheapest_in_segs))
                combined = round(out_price + in_price, 2)
                out_route = FlightRoute(
                    segments=out_segs,
                    total_duration_seconds=sum(s.duration_seconds for s in out_segs),
                    stopovers=max(0, len(out_segs) - 1),
                )
                in_route = FlightRoute(
                    segments=in_segs,
                    total_duration_seconds=sum(s.duration_seconds for s in in_segs),
                    stopovers=max(0, len(in_segs) - 1),
                )
                all_airlines = self._airlines_in_segment_order(out_segs + in_segs)
                oid = hashlib.md5(
                    f"ytra_d2rt_{req.origin}{req.destination}{date_str}{return_date_str}{combined}{out_segs[0].flight_no}".encode()
                ).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"ytra_{oid}", price=combined, currency="INR",
                    price_formatted=f"{combined:.2f} INR",
                    outbound=out_route, inbound=in_route,
                    airlines=all_airlines,
                    owner_airline=out_segs[0].airline if out_segs else "Yatra",
                    booking_url=booking_url,
                    is_locked=False, source="yatra_ota", source_tier="free",
                ))

            return offers
        except Exception as e:
            logger.debug("Yatra dom2 poll parse error: %s", e)
            return []

    def _resolve_dom2_leg_key(
        self,
        fare_details: dict,
        origin: str,
        destination: str,
        date_yyyymmdd: str,
    ) -> Optional[str]:
        """Resolve Yatra dom2 fareDetails leg key.

        Normally keys are ORGDESTYYYYMMDD (e.g. DELBOM20260801), but Yatra can
        substitute nearby airports (e.g. NMI) depending on inventory/UI state.
        """
        exact = f"{origin}{destination}{date_yyyymmdd}"
        if exact in fare_details:
            return exact

        keys = [k for k in fare_details.keys() if isinstance(k, str)]
        candidates = [k for k in keys if k.endswith(date_yyyymmdd) and len(k) >= 14]
        if not candidates:
            return None

        # Score candidates to tolerate nearby-airport substitutions (e.g., BOM <-> NMI)
        # while still preferring exact route matches.
        ranked: list[tuple[int, str]] = []
        for k in candidates:
            src = k[:3]
            dst = k[3:6]
            score = 0
            if src == origin:
                score += 4
            if dst == destination:
                score += 3
            ranked.append((score, k))

        ranked.sort(key=lambda t: t[0], reverse=True)
        best_score, best_key = ranked[0]
        return best_key if best_score > 0 else None

    def _parse_dom2_fs_segments(
        self,
        sched_item: dict,
        default_origin: str,
        default_dest: str,
        airline_names: dict,
        od_index: Optional[int] = None,
    ) -> list[FlightSegment]:
        """Parse FS segments from a dom2 fltSchedule itinerary item.

        FS fields: dac, aac, ac, fl, ddt (date), dd (time), adt (date), ad (time), dum (minutes) / du (HHMM).
        od_index: if set, only parse that OD entry (0=outbound, 1=inbound for combined-RT).
        """
        segments: list[FlightSegment] = []
        od_list = sched_item.get("OD") or []
        if od_index is not None:
            od_list = od_list[od_index : od_index + 1]
        for od in od_list:
            for seg in (od.get("FS") or []):
                try:
                    ac = seg.get("ac") or ""
                    airline_name = airline_names.get(ac) or seg.get("acn") or ac
                    fl = seg.get("fl") or ""
                    flight_no = f"{ac}{fl}"
                    dac = seg.get("dac") or default_origin
                    aac = seg.get("aac") or default_dest

                    # dom2 FS splits date (ddt/adt) and time (dd/ad) into separate fields
                    ddt = seg.get("ddt") or ""
                    dd = seg.get("dd") or ""
                    adt_date = seg.get("adt") or ""
                    ad = seg.get("ad") or ""
                    dep_dt = _parse_dt(f"{ddt}T{dd}:00" if ddt and dd else ddt)
                    arr_dt = _parse_dt(f"{adt_date}T{ad}:00" if adt_date and ad else adt_date)

                    dum = seg.get("dum")
                    du = str(seg.get("du") or "")
                    if dum:
                        dur_s = int(dum) * 60
                    elif du.isdigit() and len(du) == 4:
                        # du is HHMM string e.g. "0545" = 5h 45m
                        dur_s = int(du[:2]) * 3600 + int(du[2:]) * 60
                    elif dep_dt != datetime(2000, 1, 1) and arr_dt != datetime(2000, 1, 1):
                        dur_s = max(0, int((arr_dt - dep_dt).total_seconds()))
                    else:
                        dur_s = 0

                    segments.append(FlightSegment(
                        airline=airline_name, flight_no=flight_no,
                        origin=dac, destination=aac,
                        departure=dep_dt, arrival=arr_dt,
                        duration_seconds=dur_s,
                    ))
                except Exception as e:
                    logger.debug("Yatra dom2 FS seg error: %s", e)
        return segments

    def _parse_lowest_fare(self, data: dict, req: FlightSearchRequest, date_str: str, return_date_str: Optional[str] = None) -> list[FlightOffer]:
        """Parse lowest-fare-service API response: {"day": {"YYYY-MM-DD": {"af": {airline: {tf, ow: [...]}}}}}"""
        offers: list[FlightOffer] = []
        day_data = data.get("day", {})

        # Outbound: parse req.date_from entries
        outbound_day = day_data.get(date_str)
        if not outbound_day or not isinstance(outbound_day.get("af"), dict):
            return offers

        # Inbound: parse req.return_from entries (round-trip)
        inbound_day = day_data.get(return_date_str) if return_date_str else None

        trip_type = "R" if req.return_from else "O"
        booking_url = self._booking_url(req)

        for airline_code, entry in outbound_day["af"].items():
            try:
                price = float(entry.get("tf") or entry.get("bf") or 0)
                if price <= 0:
                    continue

                ow_segs = entry.get("ow") or entry.get("segments") or []
                outbound_segments = self._parse_yatra_segments(ow_segs, req.origin, req.destination)
                if not outbound_segments:
                    continue

                inbound = None
                inbound_segments: list[FlightSegment] = []
                if req.return_from and inbound_day and isinstance(inbound_day.get("af"), dict):
                    inbound_entry = inbound_day["af"].get(airline_code) or next(iter(inbound_day["af"].values()), None)
                    if inbound_entry:
                        rt_segs = inbound_entry.get("rt") or inbound_entry.get("ow") or inbound_entry.get("segments") or []
                        inbound_segments = self._parse_yatra_segments(rt_segs, req.destination, req.origin)
                        if inbound_segments:
                            total_dur = sum(s.duration_seconds for s in inbound_segments)
                            inbound = FlightRoute(segments=inbound_segments, total_duration_seconds=total_dur, stopovers=max(0, len(inbound_segments) - 1))

                total_dur = sum(s.duration_seconds for s in outbound_segments)
                route = FlightRoute(segments=outbound_segments, total_duration_seconds=total_dur, stopovers=max(0, len(outbound_segments) - 1))
                oid = hashlib.md5(f"ytra_{req.origin}{req.destination}{date_str}{return_date_str or ''}{price}{airline_code}".encode()).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"ytra_{oid}", price=round(price, 2), currency="INR",
                    price_formatted=f"{price:.2f} INR",
                    outbound=route, inbound=inbound,
                    airlines=self._airlines_in_segment_order(outbound_segments + inbound_segments),
                    owner_airline=outbound_segments[0].airline if outbound_segments else airline_code,
                    booking_url=booking_url,
                    is_locked=False, source="yatra_ota", source_tier="free",
                ))
            except Exception as e:
                logger.debug("Yatra lowest-fare parse error airline=%s: %s", airline_code, e)

        return offers

    def _parse_yatra_segments(self, segs: list, default_origin: str, default_dest: str) -> list[FlightSegment]:
        """Parse Yatra lowest-fare-service segment format: {dac, aac, fl, ac, an, ddt, adt}"""
        segments: list[FlightSegment] = []
        for seg in (segs if isinstance(segs, list) else []):
            try:
                carrier = seg.get("ac") or ""
                airline_name = seg.get("an") or carrier
                if str(airline_name).strip().upper() == "NA" and carrier:
                    airline_name = carrier
                flight_no = f"{carrier}{seg.get('fl', '')}"
                dep_airport = seg.get("dac") or default_origin
                arr_airport = seg.get("aac") or default_dest
                dep_dt = _parse_dt(seg.get("ddt") or seg.get("departureDateTime") or "")
                arr_dt = _parse_dt(seg.get("adt") or seg.get("arrivalDateTime") or "")
                dur_s = max(0, int((arr_dt - dep_dt).total_seconds())) if dep_dt != datetime(2000, 1, 1) and arr_dt != datetime(2000, 1, 1) else 0
                segments.append(FlightSegment(
                    airline=airline_name, flight_no=flight_no,
                    origin=dep_airport, destination=arr_airport,
                    departure=dep_dt, arrival=arr_dt,
                    duration_seconds=dur_s,
                ))
            except Exception as e:
                logger.debug("Yatra segment parse error: %s", e)
        return segments

    def _parse_segments_legacy(self, seg_data: Any, default_origin: str, default_dest: str) -> list[FlightSegment]:
        """Parse legacy segment format with verbose field names."""
        segments: list[FlightSegment] = []
        for seg in (seg_data if isinstance(seg_data, list) else [seg_data]):
            try:
                carrier = seg.get("airlineCode") or seg.get("airline") or seg.get("carrier") or seg.get("al") or ""
                airline_name = seg.get("airlineName") or seg.get("alName") or carrier
                flight_no = seg.get("flightNo") or seg.get("flightNumber") or seg.get("fltNo") or ""
                dep_airport = seg.get("origin") or seg.get("departureAirport") or seg.get("org") or default_origin
                arr_airport = seg.get("destination") or seg.get("arrivalAirport") or seg.get("dest") or default_dest
                dep_dt = _parse_dt(seg.get("departureDateTime") or seg.get("departure") or seg.get("depTime") or seg.get("departTime") or "")
                arr_dt = _parse_dt(seg.get("arrivalDateTime") or seg.get("arrival") or seg.get("arrTime") or seg.get("arrivalTime") or "")
                dur = seg.get("duration") or seg.get("eft") or seg.get("durationMinutes") or 0
                segments.append(FlightSegment(
                    airline=airline_name, flight_no=f"{carrier}{flight_no}",
                    origin=dep_airport, destination=arr_airport,
                    departure=dep_dt, arrival=arr_dt,
                    duration_seconds=int(dur) * 60 if dur else 0,
                ))
            except Exception as e:
                logger.debug("Yatra legacy segment error: %s", e)
        return segments

    def _booking_url(self, req: FlightSearchRequest) -> str:
        trip_type = "R" if req.return_from else "O"
        url = (
            f"{_FLIGHT_API}/air-search-ui/seodom/trigger"
            f"?type={trip_type}&viewName=normal&flexi=0&noOfSegments=1"
            f"&ADT={req.adults or 1}&CHD={req.children or 0}&INF={req.infants or 0}"
            f"&class=Economy&source=seo&hb=0"
            f"&origin={req.origin}&destination={req.destination}"
            f"&flight_depart_date={req.date_from.strftime('%d/%m/%Y')}"
        )
        if req.return_from:
            url += f"&flight_return_date={req.return_from.strftime('%d/%m/%Y')}"
        return url

    def _airlines_in_segment_order(self, segments: list[FlightSegment]) -> list[str]:
        """Return unique airline names in the same order they appear in segments."""
        airlines: list[str] = []
        for seg in segments:
            name = seg.airline
            if name and name not in airlines:
                airlines.append(name)
        return airlines

    def _empty(self, req: FlightSearchRequest, search_params: Optional[dict[str, Any]] = None) -> FlightSearchResponse:
        h = hashlib.md5(f"yatra{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="INR", offers=[], total_results=0,
            search_params=search_params or {},
        )

    async def _fetch_split_rt(
        self,
        page,
        req: FlightSearchRequest,
        api_host: str,
        date_str: str,
        return_date_str: str,
    ) -> list[FlightOffer]:
        """Call lowest-fare-service with split=true to get true RT fares in one request.

        Yatra returns a 2-element JSON array: [outbound_day_data, inbound_day_data].
        Each element has the standard {'day': {'YYYY-MM-DD': {'af': {...}}}} format.
        Date params must be in DD-MM-YYYY format (URL uses dashes, response keys use hyphens YYYY-MM-DD).
        """
        ts = int(time.time() * 1000)
        # Yatra's lowest-fare-service expects dates in DD-MM-YYYY format
        out_dt = req.date_from.strftime("%d-%m-%Y")
        in_dt = req.return_from.strftime("%d-%m-%Y")
        url = (
            f"{_FLIGHT_API}/lowest-fare-service/{api_host}/get-fare"
            f"?origin={req.origin}&destination={req.destination}"
            f"&from={out_dt}&to={in_dt}"
            f"&tripType=R&airlines=all&split=true"
            f"&_i={ts}&src=srp"
        )
        logger.debug("Yatra split RT fetch: %s", url)
        try:
            # Use Playwright's APIRequestContext to bypass CORS — the page may be on
            # www.yatra.com while the endpoint is on flight.yatra.com (different origin).
            # context.request shares cookies with the browser context, so session auth works.
            api_ctx = page.context.request
            api_response = await api_ctx.get(
                url,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.yatra.com/",
                },
            )
            raw = await api_response.text()
            if not raw:
                return []
            data = _decode_json_like(raw)
            if not isinstance(data, list):
                logger.debug("Yatra split RT: expected array, got %s", type(data).__name__)
                return []
            if len(data) < 2:
                logger.debug("Yatra split RT: array length %d < 2", len(data))
                return []
            outbound_data = data[0] if isinstance(data[0], dict) else {}
            inbound_data = data[1] if isinstance(data[1], dict) else {}
            offers = self._parse_split_rt(outbound_data, inbound_data, req, date_str, return_date_str)
            logger.info(
                "Yatra split RT: parsed %d offers (out=%s\u2192%s %s, in=%s\u2192%s %s)",
                len(offers), req.origin, req.destination, date_str,
                req.destination, req.origin, return_date_str,
            )
            return offers
        except Exception as e:
            logger.debug("Yatra split RT fetch error: %s", e)
            return []

    def _parse_split_rt(
        self,
        outbound_data: dict,
        inbound_data: dict,
        req: FlightSearchRequest,
        date_str: str,
        return_date_str: str,
    ) -> list[FlightOffer]:
        """Build combined RT offers from the two halves of a split=true response."""
        offers: list[FlightOffer] = []
        booking_url = self._booking_url(req)

        out_day = (outbound_data.get("day") or {}).get(date_str, {})
        in_day = (inbound_data.get("day") or {}).get(return_date_str, {})

        if not isinstance(out_day.get("af"), dict) or not isinstance(in_day.get("af"), dict):
            logger.debug(
                "Yatra split RT parse: missing af dict — out_keys=%s in_keys=%s",
                list(out_day.keys())[:5] if isinstance(out_day, dict) else out_day,
                list(in_day.keys())[:5] if isinstance(in_day, dict) else in_day,
            )
            return offers

        # Parse all inbound options keyed by airline code
        inbound_by_airline: dict[str, tuple[float, list[FlightSegment]]] = {}
        for airline_code, entry in in_day["af"].items():
            try:
                price = float(entry.get("tf") or entry.get("bf") or 0)
                if price <= 0:
                    continue
                # Use "rt" key for return segments, fall back to "ow"
                segs_raw = entry.get("rt") or entry.get("ow") or entry.get("segments") or []
                segments = self._parse_yatra_segments(segs_raw, req.destination, req.origin)
                if segments:
                    inbound_by_airline[airline_code] = (price, segments)
            except Exception as e:
                logger.debug("Yatra split RT inbound airline=%s: %s", airline_code, e)

        if not inbound_by_airline:
            return offers

        cheapest_in_code = min(inbound_by_airline, key=lambda k: inbound_by_airline[k][0])
        cheapest_in_price, cheapest_in_segs = inbound_by_airline[cheapest_in_code]

        for airline_code, entry in out_day["af"].items():
            try:
                out_price = float(entry.get("tf") or entry.get("bf") or 0)
                if out_price <= 0:
                    continue
                segs_raw = entry.get("ow") or entry.get("segments") or []
                out_segs = self._parse_yatra_segments(segs_raw, req.origin, req.destination)
                if not out_segs:
                    continue

                # Prefer same-airline inbound; otherwise pair with cheapest
                in_price, in_segs = inbound_by_airline.get(
                    airline_code, (cheapest_in_price, cheapest_in_segs)
                )
                combined_price = round(out_price + in_price, 2)

                out_route = FlightRoute(
                    segments=out_segs,
                    total_duration_seconds=sum(s.duration_seconds for s in out_segs),
                    stopovers=max(0, len(out_segs) - 1),
                )
                in_route = FlightRoute(
                    segments=in_segs,
                    total_duration_seconds=sum(s.duration_seconds for s in in_segs),
                    stopovers=max(0, len(in_segs) - 1),
                )
                all_airlines = self._airlines_in_segment_order(out_segs + in_segs)
                oid = hashlib.md5(
                    f"ytra_srt_{req.origin}{req.destination}{date_str}{return_date_str}{combined_price}{airline_code}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"ytra_{oid}",
                    price=combined_price,
                    currency="INR",
                    price_formatted=f"{combined_price:.2f} INR",
                    outbound=out_route,
                    inbound=in_route,
                    airlines=all_airlines,
                    owner_airline=out_segs[0].airline if out_segs else airline_code,
                    booking_url=booking_url,
                    is_locked=False,
                    source="yatra_ota",
                    source_tier="free",
                ))
            except Exception as e:
                logger.debug("Yatra split RT outbound airline=%s: %s", airline_code, e)

        return offers

    async def _search_round_trip(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Round-trip: run outbound + reversed-inbound as two one-way searches, then combine."""
        date_str = req.date_from.strftime("%Y-%m-%d")
        return_date_str = req.return_from.strftime("%Y-%m-%d")
        booking_url = self._booking_url(req)

        out_req = req.model_copy(update={"return_from": None, "return_to": None})
        in_req = req.model_copy(update={
            "origin": req.destination,
            "destination": req.origin,
            "date_from": req.return_from,
            "return_from": None,
            "return_to": None,
        })

        out_resp, in_resp = await asyncio.gather(
            self.search_flights(out_req),
            self.search_flights(in_req),
        )

        out_offers = out_resp.offers
        in_offers = in_resp.offers

        if not out_offers or not in_offers:
            return self._empty(req, {
                "yatra_mode": "round_trip",
                "yatra_roundtrip_mode": "fallback_pairing",
                "yatra_status": "no_offers",
            })

        # For cheapest-combo behavior, always pair each outbound with the cheapest inbound leg.
        cheapest_in = min(in_offers, key=lambda o: o.price)

        combined: list[FlightOffer] = []
        for out_offer in out_offers:
            in_offer = cheapest_in
            inbound_route = in_offer.outbound if in_offer else None
            combined_price = round(out_offer.price + (in_offer.price if in_offer else 0), 2)
            all_airlines = self._airlines_in_segment_order(
                list(out_offer.outbound.segments) + (list(inbound_route.segments) if inbound_route else [])
            )
            oid = hashlib.md5(
                f"ytra_rt_{req.origin}{req.destination}{date_str}{return_date_str}{combined_price}{out_offer.owner_airline}".encode()
            ).hexdigest()[:12]
            combined.append(FlightOffer(
                id=f"ytra_{oid}", price=combined_price, currency="INR",
                price_formatted=f"{combined_price:.2f} INR",
                outbound=out_offer.outbound, inbound=inbound_route,
                airlines=all_airlines, owner_airline=out_offer.owner_airline,
                booking_url=booking_url, is_locked=False,
                source="yatra_ota", source_tier="free",
            ))

        combined.sort(key=lambda o: o.price)
        sh = hashlib.md5(f"yatra{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
            currency=combined[0].currency if combined else "INR",
            offers=combined, total_results=len(combined),
            search_params={
                "yatra_mode": "round_trip",
                "yatra_roundtrip_mode": "fallback_pairing",
                "yatra_status": "ok",
                "yatra_outbound_offer_count": len(out_offers),
                "yatra_inbound_offer_count": len(in_offers),
            },
        )
