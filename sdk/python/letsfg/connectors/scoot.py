"""
Scoot Playwright scraper — CDP Chrome + API interception via Navitaire Angular SPA.

Scoot (IATA: TR) is Singapore Airlines' low-cost subsidiary operating from SIN.
Uses a modern Navitaire Angular 20 booking engine at booking.flyscoot.com.
Protected by Akamai Bot Manager — requires real Chrome via CDP bypass.

Strategy:
1. Launch real Chrome via subprocess + connect via CDP (port 9448) to bypass
   Akamai bot challenge that blocks Playwright's bundled Chromium
2. Visit www.flyscoot.com/en first — Akamai warmup grants clearance cookies
3. Navigate to booking.flyscoot.com — Angular SPA loads with search form
4. Accept cookie consent banner
5. Fill search form via #originStation, #destinationStation, #departureDate
6. Set one-way mode, click "Let's Go!" submit button
7. Intercept the flight availability API response
8. Parse Navitaire Trips[].Flights[] structure with fare bundles for prices
9. Fallback: DOM extraction from flight result cards

Key API structure (Mar 2026):
  Session: GET /api/v1/account/anonymous  → JWT token
  Auth headers: Authorization: <JWT>, x-scoot-appsource: IBE-WEB
  Stations: GET /api/flights/resource/stations?cultureCode=en-sg
  Search: intercepted after form submit (POST availability endpoint)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime, date, timedelta
from typing import Any, Optional

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied, get_httpx_proxy_url
from .airline_routes import city_match_set

logger = logging.getLogger(__name__)

# ── Sputnik API (EveryMundo) — primary fast path ──
_SPUTNIK_URL = "https://openair-california.airtrfx.com/airfare-sputnik-service/v3/tr/fares/grouped-routes"
_SPUTNIK_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_SPUTNIK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://mm-prerendering-static-prod.airtrfx.com",
    "Referer": "https://mm-prerendering-static-prod.airtrfx.com/",
    "em-api-key": _SPUTNIK_KEY,
}
_SPUTNIK_MARKETS = ["SG", "AU", "MY", "TH", "JP", "KR", "IN"]

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-SG", "en-US", "en-GB", "en-AU"]
_TIMEZONES = [
    "Asia/Singapore", "Asia/Kuala_Lumpur", "Asia/Bangkok",
    "Asia/Tokyo", "Australia/Sydney",
]

_MAX_ATTEMPTS = 2
_DEBUG_PORT = 9448
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".scoot_chrome_data")

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real headed Chrome via CDP (Akamai blocks headless).

    Uses a persistent user-data-dir so Akamai clearance persists across runs.
    """
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

        # Try connecting to existing Chrome on the port first
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
            _pw_instance = pw
            logger.info("Scoot: connected to existing Chrome on port %d", _DEBUG_PORT)
            return _browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        # Launch Chrome HEADED (no --headless) — Akamai blocks headless Chrome.
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
            "--window-size=1366,768",
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        logger.info("Scoot: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
        return _browser


class ScootConnectorClient:
    """Scoot Playwright scraper — CDP Chrome + Navitaire API interception."""

    def __init__(self, timeout: float = 50.0):
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
        if ob_result.offers:
            segs = ob_result.offers[0].outbound.segments if ob_result.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(
                        anc_origin, anc_dest,
                        req.date_from.isoformat() if hasattr(req.date_from, 'isoformat') else str(req.date_from),
                        req.adults, ob_result.currency or req.currency,
                    ),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s->%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error: %s", _anc_err)
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Fast path: Sputnik API (~1s, no browser)
        sputnik_fallback: Optional[FlightSearchResponse] = None
        sputnik_offers = await self._try_sputnik(req)
        if sputnik_offers:
            _td = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
            sputnik_offers = [o for o in sputnik_offers if o.outbound and o.outbound.segments and o.outbound.segments[0].departure.date() == _td]
            sputnik_offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
            h = hashlib.md5(f"tr{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
            sputnik_fallback = FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=sputnik_offers[0].currency,
                offers=sputnik_offers,
                total_results=len(sputnik_offers),
            )
            if self._offers_have_bundle_metadata(sputnik_offers):
                return sputnik_fallback
            logger.info(
                "Scoot: Sputnik returned %d offers without bundle metadata, continuing to Navitaire search",
                len(sputnik_offers),
            )

        # Slow path: CDP Chrome + Navitaire API interception
        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(req, t0)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning("Scoot: attempt %d/%d got no results", attempt, _MAX_ATTEMPTS)
            except Exception as e:
                logger.warning("Scoot: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        if sputnik_fallback is not None:
            return sputnik_fallback
        return self._empty(req)

    @staticmethod
    def _offers_have_bundle_metadata(offers: list[FlightOffer]) -> bool:
        for offer in offers:
            conditions = offer.conditions or {}
            if any(
                conditions.get(key)
                for key in [
                    "fare_brand",
                    "bundle_name",
                    "bundle_subheader",
                    "service_bundle_code",
                    "outbound_fare_brand",
                    "inbound_fare_brand",
                ]
            ):
                return True
        return False

    async def _try_sputnik(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fast path: EveryMundo Sputnik grouped-routes API."""
        try:
            dt = req.date_from
            if isinstance(dt, datetime):
                dt = dt.date()
            elif not isinstance(dt, date):
                dt = datetime.strptime(str(dt), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            dt = date.today() + timedelta(days=30)

        start = dt - timedelta(days=3)
        end = dt + timedelta(days=30)

        payload = {
            "markets": _SPUTNIK_MARKETS,
            "languageCode": "en",
            "dataExpirationWindow": "7d",
            "datePattern": "dd MMM yy (E)",
            "outputCurrencies": ["SGD", "USD"],
            "departure": {"start": start.isoformat(), "end": end.isoformat()},
            "budget": {"maximum": None},
            "passengers": {"adults": max(1, req.adults or 1)},
            "travelClasses": [{"M": "ECONOMY", "W": "PREMIUM_ECONOMY", "C": "BUSINESS", "F": "FIRST"}.get(req.cabin_class or "M", "ECONOMY")],
            "flightType": "ROUND_TRIP" if req.return_from else "ONE_WAY",
            "flexibleDates": True,
            "faresPerRoute": "10",
            "trfxRoutes": True,
            "routesLimit": 500,
            "sorting": [{"popularity": "DESC"}],
            "airlineCode": "tr",
        }

        try:
            async with httpx.AsyncClient(
                timeout=15, headers=_SPUTNIK_HEADERS,
                proxy=get_httpx_proxy_url(),
            ) as client:
                r = await client.post(_SPUTNIK_URL, json=payload)
                if r.status_code != 200:
                    logger.info("Scoot Sputnik: HTTP %d", r.status_code)
                    return []
                data = r.json()
                if not isinstance(data, list):
                    return []
        except Exception as e:
            logger.info("Scoot Sputnik error: %s", e)
            return []

        origin_set = city_match_set(req.origin)
        dest_set = city_match_set(req.destination)

        offers = []
        for route in data:
            for fare in route.get("fares") or []:
                orig = (fare.get("originAirportCode") or route.get("origin") or "").upper()
                dest = (fare.get("destinationAirportCode") or route.get("destination") or "").upper()
                if dest not in dest_set and orig not in origin_set:
                    continue

                price = fare.get("totalPrice") or fare.get("usdTotalPrice")
                if not price or float(price) <= 0:
                    continue
                if fare.get("redemption"):
                    continue

                price_f = round(float(price), 2)
                currency = fare.get("currencyCode") or "SGD"
                dep_str = (fare.get("departureDate") or "")[:10]
                ret_str = (fare.get("returnDate") or "")[:10]
                cabin = (fare.get("farenetTravelClass") or "ECONOMY").lower()

                dep_dt = datetime(2000, 1, 1)
                if dep_str:
                    try:
                        dep_dt = datetime.strptime(dep_str, "%Y-%m-%d")
                    except ValueError:
                        pass

                seg = FlightSegment(
                    airline="TR", airline_name="Scoot", flight_no="",
                    origin=orig, destination=dest,
                    origin_city=fare.get("originCity") or "",
                    destination_city=fare.get("destinationCity") or "",
                    departure=dep_dt, arrival=dep_dt,
                    duration_seconds=0, cabin_class=cabin,
                )
                outbound = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

                inbound = None
                if ret_str:
                    try:
                        ret_dt = datetime.strptime(ret_str, "%Y-%m-%d")
                    except ValueError:
                        ret_dt = dep_dt
                    ret_seg = FlightSegment(
                        airline="TR", airline_name="Scoot", flight_no="",
                        origin=dest, destination=orig,
                        origin_city=fare.get("destinationCity") or "",
                        destination_city=fare.get("originCity") or "",
                        departure=ret_dt, arrival=ret_dt,
                        duration_seconds=0, cabin_class=cabin,
                    )
                    inbound = FlightRoute(segments=[ret_seg], total_duration_seconds=0, stopovers=0)

                ret_token = f"_{ret_str}" if ret_str else ""
                fid = hashlib.md5(
                    f"tr_{orig}_{dest}_{dep_str}{ret_token}_{price_f}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"tr_{fid}",
                    price=price_f,
                    currency=currency,
                    price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                    outbound=outbound,
                    inbound=inbound,
                    airlines=["Scoot"],
                    owner_airline="TR",
                    booking_url="https://booking.flyscoot.com",
                    is_locked=False,
                    source="scoot_direct",
                    source_tier="free",
                    conditions={
                        "trip_type": (fare.get("flightType") or "ROUND_TRIP").lower().replace("_", "-"),
                        "cabin": str(fare.get("formattedTravelClass") or cabin),
                        "fare_note": "Published fare from Scoot fare module",
                    },
                ))

        logger.info("Scoot Sputnik %s→%s: %d offers", req.origin, req.destination, len(offers))
        return offers

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[list[FlightOffer]]:
        browser = await _get_browser()

        # CDP browsers use default context — reuse existing tab to keep cookies
        is_cdp = hasattr(browser, 'contexts') and browser.contexts
        if is_cdp:
            context = browser.contexts[0]
            # Close extra tabs, use the first page (avoids Akamai issues with new tabs)
            for p in context.pages[1:]:
                try:
                    await p.close()
                except Exception:
                    pass
            if context.pages:
                page = context.pages[0]
            else:
                page = await context.new_page()
                await auto_block_if_proxied(page)
        else:
            context = await browser.new_context(
                viewport=random.choice(_VIEWPORTS),
                locale=random.choice(_LOCALES),
                timezone_id=random.choice(_TIMEZONES),
                service_workers="block",
            )
            page = await context.new_page()
            await auto_block_if_proxied(page)

        try:
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            # API response capture — only listen after search button is clicked
            captured_data: dict = {}
            api_event = asyncio.Event()
            search_clicked = {"ready": False}

            async def on_response(response):
                if not search_clicked["ready"]:
                    return
                try:
                    url = response.url.lower()
                    status = response.status
                    if status != 200:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    # Match search/availability endpoints, skip lowfare (both /estimate and direct)
                    if "lowfare" in url:
                        return
                    if any(k in url for k in [
                        "availability", "/api/flights/search",
                        "flightsearch", "search/flights", "/api/v1/availability",
                        "/api/nsk/", "trips", "air-bounds",
                    ]):
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            captured_data["search"] = data
                            captured_data["search_url"] = response.url
                            api_event.set()
                            logger.info("Scoot: captured search API from %s",
                                        response.url[:100])
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Akamai warmup via www.flyscoot.com
            logger.info("Scoot: Akamai warmup via www.flyscoot.com")
            try:
                await page.goto("https://www.flyscoot.com/en",
                                wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(1.5)
                await self._dismiss_cookies(page)
            except Exception:
                pass  # main site may timeout — we just need the cookies

            # Step 2: Navigate to booking engine
            logger.info("Scoot: loading booking.flyscoot.com for %s->%s",
                        req.origin, req.destination)
            await page.goto("https://booking.flyscoot.com",
                            wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            # Accept cookies on booking site too
            await self._dismiss_cookies(page)

            # The current site lands on a collapsed mobile-style search shell.
            await self._ensure_search_form_open(page)

            # Wait for SPA to render (use JS to check for visible input — avoids
            # the duplicate-ID trap where wait_for_selector picks the hidden one)
            spa_ready = False
            for _wait_round in range(6):  # up to ~12s total
                spa_ready = await page.evaluate("""() => {
                    const selectors = [
                        'input#originStation',
                        'input[aria-label="origin-input"]',
                        'input[aria-label="destination-input"]',
                    ];
                    return selectors.some(selector =>
                        Array.from(document.querySelectorAll(selector)).some(el =>
                            el.offsetHeight > 0 || el.offsetWidth > 0 || el.getClientRects().length
                        )
                    );
                }""")
                if spa_ready:
                    break
                await self._ensure_search_form_open(page)
                await asyncio.sleep(2)
            if not spa_ready:
                logger.warning("Scoot: search form never appeared")
                return None

            # Step 3: Set one-way (only for OW; RT is default)
            if not req.return_from:
                await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Step 5: Fill origin
            ok = await self._fill_station(page, "#originStation", req.origin)
            if not ok:
                logger.warning("Scoot: origin fill failed for %s", req.origin)
                return None
            await asyncio.sleep(0.5)

            # Step 6: Fill destination
            ok = await self._fill_station(page, "#destinationStation", req.destination)
            if not ok:
                logger.warning("Scoot: destination fill failed for %s", req.destination)
                return None
            await asyncio.sleep(0.5)

            # Step 7: Fill date
            ok = await self._fill_date(page, req.date_from)
            if not ok:
                logger.warning("Scoot: date fill failed")
                return None
            await asyncio.sleep(0.3)

            # Step 7b: Fill return date if RT
            if req.return_from:
                ok = await self._fill_date(page, req.return_from)
                if not ok:
                    logger.warning("Scoot: return date fill failed, continuing anyway")
                await asyncio.sleep(0.3)

            if not req.return_from and not await self._is_one_way_selected(page):
                logger.info("Scoot: one-way selection did not stick after form fill; retrying before submit")
                await self._set_one_way(page)
                await asyncio.sleep(0.5)

            # Step 8: Click search (enable API capture first)
            search_clicked["ready"] = True
            await self._click_search(page)

            # Step 9: Wait for API response
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            select_page_checked = False
            while time.monotonic() < deadline:
                if api_event.is_set():
                    break
                current_url = (page.url or "").lower()
                if "/book/flight/" in current_url and not select_page_checked:
                    select_page_checked = True
                    offers = await self._extract_select_page_offers(page, req)
                    if offers:
                        logger.info("Scoot: parsed select page DOM after search submit")
                        return offers
                await asyncio.sleep(0.75)

            if not api_event.is_set():
                logger.info("Scoot: API intercept timed out, trying DOM extraction")
                if "/book/flight/" in (page.url or "").lower():
                    offers = await self._extract_select_page_offers(page, req)
                else:
                    offers = await self._extract_from_dom(page, req)
                if offers:
                    return offers
                offers = await self._extract_from_page_data(page, req)
                return offers if offers else None

            data = captured_data.get("search")
            if data:
                offers = self._parse_navitaire_response(data, req)
                if offers:
                    return offers

            # Fallback to DOM
            if "/book/flight/" in (page.url or "").lower():
                return await self._extract_select_page_offers(page, req) or None
            return await self._extract_from_dom(page, req) or None

        finally:
            if is_cdp:
                # Don't close the reused tab — just navigate away
                try:
                    await page.goto("about:blank", timeout=5000)
                except Exception:
                    pass
            else:
                await page.close()
                await context.close()

    # ── Cookie / overlay dismissal ──────────────────────────────────────────

    async def _dismiss_cookies(self, page) -> None:
        """Dismiss cookie consent and overlay banners."""
        for selector in [
            "text='Accept all cookies'",
            "button:has-text('Accept all cookies')",
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
        ]:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="onetrust"], [id*="onetrust"], [class*="modal-overlay"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _ensure_search_form_open(self, page) -> None:
        """Expand Scoot's collapsed search shell when the full form is hidden."""
        try:
            expanded = await page.evaluate("""() => {
                const selectors = [
                    'input#originStation',
                    'input[aria-label="origin-input"]',
                    'input[aria-label="destination-input"]',
                ];
                return selectors.some(selector =>
                    Array.from(document.querySelectorAll(selector)).some(el =>
                        el.offsetHeight > 0 || el.offsetWidth > 0 || el.getClientRects().length
                    )
                );
            }""")
            if expanded:
                return

            clicked = await page.evaluate("""() => {
                const selectors = [
                    '[aria-label="destination-field-collapse"]',
                    '[aria-label="home-search-collapse-container"]',
                ];
                for (const selector of selectors) {
                    const elements = document.querySelectorAll(selector);
                    for (const el of elements) {
                        if (el.offsetHeight > 0 || el.offsetWidth > 0 || el.getClientRects().length) {
                            el.click();
                            return selector;
                        }
                    }
                }
                return null;
            }""")
            if clicked:
                logger.info("Scoot: expanded collapsed search shell via %s", clicked)
                await asyncio.sleep(1)
        except Exception as e:
            logger.debug("Scoot: failed to expand collapsed search shell: %s", e)

    # ── One-way toggle ──────────────────────────────────────────────────────

    async def _set_one_way(self, page) -> None:
        """Click One-Way radio/tab if not already selected."""
        if await self._is_one_way_selected(page):
            return

        for _attempt in range(3):
            try:
                opened = await page.evaluate("""() => {
                    const isVisible = (el) => !!(el && (el.offsetHeight || el.offsetWidth || el.getClientRects().length));
                    const triggers = [
                        '[aria-label="mobile-trip-type-trigger"]',
                        '[aria-label="desktop-trip-type-trigger"]',
                    ];
                    for (const selector of triggers) {
                        const trigger = document.querySelector(selector);
                        if (isVisible(trigger)) {
                            trigger.click();
                            return selector;
                        }
                    }
                    return null;
                }""")
                if opened:
                    await asyncio.sleep(0.4)
                    clicked = await page.evaluate("""() => {
                        const isVisible = (el) => !!(el && (el.offsetHeight || el.offsetWidth || el.getClientRects().length));
                        const selectors = [
                            '[aria-label="mobile-trip-type-menu-item-one-way"]',
                            '[aria-label="desktop-trip-type-menu-item-one-way"]',
                            '[aria-label="mobile-trip-type-label-one-way"]',
                            '[aria-label="desktop-trip-type-label-one-way"]',
                        ];
                        for (const selector of selectors) {
                            const option = document.querySelector(selector);
                            if (isVisible(option)) {
                                const clickable = option.closest('[aria-label*="menu-item"]') || option;
                                clickable.click();
                                return selector;
                            }
                        }
                        return null;
                    }""")
                    if clicked:
                        await asyncio.sleep(0.6)
                        if await self._is_one_way_selected(page):
                            return
            except Exception:
                pass

            for label in ["One-way", "One Way", "ONE WAY", "one way"]:
                try:
                    el = page.get_by_text(label, exact=False).first
                    if await el.count() > 0:
                        await el.click(timeout=2000)
                        await asyncio.sleep(0.5)
                        if await self._is_one_way_selected(page):
                            return
                except Exception:
                    continue

        logger.warning("Scoot: one-way toggle did not stick")

    async def _is_one_way_selected(self, page) -> bool:
        try:
            return bool(await page.evaluate(r"""() => {
                const normalize = (text) => (text || '').replace(/\s+/g, ' ').trim().toLowerCase();
                const isVisible = (el) => !!(el && (el.offsetHeight || el.offsetWidth || el.getClientRects().length));
                const triggerSelectors = [
                    '[aria-label="mobile-trip-type-trigger"]',
                    '[aria-label="desktop-trip-type-trigger"]',
                ];
                for (const selector of triggerSelectors) {
                    const trigger = document.querySelector(selector);
                    if (isVisible(trigger) && normalize(trigger.textContent).includes('one-way')) {
                        return true;
                    }
                }
                const oneWayCheck = document.querySelector('[aria-label="mobile-trip-type-check-one-way"], [aria-label="desktop-trip-type-check-one-way"]');
                if (isVisible(oneWayCheck)) {
                    return true;
                }
                const returnInputs = document.querySelectorAll('input[aria-label="return-date-input-roundtrip"], input[aria-label="return-date-input"]');
                const returnVisible = Array.from(returnInputs).some(isVisible);
                return !returnVisible && /one-way/i.test(document.body?.innerText || '');
            }"""))
        except Exception:
            return False

    # ── Station (airport) fill ──────────────────────────────────────────────

    async def _fill_station(self, page, selector: str, iata: str) -> bool:
        """Fill an airport input (#originStation or #destinationStation).

        The Angular SPA duplicates elements (visible + hidden tabs).
        We use JS to find the visible input and interact with it directly.
        """
        try:
            is_origin = "origin" in selector.lower()
            field_id = "originStation" if is_origin else "destinationStation"
            aria_label = "origin-input" if is_origin else "destination-input"

            # Use JS to click the visible input (avoids duplicate-ID ambiguity)
            clicked = await page.evaluate("""(inputSelectors) => {
                for (const selector of inputSelectors) {
                    const inputs = document.querySelectorAll(selector);
                    for (const inp of inputs) {
                        if (inp.offsetHeight > 0 || inp.offsetWidth > 0 || inp.getClientRects().length) {
                            inp.click();
                            inp.focus();
                            return selector;
                        }
                    }
                }
                return null;
            }""", [f"input#{field_id}", f"input[aria-label='{aria_label}']"])
            if not clicked:
                logger.debug("Scoot: no visible input for %s/%s", field_id, aria_label)
                return False
            await asyncio.sleep(0.5)

            # Clear via JS + Angular event dispatch
            await page.evaluate("""(inputSelectors) => {
                for (const selector of inputSelectors) {
                    const inputs = document.querySelectorAll(selector);
                    for (const inp of inputs) {
                        if (inp.offsetHeight > 0 || inp.offsetWidth > 0 || inp.getClientRects().length) {
                            inp.value = '';
                            inp.dispatchEvent(new Event('input', {bubbles: true}));
                            inp.dispatchEvent(new Event('change', {bubbles: true}));
                            return selector;
                        }
                    }
                }
                return null;
            }""", [f"input#{field_id}", f"input[aria-label='{aria_label}']"])
            await asyncio.sleep(0.2)

            # Type IATA code character by character (triggers Angular keypress)
            await page.keyboard.type(iata, delay=100)
            await asyncio.sleep(2.5)

            # Click station suggestion from overlay via JS
            suggestion_clicked = await page.evaluate("""(iata) => {
                const overlays = document.querySelectorAll('.stations-overlay');
                for (const overlay of overlays) {
                    if (overlay.offsetHeight === 0) continue;
                    const byLabel = overlay.querySelector('div[aria-label="' + iata + '"]');
                    if (byLabel && byLabel.offsetHeight > 0) {
                        byLabel.click();
                        return 'aria-label';
                    }
                    const codes = overlay.querySelectorAll('.code');
                    for (const code of codes) {
                        if (code.textContent.trim() === iata && code.offsetHeight > 0) {
                            code.parentElement.click();
                            return 'code-parent';
                        }
                    }
                    const items = overlay.querySelectorAll(
                        'div.current-location, div.station-item, li');
                    for (const item of items) {
                        if (item.textContent.includes(iata) && item.offsetHeight > 0) {
                            item.click();
                            return 'text-match';
                        }
                    }
                }

                const directSelectors = [
                    '[aria-label="' + iata + '"]',
                    '[aria-label*="' + iata + '"]',
                ];
                for (const selector of directSelectors) {
                    const nodes = document.querySelectorAll(selector);
                    for (const node of nodes) {
                        if (node.offsetHeight > 0 || node.offsetWidth > 0 || node.getClientRects().length) {
                            node.click();
                            return selector;
                        }
                    }
                }

                const stationNodes = document.querySelectorAll('[class*="station"], [class*="airport"]');
                for (const node of stationNodes) {
                    if ((node.offsetHeight > 0 || node.offsetWidth > 0 || node.getClientRects().length) &&
                        node.textContent && node.textContent.includes(iata)) {
                        node.click();
                        return 'station-text-match';
                    }
                }
                return null;
            }""", iata)

            if suggestion_clicked:
                logger.info("Scoot: selected station %s via JS %s", iata, suggestion_clicked)
                await asyncio.sleep(0.5)
                return True

            # Fallback: Playwright selectors
            for sel in [
                f".stations-overlay div[aria-label='{iata}']",
                f".stations-overlay :text-is('{iata}')",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("Scoot: selected station %s via %s", iata, sel)
                        return True
                except Exception:
                    continue

            # Last resort: press Enter for top suggestion
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.5)
            logger.info("Scoot: pressed Enter for station %s", iata)
            return True

        except Exception as e:
            logger.warning("Scoot: station fill error for %s: %s", iata, e)
            return False

    # ── Date fill ───────────────────────────────────────────────────────────

    async def _fill_date(self, page, target: datetime, is_return: bool = False) -> bool:
        """Fill the departure date using the ngb-datepicker calendar.

        Calendar uses: div.ngb-dp-month-name ("March 2026"),
        button[aria-label="Next month"], and
        div.ngb-dp-day[role="gridcell"][aria-label="Thursday, April 9, 2026"].
        """
        try:
            # Click the visible date input to open the calendar.
            if is_return:
                selectors = [
                    "input[aria-label='return-date-input-roundtrip']",
                    "input[aria-label='return-date-input']",
                    '#returnDate',
                ]
            else:
                selectors = [
                    "input[aria-label='departure-date-input-one-way']",
                    "input[aria-label='departure-date-input']",
                    '#departureDate',
                ]
            opened = await page.evaluate("""(inputSelectors) => {
                for (const selector of inputSelectors) {
                    const els = document.querySelectorAll(selector);
                    for (const el of els) {
                        if (el.offsetHeight > 0 || el.offsetWidth > 0 || el.getClientRects().length) {
                            el.click();
                            return selector;
                        }
                    }
                }
                return null;
            }""", selectors)
            if not opened:
                logger.warning("Scoot: no visible date input to click (return=%s)", is_return)
                return False
            await asyncio.sleep(1)

            target_date = target.date() if isinstance(target, datetime) else target
            # Build the aria-label for the target day, e.g. "Thursday, April 9, 2026"
            day_label = target_date.strftime("%A, %B ") + str(target_date.day) + target_date.strftime(", %Y")
            target_month_key = target_date.strftime("%Y-%m")

            clicked = False
            for _navigation_attempt in range(14):
                step = await page.evaluate(r"""({ label, targetMonthKey }) => {
                    const isVisible = (el) => !!(el && (el.offsetHeight || el.offsetWidth || el.getClientRects().length));
                    const monthMap = {
                        jan: 1, january: 1,
                        feb: 2, february: 2,
                        mar: 3, march: 3,
                        apr: 4, april: 4,
                        may: 5,
                        jun: 6, june: 6,
                        jul: 7, july: 7,
                        aug: 8, august: 8,
                        sep: 9, sept: 9, september: 9,
                        oct: 10, october: 10,
                        nov: 11, november: 11,
                        dec: 12, december: 12,
                    };
                    const toMonthKey = (text) => {
                        const match = (text || '').match(/([A-Za-z]{3,9})\s+(\d{4})/);
                        if (!match) {
                            return null;
                        }
                        const month = monthMap[match[1].toLowerCase()];
                        if (!month) {
                            return null;
                        }
                        return `${match[2]}-${String(month).padStart(2, '0')}`;
                    };
                    const matchDay = (root) => {
                        const nodes = root.querySelectorAll('[aria-label]');
                        for (const node of nodes) {
                            const aria = node.getAttribute('aria-label') || '';
                            if (aria === label && !node.classList.contains('disabled') && !node.closest('.disabled, [disabled]')) {
                                const clickable = node.closest('[role="gridcell"]') || node;
                                clickable.scrollIntoView({ block: 'center' });
                                clickable.click();
                                return true;
                            }
                        }
                        return false;
                    };

                    const modal = document.querySelector('[aria-label="calendar-with-fare"]')
                        || document.querySelector('.calendar-with-fare');
                    if (modal && matchDay(modal)) {
                        return { clicked: true, action: 'click-modal' };
                    }
                    if (matchDay(document)) {
                        return { clicked: true, action: 'click-document' };
                    }

                    const monthKeys = Array.from(document.querySelectorAll('.dp-month-short, .dp-month, .ngb-dp-month-name'))
                        .filter(isVisible)
                        .map(node => toMonthKey(node.textContent || ''))
                        .filter((key, index, list) => !!key && list.indexOf(key) === index);

                    if (monthKeys.length > 0) {
                        const firstMonth = monthKeys[0];
                        const lastMonth = monthKeys[monthKeys.length - 1];
                        if (targetMonthKey < firstMonth) {
                            const prev = document.querySelector('button[aria-label="Previous month"]');
                            if (isVisible(prev)) {
                                prev.click();
                                return { clicked: false, action: 'prev-month' };
                            }
                        }
                        if (targetMonthKey > lastMonth) {
                            const next = document.querySelector('button[aria-label="Next month"]');
                            if (isVisible(next)) {
                                next.click();
                                return { clicked: false, action: 'next-month' };
                            }
                        }
                    }

                    if (modal && modal.scrollHeight > modal.clientHeight + 20) {
                        const nextTop = Math.min(modal.scrollTop + Math.max(320, modal.clientHeight - 40), modal.scrollHeight - modal.clientHeight);
                        if (nextTop > modal.scrollTop) {
                            modal.scrollTop = nextTop;
                            return { clicked: false, action: 'scroll-calendar' };
                        }
                    }

                    const next = document.querySelector('button[aria-label="Next month"]');
                    if (isVisible(next)) {
                        next.click();
                        return { clicked: false, action: 'next-month-fallback' };
                    }

                    return { clicked: false, action: 'stalled' };
                }""", {"label": day_label, "targetMonthKey": target_month_key})
                if step.get("clicked"):
                    clicked = True
                    break
                if step.get("action") == "stalled":
                    break
                await asyncio.sleep(0.35)

            if clicked:
                logger.info("Scoot: selected date %s", target.strftime("%Y-%m-%d"))
                await asyncio.sleep(0.5)
                await self._close_calendar(page)
                return True

            logger.warning("Scoot: couldn't select date %s (label=%s)", target.strftime("%Y-%m-%d"), day_label)
            return False
        except Exception as e:
            logger.warning("Scoot: date fill error: %s", e)
            return False

    async def _close_calendar(self, page) -> None:
        """Click the 'Done' button to close the calendar picker."""
        try:
            done_clicked = await page.evaluate("""() => {
                const btns = document.querySelectorAll('button, div[role="button"]');
                for (const btn of btns) {
                    const aria = btn.getAttribute('aria-label') || '';
                    const text = btn.textContent.trim();
                    if ((btn.offsetHeight > 0 || btn.offsetWidth > 0 || btn.getClientRects().length) &&
                        (text === 'Done' || text === 'Close' || text === 'Confirm' || aria === 'Close')) {
                        btn.click();
                        return true;
                    }
                }
                return false;
            }""")
            if done_clicked:
                logger.info("Scoot: closed calendar via Done button")
                await asyncio.sleep(0.5)
        except Exception:
            pass

    # ── Search submit ───────────────────────────────────────────────────────

    async def _click_search(self, page) -> None:
        """Click the 'Let's Go!' search button via JS."""
        clicked = await page.evaluate("""() => {
            const ariaTargets = [
                '[aria-label="search-button"]',
                'button[aria-label="search-button"]',
            ];
            for (const selector of ariaTargets) {
                const nodes = document.querySelectorAll(selector);
                for (const node of nodes) {
                    if (node.offsetHeight > 0 || node.offsetWidth > 0 || node.getClientRects().length) {
                        node.click();
                        return selector;
                    }
                }
            }
            const labels = ["Let's Go!", "Search flights", "Search", "SEARCH", "Find flights"];
            const btns = document.querySelectorAll('button, a[role="button"], div[role="button"]');
            for (const label of labels) {
                for (const btn of btns) {
                    if (btn.offsetHeight > 0 && btn.textContent.trim() === label) {
                        btn.click();
                        return label;
                    }
                }
            }
            // Fallback: submit button
            const submit = document.querySelector('button[type="submit"]');
            if (submit && submit.offsetHeight > 0) {
                submit.click();
                return 'submit';
            }
            return null;
        }""")
        if clicked:
            logger.info("Scoot: clicked search button '%s'", clicked)
        else:
            await page.keyboard.press("Enter")
            logger.info("Scoot: pressed Enter as search fallback")

    async def _get_select_page_state(self, page) -> dict[str, Any]:
        try:
            return await page.evaluate(r"""() => {
                const isVisible = (el) => !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
                const normalize = (text) => (text || '').replace(/\s+/g, ' ').trim();
                const bodyText = document.body?.innerText || '';
                const hasRefresh = Array.from(document.querySelectorAll('button, a[role="button"], div[role="button"], a'))
                    .some(el => isVisible(el) && normalize(el.textContent) === 'Refresh');
                return {
                    isSelectPage: /\/book\/flight\//i.test(location.pathname || '') || /Scoot\s*\|\s*Select/i.test(document.title || ''),
                    cardCount: document.querySelectorAll('[aria-label="fare availability container"], .fare-availability-container').length,
                    filterCount: Array.from(document.querySelectorAll('[aria-label^="Button filter"]')).filter(isVisible).length,
                    genericCount: document.querySelectorAll('[class*="flight-card"], [class*="flight-row"], [class*="journey-card"], [class*="flight-result"], [class*="fare-card"], [class*="flight-select"], [data-test*="flight"], [class*="availability-row"]').length,
                    hasRefresh,
                    isLoading: /Add Passenger Details\s+Loading|\bLoading\.\.\.|\bLoading\b/i.test(bodyText),
                };
            }""")
        except Exception:
            return {
                "isSelectPage": False,
                "cardCount": 0,
                "filterCount": 0,
                "genericCount": 0,
                "hasRefresh": False,
                "isLoading": False,
            }

    async def _wait_for_select_page_content(self, page, timeout_ms: int = 12000) -> bool:
        try:
            await page.wait_for_function(r"""() => {
                const isVisible = (el) => !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
                const normalize = (text) => (text || '').replace(/\s+/g, ' ').trim();
                const cardCount = document.querySelectorAll('[aria-label="fare availability container"], .fare-availability-container').length;
                const filterCount = Array.from(document.querySelectorAll('[aria-label^="Button filter"]')).filter(isVisible).length;
                const hasRefresh = Array.from(document.querySelectorAll('button, a[role="button"], div[role="button"], a'))
                    .some(el => isVisible(el) && normalize(el.textContent) === 'Refresh');
                return cardCount > 0 || filterCount > 0 || hasRefresh;
            }""", timeout=timeout_ms)
            return True
        except Exception:
            return False

    async def _extract_select_page_offers(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        await self._wait_for_select_page_content(page)
        offers = await self._extract_from_dom(page, req)
        state = await self._get_select_page_state(page)
        if not state.get("isSelectPage"):
            return offers

        needs_reload = (
            (not state.get("cardCount") and not state.get("filterCount"))
            or (offers and len(offers) <= 1 and not self._offers_have_bundle_metadata(offers))
        )
        if not needs_reload:
            return offers

        current_url = page.url
        if not current_url:
            return offers

        logger.info("Scoot: reloading select page for hydrated fare containers")
        try:
            await page.goto(current_url, wait_until="domcontentloaded", timeout=20000)
            await self._wait_for_select_page_content(page, timeout_ms=15000)
            reloaded_offers = await self._extract_from_dom(page, req)
            if reloaded_offers:
                return reloaded_offers
        except Exception as e:
            logger.info("Scoot: select page reload recovery failed: %s", e)
        return offers

    # ── DOM extraction fallback ─────────────────────────────────────────────

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight offers from DOM elements on the results page."""
        try:
            generic_fallback: list[FlightOffer] = []
            for attempt, delay_seconds in enumerate((3, 5, 5)):
                await asyncio.sleep(delay_seconds)
                offers_data = await page.evaluate(r"""() => {
                    const isVisible = (el) => !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
                    const normalize = (text) => (text || '').replace(/\s+/g, ' ').trim();

                    const bundleScript = document.querySelector('script#bundle-data-v2, script#bundle-data');
                    if (bundleScript) {
                        try { return { type: 'bundle', data: JSON.parse(bundleScript.textContent) }; }
                        catch {}
                    }

                    const selectCards = [];
                    const seenSelectCards = new Set();
                    const availabilityCards = Array.from(document.querySelectorAll('.availability-card')).filter(isVisible);
                    availabilityCards.forEach(card => {
                        const cityNodes = Array.from(card.querySelectorAll('.original-text')).map(el => normalize(el.textContent)).filter(Boolean);
                        const originTexts = Array.from(card.querySelectorAll('.origin-station-text')).map(el => normalize(el.textContent)).filter(Boolean);
                        const destinationTexts = Array.from(card.querySelectorAll('.destination-station-text')).map(el => normalize(el.textContent)).filter(Boolean);
                        const dateTexts = Array.from(card.querySelectorAll('.date-text')).map(el => normalize(el.textContent)).filter(Boolean);
                        const flightTexts = Array.from(card.querySelectorAll('.flight-number-text')).map(el => normalize(el.textContent)).filter(Boolean);

                        const originCity = cityNodes[0] || '';
                        const destinationCity = cityNodes[1] || '';
                        const originCode = originTexts[0] || '';
                        const departureTime = originTexts[1] || '';
                        const destinationCode = destinationTexts[0] || '';
                        const arrivalTime = destinationTexts[1] || '';
                        const departureDay = dateTexts[0] || '';
                        const arrivalDay = dateTexts[1] || departureDay;
                        const duration = flightTexts.find(text => /\d+h\s*\d+m/i.test(text)) || '';
                        const flightNo = flightTexts.find(text => /^(TR|TZ|3K)\d+$/i.test(text)) || '';

                        Array.from(card.querySelectorAll('.fare-type-bundle')).filter(isVisible).forEach(bundle => {
                            const fareBrand = normalize(bundle.querySelector('.fare-type-header-text')?.textContent || '');
                            const currency = normalize(bundle.querySelector('.fare-currency')?.textContent || 'SGD');
                            const price = normalize(bundle.querySelector('.fare-price')?.textContent || bundle.querySelector('.fare-price-wrapper')?.textContent || '');
                            if (!fareBrand || !price) return;
                            const fullText = normalize(`${departureDay} ${departureTime} ${originCode}${originCity} ${arrivalDay} ${arrivalTime} ${destinationCode}${destinationCity} ${flightNo} ${duration} ${fareBrand} ${currency} ${price}`);
                            const dedupeKey = `${fareBrand}|${flightNo}|${fullText}`;
                            if (seenSelectCards.has(dedupeKey)) return;
                            seenSelectCards.add(dedupeKey);
                            selectCards.push({ fareBrand, fullText });
                        });
                    });
                    if (selectCards.length > 0) {
                        return { type: 'select', cards: selectCards };
                    }

                    const fareTypes = Array.from(document.querySelectorAll('[aria-label^="Button filter"], .btn-fare-filter'))
                        .filter(el => isVisible(el) && normalize(el.textContent))
                        .map(el => normalize(el.textContent).replace(/\s+From\s+[A-Z]{3}\s*[\d,.]+/i, ''));
                    const fareContainers = Array.from(document.querySelectorAll('.fare-swiper, [class*="fare-swiper"]'))
                        .filter(el => el.querySelector('[aria-label="fare availability container"], .fare-availability-container'));
                    fareContainers.forEach((container, index) => {
                        const fareBrand = fareTypes[index] || fareTypes[0] || '';
                        container.querySelectorAll('[aria-label="fare availability container"], .fare-availability-container').forEach(card => {
                            const fullText = normalize(card.innerText || card.textContent || '');
                            if (!fullText) return;
                            const dedupeKey = `${fareBrand}|${fullText}`;
                            if (seenSelectCards.has(dedupeKey)) return;
                            seenSelectCards.add(dedupeKey);
                            selectCards.push({ fareBrand, fullText });
                        });
                    });
                    if (selectCards.length === 0) {
                        document.querySelectorAll('[aria-label="fare availability container"], .fare-availability-container').forEach(card => {
                            const fullText = normalize(card.innerText || card.textContent || '');
                            if (!fullText) return;
                            const dedupeKey = `|${fullText}`;
                            if (seenSelectCards.has(dedupeKey)) return;
                            seenSelectCards.add(dedupeKey);
                            selectCards.push({ fareBrand: fareTypes[0] || '', fullText });
                        });
                    }
                    if (selectCards.length > 0) {
                        return { type: 'select', cards: selectCards };
                    }

                    const results = [];
                    const cardSelectors = [
                        '[class*="flight-card"]', '[class*="flight-row"]',
                        '[class*="journey-card"]', '[class*="flight-result"]',
                        '[class*="fare-card"]', '[class*="flight-select"]',
                        '[data-test*="flight"]', '[class*="availability-row"]',
                    ];
                    for (const sel of cardSelectors) {
                        const cards = document.querySelectorAll(sel);
                        if (cards.length > 0) {
                            cards.forEach(card => {
                                const text = card.innerText || '';
                                const priceMatch = text.match(/(?:SGD|USD|EUR|\$)\s*([\d,]+\.?\d*)/i)
                                    || text.match(/([\d,]+\.?\d*)\s*(?:SGD|USD|EUR)/i);
                                const timeMatch = text.match(/(\d{1,2}:\d{2})\s*(?:am|pm)?/gi);
                                const flightMatch = text.match(/(?:TR|TZ|3K)\s*\d+/i);
                                if (priceMatch || timeMatch)
                                    results.push({
                                        price: priceMatch ? parseFloat(priceMatch[1].replace(',', '')) : null,
                                        times: timeMatch || [],
                                        flightNo: flightMatch ? flightMatch[0] : null,
                                        fullText: text.substring(0, 300),
                                    });
                            });
                            break;
                        }
                    }

                    const bodyText = document.body?.innerText || '';
                    const isSelectPage = /\/book\/flight\//i.test(location.pathname || '') || /Scoot\s*\|\s*Select/i.test(document.title || '');
                    const filterCount = fareTypes.length;
                    const genericCount = results.length;
                    const isLoading = isSelectPage && (
                        /Add Passenger Details\s+Loading|\bLoading\.\.\.|\bLoading\b/i.test(bodyText)
                        || (!selectCards.length && !filterCount && genericCount <= 1)
                    );
                    const isStuck = /Uh-Oh, Something.?s Stuck|Something went wrong/i.test(bodyText);
                    const hasRefresh = Array.from(document.querySelectorAll('button, a[role="button"], div[role="button"], a'))
                        .some(el => isVisible(el) && normalize(el.textContent) === 'Refresh');
                    return {
                        type: 'dom',
                        cards: results,
                        isStuck,
                        hasRefresh,
                        isSelectPage,
                        isLoading,
                        filterCount,
                        genericCount,
                    };
                }""")

                if not offers_data:
                    return []

                data_type = offers_data.get("type", "")
                if data_type == "bundle":
                    return self._parse_navitaire_response(offers_data.get("data", {}), req)
                if data_type == "select":
                    offers = self._parse_select_page_cards(offers_data.get("cards", []), req)
                    if offers:
                        return offers
                if data_type == "dom":
                    offers = self._parse_dom_cards(offers_data.get("cards", []), req)
                    if offers_data.get("isSelectPage") and offers:
                        generic_fallback = offers
                        if offers_data.get("isLoading") or not offers_data.get("filterCount"):
                            logger.info(
                                "Scoot: select page still loading (%s generic cards); waiting for fare containers",
                                offers_data.get("genericCount", 0),
                            )
                            if attempt < 2:
                                continue
                    if offers:
                        return offers
                    if attempt == 0 and offers_data.get("isStuck") and offers_data.get("hasRefresh"):
                        refreshed = await page.evaluate(r"""() => {
                            const normalize = (text) => (text || '').replace(/\s+/g, ' ').trim();
                            const actions = document.querySelectorAll('button, a[role="button"], div[role="button"], a');
                            for (const action of actions) {
                                if ((action.offsetWidth || action.offsetHeight || action.getClientRects().length) && normalize(action.textContent) === 'Refresh') {
                                    action.click();
                                    return true;
                                }
                            }
                            return false;
                        }""")
                        if refreshed:
                            logger.info("Scoot: refreshed select page after stuck-flight state")
                            continue
            return generic_fallback
        except Exception as e:
            logger.debug("Scoot: DOM extraction error: %s", e)
            return []

    def _parse_select_page_cards(self, cards: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Scoot's current select-page fare DOM into offers."""
        offers: list[FlightOffer] = []
        seen: set[tuple[str, str, str, float, str]] = set()
        booking_url = self._build_booking_url(req)
        anchor_date = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
        currency = req.currency or "SGD"
        pattern = re.compile(
            r"(?P<dep_day>[A-Za-z]{3},\s+\d{2}\s+[A-Za-z]{3})\s*"
            r"(?P<dep_time>\d{2}:\d{2})\s*"
            r"(?P<origin>[A-Z]{3}).*?"
            r"(?P<arr_day>[A-Za-z]{3},\s+\d{2}\s+[A-Za-z]{3})\s*"
            r"(?P<arr_time>\d{2}:\d{2})\s*"
            r"(?P<destination>[A-Z]{3}).*?"
            r"(?P<flight>(?:TR|TZ|3K)\d+)"
            r".*?(?P<duration>\d+h\s*\d+m)?"
            r".*?(?P<currency>[A-Z]{3})\s*(?P<price>\d+(?:\.\d+)?)",
            re.IGNORECASE,
        )

        for index, card in enumerate(cards):
            if not isinstance(card, dict):
                continue
            full_text = " ".join(str(card.get("fullText") or "").split())
            if not full_text:
                continue
            match = pattern.search(full_text)
            if not match:
                continue

            try:
                price = float(match.group("price"))
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue

            dep_dt = self._parse_partial_date_time(match.group("dep_day"), match.group("dep_time"), anchor_date)
            arr_dt = self._parse_partial_date_time(match.group("arr_day"), match.group("arr_time"), anchor_date)
            if arr_dt < dep_dt:
                arr_dt += timedelta(days=1)

            flight_no = match.group("flight").replace(" ", "")
            fare_brand = " ".join(str(card.get("fareBrand") or "").split())
            if not fare_brand:
                lowered = full_text.lower()
                if "scootplus" in lowered:
                    fare_brand = "ScootPlus"
                elif "economy" in lowered:
                    fare_brand = "Economy"
            dedupe_key = (
                flight_no,
                dep_dt.isoformat(),
                arr_dt.isoformat(),
                round(price, 2),
                fare_brand,
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            route = FlightRoute(
                segments=[
                    FlightSegment(
                        airline="TR",
                        airline_name="Scoot",
                        flight_no=flight_no,
                        origin=match.group("origin"),
                        destination=match.group("destination"),
                        departure=dep_dt,
                        arrival=arr_dt,
                        cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy"),
                    )
                ],
                total_duration_seconds=max(int((arr_dt - dep_dt).total_seconds()), 0),
                stopovers=0,
            )

            conditions = {"price_type": "select_page_dom"}
            if fare_brand:
                conditions["fare_brand"] = fare_brand
                conditions["bundle_name"] = fare_brand

            offer_key = f"{flight_no}:{dep_dt.isoformat()}:{price:.2f}:{fare_brand or index}"
            offers.append(FlightOffer(
                id=f"tr_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                price=round(price, 2),
                currency=(match.group("currency") or currency).upper(),
                price_formatted=f"{price:.2f} {(match.group('currency') or currency).upper()}",
                outbound=route,
                inbound=None,
                airlines=["Scoot"],
                owner_airline="TR",
                conditions=conditions,
                booking_url=booking_url,
                is_locked=False,
                source="scoot_direct",
                source_tier="free",
            ))

        offers.sort(key=lambda offer: offer.price)
        return offers

    @staticmethod
    def _parse_partial_date_time(day_label: str, time_value: str, anchor_date: date) -> datetime:
        candidate = datetime.strptime(f"{day_label} {anchor_date.year} {time_value}", "%a, %d %b %Y %H:%M")
        delta_days = (candidate.date() - anchor_date).days
        if delta_days < -200:
            candidate = candidate.replace(year=candidate.year + 1)
        elif delta_days > 200:
            candidate = candidate.replace(year=candidate.year - 1)
        return candidate

    async def _extract_from_page_data(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from Angular component state or inline scripts."""
        try:
            data = await page.evaluate("""() => {
                const scripts = document.querySelectorAll('script');
                for (const s of scripts) {
                    const t = s.textContent || '';
                    const m = t.match(/FlightData\\s*=\\s*'([\\s\\S]*?)';/);
                    if (m) return { type: 'flightdata', raw: m[1] };
                    const m2 = t.match(/(?:availability|flightSearch)\\s*=\\s*({[\\s\\S]*?});/);
                    if (m2) return { type: 'json', raw: m2[1] };
                }
                if (window.FlightData) return { type: 'global', data: window.FlightData };
                if (window.AvailabilityV2) return { type: 'global', data: window.AvailabilityV2 };
                return null;
            }""")
            if not data:
                return []
            if data.get("type") == "flightdata":
                import html as html_mod
                raw = html_mod.unescape(data["raw"])
                return self._parse_navitaire_response(json.loads(raw), req)
            if data.get("type") == "json":
                return self._parse_navitaire_response(json.loads(data["raw"]), req)
            if data.get("type") == "global" and data.get("data"):
                return self._parse_navitaire_response(data["data"], req)
            return []
        except Exception:
            return []

    # ── Response parsing ────────────────────────────────────────────────────

    def _parse_navitaire_journey(
        self,
        journey: dict,
        req: FlightSearchRequest,
        fare_lookup: dict[str, float],
        fare_data_lookup: dict[str, dict[str, Any]],
    ) -> list[tuple[str, float, FlightRoute, dict[str, Any]]]:
        """Parse a single Navitaire journey into one offer per fare product."""
        if not isinstance(journey, dict):
            return []

        route = self._build_navitaire_route(journey, req)
        if route is None:
            return []

        offers: list[tuple[str, float, FlightRoute, dict[str, Any]]] = []
        seen: set[tuple[str, float]] = set()
        fares = journey.get("fares", [])
        if not isinstance(fares, list):
            fares = []

        for index, fare in enumerate(fares):
            if not isinstance(fare, dict):
                continue

            fare_availability_key = str(fare.get("fareAvailabilityKey") or "").strip()
            fare_key = (
                fare_availability_key
                or str(fare.get("fareSellKey") or "").strip()
                or str(fare.get("productClass") or "").strip()
                or str(index)
            )
            fare_data = fare_data_lookup.get(fare_availability_key, {})
            price = self._extract_navitaire_fare_amount(fare_data)
            if price is None:
                price = self._extract_navitaire_fare_amount(fare)
            if price is None and fare_availability_key:
                price = fare_lookup.get(fare_availability_key)
            if price is None or price <= 0:
                continue

            conditions = self._extract_navitaire_fare_metadata(fare, fare_data)
            dedupe_key = (
                str(
                    conditions.get("service_bundle_code")
                    or conditions.get("fare_brand")
                    or conditions.get("bundle_name")
                    or fare_key
                ),
                round(price, 2),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            offers.append((fare_key, round(price, 2), route, conditions))

        if offers:
            offers.sort(key=lambda item: item[1])
            return offers

        best_price = float("inf")
        for fare in fares:
            if not isinstance(fare, dict):
                continue
            fare_availability_key = str(fare.get("fareAvailabilityKey") or "").strip()
            if fare_availability_key in fare_lookup:
                price = fare_lookup[fare_availability_key]
                if 0 < price < best_price:
                    best_price = price
        if best_price == float("inf"):
            return []
        return [("journey", round(best_price, 2), route, {})]

    def _build_navitaire_route(self, journey: dict, req: FlightSearchRequest) -> Optional[FlightRoute]:
        _tr_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")

        segments: list[FlightSegment] = []
        for seg in journey.get("segments", []):
            ident = seg.get("identifier", {})
            desig = seg.get("designator", {})
            carrier = ident.get("carrierCode", "TR")
            flight_num = str(ident.get("identifier", "")).strip()
            flight_no = f"{carrier}{flight_num}" if flight_num else ""
            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Scoot",
                flight_no=flight_no,
                origin=desig.get("origin", req.origin),
                destination=desig.get("destination", req.destination),
                departure=self._parse_dt(desig.get("departure", "")),
                arrival=self._parse_dt(desig.get("arrival", "")),
                cabin_class=_tr_cabin,
            ))

        if not segments:
            desig = journey.get("designator", {})
            if desig:
                segments.append(FlightSegment(
                    airline="TR", airline_name="Scoot",
                    flight_no="",
                    origin=desig.get("origin", req.origin),
                    destination=desig.get("destination", req.destination),
                    departure=self._parse_dt(desig.get("departure", "")),
                    arrival=self._parse_dt(desig.get("arrival", "")),
                    cabin_class=_tr_cabin,
                ))
            if not segments:
                return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            delta = segments[-1].arrival - segments[0].departure
            total_dur = max(int(delta.total_seconds()), 0)

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(len(segments) - 1, 0),
        )
        return route

    @staticmethod
    def _extract_navitaire_fare_amount(fare: Any) -> Optional[float]:
        if not isinstance(fare, dict):
            return None

        totals = fare.get("totals") if isinstance(fare.get("totals"), dict) else {}
        candidates = [
            totals.get("fareTotal"),
            totals.get("totalAmount"),
            totals.get("amount"),
            fare.get("fareTotal"),
            fare.get("totalAmount"),
            fare.get("fareAmount"),
            fare.get("amount"),
            fare.get("price"),
            fare.get("PassengerFare"),
            fare.get("passengerFare"),
        ]
        for candidate in candidates:
            if isinstance(candidate, dict):
                candidate = (
                    candidate.get("Amount")
                    or candidate.get("amount")
                    or candidate.get("TotalAmount")
                    or candidate.get("value")
                )
            if candidate is None:
                continue
            try:
                value = float(candidate)
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
        return None

    @staticmethod
    def _extract_navitaire_fare_metadata(fare: dict, fare_data: dict) -> dict[str, Any]:
        def pick(*keys: str) -> Optional[str]:
            for source in (fare_data, fare):
                if not isinstance(source, dict):
                    continue
                for key in keys:
                    value = source.get(key)
                    if value is None:
                        continue
                    if isinstance(value, str):
                        value = value.strip()
                    if value in ("", [], {}):
                        continue
                    return str(value)
            return None

        conditions: dict[str, Any] = {}
        fare_brand = pick(
            "productClass",
            "productBundle",
            "bundleName",
            "fareClassOfService",
            "fareClassCode",
            "fareClass",
            "classOfService",
            "name",
            "title",
        )
        bundle_name = pick("bundleName", "productName", "name", "title", "label", "displayName")
        bundle_subheader = pick("bundleDescription", "description", "subheader", "marketingText", "summary")
        service_bundle_code = pick("serviceBundleCode", "bundleCode", "productBundleCode", "productClass")
        fare_code = pick("fareClassCode", "fareCode", "productCode")
        fare_sell_key = pick("fareSellKey")
        price_type = pick("priceType", "journeyFareType", "fareType")
        fare_availability_key = pick("fareAvailabilityKey")

        if fare_brand:
            conditions["fare_brand"] = fare_brand
        if bundle_name:
            conditions["bundle_name"] = bundle_name
        if bundle_subheader:
            conditions["bundle_subheader"] = bundle_subheader
        if service_bundle_code:
            conditions["service_bundle_code"] = service_bundle_code
        if fare_code:
            conditions["fare_code"] = fare_code
        if fare_sell_key:
            conditions["fare_sell_key"] = fare_sell_key
        if fare_availability_key:
            conditions["fare_availability_key"] = fare_availability_key
        if price_type:
            conditions["price_type"] = price_type
        return conditions

    @staticmethod
    def _merge_round_trip_conditions(
        outbound_conditions: Optional[dict[str, Any]],
        inbound_conditions: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        merged = dict(outbound_conditions or {})
        inbound = dict(inbound_conditions or {})
        brand_keys = ("fare_brand", "bundle_name")
        outbound_brand = next((merged.get(key) for key in brand_keys if merged.get(key)), "")
        inbound_brand = next((inbound.get(key) for key in brand_keys if inbound.get(key)), "")
        if outbound_brand or inbound_brand:
            merged["fare_brand"] = f"{outbound_brand or 'Outbound'} / {inbound_brand or 'Inbound'}"

        for key in [
            "bundle_name",
            "bundle_subheader",
            "service_bundle_code",
            "fare_code",
            "fare_sell_key",
            "fare_availability_key",
            "price_type",
        ]:
            outbound_value = (outbound_conditions or {}).get(key)
            inbound_value = inbound.get(key)
            if outbound_value:
                merged[f"outbound_{key}"] = outbound_value
            if inbound_value:
                merged[f"inbound_{key}"] = inbound_value
        return merged

    def _parse_navitaire_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Scoot Navitaire availability API response.

        Structure: data.trips[].journeys[] with pricing in data.faresAvailable[].
        Each journey.fares[].fareAvailabilityKey maps to faresAvailable[].totals.fareTotal.
        For RT: trips[0] = outbound, trips[1] = inbound.
        """
        if not isinstance(data, dict):
            return []

        offers: list[FlightOffer] = []
        currency = data.get("currencyCode", "SGD")
        booking_url = self._build_booking_url(req)

        # Build fare price lookup: fareAvailabilityKey → lowest fareTotal
        fare_lookup: dict[str, float] = {}
        fare_data_lookup: dict[str, dict[str, Any]] = {}
        for fa in data.get("faresAvailable", []):
            key = fa.get("fareAvailabilityKey", "")
            if not key:
                continue
            price = self._extract_navitaire_fare_amount(fa)
            if price is not None and (key not in fare_lookup or price < fare_lookup[key]):
                fare_lookup[key] = price
                fare_data_lookup[key] = fa
            elif key not in fare_data_lookup:
                fare_data_lookup[key] = fa

        trips = data.get("trips", [])
        is_rt = bool(req.return_from) and len(trips) >= 2

        # ── Parse outbound journeys (trip[0]) ──────────────────────────
        ob_journeys: list[tuple[str, str, float, FlightRoute, dict[str, Any]]] = []
        ob_trip = trips[0] if trips else {}
        if isinstance(ob_trip, dict):
            for journey in ob_trip.get("journeys", []):
                parsed = self._parse_navitaire_journey(journey, req, fare_lookup, fare_data_lookup)
                if not parsed:
                    continue
                jkey = journey.get("journeyKey", f"{req.origin}{req.destination}{time.monotonic()}")
                for fare_key, price, route, conditions in parsed:
                    ob_journeys.append((jkey, fare_key, price, route, conditions))

        # ── Parse inbound journeys (trip[1]) if RT ─────────────────────
        ib_route: Optional[FlightRoute] = None
        ib_price = 0.0
        ib_conditions: dict[str, Any] = {}
        if is_rt:
            ib_best_price = float("inf")
            ib_trip = trips[1]
            if isinstance(ib_trip, dict):
                for journey in ib_trip.get("journeys", []):
                    parsed = self._parse_navitaire_journey(journey, req, fare_lookup, fare_data_lookup)
                    for _, price, route, conditions in parsed:
                        if price < ib_best_price:
                            ib_best_price = price
                            ib_route = route
                            ib_price = price
                            ib_conditions = conditions

        # ── Build offers ───────────────────────────────────────────────
        for jkey, fare_key, price, route, conditions in ob_journeys:
            total_price = round(price + ib_price, 2) if is_rt else round(price, 2)
            prefix = "tr_rt_" if is_rt and ib_route else "tr_"
            merged_conditions = (
                self._merge_round_trip_conditions(conditions, ib_conditions)
                if is_rt and ib_route
                else dict(conditions)
            )
            offers.append(FlightOffer(
                id=f"{prefix}{hashlib.md5(f'{jkey}:{fare_key}'.encode()).hexdigest()[:12]}",
                price=total_price,
                currency=currency,
                price_formatted=f"{total_price:.2f} {currency}",
                outbound=route,
                inbound=ib_route,
                airlines=["Scoot"],
                owner_airline="TR",
                conditions=merged_conditions,
                booking_url=booking_url,
                is_locked=False,
                source="scoot_direct",
                source_tier="free",
            ))

        return offers

    def _parse_single_flight(self, flight: dict, currency: str,
                             req: FlightSearchRequest,
                             booking_url: str) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None

        segments = self._parse_segments(flight, req)
        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            delta = segments[-1].arrival - segments[0].departure
            total_dur = max(int(delta.total_seconds()), 0)

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = (
            flight.get("JourneySellKey") or flight.get("journeySellKey")
            or flight.get("journeyKey") or flight.get("id")
            or f"{req.origin}{req.destination}{time.monotonic()}"
        )

        return FlightOffer(
            id=f"tr_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["Scoot"], owner_airline="TR",
            booking_url=booking_url, is_locked=False,
            source="scoot_direct", source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        best = float("inf")
        # Navitaire Bundles (like Jetstar)
        bundles = flight.get("Bundles") or flight.get("bundles") or []
        for bundle in bundles:
            if isinstance(bundle, dict):
                for key in [
                    "RegularInclusiveAmount", "regularInclusiveAmount",
                    "CjInclusiveAmount", "cjInclusiveAmount",
                    "TotalAmount", "totalAmount",
                    "Amount", "amount", "Price", "price",
                ]:
                    val = bundle.get(key)
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        # Navitaire Fares
        fares = (flight.get("Fares") or flight.get("fares")
                 or flight.get("fareProducts") or [])
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice",
                            "fareAmount", "totalAmount",
                            "PassengerFare", "passengerFare"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = (val.get("Amount") or val.get("amount")
                               or val.get("TotalAmount"))
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        # Direct price fields
        for key in ["price", "lowestFare", "totalPrice", "farePrice",
                     "amount", "lowestPrice", "Price", "LowestFare", "TotalPrice"]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = (float(p) if not isinstance(p, dict)
                         else float(p.get("Amount") or p.get("amount") or 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _parse_segments(self, flight: dict, req: FlightSearchRequest) -> list[FlightSegment]:
        segments: list[FlightSegment] = []
        _tr_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segs_raw = (
            flight.get("Segments") or flight.get("segments")
            or flight.get("Legs") or flight.get("legs")
            or flight.get("flights") or []
        )
        if segs_raw and isinstance(segs_raw, list):
            for seg in segs_raw:
                if isinstance(seg, dict):
                    segments.append(self._build_segment(seg, req.origin, req.destination, _tr_cabin))
            if segments:
                return segments

        # Parse from JourneySellKey
        journey_key = flight.get("JourneySellKey") or flight.get("journeySellKey") or ""
        if journey_key and "~" in journey_key:
            seg = self._parse_journey_sell_key(journey_key, req)
            if seg:
                return [seg]

        # Build from flight-level fields
        dep_str = (flight.get("DepartureDateTime") or flight.get("departureDateTime")
                   or flight.get("departure") or flight.get("departureDate")
                   or flight.get("STD") or "")
        arr_str = (flight.get("ArrivalDateTime") or flight.get("arrivalDateTime")
                   or flight.get("arrival") or flight.get("arrivalDate")
                   or flight.get("STA") or "")
        flight_no_raw = (flight.get("FlightNumber") or flight.get("flightNumber")
                         or flight.get("FlightDesignator", {}).get("FlightNumber", "")
                         or "")
        carrier = (flight.get("CarrierCode") or flight.get("carrierCode")
                   or flight.get("FlightDesignator", {}).get("CarrierCode", "")
                   or "TR")
        origin = (flight.get("Origin") or flight.get("origin")
                  or flight.get("DepartureStation") or req.origin)
        dest = (flight.get("Destination") or flight.get("destination")
                or flight.get("ArrivalStation") or req.destination)

        flight_no = str(flight_no_raw).replace(" ", "")
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        segments.append(FlightSegment(
            airline=carrier, airline_name="Scoot",
            flight_no=flight_no, origin=origin, destination=dest,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class=_tr_cabin,
        ))
        return segments

    @staticmethod
    def _parse_journey_sell_key(key: str, req: FlightSearchRequest) -> Optional[FlightSegment]:
        """Parse Navitaire JourneySellKey: TR~ 501~ ~~SIN~06/15/2026 06:00~BKK~..."""
        try:
            parts = key.split("~")
            if len(parts) < 7:
                return None
            carrier = parts[0].strip() or "TR"
            flight_no_raw = parts[1].strip()
            flight_no = f"{carrier}{flight_no_raw}" if flight_no_raw else ""
            origin = dest = ""
            dep_str = arr_str = ""
            for i, part in enumerate(parts):
                part = part.strip()
                if len(part) == 3 and part.isalpha() and part.isupper():
                    if not origin:
                        origin = part
                        if i + 1 < len(parts):
                            dep_str = parts[i + 1].strip()
                    elif not dest:
                        dest = part
                        if i + 1 < len(parts):
                            arr_str = parts[i + 1].strip()
            _tr_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            return FlightSegment(
                airline=carrier, airline_name="Scoot",
                flight_no=flight_no,
                origin=origin or req.origin,
                destination=dest or req.destination,
                departure=ScootConnectorClient._parse_dt(dep_str),
                arrival=ScootConnectorClient._parse_dt(arr_str),
                cabin_class=_tr_cabin,
            )
        except Exception:
            return None

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str, cabin_class: str = "economy") -> FlightSegment:
        dep_str = (seg.get("DepartureDateTime") or seg.get("departureDateTime")
                   or seg.get("departure") or seg.get("STD") or seg.get("std") or "")
        arr_str = (seg.get("ArrivalDateTime") or seg.get("arrivalDateTime")
                   or seg.get("arrival") or seg.get("STA") or seg.get("sta") or "")
        flight_no_raw = str(
            seg.get("FlightNumber") or seg.get("flightNumber")
            or seg.get("FlightDesignator", {}).get("FlightNumber", "")
            or ""
        ).replace(" ", "")
        carrier = (seg.get("CarrierCode") or seg.get("carrierCode")
                   or seg.get("FlightDesignator", {}).get("CarrierCode", "") or "TR")
        origin = (seg.get("Origin") or seg.get("origin")
                  or seg.get("DepartureStation") or default_origin)
        dest = (seg.get("Destination") or seg.get("destination")
                or seg.get("ArrivalStation") or default_dest)

        flight_no = flight_no_raw
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        return FlightSegment(
            airline=carrier, airline_name="Scoot",
            flight_no=flight_no, origin=origin, destination=dest,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class=cabin_class,
        )

    def _parse_dom_cards(self, cards: list, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []
        booking_url = self._build_booking_url(req)
        is_rt = bool(req.return_from)

        # Classify cards into outbound and inbound by origin
        ob_cards: list[dict] = []
        ib_cards: list[dict] = []
        for card in cards:
            price = card.get("price")
            if not price or price <= 0:
                continue
            card_origin = card.get("origin", req.origin)
            if is_rt and card_origin == req.destination:
                ib_cards.append(card)
            else:
                ob_cards.append(card)

        # If no IB cards but RT, all cards are outbound (fallback)
        if is_rt and not ib_cards:
            ob_cards = [c for c in cards if c.get("price") and c["price"] > 0]

        _tr_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        # Find cheapest IB for pairing
        ib_route: Optional[FlightRoute] = None
        ib_price = 0.0
        if is_rt and ib_cards:
            cheapest_ib = min(ib_cards, key=lambda c: c["price"])
            ib_price = cheapest_ib["price"]
            ib_times = cheapest_ib.get("times", [])
            ib_dep = self._parse_dt(ib_times[0]) if ib_times else datetime(2000, 1, 1)
            ib_arr = self._parse_dt(ib_times[1]) if len(ib_times) > 1 else datetime(2000, 1, 1)
            ib_dur = max(int((ib_arr - ib_dep).total_seconds()), 0) if ib_dep.year > 2000 and ib_arr.year > 2000 else 0
            ib_seg = FlightSegment(
                airline="TR", airline_name="Scoot",
                flight_no=cheapest_ib.get("flightNo", ""),
                origin=req.destination, destination=req.origin,
                departure=ib_dep, arrival=ib_arr, cabin_class=_tr_cabin,
            )
            ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=ib_dur, stopovers=0)

        for card in ob_cards:
            price = card.get("price")
            if not price or price <= 0:
                continue
            flight_no = card.get("flightNo", "")
            times = card.get("times", [])
            dep_time = self._parse_dt(times[0]) if times else datetime(2000, 1, 1)
            arr_time = self._parse_dt(times[1]) if len(times) > 1 else datetime(2000, 1, 1)
            total_dur = 0
            if dep_time.year > 2000 and arr_time.year > 2000:
                total_dur = max(int((arr_time - dep_time).total_seconds()), 0)
            seg = FlightSegment(
                airline="TR", airline_name="Scoot",
                flight_no=flight_no or "", origin=req.origin,
                destination=req.destination,
                departure=dep_time, arrival=arr_time, cabin_class=_tr_cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=total_dur,
                                stopovers=0)
            total_price = round(price + ib_price, 2) if is_rt and ib_route else round(price, 2)
            prefix = "tr_rt_" if is_rt and ib_route else "tr_"
            offers.append(FlightOffer(
                id=f"{prefix}{hashlib.md5(f'{flight_no}{price}'.encode()).hexdigest()[:12]}",
                price=total_price, currency="SGD",
                price_formatted=f"{total_price:.2f} SGD",
                outbound=route, inbound=ib_route,
                airlines=["Scoot"], owner_airline="TR",
                booking_url=booking_url, is_locked=False,
                source="scoot_direct", source_tier="free",
            ))
        return offers

    # ── Utilities ───────────────────────────────────────────────────────────

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest,
                        elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Scoot %s->%s returned %d offers in %.1fs (CDP Chrome)",
                    req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(
            f"scoot{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s).strip()
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in (
            "%m/%d/%Y %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M",
            "%H:%M",
        ):
            try:
                return datetime.strptime(s[:len(fmt) + 4], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        trip_type = "RT" if req.return_from else "OW"
        url = (
            f"https://booking.flyscoot.com/"
            f"?origin={req.origin}&destination={req.destination}&depart={dep}"
            f"&pax={req.adults or 1}&type={trip_type}"
        )
        if req.return_from:
            ret = req.return_from.strftime("%Y-%m-%d") if hasattr(req.return_from, 'strftime') else str(req.return_from)
            url += f"&return={ret}"
        return url

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
    ) -> dict | None:
        """Return Scoot (TR) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("TR")
        if not ref:
            return None
        carry_on = ref.get("carry_on")
        checked = ref.get("checked_bag")
        seat = ref.get("seat")
        ref_cur = ref.get("currency", "USD")

        # Carry-on note
        if note := ref.get("carry_on_note"):
            bags_note = note
        elif carry_on == 0.0:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"1 cabin bag included ({kg} kg)"
        elif carry_on is not None:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"cabin bag from ~{ref_cur} {carry_on:.0f} ({kg} kg)"
        else:
            bags_note = None

        # Checked bag note
        if note := ref.get("checked_bag_note"):
            checked_note = note
        elif checked == 0.0:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"1 checked bag included ({kg} kg)"
        elif checked is not None:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"checked bag from ~{ref_cur} {checked:.0f} ({kg} kg)"
        else:
            checked_note = None

        # Seat note
        if seat == 0.0:
            seat_note = "seat selection included"
        elif seat is not None:
            seat_note = f"seat selection from ~{ref_cur} {seat:.0f}"
        else:
            seat_note = "seat selection available at checkout"

        return {
            "bags_from": carry_on,
            "bags_note": bags_note,
            "checked_bag_from": checked,
            "checked_bag_note": checked_note,
            "seat_from": seat,
            "seat_note": seat_note,
            "currency": ref_cur,
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag_note")
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_from")
        seat_from = ancillary.get("seat_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions["checked_bag"] = checked_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from is not None:
                offer.bags_price.setdefault("carry_on", bags_from)
            if checked_from is not None:
                offer.bags_price.setdefault("checked_bag", checked_from)
            if seat_from is not None:
                offer.bags_price.setdefault("seat", seat_from)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"scoot{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
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
                combined_conditions = ScootConnectorClient._merge_round_trip_conditions(
                    o.conditions,
                    i.conditions,
                )
                combos.append(FlightOffer(
                    id=f"rt_tr_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    conditions=combined_conditions,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
