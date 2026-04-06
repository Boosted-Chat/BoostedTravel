"""
Turkish Airlines (TK) CDP Chrome connector — form fill + availability API interception.

TK's booking widget is a Next.js micro-frontend (availability_mf) that fires
POST /api/v1/availability after the homepage form is submitted.  Direct API
calls are blocked by PerimeterX (crypto-challenge 428 → proof-of-work).
The ONLY reliable path is form-triggered requests.

Strategy (CDP Chrome + response interception):
1. Launch REAL Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP.  Context persists across searches.
3. Each search: new page → homepage → accept cookies → One-way toggle
   → fill origin → fill destination → pick date → click "Search flights".
4. Page navigates to /availability-international/ and fires availability API.
5. First call may return 428 (crypto challenge) — page auto-solves it.
6. Capture the 200 response from POST /api/v1/availability.
7. Parse originDestinationOptionList → FlightOffer for each flight.

API details (discovered Mar 2026):
  POST /api/v1/availability
  Response: {data: {originDestinationInformationList: [{
    originDestinationOptionList: [{  optionId, startingPrice,
      fareCategory, segmentList, journeyDuration, ...}]  }],
    originalCurrency, economyStartingPrice, ...}}
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, date as date_type, date, timedelta
from typing import Optional

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied, get_httpx_proxy_url
from .airline_routes import get_country, CITY_AIRPORTS, city_match_set

logger = logging.getLogger(__name__)

# ── Sputnik API (EveryMundo) — primary fast path ──
_SPUTNIK_URL = "https://openair-california.airtrfx.com/airfare-sputnik-service/v3/tk/fares/grouped-routes"
_SPUTNIK_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_SPUTNIK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://mm-prerendering-static-prod.airtrfx.com",
    "Referer": "https://mm-prerendering-static-prod.airtrfx.com/",
    "em-api-key": _SPUTNIK_KEY,
}
_SPUTNIK_MARKETS = ["TR", "GB", "US", "DE", "FR", "NL"]

# Reverse lookup: airport code → city code (e.g. LHR → LON)
_AIRPORT_TO_CITY: dict[str, str] = {}
for _city, _apts in CITY_AIRPORTS.items():
    for _apt in _apts:
        _AIRPORT_TO_CITY[_apt] = _city

_DEBUG_PORT = 9453
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".tk_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (headed — PX blocks headless)."""
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("TK: connected to existing Chrome on port %d", _DEBUG_PORT)
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
                f"--remote-debugging-port={_DEBUG_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                *proxy_chrome_args(),
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
                "--window-position=-2400,-2400",
                "--window-size=1400,900",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.0)

            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            logger.info(
                "TK: Chrome launched headed on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    """Wipe Chrome profile when PerimeterX flags the session beyond repair."""
    global _browser, _context, _pw_instance, _chrome_proc
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("TK: deleted stale Chrome profile")
        except Exception:
            pass


# ── Date format helpers ──────────────────────────────────────────────────────

def _to_datetime(val) -> datetime:
    """Convert date or datetime to datetime."""
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_tk_datetime(s: str) -> datetime:
    """Parse TK datetime string like '28-03-2026 08:50'."""
    return datetime.strptime(s, "%d-%m-%Y %H:%M")


class TurkishConnectorClient:
    """Turkish Airlines CDP Chrome connector — form fill + availability interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Fast path: Sputnik API (no browser needed, ~1s)
        sputnik_offers = await self._try_sputnik(req)
        if sputnik_offers:
            sputnik_offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
            h = hashlib.md5(f"tk{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=sputnik_offers[0].currency,
                offers=sputnik_offers,
                total_results=len(sputnik_offers),
            )

        # Slow path: CDP Chrome form fill + API interception
        # Retry once: first attempt warms PX cookies; second uses them.
        for attempt in range(2):
            result = await self._do_search(req)
            if result.offers or attempt == 1:
                return result
            logger.warning("TK: 0 offers on attempt %d — retrying with warm profile", attempt)
            await asyncio.sleep(2.0)
        return self._empty(req)

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
            "outputCurrencies": ["USD", "EUR", "TRY"],
            "departure": {"start": start.isoformat(), "end": end.isoformat()},
            "budget": {"maximum": None},
            "passengers": {"adults": max(1, req.adults or 1)},
            "travelClasses": ["ECONOMY"],
            "flightType": "ROUND_TRIP" if req.return_from else "ONE_WAY",
            "flexibleDates": True,
            "faresPerRoute": "10",
            "trfxRoutes": True,
            "routesLimit": 500,
            "sorting": [{"popularity": "DESC"}],
            "airlineCode": "tk",
        }

        try:
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome") as s:
                r = await s.post(_SPUTNIK_URL, json=payload, headers=_SPUTNIK_HEADERS, timeout=15)
            if r.status_code != 200:
                logger.info("TK Sputnik: HTTP %d", r.status_code)
                return []
            data = r.json()
            if not isinstance(data, list):
                return []
        except Exception as e:
            logger.info("TK Sputnik error: %s", e)
            return []

        origin_set = city_match_set(req.origin)
        dest_set = city_match_set(req.destination)

        offers = []
        for route in data:
            for fare in route.get("fares") or []:
                orig = (fare.get("originAirportCode") or route.get("origin") or "").upper()
                dest = (fare.get("destinationAirportCode") or route.get("destination") or "").upper()
                if dest not in dest_set:
                    if orig not in origin_set:
                        continue

                price = fare.get("totalPrice") or fare.get("usdTotalPrice")
                if not price or float(price) <= 0:
                    continue
                if fare.get("redemption"):
                    continue

                price_f = round(float(price), 2)
                currency = fare.get("currencyCode") or "USD"
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
                    airline="TK", airline_name="Turkish Airlines", flight_no="",
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
                        airline="TK", airline_name="Turkish Airlines", flight_no="",
                        origin=dest, destination=orig,
                        origin_city=fare.get("destinationCity") or "",
                        destination_city=fare.get("originCity") or "",
                        departure=ret_dt, arrival=ret_dt,
                        duration_seconds=0, cabin_class=cabin,
                    )
                    inbound = FlightRoute(segments=[ret_seg], total_duration_seconds=0, stopovers=0)

                ret_token = f"_{ret_str}" if ret_str else ""
                fid = hashlib.md5(
                    f"tk_{orig}_{dest}_{dep_str}{ret_token}_{price_f}".encode()
                ).hexdigest()[:12]

                target_date = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)
                offers.append(FlightOffer(
                    id=f"tk_{fid}",
                    price=price_f,
                    currency=currency,
                    price_formatted=fare.get("formattedTotalPrice") or f"{price_f:.2f} {currency}",
                    outbound=outbound,
                    inbound=inbound,
                    airlines=["Turkish Airlines"],
                    owner_airline="TK",
                    booking_url=f"https://www.turkishairlines.com/en-int/flights/?origin={req.origin}&destination={req.destination}&date={target_date}",
                    is_locked=False,
                    source="turkish_direct",
                    source_tier="free",
                    conditions={
                        "trip_type": (fare.get("flightType") or "ROUND_TRIP").lower().replace("_", "-"),
                        "cabin": str(fare.get("formattedTravelClass") or cabin),
                        "fare_note": "Published fare from Turkish Airlines fare module",
                    },
                ))

        logger.info("TK Sputnik %s→%s: %d offers", req.origin, req.destination, len(offers))
        return offers

    async def _do_search(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        import json as _json
        import re as _re

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        avail_data: dict = {}
        px_blocked = False

        # ── Target date for route interception ──
        dt = _to_datetime(req.date_from)
        target_iso = dt.strftime("%Y-%m-%d")       # 2026-06-15
        target_dmy = dt.strftime("%d-%m-%Y")        # 15-06-2026
        target_dmy_nodash = dt.strftime("%d%m%Y")   # 15062026

        async def _on_response(response):
            nonlocal px_blocked
            url = response.url
            if "/api/v1/availability" not in url:
                return
            if any(x in url for x in ("validate", "price-calendar", "cheapest", "info-by-ond",
                                       "additional-services", "banner")):
                return
            status = response.status
            if status == 428:
                logger.info("TK: 428 crypto challenge — page will auto-solve")
                return
            if status == 403:
                px_blocked = True
                logger.warning("TK: PerimeterX 403 on availability")
                return
            if status == 200:
                try:
                    data = await response.json()
                    if isinstance(data, dict) and "data" in data:
                        inner = data["data"]
                        if isinstance(inner, dict) and "originDestinationInformationList" in inner:
                            avail_data.update(inner)
                            opts = inner.get("originDestinationInformationList", [{}])[0]
                            n = len(opts.get("originDestinationOptionList", []))
                            logger.warning("TK: captured availability — %d options", n)
                except Exception as e:
                    logger.warning("TK: failed to parse availability: %s", e)

        page.on("response", _on_response)

        # ── Route interceptor: rewrite departure date in availability API requests ──
        api_request_body_logged = None

        # Flag: when True, route interceptor passes requests through unmodified.
        # Used during direct fetch to avoid re-serializing the JSON body
        # (Python json.dumps may differ from JS JSON.stringify, breaking integrity).
        direct_fetch_active = False

        async def _intercept_availability(route):
            nonlocal api_request_body_logged
            request = route.request
            body = request.post_data or ""
            api_request_body_logged = body[:1000]

            if direct_fetch_active:
                logger.warning("TK: direct fetch passthrough (%d bytes)", len(body))
                await route.continue_()
                return

            logger.warning("TK: intercepted avail request (%d bytes): %s", len(body), body[:600])

            if not body:
                await route.continue_()
                return

            try:
                data = _json.loads(body)
                modified = False

                # Walk the JSON and replace ONLY departure date fields
                def _fix_dates(obj):
                    nonlocal modified
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            kl = k.lower()
                            # Only touch keys that are explicitly departure date
                            if isinstance(v, str) and kl in ("departuredate", "departure_date",
                                                              "departureDatetime", "departure"):
                                if _re.match(r"\d{4}-\d{2}-\d{2}", v):
                                    obj[k] = target_iso
                                    modified = True
                                elif _re.match(r"\d{2}-\d{2}-\d{4}", v):
                                    obj[k] = target_dmy
                                    modified = True
                                elif _re.match(r"\d{8}$", v):
                                    obj[k] = target_dmy_nodash
                                    modified = True
                            elif isinstance(v, (dict, list)):
                                _fix_dates(v)
                    elif isinstance(obj, list):
                        for item in obj:
                            if isinstance(item, (dict, list)):
                                _fix_dates(item)

                _fix_dates(data)
                if modified:
                    new_body = _json.dumps(data)
                    logger.warning("TK: rewrote API request date → %s", target_iso)
                    await route.continue_(post_data=new_body)
                else:
                    logger.warning("TK: no date fields found in API body, passing through")
                    await route.continue_()
            except Exception as exc:
                logger.warning("TK: route intercept error: %s, passing through", exc)
                await route.continue_()

        # Only intercept the main availability calls, not sub-endpoints
        await page.route(
            _re.compile(r".*/api/v1/availability(?!/validate|/price-calendar|/cheapest|/info-by-ond)"),
            _intercept_availability,
        )

        try:
            # ── Phase 0: Homepage warm-up to establish session cookies ──
            # On fresh Chrome profiles, TK ignores booking URL query params
            # and fills the form with geo-IP defaults (e.g. nearest airport).
            # A prior homepage visit sets the session cookies that make the
            # booking URL params work (origin/dest/date pre-populated).
            try:
                await page.goto(
                    "https://www.turkishairlines.com/en-int/",
                    wait_until="domcontentloaded",
                    timeout=15000,
                )
                await asyncio.sleep(3.0)
                await self._dismiss_cookies(page)
            except Exception:
                pass  # warm-up failure is non-fatal

            # ── Path 1: Booking URL (airports pre-filled) + click search ──
            # The booking URL redirects to homepage with origin/dest pre-filled.
            # Calendar doesn't open on TK's new React UI — so we skip date fill
            # entirely and rely on the route interceptor to fix the date.
            booking_url = self._booking_url(req)
            logger.warning("TK: loading booking URL for %s→%s on %s", req.origin, req.destination, target_iso)
            await page.goto(
                booking_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(5.0)
            await self._dismiss_cookies(page)
            await asyncio.sleep(1.0)

            # Dismiss overlays
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[role="dialog"], .modal-backdrop, .overlay, [class*="popup"]'
                ).forEach(el => el.remove());
            }""")

            # One-way toggle
            try:
                ow = page.locator("span:has-text('One way')").first
                if await ow.count() > 0:
                    await ow.click(timeout=5000)
                    logger.warning("TK: One-way selected")
            except Exception:
                pass
            await asyncio.sleep(1.0)

            # Diagnostic: check displayed date
            displayed = await page.evaluate("""() => {
                const dp = document.querySelector('#bookerDatepicker');
                if (!dp) return {exists: false};
                const day = dp.querySelector('[class*="placeholder-ready-day"]');
                const month = dp.querySelector('[class*="placeholder-ready-month"]');
                const fromVal = document.querySelector('#fromPort')?.value;
                const toVal = document.querySelector('#toPort')?.value;
                return {
                    exists: true,
                    day: day?.textContent?.trim(),
                    month: month?.textContent?.trim(),
                    from: fromVal, to: toVal,
                };
            }""")
            logger.warning("TK: form state — %s", displayed)

            # ── Fix airports if booking URL params weren't picked up ──
            form_from = (displayed or {}).get("from", "") or ""
            form_to = (displayed or {}).get("to", "") or ""
            form_day = (displayed or {}).get("day")
            manually_filled = False

            if not form_to:
                logger.warning(
                    "TK: booking URL didn't populate form (from=%r, to=%r), filling manually",
                    form_from, form_to,
                )
                ok1 = await self._fill_airport(page, "#fromPort", req.origin)
                if ok1:
                    await asyncio.sleep(0.8)
                ok2 = await self._fill_airport(page, "#toPort", req.destination)
                if ok2:
                    await asyncio.sleep(0.8)
                manually_filled = ok1 and ok2

                if manually_filled and not form_day:
                    # Try to get a date into the form (route interceptor will
                    # correct it to the target date regardless).
                    await self._fill_date(page, req.date_from)
                    await asyncio.sleep(0.5)

            # ── Click "Search flights" directly ──
            # Route interceptor will rewrite the date in the API request.
            search_clicked = False
            for btn_text in ["Search flights", "Search", "Find flights"]:
                try:
                    btn = page.locator(f"button:has-text('{btn_text}')").first
                    if await btn.count() > 0 and await btn.is_visible():
                        await btn.click(timeout=5000)
                        logger.warning("TK: clicked '%s'", btn_text)
                        search_clicked = True
                        break
                except Exception:
                    continue

            if not search_clicked:
                # JS fallback for search button
                search_clicked = bool(await page.evaluate("""() => {
                    const patterns = ['search', 'find flights'];
                    for (const b of document.querySelectorAll('button')) {
                        const t = (b.textContent || '').toLowerCase().trim();
                        if (b.offsetHeight > 0 && patterns.some(p => t.includes(p))) {
                            b.click();
                            return t.slice(0, 40);
                        }
                    }
                    return null;
                }"""))

            if search_clicked:
                logger.warning("TK: search clicked, waiting for availability API…")
            else:
                logger.warning("TK: could not click search button")

            # Wait for availability API (crypto challenge may add latency)
            # Shorter wait if airports were manually filled (form may not submit properly)
            wait_secs = 10 if manually_filled else 20
            avail_deadline = time.monotonic() + wait_secs
            while not avail_data and not px_blocked and time.monotonic() < avail_deadline:
                await asyncio.sleep(0.5)

            # ── Path 2: Direct API call from page context ──
            # When the form can't be submitted (date picker won't open, search
            # button not found), bypass the form entirely and call the API via
            # fetch() inside the page.  PX cookies from the warm-up visit are
            # included automatically (same-origin request).
            if not avail_data and not px_blocked:
                logger.warning("TK: form didn't trigger API, attempting direct fetch from page context")
                # Build a complete request body matching what TK's form would send
                o_cc = get_country(req.origin) or ""
                d_cc = get_country(req.destination) or ""
                o_city = _AIRPORT_TO_CITY.get(req.origin.upper(), req.origin.upper())
                d_city = _AIRPORT_TO_CITY.get(req.destination.upper(), req.destination.upper())
                try:
                    direct_fetch_active = True
                    direct_result = await page.evaluate(
                        """async (args) => {
                        const [origin, dest, dateDMY, adults,
                               oCC, dCC, oCityCode, dCityCode] = args;
                        const controller = new AbortController();
                        const timer = setTimeout(() => controller.abort(), 15000);
                        try {
                            const resp = await fetch('/api/v1/availability', {
                                method: 'POST',
                                signal: controller.signal,
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Accept': 'application/json, text/plain, */*',
                                    'X-Requested-With': 'XMLHttpRequest',
                                },
                                body: JSON.stringify({
                                    selectedBookerSearch: 'O',
                                    selectedCabinClass: 'ECONOMY',
                                    moduleType: 'TICKETING',
                                    passengerTypeList: [{quantity: parseInt(adults), code: 'ADULT'}],
                                    originDestinationInformationList: [{
                                        originAirportCode: origin,
                                        originCountryCode: oCC,
                                        originCityCode: oCityCode,
                                        originMultiPort: false,
                                        originDomestic: false,
                                        destinationAirportCode: dest,
                                        destinationCountryCode: dCC,
                                        destinationCityCode: dCityCode,
                                        destinationMultiPort: true,
                                        destinationDomestic: false,
                                        departureDate: dateDMY,
                                    }],
                                    savedDate: new Date().toISOString(),
                                    preselectedOptionDetails: [],
                                }),
                            });
                            clearTimeout(timer);
                            const status = resp.status;
                            const text = await resp.text();
                            try {
                                return {_status: status, _body: JSON.parse(text)};
                            } catch {
                                return {_status: status, _text: text.slice(0, 500)};
                            }
                        } catch(e) {
                            clearTimeout(timer);
                            return {_error: e.message};
                        }
                    }""",
                        [req.origin, req.destination, target_dmy, str(req.adults or 1),
                         o_cc, d_cc, o_city, d_city],
                    )
                    # Give _on_response a moment to process
                    await asyncio.sleep(0.5)
                    # Parse the evaluate result directly (more reliable than _on_response timing)
                    if not avail_data and isinstance(direct_result, dict):
                        body = direct_result.get("_body")
                        status = direct_result.get("_status")
                        if status == 200 and isinstance(body, dict) and "data" in body:
                            inner = body["data"]
                            if isinstance(inner, dict) and "originDestinationInformationList" in inner:
                                avail_data.update(inner)
                                opts = inner.get("originDestinationInformationList", [{}])[0]
                                n = len(opts.get("originDestinationOptionList", []))
                                logger.warning("TK: direct fetch returned %d options", n)
                            else:
                                logger.warning("TK: direct fetch 200 but data=%s, resp=%s",
                                               type(inner).__name__,
                                               str(body)[:500])
                        elif status == 428:
                            logger.warning("TK: direct fetch got 428 (PX crypto challenge)")
                        elif status == 403:
                            px_blocked = True
                            logger.warning("TK: direct fetch got 403 (PX blocked)")
                        elif status:
                            text_preview = direct_result.get("_text", str(body)[:200] if body else "")
                            logger.warning("TK: direct fetch status %s: %s", status, text_preview[:200])
                        elif direct_result.get("_error"):
                            logger.warning("TK: direct fetch error: %s", direct_result["_error"])
                except Exception as e:
                    logger.warning("TK: direct fetch exception: %s", e)
                finally:
                    direct_fetch_active = False

            # ── Path 3 fallback: homepage form fill ──
            if not avail_data and not px_blocked:
                logger.warning("TK: Paths 1-2 didn't trigger API, trying homepage form fill")
                await page.goto(
                    "https://www.turkishairlines.com/en-int/",
                    wait_until="domcontentloaded",
                    timeout=int(self.timeout * 1000),
                )
                await asyncio.sleep(5.0)
                await self._dismiss_cookies(page)
                await asyncio.sleep(1.0)
                await page.evaluate("""() => {
                    document.querySelectorAll(
                        '[role="dialog"], .modal-backdrop, .overlay, [class*="popup"]'
                    ).forEach(el => el.remove());
                }""")

                # One-way
                try:
                    ow = page.locator("span:has-text('One way')").first
                    if await ow.count() > 0:
                        await ow.click(timeout=5000)
                except Exception:
                    pass
                await asyncio.sleep(2.0)

                # Fill airports (works reliably)
                ok = await self._fill_airport(page, "#fromPort", req.origin)
                if ok:
                    await asyncio.sleep(0.8)
                    ok = await self._fill_airport(page, "#toPort", req.destination)

                if ok:
                    await asyncio.sleep(1.0)
                    # Try date fill (may fail — calendar won't open)
                    await self._fill_date(page, req.date_from)
                    await asyncio.sleep(0.5)

                    # Click search — route interceptor will fix the date even if _fill_date failed
                    for btn_text in ["Search flights", "Search"]:
                        try:
                            btn = page.locator(f"button:has-text('{btn_text}')").first
                            if await btn.count() > 0:
                                await btn.click(timeout=5000)
                                logger.warning("TK: clicked '%s' (homepage)", btn_text)
                                break
                        except Exception:
                            continue

                # Wait again
                deadline2 = time.monotonic() + 20
                while not avail_data and not px_blocked and time.monotonic() < deadline2:
                    await asyncio.sleep(0.5)

            # ── Final wait for crypto challenge resolution ──
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            while not avail_data and not px_blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            if px_blocked:
                logger.warning("TK: PerimeterX blocked, resetting profile")
                await _reset_profile()
                return self._empty(req)

            if not avail_data:
                logger.warning("TK: no availability data captured")
                return self._empty(req)

            offers = self._parse_availability(avail_data, req)
            offers.sort(key=lambda o: o.price)

            currency = avail_data.get("originalCurrency") or "TRY"
            elapsed = time.monotonic() - t0
            logger.info(
                "TK %s->%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"tk{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.warning("TK CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Cookies
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        try:
            btn = page.locator("#allowCookiesButton")
            if await btn.count() > 0:
                await btn.first.click(timeout=3000)
                logger.info("TK: cookies accepted")
                await asyncio.sleep(0.5)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Form fill
    # ------------------------------------------------------------------

    async def _fill_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill TK search form: origin, destination, date.
        
        Note: Date fill is optional — the route interceptor will fix the date
        in the API request regardless of what's shown in the UI.
        """
        # Origin
        ok = await self._fill_airport(page, "#fromPort", req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.8)

        # Destination
        ok = await self._fill_airport(page, "#toPort", req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.8)

        # Date — optional, route interceptor will fix it in API request anyway
        ok = await self._fill_date(page, req.date_from)
        if not ok:
            logger.warning("TK: date fill failed — route interceptor will fix the date")
            # Don't return False — airports are filled, search can proceed
        await asyncio.sleep(0.5)
        return True

    async def _fill_airport(self, page, selector: str, iata: str) -> bool:
        """Fill an airport typeahead and select first match."""
        try:
            field = page.locator(selector)
            # Wait for field to be visible before interacting
            try:
                await field.wait_for(state="visible", timeout=10000)
            except Exception:
                logger.warning("TK: %s not visible after 10s", selector)
                return False
            # Force-click bypasses overlay/interceptor issues
            await field.click(timeout=5000, force=True)
            await asyncio.sleep(0.3)
            await field.click(click_count=3)
            await asyncio.sleep(0.1)
            await field.fill("")
            await asyncio.sleep(0.1)
            await field.type(iata, delay=80)
            await asyncio.sleep(2.5)

            # Click first dropdown option
            opt = page.locator("[role='option']").first
            if await opt.count() > 0:
                await opt.click(timeout=3000)
                value = await field.input_value()
                logger.info("TK: filled %s -> %s", selector, value)
                return True

            # Keyboard fallback
            await field.press("ArrowDown")
            await asyncio.sleep(0.2)
            await field.press("Enter")
            await asyncio.sleep(0.5)
            value = await field.input_value()
            if value and len(value) > 1:
                logger.info("TK: filled %s -> %s (keyboard)", selector, value)
                return True

            logger.warning("TK: could not fill %s for %s", selector, iata)
            return False
        except Exception as e:
            logger.warning("TK: airport fill error %s: %s", selector, e)
            return False

    async def _fill_date(self, page, dep_date) -> bool:
        """Pick the departure date in TK's calendar widget.

        TK uses CSS modules with hashed class names. We try multiple
        strategies to trigger and interact with the date picker.
        """
        dt = _to_datetime(dep_date)
        target_day = str(dt.day)
        target_month = dt.strftime("%B")  # e.g. "June"
        target_year = str(dt.year)
        date_iso = dt.strftime("%Y-%m-%d")
        date_ddmmyyyy = dt.strftime("%d-%m-%Y")
        date_ddmmmyyyy = dt.strftime("%d %b %Y")  # e.g. "15 Jun 2026"
        month_year = f"{target_month} {target_year}"

        try:
            # Remove overlays first
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[role="dialog"], .modal-backdrop, .overlay, [class*="popup"], [class*="modal"]'
                ).forEach(el => el.remove());
            }""")

            # ── Step 1: Find and click the date trigger element ──
            # Look for the visible date area in the form (not just #bookerDatepicker)
            trigger_info = await page.evaluate("""() => {
                // Strategy 1: #bookerDatepicker inner clickables
                const dp = document.querySelector('#bookerDatepicker');
                const results = [];

                if (dp) {
                    // Find all visible leaf/interactive elements inside datepicker
                    const all = dp.querySelectorAll('*');
                    for (const el of all) {
                        if (el.offsetHeight > 0 && el.offsetWidth > 0) {
                            results.push({
                                tag: el.tagName, cls: (el.className || '').toString().slice(0, 60),
                                text: el.textContent?.trim().slice(0, 40),
                                h: el.offsetHeight, w: el.offsetWidth,
                                children: el.children.length,
                                id: el.id || '',
                            });
                        }
                    }
                }

                // Strategy 2: any element with "Dates" text nearby
                const allEls = document.querySelectorAll('span, div, label, p, button');
                const dateLabels = [];
                for (const el of allEls) {
                    const t = el.textContent?.trim() || '';
                    if (t === 'Dates' || t === 'Date' || t === 'Departure') {
                        dateLabels.push({
                            tag: el.tagName, cls: (el.className || '').toString().slice(0, 60),
                            text: t, h: el.offsetHeight,
                            parentTag: el.parentElement?.tagName,
                            parentCls: (el.parentElement?.className || '').toString().slice(0, 60),
                        });
                    }
                }

                return {
                    dpChildren: results.slice(0, 10),
                    dateLabels: dateLabels.slice(0, 5),
                    dpExists: !!dp,
                    dpH: dp?.offsetHeight || 0,
                };
            }""")
            logger.warning("TK: date area DOM: %s", trigger_info)

            # Click the date trigger — try multiple approaches
            calendar_opened = False

            # Try A: Click on visible elements inside #bookerDatepicker
            if trigger_info.get("dpExists"):
                # Click the container first
                dp = page.locator("#bookerDatepicker")
                try:
                    await dp.click(timeout=3000, force=True)
                    await asyncio.sleep(1.5)
                except Exception:
                    pass

                # Then click child elements that look interactive
                dp_children = trigger_info.get("dpChildren", [])
                for child in dp_children:
                    if child.get("children", 0) == 0 and child.get("h", 0) > 0:
                        # This is a leaf visible element — try clicking it
                        try:
                            tag = child["tag"].lower()
                            cls = child.get("cls", "")
                            if cls:
                                first_cls = cls.split()[0] if " " in cls else cls
                                el = page.locator(f"#{('bookerDatepicker')} {tag}.{first_cls}").first
                                if await el.count() > 0:
                                    await el.click(timeout=2000, force=True)
                                    await asyncio.sleep(1.5)
                                    break
                        except Exception:
                            continue

            # Try B: Click elements with "Dates" text
            try:
                dates_el = page.locator("text=Dates").first
                if await dates_el.count() > 0:
                    await dates_el.click(timeout=3000, force=True)
                    await asyncio.sleep(1.5)
            except Exception:
                pass

            # Check if calendar opened
            cal_check = await page.evaluate("""() => {
                // Broad check for any calendar-like structure that appeared
                const all = document.querySelectorAll('*');
                const found = [];
                for (const el of all) {
                    if (el.offsetHeight < 50) continue;
                    const cls = (el.className || '').toString().toLowerCase();
                    const role = (el.getAttribute('role') || '').toLowerCase();
                    const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                    if (cls.includes('calendar') || cls.includes('datepick') ||
                        cls.includes('monthview') || cls.includes('dayview') ||
                        role === 'grid' || role === 'dialog' ||
                        aria.includes('calendar') || aria.includes('date')) {
                        found.push({
                            tag: el.tagName, cls: el.className?.toString().slice(0, 80),
                            h: el.offsetHeight, role, text: el.textContent?.slice(0, 60),
                        });
                    }
                }
                // Also check for month/year headings that indicate a calendar is visible
                const headings = [];
                const months = ['January','February','March','April','May','June',
                               'July','August','September','October','November','December'];
                for (const el of all) {
                    if (el.offsetHeight > 0 && el.children.length < 3) {
                        const t = el.textContent?.trim() || '';
                        if (months.some(m => t.includes(m)) && /\\d{4}/.test(t) && t.length < 40) {
                            headings.push({
                                tag: el.tagName, text: t,
                                cls: el.className?.toString().slice(0, 60),
                            });
                        }
                    }
                }
                return { calendars: found.slice(0, 5), monthHeadings: headings.slice(0, 5) };
            }""")
            logger.warning("TK: after click — calendars: %s, headings: %s",
                          cal_check.get("calendars"), cal_check.get("monthHeadings"))

            if cal_check.get("calendars") or cal_check.get("monthHeadings"):
                calendar_opened = True
                return await self._navigate_calendar_and_click_day(
                    page, target_day, target_month, target_year, date_iso
                )

            # ── Step 2: Keyboard approach — Tab from destination field ──
            logger.warning("TK: calendar not detected, trying keyboard approach")

            # Click destination field first, then Tab to date
            dest_field = page.locator("#toPort")
            if await dest_field.count() > 0:
                await dest_field.click(timeout=3000, force=True)
                await asyncio.sleep(0.3)
                # Tab forward to the date field
                for _ in range(3):
                    await page.keyboard.press("Tab")
                    await asyncio.sleep(0.5)

                # Check what's focused now
                focused = await page.evaluate("""() => {
                    const el = document.activeElement;
                    return el ? {
                        tag: el.tagName, id: el.id,
                        cls: (el.className || '').toString().slice(0, 60),
                        type: el.type || '',
                        text: el.textContent?.trim().slice(0, 40),
                    } : null;
                }""")
                logger.warning("TK: after Tab×3, focused: %s", focused)

                # Try typing the date
                await page.keyboard.type(date_ddmmmyyyy, delay=50)
                await asyncio.sleep(1.0)
                await page.keyboard.press("Enter")
                await asyncio.sleep(1.5)

            # ── Step 3: React state injection ──
            injected = await page.evaluate("""(args) => {
                const [dateISO, dateDDMMYYYY, day, monthName, year] = args;

                // Find React fiber on #bookerDatepicker or form elements
                function getReactFiber(el) {
                    const key = Object.keys(el).find(k =>
                        k.startsWith('__reactFiber$') || k.startsWith('__reactInternalInstance$')
                    );
                    return key ? el[key] : null;
                }

                // Try to find and update React state
                const dp = document.querySelector('#bookerDatepicker');
                if (dp) {
                    const fiber = getReactFiber(dp);
                    if (fiber) {
                        // Walk up the fiber tree to find a component with date state
                        let f = fiber;
                        for (let i = 0; i < 20 && f; i++) {
                            if (f.memoizedProps) {
                                const props = f.memoizedProps;
                                // Check if this component has date-related props/state
                                if (props.onChange || props.onDateChange || props.onDayClick) {
                                    const dt = new Date(dateISO + 'T00:00:00');
                                    try {
                                        if (props.onChange) props.onChange(dt);
                                        else if (props.onDateChange) props.onDateChange(dt);
                                        else if (props.onDayClick) props.onDayClick(dt);
                                        return 'react-callback-' + i;
                                    } catch(e) {
                                        return 'react-callback-error-' + e.message;
                                    }
                                }
                            }
                            f = f.return;
                        }
                        return 'react-fiber-no-callback';
                    }
                    return 'no-fiber';
                }
                return 'no-dp';
            }""", [date_iso, date_ddmmyyyy, target_day, target_month, target_year])
            logger.warning("TK: React injection result: %s", injected)

            # ── Step 4: Direct date input injection (same as old Strategy A) ──
            date_inputs = await page.evaluate("""(args) => {
                const [dateISO, dateDDMMYYYY, dateDDMMMYYYY] = args;
                const inputs = document.querySelectorAll('input');
                const dateInputs = [...inputs].filter(i => {
                    const n = (i.name || '').toLowerCase();
                    const id = (i.id || '').toLowerCase();
                    return n.includes('date') || n.includes('departure') ||
                           id.includes('date') || id.includes('departure');
                });
                let result = [];
                for (const inp of dateInputs) {
                    const orig = inp.value;
                    const setter = Object.getOwnPropertyDescriptor(
                        window.HTMLInputElement.prototype, 'value'
                    ).set;
                    setter.call(inp, dateDDMMYYYY);
                    inp.dispatchEvent(new Event('input', { bubbles: true }));
                    inp.dispatchEvent(new Event('change', { bubbles: true }));
                    result.push({ id: inp.id, name: inp.name, was: orig, now: inp.value });
                }
                return result;
            }""", [date_iso, date_ddmmyyyy, date_ddmmmyyyy])
            if date_inputs:
                logger.warning("TK: date input injection: %s", date_inputs)

            # Even if calendar didn't visually open, the date might be set — return True
            # to allow the form submit attempt (which will quickly reveal if date wasn't set)
            logger.warning("TK: date fill completed (calendar_opened=%s)", calendar_opened)
            return True  # Optimistically proceed — let search button attempt reveal issues

        except Exception as e:
            logger.warning("TK: date fill error: %s — route interceptor will fix date", e)
            return True  # Route interceptor handles date, so proceed anyway

    async def _navigate_calendar_and_click_day(
        self, page, target_day, target_month, target_year, date_iso
    ) -> bool:
        """Navigate visible calendar to target month and click the day."""
        try:
            for click_idx in range(18):
                # Check current visible month(s)
                visible = await page.evaluate("""() => {
                    const all = document.querySelectorAll(
                        '[class*="calendar"] *, [class*="Calendar"] *, [class*="datepicker"] *'
                    );
                    const months = [];
                    const monthNames = ['January','February','March','April','May','June',
                                       'July','August','September','October','November','December'];
                    for (const el of all) {
                        if (el.children.length > 0) continue;
                        const text = (el.textContent || '').trim();
                        for (const mn of monthNames) {
                            if (text.includes(mn) && /\\d{4}/.test(text)) {
                                months.push(text);
                                break;
                            }
                        }
                    }
                    return [...new Set(months)];
                }""")

                target_str = f"{target_month} {target_year}"
                found = any(target_month in v and target_year in v for v in (visible or []))
                if found:
                    logger.warning("TK: calendar reached %s (click %d)", target_str, click_idx)
                    break

                if click_idx == 0:
                    logger.warning("TK: calendar shows: %s, need %s", visible, target_str)

                clicked_fwd = await page.evaluate("""() => {
                    const selectors = [
                        '.react-calendar__navigation__next-button',
                        'button[aria-label*="next" i]',
                        'button[aria-label*="Next"]',
                        'button[aria-label*="forward" i]',
                        '[class*="next"]',
                        '[class*="right"]',
                        '[class*="forward"]',
                    ];
                    for (const sel of selectors) {
                        const btns = document.querySelectorAll(sel);
                        for (const btn of btns) {
                            if (btn.offsetHeight > 0 && !btn.disabled && btn.tagName === 'BUTTON') {
                                btn.click();
                                return sel;
                            }
                        }
                    }
                    const buttons = document.querySelectorAll('button');
                    for (const btn of buttons) {
                        if (btn.offsetHeight > 0 && btn.querySelector('svg, [class*="arrow"], [class*="chevron"]')) {
                            const rect = btn.getBoundingClientRect();
                            const parent = btn.parentElement;
                            if (parent) {
                                const parentRect = parent.getBoundingClientRect();
                                if (rect.left > parentRect.left + parentRect.width / 2) {
                                    btn.click();
                                    return 'svg-right';
                                }
                            }
                        }
                    }
                    return null;
                }""")
                if not clicked_fwd:
                    logger.warning("TK: no forward button found at click %d", click_idx)
                    break
                await asyncio.sleep(0.8)
            else:
                logger.warning("TK: exhausted 18 calendar clicks, visible: %s", visible)

            await asyncio.sleep(0.5)

            # Click the target day
            clicked = await page.evaluate("""(args) => {
                const [targetDay, targetMonth, targetYear, dateISO] = args;

                const ariaPatterns = [
                    targetMonth + ' ' + targetDay + ', ' + targetYear,
                    targetDay + ' ' + targetMonth + ' ' + targetYear,
                    dateISO,
                ];
                for (const pat of ariaPatterns) {
                    const els = document.querySelectorAll('[aria-label]');
                    for (const el of els) {
                        if ((el.getAttribute('aria-label') || '').includes(pat) && !el.disabled) {
                            el.click();
                            return 'aria';
                        }
                    }
                }

                const calAreas = document.querySelectorAll(
                    '.react-calendar, [class*="calendar"], [class*="Calendar"], [class*="datepicker"]'
                );
                for (const area of calAreas) {
                    if (area.offsetHeight < 50) continue;
                    const cells = area.querySelectorAll('button, td, [role="gridcell"]');
                    for (const cell of cells) {
                        const text = cell.textContent.trim();
                        if (text === targetDay && !cell.disabled && cell.offsetHeight > 0) {
                            const section = cell.closest('table, [class*="month"], [class*="Month"]') || area;
                            const sectionText = section.textContent || '';
                            if (sectionText.includes(targetMonth)) {
                                cell.click();
                                return 'cal-area';
                            }
                        }
                    }
                }

                const allBtns = document.querySelectorAll('button:not([disabled])');
                for (const btn of allBtns) {
                    if (btn.textContent.trim() === targetDay && btn.offsetHeight > 0) {
                        const parent = btn.closest('[class*="calendar"], [class*="Calendar"], [class*="datepicker"], [role="grid"]');
                        if (parent) {
                            btn.click();
                            return 'brute';
                        }
                    }
                }

                return null;
            }""", [target_day, target_month, target_year, date_iso])

            if clicked:
                logger.warning("TK: selected date %s %s %s (%s)", target_day, target_month, target_year, clicked)
                return True

            logger.warning("TK: could not select date %s-%s-%s", target_year, target_month, target_day)
            return False
        except Exception as e:
            logger.warning("TK: calendar nav error: %s", e)
            return False

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_availability(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse availability response into FlightOffers."""
        offers: list[FlightOffer] = []
        currency = data.get("originalCurrency") or "TRY"

        odil = data.get("originDestinationInformationList", [])
        if not odil:
            logger.warning("TK parse: no originDestinationInformationList in data (keys: %s)",
                           list(data.keys())[:10])
            return offers

        option_list = odil[0].get("originDestinationOptionList", [])
        logger.warning("TK parse: %d options, currency=%s", len(option_list), currency)

        for i, opt in enumerate(option_list):
            try:
                if opt.get("soldOut"):
                    logger.warning("TK parse: option %d sold out", i)
                    continue

                sp = opt.get("startingPrice", {})
                price = sp.get("amount", 0)
                cur = sp.get("currencyCode", currency)
                if price <= 0:
                    logger.warning("TK parse: option %d price<=0 (sp=%s)", i, sp)
                    continue

                seg_list = opt.get("segmentList", [])
                if not seg_list:
                    logger.warning("TK parse: option %d no segments (keys=%s)", i, list(opt.keys())[:8])
                    continue

                segments = []
                for seg in seg_list:
                    fc = seg.get("flightCode", {})
                    airline_code = fc.get("airlineCode", "TK")
                    flight_number = fc.get("flightNumber", "")
                    dep_dt = _parse_tk_datetime(seg["departureDateTime"])
                    arr_dt = _parse_tk_datetime(seg["arrivalDateTime"])
                    dur_ms = seg.get("journeyDurationInMillis", 0)

                    segments.append(FlightSegment(
                        airline=airline_code,
                        airline_name="Turkish Airlines" if airline_code == "TK" else airline_code,
                        flight_no=f"{airline_code}{flight_number}",
                        origin=seg["departureAirportCode"],
                        destination=seg["arrivalAirportCode"],
                        departure=dep_dt,
                        arrival=arr_dt,
                        duration_seconds=dur_ms // 1000,
                        cabin_class="economy",
                        aircraft=seg.get("equipmentName", ""),
                    ))

                total_dur = opt.get("journeyDuration", 0) // 1000
                stopovers = max(len(segments) - 1, 0)

                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=total_dur,
                    stopovers=stopovers,
                )

                oid = opt.get("optionId", 0)
                offer_id = hashlib.md5(
                    f"tk_{req.origin}_{req.destination}_{oid}_{price}".encode()
                ).hexdigest()[:12]

                all_airlines = list({s.airline for s in segments})

                offers.append(FlightOffer(
                    id=f"tk_{offer_id}",
                    price=price,
                    currency=cur,
                    price_formatted=f"{price:,.0f} {cur}",
                    outbound=route,
                    inbound=None,
                    airlines=[("Turkish Airlines" if a == "TK" else a) for a in all_airlines],
                    owner_airline="TK",
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source="turkish_direct",
                    source_tier="free",
                ))
            except Exception as parse_err:
                logger.warning("TK parse: option %d error: %s (keys=%s)", i, parse_err,
                               list(opt.keys())[:8])
                continue

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        adults = req.adults or 1
        # TK uses DD.MM.YYYY in some URL forms; also try ISO as fallback
        date_dot = dt.strftime("%d.%m.%Y")
        return (
            f"https://www.turkishairlines.com/en-int/flights/booking/"
            f"availability-international/"
            f"?originAirportCode={req.origin}"
            f"&destinationAirportCode={req.destination}"
            f"&departureDate={date_dot}"
            f"&adult={adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"tk{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
