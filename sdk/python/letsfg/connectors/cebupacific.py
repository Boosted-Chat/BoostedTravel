"""
Cebu Pacific CDP scraper -- direct URL navigation + SOAR API interception.

Cebu Pacific (IATA: 5J) is the Philippines' largest LCC. Uses an Angular SPA
with the SOAR API (soar.cebupacificair.com/ceb-omnix-proxy-v3) behind heavy
Akamai Bot Manager protection.

Strategy:
1. Launch Chrome via subprocess with specific flags that bypass Akamai
   (standard Playwright launch / headless / off-screen are all detected)
2. Connect via CDP (remote-debugging-port)
3. Navigate directly to the select-flight URL (skips form filling)
4. Intercept SOAR API /availability response
5. Parse routes[].journeys[] → FlightOffers
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import subprocess
import platform
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
from .browser import find_chrome, proxy_chrome_args, bandwidth_saving_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

# ── Chrome launch flags that bypass Akamai Bot Manager ────────────────────
# These match Playwright MCP's launch config. Standard Playwright launch()
# or headless modes are all detected by Akamai and served a page without
# <app-root>, preventing the Angular SPA from bootstrapping.
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
    *bandwidth_saving_args(),
]

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9459
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
    """Launch Chrome with Akamai-bypassing flags and connect via CDP."""
    global _chrome_proc, _browser, _pw_instance
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser

        from .browser import find_chrome

        chrome = find_chrome()
        user_data = os.path.join(
            os.environ.get("TEMP", "/tmp"), "chrome-cdp-cebupacific"
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
            "CebuPacific: Chrome ready via CDP (port %d, pid %d)",
            _CDP_PORT,
            _chrome_proc.pid,
        )
        return _browser


class CebuPacificConnectorClient:
    """CebuPacific scraper — direct URL + SOAR API interception."""

    def __init__(self, timeout: float = 45.0):
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
        t0 = time.monotonic()
        browser = await _get_browser()
        # Use the default browser context and its first page — Akamai blocks
        # new contexts and new pages opened via CDP; only the initial page
        # in the default context inherits Chrome's full browser properties.
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()
        await auto_block_if_proxied(page)

        try:
            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url
                    if "soar.cebupacificair.com" not in url:
                        return
                    if response.status != 200:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    body = await response.body()
                    if len(body) < 200:
                        return

                    import json as _json

                    data = _json.loads(body)
                    if not isinstance(data, dict):
                        return

                    # Primary: /availability endpoint (has journeys)
                    if "availability" in url and "routes" in data:
                        captured_data["availability"] = data
                        api_event.set()
                        logger.info(
                            "CebuPacific: captured availability (%d bytes)",
                            len(body),
                        )
                    # Secondary: /farecache endpoint (calendar low fares)
                    elif "farecache" in url:
                        captured_data["farecache"] = data
                except Exception:
                    pass

            page.on("response", on_response)

            search_url = self._build_search_url(req)
            logger.info(
                "CebuPacific: searching %s→%s on %s",
                req.origin,
                req.destination,
                req.date_from.strftime("%Y-%m-%d"),
            )
            await page.goto(
                search_url,
                wait_until="load",
                timeout=int(self.timeout * 1000),
            )

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("CebuPacific: timed out waiting for SOAR availability")

                # One retry: re-open the same deep-link once in case the first
                # navigation only solved anti-bot/cookie state.
                try:
                    api_event.clear()
                    await page.goto(
                        search_url,
                        wait_until="load",
                        timeout=int(self.timeout * 1000),
                    )
                    retry_remaining = max(12, int(self.timeout * 0.5))
                    await asyncio.wait_for(api_event.wait(), timeout=retry_remaining)
                    logger.info("CebuPacific: availability captured on retry navigation")
                except asyncio.TimeoutError:
                    logger.warning("CebuPacific: retry navigation also timed out")

            data = captured_data.get("availability")
            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_availability(data, req)
                if offers:
                    return self._build_response(offers, req, elapsed)

            return self._empty(req)

        except Exception as e:
            logger.error("CebuPacific error: %s", e)
            return self._empty(req)
        finally:
            # Navigate back to blank to free resources, but don't close the page
            try:
                page.remove_listener("response", on_response)
                await page.goto("about:blank", wait_until="commit", timeout=5000)
            except Exception:
                pass

    # ── URL builders ─────────────────────────────────────────────────────

    @staticmethod
    def _build_search_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.cebupacificair.com/en-PH/booking/select-flight"
            f"?o1={req.origin}&d1={req.destination}"
            f"&adt={req.adults}&chd=0&inl=0&inf=0&dd1={dep}"
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.cebupacificair.com/en-PH/booking/select-flight"
            f"?o1={req.origin}&d1={req.destination}"
            f"&adt={req.adults}&chd=0&inl=0&inf=0&dd1={dep}"
        )

    # ── SOAR availability parser ─────────────────────────────────────────

    def _parse_availability(
        self, data: dict, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        currency = data.get("currencyCode", "PHP")
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for route in data.get("routes", []):
            for journey in route.get("journeys", []):
                offer = self._parse_journey(journey, currency, req, booking_url)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_journey(
        self,
        journey: dict,
        currency: str,
        req: FlightSearchRequest,
        booking_url: str,
    ) -> Optional[FlightOffer]:
        fare_total = journey.get("fareTotal")
        if not fare_total or fare_total <= 0:
            return None

        designator = journey.get("designator", {})
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []

        for seg in segments_raw:
            seg_des = seg.get("designator", {})
            ident = seg.get("identifier", {})
            carrier = ident.get("carrierCode", "5J")
            flight_num = ident.get("identifier", "")
            dur = seg.get("duration", {})
            duration_min = dur.get("hour", 0) * 60 + dur.get("minutes", 0)

            # Determine airline name from carrier code
            airline_name = {
                "5J": "Cebu Pacific",
                "DG": "Cebgo",
                "2D": "AirSWIFT",
            }.get(carrier, "Cebu Pacific")

            segments.append(
                FlightSegment(
                    airline=carrier,
                    airline_name=airline_name,
                    flight_no=f"{carrier}{flight_num}",
                    origin=seg_des.get("origin", req.origin),
                    destination=seg_des.get("destination", req.destination),
                    departure=self._parse_dt(seg_des.get("departure")),
                    arrival=self._parse_dt(seg_des.get("arrival")),
                    cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy"),
                )
            )

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

        journey_key = journey.get("journeyKey", "")
        offer_id = hashlib.md5(
            f"5j_{journey_key}_{fare_total}".encode()
        ).hexdigest()[:12]

        avail_count = journey.get("availableCount")
        airlines = list({s.airline_name for s in segments})

        return FlightOffer(
            id=f"5j_{offer_id}",
            price=round(fare_total, 2),
            currency=currency,
            price_formatted=f"{fare_total:,.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=airlines,
            owner_airline="5J",
            booking_url=booking_url,
            is_locked=False,
            source="cebupacific_direct",
            source_tier="free",
            availability_seats=avail_count,
        )

    # ── Response builders ────────────────────────────────────────────────

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "CebuPacific %s→%s: %d offers in %.1fs",
            req.origin,
            req.destination,
            len(offers),
            elapsed,
        )
        h = hashlib.md5(
            f"cebupacific{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"5j_rt_{o.id}_{i.id}",
                    price=round(o.price + i.price, 2),
                    currency=o.currency,
                    outbound=o.outbound,
                    inbound=i.outbound,
                    owner_airline=o.owner_airline,
                    airlines=list(set(o.airlines + i.airlines)),
                    source=o.source,
                    booking_url=o.booking_url,
                    conditions=o.conditions,
                ))
        combos.sort(key=lambda x: x.price)
        return combos[:20]

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
    ) -> dict | None:
        """Return Cebu Pacific (5J) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("5J")
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
            f"cebupacific{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M",
        ):
            try:
                return datetime.strptime(s[: len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)
