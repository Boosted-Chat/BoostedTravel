"""
Spirit Airlines Playwright scraper -- navigates to spirit.com and searches flights.

Spirit (IATA: NK) is a US ultra-low-cost carrier operating domestic and
Caribbean/Latin America routes. Heavy Akamai/PerimeterX bot protection.

Strategy:
1. Navigate to spirit.com homepage
2. Dismiss cookie/overlay banners
3. Fill search form (origin, destination, date, one-way)
4. Intercept API responses (availability/shopping endpoints)
5. Parse results -> FlightOffers
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-US", "en-GB", "en-CA"]
_TIMEZONES = [
    "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles", "America/Phoenix",
]

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9463
_chrome_proc = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Connect to a real Chrome instance via CDP (launched once, reused)."""
    global _chrome_proc, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from connectors.browser import get_or_launch_cdp
        _user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-cdp-spirit")
        _browser, _chrome_proc = await get_or_launch_cdp(_CDP_PORT, _user_data)
        logger.info("Spirit: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _browser


class SpiritConnectorClient:
    """Spirit Playwright scraper -- homepage form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(3):
            # Fresh context per attempt (real Chrome via CDP defeats PX better)
            browser = await _get_browser()
            context = await browser.new_context(
                viewport=random.choice(_VIEWPORTS),
                locale=random.choice(_LOCALES),
                timezone_id=random.choice(_TIMEZONES),
            )
            try:
                result = await self._attempt_search(context, req, t0)
                if result and result.total_results > 0:
                    return result
                if attempt < 2:
                    logger.info("Spirit: attempt %d returned no results, retrying with fresh context", attempt + 1)
            except Exception as e:
                logger.warning("Spirit: attempt %d error: %s", attempt + 1, e)
            finally:
                await context.close()
            await asyncio.sleep(2.0)

        logger.warning("Spirit: all attempts exhausted for %s->%s", req.origin, req.destination)
        return self._empty(req)

    async def _attempt_search(self, context, req: FlightSearchRequest, t0: float) -> FlightSearchResponse:
        """Single search attempt within a fresh browser context."""
        try:
            from playwright_stealth import stealth_async
            page = await context.new_page()
            await stealth_async(page)
        except ImportError:
            page = await context.new_page()

        captured_data: dict = {}
        api_event = asyncio.Event()
        px_blocked = {"token": False}

        async def on_response(response):
            try:
                url = response.url
                # Detect PX block on token endpoint
                if "/api/prod-token/" in url and response.status == 403:
                    px_blocked["token"] = True
                if response.status == 200 and (
                    "/api/prod-availability/api/availability/v3/search" in url
                    or ("/availability" in url and "/search" in url)
                ):
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        if data and isinstance(data, dict):
                            captured_data["json"] = data
                            api_event.set()
                            logger.info("Spirit: captured search API response")
            except Exception:
                pass

        page.on("response", on_response)

        logger.info("Spirit: loading homepage for %s->%s", req.origin, req.destination)
        await page.goto(
            "https://www.spirit.com/",
            wait_until="domcontentloaded",
            timeout=int(self.timeout * 1000),
        )
        await asyncio.sleep(12.0)  # Let PerimeterX fully initialize

        await self._dismiss_cookies(page)
        await asyncio.sleep(0.5)
        await self._dismiss_cookies(page)

        # Check if PX already blocked the token (Angular fetches it on page load)
        if px_blocked["token"]:
            logger.warning("Spirit: PerimeterX blocked token endpoint — session poisoned")
            return self._empty(req)

        await self._set_one_way(page)
        await asyncio.sleep(0.5)

        ok = await self._fill_airport_field(page, "From", req.origin, 0)
        if not ok:
            logger.warning("Spirit: origin fill failed")
            return self._empty(req)
        await asyncio.sleep(0.5)

        ok = await self._fill_airport_field(page, "To", req.destination, 1)
        if not ok:
            logger.warning("Spirit: destination fill failed")
            return self._empty(req)
        await asyncio.sleep(0.5)

        # Check PX again after form fill (Angular may refresh token)
        if px_blocked["token"]:
            logger.warning("Spirit: PerimeterX blocked token during form fill")
            return self._empty(req)

        ok = await self._fill_date(page, req)
        if not ok:
            logger.warning("Spirit: date fill failed")
            return self._empty(req)
        await asyncio.sleep(0.3)

        await self._click_search(page)

        remaining = max(self.timeout - (time.monotonic() - t0), 10)
        try:
            await asyncio.wait_for(api_event.wait(), timeout=min(remaining, 25))
        except asyncio.TimeoutError:
            if px_blocked["token"]:
                logger.warning("Spirit: search timed out (PX blocked token)")
            else:
                logger.warning("Spirit: timed out waiting for API response")
            return self._empty(req)

        data = captured_data.get("json", {})
        if not data:
            return self._empty(req)

        elapsed = time.monotonic() - t0
        offers = self._parse_response(data, req)
        return self._build_response(offers, req, elapsed)

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept all cookies", "Accept All", "Accept", "I agree",
            "Got it", "OK", "Close", "Dismiss",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                continue
        # Force-remove modals, PX captcha iframes, and overlays
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    'ngb-modal-window, ngb-modal-backdrop, [class*="modal-backdrop"], ' +
                    '#px-captcha-modal, [id*="px-captcha"], ' +
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="onetrust"], [id*="onetrust"]'
                ).forEach(el => el.remove());
                document.body.classList.remove('modal-open');
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        """Select One Way trip type via the custom dropdown toggle."""
        try:
            # Spirit uses a checkbox toggle to open the trip type dropdown
            await page.evaluate("""() => {
                const toggle = document.getElementById('dropdown-toggle-controler-toggleId');
                if (toggle) toggle.click();
            }""")
            await asyncio.sleep(0.8)
            # Click the "One Way" label that appears
            ow_label = page.locator("label").filter(has_text=re.compile(r"one\s*way", re.IGNORECASE))
            if await ow_label.count() > 0:
                await ow_label.first.click(timeout=5000)
                await asyncio.sleep(0.5)
                logger.info("Spirit: selected One Way trip type")
                return
            # Fallback: JS click
            await page.evaluate("""() => {
                const labels = document.querySelectorAll('label');
                for (const l of labels) {
                    if (l.textContent.trim().match(/one\\s*way/i)) { l.click(); return; }
                }
            }""")
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("Spirit: trip type error: %s", e)

    async def _fill_airport_field(self, page, label: str, iata: str, index: int) -> bool:
        """Fill airport field using Spirit's station picker (requires city name, not IATA)."""
        field_id = "flight-OriginStationCode" if index == 0 else "flight-DestinationStationCode"
        label_cls = "label.fromStation" if index == 0 else "label.toStation"
        city_name = await self._iata_to_city(page, iata)
        try:
            await self._dismiss_cookies(page)
            # Click label to open the station picker
            await page.evaluate(f"() => document.querySelector('{label_cls}')?.click()")
            await asyncio.sleep(0.5)
            # Focus the input via JS (bypasses label overlay)
            await page.evaluate(f"""() => {{
                const el = document.getElementById('{field_id}');
                if (el) {{ el.focus(); el.select(); }}
            }}""")
            await asyncio.sleep(0.3)
            await page.keyboard.press("Control+a")
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.3)
            await page.keyboard.type(city_name, delay=80)
            await asyncio.sleep(2.5)
            # Click station suggestion using Playwright (not JS — Angular model needs real click)
            suggestion = page.locator(
                "div.station-picker-typeahead__station-list[role='button']"
            ).filter(has_text=re.compile(rf"\b{re.escape(iata)}\b", re.IGNORECASE))
            if await suggestion.count() > 0:
                await suggestion.first.click(timeout=5000)
                logger.info("Spirit: selected %s (%s) for %s", iata, city_name, label)
                return True
            # Fallback: try typing IATA directly
            await page.evaluate(f"""() => {{
                const el = document.getElementById('{field_id}');
                if (el) {{ el.focus(); el.value = ''; }}
            }}""")
            await asyncio.sleep(0.3)
            await page.keyboard.type(iata, delay=80)
            await asyncio.sleep(2.5)
            suggestion = page.locator(
                "div.station-picker-typeahead__station-list[role='button']"
            ).filter(has_text=re.compile(rf"\b{re.escape(iata)}\b", re.IGNORECASE))
            if await suggestion.count() > 0:
                await suggestion.first.click(timeout=5000)
                logger.info("Spirit: selected %s for %s (IATA fallback)", iata, label)
                return True
            logger.warning("Spirit: no suggestion found for %s/%s", iata, city_name)
            return False
        except Exception as e:
            logger.debug("Spirit: %s field error: %s", label, e)
            return False

    async def _iata_to_city(self, page, iata: str) -> str:
        """Look up city name for an IATA code via Spirit's station API."""
        try:
            stations = await page.evaluate("""() =>
                fetch('/api/prod-station/api/resources/v2/stations', {credentials: 'same-origin'})
                    .then(r => r.ok ? r.json() : null).catch(() => null)
            """)
            if stations and isinstance(stations, dict):
                items = stations.get("data", [])
                for s in (items if isinstance(items, list) else []):
                    code = s.get("stationCode", "")
                    if code.upper() == iata.upper():
                        # shortName is like "Fort Lauderdale, FL" — extract city part
                        name = s.get("shortName") or s.get("fullName") or ""
                        city = name.split(",")[0].strip() if name else ""
                        if city:
                            logger.debug("Spirit: station API: %s -> %s", iata, city)
                            return city
        except Exception as e:
            logger.debug("Spirit: station API lookup failed: %s", e)
        return iata

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill departure date via Spirit's calendar-selection trigger + bs-datepicker."""
        target = req.date_from
        try:
            await self._dismiss_cookies(page)
            # Open calendar by clicking the calendar-selection div
            cal_trigger = page.locator("div.calendar-selection").first
            try:
                await cal_trigger.click(timeout=5000)
            except Exception:
                await self._dismiss_cookies(page)
                await asyncio.sleep(0.3)
                await cal_trigger.click(force=True, timeout=5000)
            await asyncio.sleep(1)

            # Navigate to the target month (month name and year are separate button.current elements)
            target_month = target.strftime("%B")
            target_year = str(target.year)
            for _ in range(12):
                headers = await page.evaluate("""() => {
                    const el = document.querySelector('bs-datepicker-container, bs-daterangepicker-container');
                    if (!el || !el.offsetHeight) return [];
                    return Array.from(el.querySelectorAll('button.current')).map(b => b.textContent.trim());
                }""")
                month_ok = any(target_month.lower() in h.lower() for h in headers)
                year_ok = any(target_year in h for h in headers)
                if month_ok and year_ok:
                    break
                nxt = page.locator(
                    "bs-datepicker-container .next, bs-daterangepicker-container .next"
                ).first
                if await nxt.count() > 0:
                    await nxt.click(timeout=2000)
                    await asyncio.sleep(0.5)
                else:
                    break

            # Click the target day (exclude .is-other-month days)
            day_str = str(target.day)
            await page.evaluate(f"""() => {{
                const containers = document.querySelectorAll(
                    'bs-datepicker-container, bs-daterangepicker-container'
                );
                for (const c of containers) {{
                    const spans = c.querySelectorAll('td span');
                    for (const s of spans) {{
                        if (s.textContent.trim() === '{day_str}' && !s.closest('.is-other-month')) {{
                            s.click();
                            return;
                        }}
                    }}
                }}
            }}""")
            await asyncio.sleep(0.5)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
            logger.info("Spirit: departure date set to %s", target.strftime("%m/%d/%Y"))
            return True
        except Exception as e:
            logger.warning("Spirit: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        await self._dismiss_cookies(page)
        # Remove datepicker containers that may overlay the search button
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    'bs-datepicker-container, bs-daterangepicker-container'
                ).forEach(el => el.remove());
            }""")
        except Exception:
            pass
        await asyncio.sleep(0.3)
        btn = page.locator("button[type='submit']").filter(
            has_text=re.compile(r"search", re.IGNORECASE)
        )
        if await btn.count() > 0:
            try:
                await btn.first.click(timeout=10000)
                logger.info("Spirit: clicked search")
                return
            except Exception:
                pass
        # Fallback: any submit button
        try:
            await page.locator("button[type='submit']").first.click(timeout=5000)
        except Exception:
            await page.keyboard.press("Enter")

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Spirit availability/v3/search response.

        Structure: data.trips[].journeysAvailable[].fares{<key>: {details: {passengerFares: [{fareAmount}]}}}
        """
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = []
        if isinstance(data, dict):
            d = data.get("data", data)
            trips = d.get("trips", []) if isinstance(d, dict) else []
        if not isinstance(trips, list):
            trips = []

        for trip in trips:
            if not isinstance(trip, dict):
                continue
            journeys = trip.get("journeysAvailable", [])
            if not isinstance(journeys, list):
                continue
            for journey in journeys:
                if not isinstance(journey, dict) or not journey.get("isSelectable", True):
                    continue
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)
        return offers

    def _parse_journey(self, journey: dict, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        """Parse a single journey (one itinerary option) into a FlightOffer."""
        fares = journey.get("fares", {})
        if not isinstance(fares, dict) or not fares:
            return None

        # Find cheapest fare
        best_price = float("inf")
        for fare_val in fares.values():
            det = fare_val.get("details", {}) if isinstance(fare_val, dict) else {}
            for pf in det.get("passengerFares", []):
                amt = pf.get("fareAmount")
                if isinstance(amt, (int, float)) and 0 < amt < best_price:
                    best_price = amt
        if best_price == float("inf"):
            return None

        # Build segments from journey.segments
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []
        for seg in (segments_raw if isinstance(segments_raw, list) else []):
            des = seg.get("designator", {})
            ident = seg.get("identifier", {})
            carrier = ident.get("carrierCode", "NK")
            flight_num = ident.get("identifier", "")
            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Spirit Airlines" if carrier == "NK" else carrier,
                flight_no=f"{carrier}{flight_num}",
                origin=des.get("origin", req.origin),
                destination=des.get("destination", req.destination),
                departure=self._parse_dt(des.get("departure", "")),
                arrival=self._parse_dt(des.get("arrival", "")),
                cabin_class="M",
            ))

        if not segments:
            # Fallback: use journey-level designator
            des = journey.get("designator", {})
            segments.append(FlightSegment(
                airline="NK", airline_name="Spirit Airlines", flight_no="",
                origin=des.get("origin", req.origin),
                destination=des.get("destination", req.destination),
                departure=self._parse_dt(des.get("departure", "")),
                arrival=self._parse_dt(des.get("arrival", "")),
                cabin_class="M",
            ))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        jk = journey.get("journeyKey", f"{time.monotonic()}")
        return FlightOffer(
            id=f"nk_{hashlib.md5(str(jk).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency="USD",
            price_formatted=f"${best_price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Spirit"],
            owner_airline="NK",
            booking_url=booking_url,
            is_locked=False,
            source="spirit_direct",
            source_tier="free",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Spirit %s->%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"spirit{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="USD", offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.spirit.com/book/flights?from={req.origin}"
            f"&to={req.destination}&date={dep}&pax={req.adults}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"spirit{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
