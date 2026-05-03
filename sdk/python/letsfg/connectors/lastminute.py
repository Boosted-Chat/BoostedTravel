"""
Lastminute connector — European OTA group (CDP Chrome + API interception).

Covers Lastminute.com, Bravofly, and Volagratis (all same backend).
Lastminute Group is a major European consolidator with access to net fares
from multiple GDS sources.

Strategy (CDP Chrome + API response interception):
1. Launch real system Chrome via --remote-debugging-port.
2. Connect via Playwright CDP (persistent session, cookies carry over).
3. Navigate to lastminute.com homepage → fill flight search form.
4. Intercept /s/flights/search/api/v3/availability JSON response.
5. Parse flights → FlightOffers.
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
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, bandwidth_saving_args, disable_background_networking_args, apply_cdp_url_blocking

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min

_BASE = "https://www.lastminute.com"
_CDP_PORT = 9464
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".lastminute_chrome_data"
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
        _context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
        )
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

        # Try connecting to existing Chrome on the port
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
            _pw_instance = pw
            logger.info("Lastminute: connected to existing Chrome on port %d", _CDP_PORT)
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
            *bandwidth_saving_args(),
            *disable_background_networking_args(),
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
        logger.info("Lastminute: Chrome launched on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


async def _dismiss_cookies(page) -> None:
    # Lastminute uses iubenda cookie consent banner — use JS click to bypass overlay issues
    try:
        dismissed = await page.evaluate("""() => {
            const banner = document.getElementById('iubenda-cs-banner');
            if (!banner) return false;
            const btn = banner.querySelector('button.iubenda-cs-accept-btn') || banner.querySelector('.iubenda-cs-accept-btn');
            if (btn) { btn.click(); return true; }
            // Try any button in the banner
            const buttons = banner.querySelectorAll('button');
            for (const b of buttons) {
                const text = b.textContent.toLowerCase();
                if (text.includes('accept') || text.includes('agree') || text.includes('ok')) {
                    b.click(); return true;
                }
            }
            return false;
        }""")
        if dismissed:
            await asyncio.sleep(1.0)
            logger.debug("Lastminute: dismissed iubenda cookie banner via JS")
            return
    except Exception:
        pass
    # Fallback: generic labels
    for label in ["Accept", "Accept all", "Accept All", "I agree", "Got it", "OK"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue


class LastminuteConnectorClient:
    """Lastminute.com — European OTA, CDP Chrome + API response interception."""

    def __init__(self, timeout: float = 55.0):
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
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, ob_result.currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        return {
            "checked_bag": "not included – add-on from ~35 € at eDreams checkout (OTA markup; book airline direct for lower fees)",
            "bags_note": "personal item free; cabin bag add-on from ~30 € at eDreams checkout",
            "seat_note": "seat add-on from ~10 € at eDreams checkout; skip for random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_price")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["cabin_bag"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from == 0.0:
                offer.bags_price["cabin_bag"] = 0.0
            if checked_from == 0.0:
                offer.bags_price["checked_bag"] = 0.0

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")

        context = await _get_context()
        page = await context.new_page()
        await apply_cdp_url_blocking(page)

        # Intercept the availability API response
        search_data: dict = {}

        async def _on_response(response):
            url = response.url
            if "/api/v3/availability" in url and response.status == 200:
                try:
                    data = await response.json()
                    if isinstance(data, dict) and ("flights" in data or "numberOfResults" in data):
                        search_data.update(data)
                        logger.info("Lastminute: captured availability API (%d bytes)", len(json.dumps(data)))
                except Exception as e:
                    logger.debug("Lastminute: failed to parse availability response: %s", e)

        page.on("response", _on_response)

        try:
            # Navigate to homepage
            logger.info("Lastminute: loading homepage for %s→%s on %s", req.origin, req.destination, date_str)
            await page.goto(f"{_BASE}/flights", wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            await asyncio.sleep(2.0)

            # Dismiss cookies
            await _dismiss_cookies(page)
            await asyncio.sleep(0.5)

            # Fill search form
            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("Lastminute: form fill failed")
                return self._empty(req)

            # Click search button — Lastminute uses "Find" button with aria-label="Find"
            clicked = False
            for sel in (
                'button[aria-label="Find"]',
                'button:has-text("Find")',
                'button:has-text("Search")',
                'button[type="submit"]',
            ):
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0:
                        await btn.click(timeout=5000)
                        clicked = True
                        logger.info("Lastminute: clicked search button via %s", sel)
                        break
                except Exception:
                    continue
            if not clicked:
                logger.warning("Lastminute: could not click search button")
                return self._empty(req)

            # Wait for navigation to results page
            try:
                await page.wait_for_url("**/flights/search/**", timeout=15000)
                logger.info("Lastminute: navigated to %s", page.url)
            except Exception:
                logger.debug("Lastminute: didn't navigate to search results, URL: %s", page.url)

            # Wait for the intercepted availability response
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while not search_data and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            if not search_data:
                logger.warning("Lastminute: no availability data captured")
                return self._empty(req)

            offers = self._parse(search_data, req, date_str)
            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("Lastminute %s→%s: %d offers in %.1fs (CDP Chrome)", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"lastminute{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "EUR",
                offers=offers, total_results=len(offers),
            )
        except Exception as e:
            logger.error("Lastminute CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill the lastminute.com flight search form."""
        target = req.date_from

        # Lastminute does not have a one-way toggle button on the Flights page.
        # By default, leaving the return date empty creates a one-way search.

        # --- Origin ---
        ok = await self._fill_airport(page, "Departure", req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        # --- Destination ---
        ok = await self._fill_airport(page, "Destination", req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        # --- Date ---
        ok = await self._fill_date(page, target)
        if not ok:
            return False

        return True

    async def _fill_airport(self, page, label: str, iata: str) -> bool:
        """Fill an airport field using aria-label selectors."""
        try:
            # Lastminute uses aria-label="Departure" / "Destination"
            field = page.locator(f'[aria-label="{label}"]').first
            if await field.count() == 0:
                # Fallback: try input with placeholder
                field = page.get_by_placeholder(label).first
            if await field.count() == 0:
                logger.warning("Lastminute: could not find %s field", label)
                return False

            # Use JS click to bypass any overlays (iubenda residual)
            try:
                await page.evaluate("""(label) => {
                    const el = document.querySelector(`[aria-label="${label}"]`);
                    if (el) el.click();
                }""", label)
                await asyncio.sleep(0.5)
            except Exception:
                await field.click(timeout=3000)
                await asyncio.sleep(0.3)

            await field.fill(iata)
            await asyncio.sleep(1.5)

            # Select the first suggestion
            for sel in (
                '[role="option"]',
                '[role="listbox"] li',
                'ul[role="listbox"] > *',
                '[class*="suggestion"] li',
                '[class*="autocomplete"] li',
                '[class*="dropdown"] li',
            ):
                try:
                    item = page.locator(sel).first
                    if await item.count() > 0:
                        await item.click(timeout=3000)
                        logger.info("Lastminute: selected %s for %s", iata, label)
                        return True
                except Exception:
                    continue

            # Fallback: press Enter to accept first suggestion
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.3)
            logger.info("Lastminute: pressed Enter for %s (%s)", label, iata)
            return True
        except Exception as e:
            logger.warning("Lastminute: %s field error: %s", label, e)
            return False

    async def _fill_date(self, page, target) -> bool:
        """Select the departure date in the date picker.

        Lastminute uses a calendar with day buttons that have aria-labels like:
        'Thursday, 15 June 2026' (format: '{weekday}, {day} {month_name} {year}').
        Navigation via buttons with aria-label='Next month' / 'Previous month'.
        The date field itself is opened via [aria-label='Dates'].
        """
        try:
            # Click the date field to open the picker
            date_field = page.locator('[aria-label="Dates"]')
            if await date_field.count() > 0:
                await date_field.first.click(timeout=3000)
                await asyncio.sleep(1.0)
                logger.debug("Lastminute: opened date picker")
            else:
                # Fallback: click any visible date-like element
                for sel in ('[aria-label*="date" i]', '[class*="DateInput"]'):
                    try:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.click(timeout=3000)
                            await asyncio.sleep(1.0)
                            break
                    except Exception:
                        continue

            # Build the target aria-label: "Thursday, 15 June 2026"
            # Windows strftime doesn't support %-d, use manual approach
            day_no_pad = str(target.day)
            month_name = target.strftime("%B")
            year = target.year
            weekday = target.strftime("%A")
            target_label = f"{weekday}, {day_no_pad} {month_name} {year}"

            for nav_attempt in range(12):
                # Try clicking the target date by exact aria-label
                try:
                    day_btn = page.locator(f'button[aria-label="{target_label}"]')
                    if await day_btn.count() > 0:
                        await day_btn.first.click(timeout=2000)
                        logger.info("Lastminute: selected date %s", target_label)
                        await asyncio.sleep(0.5)
                        return True
                except Exception:
                    pass

                # Also try partial match: aria-label containing day + month
                try:
                    partial = page.locator(f'button[aria-label*="{day_no_pad} {month_name} {year}"]')
                    if await partial.count() > 0:
                        await partial.first.click(timeout=2000)
                        logger.info("Lastminute: selected date (partial) %s %s %s", day_no_pad, month_name, year)
                        await asyncio.sleep(0.5)
                        return True
                except Exception:
                    pass

                # Navigate to next month
                try:
                    next_btn = page.locator('button[aria-label="Next month"]')
                    if await next_btn.count() > 0:
                        await next_btn.click(timeout=2000)
                        await asyncio.sleep(0.5)
                    else:
                        break
                except Exception:
                    break

            logger.warning("Lastminute: could not select date %s", target_label)
            return False
        except Exception as e:
            logger.warning("Lastminute: date fill error: %s", e)
            return False

    def _parse(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse Lastminute v3/availability API response.

        Actual structure (discovered via probe):
        {
            "flights": [
                {
                    "itinerary": [
                        {
                            "legOptions": [
                                {
                                    "duration": 130,
                                    "flightSegments": [...],
                                    "segments": [...],
                                    "price": {"amount": {...}}
                                }
                            ]
                        }
                    ],
                    "prices": {"selected": ...},
                    "tripType": "OW",
                }
            ],
            "dataRegistry": {
                "airlines": [...],  # list of airline objects
                "airportCities": [...]
            }
        }
        """
        offers: list[FlightOffer] = []

        # Build airline lookup from dataRegistry (it's a list, not a dict)
        registry = data.get("dataRegistry") or {}
        airlines_list = registry.get("airlines") or []
        airlines_map = {}
        for a in airlines_list:
            if isinstance(a, dict):
                code = a.get("code") or a.get("iata") or ""
                name = a.get("name") or code
                if code:
                    airlines_map[code] = name

        # Currency from searchOptions
        currency = "EUR"
        search_opts = data.get("searchOptions") or {}
        if isinstance(search_opts, dict):
            currency = search_opts.get("currency") or "EUR"

        flights = data.get("flights") or []
        if not isinstance(flights, list):
            return offers

        for item in flights[:30]:
            try:
                # Extract itinerary → legOptions → segments
                itinerary = item.get("itinerary") or []
                if not isinstance(itinerary, list) or not itinerary:
                    continue

                first_itin = itinerary[0]
                leg_options = first_itin.get("legOptions") or []
                if not isinstance(leg_options, list) or not leg_options:
                    continue

                first_leg = leg_options[0]

                # Price — from prices.selected.price.amount.perPassenger (total incl taxes)
                # Structure: {"amountAsNumber": 2498, "decimalDigits": 2, "currencyCode": "GBP"}
                # → 2498 / 10^2 = 24.98 GBP
                price = 0
                price_currency = currency

                # Try prices.selected first (total price incl taxes)
                prices = item.get("prices") or {}
                selected = prices.get("selected") or {}
                if isinstance(selected, dict):
                    sel_price = selected.get("price") or {}
                    sel_amount = sel_price.get("amount") or {}
                    per_pax = sel_amount.get("perPassenger") or {}
                    if per_pax.get("amountAsNumber"):
                        decimals = per_pax.get("decimalDigits", 2)
                        price = per_pax["amountAsNumber"] / (10 ** decimals)
                        price_currency = per_pax.get("currencyCode") or currency

                # Fallback: legOption.price.amount.perPassenger (base fare only)
                if price <= 0:
                    price_obj = first_leg.get("price") or {}
                    leg_amount = price_obj.get("amount") or {}
                    per_pax = leg_amount.get("perPassenger") or {}
                    if per_pax.get("amountAsNumber"):
                        decimals = per_pax.get("decimalDigits", 2)
                        price = per_pax["amountAsNumber"] / (10 ** decimals)
                        price_currency = per_pax.get("currencyCode") or currency

                if price <= 0:
                    continue

                # Segments — from legOption.segments or legOption.flightSegments
                seg_data = first_leg.get("segments") or first_leg.get("flightSegments") or []
                if not isinstance(seg_data, list) or not seg_data:
                    continue

                segments: list[FlightSegment] = []
                for seg in seg_data:
                    carrier_code = seg.get("displayAirlineCode") or seg.get("operatingAirlineCode") or seg.get("marketingCarrier") or ""
                    carrier_name = seg.get("displayAirlineName") or airlines_map.get(carrier_code, carrier_code)

                    flight_no = seg.get("flightNumber") or seg.get("number") or ""
                    dep_airport = seg.get("departureAirportCode") or seg.get("departureAirport") or req.origin
                    arr_airport = seg.get("arrivalAirportCode") or seg.get("arrivalAirport") or req.destination
                    dep_dt = _parse_dt(seg.get("departureTime") or seg.get("departureDateTime"))
                    arr_dt = _parse_dt(seg.get("arrivalTime") or seg.get("arrivalDateTime"))
                    dur = seg.get("duration") or 0

                    segments.append(FlightSegment(
                        airline=carrier_name or carrier_code, flight_no=f"{carrier_code}{flight_no}",
                        origin=dep_airport, destination=arr_airport,
                        departure=dep_dt, arrival=arr_dt,
                        duration_seconds=int(dur) * 60 if dur else 0,
                    ))

                if not segments:
                    continue

                total_dur = first_leg.get("duration") or sum(s.duration_seconds // 60 for s in segments)
                total_dur_sec = int(total_dur) * 60 if isinstance(total_dur, (int, float)) else 0
                route = FlightRoute(segments=segments, total_duration_seconds=total_dur_sec, stopovers=max(0, len(segments) - 1))
                oid = hashlib.md5(f"lm_{req.origin}{req.destination}{date_str}{price}{segments[0].flight_no}".encode()).hexdigest()[:12]

                _lm_cabin = {"M": "economy", "W": "premium", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                offers.append(FlightOffer(
                    id=f"lm_{oid}", price=round(price, 2), currency=price_currency,
                    price_formatted=f"{price:.2f} {price_currency}",
                    outbound=route, inbound=None,
                    airlines=list({s.airline for s in segments if s.airline}),
                    owner_airline=segments[0].airline if segments else "Lastminute",
                    booking_url=f"{_BASE}/flights/{req.origin}-{req.destination}/{date_str}/1adults/{_lm_cabin}/oneway",
                    is_locked=False, source="lastminute_ota", source_tier="free",
                ))
            except Exception as e:
                logger.debug("Lastminute parse error: %s", e)

        return offers

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
                    id=f"rt_lm_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"lastminute{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="EUR", offers=[], total_results=0,
        )
