"""
Alaska Airlines direct connector — Playwright + flightresults API.

Alaska Airlines (IATA: AS) is a major US carrier based in Seattle (SEA).
Post-Hawaiian Airlines merger, operates 300+ routes across the US, Hawaii,
Mexico, Costa Rica, Belize, and Canada.

Strategy:
  1. Launch headed Playwright Chrome → load alaskaair.com/search/ to establish
     session cookies (no WAF/CAPTCHA, SvelteKit SPA).
  2. Use in-browser fetch() to POST /search/api/flightresults with IATA codes
     and date. The API returns full itineraries with segments, fare classes,
     pricing, and seat availability.
  3. Parse rows → build FlightOffer per itinerary with cheapest economy fare.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
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
from .browser import auto_block_if_proxied, get_default_proxy

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    global _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        pw = await async_playwright().start()
        launch_kw: dict = {
            "headless": False,
            "channel": "chrome",
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
            ],
        }
        _proxy = get_default_proxy()
        if _proxy:
            launch_kw["proxy"] = _proxy
        try:
            _browser = await pw.chromium.launch(**launch_kw)
        except Exception:
            launch_kw.pop("channel", None)
            _browser = await pw.chromium.launch(**launch_kw)
        logger.info("Alaska: headed Chrome launched")
        return _browser


# Fare class → cabin mapping
_CABIN_MAP = {
    "SAVER": "economy",
    "MAIN": "economy",
    "PREMIUM": "premium_economy",
    "FIRST": "first",
    "REFUNDABLE_MAIN": "economy",
    "REFUNDABLE_PREMIUM": "premium_economy",
    "REFUNDABLE_FIRST": "first",
}

# Preference order for fare selection by cabin
_ECONOMY_FARES = ["SAVER", "MAIN", "REFUNDABLE_MAIN"]
_PREMIUM_FARES = ["PREMIUM", "REFUNDABLE_PREMIUM"]
_FIRST_FARES = ["FIRST", "REFUNDABLE_FIRST"]

# Live bag/seat pricing per fare class (from Alaska fare policy)
_AS_FARE_BAGS: dict[str, dict] = {
    "SAVER": {
        "carry_on": 0.0,
        "carry_on_note": "Carry-on included (Saver fare)",
        "checked_bag": 30.0,
        "checked_bag_note": "1st checked bag +USD 30 (Saver — no free bag included)",
        "seat": None,
        "seat_note": "Seat assigned at check-in (Saver — no advance selection)",
    },
    "MAIN": {
        "carry_on": 0.0,
        "carry_on_note": "Carry-on included (Main fare)",
        "checked_bag": 0.0,
        "checked_bag_note": "1st checked bag included free (Main fare)",
        "seat": 0.0,
        "seat_note": "Standard seat included (Main — preferred from +USD 15)",
    },
    "REFUNDABLE_MAIN": {
        "carry_on": 0.0,
        "carry_on_note": "Carry-on included (Refundable Main)",
        "checked_bag": 0.0,
        "checked_bag_note": "1st checked bag included free (Refundable Main)",
        "seat": 0.0,
        "seat_note": "Standard seat included (preferred from +USD 15)",
    },
    "PREMIUM": {
        "carry_on": 0.0,
        "carry_on_note": "Carry-on included (Premium Class)",
        "checked_bag": 0.0,
        "checked_bag_note": "2 checked bags included free (Premium Class)",
        "seat": 0.0,
        "seat_note": "Premium seat included",
    },
    "REFUNDABLE_PREMIUM": {
        "carry_on": 0.0,
        "carry_on_note": "Carry-on included (Refundable Premium)",
        "checked_bag": 0.0,
        "checked_bag_note": "2 checked bags included free (Refundable Premium)",
        "seat": 0.0,
        "seat_note": "Premium seat included",
    },
    "FIRST": {
        "carry_on": 0.0,
        "carry_on_note": "Carry-on included (First Class)",
        "checked_bag": 0.0,
        "checked_bag_note": "2 checked bags included free (First Class)",
        "seat": 0.0,
        "seat_note": "Seat included (First Class)",
    },
    "REFUNDABLE_FIRST": {
        "carry_on": 0.0,
        "carry_on_note": "Carry-on included (Refundable First Class)",
        "checked_bag": 0.0,
        "checked_bag_note": "2 checked bags included free (Refundable First Class)",
        "seat": 0.0,
        "seat_note": "Seat included (Refundable First Class)",
    },
}


class AlaskaConnectorClient:
    """Alaska Airlines connector — Playwright + flightresults API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            offers = await self._search_via_browser(req)
            elapsed = time.monotonic() - t0
            logger.info(
                "Alaska: %s→%s on %s — %d offers in %.1fs",
                req.origin, req.destination, req.date_from, len(offers), elapsed,
            )
            if offers:
                segs = offers[0].outbound.segments if offers[0].outbound else []
                anc_origin = segs[0].origin if segs else req.origin
                anc_dest = segs[-1].destination if segs else req.destination
                try:
                    ancillary = await asyncio.wait_for(
                        self._fetch_ancillaries(
                            anc_origin, anc_dest,
                            req.date_from.isoformat(), req.adults, "USD",
                        ),
                        timeout=10.0,
                    )
                    if ancillary:
                        self._apply_ancillaries(offers, ancillary)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
                except Exception as _anc_err:
                    logger.debug("AS: ancillary fetch error: %s", _anc_err)
            return FlightSearchResponse(
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
                search_id=f"alaska_{req.origin}_{req.destination}_{req.date_from}_{req.return_from or ''}",
            )
        except Exception as e:
            logger.error("Alaska search error: %s", e)
            return self._empty(req)

    async def _search_via_browser(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="America/Los_Angeles",
            service_workers="block",
        )
        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            logger.info("Alaska: loading search page for session cookies")
            await page.goto(
                "https://www.alaskaair.com/search/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            # Wait for SvelteKit SPA to hydrate
            await page.wait_for_load_state("networkidle", timeout=15000)

            date_str = req.date_from.strftime("%Y-%m-%d")

            result = await page.evaluate(
                """async ([origin, destination, dateStr, adults]) => {
                    try {
                        const body = {
                            origins: [origin],
                            destinations: [destination],
                            dates: [dateStr],
                            numADTs: adults,
                            numINFs: 0,
                            numCHDs: 0,
                            fareView: 'Default',
                            onba: false,
                            dnba: false,
                            discount: { code: '', type: 'NONE', memo: '' },
                            isAlaska: true,
                            isMobileApp: false,
                            sliceId: 0,
                            umnrAgeGroup: 'NONE',
                            isAddingToAdultRes: false,
                            lockFare: false,
                            sessionID: '',
                            solutionIDs: [],
                            solutionSetIDs: [],
                            qpxcVersion: '',
                            trackingTags: [],
                            isAwards: false,
                            isMultiCity: false,
                        };
                        const r = await fetch('/search/api/flightresults', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(body),
                        });
                        if (!r.ok) return { error: r.status };
                        return await r.json();
                    } catch (e) {
                        return { error: e.message };
                    }
                }""",
                [req.origin, req.destination, date_str, req.adults],
            )

            if not result or result.get("error"):
                logger.warning("Alaska: API error: %s", result)
                return []

            ob_offers = self._parse_rows(result, req)

            # --- RT: second fetch for inbound ---
            if not req.return_from or not ob_offers:
                return ob_offers

            ret_str = req.return_from.strftime("%Y-%m-%d")
            ib_result = await page.evaluate(
                """async ([origin, destination, dateStr, adults]) => {
                    try {
                        const body = {
                            origins: [origin],
                            destinations: [destination],
                            dates: [dateStr],
                            numADTs: adults,
                            numINFs: 0,
                            numCHDs: 0,
                            fareView: 'Default',
                            onba: false,
                            dnba: false,
                            discount: { code: '', type: 'NONE', memo: '' },
                            isAlaska: true,
                            isMobileApp: false,
                            sliceId: 0,
                            umnrAgeGroup: 'NONE',
                            isAddingToAdultRes: false,
                            lockFare: false,
                            sessionID: '',
                            solutionIDs: [],
                            solutionSetIDs: [],
                            qpxcVersion: '',
                            trackingTags: [],
                            isAwards: false,
                            isMultiCity: false,
                        };
                        const r = await fetch('/search/api/flightresults', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(body),
                        });
                        if (!r.ok) return { error: r.status };
                        return await r.json();
                    } catch (e) {
                        return { error: e.message };
                    }
                }""",
                [req.destination, req.origin, ret_str, req.adults],
            )

            if not ib_result or ib_result.get("error"):
                return ob_offers

            ib_offers = self._parse_rows(ib_result, req)
            if not ib_offers:
                return ob_offers

            return self._combine_rt(ob_offers, ib_offers, req)
        finally:
            await context.close()

    def _parse_rows(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        rows = data.get("rows") or []
        max_stops = req.max_stopovers
        booking_url = (
            f"https://www.alaskaair.com/search/results"
            f"?A={req.adults}&OD1={req.origin},{req.destination},"
            f"{req.date_from.strftime('%Y-%m-%d')}"
        )
        offers: list[FlightOffer] = []

        for row in rows:
            segments_data = row.get("segments") or []
            solutions = row.get("solutions") or {}
            stopovers = len(segments_data) - 1

            if stopovers > max_stops:
                continue

            # Find cheapest fare for requested cabin
            _cabin_fares = {
                "M": _ECONOMY_FARES, "W": _PREMIUM_FARES,
                "C": _FIRST_FARES, "F": _FIRST_FARES,
            }.get(req.cabin_class or "M", _ECONOMY_FARES)
            fare = None
            selected_fare_key: str | None = None
            for fk in _cabin_fares:
                if fk in solutions:
                    fare = solutions[fk]
                    selected_fare_key = fk
                    break
            if not fare:
                continue

            price = fare.get("grandTotal")
            if not price or price <= 0:
                continue

            seats = fare.get("seatsRemaining")
            cabin_class = _CABIN_MAP.get(selected_fare_key or "MAIN", "economy")

            # Build segments
            segments: list[FlightSegment] = []
            for seg_data in segments_data:
                pub = seg_data.get("publishingCarrier") or {}
                carrier_code = pub.get("carrierCode", "AS")
                carrier_name = pub.get("carrierFullName", "Alaska Airlines")
                flight_num = pub.get("flightNumber", "")

                dep_str = seg_data.get("departureTime", "")
                arr_str = seg_data.get("arrivalTime", "")
                try:
                    dep_dt = datetime.fromisoformat(dep_str)
                    arr_dt = datetime.fromisoformat(arr_str)
                except (ValueError, TypeError):
                    continue

                duration_min = seg_data.get("duration", 0)
                aircraft = seg_data.get("aircraft", "")

                segments.append(FlightSegment(
                    airline=carrier_code,
                    airline_name=carrier_name,
                    flight_no=f"{carrier_code}{flight_num}",
                    origin=seg_data.get("departureStation", req.origin),
                    destination=seg_data.get("arrivalStation", req.destination),
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=duration_min * 60,
                    cabin_class=cabin_class,
                    aircraft=aircraft,
                ))

            if not segments:
                continue

            total_duration = row.get("duration", 0) * 60
            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_duration,
                stopovers=stopovers,
            )

            # Unique ID from route + schedule + price
            key = (
                f"{req.origin}{req.destination}"
                f"{segments[0].departure.isoformat()}"
                f"{price}"
            )
            offer_id = f"as_{hashlib.md5(key.encode()).hexdigest()[:12]}"

            # Collect all carriers
            airlines = list({s.airline_name for s in segments})

            offer = FlightOffer(
                id=offer_id,
                price=round(price, 2),
                currency="USD",
                price_formatted=f"${price:.2f}",
                outbound=route,
                inbound=None,
                airlines=airlines,
                owner_airline="AS",
                booking_url=booking_url,
                is_locked=False,
                source="alaska_direct",
                source_tier="free",
                availability_seats=seats if isinstance(seats, int) else None,
            )

            # Inline bag/seat pricing from live fare class (SAVER/MAIN/FIRST/etc.)
            fare_bags = _AS_FARE_BAGS.get(selected_fare_key or "SAVER")
            if fare_bags:
                offer.conditions["carry_on"] = fare_bags["carry_on_note"]
                offer.conditions["checked_bag"] = fare_bags["checked_bag_note"]
                if fare_bags["seat_note"]:
                    offer.conditions["seat"] = fare_bags["seat_note"]
                offer.bags_price["carry_on"] = fare_bags["carry_on"]
                offer.bags_price["checked_bag"] = fare_bags["checked_bag"]
                if fare_bags["seat"] is not None:
                    offer.bags_price["seat_selection"] = fare_bags["seat"]

            offers.append(offer)

        return offers

    def _combine_rt(
        self,
        ob_offers: list[FlightOffer],
        ib_offers: list[FlightOffer],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Cross-multiply OB x IB one-way offers into RT combos."""
        ob_sorted = sorted(ob_offers, key=lambda o: o.price)[:15]
        ib_sorted = sorted(ib_offers, key=lambda o: o.price)[:10]
        date_str = req.date_from.strftime("%Y-%m-%d")
        ret_str = req.return_from.strftime("%Y-%m-%d")
        bk_url = (
            f"https://www.alaskaair.com/search/results"
            f"?A={req.adults}&OD1={req.origin},{req.destination},{date_str}"
            f"&OD2={req.destination},{req.origin},{ret_str}"
        )
        combined: list[FlightOffer] = []
        for ob in ob_sorted:
            for ib in ib_sorted:
                total = round(ob.price + ib.price, 2)
                key = f"{ob.id}_{ib.id}"
                cid = f"as_rt_{hashlib.md5(key.encode()).hexdigest()[:12]}"
                combined.append(FlightOffer(
                    id=cid,
                    price=total,
                    currency="USD",
                    price_formatted=f"${total:.2f}",
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    airlines=list(set(ob.airlines + ib.airlines)),
                    owner_airline="AS",
                    booking_url=bk_url,
                    is_locked=False,
                    source="alaska_direct",
                    source_tier="free",
                ))
        combined.sort(key=lambda o: o.price)
        return combined

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
            search_id=f"alaska_{req.origin}_{req.destination}_{req.date_from}_{req.return_from or ''}",
        )

    # ------------------------------------------------------------------
    # Ancillary pricing
    # ------------------------------------------------------------------

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        cache_key = f"as_{origin}_{dest}_{date_str}"
        now = time.time()
        if cache_key in _ancillary_cache:
            ts, cached = _ancillary_cache[cache_key]
            if now - ts < _ANCILLARY_CACHE_TTL:
                return cached
        # Alaska: Saver fare has no free checked bags (first bag ~$30).
        # Main fare includes 1 free checked bag. Carry-on is included on all fares.
        # These are fallback values only — inline per-fare pricing is set in _parse_rows.
        result: dict = {
            "carry_on_from": 0.0,
            "carry_on_note": "Carry-on included on all Alaska fares",
            "checked_from": 30.0,
            "checked_note": "1st checked bag from +USD 30 (Saver fare — Main fare includes 1 bag free)",
            "seat_from": 15.0,
            "seat_note": "Seat from +USD 15 (Saver/Main — preferred seats extra)",
            "currency": "USD",
        }
        _ancillary_cache[cache_key] = (now, result)
        return result

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        carry_on_from = ancillary.get("carry_on_from")
        carry_on_note = ancillary.get("carry_on_note")
        checked_from = ancillary.get("checked_from")
        checked_note = ancillary.get("checked_note")
        seat_from = ancillary.get("seat_from")
        seat_note = ancillary.get("seat_note")
        anc_currency = ancillary.get("currency", "USD")
        for offer in offers:
            ccy_ok = offer.currency.upper() == anc_currency.upper()
            # Skip if already set inline (inline per-fare pricing takes precedence)
            if carry_on_note and "carry_on" not in offer.conditions:
                offer.conditions["carry_on"] = carry_on_note
            if checked_note and "checked_bag" not in offer.conditions:
                offer.conditions["checked_bag"] = checked_note
            if seat_note and "seat" not in offer.conditions:
                offer.conditions["seat"] = seat_note
            if carry_on_from == 0.0 and "carry_on" not in offer.bags_price:
                offer.bags_price["carry_on"] = 0.0
            if checked_from == 0.0 and "checked_bag" not in offer.bags_price:
                offer.bags_price["checked_bag"] = 0.0
            if seat_from == 0.0 and "seat_selection" not in offer.bags_price:
                offer.bags_price["seat_selection"] = 0.0
