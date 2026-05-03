"""
Frontier Airlines hybrid scraper -- curl_cffi SSR extraction with Playwright fallback.

Frontier (IATA: F9) is a US ultra-low-cost carrier operating domestic and
select international routes from Denver and other US hubs.

Strategy (hybrid):
1. PRIMARY: curl_cffi GET to booking.flyfrontier.com/Flight/InternalSelect
   with Chrome TLS fingerprint -- server returns 1.4MB HTML with FlightData
   JSON embedded in an inline <script> tag (HTML-entity encoded). ~2-3s.
2. FALLBACK: Playwright headed Chrome with stealth if curl_cffi fails
   (PX challenge, network error, etc.)
3. Parse journeys[0].flights[] for prices (standardFare/economyFare), legs, stops
4. Return FlightOffers sorted by price
"""

from __future__ import annotations

import asyncio
import hashlib
import html as html_mod
import json
import logging
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

try:
    from curl_cffi.requests import AsyncSession
except ImportError:
    AsyncSession = None

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import stealth_args, auto_block_if_proxied, get_curl_cffi_proxies

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
    "America/Denver", "America/New_York", "America/Chicago",
    "America/Los_Angeles", "America/Phoenix",
]

_MAX_ATTEMPTS = 3

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800

_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Shared headed Chromium (launched once, reused across searches)."""
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from .browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("Frontier: browser launched")
        return _browser


class FrontierConnectorClient:
    """Frontier hybrid scraper -- curl_cffi SSR first, Playwright fallback."""

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
                        req.date_from.isoformat(), req.adults, ob_result.currency,
                    ),
                    timeout=15.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("F9: ancillary fetch timed out")
            except Exception as _anc_err:
                logger.debug("F9: ancillary fetch error: %s", _anc_err)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # --- PRIMARY: curl_cffi SSR extraction ---
        try:
            offers = await self._search_via_api(req)
            if offers:
                elapsed = time.monotonic() - t0
                logger.info(
                    "Frontier %s->%s: %d offers in %.1fs (curl_cffi SSR)",
                    req.origin, req.destination, len(offers), elapsed,
                )
                return self._build_response(offers, req, elapsed)
            logger.warning("Frontier %s->%s: curl_cffi returned no offers, trying Playwright",
                           req.origin, req.destination)
        except Exception as e:
            logger.warning("Frontier %s->%s: curl_cffi failed (%s), trying Playwright",
                           req.origin, req.destination, e)

        # --- FALLBACK: Playwright ---
        return await self._search_via_playwright(req, t0)

    # ------------------------------------------------------------------ #
    #  PRIMARY PATH: curl_cffi SSR                                        #
    # ------------------------------------------------------------------ #

    async def _search_via_api(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        adults = getattr(req, "adults", 1) or 1
        dep = req.date_from.strftime("%m/%d/%Y")
        url = (
            f"https://booking.flyfrontier.com/Flight/InternalSelect"
            f"?o1={req.origin}&d1={req.destination}&dd1={dep}"
            f"&ADT={adults}&mon=true&promo="
        )
        if req.return_from:
            ret = req.return_from.strftime("%m/%d/%Y")
            url += f"&dr1={ret}"

        async with AsyncSession(impersonate="chrome131", proxies=get_curl_cffi_proxies()) as s:
            resp = await s.get(url, timeout=15)

        if resp.status_code != 200:
            logger.warning("Frontier SSR: HTTP %d", resp.status_code)
            return None

        page_html = resp.text
        m = re.search(r"FlightData\s*=\s*'([\s\S]*?)';", page_html)
        if not m:
            logger.warning("Frontier SSR: FlightData var not found in %d chars", len(page_html))
            return None

        decoded = html_mod.unescape(m.group(1))
        try:
            data = json.loads(decoded)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("Frontier SSR: JSON parse error: %s", e)
            return None

        offers = self._parse_response(data, req)

        # Cache live bundle ancillary pricing from SSR response
        bundle_anc = FrontierConnectorClient._parse_bundle_ancillary(data)
        if bundle_anc:
            _anc_key = f"f9_{req.origin}_{req.destination}_{req.date_from}"
            _ancillary_cache[_anc_key] = (time.time(), bundle_anc)

        return offers

    # ------------------------------------------------------------------ #
    #  FALLBACK PATH: Playwright                                          #
    # ------------------------------------------------------------------ #

    async def _search_via_playwright(
        self, req: FlightSearchRequest, t0: float
    ) -> FlightSearchResponse:
        dep = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        search_url = (
            f"https://booking.flyfrontier.com/Flight/InternalSelect"
            f"?o1={req.origin}&d1={req.destination}&dd1={dep}"
            f"&ADT={adults}&mon=true&promo="
        )

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_pw_search(search_url, req)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "Frontier %s->%s: %d offers in %.1fs (Playwright fallback)",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    return self._build_response(offers, req, elapsed)
                logger.warning(
                    "Frontier: attempt %d/%d blocked by PX or empty",
                    attempt, _MAX_ATTEMPTS,
                )
            except Exception as e:
                logger.warning("Frontier: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    async def _attempt_pw_search(
        self, url: str, req: FlightSearchRequest
    ) -> Optional[list[FlightOffer]]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
        )
        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await auto_block_if_proxied(page)
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()
                await auto_block_if_proxied(page)

            logger.info("Frontier: loading %s", url[:100])
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3)

            title = await page.title()
            if "denied" in title.lower() or "blocked" in title.lower():
                return None

            flight_data = await self._extract_flight_data(page)
            if not flight_data:
                return None

            offers = self._parse_response(flight_data, req)

            # Cache live bundle ancillary pricing from Playwright-extracted FlightData
            bundle_anc = FrontierConnectorClient._parse_bundle_ancillary(flight_data)
            if bundle_anc:
                _anc_key = f"f9_{req.origin}_{req.destination}_{req.date_from}"
                _ancillary_cache[_anc_key] = (time.time(), bundle_anc)

            return offers
        finally:
            await context.close()

    async def _extract_flight_data(self, page) -> Optional[dict]:
        """Extract FlightData JSON from the page's inline <script> tag."""
        raw = await page.evaluate(r"""() => {
            const scripts = document.querySelectorAll('script[type="text/javascript"]');
            for (const s of scripts) {
                const t = s.textContent || '';
                const m = t.match(/FlightData\s*=\s*'([\s\S]*?)';/);
                if (m) return m[1];
            }
            return null;
        }""")
        if not raw:
            return None
        decoded = html_mod.unescape(raw)
        try:
            return json.loads(decoded)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("Frontier: FlightData JSON parse error: %s", e)
            return None

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        journeys = data.get("journeys") or []

        # Parse outbound flights from journeys[0]
        outbound_offers: list[FlightOffer] = []
        if journeys:
            for flight in (journeys[0].get("flights") or []):
                offer = self._parse_single_flight(flight, req, booking_url)
                if offer:
                    outbound_offers.append(offer)

        # Parse inbound flights from journeys[1] if present (round-trip)
        inbound_offers: list[FlightOffer] = []
        if len(journeys) > 1:
            for flight in (journeys[1].get("flights") or []):
                offer = self._parse_single_flight(flight, req, booking_url)
                if offer:
                    inbound_offers.append(offer)

        # Build RT combos if we have both directions
        if outbound_offers and inbound_offers:
            outbound_offers.sort(key=lambda o: o.price)
            inbound_offers.sort(key=lambda o: o.price)
            for ob in outbound_offers[:15]:
                for ib in inbound_offers[:10]:
                    rt_price = round(ob.price + ib.price, 2)
                    rt_key = f"{ob.id}_{ib.id}"
                    offers.append(FlightOffer(
                        id=f"f9_{hashlib.md5(rt_key.encode()).hexdigest()[:12]}",
                        price=rt_price,
                        currency="USD",
                        price_formatted=f"${rt_price:.2f}",
                        outbound=ob.outbound,
                        inbound=ib.outbound,
                        airlines=["Frontier"],
                        owner_airline="F9",
                        booking_url=booking_url,
                        is_locked=False,
                        source="frontier_direct",
                        source_tier="free",
                    ))
        else:
            offers.extend(outbound_offers)

        return offers

    def _parse_single_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        price = self._extract_best_price(flight)
        if price is None or price <= 0:
            return None

        legs_raw = flight.get("legs") or []
        if isinstance(legs_raw, str):
            try:
                legs_raw = json.loads(legs_raw)
            except (json.JSONDecodeError, ValueError):
                legs_raw = []

        _f9_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segments: list[FlightSegment] = []
        for leg in legs_raw:
            segments.append(self._build_segment(leg, _f9_cabin))
        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int(
                (segments[-1].arrival - segments[0].departure).total_seconds()
            )

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = (
            flight.get("standardFareKey")
            or flight.get("baseFareKey")
            or f"{req.origin}_{req.destination}_{time.monotonic()}"
        )
        return FlightOffer(
            id=f"f9_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency="USD",
            price_formatted=f"${price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Frontier"],
            owner_airline="F9",
            booking_url=booking_url,
            is_locked=False,
            source="frontier_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        for key in [
            "standardFare",
            "basicStandardFare",
            "economyFare",
            "basicEconomyFare",
            "discountDenFare",
            "bizedFare",
        ]:
            val = flight.get(key)
            if val is not None:
                try:
                    v = float(val)
                    if v > 0:
                        return v
                except (TypeError, ValueError):
                    pass
        return None

    @staticmethod
    def _build_segment(leg: dict, cabin_class: str = "economy") -> FlightSegment:
        dep_str = leg.get("departureDate") or ""
        arr_str = leg.get("arrivalDate") or ""
        flight_no = str(leg.get("flightNumber") or "")
        origin = leg.get("departureStation") or ""
        destination = leg.get("arrivalStation") or ""
        return FlightSegment(
            airline="F9",
            airline_name="Frontier Airlines",
            flight_no=flight_no,
            origin=origin,
            destination=destination,
            departure=FrontierConnectorClient._parse_dt(dep_str),
            arrival=FrontierConnectorClient._parse_dt(arr_str),
            cabin_class=cabin_class,
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        h = hashlib.md5(
            f"frontier{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=offers,
            total_results=len(offers),
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

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        url = (
            f"https://booking.flyfrontier.com/Flight/InternalSelect"
            f"?o1={req.origin}&d1={req.destination}&dd1={dep}"
            f"&ADT={adults}&mon=true&promo="
        )
        if req.return_from:
            ret = req.return_from.strftime("%Y-%m-%d")
            url += f"&dr1={ret}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"frontier{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )

    # ------------------------------------------------------------------
    # Ancillary pricing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_bundle_ancillary(data: dict) -> dict | None:
        """Extract live WORKS bundle price from Frontier FlightData SSR response.

        The WORKS bundle (Economy) covers carry-on + first checked bag together.
        ``economyBundleFare`` is the per-passenger add-on price over the base fare.
        """
        try:
            journeys = data.get("journeys") or []
            bundle_price: float | None = None
            for journey in journeys:
                for flight in (journey.get("flights") or []):
                    for key in ("economyBundleFare", "premiumBundleFare", "bizedBundleFare"):
                        val = flight.get(key)
                        if val is None:
                            continue
                        try:
                            v = float(val)
                        except (TypeError, ValueError):
                            continue
                        if v > 0 and (bundle_price is None or v < bundle_price):
                            bundle_price = v
                if bundle_price is not None:
                    break  # use only the first journey's data
            if bundle_price:
                note = (
                    f"WORKS bundle from +USD {bundle_price:.0f} "
                    "(carry-on + 1 checked bag — cheapest bag option)"
                )
                return {
                    "carry_on_from": bundle_price,
                    "carry_on_note": f"carry-on from +USD {bundle_price:.0f} (WORKS bundle incl. carry-on + checked bag)",
                    "checked_from": bundle_price,
                    "checked_note": f"checked bag from +USD {bundle_price:.0f} (WORKS bundle incl. carry-on + checked bag)",
                    "seat_from": 5.0,
                    "seat_note": "seat selection from +USD 5 (add at booking)",
                    "currency": "USD",
                }
        except Exception:
            pass
        return None

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        import time as _time
        cache_key = f"f9_{origin}_{dest}_{date_str}"
        now = _time.time()
        if cache_key in _ancillary_cache:
            ts, cached = _ancillary_cache[cache_key]
            if now - ts < _ANCILLARY_CACHE_TTL:
                return cached
        # Frontier Economy fare: no carry-on or checked bag included.
        # WORKS bundle adds carry-on + checked bag.  Carry-on separately ~$35–$55.
        result: dict = {
            "carry_on_from": 39.0,
            "carry_on_note": "carry-on from +USD 39 (Economy — WORKS bundle saves on carry-on + bag)",
            "checked_from": 49.0,
            "checked_note": "first checked bag from +USD 49 (Economy — add at booking)",
            "seat_from": 5.0,
            "seat_note": "seat selection from +USD 5",
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
            if carry_on_note:
                offer.conditions["carry_on"] = carry_on_note
            if checked_note:
                offer.conditions["checked_bag"] = checked_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if carry_on_from is not None and (carry_on_from == 0.0 or ccy_ok):
                offer.bags_price["carry_on"] = carry_on_from
            if checked_from is not None and (checked_from == 0.0 or ccy_ok):
                offer.bags_price["checked_bag"] = checked_from
            if seat_from is not None and (seat_from == 0.0 or ccy_ok):
                offer.bags_price["seat_selection"] = seat_from

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
                    id=f"rt_fron_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
