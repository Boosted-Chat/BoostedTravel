"""
Azul Brazilian Airlines scraper — headed Chrome + form fill + API capture.

Azul (IATA: AD) is Brazil's third-largest airline with the widest domestic network.
Website: passagens.voeazul.com.br — booking portal with form-based search.

Architecture:
- React SPA frontend (Next.js) on passagens.voeazul.com.br
- Akamai Bot Manager blocks headless Chrome and non-browser HTTP
- Form fill triggers availability API automatically
- Capture availability response → parse Navitaire format → FlightOffer objects

Strategy:
1. Launch persistent headed Chrome (Akamai blocks headless)
2. Navigate to passagens.voeazul.com.br/en → accept cookies
3. Fill origin/destination comboboxes, set one-way, adjust date
4. Click Search → capture availability API response
5. Parse Navitaire format → FlightOffer objects

Performance: ~8-15s per search (form fill is slower than deep link).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, date
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import auto_block_if_proxied

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────

_MAX_ATTEMPTS = 2
_API_WAIT = 45  # seconds to wait for availability API per attempt
_BOOKING_URL = "https://passagens.voeazul.com.br/en"

_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), "..", ".azul_chrome_data"
)

# ── Persistent browser context (headed to bypass Akamai) ────────────────

_pw_instance = None
_pw_context = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Persistent headed Chrome context — cookies survive across searches."""
    global _pw_instance, _pw_context
    lock = _get_lock()
    async with lock:
        if _pw_context:
            try:
                _pw_context.pages
                return _pw_context
            except Exception:
                _pw_context = None

        from playwright.async_api import async_playwright

        os.makedirs(os.path.abspath(_USER_DATA_DIR), exist_ok=True)
        _pw_instance = await async_playwright().start()

        _pw_context = await _pw_instance.chromium.launch_persistent_context(
            os.path.abspath(_USER_DATA_DIR),
            channel="chrome",
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
            ],
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/Sao_Paulo",
            service_workers="block",
        )
        logger.info("Azul: persistent Chrome context ready")
        return _pw_context


class AzulConnectorClient:
    """Azul scraper — headed Chrome + form fill + API capture."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        ob_result = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                ob_result = await self._attempt_search(req, t0)
                if ob_result is not None:
                    break
            except Exception as e:
                logger.warning("Azul: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        if ob_result is None:
            return self._empty(req)

        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination, 
                "destination": req.origin, 
                "date_from": req.return_from, 
                "return_from": None
            })
            ib_result = None
            for attempt in range(1, _MAX_ATTEMPTS + 1):
                try:
                    ib_result = await self._attempt_search(ib_req, t0)
                    if ib_result is not None:
                        break
                except Exception:
                    pass
            if ib_result and ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)

        return ob_result

    async def _attempt_search(
        self, req: FlightSearchRequest, t0: float
    ) -> Optional[FlightSearchResponse]:
        """Single attempt: form fill → capture API response."""
        ctx = await _get_context()
        page = await ctx.new_page()
        await auto_block_if_proxied(page)

        # Capture API response
        captured: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                url = response.url
                # Capture availability API responses (v5 or v6 format)
                if "availability" not in url.lower():
                    return
                if response.status != 200:
                    logger.debug("Azul: availability API returned %d", response.status)
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = await response.json()
                if isinstance(body, dict) and (body.get("data") or body.get("trips")):
                    captured["avail"] = body
                    api_event.set()
                    logger.debug("Azul: captured availability response")
            except Exception as e:
                logger.debug("Azul: response capture error: %s", e)

        page.on("response", on_response)

        dep = req.date_from
        logger.info("Azul: searching %s→%s on %s", req.origin, req.destination, dep)

        try:
            # 1. Navigate to booking portal
            await page.goto(_BOOKING_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(1)

            # 2. Accept cookie consent
            await self._accept_cookies(page)

            # 3. Set one-way trip
            await self._set_one_way(page)

            # 4. Fill origin
            await self._fill_airport(page, "origin", req.origin)

            # 5. Fill destination
            await self._fill_airport(page, "destination", req.destination)

            # 6. Set departure date
            await self._set_date(page, dep)

            # 7. Click search
            search_btn = page.locator('button:has-text("Search")')
            await search_btn.click(timeout=5000)

            # 8. Wait for availability API response
            await asyncio.wait_for(api_event.wait(), timeout=_API_WAIT)

        except asyncio.TimeoutError:
            logger.warning("Azul: availability API timed out")
            return None
        except Exception as e:
            logger.warning("Azul: search error: %s", e)
            return None
        finally:
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

        data = captured.get("avail")
        if data is None:
            return None

        elapsed = time.monotonic() - t0
        offers = self._parse_availability(data, req)
        return self._build_response(offers, req, elapsed)

    async def _accept_cookies(self, page) -> None:
        """Click cookie consent button if present."""
        try:
            btn = page.locator('button:has-text("Accept"), button:has-text("Aceitar")')
            if await btn.count() > 0:
                await btn.first.click(timeout=3000)
                await asyncio.sleep(0.5)
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        """Switch to one-way trip mode."""
        try:
            # Click journey type dropdown
            jt_btn = page.locator('button[aria-label*="journey-type"]')
            if await jt_btn.count() > 0:
                await jt_btn.first.click(timeout=3000)
                await asyncio.sleep(0.3)
                # Select one-way
                ow = page.locator('*[role="radio"]:has-text("One-way")')
                if await ow.count() > 0:
                    await ow.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
                else:
                    # Close dropdown
                    await page.keyboard.press("Escape")
        except Exception as e:
            logger.debug("Azul: one-way toggle error: %s", e)

    async def _fill_airport(self, page, field_type: str, iata: str) -> None:
        """Fill origin or destination airport combobox."""
        aria = "origin" if field_type == "origin" else "destination"
        try:
            # Click the airport button to open the combobox
            btn = page.locator(f'button[aria-label*="{aria}"]')
            await btn.first.click(timeout=5000)
            await asyncio.sleep(0.5)

            # Type into the combobox
            cb = page.locator(f'*[role="combobox"][aria-label*="{aria}"]')
            await cb.fill(iata, timeout=3000)
            await asyncio.sleep(0.8)

            # Click the first option
            opt = page.locator('*[role="option"]').first
            await opt.click(timeout=3000)
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug("Azul: airport fill error (%s): %s", field_type, e)
            # Press escape to close any open dropdown
            await page.keyboard.press("Escape")

    async def _set_date(self, page, dep_date: date) -> None:
        """Set departure date using the date picker."""
        try:
            # Click departure date button
            date_btn = page.locator('button[aria-label*="departure-date"]')
            await date_btn.first.click(timeout=5000)
            await asyncio.sleep(0.5)

            # The date picker should be open - type the date in MM/DD/YYYY format
            date_str = dep_date.strftime("%m/%d/%Y")
            date_input = page.locator('input[type="text"]').first
            if await date_input.count() > 0:
                await date_input.fill(date_str, timeout=3000)
                await asyncio.sleep(0.3)
                await page.keyboard.press("Enter")
            else:
                # Fallback: close the picker (use default date)
                await page.keyboard.press("Escape")
        except Exception as e:
            logger.debug("Azul: date set error: %s", e)
            await page.keyboard.press("Escape")

    # ── Navitaire availability parsing ───────────────────────────────────

    def _parse_availability(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = data.get("data", {}).get("trips") or data.get("trips") or []
        for trip in trips:
            journeys = trip.get("journeys") or trip.get("journeysAvailable") or []
            if not isinstance(journeys, list):
                continue
            for journey in journeys:
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_journey(
        self, journey: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single Navitaire journey into a FlightOffer."""
        best_price = self._extract_journey_price(journey)
        if best_price is None or best_price <= 0:
            return None

        currency = self._extract_currency(journey) or "BRL"

        identifier = journey.get("identifier") or journey.get("designator") or {}
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []

        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._parse_segment(seg, req))
        else:
            dep_str = identifier.get("std") or journey.get("departureDateTime") or ""
            arr_str = identifier.get("sta") or journey.get("arrivalDateTime") or ""
            origin = identifier.get("departureStation") or req.origin
            dest = identifier.get("arrivalStation") or req.destination
            carrier = identifier.get("carrierCode") or "AD"
            flight_num = str(identifier.get("flightNumber") or "")
            segments.append(FlightSegment(
                airline=carrier, airline_name="Azul",
                flight_no=f"{carrier}{flight_num}" if flight_num else "",
                origin=origin, destination=dest,
                departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
                cabin_class="economy",
            ))

        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            diff = (segments[-1].arrival - segments[0].departure).total_seconds()
            total_dur = int(diff) if diff > 0 else 0

        stops = max(len(segments) - 1, 0)
        route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=stops)

        journey_key = journey.get("journeyKey") or ""
        if not journey_key and segments:
            journey_key = f"{segments[0].departure.isoformat()}_{segments[0].flight_no}"

        conditions, bags_price = self._extract_azul_bag_info(journey, currency)

        return FlightOffer(
            id=f"ad_{hashlib.md5(journey_key.encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=list(set(s.airline for s in segments)) or ["AD"],
            owner_airline="AD",
            conditions=conditions,
            bags_price=bags_price,
            booking_url=booking_url,
            is_locked=False, source="azul_direct", source_tier="free",
        )

    @staticmethod
    def _extract_azul_bag_info(journey: dict, currency: str) -> tuple[dict, dict]:
        """Extract bag allowance from cheapest Navitaire fare on the Azul journey."""
        conditions: dict[str, str] = {}
        bags_price: dict[str, float] = {}

        fares = journey.get("fares", [])
        if not fares:
            return conditions, bags_price

        # Find the fare entry that gave the cheapest price
        best_price = float("inf")
        cheapest_fare: dict = {}
        for fare in fares:
            if not isinstance(fare, dict):
                continue
            pax_fares = fare.get("paxFares") or fare.get("passengerFares") or []
            fare_min = float("inf")
            for pf in pax_fares:
                for key in ("totalAmount", "originalAmount", "fareAmount"):
                    val = pf.get(key)
                    if val is not None:
                        try:
                            v = float(val)
                            if v > 0:
                                fare_min = min(fare_min, v)
                        except (TypeError, ValueError):
                            pass
            if fare_min < best_price:
                best_price = fare_min
                cheapest_fare = fare

        if not cheapest_fare and fares:
            cheapest_fare = fares[0] if isinstance(fares[0], dict) else {}

        bundle_code = str(
            cheapest_fare.get("bundleCode") or cheapest_fare.get("bundleInformation")
            or cheapest_fare.get("fareClass") or cheapest_fare.get("fareName")
            or cheapest_fare.get("fareCode") or cheapest_fare.get("fareSellKey") or ""
        ).upper()

        if bundle_code:
            conditions["fare_family"] = bundle_code

        # Azul Navitaire bundleCodes: AZUL (no bag), BLUE (1×23kg), BLACK (2×23kg)
        if "AZUL" in bundle_code or bundle_code in ("A", "AZ"):
            conditions["checked_bag"] = "no free checked bag (Azul base fare)"
        elif "BLACK" in bundle_code or bundle_code in ("BL", "BLK"):
            conditions["checked_bag"] = "2x 23kg bags included"
            bags_price["checked_bag"] = 0.0
        elif "BLUE" in bundle_code or bundle_code in ("BU", "XTRA", "XT"):
            conditions["checked_bag"] = "1x 23kg bag included"
            bags_price["checked_bag"] = 0.0
        elif bundle_code:
            # Unknown bundle code — try to detect from Navitaire fare services
            services = cheapest_fare.get("bundleInformation") or cheapest_fare.get("services") or []
            if isinstance(services, list):
                for svc in services:
                    svc_str = str(svc).upper()
                    if any(k in svc_str for k in ("CHECKED", "BAG", "BAGGAGE", "MALA")):
                        conditions["checked_bag"] = "checked bag included"
                        bags_price["checked_bag"] = 0.0
                        break

        # Carry-on: infer from Azul bundle code
        if "carry_on" not in conditions:
            if "AZUL" in bundle_code or bundle_code in ("A", "AZ"):
                conditions["carry_on"] = "1x 10kg carry-on included (no free checked bag on base fare)"
                bags_price["carry_on"] = 0.0
            else:
                conditions["carry_on"] = "1x 10kg carry-on included"
                bags_price["carry_on"] = 0.0

        # Seat selection
        if "seat" not in conditions:
            conditions["seat"] = "seat selection from ~BRL 30 — add at checkout"

        return conditions, bags_price

    def _parse_segment(self, seg: dict, req: FlightSearchRequest) -> FlightSegment:
        identifier = seg.get("identifier") or seg.get("designator") or {}
        flight_des = seg.get("flightDesignator") or {}

        dep_str = identifier.get("std") or seg.get("departureDateTime") or ""
        arr_str = identifier.get("sta") or seg.get("arrivalDateTime") or ""
        origin = identifier.get("departureStation") or seg.get("departureStation") or req.origin
        dest = identifier.get("arrivalStation") or seg.get("arrivalStation") or req.destination
        carrier = identifier.get("carrierCode") or flight_des.get("carrierCode") or "AD"
        flight_num = str(identifier.get("flightNumber") or flight_des.get("flightNumber") or "")

        return FlightSegment(
            airline=carrier, airline_name="Azul",
            flight_no=f"{carrier}{flight_num}" if flight_num else "",
            origin=origin, destination=dest,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="economy",
        )

    @staticmethod
    def _extract_journey_price(journey: dict) -> Optional[float]:
        best = float("inf")
        for fare in journey.get("fares", []):
            if not isinstance(fare, dict):
                continue
            pax_fares = fare.get("paxFares") or fare.get("passengerFares") or []
            for pf in pax_fares:
                for key in ("totalAmount", "originalAmount", "fareAmount"):
                    val = pf.get(key)
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
                total_charge = 0.0
                for charge in pf.get("serviceCharges", []):
                    try:
                        total_charge += float(charge.get("amount", 0))
                    except (TypeError, ValueError):
                        pass
                if total_charge > 0 and total_charge < best:
                    best = total_charge
        return best if best < float("inf") else None

    @staticmethod
    def _extract_currency(journey: dict) -> Optional[str]:
        for fare in journey.get("fares", []):
            if not isinstance(fare, dict):
                continue
            for pf in fare.get("paxFares") or fare.get("passengerFares") or []:
                cc = pf.get("currencyCode")
                if cc:
                    return cc
        return None

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%m/%d/%Y")
        from urllib.parse import quote
        return (
            f"https://www.voeazul.com.br/us/en/home/selecao-voo"
            f"?c%5B0%5D.ds={req.origin}&c%5B0%5D.as={req.destination}"
            f"&c%5B0%5D.std={quote(dep, safe='')}"
            f"&p%5B0%5D.t=ADT&p%5B0%5D.c={req.adults}"
            f"&p%5B0%5D.cp=false&cc=BRL"
        )

    @staticmethod
    def _combine_rt(ob: list, ib: list, req) -> list:
        combos = []
        for o in sorted(ob, key=lambda x: x.price)[:15]:
            for i in sorted(ib, key=lambda x: x.price)[:10]:
                combos.append(FlightOffer(
                    id=f"ad_rt_{o.id}_{i.id}",
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

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"azul{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Azul %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(
            f"azul{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers, total_results=len(offers),
        )
