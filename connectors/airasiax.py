"""
AirAsia X Playwright scraper -- direct URL navigation + API interception.

AirAsia X (IATA: D7) is a Malaysian low-cost long-haul airline, part of
the AirAsia Group.  It operates from Kuala Lumpur (KUL) to destinations
in Australia, Japan, South Korea, India, and Indonesia.

Shares the same airasia.com booking platform as AirAsia (AK).  We use the
same search URL and filter for D7 carrier codes in the parsed results.

Strategy:
1. Launch Chrome in headed mode (--headless=new is detected by Akamai)
2. Navigate directly to the search results URL
3. Intercept the aggregated-results JSON API response
4. Parse searchResults.trips[].flightsList[] → filter D7 carrier → FlightOffers
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
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
from .browser import auto_block_if_proxied, get_default_proxy

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-MY", "en-SG"]
_TIMEZONES = [
    "Asia/Kuala_Lumpur", "Asia/Singapore", "Asia/Bangkok",
    "Asia/Jakarta", "Asia/Manila",
]

# ── Shared browser singleton ─────────────────────────────────────────────
_browser = None
_pw_instance = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch Chrome in headed mode (off-screen) — AirAsia's Akamai bot
    protection returns empty API bodies when ``--headless=new`` is used."""
    global _browser, _pw_instance
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        _pw_instance = await async_playwright().start()
        _browser = await _pw_instance.chromium.launch(
            headless=False,
            channel="chrome",
            proxy=get_default_proxy(),
            args=[
                "--window-position=-2400,-2400",
                "--window-size=800,600",
                "--disable-http2",
            ],
        )
        logger.info("AirAsia X: headed Chrome launched (off-screen)")
        return _browser


class AirAsiaXConnectorClient:
    """AirAsia X (D7) Playwright scraper -- direct URL + response interception."""

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
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
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

            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    if any(p in url for p in (
                        "aggregated-results", "availability",
                        "search/flights", "low-fare",
                    )):
                        body = await response.body()
                        if len(body) < 100:
                            return
                        import json as _json
                        data = _json.loads(body)
                        if isinstance(data, dict) and (
                            "searchResults" in data
                            or "trips" in data
                            or "flights" in data
                        ):
                            captured_data["json"] = data
                            captured_data["url"] = response.url
                            api_event.set()
                            logger.info("AirAsia X: captured %d bytes from %s", len(body), response.url[:120])
                except Exception:
                    pass

            page.on("response", on_response)

            search_url = self._build_search_url(req)
            logger.info("AirAsia X: navigating to search URL for %s->%s", req.origin, req.destination)
            await page.goto(
                search_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("AirAsia X: timed out waiting for API response")

            data = captured_data.get("json")
            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_response(data, req)
                if offers:
                    return self._build_response(offers, req, elapsed)

            # Fallback: DOM extraction from __NEXT_DATA__
            offers = await self._extract_from_dom(page, req)
            if offers:
                return self._build_response(offers, req, time.monotonic() - t0)
            return self._empty(req)

        except Exception as e:
            logger.error("AirAsia X Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: extract from __NEXT_DATA__ or re-parsed script tags."""
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                const pp = window.__NEXT_DATA__?.props?.pageProps;
                if (pp?.aggregatorResponse) {
                    const sr = pp.aggregatorResponse.searchResults;
                    if (sr && sr.trips) return pp.aggregatorResponse;
                }
                const ndEl = document.getElementById('__NEXT_DATA__');
                if (ndEl) {
                    try {
                        const nd = JSON.parse(ndEl.textContent);
                        const ar = nd?.props?.pageProps?.aggregatorResponse;
                        if (ar?.searchResults?.trips) return ar;
                    } catch {}
                }
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d?.searchResults?.trips) return d;
                        if (d?.props?.pageProps?.aggregatorResponse?.searchResults?.trips)
                            return d.props.pageProps.aggregatorResponse;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        currency = req.currency
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # AirAsia aggregated-results: searchResults.trips[].flightsList[]
        search_results = data.get("searchResults")
        if isinstance(search_results, dict):
            trips = search_results.get("trips", []) or []
            if isinstance(trips, list) and trips:
                # trips[0] = outbound, trips[1] = inbound (if RT)
                ob_trip = trips[0] if isinstance(trips[0], dict) else {}
                for flight in ob_trip.get("flightsList", []):
                    if not self._is_d7_flight(flight):
                        continue
                    offer = self._parse_airasiax_flight(flight, currency, req, booking_url)
                    if offer:
                        offers.append(offer)

                # RT: pair each OB with cheapest IB from trips[1]
                if req.return_from and len(trips) > 1 and offers:
                    ib_offers = []
                    ib_trip = trips[1] if isinstance(trips[1], dict) else {}
                    for flight in ib_trip.get("flightsList", []):
                        if not self._is_d7_flight(flight):
                            continue
                        ib_offer = self._parse_airasiax_flight(flight, currency, req, booking_url)
                        if ib_offer:
                            ib_offers.append(ib_offer)
                    if ib_offers:
                        _ib_best = min(ib_offers, key=lambda o: o.price)
                        _ib_route = _ib_best.outbound
                        for _i, _o in enumerate(offers):
                            offers[_i] = FlightOffer(
                                id=f"rt_{_o.id}",
                                price=round(_o.price + _ib_best.price, 2),
                                currency=_o.currency,
                                price_formatted=f"{round(_o.price + _ib_best.price, 2):.2f} {_o.currency}",
                                outbound=_o.outbound,
                                inbound=_ib_route,
                                airlines=_o.airlines,
                                owner_airline=_o.owner_airline,
                                booking_url=_o.booking_url,
                                is_locked=False,
                                source=_o.source,
                                source_tier=_o.source_tier,
                            )
        if offers:
            return offers

        # Fallback: generic format
        flights_raw = (
            data.get("outboundFlights")
            or data.get("outbound")
            or data.get("journeys")
            or data.get("flights")
            or data.get("availability", {}).get("trips", [])
            or data.get("data", {}).get("flights", [])
            or data.get("data", {}).get("journeys", [])
            or data.get("lowFareAvailability", {}).get("outboundOptions", [])
            or []
        )
        if isinstance(flights_raw, dict):
            flights_raw = flights_raw.get("outbound", []) or flights_raw.get("journeys", [])
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    @staticmethod
    def _is_d7_flight(flight: dict) -> bool:
        """Check if a flight is operated by AirAsia X (D7 or XJ)."""
        details = flight.get("flightDetails", {})
        segments = details.get("segments", [])
        if segments:
            for seg in segments:
                carrier = seg.get("carrierCode") or seg.get("airline") or ""
                if carrier.upper() in ("D7", "XJ"):
                    return True
            return False
        # Fallback: check designator-level carrier
        des = details.get("designator", {})
        carrier = des.get("carrierCode") or flight.get("carrierCode") or ""
        return carrier.upper() in ("D7", "XJ")

    def _parse_airasiax_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        """Parse a single flight from AirAsia's aggregated-results format."""
        price = None
        flight_currency = currency
        converted = flight.get("convertedPrice")
        if converted is not None:
            try:
                price = float(converted)
                flight_currency = flight.get("userCurrencyCode") or currency
            except (TypeError, ValueError):
                pass
        if not price or price <= 0:
            try:
                price = float(flight.get("price", 0))
                flight_currency = flight.get("currencyCode") or currency
            except (TypeError, ValueError):
                pass
        if not price or price <= 0:
            return None

        details = flight.get("flightDetails", {})
        designator = details.get("designator", {})
        segments_raw = details.get("segments", [])

        _d7_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                seg_des = seg.get("designator", {})
                carrier = seg.get("carrierCode") or seg.get("airline") or "D7"
                segments.append(FlightSegment(
                    airline=carrier,
                    airline_name="AirAsia X",
                    flight_no=seg.get("marketingFlightNo") or seg.get("flightNumber") or "",
                    origin=seg_des.get("departureStation", req.origin),
                    destination=seg_des.get("arrivalStation", req.destination),
                    departure=self._parse_dt(seg_des.get("departureTime", "")),
                    arrival=self._parse_dt(seg_des.get("arrivalTime", "")),
                    cabin_class=_d7_cabin,
                ))
        else:
            segments.append(FlightSegment(
                airline="D7", airline_name="AirAsia X", flight_no="",
                origin=designator.get("departureStation", req.origin),
                destination=designator.get("arrivalStation", req.destination),
                departure=self._parse_dt(designator.get("departureTime", "")),
                arrival=self._parse_dt(designator.get("arrivalTime", "")),
                cabin_class=_d7_cabin,
            ))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = flight.get("tripId") or f"{designator.get('departureTime', '')}_{time.monotonic()}"
        return FlightOffer(
            id=f"d7_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=flight_currency,
            price_formatted=f"{price:.2f} {flight_currency}",
            outbound=route, inbound=None,
            airlines=["AirAsia X"], owner_airline="D7",
            booking_url=booking_url, is_locked=False,
            source="airasiax_direct", source_tier="free",
        )

    def _parse_single_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None
        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination, req.cabin_class or "M"))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination, req.cabin_class or "M"))
        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
        route = FlightRoute(segments=segments, total_duration_seconds=max(total_dur, 0), stopovers=max(len(segments) - 1, 0))
        flight_key = flight.get("journeyKey") or flight.get("id") or f"{flight.get('departureDate', '')}_{time.monotonic()}"
        return FlightOffer(
            id=f"d7_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["AirAsia X"], owner_airline="D7",
            booking_url=booking_url, is_locked=False,
            source="airasiax_direct", source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareProducts") or flight.get("bundles") or flight.get("fareBundles") or []
        best = float("inf")
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice", "fareAmount", "totalAmount"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        for key in ["price", "lowestFare", "totalPrice", "farePrice", "amount", "lowestPrice"]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = float(p) if not isinstance(p, dict) else float(p.get("amount", 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str, cabin_class: str = "M") -> FlightSegment:
        dep_str = seg.get("departureDateTime") or seg.get("departure") or seg.get("departureDate") or seg.get("std") or ""
        arr_str = seg.get("arrivalDateTime") or seg.get("arrival") or seg.get("arrivalDate") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureStation") or seg.get("departureAirport") or default_origin
        destination = seg.get("destination") or seg.get("arrivalStation") or seg.get("arrivalAirport") or default_dest
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "D7"
        _d7_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(cabin_class, "economy")
        return FlightSegment(
            airline=carrier, airline_name="AirAsia X", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class=_d7_cabin,
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("AirAsia X %s->%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"airasiax{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
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
        dep = req.date_from.strftime("%d/%m/%Y")
        return (
            f"https://www.airasia.com/flights/search/?origin={req.origin}"
            f"&destination={req.destination}&departDate={dep}"
            f"&tripType={'R' if req.return_from else 'O'}&adult={req.adults}&child=0&infant=0"
            f"&locale=en-gb&currency={req.currency}"
        )

    @staticmethod
    def _build_search_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%d/%m/%Y")
        _aax_cabin = {"M": "economy", "W": "premium", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        url = (
            f"https://www.airasia.com/flights/search/"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departDate={dep}&tripType={'R' if req.return_from else 'O'}"
            f"&adult={req.adults}&child=0&infant=0"
            f"&locale=en-gb&currency={req.currency}"
            f"&airlineProfile=k,d,g&type=paired&cabinClass={_aax_cabin}&uce=true"
        )
        if req.return_from:
            ret = req.return_from.strftime("%d/%m/%Y") if hasattr(req.return_from, "strftime") else str(req.return_from)
            url += f"&returnDate={ret}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"airasiax{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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
                combos.append(FlightOffer(
                    id=f"rt_aira_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
