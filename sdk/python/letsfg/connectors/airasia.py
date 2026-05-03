"""
AirAsia Playwright scraper -- direct URL navigation + API interception.

AirAsia (IATA: AK) is a Malaysian super-LCC operating across Asia-Pacific.
Uses a Navitaire-based booking engine with Akamai bot protection.

Strategy:
1. Launch Chrome in headed mode (--headless=new is detected by Akamai)
2. Navigate directly to the search results URL
3. Intercept the aggregated-results JSON API response
4. Parse searchResults.trips[].flightsList[] → FlightOffers
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
        logger.info("AirAsia: headed Chrome launched (off-screen)")
        return _browser


class AirAsiaConnectorClient:
    """AirAsia Playwright scraper -- direct URL navigation + response interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result, ob_anc = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result, _ = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        if ob_result.offers:
            # Use live checkout probe prices if available, else fall back to static ref
            if ob_anc:
                self._apply_ancillaries(ob_result.offers, ob_anc)
            else:
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

    async def _search_ow(self, req: FlightSearchRequest) -> tuple[FlightSearchResponse, dict | None]:
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
                            return  # Skip 206 partial / empty responses
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
                            logger.info("AirAsia: captured %d bytes from %s", len(body), response.url[:120])
                except Exception:
                    pass

            page.on("response", on_response)

            # Navigate directly to search results URL (skip form fill)
            search_url = self._build_search_url(req)
            logger.info("AirAsia: navigating to search URL for %s->%s", req.origin, req.destination)
            await page.goto(
                search_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("AirAsia: timed out waiting for API response")

            data = captured_data.get("json")
            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_response(data, req)
                if offers:
                    # Run checkout probe for live ancillary prices while browser is still open
                    probe = None
                    try:
                        probe = await asyncio.wait_for(
                            self._probe_checkout_ancillary(page),
                            timeout=25.0,
                        )
                        if probe:
                            logger.info("AirAsia: checkout probe succeeded (%s->%s)", req.origin, req.destination)
                    except Exception as _pe:
                        logger.debug("AirAsia: checkout probe skipped: %s", _pe)
                    return self._build_response(offers, req, elapsed), probe

            # Fallback: DOM extraction from __NEXT_DATA__ etc.
            offers = await self._extract_from_dom(page, req)
            if offers:
                return self._build_response(offers, req, time.monotonic() - t0), None
            return self._empty(req), None

        except Exception as e:
            logger.error("AirAsia Playwright error: %s", e)
            return self._empty(req), None
        finally:
            await context.close()

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: extract from __NEXT_DATA__ or re-parsed script tags."""
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                // Check __NEXT_DATA__ pageProps.aggregatorResponse first
                const pp = window.__NEXT_DATA__?.props?.pageProps;
                if (pp?.aggregatorResponse) {
                    const sr = pp.aggregatorResponse.searchResults;
                    if (sr && sr.trips) return pp.aggregatorResponse;
                }
                // Re-parse the __NEXT_DATA__ script tag (may be updated by hydration)
                const ndEl = document.getElementById('__NEXT_DATA__');
                if (ndEl) {
                    try {
                        const nd = JSON.parse(ndEl.textContent);
                        const ar = nd?.props?.pageProps?.aggregatorResponse;
                        if (ar?.searchResults?.trips) return ar;
                    } catch {}
                }
                // Check other script tags
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
            trips = search_results.get("trips", [])
            if isinstance(trips, list) and trips:
                # trips[0] = outbound, trips[1] = inbound (if RT)
                ob_trip = trips[0] if trips else {}
                for flight in ob_trip.get("flightsList", []):
                    offer = self._parse_airasia_flight(flight, currency, req, booking_url)
                    if offer:
                        offers.append(offer)

                # RT: pair each OB with cheapest IB from trips[1]
                if req.return_from and len(trips) > 1 and offers:
                    ib_offers = []
                    for flight in trips[1].get("flightsList", []):
                        ib_offer = self._parse_airasia_flight(flight, currency, req, booking_url)
                        if ib_offer:
                            ib_offers.append(ib_offer)
                    if ib_offers:
                        _ib_best = min(ib_offers, key=lambda o: o.price)
                        for _i, _o in enumerate(offers):
                            offers[_i] = self._build_round_trip_offer(_o, _ib_best, f"rt_{_o.id}")
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

    @classmethod
    def _extract_airasia_offer_truth(cls, flight: dict) -> tuple[dict[str, str], dict[str, Any]]:
        conditions: dict[str, str] = {}
        bags_price: dict[str, Any] = {}

        bundle_code = cls._clean_metadata_text(flight.get("bundleCode"))
        if bundle_code:
            conditions["fare_bundle"] = bundle_code

        fare_type_category = cls._clean_metadata_text(flight.get("fareTypeCategory"))
        if fare_type_category:
            conditions["fare_type_category"] = fare_type_category

        details = flight.get("flightDetails") if isinstance(flight, dict) else {}
        baggage = details.get("baggage") if isinstance(details, dict) else {}
        complimentary_types: set[str] = set()

        if isinstance(baggage, dict):
            complimentary = baggage.get("complimentaryBaggage")
            if isinstance(complimentary, list):
                for item in complimentary:
                    ancillary_type = cls._map_airasia_baggage_type(item)
                    description = cls._describe_airasia_baggage(item, ancillary_type)
                    if not ancillary_type or not description:
                        continue
                    conditions[ancillary_type] = f"included - {description}"
                    complimentary_types.add(ancillary_type)

            if baggage.get("checkedBaggageAllowed") and "checked_bag" not in complimentary_types:
                conditions["checked_bag"] = "not included - purchasable in checkout; no numeric search price"

        return conditions, bags_price

    @staticmethod
    def _clean_metadata_text(value: Any) -> str:
        if value is None:
            return ""
        text = " ".join(str(value).split())
        if text.lower() in {"", "none", "null"}:
            return ""
        return text

    @classmethod
    def _describe_airasia_baggage(cls, item: Any, ancillary_type: str) -> str:
        if not isinstance(item, dict) or not ancillary_type:
            return ""

        parts: list[str] = []
        count = cls._format_airasia_number(item.get("count"))
        if count:
            unit = "piece" if count == "1" else "pieces"
            parts.append(f"{count} {unit}")

        weight = cls._format_airasia_number(item.get("weight"))
        if weight:
            parts.append(f"{weight}kg")

        bag_label = "cabin bag" if ancillary_type == "cabin_bag" else "checked bag"
        parts.append(bag_label)

        dimensions = [cls._format_airasia_number(item.get(key)) for key in ("length", "width", "height")]
        if all(dimensions):
            parts.append(f"{dimensions[0]}x{dimensions[1]}x{dimensions[2]}cm")

        return " ".join(part for part in parts if part).strip()

    @staticmethod
    def _format_airasia_number(value: Any) -> str:
        if value in (None, ""):
            return ""
        try:
            number = float(value)
        except (TypeError, ValueError):
            text = str(value).strip()
            return text if text else ""
        if number.is_integer():
            return str(int(number))
        return f"{number:g}"

    @staticmethod
    def _map_airasia_baggage_type(item: Any) -> str:
        if not isinstance(item, dict):
            return ""
        raw_type = str(item.get("type") or "").strip().lower()
        if raw_type in {"cabin_bag", "carry_on", "carry-on", "carry_on_bag", "carry-on_bag"}:
            return "cabin_bag"
        if raw_type in {"checked_bag", "checked_baggage", "hold_bag", "hold_baggage"}:
            return "checked_bag"
        return ""

    @staticmethod
    def _merge_round_trip_conditions(
        outbound_conditions: Optional[dict[str, str]],
        inbound_conditions: Optional[dict[str, str]],
    ) -> dict[str, str]:
        outbound = dict(outbound_conditions or {})
        inbound = dict(inbound_conditions or {})
        merged: dict[str, str] = dict(outbound)

        for key, value in inbound.items():
            if value in (None, ""):
                continue
            existing = merged.get(key)
            if existing is None:
                merged[key] = value
                continue
            if existing == value:
                continue
            merged.pop(key, None)
            if outbound.get(key):
                merged[f"outbound_{key}"] = outbound[key]
            merged[f"inbound_{key}"] = value

        return merged

    @staticmethod
    def _merge_round_trip_bags_price(
        outbound_bags_price: Optional[dict[str, Any]],
        inbound_bags_price: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        outbound = dict(outbound_bags_price or {})
        inbound = dict(inbound_bags_price or {})
        merged: dict[str, Any] = dict(outbound)

        for key, value in inbound.items():
            if value is None:
                continue
            existing = merged.get(key)
            if existing is None:
                merged[key] = value
                continue
            if existing == value:
                continue
            merged.pop(key, None)
            if key in outbound:
                merged[f"outbound_{key}"] = outbound[key]
            merged[f"inbound_{key}"] = value

        return merged

    @classmethod
    def _build_round_trip_offer(
        cls,
        outbound_offer: FlightOffer,
        inbound_offer: FlightOffer,
        offer_id: str,
    ) -> FlightOffer:
        total_price = round(outbound_offer.price + inbound_offer.price, 2)
        return FlightOffer(
            id=offer_id,
            price=total_price,
            currency=outbound_offer.currency,
            price_formatted=f"{total_price:.2f} {outbound_offer.currency}",
            outbound=outbound_offer.outbound,
            inbound=inbound_offer.outbound,
            airlines=list(dict.fromkeys(outbound_offer.airlines + inbound_offer.airlines)),
            owner_airline=outbound_offer.owner_airline,
            bags_price=cls._merge_round_trip_bags_price(outbound_offer.bags_price, inbound_offer.bags_price),
            conditions=cls._merge_round_trip_conditions(outbound_offer.conditions, inbound_offer.conditions),
            booking_url=outbound_offer.booking_url,
            is_locked=False,
            source=outbound_offer.source,
            source_tier=outbound_offer.source_tier,
        )

    def _parse_airasia_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        """Parse a single flight from AirAsia's aggregated-results format."""
        # Price: prefer convertedPrice (user currency USD), else price (local MYR)
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

        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                seg_des = seg.get("designator", {})
                segments.append(FlightSegment(
                    airline=seg.get("carrierCode") or seg.get("airline") or "AK",
                    airline_name="AirAsia",
                    flight_no=seg.get("marketingFlightNo") or seg.get("flightNumber") or "",
                    origin=seg_des.get("departureStation", req.origin),
                    destination=seg_des.get("arrivalStation", req.destination),
                    departure=self._parse_dt(seg_des.get("departureTime", "")),
                    arrival=self._parse_dt(seg_des.get("arrivalTime", "")),
                    cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy"),
                ))
        else:
            segments.append(FlightSegment(
                airline="AK", airline_name="AirAsia", flight_no="",
                origin=designator.get("departureStation", req.origin),
                destination=designator.get("arrivalStation", req.destination),
                departure=self._parse_dt(designator.get("departureTime", "")),
                arrival=self._parse_dt(designator.get("arrivalTime", "")),
                cabin_class={"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy"),
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
        conditions, bags_price = self._extract_airasia_offer_truth(flight)
        return FlightOffer(
            id=f"ak_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=flight_currency,
            price_formatted=f"{price:.2f} {flight_currency}",
            outbound=route, inbound=None,
            airlines=["AirAsia"], owner_airline="AK",
            bags_price=bags_price,
            conditions=conditions,
            booking_url=booking_url, is_locked=False,
            source="airasia_direct", source_tier="free",
        )

    def _parse_single_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None
        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))
        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
        route = FlightRoute(segments=segments, total_duration_seconds=max(total_dur, 0), stopovers=max(len(segments) - 1, 0))
        flight_key = flight.get("journeyKey") or flight.get("id") or f"{flight.get('departureDate', '')}_{time.monotonic()}"
        conditions, bags_price = self._extract_airasia_offer_truth(flight)
        return FlightOffer(
            id=f"ak_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["AirAsia"], owner_airline="AK",
            bags_price=bags_price,
            conditions=conditions,
            booking_url=booking_url, is_locked=False,
            source="airasia_direct", source_tier="free",
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
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "AK"
        _ak_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(cabin_class, "economy")
        return FlightSegment(
            airline=carrier, airline_name="AirAsia", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class=_ak_cabin,
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("AirAsia %s->%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"airasia{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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
            f"&tripType=O&adult={req.adults}&child=0&infant=0"
            f"&locale=en-gb&currency={req.currency}"
        )

    @staticmethod
    def _build_search_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%d/%m/%Y")
        _aa_cabin = {"M": "economy", "W": "premium", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        url = (
            f"https://www.airasia.com/flights/search/"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departDate={dep}&tripType={'R' if req.return_from else 'O'}"
            f"&adult={req.adults}&child=0&infant=0"
            f"&locale=en-gb&currency={req.currency}"
            f"&airlineProfile=k,d,g&type=paired&cabinClass={_aa_cabin}&uce=true"
        )
        if req.return_from:
            ret = req.return_from.strftime("%d/%m/%Y") if hasattr(req.return_from, "strftime") else str(req.return_from)
            url += f"&returnDate={ret}"
        return url

    async def _probe_checkout_ancillary(self, page) -> dict | None:
        """After search results load, click 'Select' on first offer and
        extract live bag/seat prices from the checkout page __NEXT_DATA__."""
        try:
            # Wait for a Select button to be visible (React renders async)
            select_btn = None
            for selector in [
                "button:has-text('Select')",
                "button:has-text('Book')",
                "button:has-text('Choose')",
                "[data-testid*='select']",
            ]:
                try:
                    btn = page.locator(selector).first
                    await btn.wait_for(state="visible", timeout=8000)
                    select_btn = btn
                    logger.debug("AirAsia probe: found button via %s", selector)
                    break
                except Exception:
                    pass

            if select_btn is None:
                logger.debug("AirAsia probe: no Select button found on search page")
                return None

            await select_btn.click()

            # Wait for checkout URL (contains tripId= from Navitaire session)
            await page.wait_for_url(
                lambda url: "tripId=" in url,
                timeout=20000,
            )
            await page.wait_for_load_state("domcontentloaded", timeout=15000)

            # Extract __NEXT_DATA__ from checkout page
            raw = await page.evaluate("""() => {
                try {
                    const el = document.getElementById('__NEXT_DATA__');
                    return el ? JSON.parse(el.textContent) : null;
                } catch { return null; }
            }""")
            if not raw:
                return None

            pp = (raw.get("props") or {}).get("pageProps") or {}
            result = self._parse_checkout_ancillary(pp)
            if result:
                logger.info("AirAsia probe: parsed carry_on=%s checked=%s seat=%s %s",
                            result.get("bags_from"), result.get("checked_bag_from"),
                            result.get("seat_from"), result.get("currency", ""))
            return result
        except Exception as e:
            logger.debug("AirAsia checkout probe failed: %s", e)
            return None

    def _parse_checkout_ancillary(self, pp: dict) -> dict | None:
        """Parse AirAsia checkout pageProps for live bag/seat prices."""
        anc = pp.get("ancillary") or {}
        baggage = (
            anc.get("baggage")
            or pp.get("baggageList")
            or {}
        )
        currency = baggage.get("currency", "EUR")

        bag_data = baggage.get("data") or []
        if not bag_data:
            return None

        hold_price: float | None = None
        hold_kg = 15
        hand_included = False
        hand_kg = 7

        # Use the first route segment's ancillary data
        first_seg = bag_data[0]
        al = (first_seg.get("ancillarylist") or {})

        # Carry-on (hand bags)
        for item in al.get("hand") or []:
            rules = item.get("rules") or {}
            price_obj = item.get("price") or {}
            dims = ((item.get("properties") or {}).get("baggageDimension") or [])
            if rules.get("isIncluded") and price_obj.get("amount", 1) == 0:
                hand_included = True
                for dim in dims:
                    w = dim.get("weight") or {}
                    if w.get("unit") == "kg":
                        hand_kg = int(w.get("value", 7))
                break

        # Checked bags (hold) — take first/cheapest available option
        hold_items = sorted(
            al.get("hold") or [],
            key=lambda x: (x.get("price") or {}).get("amount") or 99999,
        )
        for item in hold_items:
            price_obj = item.get("price") or {}
            price_amt = price_obj.get("amount")
            dims = ((item.get("properties") or {}).get("baggageDimension") or [])
            if price_amt is not None and float(price_amt) > 0:
                hold_price = float(price_amt)
                for dim in dims:
                    w = dim.get("weight") or {}
                    if w.get("unit") == "kg":
                        hold_kg = int(w.get("value", 15))
                break

        # Seat pricing from seatMap data (if available)
        seat_price: float | None = None
        seat_map = anc.get("seatMap") or pp.get("seatData") or {}
        seat_list = seat_map.get("data") or []
        for seat_seg in seat_list:
            rows = (seat_seg.get("seatMap") or {}).get("rows") or []
            for row in rows:
                for seat in row.get("seats") or []:
                    price_obj = seat.get("price") or {}
                    p = price_obj.get("amount")
                    if p is not None and float(p) > 0:
                        if seat_price is None or float(p) < seat_price:
                            seat_price = float(p)
            if seat_price is not None:
                break

        result: dict = {"currency": currency, "source": "checkout_probe"}

        if hand_included:
            result["bags_from"] = 0.0
            result["bags_note"] = f"1 cabin bag included ({hand_kg} kg)"
        else:
            result["bags_from"] = None
            result["bags_note"] = "cabin bag add-on — see checkout"

        if hold_price is not None:
            result["checked_bag_from"] = hold_price
            result["checked_bag_note"] = (
                f"checked bag from {currency} {hold_price:.0f} ({hold_kg} kg)"
            )
        else:
            result["checked_bag_from"] = None
            result["checked_bag_note"] = "checked bag add-on — see checkout"

        if seat_price is not None:
            result["seat_from"] = seat_price
            result["seat_note"] = f"seat selection from {currency} {seat_price:.0f}"
        else:
            result["seat_from"] = None
            result["seat_note"] = "seat selection available at checkout"

        return result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
    ) -> dict | None:
        """Return AirAsia (AK) ancillary pricing from published tariff reference (fallback)."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("AK")
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
            # Set bags_price for any numeric value (0.0 = included, >0 = add-on price)
            if bags_from is not None:
                offer.bags_price.setdefault("carry_on", bags_from)
            if checked_from is not None:
                offer.bags_price.setdefault("checked_bag", checked_from)
            if seat_from is not None:
                offer.bags_price.setdefault("seat", seat_from)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"airasia{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
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
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(AirAsiaConnectorClient._build_round_trip_offer(o, i, f"rt_aira_{cid}"))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
