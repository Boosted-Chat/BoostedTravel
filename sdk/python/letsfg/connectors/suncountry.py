"""
Sun Country Airlines direct connector — Playwright lowfare API.

Sun Country Airlines (IATA: SY) is a US low-cost carrier based in Minneapolis/
St. Paul (MSP). Operates 90+ routes across the US, Mexico, Caribbean, and
Central America.

Strategy:
  1. Launch Playwright browser → load suncountry.com (gets Incapsula WAF cookies
     and Navitaire JWT from sessionStorage).
  2. Use in-browser fetch() to call the lowfare/outbound API, which returns
     the cheapest fare per day with availability counts.
  3. Build one FlightOffer per requested date.

Note: The Navitaire availability/search endpoint (ext/v1/availability/search)
is unreachable — it times out from both browser and direct httpx, confirmed
2026-03 testing. The lowfare endpoint is the only working pricing source.
Direct httpx calls to the API are blocked by Incapsula WAF (403).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
from datetime import datetime, timedelta
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

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800

_GQL_SEAT_WAIT = 8  # seconds to wait for seat map API response
_FLIGHT_CARD_SELECTORS = [
    "[class*='flight'] button",
    "button[class*='select']",
    "button[class*='flight']",
    "[data-testid*='flight'] button",
    "button:has-text('Select')",
    "button:has-text('Choose')",
]


def _flatten_keys(data: Any, _depth: int = 0) -> list[str]:
    """Return all dict keys found in a nested JSON structure (up to depth 8)."""
    if _depth > 8 or not isinstance(data, (dict, list)):
        return []
    if isinstance(data, list):
        keys: list[str] = []
        for item in data:
            keys.extend(_flatten_keys(item, _depth + 1))
        return keys
    keys = list(data.keys())
    for v in data.values():
        keys.extend(_flatten_keys(v, _depth + 1))
    return keys

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

_SUB_KEY = os.environ.get("SUNCOUNTRY_SUB_KEY", "")

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
        logger.info("SunCountry: headed Chrome launched")
        return _browser


class SunCountryConnectorClient:
    """Sun Country Airlines connector — Playwright + lowfare API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout
        self._last_live_seat_from: float | None = None

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        live_seat_from = self._last_live_seat_from
        self._last_live_seat_from = None
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
                        req.date_from.isoformat(), req.adults, "USD",
                        live_seat_from=live_seat_from,
                    ),
                    timeout=10.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                pass
            except Exception as _anc_err:
                logger.debug("SY: ancillary fetch error: %s", _anc_err)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            offers = await self._search_via_browser(req)
            elapsed = time.monotonic() - t0
            logger.info(
                "SunCountry: %s→%s on %s — %d offers in %.1fs",
                req.origin, req.destination, req.date_from, len(offers), elapsed,
            )
            return FlightSearchResponse(
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
                search_id=f"suncountry_{req.origin}_{req.destination}_{req.date_from}_{req.return_from or ''}",
            )
        except Exception as e:
            logger.error("SunCountry search error: %s", e)
            return self._empty(req)

    async def _search_via_browser(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="America/Chicago",
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

            token_holder: dict = {}
            token_event = asyncio.Event()
            seat_captured: dict = {}
            seat_event = asyncio.Event()

            async def on_response(response):
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    if "/nsk/v1/token" in response.url and response.status in (200, 201):
                        data = await response.json()
                        tok = (data.get("data") or data).get("token", "")
                        if tok and not token_holder.get("token"):
                            token_holder["token"] = tok
                            token_event.set()
                        return
                    # Also try to capture any seat map responses during page load
                    if response.status == 200 and not seat_event.is_set():
                        data = await response.json()
                        if isinstance(data, dict):
                            all_keys = set(_flatten_keys(data))
                            seat_keys = {k for k in all_keys if any(
                                kw in k.lower() for kw in ("seat", "cabin", "map")
                            )}
                            if seat_keys:
                                seat_captured["seat_data"] = data
                                seat_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Load homepage to get WAF cookies and Navitaire token
            logger.info("SunCountry: loading homepage for WAF cookies")
            await page.goto(
                "https://www.suncountry.com/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            # Wait for the SPA to obtain the Navitaire JWT
            try:
                await asyncio.wait_for(token_event.wait(), timeout=15)
            except asyncio.TimeoutError:
                pass
            token = token_holder.get("token")
            if not token:
                logger.warning("SunCountry: no Navitaire token found")
                return []

            # Step 2: Call lowfare/outbound via in-browser fetch
            # Request a ±3 day window centred on the target date
            is_rt = bool(req.return_from)
            start_dt = req.date_from - timedelta(days=3)
            end_dt = req.date_from + timedelta(days=3)
            body = {
                "request": {
                    "origin": req.origin,
                    "destination": req.destination,
                    "currencyCode": "USD",
                    "includeTaxesAndFees": True,
                    "isRoundTrip": is_rt,
                    "numberOfPassengers": req.adults,
                    "startDate": start_dt.strftime("%m/%d/%Y"),
                    "endDate": end_dt.strftime("%m/%d/%Y"),
                }
            }

            result = await page.evaluate(
                """async ([token, subKey, body]) => {
                    try {
                        const resp = await fetch(
                            'https://syprod-api.suncountry.com/ext/v1/lowfare/outbound',
                            {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Ocp-Apim-Subscription-Key': subKey,
                                    'Authorization': token
                                },
                                body: JSON.stringify(body)
                            }
                        );
                        if (!resp.ok) return { error: resp.status };
                        return await resp.json();
                    } catch (e) {
                        return { error: e.message };
                    }
                }""",
                [token, _SUB_KEY, body],
            )

            if not result or result.get("error"):
                logger.warning("SunCountry: lowfare error: %s", result)
                return []

            ob_offers = self._parse_lowfare(result, req)

            # Try live seat pricing via Navitaire seat map API probe
            if ob_offers and token and not seat_event.is_set():
                live_seat = await self._probe_navitaire_seat_price(
                    page, token, req.origin, req.destination
                )
                if live_seat is not None:
                    seat_captured["seat_data"] = {"probed_min_price": live_seat}
                    seat_event.set()

            if seat_event.is_set():
                probe_price = seat_captured.get("seat_data", {}).get("probed_min_price")
                if probe_price is not None:
                    self._last_live_seat_from = float(probe_price)
                else:
                    self._last_live_seat_from = self._extract_min_seat_price(
                        seat_captured.get("seat_data") or {}
                    )

            # For RT, also fetch inbound lowfares
            if is_rt and ob_offers:
                ib_start = req.return_from - timedelta(days=3)
                ib_end = req.return_from + timedelta(days=3)
                ib_body = {
                    "request": {
                        "origin": req.destination,
                        "destination": req.origin,
                        "currencyCode": "USD",
                        "includeTaxesAndFees": True,
                        "isRoundTrip": True,
                        "numberOfPassengers": req.adults,
                        "startDate": ib_start.strftime("%m/%d/%Y"),
                        "endDate": ib_end.strftime("%m/%d/%Y"),
                    }
                }
                ib_result = await page.evaluate(
                    """async ([token, subKey, body]) => {
                        try {
                            const resp = await fetch(
                                'https://syprod-api.suncountry.com/ext/v1/lowfare/inbound',
                                {
                                    method: 'POST',
                                    headers: {
                                        'Content-Type': 'application/json',
                                        'Ocp-Apim-Subscription-Key': subKey,
                                        'Authorization': token
                                    },
                                    body: JSON.stringify(body)
                                }
                            );
                            if (!resp.ok) return { error: resp.status };
                            return await resp.json();
                        } catch (e) {
                            return { error: e.message };
                        }
                    }""",
                    [token, _SUB_KEY, ib_body],
                )

                if ib_result and not ib_result.get("error"):
                    ib_offers = self._parse_lowfare_inbound(ib_result, req)
                    if ib_offers:
                        # Build RT offers: outbound × cheapest inbound
                        ib_offers.sort(key=lambda x: x[1])
                        cheapest_ib_route, cheapest_ib_price = ib_offers[0]
                        rt_offers: list[FlightOffer] = []
                        for ob in ob_offers:
                            total = round(ob.price + cheapest_ib_price, 2)
                            key = f"{ob.id}_rt_{total}"
                            rt_offers.append(FlightOffer(
                                id=f"sy_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                                price=total,
                                currency="USD",
                                price_formatted=f"${total:.2f}",
                                outbound=ob.outbound,
                                inbound=cheapest_ib_route,
                                airlines=["Sun Country Airlines"],
                                owner_airline="SY",
                                booking_url="https://www.suncountry.com/booking/select",
                                is_locked=False,
                                source="suncountry_direct",
                                source_tier="free",
                            ))
                        # RT offers first, then OW for combo engine
                        return rt_offers + ob_offers

            return ob_offers
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_lowfare(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        fares = data.get("lowfares") or []
        target = req.date_from.strftime("%Y-%m-%d")
        booking_url = "https://www.suncountry.com/booking/select"
        offers: list[FlightOffer] = []

        for fare in fares:
            fare_date = (fare.get("date") or "")[:10]  # "2026-04-13T00:00:00" → "2026-04-13"
            if fare_date != target:
                continue
            if fare.get("noFlights") or fare.get("soldOut"):
                continue
            price = fare.get("price")
            if not price or price <= 0:
                continue

            seats = fare.get("available")
            dep_dt = datetime.fromisoformat(fare["date"])

            _sy_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="SY",
                airline_name="Sun Country Airlines",
                flight_no="SY",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=_sy_cabin,
            )
            route = FlightRoute(segments=[seg])

            key = f"{req.origin}{req.destination}{fare_date}{price}"
            offer_id = f"sy_{hashlib.md5(key.encode()).hexdigest()[:12]}"

            offers.append(FlightOffer(
                id=offer_id,
                price=round(price, 2),
                currency="USD",
                price_formatted=f"${price:.2f}",
                outbound=route,
                inbound=None,
                airlines=["Sun Country Airlines"],
                owner_airline="SY",
                booking_url=booking_url,
                is_locked=False,
                source="suncountry_direct",
                source_tier="free",
                availability_seats=seats if isinstance(seats, int) else None,
            ))

        return offers

    def _parse_lowfare_inbound(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[tuple[FlightRoute, float]]:
        """Parse inbound lowfares into (route, price) tuples."""
        fares = data.get("lowfares") or []
        target = req.return_from.strftime("%Y-%m-%d")
        results: list[tuple[FlightRoute, float]] = []

        for fare in fares:
            fare_date = (fare.get("date") or "")[:10]
            if fare_date != target:
                continue
            if fare.get("noFlights") or fare.get("soldOut"):
                continue
            price = fare.get("price")
            if not price or price <= 0:
                continue

            dep_dt = datetime.fromisoformat(fare["date"])
            _sy_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="SY",
                airline_name="Sun Country Airlines",
                flight_no="SY",
                origin=req.destination,
                destination=req.origin,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=_sy_cabin,
            )
            route = FlightRoute(segments=[seg])
            results.append((route, round(price, 2)))

        return results


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
                    id=f"rt_sunc_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
            search_id=f"suncountry_{req.origin}_{req.destination}_{req.date_from}_{req.return_from or ''}",
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
        live_seat_from: float | None = None,
    ) -> dict | None:
        if live_seat_from is not None:
            return {
                "carry_on_from": 0.0,
                "carry_on_note": "1 carry-on bag included",
                "checked_from": 35.0,
                "checked_note": "first checked bag from +USD 35 (Basic fare — add at booking)",
                "seat_from": live_seat_from,
                "seat_note": f"seat selection from +USD {live_seat_from:.2f} (live, this route)",
                "currency": "USD",
            }
        cache_key = f"sy_{origin}_{dest}_{date_str}"
        now = time.time()
        if cache_key in _ancillary_cache:
            ts, cached = _ancillary_cache[cache_key]
            if now - ts < _ANCILLARY_CACHE_TTL:
                return cached
        # Sun Country: Basic fare has no free bags. First checked bag ~$35.
        result: dict = {
            "carry_on_from": 0.0,
            "carry_on_note": "1 carry-on bag included",
            "checked_from": 35.0,
            "checked_note": "first checked bag from +USD 35 (Basic fare — add at booking)",
            "seat_from": 10.0,
            "seat_note": "seat selection from +USD 10",
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
            if carry_on_from == 0.0:
                offer.bags_price["carry_on"] = 0.0
            if checked_from == 0.0:
                offer.bags_price["checked_bag"] = 0.0
            if seat_from == 0.0:
                offer.bags_price["seat_selection"] = 0.0

    # ------------------------------------------------------------------
    # Live seat price helpers
    # ------------------------------------------------------------------

    async def _probe_navitaire_seat_price(
        self, page, token: str, origin: str, dest: str
    ) -> float | None:
        """Try Sun Country Navitaire seat map API to get min seat price."""
        try:
            result = await page.evaluate(
                """async ([token, subKey, origin, dest]) => {
                    try {
                        const resp = await fetch(
                            `https://syprod-api.suncountry.com/ext/v1/seatmap/${origin}/${dest}`,
                            {
                                method: 'GET',
                                headers: {
                                    'Authorization': token,
                                    'Ocp-Apim-Subscription-Key': subKey,
                                    'Accept': 'application/json',
                                }
                            }
                        );
                        if (!resp.ok) return null;
                        return await resp.json();
                    } catch (e) {
                        return null;
                    }
                }""",
                [token, _SUB_KEY, origin, dest],
            )
            if result and isinstance(result, dict):
                return self._extract_min_seat_price(result)
        except Exception as _exc:
            logger.debug("SY: seat map probe error: %s", _exc)
        return None

    async def _try_capture_seat_prices(
        self, page, seat_event: asyncio.Event, captured: dict
    ) -> None:
        """Click first flight card to trigger seat map API, capture response."""
        if seat_event.is_set():
            return
        for selector in _FLIGHT_CARD_SELECTORS:
            try:
                locator = page.locator(selector).first
                await locator.scroll_into_view_if_needed(timeout=2000)
                await locator.click(timeout=3000)
                logger.debug("SY: clicked flight card with selector '%s'", selector)
                try:
                    await asyncio.wait_for(seat_event.wait(), timeout=_GQL_SEAT_WAIT)
                except asyncio.TimeoutError:
                    logger.debug("SY: seat event timed out after clicking '%s'", selector)
                return
            except Exception as _exc:
                logger.debug("SY: seat card click failed for '%s': %s", selector, _exc)

    @staticmethod
    def _extract_min_seat_price(data: dict) -> float | None:
        """Recursively walk API response and return minimum selectable seat price."""
        _SEAT_ID_KEYS = frozenset({"seatCode", "seatNo", "seat", "code", "row", "column", "letter"})
        _PRICE_KEYS = frozenset({"price", "amount", "seatPrice", "selectionFee", "fee", "charge"})
        _MAX_DEPTH = 12

        def _walk(node: Any, depth: int) -> float | None:
            if depth > _MAX_DEPTH:
                return None
            if isinstance(node, list):
                mins = [v for v in (_walk(item, depth + 1) for item in node) if v is not None]
                return min(mins) if mins else None
            if not isinstance(node, dict):
                return None
            is_seat = bool(_SEAT_ID_KEYS & node.keys())
            if is_seat:
                for pk in _PRICE_KEYS:
                    raw = node.get(pk)
                    if raw is None:
                        continue
                    if isinstance(raw, dict):
                        raw = raw.get("amount")
                    try:
                        val = float(raw)
                        if 0.50 <= val <= 250:
                            return val
                    except (TypeError, ValueError):
                        pass
            mins = [v for v in (_walk(v, depth + 1) for v in node.values()) if v is not None]
            return min(mins) if mins else None

        return _walk(data, 0)
