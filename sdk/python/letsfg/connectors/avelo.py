"""
Avelo Airlines direct connector — deep link URL + Playwright DOM scraping.

Avelo (IATA: XP) is a US ultra-low-cost carrier with ~42 routes across the
US, operating from bases at New Haven (HVN), Hollywood/Burbank (BUR), and
other cities to leisure destinations.

Strategy:
  1. Navigate to deep link URL which triggers server-side flight search:
     /flight-search/deeplink/searchflights/oneway/{origin}/{dest}/{date}/{adults}/{children}/{infants_seat}/{infants_lap}/?calendar=false
  2. Wait for Blazor WASM app to render results.
  3. Parse flight cards from the DOM (times, prices, duration, stops).

Avelo's booking engine is a Blazor WebAssembly app that uses gRPC and
session-scoped APIs. Direct API access requires session management,
so Playwright DOM scraping is the simplest reliable approach.

The route data API is public (no auth):
  GET https://api.aveloair.com/proxies/proxy-refdata/v1.0/read/location/route/all
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
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
from .browser import auto_block_if_proxied

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800


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

_GQL_SEAT_WAIT = 8  # seconds to wait for seat map API after clicking a flight card
_FLIGHT_CARD_SELECTORS = [
    "[class*='flight'] button",
    "button[class*='select']",
    "button[class*='flight']",
    "[data-testid*='flight'] button",
    "button:has-text('Select')",
    "button:has-text('Choose')",
]

_DEEPLINK_TPL = (
    "https://www.aveloair.com/flight-search/deeplink/searchflights/oneway"
    "/{origin}/{dest}/{date}/{adults}/{children}/0/0/?calendar=false"
)
_BOOKING_URL_TPL = (
    "https://www.aveloair.com/flight-search/deeplink/searchflights/oneway"
    "/{origin}/{dest}/{date}/{adults}/0/0/0/?calendar=false"
)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

# Shared browser singleton
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_proxy_url() -> str:
    return os.environ.get("AVELO_PROXY", "").strip()


def _get_pw_proxy() -> Optional[dict]:
    raw = _get_proxy_url()
    if not raw:
        return None
    from urllib.parse import urlparse
    p = urlparse(raw)
    result: dict[str, str] = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        result["username"] = p.username
    if p.password:
        result["password"] = p.password
    return result


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
        proxy = _get_pw_proxy()
        if proxy:
            launch_kw["proxy"] = proxy
        try:
            _browser = await pw.chromium.launch(**launch_kw)
        except Exception:
            launch_kw.pop("channel", None)
            _browser = await pw.chromium.launch(**launch_kw)
        logger.info("Avelo: headed Chrome launched (proxy=%s)", bool(proxy))
        return _browser


def _parse_duration(text: str) -> int:
    """Parse '2h 55m nonstop' or '4h 10m 1 stop' to seconds."""
    m = re.search(r'(\d+)h\s*(\d+)m', text)
    if m:
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60
    m = re.search(r'(\d+)h', text)
    if m:
        return int(m.group(1)) * 3600
    m = re.search(r'(\d+)m', text)
    if m:
        return int(m.group(1)) * 60
    return 0


def _parse_stops(text: str) -> int:
    """Parse 'nonstop' or '1 stop' from duration text."""
    if "nonstop" in text.lower():
        return 0
    m = re.search(r'(\d+)\s*stop', text.lower())
    return int(m.group(1)) if m else 0


def _parse_time(time_str: str, date_str: str) -> Optional[datetime]:
    """Parse '8:30 am' or '8:30 AM' + '2026-04-04' to datetime."""
    try:
        return datetime.strptime(f"{date_str} {time_str.strip().upper()}", "%Y-%m-%d %I:%M %p")
    except ValueError:
        return None


def _parse_price(text: str) -> Optional[float]:
    """Extract price from '$175' or '$1,234'."""
    m = re.search(r'\$([0-9,]+(?:\.\d{2})?)', text)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


class AveloConnectorClient:
    """Avelo Airlines connector — deep link URL + Playwright DOM scraping."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout
        self._last_live_seat_from: float | None = None

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            ob_offers = await self._search_via_playwright(req)
            live_seat_from = self._last_live_seat_from
            self._last_live_seat_from = None

            # --- RT: second OW search for inbound ---
            if req.return_from and ob_offers:
                ib_req = req.model_copy(update={
                    "origin": req.destination,
                    "destination": req.origin,
                    "date_from": req.return_from,
                    "return_from": None,
                })
                ib_offers = await self._search_via_playwright(ib_req)
                if ib_offers:
                    ob_offers = self._combine_rt(ob_offers, ib_offers, req)

            if ob_offers:
                segs = ob_offers[0].outbound.segments if ob_offers[0].outbound else []
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
                        self._apply_ancillaries(ob_offers, ancillary)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
                except Exception as _anc_err:
                    logger.debug("XP: ancillary fetch error: %s", _anc_err)

            return FlightSearchResponse(
                search_id=f"avelo_{req.origin}_{req.destination}_{req.date_from.strftime('%Y%m%d')}",
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=ob_offers,
                total_results=len(ob_offers),
            )
        except Exception as e:
            logger.error("Avelo search error: %s", e)
            return self._empty(req)

    async def _search_via_playwright(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
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

            date_str = req.date_from.strftime("%Y-%m-%d")
            url = _DEEPLINK_TPL.format(
                origin=req.origin,
                dest=req.destination,
                date=date_str,
                adults=req.adults,
                children=0,
            )

            logger.info("Avelo: navigating to %s", url)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Wait for the results page (Blazor WASM app loads)
            try:
                await page.wait_for_url(
                    "**/flight-search/select**",
                    timeout=25000,
                )
            except Exception:
                logger.warning("Avelo: did not redirect to select page")
                return []

            # Wait for flight cards to appear — Blazor WASM app renders flight buttons
            # Wait for any price text ($NNN) to appear on the page
            try:
                await page.wait_for_function(
                    "() => /\\$\\d{2,}/.test(document.body.innerText || '')",
                    timeout=25000,
                )
            except Exception:
                logger.warning("Avelo: no flight cards found")
                return []

            # Small extra wait for Blazor rendering
            await asyncio.sleep(1.5)

            # Set up seat map interception before clicking
            seat_event = asyncio.Event()
            captured: dict = {}

            async def _on_seat_response(response) -> None:
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    data = await response.json()
                    if isinstance(data, dict):
                        all_keys = set(_flatten_keys(data))
                        seat_keys = {k for k in all_keys if any(kw in k.lower() for kw in ("seat", "cabin", "map"))}
                        if seat_keys and not seat_event.is_set():
                            captured["seat_data"] = data
                            seat_event.set()
                except Exception:
                    pass

            page.on("response", _on_seat_response)

            # Extract flight data from DOM
            offers = await self._extract_flights(page, req, date_str)

            # Try to click a flight card to trigger seat map API
            if offers:
                await self._try_capture_seat_prices(page, seat_event, captured)
            self._last_live_seat_from = self._extract_min_seat_price(captured.get("seat_data") or {})

            return offers

        finally:
            await context.close()

    async def _extract_flights(
        self, page, req: FlightSearchRequest, date_str: str,
    ) -> list[FlightOffer]:
        """Extract flight offers from Blazor-rendered DOM."""
        booking_url = _BOOKING_URL_TPL.format(
            origin=req.origin,
            dest=req.destination,
            date=date_str,
            adults=req.adults,
        )

        raw_flights = await page.evaluate("""() => {
            // Avelo uses button elements for each flight card
            const buttons = document.querySelectorAll('button');
            const flights = [];
            for (const btn of buttons) {
                const text = btn.innerText || btn.textContent || '';
                // Flight buttons contain duration info like "2h 50m nonstop"
                if (!/(\\d+h\\s*\\d+m|nonstop|\\d+\\s*stop)/i.test(text)) continue;
                // Must have a price
                if (!/\\$\\d/.test(text)) continue;

                const lines = text.split(/\\r?\\n|\\r/).map(l => l.trim()).filter(Boolean);
                if (lines.length < 3) continue;

                const times = lines.filter(l => /^\\d{1,2}:\\d{2}\\s*[ap]m$/i.test(l));
                const codeLines = [];
                for (const l of lines) {
                    const m3 = l.match(/^([A-Z]{3})$/);
                    if (m3) { codeLines.push(m3[1]); continue; }
                    const m4 = l.match(/\\(([A-Z]{3})\\)/);
                    if (m4) codeLines.push(m4[1]);
                }
                const durLine = lines.find(l => /\\d+h\\s*\\d+m/i.test(l));
                const prices = lines.filter(l => /^\\$\\d/.test(l))
                    .map(l => parseFloat(l.replace('$', '').replace(',', '')));

                if (times.length < 2 || codeLines.length < 2 || !durLine || prices.length === 0) continue;

                flights.push({
                    departure_time: times[0],
                    arrival_time: times[1],
                    origin: codeLines[0],
                    destination: codeLines[1],
                    duration_text: durLine,
                    prices: prices,
                });
            }
            return {flights: flights};
        }""")

        if isinstance(raw_flights, dict):
            raw_flights = raw_flights.get("flights", [])

        offers: list[FlightOffer] = []
        for flight in raw_flights:
            dep_time = _parse_time(flight["departure_time"], date_str)
            arr_time = _parse_time(flight["arrival_time"], date_str)
            duration_secs = _parse_duration(flight["duration_text"])
            stops = _parse_stops(flight["duration_text"])

            # Use the "Standard" fare price (last/highest), which is the base fare
            # Avelo shows "Avelo PLUS" (member) and "Standard" (regular)
            prices = flight.get("prices", [])
            if not prices:
                continue

            # Standard price is typically the higher one (non-member)
            price = max(prices) if prices else 0
            if price <= 0:
                continue

            if not dep_time:
                dep_time = datetime.strptime(date_str, "%Y-%m-%d")
            if not arr_time:
                arr_time = dep_time + timedelta(seconds=duration_secs) if duration_secs else dep_time

            _xp_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="XP",
                airline_name="Avelo Airlines",
                flight_no="",
                origin=flight.get("origin", req.origin),
                destination=flight.get("destination", req.destination),
                departure=dep_time,
                arrival=arr_time,
                cabin_class=_xp_cabin,
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=duration_secs,
                stopovers=stops,
            )
            dep_key = flight["departure_time"]
            offer_id = f"xp_{hashlib.md5(f'{req.origin}{req.destination}{date_str}{price}{dep_key}'.encode()).hexdigest()[:12]}"
            offers.append(FlightOffer(
                id=offer_id,
                price=round(price, 2),
                currency="USD",
                price_formatted=f"${price:.2f}",
                outbound=route,
                inbound=None,
                airlines=["Avelo"],
                owner_airline="XP",
                booking_url=booking_url,
                is_locked=False,
                source="avelo_direct",
                source_tier="free",
                availability_seats=None,
            ))

        logger.info("Avelo: extracted %d offers for %s->%s", len(offers), req.origin, req.destination)
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
        bk_url = _BOOKING_URL_TPL.format(
            origin=req.origin, dest=req.destination,
            date=date_str, adults=req.adults,
        ) + f"&returnDate={ret_str}"
        combined: list[FlightOffer] = []
        for ob in ob_sorted:
            for ib in ib_sorted:
                total = round(ob.price + ib.price, 2)
                key = f"{ob.id}_{ib.id}"
                cid = f"xp_rt_{hashlib.md5(key.encode()).hexdigest()[:12]}"
                combined.append(FlightOffer(
                    id=cid,
                    price=total,
                    currency="USD",
                    price_formatted=f"${total:.2f}",
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    airlines=["Avelo"],
                    owner_airline="XP",
                    booking_url=bk_url,
                    is_locked=False,
                    source="avelo_direct",
                    source_tier="free",
                ))
        combined.sort(key=lambda o: o.price)
        return combined

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id=f"avelo_{req.origin}_{req.destination}_{req.date_from.strftime('%Y%m%d')}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
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
            # Live seat price captured — skip cache, return immediately
            return {
                "carry_on_from": 25.0,
                "carry_on_note": "carry-on from +USD 25 (no bags on base fare — add at booking)",
                "checked_from": 35.0,
                "checked_note": "first checked bag from +USD 35 (add at booking)",
                "seat_from": live_seat_from,
                "seat_note": f"seat selection from +USD {live_seat_from:.2f} (live, this route)",
                "currency": "USD",
            }
        cache_key = f"xp_{origin}_{dest}_{date_str}"
        now = time.time()
        if cache_key in _ancillary_cache:
            ts, cached = _ancillary_cache[cache_key]
            if now - ts < _ANCILLARY_CACHE_TTL:
                return cached
        # Avelo: no bags included on any fare. Carry-on ~$25–$55.
        result: dict = {
            "carry_on_from": 25.0,
            "carry_on_note": "carry-on from +USD 25 (no bags on base fare — add at booking)",
            "checked_from": 35.0,
            "checked_note": "first checked bag from +USD 35 (add at booking)",
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
            if carry_on_from == 0.0:
                offer.bags_price["carry_on"] = 0.0
            if checked_from == 0.0:
                offer.bags_price["checked_bag"] = 0.0
            if seat_from == 0.0:
                offer.bags_price["seat_selection"] = 0.0

    # ------------------------------------------------------------------
    # Live seat price capture helpers (same pattern as allegiant.py)
    # ------------------------------------------------------------------

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
                logger.debug("Avelo: clicked flight card with selector '%s'", selector)
                try:
                    await asyncio.wait_for(seat_event.wait(), timeout=_GQL_SEAT_WAIT)
                except asyncio.TimeoutError:
                    logger.debug("Avelo: seat event timed out after clicking '%s'", selector)
                return
            except Exception as _exc:
                logger.debug("Avelo: seat card click failed for '%s': %s", selector, _exc)

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
