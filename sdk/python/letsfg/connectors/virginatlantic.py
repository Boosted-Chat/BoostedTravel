"""
Virgin Atlantic CDP Chrome connector — GraphQL response interception.

Virgin Atlantic (IATA: VS) is a UK long-haul airline.
Hub at London Heathrow (LHR) flying to 30+ destinations in the Americas,
Caribbean, Africa, Asia, and Middle East. Part of the SkyTeam alliance.

Strategy (CDP Chrome + GraphQL interception):
1. Launch REAL system Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP.  NO stealth injection (causes Akamai detection).
3. Navigate directly to parameterised search results URL.
4. Akamai challenges the GraphQL requests (429) — browser JS auto-solves.
5. Capture SearchOffers GraphQL response via page.on("response").
6. Parse flightsAndFares → FlightOffers with real bookable prices.
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
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import (
    find_chrome,
    stealth_popen_kwargs,
    proxy_chrome_args,
    auto_block_if_proxied,
    disable_background_networking_args,
)

logger = logging.getLogger(__name__)

# ── Singleton Chrome state ────────────────────────────────────────────────

_DEBUG_PORT = 9451
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".virginatlantic_chrome_data"
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


async def _get_context():
    global _context
    browser = await _get_browser()
    if _context:
        try:
            if _context.pages is not None:
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
    """Launch real Chrome via CDP — NO stealth injection (Akamai detects it)."""
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
        from .browser import _launched_procs

        # Try connecting to existing Chrome on the port first
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("VS: connected to existing Chrome on port %d", _DEBUG_PORT)
            return _browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        # Launch Chrome HEADED — Akamai 403s headless.
        # CRITICAL: Do NOT use inject_stealth_js — it triggers Akamai detection.
        chrome = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)
        args = [
            chrome,
            f"--remote-debugging-port={_DEBUG_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--no-first-run",
            *proxy_chrome_args(),
            *disable_background_networking_args(),
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_DEBUG_PORT}"
        )
        logger.info(
            "VS: Chrome launched headed on CDP port %d (pid %d)",
            _DEBUG_PORT,
            _chrome_proc.pid,
        )
        return _browser


async def _reset_chrome_profile():
    """Kill Chrome and wipe user-data-dir to clear Akamai-flagged sessions."""
    global _browser, _chrome_proc, _context
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    _browser = None
    _context = None
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
        _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("VS: deleted stale Chrome profile %s", _USER_DATA_DIR)
        except Exception as e:
            logger.warning("VS: failed to delete Chrome profile: %s", e)


# ── ISO 8601 duration parser ─────────────────────────────────────────────

_PT_RE = re.compile(
    r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", re.IGNORECASE
)


def _parse_pt_duration(s: str) -> int:
    """Parse ISO 8601 duration like 'PT8H10M' → seconds."""
    m = _PT_RE.match(s or "")
    if not m:
        return 0
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def _parse_dt(s: str) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        try:
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return datetime(2000, 1, 1)


# ── Fare family → cabin class mapping ────────────────────────────────────

def _cabin_from_fare_family(fare_family_type: str) -> str:
    """Map VA fareFamilyType to standard cabin class code."""
    ff = (fare_family_type or "").upper()
    if "BUS" in ff or "UPPER" in ff:
        return "C"
    if "PREMIUM" in ff or "COMFORT" in ff:
        return "W"
    if "FIRST" in ff:
        return "F"
    return "M"


# ── Connector class ──────────────────────────────────────────────────────

class VirginAtlanticConnectorClient:
    """Virgin Atlantic — CDP Chrome + GraphQL SearchOffers interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return await self._search_single(req)

    async def _search_single(
        self, req: FlightSearchRequest, _retry: int = 0
    ) -> FlightSearchResponse:
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        # CRITICAL: Do NOT call inject_stealth_js(page) — it triggers Akamai.
        await auto_block_if_proxied(page)

        # Response interception state
        search_data: dict = {}
        akamai_blocked = False
        got_429_count = 0

        async def _on_response(response):
            nonlocal akamai_blocked, got_429_count
            url = response.url
            if "/graphql" not in url:
                return
            status = response.status
            if status == 429:
                got_429_count += 1
                if got_429_count <= 3:
                    logger.info("VS: GraphQL 429 (Akamai challenge %d) — waiting for auto-resolve", got_429_count)
                return
            if status in (403, 444):
                akamai_blocked = True
                logger.warning("VS: Akamai %d on GraphQL", status)
                return
            if status != 200:
                return
            try:
                body = await response.json()
                if not isinstance(body, dict):
                    return
                faf = (
                    body.get("data", {})
                    .get("searchOffers", {})
                    .get("result", {})
                    .get("slice", {})
                    .get("flightsAndFares")
                )
                if faf is not None:
                    search_data.update(body)
                    logger.info(
                        "VS: captured SearchOffers GraphQL (%d flights)",
                        len(faf),
                    )
            except Exception as e:
                logger.debug("VS: GraphQL parse error: %s", e)

        page.on("response", _on_response)

        try:
            # Build search URL
            adults = max(1, req.adults or 1)
            children = req.children or 0
            infants = req.infants or 0
            date_str = req.date_from.strftime("%Y-%m-%d") if hasattr(req.date_from, "strftime") else str(req.date_from)[:10]

            search_url = (
                f"https://www.virginatlantic.com/en-EU/flights/search/slice"
                f"?passengers=a{adults}t0c{children}i{infants}"
                f"&origin={req.origin}&destination={req.destination}"
                f"&departing={date_str}"
            )

            logger.info("VS: navigating to %s", search_url)
            await page.goto(
                search_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            # Wait for GraphQL SearchOffers response.
            # Akamai initially returns 429, browser JS auto-solves (~5–25s).
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while (
                not search_data
                and not akamai_blocked
                and time.monotonic() < deadline
            ):
                await asyncio.sleep(0.5)

            # If hard-blocked, reset profile and retry once
            if akamai_blocked and not search_data:
                logger.warning("VS: Akamai hard-blocked, resetting Chrome profile")
                await _reset_chrome_profile()
                if _retry < 1:
                    logger.info("VS: retrying with fresh profile")
                    await asyncio.sleep(2.0)
                    return await self._search_single(req, _retry=_retry + 1)
                logger.warning("VS: Akamai blocked after retry, giving up")
                return self._empty(req)

            if not search_data:
                logger.warning("VS: no SearchOffers data received within timeout")
                return self._empty(req)

            # Parse
            offers = self._parse_graphql(search_data, req, search_url)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "VS %s→%s: %d offers in %.1fs (CDP Chrome)",
                req.origin, req.destination, len(offers), elapsed,
            )

            h = hashlib.md5(
                f"vs{req.origin}{req.destination}{date_str}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else "GBP",
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("VS CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # GraphQL response parsing
    # ------------------------------------------------------------------

    def _parse_graphql(
        self, body: dict, req: FlightSearchRequest, booking_url: str
    ) -> list[FlightOffer]:
        faf_list = (
            body.get("data", {})
            .get("searchOffers", {})
            .get("result", {})
            .get("slice", {})
            .get("flightsAndFares", [])
        )
        if not faf_list:
            return []

        offers: list[FlightOffer] = []
        for item in faf_list:
            offer = self._parse_flight_and_fares(item, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _parse_flight_and_fares(
        self, item: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        flight = item.get("flight", {})
        fares = item.get("fares", [])
        if not flight or not fares:
            return None

        # Find cheapest fare
        best_fare = None
        best_price = float("inf")
        for fare in fares:
            price_obj = fare.get("price")
            if not price_obj:
                continue
            amt = price_obj.get("amountIncludingTax")
            if amt is not None and amt > 0 and amt < best_price:
                best_price = amt
                best_fare = fare

        if best_fare is None:
            return None

        price_obj = best_fare["price"]
        price_f = round(best_price, 2)
        currency = price_obj.get("currency", "GBP")
        fare_family = best_fare.get("fareFamilyType", "")
        cabin_class = _cabin_from_fare_family(fare_family)

        # Get cabin name from fareSegments if available
        fare_segs = best_fare.get("fareSegments", [])
        cabin_name = fare_segs[0].get("cabinName", "") if fare_segs else ""

        # Build segments
        raw_segments = flight.get("segments", [])
        if not raw_segments:
            return None

        segments: list[FlightSegment] = []
        for seg in raw_segments:
            airline_obj = seg.get("airline", {})
            op_airline = seg.get("operatingAirline", {})
            origin_obj = seg.get("origin", {})
            dest_obj = seg.get("destination", {})

            airline_code = airline_obj.get("code", "VS")
            airline_name = airline_obj.get("name", "Virgin Atlantic")
            flight_number = seg.get("flightNumber", "")

            seg_duration = _parse_pt_duration(seg.get("duration", ""))

            segments.append(FlightSegment(
                airline=airline_code,
                airline_name=airline_name,
                flight_no=flight_number,
                origin=origin_obj.get("code", ""),
                destination=dest_obj.get("code", ""),
                origin_city=origin_obj.get("cityName", ""),
                destination_city=dest_obj.get("cityName", ""),
                departure=_parse_dt(seg.get("departure", "")),
                arrival=_parse_dt(seg.get("arrival", "")),
                duration_seconds=seg_duration,
                cabin_class=cabin_class,
            ))

        total_duration = _parse_pt_duration(flight.get("duration", ""))
        stopovers = max(0, len(segments) - 1)

        outbound = FlightRoute(
            segments=segments,
            total_duration_seconds=total_duration,
            stopovers=stopovers,
        )

        # Build unique ID from flight details
        seg_key = "_".join(
            f"{s.flight_no}_{s.departure.strftime('%H%M') if s.departure.year > 2000 else ''}"
            for s in segments
        )
        fid = hashlib.md5(
            f"vs_{req.origin}_{req.destination}_{seg_key}_{price_f}".encode()
        ).hexdigest()[:12]

        # Collect all airline names
        airlines = list(dict.fromkeys(
            seg.get("airline", {}).get("name", "Virgin Atlantic")
            for seg in raw_segments
        ))

        conditions = {}
        if cabin_name:
            conditions["cabin"] = cabin_name
        if fare_family:
            conditions["fare_family"] = fare_family

        return FlightOffer(
            id=f"vs_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=outbound,
            inbound=None,
            airlines=airlines,
            owner_airline="VS",
            booking_url=booking_url,
            is_locked=False,
            source="virginatlantic_direct",
            source_tier="free",
            conditions=conditions,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"vs{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="GBP",
            offers=[],
            total_results=0,
        )
