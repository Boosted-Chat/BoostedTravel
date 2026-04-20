"""Bangkok Airways connector — Playwright session + Amadeus DES API.

Bangkok Airways (IATA: PG) — BKK/USM hubs, 25 destinations across
Thailand, Cambodia, Laos, Maldives, Bangladesh, Hong Kong.

Strategy:
  Bangkok Airways uses Amadeus Digital Experience Suite (DES) behind an
  Incapsula WAF.  Direct httpx calls to the search endpoint get 403.  We:

  1. Launch headed Chrome → visit digital.bangkokair.com/booking to pass
     the Incapsula challenge and obtain WAF cookies.
  2. Open a second page on api-des.bangkokair.com (same context = shared
     cookies) and call the API via page.evaluate(fetch):
       - POST /v1/security/oauth2/token/initialization (OAuth2 token)
       - POST /v2/search/air-bounds (flight search)
  3. Parse the Amadeus DES response into FlightOffer objects.

  Session setup: ~15s (one-time).  Each search: <1s.
  Token lives 1800s; session refreshed every 15 min.

API endpoints (discovered Mar 2026):
  Base: https://api-des.bangkokair.com
  Token: POST /v1/security/oauth2/token/initialization
         form-urlencoded: client_id, client_secret, grant_type=client_credentials, fact
  Search: POST /v2/search/air-bounds
         Bearer token, JSON body with itinerary/travelers

Response format: Amadeus DES airBoundGroups
  data.airBoundGroups[].boundDetails — origin, dest, duration, segments
  data.airBoundGroups[].airBounds[] — fare options with prices, cabin, quota
  Segment IDs encode flight info: SEG-PG109-BKKUSM-2026-04-16-0640
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
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
from .browser import _launched_pw_instances, acquire_browser_slot, auto_block_if_proxied, release_browser_slot, get_proxy

logger = logging.getLogger(__name__)

_API_BASE = "https://api-des.bangkokair.com"
_TOKEN_URL = f"{_API_BASE}/v1/security/oauth2/token/initialization"
_SEARCH_URL = f"{_API_BASE}/v2/search/air-bounds"
_CLIENT_ID = "GOsS3AbywB1UUsiTBqmS03GR2eYX5pZn"
_CLIENT_SECRET = "CcKzc5n82S2ya6Zv"
_SESSION_MAX_AGE = 15 * 60  # Refresh Incapsula session every 15 min
_TOKEN_MAX_AGE = 25 * 60  # Token expires in 1800s, refresh at 25 min

# Shared browser state (module-level singleton)
_farm_lock: Optional[asyncio.Lock] = None
_pw_instance = None
_browser = None
_api_page = None
_session_ts: float = 0.0
_token: Optional[str] = None
_token_ts: float = 0.0

_SEG_RE = re.compile(
    r"SEG-([A-Z0-9]+?)(\d+)-"          # airline + flight_no
    r"([A-Z]{3})([A-Z]{3})-"           # origin + destination
    r"(\d{4}-\d{2}-\d{2})-(\d{4})$"    # date + HHMM
)


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _ensure_session():
    """Return an api_page with valid Incapsula cookies."""
    global _pw_instance, _browser, _api_page, _session_ts

    age = time.monotonic() - _session_ts
    if _api_page and age < _SESSION_MAX_AGE:
        try:
            await _api_page.evaluate("1+1")
            return _api_page
        except Exception:
            pass

    return await _refresh_session()


async def _refresh_session():
    """Create Playwright session with Incapsula cookies."""
    global _pw_instance, _browser, _api_page, _session_ts, _token, _token_ts

    # Close old resources
    if _api_page:
        try:
            await _api_page.close()
        except Exception:
            pass
        _api_page = None

    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None

    if _pw_instance:
        try:
            await _pw_instance.stop()
        except Exception:
            pass
        _pw_instance = None

    _token = None
    _token_ts = 0.0

    from playwright.async_api import async_playwright

    pw = await async_playwright().start()
    _pw_instance = pw
    _launched_pw_instances.append(pw)

    browser = await pw.chromium.launch(
        headless=False, channel="chrome",
        proxy=get_proxy("BANGKOKAIRWAYS_PROXY"),
        args=[
            "--disable-blink-features=AutomationControlled",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
        ],
    )
    _browser = browser

    ctx = await browser.new_context(
        viewport={"width": 1366, "height": 768},
        locale="en-US",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        ),
    )

    # Visit booking page to pass Incapsula challenge
    page = await ctx.new_page()
    await auto_block_if_proxied(page)
    await page.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    )

    try:
        await page.goto(
            "https://www.bangkokair.com/",
            wait_until="domcontentloaded", timeout=30000,
        )
        await asyncio.sleep(2)
        await page.goto(
            "https://digital.bangkokair.com/booking?lang=en-GB",
            wait_until="domcontentloaded", timeout=45000,
        )

        # Wait for Incapsula challenge to resolve
        for _ in range(15):
            await asyncio.sleep(1)
            title = await page.title()
            if title and "Pardon" not in title:
                break

        # Open a clean page on the API domain (shares cookies)
        _api_page = await ctx.new_page()
        await auto_block_if_proxied(_api_page)
        await _api_page.goto(
            f"{_API_BASE}/", wait_until="commit", timeout=15000
        )
        await asyncio.sleep(0.5)

        # Close the booking page (no longer needed)
        await page.close()

        _session_ts = time.monotonic()
        logger.info("Bangkok Airways: session established")
        return _api_page

    except Exception as e:
        logger.error("Bangkok Airways: session setup failed: %s", e)
        try:
            await page.close()
        except Exception:
            pass
        return None


async def _get_token(api_page, origin: str, dest: str, dep_date: str) -> Optional[str]:
    """Get or refresh OAuth2 token."""
    global _token, _token_ts

    age = time.monotonic() - _token_ts
    if _token and age < _TOKEN_MAX_AGE:
        return _token

    result = await api_page.evaluate("""async ([origin, dest, dt]) => {
        const fact = JSON.stringify({keyValuePairs:[
            {key:'originLocationCode1',value:origin},
            {key:'destinationLocationCode1',value:dest},
            {key:'departureDateTime1',value:dt},
            {key:'countrySite',value:'THDESKTOP'}
        ]});
        const body = 'client_id=GOsS3AbywB1UUsiTBqmS03GR2eYX5pZn'
            + '&client_secret=CcKzc5n82S2ya6Zv'
            + '&grant_type=client_credentials'
            + '&fact=' + encodeURIComponent(fact);
        try {
            const resp = await fetch('%s', {
                method: 'POST',
                headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                body: body
            });
            if (!resp.ok) return {error: resp.status};
            const data = await resp.json();
            return {token: data.access_token};
        } catch(e) { return {error: e.message}; }
    }""" % _TOKEN_URL, [origin, dest, dep_date])

    if "error" in result:
        logger.warning("Bangkok Airways token error: %s", result["error"])
        return None

    _token = result["token"]
    _token_ts = time.monotonic()
    return _token


async def _api_search(api_page, token: str, origin: str, dest: str, dep_date: str) -> Optional[dict]:
    """Call air-bounds search API via page.evaluate(fetch)."""
    result = await api_page.evaluate("""async ([token, origin, dest, dt]) => {
        const body = {
            commercialFareFamilies: ["PGREFXFLEX"],
            itineraries: [{
                originLocationCode: origin,
                destinationLocationCode: dest,
                departureDateTime: dt + "T00:00:00.000",
                isRequestedBound: true
            }],
            travelers: [{passengerTypeCode: "ADT"}],
            searchPreferences: {showMilesPrice: false}
        };
        try {
            const resp = await fetch('%s', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + token
                },
                body: JSON.stringify(body)
            });
            if (!resp.ok) return {error: resp.status};
            return {data: await resp.json()};
        } catch(e) { return {error: e.message}; }
    }""" % _SEARCH_URL, [token, origin, dest, dep_date])

    if "error" in result:
        logger.warning("Bangkok Airways search error: %s", result["error"])
        return None

    return result.get("data")


def _parse_segment_id(seg_id: str) -> Optional[dict]:
    """Parse 'SEG-PG109-BKKUSM-2026-04-16-0640' into components."""
    m = _SEG_RE.match(seg_id)
    if not m:
        return None
    airline, fno, orig, dest, dep_date, hhmm = m.groups()
    dep_dt = datetime.strptime(f"{dep_date} {hhmm}", "%Y-%m-%d %H%M")
    return {
        "airline": airline,
        "flight_no": f"{airline}{fno}",
        "origin": orig,
        "destination": dest,
        "departure": dep_dt,
    }


def _parse(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse Amadeus DES airBoundGroups into FlightOffers."""
    offers: list[FlightOffer] = []
    groups = data.get("data", {}).get("airBoundGroups", [])

    for group in groups:
        bd = group.get("boundDetails", {})
        duration = bd.get("duration", 0)
        seg_ids = [s["flightId"] if isinstance(s, dict) else s
                   for s in bd.get("segments", [])]

        # Parse segments from IDs
        segments: list[FlightSegment] = []
        for sid in seg_ids:
            info = _parse_segment_id(sid)
            if not info:
                continue
            arr_dt = datetime.fromtimestamp(
                info["departure"].timestamp() + duration
            ) if duration else info["departure"]
            segments.append(FlightSegment(
                airline=info["airline"],
                airline_name="Bangkok Airways",
                flight_no=info["flight_no"],
                origin=info["origin"],
                destination=info["destination"],
                departure=info["departure"],
                arrival=arr_dt,
            ))

        if not segments:
            continue

        stopovers = max(0, len(segments) - 1)
        route = FlightRoute(
            segments=segments,
            total_duration_seconds=duration,
            stopovers=stopovers,
        )

        for ab in group.get("airBounds", []):
            tp = ab.get("prices", {}).get("totalPrices", [])
            if not tp:
                continue
            price = tp[0].get("total", 0)
            currency = tp[0].get("currencyCode", "THB")
            if price <= 0:
                continue

            cabin = "Economy"
            avail = ab.get("availabilityDetails", [])
            if avail and avail[0].get("cabin") == "bus":
                cabin = "Business"
            seats = avail[0].get("quota", 0) if avail else 0
            fare_family = ab.get("fareFamilyCode", "")

            key = f"pg_{segments[0].flight_no}_{fare_family}_{price}"
            oid = hashlib.md5(key.encode()).hexdigest()[:12]

            booking_url = (
                f"https://www.bangkokair.com/flight/booking"
                f"?origin={req.origin}&destination={req.destination}"
            )

            offers.append(FlightOffer(
                id=f"pg_{oid}",
                price=round(float(price), 2),
                currency=currency,
                price_formatted=f"{price:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Bangkok Airways"],
                owner_airline="PG",
                conditions={
                    "cabin": cabin,
                    "fare_family": fare_family,
                    "seats_left": str(seats),
                },
                booking_url=booking_url,
                is_locked=False,
                source="bangkokairways_direct",
                source_tier="free",
            ))

    return offers


def _combine_rt(
    ob_offers: list[FlightOffer],
    ib_offers: list[FlightOffer],
    req: FlightSearchRequest,
) -> list[FlightOffer]:
    """Cross-multiply OB x IB into RT combo offers."""
    ob_sorted = sorted(ob_offers, key=lambda o: o.price)[:15]
    ib_sorted = sorted(ib_offers, key=lambda o: o.price)[:10]
    date_str = req.date_from.strftime("%Y-%m-%d")
    ret_str = req.return_from.strftime("%Y-%m-%d")
    bk_url = (
        f"https://www.bangkokair.com/flight/booking"
        f"?origin={req.origin}&destination={req.destination}"
        f"&departDate={date_str}&returnDate={ret_str}&tripType=R"
    )
    combined: list[FlightOffer] = []
    for ob in ob_sorted:
        for ib in ib_sorted:
            total = round(ob.price + ib.price, 2)
            cur = ob.currency or ib.currency or "THB"
            key = f"{ob.id}_{ib.id}"
            cid = f"pg_rt_{hashlib.md5(key.encode()).hexdigest()[:12]}"
            combined.append(FlightOffer(
                id=cid,
                price=total,
                currency=cur,
                price_formatted=f"{total:.0f} {cur}",
                outbound=ob.outbound,
                inbound=ib.outbound,
                airlines=list(set(ob.airlines + ib.airlines)),
                owner_airline="PG",
                conditions=ob.conditions,
                booking_url=bk_url,
                is_locked=False,
                source="bangkokairways_direct",
                source_tier="free",
            ))
    combined.sort(key=lambda o: o.price)
    return combined


class BangkokAirwaysConnectorClient:
    """Bangkok Airways — Playwright session + Amadeus DES API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        dep_date = req.date_from.isoformat()

        try:
            lock = _get_farm_lock()
            async with lock:
                api_page = await _ensure_session()

            if not api_page:
                logger.warning("Bangkok Airways: session setup failed")
                return self._empty(req)

            token = await _get_token(api_page, req.origin, req.destination, dep_date)
            if not token:
                # Session might be stale — refresh once
                logger.info("Bangkok Airways: token failed, refreshing session")
                async with lock:
                    api_page = await _refresh_session()
                if api_page:
                    token = await _get_token(
                        api_page, req.origin, req.destination, dep_date
                    )
                if not token:
                    return self._empty(req)

            data = await _api_search(api_page, token, req.origin, req.destination, dep_date)
            if not data:
                return self._empty(req)

            offers = _parse(data, req)

            # --- RT: second API call for inbound ---
            if req.return_from and offers:
                ret_date = req.return_from.isoformat()
                ib_data = await _api_search(
                    api_page, token, req.destination, req.origin, ret_date,
                )
                if ib_data:
                    ib_offers = _parse(ib_data, req)
                    if ib_offers:
                        offers = _combine_rt(offers, ib_offers, req)

            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "Bangkok Airways %s→%s: %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            sh = hashlib.md5(
                f"pg{req.origin}{req.destination}{dep_date}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{sh}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else "THB",
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Bangkok Airways search error: %s", e)
            return self._empty(req)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        sh = hashlib.md5(
            f"pg{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency="THB",
            offers=[],
            total_results=0,
        )
