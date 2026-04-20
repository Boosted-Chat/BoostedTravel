"""
Finnair connector — Playwright browser session + instantsearch API.

Finnair (IATA: AY) is Finland's flag carrier. Oneworld member.
Key for Nordic/Asia routes via HEL hub. 130+ destinations.

Strategy:
  The instantsearch API at api.finnair.com returns "starting from" prices
  per route per cabin class. It requires an Akamai-validated browser session
  (direct httpx gets 403). We use Playwright to visit finnair.com once,
  then call the API via page.evaluate(fetch(...)).

  Multi-destination batching: the API accepts comma-separated destination codes,
  so we batch up to ~20 destinations per call.

  Limitation: only works for routes FROM Helsinki (HEL). Non-HEL origins
  return empty prices.

API details (discovered Mar 2026):
  GET api.finnair.com/d/fcom/instantsearch-prod/current/api/instantsearch/prices/flights
    ?departureLocationCodes=HEL&destinationLocationCodes=LHR,CDG,BCN
  Response: {
    "callDetails": {"calls": [{"duration": 132}]},
    "prices": {
      "HEL": {
        "LHR": {
          "currency": "EUR", "from": "HEL", "to": "LHR",
          "travelClassPrices": [
            {"fromDate":"2026-09-21","price":208,"toDate":"2026-09-28",
             "travelClass":"Economy","tripType":"return"},
            {"fromDate":"2026-04-21","price":584,"toDate":"2026-05-02",
             "travelClass":"Business","tripType":"return"}
          ]
        }
      }
    }
  }
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
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
from .browser import _launched_procs, _launched_pw_instances, auto_block_if_proxied, find_chrome, proxy_chrome_args, stealth_popen_kwargs

logger = logging.getLogger(__name__)

_SEARCH_API = (
    "https://api.finnair.com/d/fcom/instantsearch-prod"
    "/current/api/instantsearch/prices/flights"
)
_DEBUG_PORT = 9514
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".finnair_chrome_profile"
)
_SESSION_MAX_AGE = 20 * 60  # Re-establish session every 20 min

# Shared browser state
_farm_lock: Optional[asyncio.Lock] = None
_pw_instance = None
_browser = None
_chrome_proc = None
_warm_page = None
_session_ts: float = 0.0


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Launch real Chrome via CDP for Finnair session."""
    global _pw_instance, _browser, _chrome_proc

    if _browser:
        try:
            if _browser.is_connected():
                return _browser
        except Exception:
            pass

    from playwright.async_api import async_playwright

    # Try connecting to existing Chrome
    pw = None
    try:
        pw = await async_playwright().start()
        _browser = await pw.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_DEBUG_PORT}"
        )
        _pw_instance = pw
        _launched_pw_instances.append(pw)
        logger.info("Finnair: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch new Chrome
    chrome = find_chrome()
    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    args = [
        chrome,
        f"--remote-debugging-port={_DEBUG_PORT}",
        f"--user-data-dir={_USER_DATA_DIR}",
        "--no-first-run",
        *proxy_chrome_args(),
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",
        "--headless=new",
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
    _launched_pw_instances.append(pw)
    _browser = await pw.chromium.connect_over_cdp(
        f"http://127.0.0.1:{_DEBUG_PORT}"
    )
    logger.info(
        "Finnair: Chrome launched on CDP port %d (pid %d)",
        _DEBUG_PORT,
        _chrome_proc.pid,
    )
    return _browser


class FinnairConnectorClient:
    """Finnair — Playwright browser + instantsearch API."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

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

        try:
            page = await self._ensure_session()
            if not page:
                logger.warning("Finnair: session setup failed")
                return self._empty(req)

            data = await self._api_search(page, req.origin, req.destination)
            if data is None:
                # Session might be stale — refresh once
                logger.info("Finnair: API failed, refreshing session")
                page = await self._refresh_session()
                if page:
                    data = await self._api_search(page, req.origin, req.destination)

            if not data:
                return self._empty(req)

            offers = self._parse(data, req)
            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info(
                "Finnair %s→%s: %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            sh = hashlib.md5(
                f"finnair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else "EUR",
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("Finnair search error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------
    # Session management — visit finnair.com to get Akamai cookies
    # ------------------------------------------------------------------

    async def _ensure_session(self):
        """Return a warm page with valid Finnair session."""
        global _warm_page, _session_ts

        lock = _get_farm_lock()
        async with lock:
            age = time.monotonic() - _session_ts
            if _warm_page and age < _SESSION_MAX_AGE:
                try:
                    # Quick health check
                    await _warm_page.evaluate("1+1")
                    return _warm_page
                except Exception:
                    pass
            return await self._refresh_session()

    async def _refresh_session(self):
        """Create a new Playwright page with valid Finnair session cookies."""
        global _warm_page, _session_ts

        browser = await _get_browser()
        context = (
            browser.contexts[0]
            if browser.contexts
            else await browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="en-US",
            )
        )

        # Close old warm page
        if _warm_page:
            try:
                await _warm_page.close()
            except Exception:
                pass

        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)
            await page.goto(
                "https://www.finnair.com/en",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(4)

            # Accept cookie banner
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    const t = b.innerText.toLowerCase().trim();
                    if (t.includes('allow all') || t.includes('accept all')
                        || t === 'accept') {
                        b.click(); return true;
                    }
                }
                return false;
            }""")
            await asyncio.sleep(1)

            # Verify session works by making a test API call
            test = await page.evaluate("""async () => {
                try {
                    const r = await fetch('%s?departureLocationCodes=HEL&destinationLocationCodes=LHR');
                    return r.status;
                } catch(e) { return 0; }
            }""" % _SEARCH_API)

            if test == 200:
                _warm_page = page
                _session_ts = time.monotonic()
                logger.info("Finnair: session established (API returns 200)")
                return page
            else:
                logger.warning("Finnair: session test returned %s", test)
                await page.close()
                return None

        except Exception as e:
            logger.error("Finnair: session setup error: %s", e)
            return None

    # ------------------------------------------------------------------
    # API call via page.evaluate(fetch(...))
    # ------------------------------------------------------------------

    async def _api_search(
        self, page, origin: str, destination: str
    ) -> Optional[dict]:
        """Call instantsearch API from the browser context."""
        import json as _json

        url = f"{_SEARCH_API}?departureLocationCodes={origin}&destinationLocationCodes={destination}"
        try:
            result = await page.evaluate("""async (url) => {
                try {
                    const resp = await fetch(url);
                    if (resp.status !== 200) return {error: resp.status};
                    return {data: await resp.json()};
                } catch(e) { return {error: e.message}; }
            }""", url)

            if "error" in result:
                logger.warning("Finnair API error: %s", result["error"])
                return None
            return result.get("data")

        except Exception as e:
            logger.error("Finnair page.evaluate error: %s", e)
            return None

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse instantsearch response into FlightOffers."""
        offers: list[FlightOffer] = []
        prices = data.get("prices", {})

        for origin_code, dests in prices.items():
            dest_info = dests.get(req.destination, {})
            if not dest_info:
                continue

            currency = dest_info.get("currency", "EUR")
            _burl = (
                f"https://www.finnair.com/en/flights/{req.origin.lower()}-"
                f"{req.destination.lower()}"
            )
            if req.return_from:
                _burl += f"?returnDate={req.return_from}"
            booking_url = _burl

            for tp in dest_info.get("travelClassPrices", []):
                price = tp.get("price", 0)
                if price <= 0:
                    continue

                travel_class = tp.get("travelClass", "Economy")
                from_date = tp.get("fromDate", "")
                to_date = tp.get("toDate", "")
                trip_type = tp.get("tripType", "return")

                # Build a representative segment
                _ay_cabin = {"Economy": "economy", "PremiumEconomy": "premium_economy", "Business": "business", "First": "first"}.get(travel_class, "economy")
                dep_dt = datetime.combine(
                    req.date_from, datetime.min.time().replace(hour=8)
                )
                seg = FlightSegment(
                    airline="AY",
                    airline_name="Finnair",
                    flight_no="AY",
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_dt,
                    arrival=dep_dt,
                    cabin_class=_ay_cabin,
                )
                route = FlightRoute(
                    segments=[seg], total_duration_seconds=0, stopovers=0
                )

                # RT: add placeholder inbound (Finnair fares are already return-trip priced)
                ib_route = None
                if trip_type == "return" and req.return_from:
                    ret_dt = datetime.combine(
                        req.return_from, datetime.min.time().replace(hour=8)
                    ) if not isinstance(req.return_from, datetime) else req.return_from
                    ib_seg = FlightSegment(
                        airline="AY", airline_name="Finnair", flight_no="AY",
                        origin=req.destination, destination=req.origin,
                        departure=ret_dt, arrival=ret_dt,
                        cabin_class=_ay_cabin,
                    )
                    ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)

                key = f"ay_{req.origin}{req.destination}{travel_class}{price}"
                oid = hashlib.md5(key.encode()).hexdigest()[:12]

                offers.append(
                    FlightOffer(
                        id=f"ay_rt_{oid}" if ib_route else f"ay_{oid}",
                        price=round(float(price), 2),
                        currency=currency,
                        price_formatted=f"{price:.2f} {currency}",
                        outbound=route,
                        inbound=ib_route,
                        airlines=["Finnair"],
                        owner_airline="AY",
                        conditions={
                            "cabin": travel_class,
                            "valid_from": from_date,
                            "valid_to": to_date,
                            "trip_type": trip_type,
                            "price_type": "starting_from",
                        },
                        booking_url=booking_url,
                        is_locked=False,
                        source="finnair_direct",
                        source_tier="free",
                    )
                )

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        sh = hashlib.md5(
            f"finnair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
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
                    id=f"rt_finn_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
