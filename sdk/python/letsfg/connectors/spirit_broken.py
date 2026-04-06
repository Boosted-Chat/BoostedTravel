"""
Spirit Airlines connector — CDP Chrome + in-page fetch API calls.

Spirit (IATA: NK) is a US ultra-low-cost carrier operating domestic and
Caribbean/Latin America routes. Protected by Akamai Bot Manager + PerimeterX.

Strategy (CDP Chrome + same-origin fetch):
1.  Launch real Chrome via CDP (--remote-debugging-port).
2.  Navigate to spirit.com homepage to establish Akamai/PX cookies.
3.  Call ``POST /api/prod-token/api/v1/token`` from page context
    via ``page.evaluate(fetch(…))`` to get a Navitaire session token.
4.  Call ``POST /api/prod-availability/api/availability/v3/search``
    with the bearer token to get flight availability.
5.  Parse the Navitaire response into FlightOffers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, date as date_type
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
<<<<<<< Updated upstream
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs
=======
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied
>>>>>>> Stashed changes

logger = logging.getLogger(__name__)

_CDP_PORT = 9463
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".spirit_chrome_data"
)

_SUB_KEY = "3b6a6994753b4efc86376552e52b8432"
_TOKEN_URL = "/api/prod-token/api/v1/token"
_SEARCH_URL = "/api/prod-availability/api/availability/v3/search"

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_homepage_warmed = False


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (real Chrome via CDP)."""
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            _pw_instance = pw
            logger.info("NK: connected to existing Chrome on port %d", _CDP_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

            chrome = find_chrome()
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            args = [
                chrome,
                f"--remote-debugging-port={_CDP_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
<<<<<<< Updated upstream
=======
                *proxy_chrome_args(),
>>>>>>> Stashed changes
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
                "--lang=en-US",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.5)

            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            logger.info(
                "NK: Chrome launched on CDP port %d (pid %d)",
                _CDP_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    """Wipe Chrome profile when session is corrupted."""
    global _browser, _context, _pw_instance, _chrome_proc, _homepage_warmed
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    _homepage_warmed = False
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("NK: deleted stale Chrome profile")
        except Exception:
            pass


# ── Helpers ──────────────────────────────────────────────────────────────

def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


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
            return datetime.strptime(s[: len(fmt) + 2], fmt)
        except (ValueError, IndexError):
            continue
    return datetime(2000, 1, 1)


class SpiritConnectorClient:
    """Spirit CDP Chrome connector — Navitaire dotRezWeb availability API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        global _homepage_warmed
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        try:
            # Warm Akamai/PX cookies by visiting homepage
            if not _homepage_warmed:
                logger.info("NK: warming Akamai cookies via homepage")
                try:
                    await page.goto(
                        "https://www.spirit.com/",
                        wait_until="domcontentloaded",
                        timeout=25000,
                    )
                except Exception as e:
                    logger.debug("NK: homepage nav: %s", e)
                await asyncio.sleep(8)
                _homepage_warmed = True
            else:
                try:
                    await page.goto(
                        "https://www.spirit.com/",
                        wait_until="domcontentloaded",
                        timeout=20000,
                    )
                except Exception as e:
                    logger.debug("NK: homepage nav: %s", e)
                await asyncio.sleep(3)

            # Step 1: Get Navitaire session token
            token_result = await page.evaluate(
                """async (subKey) => {
                    try {
                        const resp = await fetch('/api/prod-token/api/v1/token', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Ocp-Apim-Subscription-Key': subKey,
                                'Cache-Control': 'no-cache'
                            },
                            credentials: 'include',
                            body: JSON.stringify({"applicationName": "dotRezWeb"})
                        });
                        const text = await resp.text();
                        return {status: resp.status, body: text};
                    } catch(e) {
                        return {error: e.message};
                    }
                }""",
                _SUB_KEY,
            )

            if token_result.get("error"):
                logger.error("NK: token fetch error: %s", token_result["error"])
                return self._empty(req)

            if token_result["status"] not in (200, 201):
                logger.warning("NK: token returned %d, resetting profile", token_result["status"])
                await _reset_profile()
                return self._empty(req)

            token_data = json.loads(token_result["body"])
            bearer = (
                token_data.get("data", {}).get("token", "")
                or token_data.get("token", "")
            )
            if not bearer:
                logger.error("NK: no token in response: %s", token_result["body"][:200])
                return self._empty(req)
            logger.info("NK: got session token (%d chars)", len(bearer))

            # Step 2: Search flights
            dt = _to_datetime(req.date_from)
            date_str = dt.strftime("%Y-%m-%d")
            adults = req.adults or 1
            children = req.children or 0
            infants = req.infants or 0

            pax_types = [{"type": "ADT", "count": adults}]
            if children:
                pax_types.append({"type": "CHD", "count": children})

            search_payload = {
                "criteria": [{
                    "stations": {
                        "originStationCodes": [req.origin],
                        "destinationStationCodes": [req.destination],
                    },
                    "dates": {"beginDate": date_str, "endDate": date_str},
                    "filters": {"filter": "Default"},
                }],
                "passengers": {"types": pax_types},
                "codes": {"currency": "USD", "promotionCode": ""},
                "fareFilters": {
                    "loyalty": "MonetaryOnly",
                    "types": [],
                    "classControl": 1,
                },
                "taxesAndFees": "TaxesAndFees",
                "infantCount": infants,
                "includeWifiAvailability": True,
                "includeBundleAvailability": True,
                "originalJourneyKeys": [],
                "originalBookingRecordLocator": None,
                "birthDates": [],
            }

            api_result = await page.evaluate(
                """async (args) => {
                    try {
                        const resp = await fetch(args.url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'Accept-Language': 'en-US',
                                'Authorization': 'Bearer ' + args.token,
                                'Ocp-Apim-Subscription-Key': args.subKey,
                                'Cache-Control': 'no-cache'
                            },
                            credentials: 'include',
                            body: args.body
                        });
                        const text = await resp.text();
                        return {status: resp.status, body: text};
                    } catch(e) {
                        return {error: e.message};
                    }
                }""",
                {
                    "url": _SEARCH_URL,
                    "token": bearer,
                    "subKey": _SUB_KEY,
                    "body": json.dumps(search_payload),
                },
            )

            if api_result.get("error"):
                logger.error("NK: search fetch error: %s", api_result["error"])
                return self._empty(req)

            status = api_result.get("status", 0)
            body_text = api_result.get("body", "")

            if status == 403 or status == 429:
                logger.warning("NK: blocked (%d), resetting profile", status)
                await _reset_profile()
                return self._empty(req)

            if status != 200:
                logger.warning("NK: search returned %d: %s", status, body_text[:300])
                _homepage_warmed = False
                return self._empty(req)

            data = json.loads(body_text)
            offers = self._parse_response(data, req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info("NK %s->%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            h = hashlib.md5(f"spirit{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("NK CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Navitaire availability/v3/search response.

        Structure: data.trips[].journeysAvailable[].fares{<key>: {details: {passengerFares: [{fareAmount}]}}}
        """
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = []
        if isinstance(data, dict):
            d = data.get("data", data)
            trips = d.get("trips", []) if isinstance(d, dict) else []
        if not isinstance(trips, list):
            trips = []

        for trip in trips:
            if not isinstance(trip, dict):
                continue
            journeys = trip.get("journeysAvailable", [])
            if not isinstance(journeys, list):
                continue
            for journey in journeys:
                if not isinstance(journey, dict) or not journey.get("isSelectable", True):
                    continue
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)
        return offers

    def _parse_journey(self, journey: dict, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        """Parse a single journey (one itinerary option) into a FlightOffer."""
        fares = journey.get("fares", {})
        if not isinstance(fares, dict) or not fares:
            return None

        # Find cheapest fare
        best_price = float("inf")
        for fare_val in fares.values():
            det = fare_val.get("details", {}) if isinstance(fare_val, dict) else {}
            for pf in det.get("passengerFares", []):
                amt = pf.get("fareAmount")
                if isinstance(amt, (int, float)) and 0 < amt < best_price:
                    best_price = amt
        if best_price == float("inf"):
            return None

        # Build segments from journey.segments
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []
        for seg in (segments_raw if isinstance(segments_raw, list) else []):
            des = seg.get("designator", {})
            ident = seg.get("identifier", {})
            carrier = ident.get("carrierCode", "NK")
            flight_num = ident.get("identifier", "")
            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Spirit Airlines" if carrier == "NK" else carrier,
                flight_no=f"{carrier}{flight_num}",
                origin=des.get("origin", req.origin),
                destination=des.get("destination", req.destination),
                departure=_parse_dt(des.get("departure", "")),
                arrival=_parse_dt(des.get("arrival", "")),
                cabin_class="M",
            ))

        if not segments:
            des = journey.get("designator", {})
            segments.append(FlightSegment(
                airline="NK", airline_name="Spirit Airlines", flight_no="",
                origin=des.get("origin", req.origin),
                destination=des.get("destination", req.destination),
                departure=_parse_dt(des.get("departure", "")),
                arrival=_parse_dt(des.get("arrival", "")),
                cabin_class="M",
            ))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        jk = journey.get("journeyKey", f"{time.monotonic()}")
        return FlightOffer(
            id=f"nk_{hashlib.md5(str(jk).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency="USD",
            price_formatted=f"${best_price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Spirit"],
            owner_airline="NK",
            booking_url=booking_url,
            is_locked=False,
            source="spirit_direct",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = _to_datetime(req.date_from).strftime("%Y-%m-%d")
        return (
            f"https://www.spirit.com/book/flights?from={req.origin}"
            f"&to={req.destination}&date={dep}&pax={req.adults or 1}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"spirit{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
