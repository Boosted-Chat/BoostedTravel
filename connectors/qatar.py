"""
Qatar Airways (QR) CDP Chrome connector — /dapi BFF flight-offers API.

Strategy (CDP Chrome + same-origin fetch):
1.  Launch real Chrome via CDP (--remote-debugging-port).
2.  Navigate to the homepage once to establish Akamai bot cookies.
3.  Call ``POST /dapi/public/bff/web/flight-search/flight-offers``
    from the page context via ``page.evaluate(fetch(…))``.
4.  Parse the JSON response into FlightOffers.

The ``/dapi`` BFF requires:
- Valid Akamai ``_abck`` / ``bm_sz`` cookies (set by visiting the homepage).
- Headers: ``Accept-Language: en``, ``X-AssignedDeviceID``, ``Session-Id``.
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
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9454
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".qr_chrome_data"
)

_FLIGHT_OFFERS_URL = (
    "https://www.qatarairways.com/dapi/public/bff/web/flight-search/flight-offers"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_homepage_warmed = False  # track whether Akamai cookies are established


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (headed — bot protection)."""
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
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("QR: connected to existing Chrome on port %d", _DEBUG_PORT)
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
                f"--remote-debugging-port={_DEBUG_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                *proxy_chrome_args(),
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
                "--window-position=-2400,-2400",
                "--window-size=1400,900",
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
                "QR: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
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
            logger.info("QR: deleted stale Chrome profile")
        except Exception:
            pass


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_datetime(s: str) -> datetime:
    """Parse ISO-ish datetime strings from Qatar's API responses."""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M",
    ):
        try:
            return datetime.strptime(s[: len(fmt) + 3], fmt)
        except (ValueError, IndexError):
            continue
    return datetime.strptime(s[:10], "%Y-%m-%d")


class QatarConnectorClient:
    """Qatar Airways CDP Chrome connector — /dapi BFF flight-offers API."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ------------------------------------------------------------------
    # Main search entry-point
    # ------------------------------------------------------------------

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
        global _homepage_warmed
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        try:
            # Warm Akamai cookies once by visiting the homepage
            if not _homepage_warmed:
                logger.info("QR: warming Akamai cookies via homepage")
                await page.goto(
                    "https://www.qatarairways.com/en/homepage.html",
                    wait_until="domcontentloaded",
                    timeout=25000,
                )
                await asyncio.sleep(3)
                _homepage_warmed = True
            else:
                # Reuse same page context — just go to homepage quickly
                await page.goto(
                    "https://www.qatarairways.com/en/homepage.html",
                    wait_until="domcontentloaded",
                    timeout=20000,
                )
                await asyncio.sleep(2)

            # Call the /dapi flight-offers API from the page context
            dt = _to_datetime(req.date_from)
            date_str = dt.strftime("%Y-%m-%d")
            adults = req.adults or 1
            children = req.children or 0
            infants = req.infants or 0

            passengers = [{"type": "ADT", "count": adults}]
            if children:
                passengers.append({"type": "CHD", "count": children})
            if infants:
                passengers.append({"type": "INF", "count": infants})

            payload = {
                "channel": "WEB_DESKTOP",
                "itineraries": [
                    {
                        "origin": req.origin,
                        "destination": req.destination,
                        "departureDate": date_str,
                        "isRequested": True,
                    }
                ],
                "cabinClass": {"M": "ECONOMY", "W": "ECONOMY", "C": "BUSINESS", "F": "FIRST"}.get(req.cabin_class or "M", "ECONOMY"),
                "ignoreInvalidPromoCode": True,
                "passengers": passengers,
            }
            if req.return_from:
                ret_dt = _to_datetime(req.return_from)
                payload["itineraries"].append({
                    "origin": req.destination,
                    "destination": req.origin,
                    "departureDate": ret_dt.strftime("%Y-%m-%d"),
                    "isRequested": True,
                })

            api_result = await page.evaluate(
                """async ([url, payload]) => {
                    const deviceId = localStorage.getItem('booking-widget.device.id')
                                     || crypto.randomUUID().replace(/-/g, '');
                    const sessionId = crypto.randomUUID().replace(/-/g, '');
                    try {
                        const resp = await fetch(url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'Accept-Language': 'en',
                                'X-AssignedDeviceID': deviceId,
                                'Session-Id': sessionId,
                            },
                            credentials: 'include',
                            body: JSON.stringify(payload),
                        });
                        const text = await resp.text();
                        return {status: resp.status, body: text};
                    } catch(e) {
                        return {error: e.message};
                    }
                }""",
                [_FLIGHT_OFFERS_URL, payload],
            )

            status = api_result.get("status", 0)
            body_text = api_result.get("body", "")

            if api_result.get("error"):
                logger.error("QR: fetch error: %s", api_result["error"])
                return self._empty(req)

            if status == 403 or status == 429:
                logger.warning("QR: blocked (%d), resetting profile", status)
                await _reset_profile()
                return self._empty(req)

            if status != 200:
                logger.warning("QR: API returned %d: %s", status, body_text[:200])
                # Invalidate cookie warmup so next call re-warms
                _homepage_warmed = False
                return self._empty(req)

            data = json.loads(body_text)
            offers = self._parse_offers(data, req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "QR %s->%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"qr{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]

            currency = "QAR"
            if offers:
                currency = offers[0].currency

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("QR CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_offers(
        self, data: dict, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse /dapi/public/bff/web/flight-search/flight-offers response."""
        flight_offers = data.get("flightOffers", [])
        if not isinstance(flight_offers, list):
            return []

        is_rt = bool(req.return_from)
        booking_url = self._user_booking_url(req)

        # Parse all flight offer options
        ob_offers: list[FlightOffer] = []
        ib_routes: list[tuple[FlightRoute, float, str]] = []  # (route, price, currency)

        for fo in flight_offers:
            if not isinstance(fo, dict):
                continue
            segments_raw = fo.get("segments", [])
            fare_offers = fo.get("fareOffers", [])
            if not segments_raw or not fare_offers:
                continue

            # Build segments
            segments: list[FlightSegment] = []
            for seg in segments_raw:
                dep_info = seg.get("departure", {})
                arr_info = seg.get("arrival", {})
                dep_dt = _parse_datetime(dep_info.get("dateTime", ""))
                arr_dt = _parse_datetime(arr_info.get("dateTime", ""))
                origin = dep_info.get("origin", {}).get("iataCode", req.origin)
                dest = arr_info.get("destination", {}).get("iataCode", req.destination)
                flight_no = seg.get("flightNumber", "")
                carrier = seg.get("operatorLogo", {}).get("operatorCode", "QR")
                airline_name = seg.get("operatingAirlineName", "Qatar Airways")
                dur = seg.get("duration", 0)

                _qr_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                segments.append(
                    FlightSegment(
                        airline=carrier,
                        airline_name=airline_name,
                        flight_no=flight_no,
                        origin=origin,
                        destination=dest,
                        departure=dep_dt,
                        arrival=arr_dt,
                        duration_seconds=dur,
                        cabin_class=_qr_cabin,
                    )
                )

            if not segments:
                continue

            total_dur = fo.get("duration", 0) or int(
                (segments[-1].arrival - segments[0].departure).total_seconds()
            )
            stopovers = fo.get("numberOfStops", max(len(segments) - 1, 0))

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=stopovers,
            )

            # Take the cheapest economy fare offer
            best_price = None
            best_currency = "QAR"
            for fare in fare_offers:
                cabin = fare.get("cabinType", "")
                if cabin not in ("ECONOMY", "PREMIUM"):
                    continue
                price_obj = fare.get("price", {})
                total = price_obj.get("total")
                if total and (best_price is None or total < best_price):
                    best_price = total
                    best_currency = price_obj.get("currencyCode", "QAR")

            # If no economy fare, take the overall cheapest
            if best_price is None:
                for fare in fare_offers:
                    price_obj = fare.get("price", {})
                    total = price_obj.get("total")
                    if total and (best_price is None or total < best_price):
                        best_price = total
                        best_currency = price_obj.get("currencyCode", "QAR")

            if not best_price or best_price <= 0:
                continue

            # Determine direction by first segment origin
            first_origin = segments[0].origin
            is_inbound = is_rt and first_origin == req.destination

            if is_inbound:
                ib_routes.append((route, round(best_price, 2), best_currency))
            else:
                offer_key = (
                    f"qr_{req.origin}_{req.destination}"
                    f"_{segments[0].departure.isoformat()}_{best_price}"
                )
                offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]
                all_airlines = list({s.airline for s in segments})

                ob_offers.append(
                    FlightOffer(
                        id=f"qr_{offer_id}",
                        price=round(best_price, 2),
                        currency=best_currency,
                        outbound=route,
                        inbound=None,
                        airlines=[
                            ("Qatar Airways" if a == "QR" else a) for a in all_airlines
                        ],
                        owner_airline="QR",
                        booking_url=booking_url,
                        is_locked=False,
                        source="qatar_direct",
                        source_tier="free",
                    )
                )

        # Build RT offers pairing OB × cheapest IB
        if is_rt and ib_routes and ob_offers:
            ib_routes.sort(key=lambda x: x[1])
            cheapest_ib_route, cheapest_ib_price, _ = ib_routes[0]
            rt_offers: list[FlightOffer] = []
            for ob in ob_offers:
                total = round(ob.price + cheapest_ib_price, 2)
                key = f"{ob.id}_rt_{total}"
                rt_offers.append(FlightOffer(
                    id=f"qr_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                    price=total,
                    currency=ob.currency,
                    outbound=ob.outbound,
                    inbound=cheapest_ib_route,
                    airlines=ob.airlines,
                    owner_airline="QR",
                    booking_url=booking_url,
                    is_locked=False,
                    source="qatar_direct",
                    source_tier="free",
                ))
            # RT + OW for combo engine
            return rt_offers + ob_offers

        return ob_offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _user_booking_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        is_rt = bool(req.return_from)
        url = (
            f"https://www.qatarairways.com/en/booking.html"
            f"?from={req.origin}&to={req.destination}"
            f"&departing={dt.strftime('%Y-%m-%d')}"
            f"&adults={req.adults or 1}"
            f"&tripType={'R' if is_rt else 'O'}&bookingClass=E"
        )
        if is_rt:
            ret_dt = _to_datetime(req.return_from)
            url += f"&returning={ret_dt.strftime('%Y-%m-%d')}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"qr{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
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
                    id=f"rt_qata_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
