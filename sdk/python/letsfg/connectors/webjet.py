"""
Webjet connector — CDP Chrome + API response interception.

Webjet is Australia's #1 OTA, covering all AU/NZ domestic and international
routes. Aggregates fares from Qantas, Virgin Australia, Jetstar, Rex, Air NZ,
Fiji Airways, LATAM, Singapore Airlines, etc.

Strategy (CDP Chrome):
1.  Launch real Chrome via CDP (--remote-debugging-port).
2.  Navigate to Webjet search results page.
3.  Intercept XHR responses for flight search API calls.
4.  Parse into FlightOffers.
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
from datetime import datetime, date as date_type
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args

logger = logging.getLogger(__name__)

_CDP_PORT = 9482
_USER_DATA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".webjet_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


async def _get_context():
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
            logger.info("WBJT: connected to existing Chrome on port %d", _CDP_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

            chrome = find_chrome()
            os.makedirs(_USER_DATA, exist_ok=True)
            args = [
                chrome,
                f"--remote-debugging-port={_CDP_PORT}",
                f"--user-data-dir={_USER_DATA}",
                "--no-first-run",
                *proxy_chrome_args(),
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
            logger.info("WBJT: Chrome launched CDP port %d pid %d", _CDP_PORT, _chrome_proc.pid)

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    global _browser, _context, _pw_instance, _chrome_proc
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
    _browser = _context = _pw_instance = _chrome_proc = None
    if os.path.isdir(_USER_DATA):
        try:
            shutil.rmtree(_USER_DATA)
        except Exception:
            pass


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
            return datetime.strptime(s[:len(fmt) + 2], fmt)
        except (ValueError, IndexError):
            continue
    return datetime(2000, 1, 1)


class WebjetConnectorClient:
    """Webjet — Australia's #1 OTA, CDP Chrome + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        dt = _to_datetime(req.date_from)
        date_compact = dt.strftime("%Y%m%d")

        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0

        origin = req.origin.upper()
        dest = req.destination.upper()

        trip_type = "Roundtrip" if req.return_from else "Oneway"
        leg_param = f"&OneWay={origin}-{origin}-{dest}ALL-{dest}ALL-{date_compact}"
        if req.return_from:
            ret_dt = _to_datetime(req.return_from)
            ret_compact = ret_dt.strftime("%Y%m%d")
            leg_param = (
                f"&Outbound={origin}-{origin}-{dest}ALL-{dest}ALL-{date_compact}"
                f"&Inbound={dest}-{dest}-{origin}ALL-{origin}ALL-{ret_compact}"
            )
        search_url = (
            f"https://services.webjet.com.au/web/flights/matrix/"
            f"?Adults={adults}&Children={children}&Infants={infants}"
            f"&TravelClass=Economy&TripType={trip_type}"
            f"{leg_param}"
            f"&CityCodeFrom={origin}&CityCodeTo={dest}"
        )

        for attempt in range(2):
            try:
                offers = await self._do_search(search_url, req, dt)
                if offers is not None:
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "WBJT %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"wbjt{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=offers[0].currency if offers else "AUD",
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("WBJT attempt %d failed: %s", attempt, e)
                if attempt == 0:
                    await _reset_profile()

        return self._empty(req)

    async def _do_search(
        self, search_url: str, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer] | None:
        context = await _get_context()
        page = await context.new_page()

        captured_data: list[dict] = []

        async def on_response(response):
            url = response.url
            if "flightssearchservice/matrix" not in url:
                return
            try:
                ct = response.headers.get("content-type", "")
                if "json" in ct and response.status == 200:
                    body = await response.text()
                    data = json.loads(body)
                    if isinstance(data, dict) and data.get("data") and "outbound" in data["data"]:
                        captured_data.append(data)
                        logger.debug("WBJT: captured matrix (%d bytes)", len(body))
            except Exception:
                pass

        page.on("response", on_response)

        try:
            logger.info("WBJT: navigating to %s", search_url)
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for results to load
            deadline = time.monotonic() + 40
            last_count = 0
            stable_ticks = 0
            while time.monotonic() < deadline:
                await asyncio.sleep(3)
                if len(captured_data) > last_count:
                    last_count = len(captured_data)
                    stable_ticks = 0
                else:
                    stable_ticks += 1
                    if stable_ticks >= 3 and captured_data:
                        break

            if not captured_data:
                logger.warning("WBJT: no API responses intercepted, trying DOM")
                return await self._extract_from_dom(page, req, dt)

            offers: list[FlightOffer] = []
            seen: set[str] = set()
            for data in captured_data:
                parsed = self._parse_response(data, req, dt, seen)
                offers.extend(parsed)

            return offers

        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(
        self, data: dict, req: FlightSearchRequest, dt: datetime, seen: set,
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        inner = data.get("data") or {}
        airlines_map = inner.get("airlines") or {}
        outbound = inner.get("outbound") or {}
        ob_groups = outbound.get("flightGroups") or []

        # Parse inbound flight groups for RT
        is_rt = bool(req.return_from)
        ib_route: FlightRoute | None = None
        ib_price = 0.0
        if is_rt:
            inbound = inner.get("return") or inner.get("inbound") or {}
            ib_groups = inbound.get("flightGroups") or []
            if ib_groups:
                best_ib_price = float("inf")
                for ib_fg in ib_groups:
                    try:
                        ib_offer = self._parse_flight_group(
                            ib_fg, airlines_map, req, dt, set(), direction="inbound",
                        )
                        if ib_offer and ib_offer.price < best_ib_price:
                            best_ib_price = ib_offer.price
                            ib_route = ib_offer.outbound  # parsed as outbound, reuse as ib_route
                            ib_price = ib_offer.price
                    except Exception:
                        pass

        for fg in ob_groups:
            try:
                offer = self._parse_flight_group(fg, airlines_map, req, dt, seen)
                if offer:
                    if is_rt and ib_route:
                        total_price = round(offer.price + ib_price, 2)
                        offer = FlightOffer(
                            id=f"wbjt_rt_{offer.id[5:]}",
                            price=total_price,
                            currency=offer.currency,
                            price_formatted=f"{total_price:.2f} {offer.currency}",
                            outbound=offer.outbound,
                            inbound=ib_route,
                            airlines=offer.airlines,
                            owner_airline=offer.owner_airline,
                            booking_url=offer.booking_url,
                            is_locked=False,
                            source="webjet_ota",
                            source_tier="free",
                        )
                    offers.append(offer)
            except Exception as e:
                logger.debug("WBJT: parse flight group error: %s", e)

        return offers

    def _parse_flight_group(
        self, fg: dict, airlines_map: dict,
        req: FlightSearchRequest, dt: datetime, seen: set,
        direction: str = "outbound",
    ) -> FlightOffer | None:
        seg_data = fg.get("flights") or []
        if not seg_data:
            return None

        # Find cheapest fare across all segments
        cheapest_price = float("inf")
        cheapest_currency = "AUD"
        for sd in seg_data:
            for fare in sd.get("fares") or []:
                p = fare.get("price", 0)
                if isinstance(p, (int, float)) and 0 < p < cheapest_price:
                    cheapest_price = p

        if cheapest_price == float("inf"):
            return None

        price_f = round(float(cheapest_price), 2)

        segments: list[FlightSegment] = []
        for sd in seg_data:
            carrier = sd.get("carrier") or ""
            op_carrier = sd.get("operatingCarrier") or carrier
            fno_raw = sd.get("flightNumber") or ""
            fno = f"{carrier}{fno_raw}" if carrier and fno_raw and not fno_raw.startswith(carrier) else fno_raw

            dep_code = sd.get("departureCityCode") or req.origin
            arr_code = sd.get("arrivalCityCode") or req.destination
            dep_time = _parse_dt(sd.get("departureTime"))
            arr_time = _parse_dt(sd.get("arrivalTime"))

            dur_str = sd.get("duration") or ""
            dur_s = 0
            if dur_str:
                parts = dur_str.split(":")
                if len(parts) >= 2:
                    try:
                        dur_s = int(parts[0]) * 3600 + int(parts[1]) * 60
                    except (ValueError, IndexError):
                        pass

            airline_info = airlines_map.get(carrier) or {}
            airline_name = airline_info.get("name") or carrier

            segments.append(FlightSegment(
                airline=carrier,
                airline_name=airline_name,
                flight_no=fno,
                origin=dep_code,
                destination=arr_code,
                departure=dep_time,
                arrival=arr_time,
                duration_seconds=dur_s,
                cabin_class="economy",
            ))

        if not segments:
            return None

        total_dur = fg.get("totalDuration") or fg.get("totalFlightDuration") or 0
        total_dur_s = int(float(total_dur) * 60) if total_dur else sum(s.duration_seconds for s in segments)
        stops = fg.get("stops") or max(0, len(segments) - 1)

        fno_key = "_".join(s.flight_no for s in segments)
        dedup = f"{direction}_{req.origin}_{req.destination}_{dt:%Y%m%d}_{price_f}_{fno_key}"
        if dedup in seen:
            return None
        seen.add(dedup)

        airlines_set = list(dict.fromkeys(s.airline for s in segments if s.airline))
        names_set = list(dict.fromkeys(s.airline_name for s in segments if s.airline_name))

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur_s,
            stopovers=stops,
        )

        booking_url = f"https://www.webjet.com.au/flights/{req.origin}/{req.destination}/{dt:%Y%m%d}/"
        if req.return_from:
            ret = _to_datetime(req.return_from)
            booking_url += f"?return={ret:%Y%m%d}"

        fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"wbjt_{fid}",
            price=price_f,
            currency=cheapest_currency,
            price_formatted=f"{price_f:.2f} {cheapest_currency}",
            outbound=route,
            inbound=None,
            airlines=names_set or airlines_set,
            owner_airline=airlines_set[0] if airlines_set else "",
            booking_url=booking_url,
            is_locked=False,
            source="webjet_ota",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # DOM fallback
    # ------------------------------------------------------------------

    async def _extract_from_dom(
        self, page, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Fallback: scrape flight cards from Webjet search results DOM."""
        try:
            data = await page.evaluate("""() => {
                const cards = document.querySelectorAll(
                    '[class*="flight-result"], [class*="FlightResult"], '
                  + '[class*="result-card"], [class*="ResultCard"], '
                  + '[data-testid*="flight"], [data-testid*="result"]'
                );
                const out = [];
                cards.forEach(c => {
                    const p = c.querySelector(
                        '[class*="price"], [class*="Price"], [data-testid*="price"]'
                    );
                    const a = c.querySelector(
                        '[class*="airline"], [class*="Airline"], [class*="carrier"]'
                    );
                    const d = c.querySelector(
                        '[class*="duration"], [class*="Duration"]'
                    );
                    if (p) out.push({
                        price: p.textContent.trim(),
                        airline: a ? a.textContent.trim() : '',
                        duration: d ? d.textContent.trim() : '',
                    });
                });
                return out;
            }""")

            offers: list[FlightOffer] = []
            seen: set[str] = set()
            for item in data or []:
                nums = re.findall(r"[\d]+", item.get("price", "").replace(",", ""))
                if not nums:
                    continue
                try:
                    price_f = round(float(nums[-1]), 2)
                except (ValueError, IndexError):
                    continue
                if price_f <= 0:
                    continue

                airline = item.get("airline") or "Unknown"
                dedup = f"{req.origin}_{req.destination}_{price_f}_{airline}"
                if dedup in seen:
                    continue
                seen.add(dedup)

                seg = FlightSegment(
                    airline=airline, flight_no="",
                    origin=req.origin, destination=req.destination,
                    departure=dt, arrival=dt, duration_seconds=0,
                )
                route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
                fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"wbjt_{fid}",
                    price=price_f,
                    currency="AUD",
                    price_formatted=f"{price_f:.2f} AUD",
                    outbound=route,
                    inbound=None,
                    airlines=[airline],
                    owner_airline="",
                    booking_url=(
                        f"https://www.webjet.com.au/flights/"
                    ),
                    is_locked=False,
                    source="webjet_ota",
                    source_tier="free",
                ))
            return offers
        except Exception as e:
            logger.debug("WBJT: DOM extraction failed: %s", e)
            return []

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"wbjt{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="AUD",
            offers=[],
            total_results=0,
        )
