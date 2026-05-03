"""
Tiket.com connector — CDP Chrome + API response interception.

Tiket.com is Indonesia's #2 OTA (after Traveloka), covering all Indonesian
domestic airlines (Lion Air, Garuda, Citilink, Batik Air, Sriwijaya, etc.)
plus regional international routes.

Strategy (CDP Chrome — Cloudflare protection):
1.  Launch real Chrome via CDP (--remote-debugging-port).
2.  Navigate to Tiket.com flight search results page.
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
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, bandwidth_saving_args, disable_background_networking_args, apply_cdp_url_blocking

logger = logging.getLogger(__name__)

_CDP_PORT = 9483
_USER_DATA = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".tiket_chrome_data"
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
            logger.info("TIKT: connected to existing Chrome on port %d", _CDP_PORT)
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
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
                "--lang=en-US",
                *bandwidth_saving_args(),
                *disable_background_networking_args(),
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
            logger.info("TIKT: Chrome launched CDP port %d pid %d", _CDP_PORT, _chrome_proc.pid)

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


class TiketConnectorClient:
    """Tiket.com — Indonesia's #2 OTA, CDP Chrome + API interception."""

    def __init__(self, timeout: float = 55.0):
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
        if ob_result.offers:
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

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        dt = _to_datetime(req.date_from)
        date_str = dt.strftime("%Y-%m-%d")

        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0

        _tk_cabin = {"M": "economy", "W": "premium", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")

        # Tiket.com search URL format
        search_url = (
            f"https://www.tiket.com/flights/search"
            f"?d={req.origin}&a={req.destination}"
            f"&date={date_str}"
            f"&adult={adults}&child={children}&infant={infants}"
            f"&class={_tk_cabin}"
        )

        for attempt in range(2):
            try:
                offers = await self._do_search(search_url, req, dt)
                if offers is not None:
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "TIKT %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"tikt{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=offers[0].currency if offers else "IDR",
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("TIKT attempt %d failed: %s", attempt, e)
                if attempt == 0:
                    await _reset_profile()

        return self._empty(req)

    async def _do_search(
        self, search_url: str, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer] | None:
        context = await _get_context()
        page = await context.new_page()
        await apply_cdp_url_blocking(page)

        captured_data: list[dict] = []

        async def on_response(response):
            url = response.url
            if "tix-flight-search" not in url:
                return
            try:
                ct = response.headers.get("content-type", "")
                if "json" in ct and response.status == 200:
                    body = await response.text()
                    data = json.loads(body)
                    sl = (data.get("data") or {}).get("searchList") or {}
                    flights = sl.get("departureFlights") or []
                    if flights:
                        captured_data.append(data)
                        logger.debug("TIKT: captured %d flights (%d bytes)", len(flights), len(body))
            except Exception:
                pass

        page.on("response", on_response)

        try:
            logger.info("TIKT: navigating to %s", search_url)
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for results
            deadline = time.monotonic() + 35
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
                logger.warning("TIKT: no API responses intercepted, trying DOM")
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

        # Price scale: multiCurrency.scale (default 2 → divide by 100)
        mc = inner.get("multiCurrency") or {}
        scale = mc.get("scale", 0)
        divisor = 10 ** scale if scale > 0 else 1
        currency = mc.get("currency") or "USD"

        # Airlines map
        airlines_map = inner.get("airlines") or {}

        search_list = inner.get("searchList") or {}
        flights = search_list.get("departureFlights") or []

        for f in flights:
            try:
                offer = self._parse_flight(f, airlines_map, currency, divisor, req, dt, seen)
                if offer:
                    offers.append(offer)
            except Exception as e:
                logger.debug("TIKT: parse flight error: %s", e)

        return offers

    def _parse_flight(
        self, f: dict, airlines_map: dict, currency: str, divisor: float,
        req: FlightSearchRequest, dt: datetime, seen: set,
    ) -> FlightOffer | None:
        fare_detail = f.get("fareDetail") or {}
        raw_price = fare_detail.get("cheapestFare") or 0
        if not raw_price:
            return None

        price_f = round(float(raw_price) / divisor, 2) if divisor > 1 else round(float(raw_price), 2)
        if price_f <= 0:
            return None

        dep_code = f.get("departureAirportCode") or req.origin
        arr_code = f.get("arrivalAirportCode") or req.destination
        airline_code = f.get("marketingAirlineCode") or ""

        # Parse departure/arrival times
        dep_time_str = f.get("departureTime") or ""
        arr_time_str = f.get("arrivalTime") or ""

        # journeySellKey has full datetimes: GA~652~CGK~08/16/2026 21:25~DPS~08/17/2026 00:20~
        jsk = f.get("journeySellKey") or ""
        dep_dt = dt
        arr_dt = dt
        if jsk:
            parts = jsk.split("~")
            for i, p in enumerate(parts):
                if "/" in p and ":" in p and len(p) > 10:
                    try:
                        parsed = datetime.strptime(p.strip(), "%m/%d/%Y %H:%M")
                        if i < len(parts) // 2:
                            dep_dt = parsed
                        else:
                            arr_dt = parsed
                    except ValueError:
                        pass

        if dep_dt == dt and dep_time_str:
            try:
                h, m = map(int, dep_time_str.split(":"))
                dep_dt = datetime(dt.year, dt.month, dt.day, h, m)
            except (ValueError, IndexError):
                pass

        if arr_dt == dt and arr_time_str:
            try:
                h, m = map(int, arr_time_str.split(":"))
                arr_dt = datetime(dt.year, dt.month, dt.day, h, m)
                if arr_dt < dep_dt:
                    from datetime import timedelta
                    arr_dt += timedelta(days=1)
            except (ValueError, IndexError):
                pass

        total_dur_min = f.get("totalTravelTimeInMinutes") or 0
        total_dur_s = int(total_dur_min) * 60
        stops = f.get("totalTransit") or 0

        # Flight number(s) from flightSelect: "GA 652" or "ID 6572|IU 706"
        flight_select = f.get("flightSelect") or ""
        fno = flight_select.replace(" ", "").replace("|", "-") if flight_select else ""

        airline_info = airlines_map.get(airline_code) or {}
        airline_name = airline_info.get("name") or airline_code

        seg = FlightSegment(
            airline=airline_code,
            airline_name=airline_name,
            flight_no=fno,
            origin=dep_code,
            destination=arr_code,
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=total_dur_s,
            cabin_class=f.get("cabinClass", "ECONOMY").lower(),
        )

        dedup = f"{dep_code}_{arr_code}_{dt:%Y%m%d}_{price_f}_{fno}"
        if dedup in seen:
            return None
        seen.add(dedup)

        route = FlightRoute(
            segments=[seg],
            total_duration_seconds=total_dur_s,
            stopovers=stops,
        )

        fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
        _tk_cls = {"M": "economy", "W": "premium", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        return FlightOffer(
            id=f"tikt_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=[airline_name] if airline_name else [airline_code],
            owner_airline=airline_code,
            booking_url=(
                f"https://www.tiket.com/flights/search"
                f"?d={req.origin}&a={req.destination}"
                f"&date={dt:%Y-%m-%d}&adult={req.adults or 1}"
                f"&child={req.children or 0}&infant={req.infants or 0}"
                f"&class={_tk_cls}"
            ),
            is_locked=False,
            source="tiket_ota",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # DOM fallback
    # ------------------------------------------------------------------

    async def _extract_from_dom(
        self, page, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Fallback: scrape flight cards from Tiket.com DOM."""
        try:
            data = await page.evaluate("""() => {
                const cards = document.querySelectorAll(
                    '[class*="flight-card"], [class*="FlightCard"], '
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
                    if (p) out.push({
                        price: p.textContent.trim(),
                        airline: a ? a.textContent.trim() : '',
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
                    id=f"tikt_{fid}",
                    price=price_f,
                    currency="IDR",
                    price_formatted=f"{price_f:.2f} IDR",
                    outbound=route,
                    inbound=None,
                    airlines=[airline],
                    owner_airline="",
                    booking_url=(
                        f"https://www.tiket.com/pesawat/cari"
                        f"?d={req.origin}&a={req.destination}"
                        f"&date={dt:%Y-%m-%d}"
                    ),
                    is_locked=False,
                    source="tiket_ota",
                    source_tier="free",
                ))
            return offers
        except Exception as e:
            logger.debug("TIKT: DOM extraction failed: %s", e)
            return []

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
                    id=f"rt_tikt_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        """Fetch live bag/seat pricing for Tiket.com (OTA). Returns None - ancillary_ref.py fallback."""
        return {
            "checked_bag_note": "baggage depends on airline and fare – may be included or add-on; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
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
            if bags_from is not None:
                offer.bags_price.setdefault("carry_on", bags_from)
            if checked_from is not None:
                offer.bags_price.setdefault("checked_bag", checked_from)
            if seat_from is not None:
                offer.bags_price.setdefault("seat", seat_from)

    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"tikt{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="IDR",
            offers=[],
            total_results=0,
        )
