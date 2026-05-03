"""
Traveloka connector — CDP Chrome + API response interception.

Traveloka is Southeast Asia's #1 OTA, covering 100+ airlines including
Lion Air, Garuda, Citilink, Batik Air, AirAsia, Cebu Pacific, VietJet, etc.
Hub coverage: Indonesia (CGK, DPS, SUB, UPG), Singapore, Thailand, Malaysia,
Vietnam, Philippines, Australia (from Bali/Jakarta).

Strategy (CDP Chrome — AWS WAF protection):
1.  Launch real Chrome via CDP (--remote-debugging-port).
2.  Navigate to Traveloka search results page (fullsearch URL).
3.  Intercept XHR responses for /api/v2/flight/search endpoints.
4.  Parse the Traveloka flight search response into FlightOffers.
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
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_CDP_PORT = 9480
_USER_DATA = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".traveloka_chrome_data"
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
            logger.info("TVLK: connected to existing Chrome on port %d", _CDP_PORT)
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
            logger.info("TVLK: Chrome launched CDP port %d pid %d", _CDP_PORT, _chrome_proc.pid)

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
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s[:len(fmt) + 2], fmt)
        except (ValueError, IndexError):
            continue
    return datetime(2000, 1, 1)


class TravelokaConnectorClient:
    """Traveloka — SE Asia's largest OTA, CDP Chrome + API interception."""

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
        date_str = dt.strftime("%d-%m-%Y")

        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0

        cabin_map = {"M": "ECONOMY", "W": "PREMIUM_ECONOMY",
                     "C": "BUSINESS", "F": "FIRST"}
        cabin = cabin_map.get(req.cabin_class, "ECONOMY") if req.cabin_class else "ECONOMY"

        search_url = (
            f"https://www.traveloka.com/en-id/flight/fullsearch"
            f"?ap={req.origin}.{req.destination}"
            f"&dt={date_str}.NA"
            f"&ps={adults}.{children}.{infants}"
            f"&sc={cabin}"
        )

        for attempt in range(2):
            try:
                offers = await self._do_search(search_url, req, dt)
                if offers is not None:
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "TVLK %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"tvlk{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
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
                logger.warning("TVLK attempt %d failed: %s", attempt, e)
                if attempt == 0:
                    await _reset_profile()

        return self._empty(req)

    async def _do_search(
        self, search_url: str, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer] | None:
        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        captured_data: list[dict] = []

        async def on_response(response):
            url = response.url
            if "traveloka.com/api/v2/flight/search/initial" in url:
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct and response.status == 200:
                        body = await response.text()
                        data = json.loads(body)
                        if "data" in data:
                            captured_data.append(data["data"])
                            logger.debug("TVLK: captured search/initial (%d bytes)", len(body))
                except Exception:
                    pass

        page.on("response", on_response)

        try:
            logger.info("TVLK: navigating to %s", search_url)
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for flight results to load
            deadline = time.monotonic() + 35
            while not captured_data and time.monotonic() < deadline:
                await asyncio.sleep(2)

            # Extra wait for more results
            if captured_data:
                await asyncio.sleep(3)

            if not captured_data:
                logger.warning("TVLK: no API responses intercepted, trying DOM")
                offers = await self._extract_from_dom(page, req, dt)
                return offers

            # Parse all captured API responses
            offers = []
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

    @staticmethod
    def _build_datetime(date_d: dict | None, time_d: dict | None) -> datetime | None:
        """Build datetime from Traveloka's {year,month,day} + {hour,minute} dicts."""
        if not date_d:
            return None
        try:
            y = int(date_d.get("year", 0))
            m = int(date_d.get("month", 1))
            d = int(date_d.get("day", 1))
            h = int((time_d or {}).get("hour", 0))
            mi = int((time_d or {}).get("minute", 0))
            return datetime(y, m, d, h, mi)
        except (ValueError, TypeError):
            return None

    def _parse_response(
        self, data: dict, req: FlightSearchRequest, dt: datetime, seen: set,
    ) -> list[FlightOffer]:
        """Parse Traveloka flight search/initial response.

        Structure:
          data.searchResults[]  — each has fare, flightMetadata, connectingFlightRoutes
          data.airlineDataMap   — airline code → {name, shortName, iataCode}
        """
        offers: list[FlightOffer] = []

        airline_map: dict[str, str] = {}
        for code, info in (data.get("airlineDataMap") or {}).items():
            if isinstance(info, dict):
                airline_map[code] = info.get("name") or info.get("shortName") or code

        flights = data.get("searchResults") or []

        for f in flights:
            try:
                offer = self._parse_flight(f, req, dt, seen, airline_map)
                if offer:
                    offers.append(offer)
            except Exception as e:
                logger.debug("TVLK: parse flight error: %s", e)

        return offers

    def _parse_flight(
        self, f: dict, req: FlightSearchRequest, dt: datetime, seen: set,
        airline_map: dict,
    ) -> FlightOffer | None:
        # Price: fare.display.currencyValue.{amount, currency}
        fare = f.get("fare") or {}
        display = fare.get("display") or fare.get("adult") or {}
        cv = display.get("currencyValue") or {}
        price_raw = cv.get("amount") or "0"
        currency = cv.get("currency") or "IDR"

        try:
            price_f = round(float(price_raw), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        meta = f.get("flightMetadata") or {}
        airline_ids = meta.get("airlineIds") or []
        trip_dur_min = int(meta.get("tripDuration") or 0)
        total_stops = int(meta.get("totalNumStop") or 0)

        # Segments from connectingFlightRoutes[].segments[]
        routes = f.get("connectingFlightRoutes") or []
        segments: list[FlightSegment] = []

        for route in routes:
            for sd in route.get("segments") or []:
                acode = sd.get("airlineCode") or sd.get("operatingAirlineCode") or ""
                aname = airline_map.get(acode, acode)
                fno = sd.get("flightNumber") or ""

                dep_dt = self._build_datetime(sd.get("departureDate"), sd.get("departureTime"))
                arr_dt = self._build_datetime(sd.get("arrivalDate"), sd.get("arrivalTime"))
                seg_dur = int(sd.get("durationMinutes") or 0) * 60

                segments.append(FlightSegment(
                    airline=acode,
                    airline_name=aname,
                    flight_no=fno,
                    origin=sd.get("departureAirport") or req.origin,
                    destination=sd.get("arrivalAirport") or req.destination,
                    departure=dep_dt or dt,
                    arrival=arr_dt or dt,
                    duration_seconds=seg_dur,
                    cabin_class=sd.get("seatClass", "ECONOMY").lower(),
                ))

        if not segments:
            return None

        total_dur = trip_dur_min * 60

        fno_key = "_".join(s.flight_no for s in segments)
        dedup = f"{req.origin}_{req.destination}_{dt:%Y%m%d}_{price_f}_{fno_key}"
        if dedup in seen:
            return None
        seen.add(dedup)

        names_set = list(dict.fromkeys(
            airline_map.get(a, a) for a in airline_ids if a
        ))
        codes_set = list(dict.fromkeys(a for a in airline_ids if a))

        route_obj = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=total_stops,
        )

        fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"tvlk_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route_obj,
            inbound=None,
            airlines=names_set or codes_set,
            owner_airline=codes_set[0] if codes_set else "",
            booking_url=(
                f"https://www.traveloka.com/en-id/flight/fullsearch"
                f"?ap={req.origin}.{req.destination}"
                f"&dt={dt:%d-%m-%Y}.NA"
                f"&ps={req.adults or 1}.{req.children or 0}.{req.infants or 0}"
                f"&sc=ECONOMY"
            ),
            is_locked=False,
            source="traveloka_ota",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # DOM fallback
    # ------------------------------------------------------------------

    async def _extract_from_dom(
        self, page, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Fallback: scrape visible flight cards from the search page DOM."""
        try:
            data = await page.evaluate("""() => {
                const sel = '[data-testid*="flight"], [class*="FlightCard"], '
                          + '[class*="flight-card"], [class*="ResultCard"]';
                const cards = document.querySelectorAll(sel);
                const out = [];
                cards.forEach(c => {
                    const p = c.querySelector(
                        '[class*="price"], [class*="Price"], [data-testid*="price"]'
                    );
                    const a = c.querySelector(
                        '[class*="airline"], [class*="Airline"], [data-testid*="airline"]'
                    );
                    const ts = c.querySelectorAll('[class*="time"], [class*="Time"]');
                    if (p) out.push({
                        price: p.textContent.trim(),
                        airline: a ? a.textContent.trim() : '',
                        times: Array.from(ts).map(e => e.textContent.trim()),
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
                    id=f"tvlk_{fid}",
                    price=price_f,
                    currency="IDR",
                    price_formatted=f"{price_f:.2f} IDR",
                    outbound=route,
                    inbound=None,
                    airlines=[airline],
                    owner_airline="",
                    booking_url=(
                        f"https://www.traveloka.com/en-id/flight/fullsearch"
                        f"?ap={req.origin}.{req.destination}"
                        f"&dt={dt:%d-%m-%Y}.NA"
                        f"&ps={req.adults or 1}.0.0&sc=ECONOMY"
                    ),
                    is_locked=False,
                    source="traveloka_ota",
                    source_tier="free",
                ))
            return offers
        except Exception as e:
            logger.debug("TVLK: DOM extraction failed: %s", e)
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
                    id=f"rt_tvlk_{cid}", price=price, currency=o.currency,
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
        """Fetch live bag/seat pricing for Traveloka (OTA). Returns None - ancillary_ref.py fallback."""
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
            f"tvlk{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="IDR",
            offers=[],
            total_results=0,
        )
