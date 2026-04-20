"""
TravelUp connector — UK OTA with consolidator fares (browser DOM scraping).

TravelUp.com is a UK-based OTA that sources fares from multiple consolidators
and GDS backends. Their cheapest-fare calendar API returns stale/indicative
prices that differ significantly from actual checkout prices.

Strategy (browser DOM scraping):
1. Launch patchright browser (handles Cloudflare Turnstile).
2. Navigate to TravelUp search results page (round-trip URL).
3. Wait for .fjs_item flight cards to appear (~5-10 s).
4. Extract price, airline, flight ID, times from DOM.
5. Parse flight ID for segment routing details.
6. Close browser + cleanup.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import (
    acquire_browser_slot,
    release_browser_slot,
    get_default_proxy,
    proxy_is_configured,
    patchright_bandwidth_args,
    apply_cdp_url_blocking,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.travelup.com"
_MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


def _build_travelup_url(
    origin: str, dest: str, dep_date: datetime, ret_date: datetime | None = None,
    adults: int = 1, children: int = 0, infants: int = 0,
) -> str:
    """Build a valid TravelUp search URL with the required SEO slug.

    TravelUp uses path-based URLs that REQUIRE a slug or the page returns 404.
    Format: /en-gb/flight-search/{orig}/{dest}/{depYYMMDD}/{retYYMMDD}/{slug}?params
    """
    dep_short = dep_date.strftime("%y%m%d")
    # TravelUp only supports round-trip URLs — use dep+7d if no return
    if ret_date is None:
        ret_date = dep_date + timedelta(days=7)
    ret_short = ret_date.strftime("%y%m%d")
    month_name = _MONTH_NAMES[dep_date.month - 1]
    slug = f"flying-from-{origin.lower()}-to-{dest.lower()}-in-{month_name}-{dep_date.year}"
    return (
        f"{_BASE}/en-gb/flight-search/{origin.lower()}/{dest.lower()}"
        f"/{dep_short}/{ret_short}/{slug}"
        f"?adults={adults}&children={children}&infants={infants}&class=0"
    )


# ── Segment regex for flight-ID parsing ─────────────────────────────────────
# Flight ID format: {origin3}{airline2}{flightno}{cabin}{dest3}{baggage}-...-
# e.g. lhrhu7964economycsx1-pcs-csxhu7964economyhak1-pcs-
_SEG_RE = re.compile(
    r"([a-z]{3})([a-z0-9]{2})(\d+)(economy|premiumeconomy|business|first)([a-z]{3})"
)

# ── JS executed inside the page to extract flight card data ──────────────────
_JS_EXTRACT = """() => {
    const items = document.querySelectorAll('.fjs_item');
    return [...items].map(item => {
        const box = item.querySelector('.flightBox');
        const priceSpan = item.querySelector('.flightPrice span');
        const flightId = box ? box.getAttribute('data-flight-id') : null;

        const extractJourney = (container) => {
            if (!container) return null;
            const airlineImg = container.querySelector('img[alt]');
            const spans = [...container.querySelectorAll('span')];
            const timeSpans = spans.filter(s => s.className === 'time');
            const destSpans = spans.filter(s => s.className === 'destination');
            const dateSpans = spans.filter(s => s.className === 'date');
            const durationSpan = container.querySelector('.journey_details_mobile');
            const stopsSpan = spans.find(s => /stop|direct/i.test(s.textContent) && !s.className);

            return {
                airline: airlineImg ? airlineImg.alt : null,
                airlineCode: airlineImg ? (airlineImg.src.match(/\\/([A-Z0-9]{2})\\.png/)?.[1] || null) : null,
                depTime: timeSpans[0] ? timeSpans[0].textContent.trim() : null,
                arrTime: timeSpans[1] ? timeSpans[1].textContent.trim() : null,
                depAirport: destSpans[0] ? destSpans[0].textContent.trim() : null,
                arrAirport: destSpans[1] ? destSpans[1].textContent.trim() : null,
                depDate: dateSpans[0] ? dateSpans[0].textContent.trim() : null,
                arrDate: dateSpans[1] ? dateSpans[1].textContent.trim() : null,
                duration: durationSpan ? durationSpan.textContent.trim() : null,
                stops: stopsSpan ? stopsSpan.textContent.trim() : null,
            };
        };

        return {
            price: priceSpan ? priceSpan.textContent.trim() : null,
            flightId: flightId,
            outbound: extractJourney(item.querySelector('.outbound_flight')),
            inbound: extractJourney(item.querySelector('.inbound_flight')),
        };
    });
}"""


class TravelupConnectorClient:
    """TravelUp — UK OTA, browser DOM scraping for real checkout prices."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        dt = req.date_from if isinstance(req.date_from, datetime) else datetime.combine(req.date_from, datetime.min.time())
        ret_dt = None
        if req.return_from:
            ret_dt = req.return_from if isinstance(req.return_from, datetime) else datetime.combine(req.return_from, datetime.min.time())

        url = _build_travelup_url(
            req.origin, req.destination, dt,
            ret_date=ret_dt,
            adults=req.adults or 1,
            children=req.children or 0,
            infants=req.infants or 0,
        )

        for attempt in range(2):
            try:
                offers = await self._do_search(url, req, dt, ret_dt, attempt)
                if offers is not None:
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "TravelUp %s→%s: %d offers in %.1fs (browser)",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"travelup{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
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
                logger.warning("TravelUp attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, url: str, req: FlightSearchRequest, dt: datetime,
        ret_dt: datetime | None, attempt: int,
    ) -> list[FlightOffer] | None:
        from patchright.async_api import async_playwright

        browser = None
        context = None
        pw_instance = None

        try:
            await acquire_browser_slot()

            pw_instance = await async_playwright().start()

            launch_kwargs: dict = {
                "headless": False,
                "args": ["--window-position=-2400,-2400", "--window-size=1366,800",
                         *patchright_bandwidth_args()],
            }
            if proxy_is_configured():
                session_id = f"tup{int(time.time())}{attempt}"
                launch_kwargs["proxy"] = {
                    "server": "http://gate.decodo.com:10001",
                    "username": f"{os.environ.get('DECODO_USER', '')}-session-{session_id}",
                    "password": os.environ.get("DECODO_PASS", ""),
                }
            else:
                proxy = get_default_proxy()
                if proxy:
                    launch_kwargs["proxy"] = proxy

            browser = await pw_instance.chromium.launch(**launch_kwargs)
            context = await browser.new_context(
                viewport={"width": 1366, "height": 800},
                locale="en-GB",
            )
            page = await context.new_page()
            await apply_cdp_url_blocking(page)

            logger.info("TravelUp: navigating to %s", url[:120])
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Handle Cloudflare Turnstile — patchright auto-solves, just wait
            for _ in range(20):
                try:
                    title = (await page.title()).lower()
                except Exception:
                    await asyncio.sleep(1)
                    continue
                if "just a moment" not in title and "checking" not in title:
                    break
                await asyncio.sleep(1)

            # Wait for flight result cards
            try:
                await page.wait_for_selector(".fjs_item", timeout=30000)
            except Exception:
                logger.warning("TravelUp: no .fjs_item elements appeared")
                return None

            await asyncio.sleep(1)  # let more cards render

            raw = await page.evaluate(_JS_EXTRACT)
            if not raw:
                return None

            return self._parse_cards(raw, req, dt, ret_dt)

        finally:
            try:
                if context:
                    await context.close()
            except Exception:
                pass
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass
            try:
                if pw_instance:
                    await pw_instance.stop()
            except Exception:
                pass
            release_browser_slot()

    # ------------------------------------------------------------------
    # Card parsing
    # ------------------------------------------------------------------

    def _parse_cards(
        self, raw: list[dict], req: FlightSearchRequest,
        dt: datetime, ret_dt: datetime | None,
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        seen_ids: set[str] = set()

        for card in raw:
            try:
                price_str = card.get("price", "")
                if not price_str:
                    continue
                price = float(re.sub(r"[^\d.]", "", price_str))
                if price <= 0:
                    continue

                currency = "GBP" if "£" in price_str else "EUR" if "€" in price_str else "USD" if "$" in price_str else "GBP"

                flight_id = card.get("flightId") or ""
                if flight_id in seen_ids:
                    continue
                seen_ids.add(flight_id)

                # Parse segments from flight ID
                all_segs = self._parse_flight_id(flight_id)

                ob_data = card.get("outbound")
                ib_data = card.get("inbound")

                ob_stop_count = self._parse_stops(ob_data.get("stops") if ob_data else None)
                ib_stop_count = self._parse_stops(ib_data.get("stops") if ib_data else None)
                ob_seg_count = ob_stop_count + 1
                ib_seg_count = ib_stop_count + 1

                ob_segs = all_segs[:ob_seg_count]
                ib_segs = all_segs[ob_seg_count: ob_seg_count + ib_seg_count]

                ob_route = self._build_route(ob_data, ob_segs, dt) if ob_data else None
                ib_route = self._build_route(ib_data, ib_segs, ret_dt or dt) if ib_data else None

                if not ob_route:
                    continue

                airlines: list[str] = []
                for jd in (ob_data, ib_data):
                    if jd and jd.get("airlineCode"):
                        code = jd["airlineCode"]
                        if code not in airlines:
                            airlines.append(code)

                airline_names: list[str] = []
                for jd in (ob_data, ib_data):
                    if jd and jd.get("airline"):
                        name = jd["airline"]
                        if name not in airline_names:
                            airline_names.append(name)

                oid = hashlib.md5(f"tvup_{flight_id}_{price}".encode()).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"tvup_{oid}",
                    price=round(price, 2),
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=ob_route,
                    inbound=ib_route if req.return_from else None,
                    airlines=airlines or ["XX"],
                    owner_airline=airlines[0] if airlines else "XX",
                    booking_url=_build_travelup_url(
                        req.origin, req.destination, dt,
                        ret_date=ret_dt,
                        adults=req.adults or 1,
                        children=req.children or 0,
                        infants=req.infants or 0,
                    ),
                    is_locked=False,
                    source="travelup_ota",
                    source_tier="protocol",
                ))
            except Exception as e:
                logger.debug("TravelUp: failed to parse card: %s", e)
                continue

        return offers[:30]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_flight_id(flight_id: str) -> list[dict]:
        if not flight_id:
            return []
        segments: list[dict] = []
        for m in _SEG_RE.finditer(flight_id.lower()):
            segments.append({
                "origin": m.group(1).upper(),
                "airline": m.group(2).upper(),
                "flight_no": m.group(3).lstrip("0") or "0",
                "cabin": m.group(4),
                "destination": m.group(5).upper(),
            })
        return segments

    @staticmethod
    def _parse_stops(stops_text: str | None) -> int:
        if not stops_text:
            return 0
        m = re.search(r"(\d+)\s*stop", stops_text, re.I)
        return int(m.group(1)) if m else 0

    @staticmethod
    def _parse_duration(dur: str | None) -> int:
        if not dur:
            return 0
        m = re.search(r"(\d+)h\s*(\d+)m", dur)
        if m:
            return int(m.group(1)) * 3600 + int(m.group(2)) * 60
        m = re.search(r"(\d+)h", dur)
        return int(m.group(1)) * 3600 if m else 0

    @staticmethod
    def _parse_display_dt(time_str: str | None, date_str: str | None, ref_year: int) -> datetime:
        h, mi = 0, 0
        if time_str:
            clean = re.sub(r"\+\d+$", "", time_str.strip())
            parts = clean.split(":")
            if len(parts) == 2:
                try:
                    h, mi = int(parts[0]), int(parts[1])
                except ValueError:
                    pass
        if date_str:
            try:
                parsed = datetime.strptime(f"{date_str.strip()} {ref_year}", "%d %b %Y")
                return parsed.replace(hour=h, minute=mi)
            except ValueError:
                pass
        return datetime(ref_year, 1, 1, h, mi)

    def _build_route(
        self, journey: dict, seg_defs: list[dict], ref_dt: datetime,
    ) -> FlightRoute | None:
        if not journey:
            return None

        duration_secs = self._parse_duration(journey.get("duration"))
        stops = self._parse_stops(journey.get("stops"))
        year = ref_dt.year

        dep_dt = self._parse_display_dt(journey.get("depTime"), journey.get("depDate"), year)
        arr_dt = self._parse_display_dt(
            re.sub(r"\+\d+$", "", journey.get("arrTime") or ""),
            journey.get("arrDate"),
            year,
        )

        airline_code = journey.get("airlineCode") or "XX"
        airline_name = journey.get("airline") or ""

        flight_segments: list[FlightSegment] = []
        if seg_defs:
            for i, sd in enumerate(seg_defs):
                flight_segments.append(FlightSegment(
                    airline=sd["airline"],
                    airline_name=airline_name if sd["airline"] == airline_code else "",
                    flight_no=f"{sd['airline']}{sd['flight_no']}",
                    origin=sd["origin"],
                    destination=sd["destination"],
                    departure=dep_dt if i == 0 else dep_dt,
                    arrival=arr_dt if i == len(seg_defs) - 1 else dep_dt,
                    duration_seconds=duration_secs // max(len(seg_defs), 1) if duration_secs else 0,
                    cabin_class=sd.get("cabin", "economy"),
                ))
        else:
            flight_segments.append(FlightSegment(
                airline=airline_code,
                airline_name=airline_name,
                flight_no="",
                origin=(journey.get("depAirport") or "").upper(),
                destination=(journey.get("arrAirport") or "").upper(),
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=duration_secs or 0,
            ))

        return FlightRoute(
            segments=flight_segments,
            total_duration_seconds=duration_secs or 0,
            stopovers=stops,
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"travelup{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="GBP", offers=[], total_results=0,
        )
