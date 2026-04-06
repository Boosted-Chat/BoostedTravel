"""
Musafir connector — Playwright browser + route interception + API capture.

Musafir (musafir.com) is a UAE/India-focused OTA.  The site is an ASP.NET
WebForms + jQuery + Backbone.js SPA with a custom resolution (autocomplete)
framework that calls ``app.musafir.com`` for airport lookups via cross-origin
AJAX.  The resolution AJAX is CORS-sensitive, so we intercept the requests
via ``page.route()`` and return pre-fetched XML from the server side.

The search triggers two API endpoints on ``apiae.musafir.com``:
  - ``POST /flight/init``    — initialises the search session
  - ``POST /flight/results`` — polled until ``IsComplete=true``

Flight segment data is encoded in the ``ItemIdentifier`` string:
  ``ProviderProfileId$FlightNo_Orig-Dest@DDMMHHmm[#FlightNo_Orig-Dest@DDMMHHmm](FareClass)``

Strategy:
1. Pre-fetch airport resolution XML via ``httpx`` (server-side, no CORS).
2. Launch Playwright, intercept ``**/Resolve/Default.ashx**`` and return
   the pre-fetched XML so the form-fill works.
3. Navigate to ``/Flights/Default.aspx``, fill origin/dest/date via the
   actual form elements, click SEARCH.
4. The SPA redirects to ``app.musafir.com/app/#/flight?...`` which fires
   ``/flight/init`` + ``/flight/results`` on ``apiae.musafir.com``.
5. Capture JSON responses, parse ``AirItineraries``, build offers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime
from typing import Any

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_proxy

logger = logging.getLogger(__name__)

_RESOLVE_URL = (
    "https://app.musafir.com/Trip/Resource/Pages/Resolve/Default.ashx"
)

# ── ItemIdentifier parser ──
# Format: "228$IX252_SHJ-BOM@15040855(EC)"
# Multi-seg: "1$EY5417_XNB-AUH@15041020#EY204_AUH-BOM@15041415(YBASIC)"
_SEG_RE = re.compile(
    r"([A-Z0-9*]+?)(\d+)"       # carrier + flight digits
    r"_([A-Z]{3})-([A-Z]{3})"   # origin-dest
    r"@(\d{4})(\d{4})"          # DDMM HHMM
)


def _parse_item_id(item_id: str, search_date: datetime) -> list[dict]:
    """Parse segments from ItemIdentifier string."""
    # Strip provider prefix "228$..." and fare suffix "(EC)"
    core = item_id
    if "$" in core:
        core = core.split("$", 1)[1]
    if "(" in core:
        core = core.rsplit("(", 1)[0]

    parts = core.split("#")
    segments = []
    for part in parts:
        m = _SEG_RE.search(part)
        if not m:
            continue
        carrier_raw, fnum, orig, dest, ddmm, hhmm = m.groups()
        carrier = carrier_raw.replace("*", "")  # codeshare "EY1014*QP"
        flight_no = f"{carrier}{fnum}"
        day = int(ddmm[:2])
        month = int(ddmm[2:4])
        hour = int(hhmm[:2])
        minute = int(hhmm[2:4])
        year = search_date.year
        # Handle month rollover (search in Dec, flight in Jan)
        if month < search_date.month:
            year += 1
        try:
            dep_dt = datetime(year, month, day, hour, minute)
        except ValueError:
            dep_dt = datetime(2000, 1, 1)
        segments.append({
            "carrier": carrier,
            "flight_no": flight_no,
            "origin": orig,
            "destination": dest,
            "departure": dep_dt,
        })

    # Estimate arrival times from next segment departure or duration
    for i, seg in enumerate(segments):
        if i + 1 < len(segments):
            seg["arrival"] = segments[i + 1]["departure"]
        else:
            # Last segment: estimate 3h for short-haul, no better info
            seg["arrival"] = seg["departure"]

    return segments


def _build_route_from_item(
    item_id: str, search_date: datetime
) -> FlightRoute | None:
    segs = _parse_item_id(item_id, search_date)
    if not segs:
        return None
    flight_segments = []
    for s in segs:
        dep = s["departure"]
        arr = s["arrival"]
        dur = max(0, int((arr - dep).total_seconds())) if dep != arr else 0
        flight_segments.append(FlightSegment(
            airline=s["carrier"],
            flight_no=s["flight_no"],
            origin=s["origin"],
            destination=s["destination"],
            departure=dep,
            arrival=arr,
            duration_seconds=dur,
        ))
    if not flight_segments:
        return None
    total_dur = 0
    if len(flight_segments) > 1:
        total_dur = max(
            0,
            int(
                (flight_segments[-1].arrival - flight_segments[0].departure)
                .total_seconds()
            ),
        )
    elif flight_segments[0].duration_seconds:
        total_dur = flight_segments[0].duration_seconds
    return FlightRoute(
        segments=flight_segments,
        total_duration_seconds=total_dur,
        stopovers=max(0, len(flight_segments) - 1),
    )


def _booking_url(origin: str, dest: str, date: datetime, adults: int) -> str:
    dd = date.strftime("%d")
    mm = date.strftime("%m")
    yy = date.strftime("%y")
    return (
        f"https://app.musafir.com/app/#/flight"
        f"?p=1&f=0&o={origin}&d={dest}&sd={dd}/{mm}/{yy}&ad={adults}"
    )


def _extract_offers(
    itineraries: list[dict],
    currency: str,
    search_date: datetime,
    req: FlightSearchRequest,
) -> list[FlightOffer]:
    offers: list[FlightOffer] = []
    book_url = _booking_url(
        req.origin, req.destination, search_date, req.adults or 1
    )

    for i, itin in enumerate(itineraries):
        try:
            price_info = itin.get("PriceInformation", {})
            price = float(price_info.get("TotalPrice", 0))
            if price <= 0:
                continue

            item_id = itin.get("ItemIdentifier", "")
            if not item_id:
                continue

            airline_code = itin.get("ValidatingAirline", "")
            cabin = itin.get("CabinClass", "Economy class")

            outbound = _build_route_from_item(item_id, search_date)
            if not outbound:
                continue

            airlines = list(
                {s.airline for s in outbound.segments if s.airline}
            )
            if not airlines:
                airlines = [airline_code] if airline_code else ["Musafir"]

            h = hashlib.md5(
                f"mus{item_id}{price}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"off_mus_{h}",
                source="musafir_ota",
                price=price,
                currency=currency,
                airlines=airlines,
                owner_airline=airline_code or airlines[0],
                outbound=outbound,
                inbound=None,
                booking_url=book_url,
            ))
        except Exception as e:
            logger.debug("Musafir parse itin %d: %s", i, e)

    return offers


async def _fetch_resolution_xml(keyword: str) -> str:
    """Fetch airport resolution XML from app.musafir.com (server-side)."""
    try:
        import httpx

        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                _RESOLVE_URL,
                params={"mode": "1", "keyword": keyword, "m": "1"},
            )
            if resp.status_code == 200:
                return resp.text
    except Exception as e:
        logger.debug("Musafir resolution fetch for %s: %s", keyword, e)
    return ""


class MusafirConnectorClient:
    """Musafir — UAE/India OTA, Playwright + route interception."""

    def __init__(self, timeout: float = 70.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(2):
            try:
                offers = await self._do_search(req)
                if offers is not None:
                    offers.sort(
                        key=lambda o: o.price if o.price > 0 else float("inf")
                    )
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "MUSAFIR %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"mus{req.origin}{req.destination}{req.date_from}"
                        .encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_mus_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("MUSAFIR attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright

        # Pre-fetch resolution XML for origin & destination (server-side)
        origin_xml, dest_xml = await asyncio.gather(
            _fetch_resolution_xml(req.origin),
            _fetch_resolution_xml(req.destination),
        )
        if not origin_xml or not dest_xml:
            logger.warning(
                "MUSAFIR: resolution XML unavailable (origin=%d dest=%d)",
                len(origin_xml), len(dest_xml),
            )
            return None

        resolution_cache = {
            req.origin.lower(): origin_xml,
            req.destination.lower(): dest_xml,
        }

        # Capture flight API responses
        itineraries: list[dict] = []
        currency = "AED"
        search_complete = asyncio.Event()

        async def on_response(response):
            nonlocal currency
            url = response.url
            if "apiae.musafir.com" not in url:
                return
            try:
                body = await response.text()
                data = json.loads(body)
                entity = data.get("Entity", {})

                if "/flight/init" in url:
                    currency = entity.get("Currency", "AED")

                elif "/flight/results" in url:
                    flights = entity.get("Flights", {})
                    batch = flights.get("AirItineraries", [])
                    if batch:
                        itineraries.extend(batch)
                    if entity.get("IsComplete"):
                        search_complete.set()
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("MUSAFIR_PROXY")
            launch_kw: dict = {
                "headless": False,
                "args": [
                    "--headless=new",
                    "--no-sandbox",
                    "--disable-http2",
                    "--window-position=-2400,-2400",
                    "--window-size=1366,768",
                    "--disable-blink-features=AutomationControlled",
                ],
            }
            if proxy:
                launch_kw["proxy"] = proxy
            browser = await pw.chromium.launch(**launch_kw)
            ctx = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            if proxy:
                from .browser import block_heavy_resources

                await block_heavy_resources(page)
            page.on("response", on_response)

            # Intercept resolution AJAX and return pre-fetched XML
            async def _handle_resolve(route):
                url = route.request.url
                keyword = ""
                if "keyword=" in url:
                    keyword = (
                        url.split("keyword=")[1].split("&")[0].lower()
                    )
                xml = resolution_cache.get(keyword, "")
                if xml:
                    await route.fulfill(
                        status=200,
                        content_type="text/xml; charset=utf-8",
                        body=xml,
                    )
                else:
                    await route.continue_()

            await page.route("**/Resolve/Default.ashx**", _handle_resolve)

            await page.goto(
                "https://www.musafir.com/Flights/Default.aspx",
                wait_until="networkidle",
                timeout=30000,
            )
            await page.wait_for_timeout(2000)

            # Set one-way
            if not req.return_from:
                try:
                    await page.click("label[for='trip_one']", timeout=3000)
                    await page.wait_for_timeout(300)
                except Exception:
                    pass

            # Fill origin
            await self._fill_airport(page, "Origin", req.origin)

            # Fill destination
            await self._fill_airport(page, "Destination", req.destination)

            # Fill date
            await self._fill_date(page, req.date_from)

            # Click search
            try:
                await page.locator(
                    "a.submit.button.yellow.flightsOnly_all"
                ).click(timeout=5000)
            except Exception:
                await page.keyboard.press("Enter")

            # Wait for search to complete
            try:
                await asyncio.wait_for(
                    search_complete.wait(), timeout=self.timeout
                )
            except asyncio.TimeoutError:
                logger.debug("MUSAFIR: timeout waiting for IsComplete")

            # Small extra wait to capture final poll
            await page.wait_for_timeout(2000)

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("MUSAFIR browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not itineraries:
            logger.warning("MUSAFIR: no itineraries captured")
            return None

        search_date = (
            req.date_from
            if isinstance(req.date_from, datetime)
            else datetime.combine(req.date_from, datetime.min.time())
        )

        all_offers = _extract_offers(
            itineraries, currency, search_date, req
        )

        # Deduplicate by item identifier
        seen: set[str] = set()
        unique: list[FlightOffer] = []
        for o in all_offers:
            key = f"{o.price}_{o.outbound.segments[0].flight_no if o.outbound and o.outbound.segments else ''}"
            if key not in seen:
                seen.add(key)
                unique.append(o)

        return unique

    async def _fill_airport(self, page, field_name: str, iata: str):
        """Fill an airport autocomplete field using route interception."""
        inp = page.locator(f"input[name='{field_name}']")
        await inp.click()
        await page.wait_for_timeout(200)
        await inp.fill("")
        for c in iata:
            await page.keyboard.type(c, delay=80)
            await page.wait_for_timeout(150)
        await page.wait_for_timeout(1500)

        # Click the matching airport in #resolutionControl
        rc = page.locator("#resolutionControl li.airport")
        count = await rc.count()
        if count > 0:
            # Try to match by IATA code text
            for j in range(count):
                text = await rc.nth(j).text_content() or ""
                if iata.upper() in text.upper():
                    await rc.nth(j).click()
                    await page.wait_for_timeout(300)
                    return
            # Fallback: click first item
            await rc.first.click()
            await page.wait_for_timeout(300)
        else:
            logger.debug(
                "MUSAFIR: no resolution items for %s (%s)", field_name, iata
            )

    async def _fill_date(self, page, date):
        """Navigate the Musafir custom calendar and select the date."""
        try:
            await page.locator("input[name='StartDate']").click()
            await page.wait_for_timeout(800)

            target = date.strftime("%B %Y").lower()  # "april 2026"
            date_str = date.strftime("%Y-%m-%d")      # "2026-04-15"

            for _ in range(18):
                cal_text = await page.evaluate("""() => {
                    const c = document.getElementById('calendar');
                    if (!c) return '';
                    for (const el of c.querySelectorAll('h3, h4')) {
                        const t = el.textContent?.trim();
                        if (t && t.length > 3) return t;
                    }
                    return '';
                }""")
                if cal_text.lower().startswith(target[:3]) and str(date.year) in cal_text:
                    break
                try:
                    await page.locator("#calendar a.forward").click(
                        timeout=2000
                    )
                    await page.wait_for_timeout(300)
                except Exception:
                    break

            await page.locator(
                f'#calendar li[date="{date_str}"]'
            ).click(timeout=3000)
            await page.wait_for_timeout(400)
        except Exception as e:
            logger.debug("MUSAFIR date fill: %s", e)

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
                    id=f"rt_mus_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
