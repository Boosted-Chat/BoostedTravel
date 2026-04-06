"""
Akbar Travels connector — Playwright browser + API response interception.

Akbar Travels (akbartravels.com) is one of India's largest travel agencies.
Uses a BenzyInfotech-powered Angular SPA with a two-phase search API:

1. ``POST b2capit.akbartravels.com/flights/ExpressSearch`` — initiate search
2. ``POST b2capi.akbartravels.com/flights/GetExpSearch``  — poll until Completed

Strategy: navigate directly to the results URL (bypasses Angular form-fill),
intercept ``GetExpSearch`` JSON responses, collect ``Trips[].Journeys[]``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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

_BASE = "https://www.akbartravels.com"


# ── helpers ──

def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except Exception:
        pass
    for fmt in ("%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S", "%d %b %Y %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return datetime(2000, 1, 1)


def _dur_seconds(segments: list[FlightSegment]) -> int:
    if not segments:
        return 0
    dep = segments[0].departure
    arr = segments[-1].arrival
    return max(0, int((arr - dep).total_seconds()))


def _parse_segment(seg: dict) -> FlightSegment | None:
    """Parse a segment from AkbarTravels GetExpSearch response.

    Flat format — each Journey IS the segment:
        From, To, DepartureTime, ArrivalTime
        VAC (airline code), AirlineName, FlightNo
    """
    origin_code = seg.get("From") or ""
    dest_code = seg.get("To") or ""
    if not origin_code or not dest_code:
        return None

    dep_time = seg.get("DepartureTime") or ""
    arr_time = seg.get("ArrivalTime") or ""
    airline_code = seg.get("VAC") or seg.get("MAC") or ""
    airline_name = (seg.get("AirlineName") or "").split("|")[0].strip()
    flight_no_raw = str(seg.get("FlightNo") or "")
    flight_no = f"{airline_code}{flight_no_raw}" if airline_code and flight_no_raw else flight_no_raw

    dep_dt = _parse_dt(dep_time)
    arr_dt = _parse_dt(arr_time)
    dur = max(0, int((arr_dt - dep_dt).total_seconds())) if dep_dt.year > 2000 and arr_dt.year > 2000 else 0

    return FlightSegment(
        airline=airline_name or airline_code,
        flight_no=flight_no,
        origin=str(origin_code)[:3].upper(),
        destination=str(dest_code)[:3].upper(),
        departure=dep_dt,
        arrival=arr_dt,
        duration_seconds=dur,
    )


def _build_route(segs_raw: list) -> FlightRoute | None:
    segments = [_parse_segment(s) for s in segs_raw if isinstance(s, dict)]
    segments = [s for s in segments if s is not None]
    if not segments:
        return None
    return FlightRoute(
        segments=segments,
        total_duration_seconds=_dur_seconds(segments),
        stopovers=max(0, len(segments) - 1),
    )


def _results_url(req: FlightSearchRequest) -> str:
    """Build the direct results page URL that triggers ExpressSearch
    automatically — no form-fill needed."""
    d = req.date_from.strftime("%Y-%m-%d")
    adt = req.adults or 1
    chd = req.children or 0
    inf = req.infants or 0
    # SecType: D=domestic (auto-detected by site), FareType: ON=one-way
    ft = "RT" if req.return_from else "ON"
    cabin = "E"  # economy
    return (
        f"{_BASE}/in/flight/display/"
        f"{req.origin}-{req.destination}/{d}/{adt}_{chd}_{inf}/D/{ft}/{cabin}/false/NA/"
    )


def _booking_url(req: FlightSearchRequest) -> str:
    dep = req.date_from.strftime("%d-%m-%Y")
    trip = "R" if req.return_from else "O"
    url = (
        f"{_BASE}/in/flight/search"
        f"?from={req.origin}&to={req.destination}"
        f"&depart={dep}&adults={req.adults or 1}"
        f"&children={req.children or 0}&infants={req.infants or 0}"
        f"&class=Economy&trip={trip}&lan=en"
    )
    if req.return_from:
        url += f"&return={req.return_from.strftime('%d-%m-%Y')}"
    return url


def _extract_offers(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse offers from a GetExpSearch response.

    Flat format: Trips[].Journey[] — each Journey is a complete offer with
    From, To, DepartureTime, ArrivalTime, VAC, FlightNo, GrossFare, etc.
    """
    offers: list[FlightOffer] = []
    book_url = _booking_url(req)
    default_cur = req.currency or "INR"

    trips = data.get("Trips") or []
    if not isinstance(trips, list):
        return offers

    for trip in trips:
        if not isinstance(trip, dict):
            continue
        # Key is "Journey" (singular), not "Journeys"
        journeys = trip.get("Journey") or trip.get("Journeys") or []
        if not isinstance(journeys, list):
            continue

        for i, j in enumerate(journeys[:200]):
            if not isinstance(j, dict):
                continue
            try:
                # Price: GrossFare is the total ticket price
                price = float(
                    j.get("GrossFare") or j.get("NetFare")
                    or j.get("TotalFare") or 0
                )
                if price <= 0:
                    continue

                # Currency from FareKey ("INR,Q,7110") or default
                fare_key = j.get("FareKey") or ""
                cur = fare_key.split(",")[0] if fare_key and "," in fare_key else default_cur

                # Build the outbound segment from the flat Journey
                seg = _parse_segment(j)
                if not seg:
                    continue

                outbound = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=seg.duration_seconds,
                    stopovers=int(j.get("Stops") or 0),
                )

                airline_code = j.get("VAC") or j.get("MAC") or ""
                airline_name = (j.get("AirlineName") or "").split("|")[0].strip()
                airlines = [airline_name] if airline_name else ([airline_code] if airline_code else ["Unknown"])

                h = hashlib.md5(
                    f"akb{req.origin}{req.destination}{i}{price}{j.get('FlightNo','')}".encode()
                ).hexdigest()[:10]

                offers.append(FlightOffer(
                    id=f"off_akb_{h}",
                    source="akbartravels_ota",
                    price=price,
                    currency=cur,
                    airlines=airlines,
                    owner_airline=airlines[0],
                    outbound=outbound,
                    inbound=None,
                    booking_url=book_url,
                ))
            except Exception as e:
                logger.debug("AkbarTravels parse journey %d: %s", i, e)

    return offers


class AkbartravelsConnectorClient:
    """Akbar Travels — Indian OTA, Playwright + direct results URL + API interception."""

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
                        "AKBARTRAVELS %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"akb{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_akb_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("AKBARTRAVELS attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        completed_event = asyncio.Event()
        api_responses: list[dict] = []

        # ── Response interceptor: collect GetExpSearch results ──
        async def on_response(response):
            url = response.url
            if "GetExpSearch" not in url and "getexpsearch" not in url.lower():
                return
            try:
                if response.status == 200:
                    body = await response.text()
                    if len(body) > 500:
                        data = json.loads(body)
                        if isinstance(data, dict) and data.get("Trips"):
                            api_responses.append(data)
                            if data.get("Completed"):
                                completed_event.set()
            except Exception:
                pass

        # ── Request interceptor: modify ExpressSearch body with our route ──
        dep_str = req.date_from.strftime("%Y-%m-%d")
        ret_str = req.return_from.strftime("%Y-%m-%d") if req.return_from else ""
        fare_type = "RT" if req.return_from else "ON"
        custom_body = json.dumps({
            "ADT": req.adults or 1,
            "CHD": req.children or 0,
            "INF": req.infants or 0,
            "Cabin": "E",
            "Source": "CF",
            "Mode": "AS",
            "ClientID": "",
            "TUI": "",
            "FareType": fare_type,
            "SecType": "D",
            "Trips": [{
                "From": req.origin,
                "To": req.destination,
                "ReturnDate": ret_str,
                "OnwardDate": dep_str,
                "TUI": "",
            }],
            "Parameters": {
                "Airlines": "",
                "GroupType": "",
                "Refundable": "",
                "IsDirect": False,
                "PaxCategory": "",
            },
        })

        async def modify_express_search(route, request):
            """Intercept ExpressSearch and replace body with our custom route."""
            await route.continue_(post_data=custom_body)

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("AKBARTRAVELS_PROXY")
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

            # Wire up interceptors
            page.on("response", on_response)
            await page.route("**/flights/ExpressSearch", modify_express_search)

            # Navigate to homepage — Angular SPA bootstraps
            await page.goto(
                f"{_BASE}/in/?lan=en",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_timeout(4000)

            # Dismiss notification popup and remove calendar overlay
            await page.evaluate("""
                // Remove overlays that block the SEARCH button
                document.querySelectorAll(
                    '.cdk-overlay-container, .cdk-overlay-backdrop, '
                    + '.notification-popup, .popup-overlay, '
                    + '.mat-datepicker-popup, .calendar-popup, .cal-popup'
                ).forEach(el => el.remove());
            """)
            await page.wait_for_timeout(500)

            # JS-click the SEARCH button (retry up to 3 times with wait)
            clicked = "no-search-button"
            for _btn_attempt in range(3):
                await page.evaluate("""
                    document.querySelectorAll(
                        '.cdk-overlay-container, .cdk-overlay-backdrop, '
                        + '.notification-popup, .popup-overlay, '
                        + '.mat-datepicker-popup, .calendar-popup, .cal-popup'
                    ).forEach(el => el.remove());
                """)
                clicked = await page.evaluate("""
                    () => {
                        // Primary: mat-flat-button primary (observed in probes)
                        const btns = document.querySelectorAll('button.mat-flat-button.mat-primary');
                        for (const btn of btns) {
                            const txt = (btn.textContent || '').trim().toUpperCase();
                            if (txt.includes('SEARCH') || txt.includes('FIND')) {
                                btn.click();
                                return 'clicked: ' + txt;
                            }
                        }
                        // Fallback: any button with search text
                        const all = document.querySelectorAll('button');
                        for (const btn of all) {
                            const txt = (btn.textContent || '').trim().toUpperCase();
                            if (txt.includes('SEARCH')) {
                                btn.click();
                                return 'clicked-fallback: ' + txt;
                            }
                        }
                        return 'no-search-button';
                    }
                """)
                if "no-search-button" not in str(clicked):
                    break
                await page.wait_for_timeout(2000)

            logger.debug("AKBARTRAVELS SEARCH click result: %s", clicked)

            if "no-search-button" in str(clicked):
                logger.warning("AKBARTRAVELS: could not find SEARCH button")
                await page.close(); await ctx.close(); await browser.close()
                return None

            # Wait for Completed=true from GetExpSearch polling
            try:
                await asyncio.wait_for(
                    completed_event.wait(), timeout=self.timeout - 10
                )
            except asyncio.TimeoutError:
                logger.debug(
                    "AKBARTRAVELS: timeout waiting for Completed, "
                    "have %d partial responses", len(api_responses),
                )

            await page.wait_for_timeout(1000)
            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("AKBARTRAVELS browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("AKBARTRAVELS: no GetExpSearch response captured")
            return None

        # Use the last (most complete) response
        final_data = api_responses[-1]
        all_offers = _extract_offers(final_data, req)

        # Deduplicate by price + first flight number
        seen: set[str] = set()
        unique: list[FlightOffer] = []
        for o in all_offers:
            fno = o.outbound.segments[0].flight_no if o.outbound and o.outbound.segments else ""
            key = f"{o.price}_{fno}"
            if key not in seen:
                seen.add(key)
                unique.append(o)

        return unique

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
                    id=f"rt_akb_{cid}", price=price, currency=o.currency,
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
