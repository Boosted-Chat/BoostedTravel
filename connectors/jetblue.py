"""
JetBlue Airways direct API connector — bestFares calendar + Playwright fallback.

JetBlue (IATA: B6) is a major US low-cost carrier with 114+ destinations
across the US, Caribbean, Latin America, and Europe (London, Paris, Amsterdam).

IMPORTANT: JetBlue's booking/results page (jetblue.com/booking/flights) uses
PerimeterX bot detection that may block non-US IP addresses or automated
browsers. The bestFares calendar API works globally, but the Playwright
fallback (for individual flight details) may require a US IP address.
Set JETBLUE_PROXY to an HTTP proxy URL with a US exit IP if needed.

Strategy:
  1. Fast path: direct REST GET to jbrest.jetblue.com/bff-service-v2/bestFares/
     Returns cheapest fare per day for the requested month — no auth required.
  2. Fallback: Playwright browser fills the search form, intercepts the
     flight results API response for full flight details (times, flight numbers).

API details:
  GET https://jbrest.jetblue.com/bff-service-v2/bestFares/
  Params: origin, destination, fareType=LOWEST, month=APRIL+2026,
          tripType=ONE_WAY, adult=1, child=0, infant=0, apiKey=None
  Response: { currencyCode, outboundFares: [{date, amount, tax, seats}] }
  No authentication or cookies required.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import auto_block_if_proxied

logger = logging.getLogger(__name__)

_BEST_FARES_URL = "https://jbrest.jetblue.com/bff-service-v2/bestFares/"
_BOOKING_URL_TPL = (
    "https://www.jetblue.com/booking/flights"
    "?from={origin}&to={dest}&depart={date}"
    "&isMultiCity=false&noOfRoute=1&adults={adults}&children=0&infants=0"
    "&sharedMarket=false&roundTripFaresFlag={rt_flag}&usePoints=false"
    "{return_part}"
)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]
_TIMEZONES = [
    "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles",
]

# Shared browser singleton for Playwright fallback
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_proxy_url() -> str:
    """Read proxy URL from JETBLUE_PROXY env var (no default — works globally for API)."""
    return os.environ.get("JETBLUE_PROXY", "").strip()


def _get_pw_proxy() -> Optional[dict]:
    raw = _get_proxy_url()
    if not raw:
        return None
    from urllib.parse import urlparse
    p = urlparse(raw)
    result: dict[str, str] = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        result["username"] = p.username
    if p.password:
        result["password"] = p.password
    return result


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    global _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        pw = await async_playwright().start()
        launch_kw: dict = {
            "headless": False,
            "channel": "chrome",
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
            ],
        }
        proxy = _get_pw_proxy()
        if proxy:
            launch_kw["proxy"] = proxy
        try:
            _browser = await pw.chromium.launch(**launch_kw)
        except Exception:
            launch_kw.pop("channel", None)
            _browser = await pw.chromium.launch(**launch_kw)
        logger.info("JetBlue: headed Chrome launched (proxy=%s)", bool(proxy))
        return _browser


class JetBlueConnectorClient:
    """JetBlue connector — calendar API + Playwright fallback."""

    def __init__(self, timeout: float = 30.0):
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
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            # Fast path: calendar best fares API
            offers = await self._search_via_api(req)
            if offers:
                elapsed = time.monotonic() - t0
                return self._build_response(offers, req, elapsed, method="API bestFares")

            # Fallback: Playwright browser
            logger.info("JetBlue: API returned no fares, falling back to Playwright")
            return await self._playwright_fallback(req, t0)
        except Exception as e:
            logger.error("JetBlue search error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------
    # Direct API — bestFares calendar
    # ------------------------------------------------------------------

    async def _search_via_api(self, req: FlightSearchRequest) -> list[FlightOffer]:
        is_rt = bool(req.return_from)
        month_str = req.date_from.strftime("%B %Y").upper()  # "APRIL 2026"
        params = {
            "apiKey": "None",
            "origin": req.origin,
            "destination": req.destination,
            "fareType": "LOWEST",
            "month": month_str,
            "tripType": "ROUND_TRIP" if is_rt else "ONE_WAY",
            "adult": str(req.adults),
            "child": "0",
            "infant": "0",
        }
        if is_rt:
            params["returnMonth"] = req.return_from.strftime("%B %Y").upper()
        proxy_url = _get_proxy_url()
        try:
            async with httpx.AsyncClient(
                timeout=15,
                proxy=proxy_url if proxy_url else None,
            ) as client:
                r = await client.get(_BEST_FARES_URL, params=params)
            if r.status_code != 200:
                logger.warning("JetBlue bestFares HTTP %d", r.status_code)
                return []
            data = r.json()
        except Exception as e:
            logger.warning("JetBlue bestFares error: %s", e)
            return []

        return self._parse_best_fares(data, req)

    def _parse_best_fares(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        currency = data.get("currencyCode", "USD")
        fares = data.get("outboundFares") or []
        target_date = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        # Map cabin code to cabin name for segment
        _b6_cabin = {"M": "economy", "W": "economy", "C": "mint", "F": "mint"}.get(req.cabin_class or "M", "economy")
        is_rt = bool(req.return_from)

        # Find return fare for RT
        inbound_route = None
        inbound_price = 0.0
        if is_rt:
            ret_fares = data.get("inboundFares") or data.get("returnFares") or []
            ret_target = req.return_from.strftime("%Y-%m-%d")
            for rf in ret_fares:
                if rf.get("date") == ret_target and rf.get("amount", 0) > 0:
                    ib_amount = rf.get("amount", 0) + rf.get("tax", 0)
                    inbound_price = ib_amount
                    inbound_route = FlightRoute(
                        segments=[FlightSegment(
                            airline="B6", airline_name="JetBlue Airways",
                            flight_no="", origin=req.destination,
                            destination=req.origin,
                            departure=datetime.strptime(ret_target, "%Y-%m-%d"),
                            arrival=datetime.strptime(ret_target, "%Y-%m-%d"),
                            cabin_class=_b6_cabin,
                        )],
                        total_duration_seconds=0, stopovers=0,
                    )
                    break

        for fare in fares:
            fare_date = fare.get("date", "")
            amount = fare.get("amount", 0)
            if not fare_date or not amount or amount <= 0:
                continue
            if fare_date != target_date:
                continue

            tax = fare.get("tax", 0)
            ob_total = amount + tax
            seats = fare.get("seats", 0)

            seg = FlightSegment(
                airline="B6", airline_name="JetBlue Airways",
                flight_no="", origin=req.origin,
                destination=req.destination,
                departure=datetime.strptime(fare_date, "%Y-%m-%d"),
                arrival=datetime.strptime(fare_date, "%Y-%m-%d"),
                cabin_class=_b6_cabin,
            )
            outbound_route = FlightRoute(
                segments=[seg], total_duration_seconds=0, stopovers=0,
            )

            if is_rt and inbound_route:
                total = round(ob_total + inbound_price, 2)
                offer_id = f"b6_{hashlib.md5(f'{req.origin}{req.destination}{fare_date}rt{total}'.encode()).hexdigest()[:12]}"
                offers.append(FlightOffer(
                    id=offer_id, price=total, currency=currency,
                    price_formatted=f"${total:.2f}",
                    outbound=outbound_route, inbound=inbound_route,
                    airlines=["JetBlue"], owner_airline="B6",
                    booking_url=booking_url, is_locked=False,
                    source="jetblue_direct", source_tier="free",
                    availability_seats=seats if seats else None,
                ))

            # Always emit OW for combo engine
            ow_total = round(ob_total, 2)
            offer_id = f"b6_{hashlib.md5(f'{req.origin}{req.destination}{fare_date}{ow_total}'.encode()).hexdigest()[:12]}"
            offers.append(FlightOffer(
                id=offer_id, price=ow_total, currency=currency,
                price_formatted=f"${ow_total:.2f}",
                outbound=outbound_route, inbound=None,
                airlines=["JetBlue"], owner_airline="B6",
                booking_url=booking_url, is_locked=False,
                source="jetblue_direct", source_tier="free",
                availability_seats=seats if seats else None,
            ))

        return offers

    # ------------------------------------------------------------------
    # Playwright fallback — fill form + intercept API
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            timezone_id=random.choice(_TIMEZONES),
            locale="en-US",
            service_workers="block",
        )
        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await auto_block_if_proxied(page)
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()
                await auto_block_if_proxied(page)

            captured: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and (
                        "bestfares" in url
                        or "flights" in url and "booking" in url
                        or "availability" in url
                        or "search" in url and "jbrest" in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, dict):
                                captured["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("JetBlue Playwright: %s->%s on %s", req.origin, req.destination, req.date_from)
            await page.goto(
                "https://www.jetblue.com/flights",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)
            await self._dismiss_overlays(page)
            await asyncio.sleep(0.5)

            # Set one-way
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Fill From
            ok = await self._fill_airport(page, "From", req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill To
            ok = await self._fill_airport(page, "To", req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill date
            ok = await self._fill_date(page, req)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.3)

            # Search
            await self._click_search(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("JetBlue Playwright: timeout waiting for API response")
                return self._empty(req)

            data = captured.get("json", {})
            if not data:
                return self._empty(req)
            offers = self._parse_best_fares(data, req)
            elapsed = time.monotonic() - t0
            return self._build_response(offers, req, elapsed, method="Playwright")

        except Exception as e:
            logger.error("JetBlue Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    async def _dismiss_overlays(self, page) -> None:
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '.truste_overlay, [id*="truste"], [id*="consent-track"], [id*="pop-div"]'
                ).forEach(el => el.remove());
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        try:
            btn = page.get_by_role("button", name=re.compile(r"round\s*trip", re.IGNORECASE))
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
                await asyncio.sleep(0.5)
                ow = page.get_by_role("option", name=re.compile(r"one.?way", re.IGNORECASE))
                if await ow.count() > 0:
                    await ow.first.click(timeout=3000)
                    logger.info("JetBlue: selected One-way")
        except Exception as e:
            logger.debug("JetBlue: trip type error: %s", e)

    async def _fill_airport(self, page, label: str, iata: str) -> bool:
        try:
            field = page.get_by_role("combobox", name=re.compile(rf"^{label}$", re.IGNORECASE))
            if await field.count() == 0:
                return False
            await field.first.click(timeout=5000)
            await asyncio.sleep(0.3)
            await field.first.fill(iata)
            await asyncio.sleep(1.5)

            opt = page.get_by_role("option").filter(
                has_text=re.compile(rf"\({re.escape(iata)}\)", re.IGNORECASE)
            )
            if await opt.count() > 0:
                await opt.first.click(timeout=3000)
                logger.info("JetBlue: selected %s for %s", iata, label)
                return True

            await page.keyboard.press("Enter")
            return True
        except Exception as e:
            logger.debug("JetBlue: %s field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        try:
            date_field = page.get_by_role(
                "textbox", name=re.compile(r"Depart", re.IGNORECASE)
            )
            if await date_field.count() == 0:
                return False
            await date_field.first.click(timeout=5000)
            await asyncio.sleep(1.0)

            # Click target date button in the calendar
            target = req.date_from
            day_name = target.strftime("%A, %B %-d, %Y")  # "Tuesday, April 7, 2026"
            try:
                day_name = target.strftime("%A, %B %#d, %Y")  # Windows variant
            except ValueError:
                pass

            # Navigate months if needed (click Next Month)
            for _ in range(12):
                day_btn = page.get_by_role("button", name=re.compile(
                    rf"{target.strftime('%A')},\s*{target.strftime('%B')}\s+{target.day},\s*{target.year}",
                    re.IGNORECASE,
                ))
                if await day_btn.count() > 0:
                    await day_btn.first.click(timeout=3000)
                    logger.info("JetBlue: selected date %s", target)
                    return True
                # Try next month
                next_btn = page.get_by_role("button", name=re.compile(r"next\s*month", re.IGNORECASE))
                if await next_btn.count() > 0:
                    await next_btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                else:
                    break
            return False
        except Exception as e:
            logger.debug("JetBlue: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        try:
            btn = page.get_by_role("button", name=re.compile(r"search\s*flights", re.IGNORECASE))
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
                logger.info("JetBlue: clicked Search flights")
        except Exception as e:
            logger.debug("JetBlue: search button error: %s", e)

    # ------------------------------------------------------------------
    # Response building
    # ------------------------------------------------------------------

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest,
        elapsed: float, method: str,
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "JetBlue %s->%s returned %d offers in %.1fs (%s)",
            req.origin, req.destination, len(offers), elapsed, method,
        )
        h = hashlib.md5(
            f"jetblue{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        is_rt = bool(req.return_from)
        return _BOOKING_URL_TPL.format(
            origin=req.origin,
            dest=req.destination,
            date=req.date_from.strftime("%Y-%m-%d"),
            adults=req.adults,
            rt_flag="true" if is_rt else "false",
            return_part=f"&return={req.return_from.strftime('%Y-%m-%d')}" if is_rt else "",
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"jetblue{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
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
                    id=f"rt_jetb_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
