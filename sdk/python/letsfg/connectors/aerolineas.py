from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import date, datetime, time as dt_time
from typing import Optional
from urllib.parse import urlencode

from connectors.browser import _launched_pw_instances, acquire_browser_slot, release_browser_slot
from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.aerolineas.com.ar/en-us/flex-dates-calendar"
_SESSION_MAX_AGE = 10 * 60
_AIRPORT_TO_CITY = {
    "AEP": "BUE",
    "EZE": "BUE",
}
_CABIN_MAP = {
    "economy": "Economy",
    "premium_economy": "PremiumEconomy",
    "premiumeconomy": "PremiumEconomy",
    "business": "Business",
}

_farm_lock: Optional[asyncio.Lock] = None
_pw_instance = None
_browser = None
_context = None
_page = None
_session_ts: float = 0.0


async def _get_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _ensure_session():
    global _page, _session_ts

    age = time.monotonic() - _session_ts
    if _page and age < _SESSION_MAX_AGE:
        try:
            await _page.evaluate("1+1")
            return _page
        except Exception:
            pass

    return await _refresh_session()


async def _refresh_session():
    global _pw_instance, _browser, _context, _page, _session_ts

    if _page:
        try:
            await _page.close()
        except Exception:
            pass
        _page = None

    if _context:
        try:
            await _context.close()
        except Exception:
            pass
        _context = None

    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None

    if _pw_instance:
        try:
            await _pw_instance.stop()
        except Exception:
            pass
        _pw_instance = None

    from playwright.async_api import async_playwright

    pw = await async_playwright().start()
    _pw_instance = pw
    _launched_pw_instances.append(pw)

    browser = await pw.chromium.launch(
        headless=False,
        channel="chrome",
        args=[
            "--disable-blink-features=AutomationControlled",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
        ],
    )
    _browser = browser

    context = await browser.new_context(
        viewport={"width": 1366, "height": 900},
        locale="en-US",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        ),
    )
    _context = context

    page = await context.new_page()
    await page.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    )

    try:
        await page.goto("https://www.aerolineas.com.ar/en-us", wait_until="domcontentloaded", timeout=45000)
        try:
            await page.locator("button:has-text('Accept')").first.click(timeout=3000)
        except Exception:
            pass
        await asyncio.sleep(2)
        _page = page
        _session_ts = time.monotonic()
        return _page
    except Exception as exc:
        logger.warning("Aerolíneas session setup failed: %s", exc)
        try:
            await page.close()
        except Exception:
            pass
        _page = None
        return None


def _normalize_code(code: str) -> str:
    return _AIRPORT_TO_CITY.get((code or "").upper(), (code or "").upper())


def _as_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def _parse_price(raw: str) -> float:
    cleaned = re.sub(r"[^\d.,]", "", raw or "")
    if cleaned.count(",") > 1 and "." not in cleaned:
        cleaned = cleaned.replace(",", "")
    elif "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    return round(float(cleaned), 2)


class AerolineasConnectorClient:
    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        offers: list[FlightOffer] = []
        lock = await _get_lock()

        async with lock:
            await acquire_browser_slot()
            try:
                page = await _ensure_session()
                if page:
                    data = await self._fetch_results(page, req)
                    if data:
                        offers = self._build_offers(data, req)
            except Exception as exc:
                logger.warning("Aerolíneas search failed for %s->%s: %s", req.origin, req.destination, exc)
            finally:
                release_browser_slot()

        offers.sort(key=lambda offer: offer.price if offer.price > 0 else float("inf"))
        logger.info(
            "Aerolíneas %s->%s: %d offers in %.1fs",
            req.origin,
            req.destination,
            len(offers),
            time.monotonic() - t0,
        )

        search_hash = hashlib.md5(
            f"aerolineas{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    async def _fetch_results(self, page, req: FlightSearchRequest) -> Optional[dict]:
        url = self._build_url(req)
        await page.goto(url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
        await page.wait_for_function(
            r"""() => {
                const text = document.body?.innerText || '';
                return /Total per passenger|There are no flights|No flights|No availability/i.test(text);
            }""",
            timeout=int(self.timeout * 1000),
        )
        await asyncio.sleep(2)

        return await page.evaluate(
            r"""() => {
                const text = (document.body?.innerText || '').replace(/\u00a0/g, ' ');
                const lines = text.split('\n').map(s => s.trim()).filter(Boolean);
                return {
                    url: location.href,
                    title: document.title,
                    lines,
                    text,
                };
            }"""
        )

    def _build_offers(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        lines = data.get("lines") or []
        if not lines:
            return []

        text = "\n".join(lines[-160:])
        if re.search(r"There are no flights|No flights|No availability", text, re.IGNORECASE):
            return []

        total_match = re.search(r"Total per passenger\s+([A-Z]{3})\s+([\d.,]+)", text)
        section_matches = re.findall(
            r"(ONE WAY|RETURN TRIP)\s+([^\n]+)\s+(\d{2}/\d{2}/\d{4})\s+([A-Z]{3})\s+([\d.,]+)",
            text,
        )
        if not total_match and not section_matches:
            return []

        currency = total_match.group(1) if total_match else section_matches[0][3]
        total_price = _parse_price(total_match.group(2)) if total_match else sum(_parse_price(m[4]) for m in section_matches)

        outbound = self._build_route(req.origin, req.destination, req.date_from)
        inbound = None
        if req.return_from:
            inbound = self._build_route(req.destination, req.origin, req.return_from)

        booking_url = data.get("url") or self._build_url(req)
        offer_hash = hashlib.md5(
            f"ar_{req.origin}{req.destination}{req.date_from}{req.return_from}{total_price}".encode()
        ).hexdigest()[:12]
        return [
            FlightOffer(
                id=f"ar_{offer_hash}",
                price=round(total_price, 2),
                currency=currency,
                price_formatted=f"{currency} {total_price:.2f}",
                outbound=outbound,
                inbound=inbound,
                airlines=["Aerolíneas Argentinas"],
                owner_airline="AR",
                booking_url=booking_url,
                is_locked=False,
                source="aerolineas_direct",
                source_tier="free",
            )
        ]

    @staticmethod
    def _build_route(origin: str, destination: str, travel_date: date | datetime) -> FlightRoute:
        travel_day = _as_date(travel_date)
        departure = datetime.combine(travel_day, dt_time(hour=12, minute=0))
        segment = FlightSegment(
            airline="AR",
            airline_name="Aerolíneas Argentinas",
            flight_no="",
            origin=origin,
            destination=destination,
            origin_city="",
            destination_city="",
            departure=departure,
            arrival=departure,
            duration_seconds=0,
            cabin_class="economy",
        )
        return FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)

    @staticmethod
    def _build_url(req: FlightSearchRequest) -> str:
        cabin = _CABIN_MAP.get((req.cabin_class or "economy").replace(" ", "").replace("-", "").lower(), "Economy")
        params: list[tuple[str, str]] = [
            ("adt", str(max(1, int(req.adults or 1)))),
            ("chd", str(max(0, int(req.children or 0)))),
            ("inf", str(max(0, int(req.infants or 0)))),
            ("cabinClass", cabin),
            ("flexDates", "true"),
        ]

        origin = _normalize_code(req.origin)
        destination = _normalize_code(req.destination)
        outbound_day = _as_date(req.date_from)

        if req.return_from:
            inbound_day = _as_date(req.return_from)
            params.append(("flightType", "ROUND_TRIP"))
            params.append(("leg", f"{origin}-{destination}-{outbound_day:%Y%m%d}"))
            params.append(("leg", f"{destination}-{origin}-{inbound_day:%Y%m%d}"))
        else:
            params.append(("flightType", "ONE_WAY"))
            params.append(("leg", f"{origin}-{destination}-{outbound_day:%Y%m%d}"))

        return f"{_BASE_URL}?{urlencode(params, doseq=True)}"