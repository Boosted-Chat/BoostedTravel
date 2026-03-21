from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import (
    _launched_pw_instances,
    acquire_browser_slot,
    release_browser_slot,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://booking.flyarystan.com/ibe/availability"
_SESSION_MAX_AGE = 10 * 60

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
        viewport={"width": 1366, "height": 768},
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
        await page.goto("https://booking.flyarystan.com/ibe/", wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(2)
        _page = page
        _session_ts = time.monotonic()
        return _page
    except Exception as exc:
        logger.warning("FlyArystan session setup failed: %s", exc)
        try:
            await page.close()
        except Exception:
            pass
        _page = None
        return None


class FlyArystanConnectorClient:
    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        lock = await _get_lock()
        offers: list[FlightOffer] = []

        async with lock:
            await acquire_browser_slot()
            try:
                page = await _ensure_session()
                if page:
                    data = await self._fetch_results(page, req)
                    if data:
                        offers = self._build_offers(data, req)
            except Exception as exc:
                logger.warning("FlyArystan search failed for %s->%s: %s", req.origin, req.destination, exc)
            finally:
                release_browser_slot()

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("FlyArystan %s->%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        search_hash = hashlib.md5(
            f"flyarystan{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "KZT",
            offers=offers,
            total_results=len(offers),
        )

    async def _fetch_results(self, page, req: FlightSearchRequest) -> Optional[dict]:
        url = self._build_url(req)
        await page.goto(url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
        await page.wait_for_selector(".availability-flight-table, .js-journey, .error, .container", timeout=int(self.timeout * 1000))
        await asyncio.sleep(2)

        result = await page.evaluate(
            r"""() => {
                const journeyNodes = [...document.querySelectorAll('.js-journey')];
                const pageText = (document.body?.innerText || '').slice(0, 4000);
                const journeys = journeyNodes.map((el) => {
                    const routeBlock = el.querySelector('.desktop-route-block') || el.querySelector('.mobile-route-block') || el;
                    const timeNodes = [...routeBlock.querySelectorAll('.time')].map(n => (n.textContent || '').trim()).filter(Boolean);
                    const portNodes = [...routeBlock.querySelectorAll('.port')].map(n => (n.textContent || '').trim()).filter(Boolean);
                    const dateNodes = [...routeBlock.querySelectorAll('.date')].map(n => (n.textContent || '').trim()).filter(Boolean);
                    const flightNo = (routeBlock.querySelector('.flight-no span') || routeBlock.querySelector('.flight-no'))?.textContent?.trim() || '';
                    const fareNodes = [...el.querySelectorAll('.fare-item.js-fare-item-selector')];
                    const fares = [];
                    const seen = new Set();
                    for (const fare of fareNodes) {
                        const text = (fare.innerText || '').replace(/\s+/g, ' ').trim();
                        if (!text) continue;
                        const soldOut = /НЕТ МЕСТ|NO SEATS|SOLD OUT/i.test(text);
                        const priceMatch = text.match(/(\d[\d ]+)\s*KZT/i);
                        if (!priceMatch) continue;
                        const price = Number(priceMatch[1].replace(/ /g, ''));
                        if (!price || !Number.isFinite(price)) continue;
                        const labelNode = fare.querySelector('.fare-type span') || fare.querySelector('.fare-type') || fare.querySelector('.badge.best-offer');
                        const label = (labelNode?.textContent || '').replace(/\s+/g, ' ').trim() || 'Standard';
                        const key = `${label}|${price}`;
                        if (seen.has(key)) continue;
                        seen.add(key);
                        fares.push({ label, price, soldOut, text });
                    }
                    return {
                        journeyType: el.getAttribute('data-journeytype') || 'OUTBOUND',
                        stopCount: Number(el.getAttribute('data-stop-count') || '0'),
                        durationSeconds: Number(el.getAttribute('data-journey-duration') || '0'),
                        depTs: Number(el.getAttribute('data-dep-date') || '0'),
                        arrTs: Number(el.getAttribute('data-arr-date') || '0'),
                        flightNo,
                        depPortName: portNodes[0] || '',
                        arrPortName: portNodes[1] || '',
                        depDateText: dateNodes[0] || '',
                        arrDateText: dateNodes[1] || dateNodes[0] || '',
                        soldOut: /НЕТ МЕСТ|NO SEATS|SOLD OUT/i.test(el.innerText || ''),
                        fares,
                    };
                });
                return {
                    title: document.title,
                    url: location.href,
                    journeyCount: journeys.length,
                    pageText,
                    journeys,
                };
            }"""
        )

        if not isinstance(result, dict):
            return None
        if result.get("journeyCount", 0) <= 0:
            logger.info("FlyArystan returned no journeys: %s", (result.get("pageText") or "")[:300])
            return None
        return result

    def _build_offers(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        journeys = data.get("journeys") or []
        outbound_journeys = [j for j in journeys if (j.get("journeyType") or "").upper() == "OUTBOUND"]
        inbound_journeys = [j for j in journeys if (j.get("journeyType") or "").upper() == "INBOUND"]

        outbound_offers = self._offers_for_direction(outbound_journeys, req.origin, req.destination, req)
        if not req.return_from:
            return outbound_offers[: req.limit]

        inbound_offers = self._offers_for_direction(inbound_journeys, req.destination, req.origin, req)
        if not outbound_offers or not inbound_offers:
            return []

        combined: list[FlightOffer] = []
        booking_url = self._build_url(req)
        for out_offer in outbound_offers:
            for in_offer in inbound_offers:
                total_price = round(out_offer.price + in_offer.price, 2)
                offer_hash = hashlib.md5(
                    f"fs_{out_offer.id}_{in_offer.id}_{total_price}".encode()
                ).hexdigest()[:12]
                combined.append(
                    FlightOffer(
                        id=f"fs_{offer_hash}",
                        price=total_price,
                        currency="KZT",
                        price_formatted=f"{total_price:,.0f} KZT",
                        outbound=out_offer.outbound,
                        inbound=in_offer.outbound,
                        airlines=["FlyArystan"],
                        owner_airline="FS",
                        conditions={
                            "outbound_fare": out_offer.conditions.get("fare_brand", ""),
                            "inbound_fare": in_offer.conditions.get("fare_brand", ""),
                        },
                        booking_url=booking_url,
                        is_locked=False,
                        source="flyarystan_direct",
                        source_tier="free",
                    )
                )

        combined.sort(key=lambda o: o.price)
        return combined[: req.limit]

    def _offers_for_direction(
        self,
        journeys: list[dict],
        origin: str,
        destination: str,
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        booking_url = self._build_url(req)
        for journey in journeys:
            fares = [fare for fare in (journey.get("fares") or []) if not fare.get("soldOut")]
            if not fares or journey.get("soldOut"):
                continue

            dep_dt = self._dt_from_ms(journey.get("depTs"))
            arr_dt = self._dt_from_ms(journey.get("arrTs"))
            flight_no = (journey.get("flightNo") or "").strip()
            if not dep_dt or not arr_dt:
                continue

            segment = FlightSegment(
                airline="FS",
                airline_name="FlyArystan",
                flight_no=flight_no,
                origin=origin,
                destination=destination,
                origin_city=journey.get("depPortName") or "",
                destination_city=journey.get("arrPortName") or "",
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=int(journey.get("durationSeconds") or 0),
                cabin_class="economy",
            )
            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=int(journey.get("durationSeconds") or 0),
                stopovers=int(journey.get("stopCount") or 0),
            )

            for fare in fares:
                price = round(float(fare.get("price") or 0), 2)
                if price <= 0:
                    continue
                fare_brand = fare.get("label") or "Standard"
                offer_hash = hashlib.md5(
                    f"flyarystan_{flight_no}_{dep_dt.isoformat()}_{fare_brand}_{price}_{origin}_{destination}".encode()
                ).hexdigest()[:12]
                offers.append(
                    FlightOffer(
                        id=f"fs_{offer_hash}",
                        price=price,
                        currency="KZT",
                        price_formatted=f"{price:,.0f} KZT",
                        outbound=route,
                        inbound=None,
                        airlines=["FlyArystan"],
                        owner_airline="FS",
                        conditions={"fare_brand": fare_brand},
                        booking_url=booking_url,
                        is_locked=False,
                        source="flyarystan_direct",
                        source_tier="free",
                    )
                )

        offers.sort(key=lambda o: o.price)
        return offers[: req.limit]

    @staticmethod
    def _dt_from_ms(value) -> Optional[datetime]:
        try:
            millis = int(value)
        except Exception:
            return None
        if millis <= 0:
            return None
        return datetime.fromtimestamp(millis / 1000)

    @staticmethod
    def _fmt_date(value) -> str:
        return value.strftime("%d.%m.%Y")

    def _build_url(self, req: FlightSearchRequest) -> str:
        trip_type = "ROUND_TRIP" if req.return_from else "ONE_WAY"
        query = (
            f"tripType={trip_type}"
            f"&depPort={req.origin}"
            f"&arrPort={req.destination}"
            f"&departureDate={self._fmt_date(req.date_from)}"
        )
        if req.return_from:
            query += f"&returnDate={self._fmt_date(req.return_from)}"
        query += (
            f"&adult={req.adults or 1}"
            f"&child={req.children or 0}"
            f"&infant={req.infants or 0}"
            f"&currency=KZT&lang=en"
        )
        return f"{_BASE_URL}?{query}"