"""
Sun Country Airlines direct connector — Playwright lowfare API.

PATCH: Auto-capture Ocp-Apim-Subscription-Key from the browser's own API
requests instead of relying on SUNCOUNTRY_SUB_KEY env var.  The SPA fires
requests to syprod-api.suncountry.com that already contain the key in headers.
We intercept those outgoing requests, grab the key, then use it for our
in-browser fetch() calls.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
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
from .browser import auto_block_if_proxied, get_default_proxy

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

_SUB_KEY = os.environ.get("SUNCOUNTRY_SUB_KEY", "")

_browser = None
_browser_lock: Optional[asyncio.Lock] = None


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
        _proxy = get_default_proxy()
        if _proxy:
            launch_kw["proxy"] = _proxy
        try:
            _browser = await pw.chromium.launch(**launch_kw)
        except Exception:
            launch_kw.pop("channel", None)
            _browser = await pw.chromium.launch(**launch_kw)
        logger.info("SunCountry: headed Chrome launched")
        return _browser


class SunCountryConnectorClient:
    """Sun Country Airlines connector — Playwright + lowfare API."""

    def __init__(self, timeout: float = 45.0):
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
            offers = await self._search_via_browser(req)
            elapsed = time.monotonic() - t0
            logger.info(
                "SunCountry: %s→%s on %s — %d offers in %.1fs",
                req.origin, req.destination, req.date_from, len(offers), elapsed,
            )
            return FlightSearchResponse(
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
                search_id=f"suncountry_{req.origin}_{req.destination}_{req.date_from}_{req.return_from or ''}",
            )
        except Exception as e:
            logger.error("SunCountry search error: %s", e)
            return self._empty(req)

    async def _search_via_browser(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="America/Chicago",
            service_workers="block",
        )
        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            token_holder: dict = {}
            token_event = asyncio.Event()
            # PATCH: Also capture the subscription key from outgoing requests
            sub_key_holder: dict = {}

            async def on_response(response):
                try:
                    if "/nsk/v1/token" in response.url and response.status in (200, 201):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            tok = (data.get("data") or data).get("token", "")
                            if tok and not token_holder.get("token"):
                                token_holder["token"] = tok
                                token_event.set()
                except Exception:
                    pass

            # PATCH: Intercept outgoing requests to capture Ocp-Apim-Subscription-Key
            def on_request(request):
                try:
                    if "syprod-api.suncountry.com" in request.url and not sub_key_holder.get("key"):
                        headers = request.headers
                        sk = headers.get("ocp-apim-subscription-key") or headers.get("Ocp-Apim-Subscription-Key")
                        if sk:
                            sub_key_holder["key"] = sk
                            logger.info("SunCountry: captured subscription key from browser request")
                except Exception:
                    pass

            page.on("response", on_response)
            page.on("request", on_request)

            # Step 1: Load homepage to get WAF cookies and Navitaire token
            logger.info("SunCountry: loading homepage for WAF cookies")
            await page.goto(
                "https://www.suncountry.com/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            # Wait for the SPA to obtain the Navitaire JWT
            try:
                await asyncio.wait_for(token_event.wait(), timeout=15)
            except asyncio.TimeoutError:
                pass
            token = token_holder.get("token")
            if not token:
                logger.warning("SunCountry: no Navitaire token found")
                return []

            # PATCH: Wait a bit more for the SPA to fire API requests that carry the sub key
            if not sub_key_holder.get("key"):
                await page.wait_for_timeout(3000)

            # PATCH: Also try extracting key from JS context
            if not sub_key_holder.get("key"):
                try:
                    js_key = await page.evaluate(r"""() => {
                        // Check common config patterns
                        try {
                            if (window.__NEXT_DATA__) {
                                const props = window.__NEXT_DATA__.props || {};
                                const pageProps = props.pageProps || {};
                                if (pageProps.subscriptionKey) return pageProps.subscriptionKey;
                                if (pageProps.apiKey) return pageProps.apiKey;
                            }
                        } catch(e) {}
                        // Check meta tags
                        try {
                            const meta = document.querySelector('meta[name*="subscription"], meta[name*="api-key"]');
                            if (meta) return meta.getAttribute('content');
                        } catch(e) {}
                        // Check inline scripts for the key pattern
                        try {
                            const scripts = document.querySelectorAll('script:not([src])');
                            for (const s of scripts) {
                                const m = s.textContent.match(/['\"]Ocp-Apim-Subscription-Key['\"]\s*:\s*['\"]([a-f0-9]{20,})['\"]/) ||
                                          s.textContent.match(/subscriptionKey\s*[=:]\s*['\"]([a-f0-9]{20,})['\"/]/) ||
                                          s.textContent.match(/apiSubscriptionKey\s*[=:]\s*['\"]([a-f0-9]{20,})['\"/]/);
                                if (m) return m[1];
                            }
                        } catch(e) {}
                        return null;
                    }""")
                    if js_key:
                        sub_key_holder["key"] = js_key
                        logger.info("SunCountry: captured subscription key from JS context")
                except Exception:
                    pass

            # PATCH: Use captured key, fall back to env var
            effective_sub_key = sub_key_holder.get("key") or _SUB_KEY
            if not effective_sub_key:
                logger.warning("SunCountry: no subscription key found (env or browser)")
                return []

            # Step 2: Call lowfare/outbound via in-browser fetch
            # Request a ±3 day window centred on the target date
            is_rt = bool(req.return_from)
            start_dt = req.date_from - timedelta(days=3)
            end_dt = req.date_from + timedelta(days=3)
            body = {
                "request": {
                    "origin": req.origin,
                    "destination": req.destination,
                    "currencyCode": "USD",
                    "includeTaxesAndFees": True,
                    "isRoundTrip": is_rt,
                    "numberOfPassengers": req.adults,
                    "startDate": start_dt.strftime("%m/%d/%Y"),
                    "endDate": end_dt.strftime("%m/%d/%Y"),
                }
            }

            result = await page.evaluate(
                """async ([token, subKey, body]) => {
                    try {
                        const resp = await fetch(
                            'https://syprod-api.suncountry.com/ext/v1/lowfare/outbound',
                            {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Ocp-Apim-Subscription-Key': subKey,
                                    'Authorization': token
                                },
                                body: JSON.stringify(body)
                            }
                        );
                        if (!resp.ok) return { error: resp.status };
                        return await resp.json();
                    } catch (e) {
                        return { error: e.message };
                    }
                }""",
                [token, effective_sub_key, body],
            )

            if not result or result.get("error"):
                logger.warning("SunCountry: lowfare error: %s", result)
                return []

            ob_offers = self._parse_lowfare(result, req)

            # For RT, also fetch inbound lowfares
            if is_rt and ob_offers:
                ib_start = req.return_from - timedelta(days=3)
                ib_end = req.return_from + timedelta(days=3)
                ib_body = {
                    "request": {
                        "origin": req.destination,
                        "destination": req.origin,
                        "currencyCode": "USD",
                        "includeTaxesAndFees": True,
                        "isRoundTrip": True,
                        "numberOfPassengers": req.adults,
                        "startDate": ib_start.strftime("%m/%d/%Y"),
                        "endDate": ib_end.strftime("%m/%d/%Y"),
                    }
                }
                ib_result = await page.evaluate(
                    """async ([token, subKey, body]) => {
                        try {
                            const resp = await fetch(
                                'https://syprod-api.suncountry.com/ext/v1/lowfare/inbound',
                                {
                                    method: 'POST',
                                    headers: {
                                        'Content-Type': 'application/json',
                                        'Ocp-Apim-Subscription-Key': subKey,
                                        'Authorization': token
                                    },
                                    body: JSON.stringify(body)
                                }
                            );
                            if (!resp.ok) return { error: resp.status };
                            return await resp.json();
                        } catch (e) {
                            return { error: e.message };
                        }
                    }""",
                    [token, effective_sub_key, ib_body],
                )

                if ib_result and not ib_result.get("error"):
                    ib_offers = self._parse_lowfare_inbound(ib_result, req)
                    if ib_offers:
                        ib_offers.sort(key=lambda x: x[1])
                        cheapest_ib_route, cheapest_ib_price = ib_offers[0]
                        rt_offers: list[FlightOffer] = []
                        for ob in ob_offers:
                            total = round(ob.price + cheapest_ib_price, 2)
                            key = f"{ob.id}_rt_{total}"
                            rt_offers.append(FlightOffer(
                                id=f"sy_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                                price=total,
                                currency="USD",
                                price_formatted=f"${total:.2f}",
                                outbound=ob.outbound,
                                inbound=cheapest_ib_route,
                                airlines=["Sun Country Airlines"],
                                owner_airline="SY",
                                booking_url="https://www.suncountry.com/booking/select",
                                is_locked=False,
                                source="suncountry_direct",
                                source_tier="free",
                            ))
                        return rt_offers + ob_offers

            return ob_offers
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_lowfare(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        fares = data.get("lowfares") or []
        target = req.date_from.strftime("%Y-%m-%d")
        booking_url = "https://www.suncountry.com/booking/select"
        offers: list[FlightOffer] = []

        for fare in fares:
            fare_date = (fare.get("date") or "")[:10]
            if fare_date != target:
                continue
            if fare.get("noFlights") or fare.get("soldOut"):
                continue
            price = fare.get("price")
            if not price or price <= 0:
                continue

            seats = fare.get("available")
            dep_dt = datetime.fromisoformat(fare["date"])

            seg = FlightSegment(
                airline="SY",
                airline_name="Sun Country Airlines",
                flight_no="SY",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class="economy",
            )
            route = FlightRoute(segments=[seg])

            key = f"{req.origin}{req.destination}{fare_date}{price}"
            offer_id = f"sy_{hashlib.md5(key.encode()).hexdigest()[:12]}"

            offers.append(FlightOffer(
                id=offer_id,
                price=round(price, 2),
                currency="USD",
                price_formatted=f"${price:.2f}",
                outbound=route,
                inbound=None,
                airlines=["Sun Country Airlines"],
                owner_airline="SY",
                booking_url=booking_url,
                is_locked=False,
                source="suncountry_direct",
                source_tier="free",
                availability_seats=seats if isinstance(seats, int) else None,
            ))

        return offers

    def _parse_lowfare_inbound(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[tuple[FlightRoute, float]]:
        """Parse inbound lowfares into (route, price) tuples."""
        fares = data.get("lowfares") or []
        target = req.return_from.strftime("%Y-%m-%d")
        results: list[tuple[FlightRoute, float]] = []

        for fare in fares:
            fare_date = (fare.get("date") or "")[:10]
            if fare_date != target:
                continue
            if fare.get("noFlights") or fare.get("soldOut"):
                continue
            price = fare.get("price")
            if not price or price <= 0:
                continue

            dep_dt = datetime.fromisoformat(fare["date"])
            seg = FlightSegment(
                airline="SY",
                airline_name="Sun Country Airlines",
                flight_no="SY",
                origin=req.destination,
                destination=req.origin,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class="economy",
            )
            route = FlightRoute(segments=[seg])
            results.append((route, round(price, 2)))

        return results

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
                    id=f"rt_sunc_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
            search_id=f"suncountry_{req.origin}_{req.destination}_{req.date_from}_{req.return_from or ''}",
        )
