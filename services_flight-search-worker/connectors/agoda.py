"""
Agoda Flights connector — Playwright browser + Kayak/Booking Holdings poll API.

Agoda (flights.agoda.com) is part of Booking Holdings. Its flights vertical
uses Kayak's search backend (same /flights/poll endpoint, same results JSON
schema).  We reuse the shared Booking Holdings parser from momondo.py.

Strategy (identical to kayak.py but on the flights.agoda.com domain):
1.  Launch Playwright browser (non-headless, offscreen).
2.  Navigate to flights.agoda.com search results URL.
3.  Intercept the /flights/poll or /s/horizon/ API responses.
4.  Parse using _parse_booking_holdings_poll (shared with Kayak/Momondo).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time

from ..models.flights import (
    FlightOffer,
    FlightSearchRequest,
    FlightSearchResponse,
)
from .browser import get_proxy
from .momondo import _parse_booking_holdings_poll

logger = logging.getLogger(__name__)


class AgodaConnectorClient:
    """Agoda Flights — Booking Holdings / Kayak backend, Playwright + poll API."""

    def __init__(self, timeout: float = 55.0):
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
                        "AGODA %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"agoda{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_ag_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("AGODA attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            # Agoda uses /i/api/search/dynamic/flights/poll (Kayak backend)
            hit = any(k in url for k in [
                "/api/search/dynamic/flights/poll",
                "/flights/poll", "/flights/results",
                "/s/horizon/", "/s/run/",
                "/api/flight", "/graphql",
            ])
            if not hit:
                return
            try:
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    body = await response.text()
                    if len(body) > 5000:
                        data = json.loads(body)
                        # Booking Holdings format has results + legs
                        if data.get("results") and data.get("legs"):
                            api_responses.append(data)
                        # Alternative: check for itineraries / offers arrays
                        elif data.get("itineraries") or data.get("offers"):
                            api_responses.append(data)
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("AGODA_PROXY")
            launch_kw: dict = {
                "headless": False,
                "args": [
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
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            if proxy:
                from .browser import auto_block_if_proxied
                await auto_block_if_proxied(page)
            page.on("response", on_response)

            dep_date = req.date_from.isoformat()

            # flights.agoda.com URL patterns (Kayak-style routing)
            # Primary: flights.agoda.com/flights/{ORIGIN}-{DEST}/{DATE}?sort=price_a
            # Fallback: www.agoda.com/flights/results?origin={ORIGIN}...
            url = (
                f"https://flights.agoda.com/flights/"
                f"{req.origin}-{req.destination}/{dep_date}"
                f"?sort=price_a"
            )

            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            # If redirected away from flights, try main agoda domain
            current = page.url
            if "flights" not in current.lower():
                _ag_cabin = {"M": "Economy", "W": "PremiumEconomy", "C": "Business", "F": "First"}.get(req.cabin_class, "Economy") if req.cabin_class else "Economy"
                alt_url = (
                    f"https://www.agoda.com/flights/results"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&departDate={dep_date}&adults={req.adults}"
                    f"&class={_ag_cabin}"
                )
                await page.goto(alt_url, wait_until="domcontentloaded", timeout=20000)

            # Wait for poll responses (Booking Holdings progressive loading)
            for _ in range(12):
                await page.wait_for_timeout(3000)
                if len(api_responses) >= 2:
                    await page.wait_for_timeout(5000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("AGODA browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("AGODA: no flight API response captured")
            return None

        offers = _parse_booking_holdings_poll(
            api_responses, req,
            source="agoda_meta",
            id_prefix="ag",
            booking_base_url="https://flights.agoda.com/flights",
        )

        # Enhance booking URLs with per-result deep links from Agoda
        data = api_responses[-1]
        result_map: dict[str, dict] = {}
        for r in data.get("results", []):
            if r.get("type") == "core":
                rid = r.get("resultId", "")
                opts = r.get("bookingOptions") or []
                if opts:
                    burl = opts[0].get("bookingUrl", {})
                    if burl.get("url"):
                        result_map[rid] = burl

        for offer in offers:
            # Match by result ID embedded in offer ID
            for rid, burl in result_map.items():
                bid_hash = hashlib.md5(f"ag_{rid}_{offer.price}".encode()).hexdigest()[:10]
                if offer.id == f"ag_{bid_hash}":
                    rel_url = burl["url"]
                    offer.booking_url = f"https://flights.agoda.com{rel_url}"
                    break

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
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
                    id=f"rt_ag_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
