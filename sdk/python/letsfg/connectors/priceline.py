"""
Priceline connector — Playwright browser + Booking Holdings poll API.

Priceline (priceline.com) is part of Booking Holdings and shares the
Kayak/Momondo flight search backend.  We reuse the shared Booking Holdings
parser from momondo.py (same ``_parse_booking_holdings_poll`` function).

Strategy (identical to agoda.py / kayak.py):
1. Launch Playwright browser (non-headless, offscreen).
2. Navigate to Priceline flights search results URL.
3. Intercept the /flights/poll or /s/horizon/ API responses.
4. Parse using _parse_booking_holdings_poll (shared with Kayak/Momondo/Agoda).
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


class PricelineConnectorClient:
    """Priceline — Booking Holdings / Kayak backend, Playwright + poll API."""

    def __init__(self, timeout: float = 60.0):
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
                        "PRICELINE %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"pcl{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_pcl_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("PRICELINE attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            # Priceline/Kayak backend endpoints:
            #   /flights/poll, /flights/results, /s/horizon/, /s/run/
            #   /api/search/flight, /graphql
            hit = any(k in url for k in [
                "/flights/poll", "/flights/results",
                "/s/horizon/", "/s/run/",
                "/api/search", "/graphql",
                "/api/flight",
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
                        # Booking Holdings format (results + legs)
                        if data.get("results") and data.get("legs"):
                            api_responses.append(data)
                        # Alternative shapes
                        elif data.get("itineraries") or data.get("offers"):
                            api_responses.append(data)
                        # Priceline's own wrapper
                        elif data.get("data", {}).get("results"):
                            api_responses.append(data.get("data"))
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("PRICELINE_PROXY")
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

            dep_date = req.date_from.strftime("%Y%m%d")
            dep_iso = req.date_from.isoformat()
            adults = req.adults or 1

            # Priceline URL pattern: /m/fly/search/{ORIGIN}-{DEST}-{YYYYMMDD}/{PAX}
            url = (
                f"https://www.priceline.com/m/fly/search/"
                f"{req.origin}-{req.destination}-{dep_date}/{adults}"
            )
            if req.return_from:
                ret_date = req.return_from.strftime("%Y%m%d")
                url = (
                    f"https://www.priceline.com/m/fly/search/"
                    f"{req.origin}-{req.destination}-{dep_date}/"
                    f"{req.destination}-{req.origin}-{ret_date}/{adults}"
                )

            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            # If redirected away, try alternate URL
            current = page.url
            if "fly" not in current.lower() and "flight" not in current.lower():
                alt_url = (
                    f"https://www.priceline.com/flights/search"
                    f"?origin={req.origin}&destination={req.destination}"
                    f"&date={dep_iso}&adults={adults}"
                )
                try:
                    await page.goto(alt_url, wait_until="domcontentloaded", timeout=20000)
                except Exception:
                    pass

            # Wait for poll responses (Booking Holdings progressive loading)
            for _ in range(14):
                await page.wait_for_timeout(3000)
                if len(api_responses) >= 2:
                    await page.wait_for_timeout(5000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("PRICELINE browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("PRICELINE: no flight API response captured")
            return None

        return _parse_booking_holdings_poll(
            api_responses, req,
            source="priceline_meta",
            id_prefix="pcl",
            booking_base_url="https://www.priceline.com/m/fly/search",
        )

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
                    id=f"rt_pcl_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        return {
            "checked_bag_note": "baggage depends on airline and fare – may be included or add-on from ~30 USD; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        pass  # conditions + bags_price set per-offer by engine's apply_ref_ancillaries via owner_airline
