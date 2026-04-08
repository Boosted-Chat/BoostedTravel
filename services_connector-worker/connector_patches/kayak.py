"""
Kayak connector — Playwright browser + API response interception.

Kayak (Booking Holdings) is a major global flight meta-search engine.
Uses the same /i/api/search/dynamic/flights/poll endpoint as Momondo
and Cheapflights (all Booking Holdings properties).

Strategy:
1.  Launch Playwright browser (non-headless).
2.  Navigate to Kayak search results URL.
3.  Intercept the /flights/poll API response with progressive results.
4.  Parse itineraries using the shared Booking Holdings parser.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time

from ..models.flights import (
    FlightSearchRequest,
    FlightSearchResponse,
)
from .browser import get_proxy
from .momondo import _parse_booking_holdings_poll

logger = logging.getLogger(__name__)


class KayakConnectorClient:
    """Kayak — meta-search (Booking Holdings), Playwright + poll API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(
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
                        "KAYAK %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"kayak{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_ky_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("KAYAK attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest):
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            if "/flights/poll" not in url and "/flights/results" not in url:
                return
            try:
                if response.status == 200:
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    body = await response.text()
                    if len(body) > 5000:
                        data = json.loads(body)
                        if data.get("results") and data.get("legs"):
                            api_responses.append(data)
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("KAYAK_PROXY")
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
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            from .browser import auto_block_if_proxied
            await auto_block_if_proxied(page)
            page.on("response", on_response)

            dep_date = req.date_from.isoformat()
            date_path = dep_date
            if req.return_from:
                date_path = f"{dep_date}/{req.return_from.isoformat()}"
            url = (
                f"https://www.kayak.com/flights/"
                f"{req.origin}-{req.destination}/{date_path}"
                f"?sort=price_a"
            )

            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            for _ in range(10):
                await page.wait_for_timeout(3000)
                if len(api_responses) >= 2:
                    await page.wait_for_timeout(5000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("KAYAK browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("KAYAK: no flight API response captured")
            return None

        return _parse_booking_holdings_poll(
            api_responses, req,
            source="kayak_meta",
            id_prefix="ky",
            booking_base_url="https://www.kayak.com/flights",
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
