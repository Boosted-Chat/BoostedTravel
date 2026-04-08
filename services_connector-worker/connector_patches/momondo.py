"""
Momondo connector — Playwright browser + API response interception.

Momondo (Booking Holdings / Kayak) is a global flight meta-search engine
known for finding obscure routes and low-cost carriers.

Strategy:
1.  Launch Playwright browser (non-headless).
2.  Navigate to Momondo search results URL.
3.  Intercept the FlightSearchPoll API response with progressive results.
4.  Parse itineraries from the JSON response.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, date as date_type
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

from .browser import get_proxy

logger = logging.getLogger(__name__)


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        return datetime.fromisoformat(s.replace("Z", "").split("+")[0])
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


class MomondoConnectorClient:
    """Momondo — meta-search (Kayak/Booking Holdings), Playwright + API interception."""

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
                        "MOMONDO %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"momondo{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_mm_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("MOMONDO attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright

        api_responses: list[dict] = []

        async def on_response(response):
            url = response.url
            # Momondo polls /i/api/search/dynamic/flights/poll for results
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
            proxy = get_proxy("MOMONDO_PROXY") or get_proxy("KAYAK_PROXY")
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
            date_path = f"{dep_date}/"
            if req.return_from:
                date_path = f"{dep_date}/{req.return_from.isoformat()}/"
            url = (
                f"https://www.momondo.com/flight-search/"
                f"{req.origin}-{req.destination}/{date_path}"
                f"{req.adults or 1}adult"
                f"?sort=price_a"
            )

            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            # Momondo progressively polls for results (multiple poll rounds)
            for _ in range(10):
                await page.wait_for_timeout(3000)
                if len(api_responses) >= 2:
                    # Wait for more poll rounds
                    await page.wait_for_timeout(5000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("MOMONDO browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_responses:
            logger.warning("MOMONDO: no flight API response captured")
            return None

        # Merge all poll responses (later ones may have more results)
        return _parse_booking_holdings_poll(api_responses, req)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


def _parse_booking_holdings_poll(
    responses: list[dict],
    req: FlightSearchRequest,
    source: str = "momondo_meta",
    id_prefix: str = "mm",
    booking_base_url: str = "https://www.momondo.com/flight-search",
) -> list[FlightOffer]:
    """Parse Kayak/Momondo/Cheapflights poll responses into FlightOffer list.

    All three sites (Booking Holdings) share the same /flights/poll API:
      results[] — each has bookingOptions[] with legFarings[].legId
      legs{}    — dict keyed by composite leg ID, each has segments[].id
      segments{}— dict keyed by composite segment ID with flight details
      airlines{}— dict keyed by code with name
      airports{}— dict keyed by IATA code
    """
    target_cur = req.currency or "EUR"
    seen_ids: set[str] = set()
    offers: list[FlightOffer] = []

    # Use last (most complete) response
    data = responses[-1]
    legs_map: dict[str, dict] = data.get("legs", {})
    segs_map: dict[str, dict] = data.get("segments", {})
    airlines_map: dict[str, dict] = data.get("airlines", {})

    for result in data.get("results", []):
        try:
            if result.get("type") != "core":
                continue

            rid = result.get("resultId", "")
            if rid in seen_ids:
                continue
            seen_ids.add(rid)

            booking_options = result.get("bookingOptions") or []
            if not booking_options:
                continue

            # Use cheapest booking option
            best_option = booking_options[0]
            price_obj = best_option.get("displayPrice") or {}
            price = float(price_obj.get("price", 0))
            currency = price_obj.get("currency", target_cur)
            if price <= 0:
                continue

            leg_farings = best_option.get("legFarings") or []
            if not leg_farings:
                continue

            # Build outbound route from first leg
            outbound = _build_route(leg_farings[0], legs_map, segs_map, airlines_map, req)
            if outbound is None:
                continue

            # Build inbound route if round-trip
            inbound = None
            if len(leg_farings) > 1:
                inbound = _build_route(leg_farings[1], legs_map, segs_map, airlines_map, req)

            all_airlines = list(dict.fromkeys(
                s.airline for s in outbound.segments if s.airline
            ))

            h = hashlib.md5(f"{id_prefix}_{rid}_{price}".encode()).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"{id_prefix}_{h}",
                price=price,
                currency=currency,
                price_formatted=f"{currency} {price:.2f}",
                outbound=outbound,
                inbound=inbound,
                airlines=all_airlines,
                owner_airline=all_airlines[0] if all_airlines else "",
                source=source,
                source_tier="free",
                is_locked=False,
                booking_url=(
                    f"{booking_base_url}/"
                    f"{req.origin}-{req.destination}/{req.date_from.isoformat()}"
                ),
            ))
        except Exception as e:
            logger.warning("MOMONDO: parse result failed: %s", e)

    return offers


def _build_route(
    leg_faring: dict,
    legs_map: dict[str, dict],
    segs_map: dict[str, dict],
    airlines_map: dict[str, dict],
    req: FlightSearchRequest,
) -> FlightRoute | None:
    """Build a FlightRoute from a legFaring by looking up legs and segments."""
    leg_id = leg_faring.get("legId", "")
    leg = legs_map.get(leg_id)
    if not leg:
        return None

    flight_segments: list[FlightSegment] = []
    for seg_ref in leg.get("segments", []):
        seg_id = seg_ref.get("id", "") if isinstance(seg_ref, dict) else str(seg_ref)
        seg = segs_map.get(seg_id, {})
        if not seg:
            continue

        airline_code = seg.get("airline", "")
        airline_info = airlines_map.get(airline_code, {})
        airline_name = airline_info.get("name", airline_code)

        flight_segments.append(FlightSegment(
            airline=airline_code,
            airline_name=airline_name,
            flight_no=f"{airline_code}{seg.get('flightNumber', '')}",
            origin=seg.get("origin", req.origin),
            destination=seg.get("destination", req.destination),
            departure=_parse_dt(seg.get("departure")),
            arrival=_parse_dt(seg.get("arrival")),
        ))

    if not flight_segments:
        return None

    duration_min = leg.get("duration", 0)
    return FlightRoute(
        segments=flight_segments,
        total_duration_seconds=int(duration_min) * 60,
        stopovers=max(0, len(flight_segments) - 1),
    )
