"""
Opodo connector — Playwright browser + GraphQL API interception.

Opodo (eDreams ODIGEO group) is a major European OTA.
Same backend as eDreams — uses identical GraphQL searchItinerary API.
Also powers GoVoyages and Liligo.

Strategy: Same as eDreams — Playwright + GraphQL interception, different domain.
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

logger = logging.getLogger(__name__)


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


class OpodoConnectorClient:
    """Opodo — European OTA (ODIGEO group), Playwright + GraphQL interception."""

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
                        "OPODO %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"opodo{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_op_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("OPODO attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright

        graphql_data: list[dict] = []

        async def on_response(response):
            if "graphql" not in response.url:
                return
            try:
                if response.status == 200:
                    body = await response.text()
                    if len(body) > 50000:
                        data = json.loads(body)
                        si = data.get("data", {}).get("searchItinerary")
                        if si and si.get("itineraries"):
                            graphql_data.append(si)
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            from .browser import get_proxy
            proxy = get_proxy("OPODO_PROXY") or get_proxy("ODIGEO_PROXY")
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
            if proxy:
                from .browser import auto_block_if_proxied
                await auto_block_if_proxied(page)
            page.on("response", on_response)

            dep_date = req.date_from.isoformat()
            trip_type = "R" if req.return_from else "O"
            url = (
                f"https://www.opodo.co.uk/travel/"
                f"#results/type={trip_type}"
                f";dep={dep_date}"
                f";from={req.origin}"
                f";to={req.destination}"
                f";pa={req.adults or 1}"
                f";py=E"
            )
            if req.return_from:
                url += f";ret={req.return_from.isoformat()}"

            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)

            # Dismiss cookie consent
            for sel in [
                "#didomi-notice-agree-button",
                "button:has-text('Continue without agreeing')",
                "button:has-text('Accept')",
            ]:
                try:
                    btn = page.locator(sel)
                    if await btn.count() > 0:
                        await btn.first.click(force=True)
                        await page.wait_for_timeout(500)
                        break
                except Exception:
                    pass

            for _ in range(6):
                await page.wait_for_timeout(5000)
                if graphql_data:
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("OPODO browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not graphql_data:
            logger.warning("OPODO: no GraphQL searchItinerary captured")
            return None

        return _parse_graphql(graphql_data[0], req)

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
                    id=f"rt_op_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_graphql(si: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse Opodo/ODIGEO GraphQL searchItinerary response."""
    carrier_map: dict[str, str] = {}
    for c in si.get("carriers") or []:
        info = c.get("carrier") or {}
        carrier_map[c.get("id", "")] = info.get("name", c.get("id", ""))

    loc_map: dict[str, str] = {}
    for loc in si.get("locations") or []:
        info = loc.get("location") or {}
        loc_map[loc.get("id", "")] = info.get("iata", "")

    sec_map: dict[str, dict] = {}
    for s in si.get("sections") or []:
        sec_map[s.get("id", "")] = s.get("section") or s

    seg_map: dict[str, dict] = {}
    for s in si.get("segments") or []:
        seg_map[s.get("id", "")] = s.get("segment") or s

    offers: list[FlightOffer] = []
    for itin in si.get("itineraries") or []:
        try:
            price = 0.0
            currency = "EUR"
            for fee in itin.get("fees") or []:
                if fee.get("type") == "MEMBER_PRICE_POLICY_UNDISCOUNTED":
                    pr = fee.get("price") or {}
                    price = float(pr.get("amount", 0))
                    currency = pr.get("currency", "EUR")
                    break
            if price <= 0:
                for fee in itin.get("fees") or []:
                    pr = fee.get("price") or {}
                    amt = float(pr.get("amount", 0))
                    if amt > 0:
                        price = amt
                        currency = pr.get("currency", "EUR")
                        break
            if price <= 0:
                continue

            legs = itin.get("legs") or []
            if not legs:
                continue

            out_leg = legs[0]
            seg_id = out_leg.get("segmentId", "")
            seg_data = seg_map.get(seg_id, {})
            section_ids = seg_data.get("sections") or []
            flight_segments: list[FlightSegment] = []
            total_dur = (seg_data.get("duration") or 0) * 60

            for sec_ref in section_ids:
                sec_id = sec_ref if isinstance(sec_ref, str) else sec_ref.get("id", "")
                sec = sec_map.get(sec_id, {})
                dep_id = str(sec.get("departureId", ""))
                arr_id = str(sec.get("destinationId", ""))
                carrier_id = sec.get("carrierId", "")

                flight_segments.append(FlightSegment(
                    airline=carrier_id,
                    airline_name=carrier_map.get(carrier_id, carrier_id),
                    flight_no=f"{carrier_id}{sec.get('flightCode', '')}",
                    origin=loc_map.get(dep_id, req.origin),
                    destination=loc_map.get(arr_id, req.destination),
                    departure=_parse_dt(sec.get("departureDate")),
                    arrival=_parse_dt(sec.get("arrivalDate")),
                ))

            if not flight_segments:
                continue

            outbound = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=total_dur,
                stopovers=max(0, len(flight_segments) - 1),
            )

            inbound = None
            if len(legs) > 1:
                ret_seg_id = legs[1].get("segmentId", "")
                ret_seg_data = seg_map.get(ret_seg_id, {})
                ret_section_ids = ret_seg_data.get("sections") or []
                ret_segments: list[FlightSegment] = []
                for sec_ref in ret_section_ids:
                    sec_id = sec_ref if isinstance(sec_ref, str) else sec_ref.get("id", "")
                    sec = sec_map.get(sec_id, {})
                    cid = sec.get("carrierId", "")
                    ret_segments.append(FlightSegment(
                        airline=cid,
                        airline_name=carrier_map.get(cid, cid),
                        flight_no=f"{cid}{sec.get('flightCode', '')}",
                        origin=loc_map.get(str(sec.get("departureId", "")), req.destination),
                        destination=loc_map.get(str(sec.get("destinationId", "")), req.origin),
                        departure=_parse_dt(sec.get("departureDate")),
                        arrival=_parse_dt(sec.get("arrivalDate")),
                    ))
                if ret_segments:
                    inbound = FlightRoute(
                        segments=ret_segments,
                        total_duration_seconds=(ret_seg_data.get("duration") or 0) * 60,
                        stopovers=max(0, len(ret_segments) - 1),
                    )

            all_airlines = list(dict.fromkeys(
                s.airline for s in flight_segments if s.airline
            ))
            h = hashlib.md5(
                f"op_{itin.get('key', '')}_{price}".encode()
            ).hexdigest()[:10]

            offers.append(FlightOffer(
                id=f"op_{h}",
                price=price,
                currency=currency,
                price_formatted=f"{currency} {price:.2f}",
                outbound=outbound,
                inbound=inbound,
                airlines=all_airlines,
                owner_airline=all_airlines[0] if all_airlines else "",
                source="opodo_ota",
                source_tier="free",
                is_locked=False,
                booking_url=(
                    f"https://www.opodo.co.uk/travel/"
                    f"#results/type=O"
                    f";dep={req.date_from.isoformat()}"
                    f";from={req.origin};to={req.destination}"
                    f";pa={req.adults or 1};py=E"
                ),
            ))
        except Exception as e:
            logger.warning("OPODO: parse itinerary failed: %s", e)

    return offers
