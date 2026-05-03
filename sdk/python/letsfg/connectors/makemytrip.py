"""
MakeMyTrip connector — Playwright browser + API response interception.

MakeMyTrip (NASDAQ: MMYT) is India's largest OTA. The SPA fires async
search API calls after page load. We intercept the JSON response.

Strategy:
1.  Launch Playwright browser (non-headless).
2.  Navigate to MakeMyTrip flight listing URL.
3.  Intercept JSON API responses with flight search results.
4.  Parse response into FlightOffers.
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

logger = logging.getLogger(__name__)


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except Exception:
        return datetime(2000, 1, 1)


def _parse_segments(legs: list, req: FlightSearchRequest) -> list[FlightSegment]:
    """Parse a list of segment dicts into FlightSegment objects."""
    segs: list[FlightSegment] = []
    for seg in legs:
        if not isinstance(seg, dict):
            continue
        da = seg.get("da", {})
        aa = seg.get("aa", {})
        fD = seg.get("fD", {})
        airline = fD.get("aI", {}).get("code", "") if isinstance(fD.get("aI"), dict) else str(fD.get("aI", ""))
        flight_no = fD.get("fN", "")
        if not airline:
            airline = seg.get("airline") or seg.get("carrier", "")
        origin = da.get("code", "") if isinstance(da, dict) else str(da)
        dest = aa.get("code", "") if isinstance(aa, dict) else str(aa)
        dep_time = seg.get("dt", "") or seg.get("departure", "")
        arr_time = seg.get("at", "") or seg.get("arrival", "")
        if not origin:
            origin = seg.get("origin") or seg.get("from", "")
        if not dest:
            dest = seg.get("destination") or seg.get("to", "")
        segs.append(FlightSegment(
            airline=str(airline),
            flight_no=f"{airline}{flight_no}" if flight_no else str(airline),
            origin=str(origin),
            destination=str(dest),
            departure=_parse_dt(dep_time),
            arrival=_parse_dt(arr_time),
        ))
    return segs


class MakemytripConnectorClient:
    """MakeMyTrip — India's largest OTA, Playwright + API interception."""

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
        if ob_result.offers:
            segs = ob_result.offers[0].outbound.segments if ob_result.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(
                        anc_origin, anc_dest,
                        req.date_from.isoformat() if hasattr(req.date_from, 'isoformat') else str(req.date_from),
                        req.adults, ob_result.currency or req.currency,
                    ),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s->%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error: %s", _anc_err)
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
                        "MAKEMYTRIP %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"mmt{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_mmt_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("MAKEMYTRIP attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright

        api_data: list[dict] = []

        async def on_response(response):
            url = response.url
            try:
                if response.status != 200:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                # MMT API patterns: /api/flightSearch, /flights/listing, /searchResult
                keywords = ("flightsearch", "listing", "searchresult", "search",
                            "flightlist", "itinerar")
                if not any(k in url.lower() for k in keywords):
                    return
                body = await response.text()
                if len(body) < 5000:
                    return
                data = json.loads(body)
                if isinstance(data, dict):
                    # MMT response keys
                    for key in ("searchResult", "flightResults", "itineraries",
                                "flights", "ssrResponse", "results", "offers"):
                        val = data.get(key)
                        if isinstance(val, (list, dict)):
                            if isinstance(val, list) and len(val) > 2:
                                api_data.append(data)
                                logger.info("MMT: captured %s (%d items)", key, len(val))
                                return
                            if isinstance(val, dict) and val:
                                api_data.append(data)
                                logger.info("MMT: captured %s (dict)", key)
                                return
            except Exception:
                pass

        pw = await async_playwright().start()
        try:
            from .browser import get_proxy
            proxy = get_proxy("MAKEMYTRIP_PROXY") or get_proxy("MMT_PROXY")
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

            # MakeMyTrip URL format
            dep = req.date_from.strftime("%d/%m/%Y")
            _mmt_cabin = {"M": "E", "W": "PE", "C": "B", "F": "F"}
            cabin = _mmt_cabin.get(req.cabin_class, "E") if req.cabin_class else "E"
            url = (
                f"https://www.makemytrip.com/flights/flight-listing/"
                f"?itinerary={req.origin}-{req.destination}-{dep}"
                f"&tripType=O"
                f"&paxType=A-{req.adults or 1}_C-{req.children or 0}_I-0"
                f"&cabinClass={cabin}"
                f"&sTime=1&ccde=IN&lang=eng"
            )
            if req.return_from:
                ret = req.return_from.strftime("%d/%m/%Y")
                url = url.replace("&tripType=O", "&tripType=R")
                url = url.replace(
                    f"&paxType=",
                    f"&returnDate={ret}&paxType=",
                )

            await page.goto(url, wait_until="domcontentloaded", timeout=25000)

            # Dismiss popups
            for sel in [
                "button:has-text('Accept')",
                "[data-cy='closeModal']",
                ".commonModal__close",
                "span.close",
            ]:
                try:
                    btn = page.locator(sel)
                    if await btn.count() > 0:
                        await btn.first.click(force=True, timeout=2000)
                        break
                except Exception:
                    pass

            # Wait for API response
            for _ in range(8):
                await page.wait_for_timeout(5000)
                if api_data:
                    await page.wait_for_timeout(3000)
                    break

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("MAKEMYTRIP browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not api_data:
            logger.warning("MAKEMYTRIP: no API response captured")
            return None

        return _parse_mmt(api_data[0], req)

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        """Fetch live bag/seat pricing for MakeMyTrip (OTA). Returns None - ancillary_ref.py fallback."""
        return {
            "checked_bag_note": "baggage depends on airline – full-service carriers include 15–25 kg; LCCs charge add-on from ~INR 1,000",
            "bags_note": "personal item / hand bag typically free; cabin bag varies by airline",
            "seat_note": "seat selection varies by airline; skip for free random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag_note")
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_from")
        seat_from = ancillary.get("seat_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions["checked_bag"] = checked_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from is not None:
                offer.bags_price.setdefault("carry_on", bags_from)
            if checked_from is not None:
                offer.bags_price.setdefault("checked_bag", checked_from)
            if seat_from is not None:
                offer.bags_price.setdefault("seat", seat_from)

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
                    id=f"rt_mmt_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_mmt(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse MakeMyTrip API response."""
    offers: list[FlightOffer] = []

    # Find itinerary list — MMT uses various structures
    itins = None
    # Also check for separate OB/IB groupings (tripInfos, ONWARD/RETURN)
    ob_itins = None
    ib_itins = None
    for key in ("searchResult", "flightResults", "itineraries", "flights", "results"):
        val = data.get(key)
        if isinstance(val, list) and len(val) > 0:
            itins = val
            break
        if isinstance(val, dict):
            # Check for tripInfos structure (ONWARD / RETURN)
            trip_infos = val.get("tripInfos")
            if isinstance(trip_infos, dict):
                ob_list = trip_infos.get("ONWARD") or trip_infos.get("OUTBOUND") or []
                ib_list = trip_infos.get("RETURN") or trip_infos.get("INBOUND") or []
                if isinstance(ob_list, list) and ob_list:
                    ob_itins = ob_list
                    ib_itins = ib_list if isinstance(ib_list, list) else []
                    break
            for inner_key in ("j", "journey", "flights", "itineraries"):
                inner = val.get(inner_key)
                if isinstance(inner, list) and len(inner) > 0:
                    itins = inner
                    break
            if itins or ob_itins:
                break

    if not itins and not ob_itins:
        return offers

    # If we have separate OB/IB from tripInfos, use those
    if ob_itins is not None:
        itins_ob = ob_itins
        itins_ib = ib_itins or []
    elif itins and req.return_from:
        # Classify itineraries by direction using first segment origin
        itins_ob = []
        itins_ib = []
        for itin in itins:
            if not isinstance(itin, dict):
                continue
            legs = itin.get("sI") or itin.get("legs") or itin.get("segments") or []
            if isinstance(legs, list) and legs:
                seg0 = legs[0] if isinstance(legs[0], dict) else {}
                da = seg0.get("da", {})
                first_origin = da.get("code", "") if isinstance(da, dict) else str(da)
                if not first_origin:
                    first_origin = seg0.get("origin") or seg0.get("from", "")
                if str(first_origin).upper() == req.destination.upper():
                    itins_ib.append(itin)
                else:
                    itins_ob.append(itin)
            else:
                itins_ob.append(itin)
    else:
        itins_ob = itins if itins else []
        itins_ib = []

    # Parse IB itineraries and find cheapest
    ib_route = None
    ib_price = 0.0
    if itins_ib and req.return_from:
        best_ib_price = float("inf")
        best_ib_segments = None
        best_ib_dur = 0
        for itin in itins_ib:
            if not isinstance(itin, dict):
                continue
            price_info = itin.get("totalPriceList") or itin.get("priceList") or []
            p = 0.0
            if isinstance(price_info, list) and price_info:
                p0 = price_info[0] if isinstance(price_info[0], dict) else {}
                p = float(p0.get("fd", {}).get("ADULT", {}).get("fC", {}).get("TF", 0))
            if p <= 0:
                for pkey in ("price", "totalPrice", "fare", "totalFare"):
                    val = itin.get(pkey)
                    if isinstance(val, (int, float)) and val > 0:
                        p = float(val)
                        break
            if p <= 0 or p >= best_ib_price:
                continue
            legs = itin.get("sI") or itin.get("legs") or itin.get("segments") or []
            if not isinstance(legs, list) or not legs:
                continue
            segs = _parse_segments(legs, req)
            if segs:
                best_ib_price = p
                best_ib_segments = segs
                best_ib_dur = itin.get("duration") or 0
        if best_ib_segments and best_ib_price < float("inf"):
            ib_price = best_ib_price
            d = int(best_ib_dur) * 60 if isinstance(best_ib_dur, (int, float)) and best_ib_dur < 2000 else int(best_ib_dur) if isinstance(best_ib_dur, (int, float)) else 0
            ib_route = FlightRoute(
                segments=best_ib_segments,
                total_duration_seconds=d,
                stopovers=max(0, len(best_ib_segments) - 1),
            )

    for itin in itins_ob:
        try:
            if not isinstance(itin, dict):
                continue

            # Price extraction
            price = 0.0
            currency = "INR"
            price_info = itin.get("totalPriceList") or itin.get("priceList") or []
            if isinstance(price_info, list) and price_info:
                p0 = price_info[0] if isinstance(price_info[0], dict) else {}
                price = float(p0.get("fd", {}).get("ADULT", {}).get("fC", {}).get("TF", 0))
            if price <= 0:
                for pkey in ("price", "totalPrice", "fare", "totalFare"):
                    val = itin.get(pkey)
                    if isinstance(val, (int, float)) and val > 0:
                        price = float(val)
                        break

            if price <= 0:
                continue

            # Segments
            legs = itin.get("sI") or itin.get("legs") or itin.get("segments") or []
            if not isinstance(legs, list):
                legs = [legs] if isinstance(legs, dict) else []

            flight_segments: list[FlightSegment] = []
            for seg in legs:
                if not isinstance(seg, dict):
                    continue

                # MMT segment fields
                da = seg.get("da", {})  # departure airport
                aa = seg.get("aa", {})  # arrival airport
                fD = seg.get("fD", {})  # flight details
                airline = fD.get("aI", {}).get("code", "") if isinstance(fD.get("aI"), dict) else str(fD.get("aI", ""))
                flight_no = fD.get("fN", "")

                if not airline:
                    airline = seg.get("airline") or seg.get("carrier", "")

                origin = da.get("code", "") if isinstance(da, dict) else str(da)
                dest = aa.get("code", "") if isinstance(aa, dict) else str(aa)
                dep_time = seg.get("dt", "") or seg.get("departure", "")
                arr_time = seg.get("at", "") or seg.get("arrival", "")

                if not origin:
                    origin = seg.get("origin") or seg.get("from", req.origin)
                if not dest:
                    dest = seg.get("destination") or seg.get("to", req.destination)

                flight_segments.append(FlightSegment(
                    airline=str(airline),
                    flight_no=f"{airline}{flight_no}" if flight_no else str(airline),
                    origin=str(origin),
                    destination=str(dest),
                    departure=_parse_dt(dep_time),
                    arrival=_parse_dt(arr_time),
                ))

            if not flight_segments:
                continue

            dur = itin.get("duration") or 0
            stopovers = max(0, len(flight_segments) - 1)
            outbound = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=int(dur) * 60 if isinstance(dur, (int, float)) and dur < 2000 else int(dur) if isinstance(dur, (int, float)) else 0,
                stopovers=stopovers,
            )

            airlines = list({s.airline for s in flight_segments if s.airline})
            combined = round(price + ib_price, 2) if ib_route else price
            h = hashlib.md5(
                f"mmt{req.origin}{req.destination}{combined}{airlines}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"off_mmt_rt_{h}" if ib_route else f"off_mmt_{h}",
                price=combined,
                currency=currency,
                outbound=outbound,
                inbound=ib_route,
                airlines=airlines,
                owner_airline=airlines[0] if airlines else "MakeMyTrip",
                source="makemytrip",
                source_tier="ota",
                booking_url="https://www.makemytrip.com/",
            ))
        except Exception as e:
            logger.debug("MMT: skipped itinerary: %s", e)
            continue

    return offers
