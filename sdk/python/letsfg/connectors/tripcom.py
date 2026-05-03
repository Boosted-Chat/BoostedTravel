"""
Trip.com / Ctrip connector — Playwright browser + batchSearch API interception.

Trip.com (Ctrip) is the world's largest OTA by transaction volume.
The international search page at flights.ctrip.com fires a batchSearch API
that returns 80-150+ itineraries with full pricing and segment details.

Strategy:
1.  Launch Playwright browser (non-headless for anti-bot).
2.  Navigate to flights.ctrip.com search results URL.
3.  Intercept batchSearch JSON response (~1 MB, 87+ itineraries).
4.  Parse flightItineraryList → flightSegments → flightList.
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

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
# CNY exchange rates (approximate) — updated periodically
_CNY_RATES = {
    "EUR": 0.127, "USD": 0.138, "GBP": 0.109,
    "INR": 11.58, "AUD": 0.214, "CAD": 0.192,
    "JPY": 20.4, "KRW": 189.0, "SGD": 0.184,
    "THB": 4.66, "MYR": 0.606, "CNY": 1.0,
}


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        return datetime.fromisoformat(s.replace(" ", "T"))
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


def _cny_to(amount: float, currency: str) -> float:
    """Convert CNY amount to target currency."""
    rate = _CNY_RATES.get(currency.upper())
    if rate:
        return round(amount * rate, 2)
    return round(amount * _CNY_RATES["EUR"], 2)


class TripcomConnectorClient:
    """Trip.com / Ctrip — Playwright + batchSearch API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            # When we navigate to the RT URL, _parse_ctrip already builds
            # complete RT offers (total price + inbound leg). Combining again
            # would double-count the return leg cost.
            if any(o.inbound is not None for o in ob_result.offers):
                return ob_result
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
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, ob_result.currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s→%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s→%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        # Trip.com (Ctrip) books via GDS/airline systems and generally passes through
        # the airline's own ancillary prices. A small service fee may be added per
        # booking (0–15 € depending on route). Bag and seat prices are airline-rate.
        return {
            "carry_on": (
                "cabin bag: included or extra depending on airline fare —"
                " check Trip.com checkout for exact allowance"
            ),
            "checked_bag": (
                "checked bag: extra cost — price shown at Trip.com checkout at airline rates"
                " (Trip.com may add a small service fee per booking)"
            ),
            "seat": (
                "seat selection: available at Trip.com checkout at airline’s own rates"
                " (standard economy seats often free; preferred/extra legroom cost more)"
            ),
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("carry_on") or ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag")
        seat_note = ancillary.get("seat") or ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions["checked_bag"] = checked_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["carry_on"] = bags_from

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
                        "TRIPCOM %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"tripcom{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_tc_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("TRIPCOM attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        from playwright.async_api import async_playwright
        from .browser import (
            get_proxy, auto_block_if_proxied, inject_stealth_js,
            stealth_args, stealth_position_arg,
        )

        batch_data: list[dict] = []
        pull_data: list[dict] = []

        async def on_response(response):
            url = response.url
            if "batchSearch" in url or "/pull/" in url:
                try:
                    if response.status == 200:
                        body = await response.text()
                        if len(body) > 10000:
                            data = json.loads(body)
                            itins = (data.get("data") or {}).get("flightItineraryList")
                            if itins:
                                if "batchSearch" in url:
                                    batch_data.append(data["data"])
                                else:
                                    pull_data.append(data["data"])
                except Exception:
                    pass

        pw = await async_playwright().start()
        try:
            proxy = get_proxy("TRIPCOM_PROXY")
            launch_kw: dict = {
                "headless": True,
                "args": [
                    *stealth_position_arg(),
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
            await inject_stealth_js(page)
            if proxy:
                await auto_block_if_proxied(page)
            page.on("response", on_response)

            dep_date = req.date_from.isoformat()
            trip_type = "oneway" if not req.return_from else "round"
            route = f"{req.origin.lower()}-{req.destination.lower()}"
            _tc_cabin = {"M": "y_s", "W": "s_s", "C": "c", "F": "f"}
            cabin = _tc_cabin.get(req.cabin_class, "y_s") if req.cabin_class else "y_s"
            url = (
                f"https://flights.ctrip.com/online/list/"
                f"{trip_type}-{route}"
                f"?depdate={dep_date}"
                f"&cabin={cabin}"
                f"&adult={req.adults or 1}"
                f"&child=0&infant=0"
            )
            if req.return_from:
                url += f"&rdate={req.return_from.isoformat()}"

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Wait for batchSearch + optional pull responses
            # Trip.com sometimes delays the API call — be patient
            for _ in range(12):
                await page.wait_for_timeout(3000)
                if batch_data:
                    # Give pull a chance to arrive
                    await page.wait_for_timeout(5000)
                    break
            
            # If still no data, try scrolling to trigger lazy-loaded results
            if not batch_data:
                try:
                    await page.evaluate("window.scrollBy(0, 600)")
                    await page.wait_for_timeout(5000)
                except Exception:
                    pass

            await page.close()
            await ctx.close()
            await browser.close()
        except Exception as e:
            logger.error("TRIPCOM browser error: %s", e)
            return None
        finally:
            try:
                await pw.stop()
            except Exception:
                pass

        if not batch_data:
            logger.warning("TRIPCOM: no batchSearch response captured")
            return None

        # Merge batch + pull itineraries
        all_itins = list(batch_data[0].get("flightItineraryList") or [])
        for pd in pull_data:
            all_itins.extend(pd.get("flightItineraryList") or [])

        return _parse_ctrip(all_itins, req)

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
                    id=f"rt_tc_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_ctrip(
    itins: list[dict], req: FlightSearchRequest
) -> list[FlightOffer]:
    """Parse Ctrip batchSearch flightItineraryList into FlightOffer list.

    Prices from Ctrip are in CNY. We convert to req.currency.
    """
    target_cur = req.currency or "EUR"
    offers: list[FlightOffer] = []

    for itin in itins:
        try:
            prices = itin.get("priceList") or []
            if not prices:
                continue
            p0 = prices[0]
            adult_price = float(p0.get("adultPrice", 0))
            adult_tax = float(p0.get("adultTax", 0))
            free_checked = int(p0.get("freeCheckedBaggage", 0) or 0)
            free_cabin = int(p0.get("freeCabinBaggage", 0) or 0)
            total_cny = adult_price + adult_tax
            if total_cny <= 0:
                continue

            price = _cny_to(total_cny, target_cur)

            segments_data = itin.get("flightSegments") or []
            if not segments_data:
                continue

            # Build outbound from first segment
            out_seg = segments_data[0]
            out_flights = out_seg.get("flightList") or []
            if not out_flights:
                continue

            flight_segments: list[FlightSegment] = []
            for fl in out_flights:
                airline_code = fl.get("marketAirlineCode", "")
                operate_code = fl.get("operateAirlineCode", "")
                airline_name = fl.get("marketAirlineName", "")
                # Prefer English names; Ctrip often returns Chinese
                if airline_name and ord(airline_name[0]) > 127:
                    airline_name = operate_code or airline_code

                flight_segments.append(FlightSegment(
                    airline=airline_code,
                    airline_name=airline_name,
                    flight_no=fl.get("flightNo", ""),
                    origin=fl.get("departureAirportCode", req.origin),
                    destination=fl.get("arrivalAirportCode", req.destination),
                    departure=_parse_dt(fl.get("departureDateTime")),
                    arrival=_parse_dt(fl.get("arrivalDateTime")),
                ))

            total_dur = (out_seg.get("duration") or 0) * 60  # min → sec
            stopovers = out_seg.get("transferCount", 0)
            outbound = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=total_dur,
                stopovers=stopovers,
            )

            # Build inbound from second segment (if round-trip)
            inbound = None
            if len(segments_data) > 1:
                ret_seg = segments_data[1]
                ret_flights = ret_seg.get("flightList") or []
                ret_segments: list[FlightSegment] = []
                for fl in ret_flights:
                    ac = fl.get("marketAirlineCode", "")
                    an = fl.get("marketAirlineName", "")
                    if an and ord(an[0]) > 127:
                        an = fl.get("operateAirlineCode", "") or ac
                    ret_segments.append(FlightSegment(
                        airline=ac,
                        airline_name=an,
                        flight_no=fl.get("flightNo", ""),
                        origin=fl.get("departureAirportCode", req.destination),
                        destination=fl.get("arrivalAirportCode", req.origin),
                        departure=_parse_dt(fl.get("departureDateTime")),
                        arrival=_parse_dt(fl.get("arrivalDateTime")),
                    ))
                if ret_segments:
                    inbound = FlightRoute(
                        segments=ret_segments,
                        total_duration_seconds=(ret_seg.get("duration") or 0) * 60,
                        stopovers=ret_seg.get("transferCount", 0),
                    )

            all_airlines = list(dict.fromkeys(
                s.airline for s in flight_segments if s.airline
            ))
            owner = flight_segments[0].airline if flight_segments else ""

            itin_id = itin.get("itineraryId", "")
            h = hashlib.md5(
                f"tc_{itin_id}_{total_cny}".encode()
            ).hexdigest()[:10]

            _tc_offer = FlightOffer(
                id=f"tc_{h}",
                price=price,
                currency=target_cur,
                price_formatted=f"{target_cur} {price:.2f}",
                outbound=outbound,
                inbound=inbound,
                airlines=all_airlines,
                owner_airline=owner,
                source="tripcom_ota",
                source_tier="free",
                is_locked=False,
                booking_url=(
                    f"https://www.trip.com/flights/"
                    f"{req.origin.lower()}-to-{req.destination.lower()}/"
                    f"tickets-{req.origin.lower()}-{req.destination.lower()}"
                ),
            )
            if free_checked == 0 and free_cabin == 0:
                _tc_offer.conditions["carry_on"] = "No free baggage included"
            elif free_checked > 0:
                _tc_offer.conditions["carry_on"] = f"{free_checked} free checked bag(s) included"
            if free_cabin > 0:
                _tc_offer.conditions["carry_on"] = f"{free_cabin} free cabin bag(s) included"
            offers.append(_tc_offer)
        except Exception as e:
            logger.warning("TRIPCOM: parse itinerary failed: %s", e)

    return offers
