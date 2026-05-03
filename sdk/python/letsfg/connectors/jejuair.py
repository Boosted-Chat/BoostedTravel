"""
Jeju Air direct API scraper -- queries sec.jejuair.net REST API.

Jeju Air (IATA: 7C) is a South Korean LCC based at Jeju International Airport.
Website: www.jejuair.net

API backend: sec.jejuair.net (publicly accessible, no auth/session required)
  - Lowest fare calendar: POST /en/ibe/booking/searchlowestFareCalendarInPeriod.json
    Headers: Content-Type: application/json, Channel-Code: WPC
  - Station list: POST /en/ibe/booking/selectDepartureStations.json
  - Arrival stations: POST /en/ibe/booking/selectArrivalStations.json

Discovered via network interception probes, Mar 2026.
Rewritten from 1000+ line Playwright scraper to direct httpx API client.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_httpx_proxy_url

logger = logging.getLogger(__name__)

_BASE = "https://sec.jejuair.net"
_LOWFARE_URL = f"{_BASE}/en/ibe/booking/searchlowestFareCalendarInPeriod.json"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Channel-Code": "WPC",
    "Origin": "https://www.jejuair.net",
    "Referer": "https://www.jejuair.net/",
}


class JejuAirConnectorClient:
    """Jeju Air scraper -- direct httpx API client for lowest fare calendar."""

    def __init__(self, timeout: float = 20.0):
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

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")
        end_str = (req.date_from + timedelta(days=6)).strftime("%Y-%m-%d")

        payload = {
            "tripRoute": [{
                "searchStartDate": date_str,
                "searchEndDate": end_str,
                "originAirport": req.origin,
                "destinationAirport": req.destination,
            }],
            "passengers": [{"type": "ADT", "count": str(req.adults)}],
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout,
                proxy=get_httpx_proxy_url(),) as client:
                resp = await client.post(
                    _LOWFARE_URL, headers=_HEADERS, json=payload,
                )
        except httpx.HTTPError as exc:
            logger.error("JejuAir API request failed: %s", exc)
            return self._empty(req)

        if resp.status_code != 200:
            logger.warning("JejuAir API returned %d", resp.status_code)
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("JejuAir API returned non-JSON response")
            return self._empty(req)

        if data.get("code") != "0000":
            logger.warning("JejuAir API error: %s", data.get("message", "unknown"))
            return self._empty(req)

        offers = self._parse_lowfare(data, req)

        # RT: fetch reverse route lowfare for inbound
        if req.return_from and offers:
            try:
                ret_str = req.return_from.strftime("%Y-%m-%d")
                ret_end = (req.return_from + timedelta(days=6)).strftime("%Y-%m-%d")
                ib_payload = {
                    "tripRoute": [{
                        "searchStartDate": ret_str,
                        "searchEndDate": ret_end,
                        "originAirport": req.destination,
                        "destinationAirport": req.origin,
                    }],
                    "passengers": [{"type": "ADT", "count": str(req.adults)}],
                }
                async with httpx.AsyncClient(timeout=self.timeout,
                    proxy=get_httpx_proxy_url(),) as ib_client:
                    ib_resp = await ib_client.post(
                        _LOWFARE_URL, headers=_HEADERS, json=ib_payload,
                    )
                if ib_resp.status_code == 200:
                    ib_data = ib_resp.json()
                    if ib_data.get("code") == "0000":
                        ib_lowfares = ib_data.get("data", {}).get("lowfares", {})
                        ib_markets = ib_lowfares if isinstance(ib_lowfares, list) else ib_lowfares.get("lowFareDateMarkets", [])
                        ib_best = float("inf")
                        for item in ib_markets:
                            if not isinstance(item, dict) or item.get("noFlights"):
                                continue
                            fi = item.get("lowestFareAmount", {})
                            if fi:
                                t = (fi.get("fareAmount", 0) or 0) + (fi.get("taxesAndFeesAmount", 0) or 0)
                                if 0 < t < ib_best:
                                    ib_best = t
                        if ib_best < float("inf"):
                            _7c_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                            _ret_dt = self._parse_dt(ret_str)
                            _ib_seg = FlightSegment(
                                airline="7C", airline_name="Jeju Air", flight_no="",
                                origin=req.destination, destination=req.origin,
                                departure=_ret_dt, arrival=_ret_dt, cabin_class=_7c_cabin,
                            )
                            _ib_route = FlightRoute(segments=[_ib_seg], total_duration_seconds=0, stopovers=0)
                            for _i, _o in enumerate(offers):
                                offers[_i] = FlightOffer(
                                    id=f"rt_{_o.id}",
                                    price=round(_o.price + ib_best, 2),
                                    currency=_o.currency,
                                    price_formatted=f"{round(_o.price + ib_best, 2):.0f} {_o.currency}",
                                    outbound=_o.outbound,
                                    inbound=_ib_route,
                                    airlines=_o.airlines,
                                    owner_airline=_o.owner_airline,
                                    booking_url=_o.booking_url,
                                    is_locked=False,
                                    source=_o.source,
                                    source_tier=_o.source_tier,
                                )
            except Exception:
                pass

        elapsed = time.monotonic() - t0
        offers.sort(key=lambda o: o.price)
        logger.info(
            "JejuAir %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_id = hashlib.md5(
            f"jejuair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=offers,
            total_results=len(offers),
        )

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_lowfare(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        lowfares_obj = data.get("data", {}).get("lowfares", {})
        if isinstance(lowfares_obj, list):
            # Old format: data.lowfares is a list directly
            markets = lowfares_obj
        else:
            # Current format: data.lowfares.lowFareDateMarkets
            markets = lowfares_obj.get("lowFareDateMarkets", [])

        if not markets:
            return []

        booking_url = self._booking_url(req)
        offers: list[FlightOffer] = []

        for item in markets:
            if not isinstance(item, dict):
                continue
            if item.get("noFlights"):
                continue

            fare_info = item.get("lowestFareAmount", {})
            if not fare_info:
                continue

            fare_amount = fare_info.get("fareAmount", 0) or 0
            tax_amount = fare_info.get("taxesAndFeesAmount", 0) or 0
            total = fare_amount + tax_amount
            if total <= 0:
                continue

            dep_date_raw = item.get("departureDate", "")
            dep_date_str = dep_date_raw[:10] if dep_date_raw else ""
            if not dep_date_str:
                continue

            currency = (
                lowfares_obj.get("currencyCode")
                if isinstance(lowfares_obj, dict)
                else fare_info.get("currencyCode", req.currency)
            ) or req.currency

            dep_dt = self._parse_dt(dep_date_raw)
            origin = item.get("origin", req.origin)
            dest = item.get("destination", req.destination)

            fid = hashlib.md5(
                f"7c_{origin}{dest}{dep_date_str}{total}".encode()
            ).hexdigest()[:12]

            _7c_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="7C",
                airline_name="Jeju Air",
                flight_no="",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=dep_dt,  # exact time not available from lowfare calendar
                cabin_class=_7c_cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            offers.append(FlightOffer(
                id=f"7c_{fid}",
                price=round(total, 2),
                currency=currency,
                price_formatted=f"{total:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Jeju Air"],
                owner_airline="7C",
                booking_url=booking_url,
                is_locked=False,
                source="jejuair_direct",
                source_tier="free",
            ))

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s).strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.jejuair.net/en/ibe/booking/Availability.do"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departure={dep}&adults={req.adults}&tripType={'RT' if req.return_from else 'OW'}"
        )

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
    ) -> dict | None:
        """Return Jeju Air (7C) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("7C")
        if not ref:
            return None
        carry_on = ref.get("carry_on")
        checked = ref.get("checked_bag")
        seat = ref.get("seat")
        ref_cur = ref.get("currency", "USD")

        # Carry-on note
        if note := ref.get("carry_on_note"):
            bags_note = note
        elif carry_on == 0.0:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"1 cabin bag included ({kg} kg)"
        elif carry_on is not None:
            kg = ref.get("carry_on_kg", 7)
            bags_note = f"cabin bag from ~{ref_cur} {carry_on:.0f} ({kg} kg)"
        else:
            bags_note = None

        # Checked bag note
        if note := ref.get("checked_bag_note"):
            checked_note = note
        elif checked == 0.0:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"1 checked bag included ({kg} kg)"
        elif checked is not None:
            kg = ref.get("checked_bag_kg", 20)
            checked_note = f"checked bag from ~{ref_cur} {checked:.0f} ({kg} kg)"
        else:
            checked_note = None

        # Seat note
        if seat == 0.0:
            seat_note = "seat selection included"
        elif seat is not None:
            seat_note = f"seat selection from ~{ref_cur} {seat:.0f}"
        else:
            seat_note = "seat selection available at checkout"

        return {
            "bags_from": carry_on,
            "bags_note": bags_note,
            "checked_bag_from": checked,
            "checked_bag_note": checked_note,
            "seat_from": seat,
            "seat_note": seat_note,
            "currency": ref_cur,
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
        h = hashlib.md5(
            f"jejuair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
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
                    id=f"rt_jeju_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
