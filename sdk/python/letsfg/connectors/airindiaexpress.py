"""
Air India Express direct API scraper -- queries api.airindiaexpress.com REST API.

Air India Express (IATA: IX) is an Indian low-cost carrier (subsidiary of Air India).
Website: www.airindiaexpress.com

API backend: api.airindiaexpress.com (publicly accessible, subscription key required)
  - Low fare calendar: POST /b2c-flightsearch/v2/lowFares
    Headers: ocp-apim-subscription-key, Content-Type: application/json
  - Station list:   GET /b2c-flightsearch/v3/station/getSources
  - Destinations:   GET /b2c-flightsearch/v3/station/getDestinations/{IATA}

Discovered via headed-Chrome network interception, Mar 2026.
Rewritten from 496-line Playwright scraper to direct httpx API client.
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

_BASE = "https://api.airindiaexpress.com"
_LOWFARE_URL = f"{_BASE}/b2c-flightsearch/v2/lowFares"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Content-Type": "application/json",
    "ocp-apim-subscription-key": "fe65ec9eec2445d9802be1d6c0295158",
    "client-id": "AIRASIA-WEB-APP",
    "Origin": "https://www.airindiaexpress.com",
    "Referer": "https://www.airindiaexpress.com/",
}


class AirIndiaExpressConnectorClient:
    """Air India Express scraper -- direct httpx API client for low fare calendar."""

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
            "startDate": date_str,
            "endDate": end_str,
            "origin": req.origin,
            "destination": req.destination,
            "currencyCode": req.currency or "INR",
            "includeTaxesAndFees": True,
            "numberOfPassengers": req.adults,
            "fareType": "None",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout,
                proxy=get_httpx_proxy_url(),) as client:
                resp = await client.post(
                    _LOWFARE_URL, headers=_HEADERS, json=payload,
                )

                # If RT, fire second request for return leg
                inbound_resp = None
                if req.return_from:
                    ret_str = req.return_from.strftime("%Y-%m-%d")
                    ret_end = (req.return_from + timedelta(days=6)).strftime("%Y-%m-%d")
                    ret_payload = {
                        "startDate": ret_str,
                        "endDate": ret_end,
                        "origin": req.destination,
                        "destination": req.origin,
                        "currencyCode": req.currency or "INR",
                        "includeTaxesAndFees": True,
                        "numberOfPassengers": req.adults,
                        "fareType": "None",
                    }
                    inbound_resp = await client.post(
                        _LOWFARE_URL, headers=_HEADERS, json=ret_payload,
                    )
        except httpx.HTTPError as exc:
            logger.error("AirIndiaExpress API request failed: %s", exc)
            return self._empty(req)

        if resp.status_code != 200:
            logger.warning("AirIndiaExpress API returned %d", resp.status_code)
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("AirIndiaExpress API returned non-JSON response")
            return self._empty(req)

        outbound_offers = self._parse_lowfares(data, req, is_outbound=True)

        # Build RT combos if return leg available
        if req.return_from and inbound_resp and inbound_resp.status_code == 200:
            try:
                ret_data = inbound_resp.json()
            except Exception:
                ret_data = None

            if ret_data:
                from copy import copy
                ret_req = copy(req)
                ret_req.origin = req.destination
                ret_req.destination = req.origin
                ret_req.date_from = req.return_from
                inbound_offers = self._parse_lowfares(ret_data, ret_req, is_outbound=False)

                if inbound_offers:
                    booking_url = self._booking_url(req)
                    rt_offers = []
                    for ob in outbound_offers[:15]:
                        for ib in inbound_offers[:10]:
                            combined_price = round(ob.price + ib.price, 2)
                            rt_id = hashlib.md5(
                                f"aie_rt_{ob.id}_{ib.id}".encode()
                            ).hexdigest()[:12]
                            rt_offers.append(FlightOffer(
                                id=f"aie_{rt_id}",
                                price=combined_price,
                                currency=ob.currency,
                                price_formatted=f"{combined_price:.0f} {ob.currency}",
                                outbound=ob.outbound,
                                inbound=ib.outbound,
                                airlines=["Air India Express"],
                                owner_airline="IX",
                                booking_url=booking_url,
                                is_locked=False,
                                source="airindiaexpress_direct",
                                source_tier="free",
                            ))
                    rt_offers.sort(key=lambda o: o.price)
                    outbound_offers = rt_offers[:50] if rt_offers else outbound_offers

        elapsed = time.monotonic() - t0
        outbound_offers.sort(key=lambda o: o.price)
        logger.info(
            "AirIndiaExpress %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(outbound_offers), elapsed,
        )

        search_id = hashlib.md5(
            f"aie{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "INR",
            offers=outbound_offers,
            total_results=len(outbound_offers),
        )

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_lowfares(self, data: dict, req: FlightSearchRequest, is_outbound: bool = True) -> list[FlightOffer]:
        fares = data.get("lowFares", [])
        if not fares:
            return []

        booking_url = self._booking_url(req)
        offers: list[FlightOffer] = []

        for item in fares:
            if not isinstance(item, dict):
                continue
            if item.get("noFlights") or item.get("soldOut"):
                continue

            price = item.get("price", 0) or 0
            if price <= 0:
                continue

            taxes = item.get("taxesAndFees", 0) or 0
            date_raw = item.get("date", "")
            date_str = date_raw[:10] if date_raw else ""
            if not date_str:
                continue

            dep_dt = self._parse_dt(date_raw)

            fid = hashlib.md5(
                f"ix_{req.origin}{req.destination}{date_str}{price}".encode()
            ).hexdigest()[:12]

            _ix_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="IX",
                airline_name="Air India Express",
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                cabin_class=_ix_cabin,
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=0,
                stopovers=0,
            )
            offers.append(FlightOffer(
                id=f"aie_{fid}",
                price=round(price, 2),
                currency=req.currency or "INR",
                price_formatted=f"{price:.0f} {req.currency or 'INR'}",
                outbound=route,
                inbound=None,
                airlines=["Air India Express"],
                owner_airline="IX",
                booking_url=booking_url,
                is_locked=False,
                source="airindiaexpress_direct",
                source_tier="free",
                availability_seats=item.get("available"),
            ))

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        d = req.date_from.strftime("%d/%m/%Y")
        trip = "R" if req.return_from else "O"
        url = (
            f"https://www.airindiaexpress.com/booking?"
            f"origin={req.origin}&destination={req.destination}"
            f"&date={d}&adults={req.adults}&children={req.children}"
            f"&infants={req.infants}&tripType={trip}"
        )
        if req.return_from:
            rd = req.return_from.strftime("%d/%m/%Y")
            url += f"&returnDate={rd}"
        return url

    @staticmethod
    def _parse_dt(raw: str) -> Optional[datetime]:
        if not raw:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw[:19] if "T" in fmt else raw[:10], fmt)
            except (ValueError, IndexError):
                continue
        return None


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
                    id=f"rt_airi_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
    ) -> dict | None:
        """Return Air India Express (IX) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("IX")
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

    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        search_id = hashlib.md5(
            f"aie{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "INR",
            offers=[],
            total_results=0,
        )