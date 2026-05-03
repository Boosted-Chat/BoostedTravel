"""
Lion Air connector — Indonesia's largest LCC, Navitaire PSS.

Lion Air (IATA: JT) is based at Jakarta Soekarno-Hatta (CGK) with a fleet of 100+
aircraft. Major domestic network across Indonesia plus international routes to
Malaysia, Singapore, Thailand, Vietnam, China, Saudi Arabia, and more.

Strategy (httpx direct API — Navitaire lowfare calendar):
  1. POST to lionair.co.id Navitaire lowfare endpoint
  2. Parse JSON fare calendar → FlightOffer objects
  3. Construct booking URL
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import date, datetime, timedelta
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

_SEARCH_URL = "https://www.lionair.co.id/api/booking/lowfare"
_AVAIL_URL = "https://www.lionair.co.id/api/booking/availability"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.lionair.co.id",
    "Referer": "https://www.lionair.co.id/",
}


class LionAirConnectorClient:
    """Lion Air (JT) — Indonesia's largest LCC, Navitaire-based API."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination, "destination": req.origin,
                "date_from": req.return_from, "return_from": None,
            })
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

        try:
            dt = (
                req.date_from
                if isinstance(req.date_from, (datetime, date))
                else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            )
            if isinstance(dt, datetime):
                dt = dt.date()
        except (ValueError, TypeError):
            dt = date.today() + timedelta(days=30)

        date_str = dt.strftime("%Y-%m-%d")
        end_str = (dt + timedelta(days=6)).strftime("%Y-%m-%d")

        payload = {
            "origin": req.origin,
            "destination": req.destination,
            "departureDate": date_str,
            "endDate": end_str,
            "adults": req.adults or 1,
            "children": req.children or 0,
            "infants": req.infants or 0,
            "currencyCode": "IDR",
            "promoCode": "",
        }

        offers: list[FlightOffer] = []
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, proxy=get_httpx_proxy_url(),
            ) as client:
                resp = await client.post(_SEARCH_URL, headers=_HEADERS, json=payload)

            if resp.status_code == 200:
                data = resp.json()
                offers = self._parse_lowfare(data, req, date_str)
            else:
                logger.warning("Lion Air lowfare returned %d, trying availability", resp.status_code)
                # Fallback: try availability endpoint
                avail_payload = {
                    "journeys": [{
                        "origin": req.origin,
                        "destination": req.destination,
                        "departureDate": date_str,
                    }],
                    "passengers": {
                        "adults": req.adults or 1,
                        "children": req.children or 0,
                        "infants": req.infants or 0,
                    },
                    "currencyCode": "IDR",
                }
                async with httpx.AsyncClient(
                    timeout=self.timeout, proxy=get_httpx_proxy_url(),
                ) as client:
                    resp2 = await client.post(_AVAIL_URL, headers=_HEADERS, json=avail_payload)
                if resp2.status_code == 200:
                    data2 = resp2.json()
                    offers = self._parse_availability(data2, req, date_str)
                else:
                    logger.warning("Lion Air availability returned %d", resp2.status_code)

        except httpx.HTTPError as exc:
            logger.error("Lion Air request failed: %s", exc)
            return self._empty(req)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("Lion Air JSON parse failed: %s", exc)
            return self._empty(req)

        offers.sort(key=lambda o: o.price)

        elapsed = time.monotonic() - t0
        logger.info(
            "Lion Air %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"lionair{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "IDR",
            offers=offers,
            total_results=len(offers),
        )

    def _parse_lowfare(
        self, data: dict, req: FlightSearchRequest, date_str: str
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        fares = data.get("fares", data.get("lowFares", data.get("data", [])))
        if isinstance(fares, dict):
            fares = fares.get("items", fares.get("fares", []))
        if not isinstance(fares, list):
            return offers

        for fare in fares:
            if not isinstance(fare, dict):
                continue
            if fare.get("soldOut") or fare.get("noFlights"):
                continue

            price = (
                fare.get("totalFare")
                or fare.get("total")
                or fare.get("amount")
                or fare.get("fareAmount")
            )
            if isinstance(price, dict):
                price = price.get("amount", price.get("totalAmount"))

            if not price:
                continue
            try:
                price_f = round(float(price), 2)
            except (ValueError, TypeError):
                continue
            if price_f <= 0:
                continue

            currency = fare.get("currencyCode", fare.get("currency", "IDR"))
            dep_date = fare.get("date", fare.get("departureDate", date_str))
            if isinstance(dep_date, str):
                dep_date = dep_date[:10]

            dedup_key = f"{req.origin}_{req.destination}_{dep_date}_{price_f}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            try:
                dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
            except (ValueError, TypeError):
                dep_dt = datetime(2000, 1, 1)

            _jt_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="JT",
                airline_name="Lion Air",
                flight_no=fare.get("flightNumber", ""),
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=_jt_cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            fid = hashlib.md5(
                f"jt_{req.origin}{req.destination}{dep_date}{price_f}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"jt_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=f"{price_f:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Lion Air"],
                owner_airline="JT",
                booking_url=(
                    f"https://www.lionair.co.id/booking/search?"
                    f"origin={req.origin}&destination={req.destination}"
                    f"&departDate={dep_date}&adults={req.adults or 1}"
                ),
                is_locked=False,
                source="lionair_direct",
                source_tier="free",
            ))

        return offers

    def _parse_availability(
        self, data: dict, req: FlightSearchRequest, date_str: str
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        journeys = data.get("journeys", data.get("data", data.get("trips", [])))
        if isinstance(journeys, dict):
            journeys = journeys.get("items", [])
        if not isinstance(journeys, list):
            return offers

        for journey in journeys:
            if not isinstance(journey, dict):
                continue
            fares = journey.get("fares", journey.get("fareAvailability", []))
            if isinstance(fares, dict):
                fares = list(fares.values())
            if not isinstance(fares, list):
                continue

            segments_data = journey.get("segments", journey.get("legs", []))

            for fare in fares:
                if not isinstance(fare, dict):
                    continue
                price = fare.get("passengerFares", [{}])
                if isinstance(price, list) and price:
                    price = sum(
                        float(pf.get("fareAmount", 0)) + float(pf.get("serviceCharges", 0))
                        for pf in price
                        if isinstance(pf, dict)
                    )
                elif isinstance(price, (int, float)):
                    pass
                else:
                    continue

                try:
                    price_f = round(float(price), 2)
                except (ValueError, TypeError):
                    continue
                if price_f <= 0:
                    continue

                dep_date = date_str
                dedup_key = f"{req.origin}_{req.destination}_{dep_date}_{price_f}"
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                try:
                    dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
                except ValueError:
                    dep_dt = datetime(2000, 1, 1)

                _jt_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                seg = FlightSegment(
                    airline="JT",
                    airline_name="Lion Air",
                    flight_no="",
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_dt,
                    arrival=dep_dt,
                    duration_seconds=0,
                    cabin_class=_jt_cabin,
                )
                route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

                fid = hashlib.md5(
                    f"jt_{req.origin}{req.destination}{dep_date}{price_f}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"jt_{fid}",
                    price=price_f,
                    currency="IDR",
                    price_formatted=f"{price_f:.0f} IDR",
                    outbound=route,
                    inbound=None,
                    airlines=["Lion Air"],
                    owner_airline="JT",
                    booking_url=(
                        f"https://www.lionair.co.id/booking/search?"
                        f"origin={req.origin}&destination={req.destination}"
                        f"&departDate={dep_date}&adults={req.adults or 1}"
                    ),
                    is_locked=False,
                    source="lionair_direct",
                    source_tier="free",
                ))

        return offers

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
                    id=f"rt_lion_{cid}", price=price, currency=o.currency,
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
        """Return Lion Air (JT) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("JT")
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
        h = hashlib.md5(
            f"lionair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="IDR",
            offers=[],
            total_results=0,
        )
