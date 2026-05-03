"""
Air Busan direct API scraper — queries airbusan.com lowfare/availability API.

Air Busan (IATA: BX) is a South Korean LCC based at Gimhae International
Airport (PUS) in Busan. Subsidiary of Asiana Airlines.
Domestic routes plus international to Japan, Southeast Asia, Taiwan, Guam.

Strategy (httpx direct API — lowfare/search endpoint):
  1. POST to airbusan.com fare search API
  2. Parse JSON response → FlightOffer objects
  3. Construct booking URL
"""

from __future__ import annotations

import hashlib
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

_SEARCH_URL = "https://www.airbusan.com/api/booking/lowfare"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.airbusan.com",
    "Referer": "https://www.airbusan.com/",
}


class AirBusanConnectorClient:
    """Air Busan (BX) — Korean LCC, httpx API client."""

    def __init__(self, timeout: float = 25.0):
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
            "tripRoute": [{
                "departureDate": date_str,
                "endDate": end_str,
                "originAirport": req.origin,
                "destinationAirport": req.destination,
            }],
            "passengers": [{"type": "ADT", "count": str(req.adults or 1)}],
        }

        offers: list[FlightOffer] = []
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, proxy=get_httpx_proxy_url(),
            ) as client:
                resp = await client.post(_SEARCH_URL, headers=_HEADERS, json=payload)

            if resp.status_code != 200:
                logger.warning("Air Busan API returned %d", resp.status_code)
                return self._empty(req)

            data = resp.json()
            offers = self._parse_lowfare(data, req, date_str)

        except httpx.HTTPError as exc:
            logger.error("Air Busan request failed: %s", exc)
            return self._empty(req)
        except Exception as exc:
            logger.warning("Air Busan error: %s", exc)
            return self._empty(req)

        offers.sort(key=lambda o: o.price)

        elapsed = time.monotonic() - t0
        logger.info(
            "Air Busan %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"airbusan{req.origin}{req.destination}{date_str}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "KRW",
            offers=offers,
            total_results=len(offers),
        )

    def _parse_lowfare(
        self, data: dict, req: FlightSearchRequest, date_str: str
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        seen: set[str] = set()

        fares_container = data.get("data", data)
        if isinstance(fares_container, dict):
            lowfares = fares_container.get("lowfares", fares_container.get("lowFares", {}))
        else:
            lowfares = {}

        markets = lowfares if isinstance(lowfares, list) else lowfares.get("lowFareDateMarkets", lowfares.get("items", []))
        if not isinstance(markets, list):
            if isinstance(lowfares, dict):
                markets = list(lowfares.values()) if lowfares else []
            else:
                return offers

        for item in markets:
            if not isinstance(item, dict):
                continue
            if item.get("noFlights") or item.get("soldOut"):
                continue

            fi = item.get("lowestFareAmount", item)
            fare_amount = fi.get("fareAmount", fi.get("baseFare", 0)) or 0
            tax_amount = fi.get("taxesAndFeesAmount", fi.get("taxes", 0)) or 0

            try:
                total = round(float(fare_amount) + float(tax_amount), 2)
            except (ValueError, TypeError):
                continue
            if total <= 0:
                continue

            dep_date = item.get("date", item.get("departureDate", date_str))
            if isinstance(dep_date, str):
                dep_date = dep_date[:10]

            currency = fi.get("currencyCode", item.get("currency", "KRW"))

            dedup_key = f"{req.origin}_{req.destination}_{dep_date}_{total}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            try:
                dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
            except ValueError:
                dep_dt = datetime(2000, 1, 1)

            _bx_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline="BX",
                airline_name="Air Busan",
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=_bx_cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            fid = hashlib.md5(
                f"bx_{req.origin}{req.destination}{dep_date}{total}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"bx_{fid}",
                price=total,
                currency=currency,
                price_formatted=f"{total:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Air Busan"],
                owner_airline="BX",
                booking_url=(
                    f"https://www.airbusan.com/booking/search?"
                    f"origin={req.origin}&destination={req.destination}"
                    f"&date={dep_date}&adults={req.adults or 1}"
                ),
                is_locked=False,
                source="airbusan_direct",
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
                    id=f"rt_bx_{cid}", price=price, currency=o.currency,
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
        """Return Air Busan (BX) ancillary pricing from published tariff reference."""
        from .ancillary_ref import get_ancillary_ref
        ref = get_ancillary_ref("BX")
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
            f"airbusan{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="KRW",
            offers=[],
            total_results=0,
        )
