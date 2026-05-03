"""
flyadeal (F3) -- EveryMundo sputnik fare search API connector.

flyadeal (IATA: F3) is a Saudi low-cost carrier, subsidiary of Saudia.
Hub at Jeddah (JED) and Riyadh (RUH) with routes across the Middle East,
South Asia, Turkey, Egypt, and East Africa.

Strategy (direct API — no browser required):
  1. POST to airTRFX sputnik fare search with EM-API-Key header
  2. Parse fare response → FlightOffer objects
  3. Construct booking URL for flyadeal.com
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import date, datetime, timedelta

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_SPUTNIK_URL = (
    "https://openair-california.airtrfx.com"
    "/airfare-sputnik-service/v3/f3/fares/search"
)
_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_HEADERS = {
    "EM-API-Key": _API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.flyadeal.com",
    "Referer": "https://www.flyadeal.com/",
}
_HOME_URL = "https://www.flyadeal.com/en"


class FlyadealConnectorClient:
    """flyadeal (F3) — EveryMundo sputnik fare search API."""

    def __init__(self, timeout: float = 25.0):
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

        days_from_now = (dt - date.today()).days
        if days_from_now < 1:
            days_from_now = 1

        is_rt = bool(req.return_from)
        # Sputnik is a low-fare calendar — search a wide window to find any available dates
        payload = {
            "origins": [req.origin],
            "destinations": [req.destination],
            "departureDaysInterval": {"min": 1, "max": 180},
            "journeyType": "ROUND_TRIP" if is_rt else "ONE_WAY",
        }

        fares = await self._call_sputnik(payload)
        offers = [
            o for o in (self._build_offer(f, req) for f in fares) if o is not None
        ]
        # Accept dates within ±180 days of requested date; sort by proximity then price
        _req_dt = dt
        offers = [
            o for o in offers
            if o.outbound
            and o.outbound.segments
            and abs((o.outbound.segments[0].departure.date() - _req_dt).days) <= 180
        ]
        offers.sort(key=lambda o: (
            abs((o.outbound.segments[0].departure.date() - _req_dt).days),
            o.price,
        ))

        elapsed = time.monotonic() - t0
        logger.info(
            "flyadeal %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        if offers:
            segs = offers[0].outbound.segments if offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, offers[0].currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s→%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s→%s: %s", anc_origin, anc_dest, _anc_err)

        h = hashlib.md5(
            f"f3{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        # flyadeal F3 — Go fare: cabin bag 7 kg free, checked bag add-on from 30 SAR
        return {
            "checked_bag_note": "checked bag 20 kg not included – add-on from ~30 SAR",
            "bags_note": "cabin bag 7 kg included free (Go fare)",
            "seat_note": "seat selection add-on from ~20 SAR",
            "checked_bag_from": 30.0,
            "currency": "SAR",
        }

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        checked_bag_note = ancillary.get("checked_bag_note")
        bags_note = ancillary.get("bags_note")
        seat_note = ancillary.get("seat_note")
        checked_bag_from = ancillary.get("checked_bag_from")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if checked_bag_note:
                offer.conditions["checked_bag"] = checked_bag_note
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if checked_bag_from is not None:
                offer.bags_price["checked_bag"] = checked_bag_from

    async def _call_sputnik(self, payload: dict) -> list[dict]:
        from curl_cffi.requests import AsyncSession

        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(3):
            try:
                async with AsyncSession(impersonate="chrome") as s:
                    r = await s.post(
                        _SPUTNIK_URL,
                        json=payload,
                        headers=_HEADERS,
                        timeout=self.timeout,
                    )

                if r.status_code == 200:
                    data = r.json()
                    return data if isinstance(data, list) else []

                logger.warning(
                    "flyadeal sputnik: status=%d attempt=%d body=%s",
                    r.status_code,
                    attempt + 1,
                    r.text[:200],
                )
                if r.status_code not in retryable_statuses or attempt == 2:
                    return []
            except Exception as e:
                logger.warning(
                    "flyadeal sputnik attempt %d failed: %s",
                    attempt + 1,
                    e,
                )
                if attempt == 2:
                    return []

            await asyncio.sleep(1.0 * (2**attempt))

        return []

    def _build_offer(
        self, fare: dict, req: FlightSearchRequest
    ) -> FlightOffer | None:
        ps = fare.get("priceSpecification", {})
        ob = fare.get("outboundFlight", {})

        price = ps.get("usdTotalPrice") or ps.get("totalPrice")
        if not price:
            return None
        try:
            price_f = round(float(price), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        if ps.get("usdTotalPrice"):
            currency = "USD"
        else:
            currency = ps.get("currencyCode") or "USD"

        dep_date_str = fare.get("departureDate", "")[:10]
        if not dep_date_str:
            return None

        origin_code = ob.get("departureAirportIataCode") or req.origin
        dest_code = ob.get("arrivalAirportIataCode") or req.destination
        cabin_input = ob.get("fareClassInput") or ob.get("fareClass") or "Economy"

        try:
            dep_dt = datetime.strptime(dep_date_str, "%Y-%m-%d")
        except ValueError:
            return None

        segment = FlightSegment(
            airline="F3",
            airline_name="flyadeal",
            flight_no="",
            origin=origin_code,
            destination=dest_code,
            origin_city="",
            destination_city="",
            departure=dep_dt,
            arrival=dep_dt,
            duration_seconds=0,
            cabin_class=cabin_input.lower() if cabin_input else "economy",
        )
        outbound = FlightRoute(
            segments=[segment], total_duration_seconds=0, stopovers=0
        )

        # Parse inbound (return) flight if present in the fare
        inbound = None
        ret_flight = fare.get("returnFlight") or fare.get("inboundFlight")
        ret_date_str = fare.get("returnDate", "")[:10] if fare.get("returnDate") else ""
        if ret_flight or ret_date_str:
            ret_origin = (ret_flight or {}).get("departureAirportIataCode") or req.destination
            ret_dest = (ret_flight or {}).get("arrivalAirportIataCode") or req.origin
            try:
                ret_dt = datetime.strptime(ret_date_str, "%Y-%m-%d") if ret_date_str else dep_dt
            except ValueError:
                ret_dt = dep_dt
            ret_seg = FlightSegment(
                airline="F3",
                airline_name="flyadeal",
                flight_no="",
                origin=ret_origin,
                destination=ret_dest,
                departure=ret_dt,
                arrival=ret_dt,
                duration_seconds=0,
                cabin_class=cabin_input.lower() if cabin_input else "economy",
            )
            inbound = FlightRoute(segments=[ret_seg], total_duration_seconds=0, stopovers=0)

        offer_hash = hashlib.md5(
            f"f3_{origin_code}_{dest_code}_{dep_date_str}_{ret_date_str}_{price_f}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"f3_{offer_hash}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=outbound,
            inbound=inbound,
            airlines=["flyadeal"],
            owner_airline="F3",
            booking_url=_HOME_URL,
            is_locked=False,
            source="flyadeal_direct",
            source_tier="free",
            conditions={
                "cabin": cabin_input or "Economy",
                "fare_note": "Fare from flyadeal EveryMundo module",
            },
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
                    id=f"rt_flya_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
