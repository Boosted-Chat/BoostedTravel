"""
Etraveli connector — Direct GraphQL API to Gotogate.

Etraveli Group operates Gotogate, Mytrip, Supersaver, and Travelgenio.
All brands share the same GraphQL backend at gotogate.com/graphql.

Strategy: Direct GraphQL POST — no browser needed.
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

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min

_GQL_URL = "https://www.gotogate.com/graphql"

_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.gotogate.com",
    "Referer": "https://www.gotogate.com/",
}

_SEARCH_QUERY = """
query SearchOnResultPage($routes: [Route!]!, $adults: Int!) {
  search(routes: $routes, adults: $adults) {
    flightsCount
    flights {
      id
      tripId
      selectionKey
      type
      shareableUrl
      bounds {
        boundId: id
        segments {
          ... on TripSegment {
            __typename
            segmentId: id
            departuredAt
            arrivedAt
            origin { code name cityName }
            destination { code name cityName }
            duration
            flightNumber
            marketingCarrier { code name }
            operatingCarrier { code name }
            cabinClassName
            numberOfTechnicalStops
          }
          ... on EventSegment {
            __typename
            segmentId: id
            duration
          }
        }
      }
      travelerPricesWithoutPaymentDiscounts {
        price {
          price { value currency { code } }
        }
        travelerId
      }
    }
  }
}
"""


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


class EtraveliConnectorClient:
    """Etraveli Group (Gotogate) — Direct GraphQL API."""

    def __init__(self, timeout: float = 55.0, brand: str = "gotogate"):
        self.timeout = timeout
        self.brand = brand

    async def close(self):
        pass

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
                    id=f"rt_et_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

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
                    self._fetch_ancillaries(anc_origin, anc_dest, req.date_from.isoformat(), req.adults, ob_result.currency),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        return {
            "checked_bag": "not included – add-on from ~35 € at eDreams checkout (OTA markup; book airline direct for lower fees)",
            "bags_note": "personal item free; cabin bag add-on from ~30 € at eDreams checkout",
            "seat_note": "seat add-on from ~10 € at eDreams checkout; skip for random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_price")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            if bags_note:
                offer.conditions["cabin_bag"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["cabin_bag"] = bags_from
            if checked_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["checked_bag"] = checked_from

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
                        "ETRAVELI(%s) %s→%s: %d offers in %.1fs",
                        self.brand, req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"etraveli{self.brand}{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_et_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("ETRAVELI(%s) attempt %d failed: %s", self.brand, attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        import httpx

        routes = [
            {
                "origin": req.origin,
                "destination": req.destination,
                "departureDate": req.date_from.strftime("%Y-%m-%d"),
            }
        ]
        if req.return_from:
            routes.append(
                {
                    "origin": req.destination,
                    "destination": req.origin,
                    "departureDate": req.return_from.strftime("%Y-%m-%d"),
                }
            )

        variables = {"routes": routes, "adults": req.adults or 1}

        async with httpx.AsyncClient(timeout=self.timeout, proxy=get_httpx_proxy_url()) as client:
            resp = await client.post(
                _GQL_URL,
                json={
                    "query": _SEARCH_QUERY,
                    "operationName": "SearchOnResultPage",
                    "variables": variables,
                },
                headers=_HEADERS,
            )
            resp.raise_for_status()
            data = resp.json()

        search = (data.get("data") or {}).get("search")
        if not search:
            errs = data.get("errors", [])
            if errs:
                logger.warning("ETRAVELI GQL errors: %s", errs[0].get("message", ""))
            return None

        flights = search.get("flights") or []
        if not flights:
            return None

        is_rt = bool(req.return_from)
        return _parse_gotogate(flights, req, self.brand, is_rt)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


class TravelgenioConnectorClient(EtraveliConnectorClient):
    """Travelgenio — Etraveli brand, same backend."""

    def __init__(self, timeout: float = 55.0):
        super().__init__(timeout=timeout, brand="travelgenio")


# ---------------------------------------------------------------------------
#  Parser
# ---------------------------------------------------------------------------


def _parse_gotogate(
    flights: list[dict],
    req: FlightSearchRequest,
    brand: str,
    is_rt: bool,
) -> list[FlightOffer]:
    """Parse Gotogate GraphQL search response into FlightOffer list."""
    offers: list[FlightOffer] = []

    for trip in flights:
        try:
            # --- Price (value is in cents) ---
            tp_list = trip.get("travelerPricesWithoutPaymentDiscounts") or []
            if not tp_list:
                continue
            # Sum prices for all travelers
            total_cents = 0
            currency = "USD"
            for tp in tp_list:
                pp = (tp.get("price") or {}).get("price") or {}
                val = pp.get("value")
                if val is not None:
                    total_cents += int(val)
                cur_obj = pp.get("currency") or {}
                if cur_obj.get("code"):
                    currency = cur_obj["code"]
            price = total_cents / 100.0
            if price <= 0:
                continue

            # --- Bounds → outbound / inbound ---
            bounds = trip.get("bounds") or []
            if not bounds:
                continue

            outbound = _parse_bound(bounds[0], req.origin, req.destination)
            if not outbound:
                continue

            inbound = None
            if is_rt and len(bounds) > 1:
                inbound = _parse_bound(bounds[1], req.destination, req.origin)

            # --- Airlines ---
            all_segs = outbound.segments + (inbound.segments if inbound else [])
            airlines = list(dict.fromkeys(s.airline for s in all_segs if s.airline))

            # --- Booking URL ---
            booking_url = trip.get("shareableUrl") or f"https://www.gotogate.com/"

            # --- Unique ID ---
            sel_key = trip.get("selectionKey") or trip.get("id") or ""
            h = hashlib.md5(
                f"et{brand}{sel_key}{price}".encode()
            ).hexdigest()[:12]

            offers.append(
                FlightOffer(
                    id=f"off_et_{h}",
                    price=price,
                    currency=currency,
                    outbound=outbound,
                    inbound=inbound,
                    airlines=airlines,
                    owner_airline=airlines[0] if airlines else "Gotogate",
                    source=brand,
                    source_tier="ota",
                    booking_url=booking_url,
                )
            )
        except Exception as e:
            logger.debug("ETRAVELI: skipped trip: %s", e)
            continue

    return offers


def _parse_bound(
    bound: dict, fallback_origin: str, fallback_dest: str
) -> FlightRoute | None:
    """Parse a single bound (outbound or inbound) into FlightRoute."""
    raw_segs = bound.get("segments") or []
    trip_segs = [s for s in raw_segs if s.get("__typename") == "TripSegment"]

    if not trip_segs:
        return None

    segments: list[FlightSegment] = []
    total_dur_ms = 0

    for seg in trip_segs:
        mc = seg.get("marketingCarrier") or {}
        carrier_code = mc.get("code") or ""
        flight_no_raw = seg.get("flightNumber") or ""

        # flightNumber already includes carrier prefix (e.g. "AA306")
        flight_no = flight_no_raw if flight_no_raw else carrier_code

        orig = (seg.get("origin") or {}).get("code") or fallback_origin
        dest = (seg.get("destination") or {}).get("code") or fallback_dest

        segments.append(
            FlightSegment(
                airline=carrier_code,
                flight_no=flight_no,
                origin=orig,
                destination=dest,
                departure=_parse_dt(seg.get("departuredAt")),
                arrival=_parse_dt(seg.get("arrivedAt")),
            )
        )

        dur = seg.get("duration")
        if isinstance(dur, (int, float)):
            total_dur_ms += int(dur)

    if not segments:
        return None

    # Duration is in milliseconds → convert to seconds
    total_dur_s = total_dur_ms // 1000

    return FlightRoute(
        segments=segments,
        total_duration_seconds=total_dur_s,
        stopovers=max(0, len(segments) - 1),
    )
