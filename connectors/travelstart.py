"""
TravelStart connector — Direct HTTP API.

TravelStart is a leading African OTA covering South Africa, Nigeria,
Kenya, Egypt, and other African markets.

Strategy: Direct POST to /server/searchFlight/ — no browser needed.

Response formats:
- International / bundled RT → ``response.itineraries[]`` each with 2+ ODOs
- Domestic unbundled RT → ``response.outboundItineraries[]`` +
  ``response.inboundItineraries[]``, cheapest pair combined
- One-way → ``response.itineraries[]`` each with 1 ODO
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any

from .browser import get_httpx_proxy_url
from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://www.travelstart.co.za/server/searchFlight/"

_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.travelstart.co.za",
    "Referer": "https://www.travelstart.co.za/",
    "TS-country": "ZA",
    "TS-language": "en",
}


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


def _build_location(iata: str) -> dict:
    """Build a location value dict for the payload."""
    return {
        "value": {
            "type": "airport",
            "city": "",
            "airport": "",
            "iata": iata,
            "code": iata,
            "country": "",
            "countryIata": "",
            "locationId": f"airport_{iata}",
        },
        "display": iata,
    }


def _build_payload(req: FlightSearchRequest) -> dict:
    dep = req.date_from.strftime("%Y-%m-%d")
    cabin_map = {"M": "ECONOMY", "W": "PREMIUM_ECONOMY", "C": "BUSINESS", "F": "FIRST"}
    cabin = cabin_map.get(req.cabin_class or "M", "ECONOMY")

    itin: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "origin": _build_location(req.origin),
        "destination": _build_location(req.destination),
        "departDate": dep,
    }
    if req.return_from:
        itin["returnDate"] = req.return_from.strftime("%Y-%m-%d")

    return {
        "tripType": "return" if req.return_from else "oneway",
        "isNewSession": True,
        "travellers": {
            "adults": req.adults or 1,
            "youngAdults": 0,
            "children": req.children or 0,
            "infants": req.infants or 0,
        },
        "moreOptions": {
            "preferredCabins": {"display": cabin.replace("_", " ").title(), "value": cabin},
            "isCalendarSearch": False,
        },
        "outboundFlightNumber": "",
        "inboundFlightNumber": "",
        "itineraries": [itin],
        "searchIdentifier": "",
        "locale": {"country": "ZA", "currentLocale": "en", "locales": []},
        "userProfileUsername": "",
        "businessLoggedOnToken": "",
        "isDeepLink": False,
    }


class TravelstartConnectorClient:
    """TravelStart — African OTA, direct HTTP API."""

    def __init__(self, timeout: float = 40.0):
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
        import httpx

        t0 = time.monotonic()
        payload = _build_payload(req)
        cid = str(uuid.uuid4())
        url = f"{_SEARCH_URL}?correlation_id={cid}"

        for attempt in range(2):
            try:
                async with httpx.AsyncClient(
                    timeout=self.timeout, follow_redirects=True,
                    proxy=get_httpx_proxy_url(),
                ) as client:
                    resp = await client.post(url, json=payload, headers=_HEADERS)

                if resp.status_code != 200:
                    logger.warning(
                        "TRAVELSTART %s→%s: HTTP %d (attempt %d)",
                        req.origin, req.destination, resp.status_code, attempt,
                    )
                    continue

                data = resp.json()
                offers = _parse_response(data, req)
                offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                elapsed = time.monotonic() - t0
                logger.info(
                    "TRAVELSTART %s→%s: %d offers in %.1fs",
                    req.origin, req.destination, len(offers), elapsed,
                )

                h = hashlib.md5(
                    f"tst{req.origin}{req.destination}{req.date_from}".encode()
                ).hexdigest()[:12]
                return FlightSearchResponse(
                    search_id=f"fs_tst_{h}",
                    origin=req.origin,
                    destination=req.destination,
                    currency=req.currency,
                    offers=offers,
                    total_results=len(offers),
                )
            except Exception as e:
                logger.warning("TRAVELSTART attempt %d error: %s", attempt, e)

        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


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
                    id=f"rt_tst_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_response(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Route to the right parser depending on response shape."""
    resp = data.get("response") or data
    airline_names: dict[str, str] = resp.get("airlineNames") or {}
    deep_link: str = data.get("deepLinkUrl") or ""

    itineraries = resp.get("itineraries") or []
    if itineraries:
        # Bundled: one-way or international RT (each itin has all ODOs)
        return [
            o for it in itineraries
            if (o := _parse_bundled_itin(it, req, airline_names, deep_link))
        ]

    # Unbundled domestic RT: separate outbound/inbound lists
    ob_list = resp.get("outboundItineraries") or []
    ib_list = resp.get("inboundItineraries") or []
    if ob_list and ib_list:
        return _parse_unbundled_rt(ob_list, ib_list, req, airline_names, deep_link)

    if ob_list:
        return [
            o for it in ob_list
            if (o := _parse_bundled_itin(it, req, airline_names, deep_link))
        ]
    return []


def _parse_bundled_itin(
    itin: dict,
    req: FlightSearchRequest,
    airline_names: dict[str, str],
    deep_link: str,
) -> FlightOffer | None:
    """Parse a single bundled itinerary (OW or RT with multiple ODOs)."""
    try:
        price = itin.get("amount")
        if not isinstance(price, (int, float)) or price <= 0:
            return None
        currency = itin.get("currencyCode", "ZAR")

        odo_list: list[dict] = itin.get("odoList") or []
        if not odo_list:
            return None

        outbound = _parse_odo(odo_list[0], airline_names)
        if outbound is None:
            return None

        inbound = None
        if len(odo_list) >= 2:
            inbound = _parse_odo(odo_list[1], airline_names)

        all_airlines: list[str] = []
        for route in (outbound, inbound):
            if route:
                for seg in route.segments:
                    if seg.airline and seg.airline not in all_airlines:
                        all_airlines.append(seg.airline)

        owner = itin.get("odoList", [{}])[0].get("validatingCarrierCode", "")
        owner_name = airline_names.get(owner, owner) if owner else (
            all_airlines[0] if all_airlines else "TravelStart"
        )

        h = hashlib.md5(
            f"tst{itin.get('id','')}{price}{currency}".encode()
        ).hexdigest()[:12]

        booking_url = deep_link or "https://www.travelstart.co.za/"

        return FlightOffer(
            id=f"off_tst_{h}",
            price=float(price),
            currency=currency,
            outbound=outbound,
            inbound=inbound,
            airlines=all_airlines,
            owner_airline=owner_name,
            source="travelstart",
            source_tier="ota",
            booking_url=booking_url,
        )
    except Exception as e:
        logger.debug("TRAVELSTART: skipped itin: %s", e)
        return None


def _parse_unbundled_rt(
    ob_list: list[dict],
    ib_list: list[dict],
    req: FlightSearchRequest,
    airline_names: dict[str, str],
    deep_link: str,
) -> list[FlightOffer]:
    """
    Domestic unbundled RT: outbound and inbound priced separately.
    Combine each outbound with the cheapest inbound to produce offers,
    then add each inbound with the cheapest outbound (deduped).
    """
    # Sort both lists by price
    ob_sorted = sorted(ob_list, key=lambda x: x.get("amount", float("inf")))
    ib_sorted = sorted(ib_list, key=lambda x: x.get("amount", float("inf")))

    if not ob_sorted or not ib_sorted:
        return []

    cheapest_ib = ib_sorted[0]
    cheapest_ib_price = cheapest_ib.get("amount", 0)
    cheapest_ib_route = _parse_odo(
        (cheapest_ib.get("odoList") or [{}])[0], airline_names
    )

    cheapest_ob = ob_sorted[0]
    cheapest_ob_price = cheapest_ob.get("amount", 0)
    cheapest_ob_route = _parse_odo(
        (cheapest_ob.get("odoList") or [{}])[0], airline_names
    )

    offers: list[FlightOffer] = []
    seen: set[str] = set()

    # Each outbound paired with cheapest inbound
    for ob in ob_sorted[:50]:
        ob_price = ob.get("amount", 0)
        if not isinstance(ob_price, (int, float)) or ob_price <= 0:
            continue
        currency = ob.get("currencyCode", "ZAR")
        total = float(ob_price) + float(cheapest_ib_price)

        outbound = _parse_odo((ob.get("odoList") or [{}])[0], airline_names)
        if outbound is None:
            continue

        key = f"{ob.get('id')}-{cheapest_ib.get('id')}"
        if key in seen:
            continue
        seen.add(key)

        all_airlines: list[str] = []
        for route in (outbound, cheapest_ib_route):
            if route:
                for seg in route.segments:
                    if seg.airline and seg.airline not in all_airlines:
                        all_airlines.append(seg.airline)

        h = hashlib.md5(f"tst{key}{total}".encode()).hexdigest()[:12]

        offers.append(FlightOffer(
            id=f"off_tst_{h}",
            price=total,
            currency=currency,
            outbound=outbound,
            inbound=cheapest_ib_route,
            airlines=all_airlines,
            owner_airline=all_airlines[0] if all_airlines else "TravelStart",
            source="travelstart",
            source_tier="ota",
            booking_url=deep_link or "https://www.travelstart.co.za/",
        ))

    # Each inbound paired with cheapest outbound (skip if already seen)
    for ib in ib_sorted[:50]:
        ib_price = ib.get("amount", 0)
        if not isinstance(ib_price, (int, float)) or ib_price <= 0:
            continue
        currency = ib.get("currencyCode", "ZAR")
        total = float(cheapest_ob_price) + float(ib_price)

        inbound = _parse_odo((ib.get("odoList") or [{}])[0], airline_names)
        if inbound is None:
            continue

        key = f"{cheapest_ob.get('id')}-{ib.get('id')}"
        if key in seen:
            continue
        seen.add(key)

        all_airlines: list[str] = []
        for route in (cheapest_ob_route, inbound):
            if route:
                for seg in route.segments:
                    if seg.airline and seg.airline not in all_airlines:
                        all_airlines.append(seg.airline)

        h = hashlib.md5(f"tst{key}{total}".encode()).hexdigest()[:12]

        offers.append(FlightOffer(
            id=f"off_tst_{h}",
            price=total,
            currency=currency,
            outbound=cheapest_ob_route,
            inbound=inbound,
            airlines=all_airlines,
            owner_airline=all_airlines[0] if all_airlines else "TravelStart",
            source="travelstart",
            source_tier="ota",
            booking_url=deep_link or "https://www.travelstart.co.za/",
        ))

    return offers


def _parse_odo(odo: dict, airline_names: dict[str, str]) -> FlightRoute | None:
    """Parse a single ODO (origin-destination option) into a FlightRoute."""
    segments_raw = odo.get("segments") or []
    if not segments_raw:
        return None

    flight_segments: list[FlightSegment] = []
    for seg in segments_raw:
        code = seg.get("airlineCode") or seg.get("marketingAirline") or ""
        fno = seg.get("flightNumber") or ""
        name = airline_names.get(code, code)

        flight_segments.append(FlightSegment(
            airline=name,
            flight_no=str(fno),
            origin=seg.get("origCode", ""),
            destination=seg.get("destCode", ""),
            departure=_parse_dt(seg.get("departureDateTime")),
            arrival=_parse_dt(seg.get("arrivalDateTime")),
        ))

    if not flight_segments:
        return None

    dur_ms = odo.get("duration", 0)
    dur_s = int(dur_ms) // 1000 if isinstance(dur_ms, (int, float)) and dur_ms > 100_000 else (
        int(dur_ms) if isinstance(dur_ms, (int, float)) else 0
    )

    return FlightRoute(
        segments=flight_segments,
        total_duration_seconds=dur_s,
        stopovers=max(0, len(flight_segments) - 1),
    )
