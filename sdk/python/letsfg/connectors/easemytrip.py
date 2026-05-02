"""
EaseMyTrip connector — public AirBus_New JSON endpoint.

EaseMyTrip's current Angular flight-search surface posts directly to a
credential-free JSON endpoint on flightservice-node.easemytrip.com.  The
endpoint returns normalized journey/segment dictionaries keyed by flight index,
which is more stable than scraping the rendered listing DOM.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .airline_routes import get_country
from .browser import get_httpx_proxy_url

logger = logging.getLogger(__name__)

SOURCE_KEY = "easemytrip_ota"
_BASE = "https://www.easemytrip.com"
_API_URL = "https://flightservice-node.easemytrip.com/AirAvail_Lights/AirBus_New"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": _BASE,
    "Referer": f"{_BASE}/flight-search/listing",
}

_CABIN_MAP = {"M": 0, "F": 1, "C": 2, "W": 4}


def _parse_duration_to_seconds(value: Any) -> int:
    """Parse EaseMyTrip durations like '02h 05m' or minute counts."""
    if isinstance(value, (int, float)):
        minutes = int(value)
        return minutes * 60 if minutes < 24 * 60 else minutes
    text = str(value or "").strip()
    if not text:
        return 0
    hours = 0
    minutes = 0
    h_match = re.search(r"(\d+)\s*h", text, re.IGNORECASE)
    m_match = re.search(r"(\d+)\s*m", text, re.IGNORECASE)
    if h_match:
        hours = int(h_match.group(1))
    if m_match:
        minutes = int(m_match.group(1))
    if hours or minutes:
        return (hours * 60 + minutes) * 60
    try:
        return int(float(text)) * 60
    except ValueError:
        return 0


def _parse_segment_datetime(date_value: Any, time_value: Any, fallback: date) -> datetime:
    """Parse EaseMyTrip segment dates such as 'Mon-15Jun2026' + '17:35'."""
    time_text = str(time_value or "00:00").strip() or "00:00"
    date_text = str(date_value or "").strip()
    candidates: list[str] = []
    if date_text:
        candidates.extend([
            f"{date_text} {time_text}",
            f"{date_text.split('-', 1)[-1]} {time_text}" if "-" in date_text else "",
        ])
    candidates.append(f"{fallback.strftime('%d%b%Y')} {time_text}")

    for candidate in candidates:
        if not candidate:
            continue
        for fmt in ("%a-%d%b%Y %H:%M", "%d%b%Y %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue
    return datetime(fallback.year, fallback.month, fallback.day)


def _is_india_route(origin: str, destination: str) -> bool:
    return get_country(origin) == "IN" or get_country(destination) == "IN"


def _city_label(code: str) -> str:
    return code.upper()


def _build_listing_url(req: FlightSearchRequest) -> str:
    dep = req.date_from.strftime("%d/%m/%Y")
    ret = f"-{req.return_from.strftime('%d/%m/%Y')}" if req.return_from else ""
    cabin = _CABIN_MAP.get(req.cabin_class or "M", 0)
    srch = (
        f"{req.origin}-{_city_label(req.origin)}-India|"
        f"{req.destination}-{_city_label(req.destination)}-India|"
        f"{dep}{ret}"
    )
    return (
        f"{_BASE}/flight-search/listing?srch={quote(srch, safe='|-')}"
        f"&px={req.adults or 1}-{req.children or 0}-{req.infants or 0}"
        f"&cbn={cabin}&ar=undefined"
        f"&isow={'false' if req.return_from else 'true'}"
        f"&isdm={'true' if _is_india_route(req.origin, req.destination) else 'false'}"
        f"&lang=en-us&CCODE=IN&curr=INR&apptype=B2C"
    )


class EasemytripConnectorClient:
    """EaseMyTrip India OTA via credential-free JSON search endpoint."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self) -> None:
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(
                update={
                    "origin": req.destination,
                    "destination": req.origin,
                    "date_from": req.return_from,
                    "return_from": None,
                }
            )
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        offers: list[FlightOffer] = []
        try:
            async with httpx.AsyncClient(
                headers=_HEADERS,
                timeout=self.timeout,
                follow_redirects=True,
                proxy=get_httpx_proxy_url(),
            ) as client:
                # Cookie warm-up is not strictly required for the node endpoint,
                # but keeps behavior aligned with the public web surface.
                try:
                    await client.get(f"{_BASE}/flights.html")
                except Exception:
                    pass
                resp = await client.post(
                    f"{_API_URL}?_={int(time.time() * 1000)}",
                    json=_build_payload(req),
                )
                resp.raise_for_status()
                content_type = resp.headers.get("content-type", "")
                if "json" not in content_type and not resp.text.strip().startswith("{"):
                    logger.warning("EaseMyTrip returned non-JSON response: %s", content_type)
                    data: dict[str, Any] = {}
                else:
                    data = resp.json()
                offers = _parse_response(data, req)
        except Exception as exc:
            logger.warning("EaseMyTrip %s→%s failed gracefully: %s", req.origin, req.destination, exc)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("EaseMyTrip %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"emt{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_emt_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _combine_rt(ob: list[FlightOffer], ib: list[FlightOffer]) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for outbound in ob[:15]:
            for inbound in ib[:10]:
                price = round(outbound.price + inbound.price, 2)
                cid = hashlib.md5(f"{outbound.id}_{inbound.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_emt_{cid}",
                    price=price,
                    currency=outbound.currency,
                    price_formatted=f"{outbound.currency} {price:,.0f}",
                    outbound=outbound.outbound,
                    inbound=inbound.outbound,
                    airlines=list(dict.fromkeys(outbound.airlines + inbound.airlines)),
                    owner_airline=outbound.owner_airline,
                    booking_url=outbound.booking_url,
                    is_locked=False,
                    source=SOURCE_KEY,
                    source_tier="ota",
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]


def _build_payload(req: FlightSearchRequest) -> dict[str, Any]:
    is_domestic = get_country(req.origin) == "IN" and get_country(req.destination) == "IN"
    trace = hashlib.md5(f"{req.origin}{req.destination}{req.date_from}{time.time()}".encode()).hexdigest()
    return {
        "org": req.origin,
        "dept": req.destination,
        "adt": req.adults or 1,
        "chd": req.children or 0,
        "inf": req.infants or 0,
        "deptDT": req.date_from.strftime("%Y-%m-%d"),
        "arrDT": "",
        "userid": "",
        "IsDoubelSeat": False,
        "isDomestic": is_domestic,
        "isOneway": True,
        "airline": "undefined",
        "Cabin": _CABIN_MAP.get(req.cabin_class or "M", 0),
        "currCode": req.currency if req.currency else "INR",
        "appType": 1,
        "isSingleView": False,
        "ResType": 2,
        "IsNBA": False,
        "CouponCode": "",
        "IsArmedForce": False,
        "AgentCode": "",
        "IsWLAPP": False,
        "IsFareFamily": False,
        "serviceid": "EMTSERVICE",
        "serviceDepatment": "",
        "IpAddress": "",
        "LoginKey": "",
        "UUID": "",
        "TKN": "",
        "TraceId": trace,
        "queryname": trace,
        "FareTypeUI": 0,
    }


def _parse_response(data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse EaseMyTrip AirBus_New JSON into normalized offers."""
    if not isinstance(data, dict):
        return []
    journeys = data.get("j")
    if not isinstance(journeys, list) or not journeys:
        return []
    details = data.get("dctFltDtl") if isinstance(data.get("dctFltDtl"), dict) else {}
    carriers = data.get("C") if isinstance(data.get("C"), dict) else {}
    currency = str(data.get("CC") or req.currency or "INR").upper()[:3]
    offers: list[FlightOffer] = []

    for journey in journeys:
        if not isinstance(journey, dict):
            continue
        raw_offers = journey.get("s")
        if not isinstance(raw_offers, list):
            continue
        for raw in raw_offers:
            offer = _parse_offer(raw, details, carriers, currency, req)
            if offer is not None:
                offers.append(offer)
    return offers


def _parse_offer(
    raw: Any,
    details: dict[Any, Any],
    carriers: dict[str, str],
    currency: str,
    req: FlightSearchRequest,
) -> FlightOffer | None:
    if not isinstance(raw, dict):
        return None
    price = raw.get("TF") or raw.get("TotalFare") or raw.get("AdultPrice")
    try:
        price_float = float(price)
    except (TypeError, ValueError):
        return None
    if price_float <= 0:
        return None

    bounds = raw.get("b")
    if not isinstance(bounds, list) or not bounds:
        return None
    outbound = _parse_bound(bounds[0], details, carriers, req)
    if outbound is None:
        return None
    # EaseMyTrip sometimes maps city/alternate airports in a metro area. Keep
    # parser strict so global wrong-route filtering is not asked to clean up
    # clearly unrelated endpoint results.
    if (
        outbound.segments[0].origin.upper() != req.origin.upper()
        or outbound.segments[-1].destination.upper() != req.destination.upper()
    ):
        return None

    airlines = list(dict.fromkeys(
        seg.airline_name or carriers.get(seg.airline, "") or seg.airline
        for seg in outbound.segments
        if seg.airline or seg.airline_name
    ))
    if not airlines:
        airlines = ["EaseMyTrip"]
    raw_id = str(raw.get("SK") or raw.get("id") or raw.get("ItineraryKey") or price_float)
    h = hashlib.md5(
        f"emt_{req.origin}_{req.destination}_{raw_id}_{price_float}_{outbound.segments[0].departure}".encode()
    ).hexdigest()[:12]

    return FlightOffer(
        id=f"off_emt_{h}",
        price=round(price_float, 2),
        currency=currency,
        price_formatted=f"{currency} {price_float:,.0f}",
        outbound=outbound,
        inbound=None,
        airlines=airlines,
        owner_airline=airlines[0],
        source=SOURCE_KEY,
        source_tier="ota",
        is_locked=False,
        booking_url=_build_listing_url(req),
        conditions={
            "refund_before_departure": "allowed" if raw.get("Refundable") else "unknown",
            "change_before_departure": "unknown",
        },
    )


def _parse_bound(
    bound: Any,
    details: dict[Any, Any],
    carriers: dict[str, str],
    req: FlightSearchRequest,
) -> FlightRoute | None:
    if not isinstance(bound, dict):
        return None
    flight_ids = bound.get("FL")
    if not isinstance(flight_ids, list) or not flight_ids:
        return None
    segments: list[FlightSegment] = []
    for flight_id in flight_ids:
        raw_detail = details.get(str(flight_id), details.get(flight_id))
        if not isinstance(raw_detail, dict):
            continue
        seg = _parse_segment(raw_detail, carriers, req)
        if seg is not None:
            segments.append(seg)
    if not segments:
        return None

    total_duration = (
        _parse_duration_to_seconds(bound.get("JyTm"))
        or sum(seg.duration_seconds for seg in segments)
        or _parse_duration_to_seconds(bound.get("TotalJyTm"))
    )
    stopovers = max(0, len(segments) - 1)
    try:
        stopovers = int(bound.get("stp", stopovers))
    except (TypeError, ValueError):
        pass
    return FlightRoute(
        segments=segments,
        total_duration_seconds=total_duration,
        stopovers=stopovers,
    )


def _parse_segment(
    raw: dict[str, Any],
    carriers: dict[str, str],
    req: FlightSearchRequest,
) -> FlightSegment | None:
    origin = str(raw.get("OG") or "").upper()
    destination = str(raw.get("DT") or "").upper()
    airline = str(raw.get("AC") or "").strip().upper()
    flight_no = str(raw.get("FN") or "").strip()
    if not origin or not destination or not airline:
        return None
    dep = _parse_segment_datetime(raw.get("DDT"), raw.get("DTM"), req.date_from)
    arr = _parse_segment_datetime(raw.get("ADT") or raw.get("DDT"), raw.get("ATM"), req.date_from)
    duration = _parse_duration_to_seconds(raw.get("DUR"))
    return FlightSegment(
        airline=airline,
        airline_name=str(raw.get("FlightName") or carriers.get(airline, "")),
        flight_no=f"{airline}{flight_no}".replace(" ", "") if flight_no else airline,
        origin=origin,
        destination=destination,
        departure=dep,
        arrival=arr,
        duration_seconds=duration,
        cabin_class=str(raw.get("CB") or "economy").lower(),
        aircraft=str(raw.get("ET") or raw.get("equipment") or ""),
    )
