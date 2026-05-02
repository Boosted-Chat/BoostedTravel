"""
Alliance Air connector — Paxlinks/Yii public schedule/fare surface.

Alliance Air exposes its public booking form at bookme.allianceair.in.  A
normal form POST to /search-schedule returns rendered HTML containing a
JSON-like dataSchedule payload with schedules, segment metadata, availability,
and total fare.  No credentials or private API keys are required.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

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

SOURCE_KEY = "allianceair_direct"
_BASE = "https://bookme.allianceair.in"
_SEARCH_URL = f"{_BASE}/search-schedule"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Origin": _BASE,
}


class AllianceAirConnectorClient:
    """Alliance Air public booking-schedule connector."""

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
                ob_result.offers = _combine_round_trips(ob_result.offers, ib_result.offers)
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
                warmup = await client.get(
                    _SEARCH_URL,
                    params={"org": req.origin, "des": req.destination},
                    headers={"Referer": f"{_BASE}/book"},
                )
                csrf = _extract_csrf(warmup.text)
                data = _build_form(req, csrf)
                headers = {
                    "Referer": str(warmup.url),
                    "Origin": _BASE,
                }
                if csrf:
                    headers["X-CSRF-Token"] = csrf
                resp = await client.post(_SEARCH_URL, data=data, headers=headers)
                resp.raise_for_status()
                offers = _parse_response(resp.text, req)
        except Exception as exc:
            logger.warning("Alliance Air %s→%s failed gracefully: %s", req.origin, req.destination, exc)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Alliance Air %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"allianceair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_allianceair_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers,
            total_results=len(offers),
        )


def _extract_csrf(html: str) -> str:
    match = re.search(r'<meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']', html, re.I)
    return match.group(1) if match else ""


def _build_form(req: FlightSearchRequest, csrf: str = "") -> dict[str, str]:
    form = {
        "org": req.origin,
        "des": req.destination,
        "dep_date": req.date_from.strftime("%d/%m/%Y"),
        "ret_date": req.return_from.strftime("%d/%m/%Y") if req.return_from else "",
        "pax": f"{req.adults or 1} Adult",
        "adult": str(req.adults or 1),
        "child": str(req.children or 0),
        "infant": str(req.infants or 0),
        "ccy": req.currency or "INR",
        "promo_code": "",
        "multi_route": "",
        "multi_date": "",
        "pax_category": "",
    }
    if csrf:
        form["_csrf"] = csrf
    return form


def _parse_response(html: str | dict[str, Any], req: FlightSearchRequest) -> list[FlightOffer]:
    if not html:
        return []
    if isinstance(html, str) and re.search(r"(access denied|permission denied|captcha)", html, re.I):
        return []
    payload = _extract_schedule_payload(html)
    if not payload:
        return []
    schedules = payload.get("departure_schedule") or []
    offers: list[FlightOffer] = []
    for idx, item in enumerate(schedules):
        try:
            offer = _parse_schedule_item(item, req, idx)
        except Exception:
            logger.debug("Alliance Air skipped malformed schedule item", exc_info=True)
            continue
        if offer:
            offers.append(offer)
    return offers


def _extract_schedule_payload(html: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(html, dict):
        return html
    patterns = [
        r"this\.dataSchedule\s*=\s*(\[[\s\S]*?\]);",
        r"dataSchedule\s*:\s*(\[[\s\S]*?\])\s*[,}]",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html):
            text = match.group(1)
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, list):
                for entry in parsed:
                    if isinstance(entry, dict) and "departure_schedule" in entry:
                        return entry
            if isinstance(parsed, dict):
                return parsed
    return {}


def _parse_schedule_item(item: dict[str, Any], req: FlightSearchRequest, idx: int) -> FlightOffer | None:
    routes = item.get("connecting_flight_routes") or []
    if not routes:
        return None
    segments: list[FlightSegment] = []
    for route in routes:
        origin = _nested_code(route.get("origin"))
        destination = _nested_code(route.get("destination"))
        if not origin or not destination:
            return None
        departure = _parse_paxlinks_datetime(route.get("departure_date"))
        arrival = _parse_paxlinks_datetime(route.get("arrival_date"))
        duration = max(0, int((arrival - departure).total_seconds()))
        flight_no = str(route.get("flight_number") or "").strip()
        segments.append(
            FlightSegment(
                airline="9I",
                airline_name="Alliance Air",
                flight_no=flight_no,
                origin=origin,
                destination=destination,
                origin_city=str((route.get("origin") or {}).get("city") or ""),
                destination_city=str((route.get("destination") or {}).get("city") or ""),
                departure=departure,
                arrival=arrival,
                duration_seconds=duration,
                cabin_class="economy",
                aircraft=str(route.get("aircraft") or ""),
            )
        )
    if segments[0].origin != req.origin or segments[-1].destination != req.destination:
        return None

    fare = ((item.get("fare_info") or {}).get("total_search_fare") or {})
    price = float(fare.get("amount") or fare.get("sum_amount") or 0)
    if price <= 0:
        return None
    currency = str(fare.get("ccy") or req.currency or "INR").upper()[:3]
    duration_total = int((segments[-1].arrival - segments[0].departure).total_seconds())
    fid = hashlib.md5(
        f"{SOURCE_KEY}{req.origin}{req.destination}{req.date_from}{idx}{price}{segments[0].flight_no}".encode()
    ).hexdigest()[:12]
    seats = _parse_int(routes[0].get("availability"))
    return FlightOffer(
        id=f"off_allianceair_{fid}",
        price=price,
        currency=currency,
        price_formatted=f"{currency} {price:,.0f}",
        outbound=FlightRoute(
            segments=segments,
            total_duration_seconds=max(0, duration_total),
            stopovers=max(0, len(segments) - 1),
        ),
        airlines=["Alliance Air"],
        owner_airline="Alliance Air",
        availability_seats=seats,
        booking_url=_booking_url(req),
        is_locked=False,
        source=SOURCE_KEY,
        source_tier="protocol",
    )


def _nested_code(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("code") or "").upper()
    return str(value or "").upper()


def _parse_paxlinks_datetime(value: Any) -> datetime:
    if isinstance(value, dict):
        day = int(value.get("day") or 1)
        month = int(value.get("month") or 1)
        year = int(value.get("year") or 1970)
        hour = int(value.get("hour") or 0)
        minute = int(value.get("minute") or 0)
        return datetime(year, month, day, hour, minute)
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported Alliance Air datetime: {value!r}")


def _parse_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _booking_url(req: FlightSearchRequest) -> str:
    return f"{_SEARCH_URL}?{urlencode({'org': req.origin, 'des': req.destination})}"


def _combine_round_trips(ob: list[FlightOffer], ib: list[FlightOffer]) -> list[FlightOffer]:
    combos: list[FlightOffer] = []
    for outbound in ob[:15]:
        for inbound in ib[:10]:
            price = round(outbound.price + inbound.price, 2)
            cid = hashlib.md5(f"{outbound.id}_{inbound.id}".encode()).hexdigest()[:12]
            combos.append(
                FlightOffer(
                    id=f"rt_allianceair_{cid}",
                    price=price,
                    currency=outbound.currency,
                    price_formatted=f"{outbound.currency} {price:,.0f}",
                    outbound=outbound.outbound,
                    inbound=inbound.outbound,
                    airlines=["Alliance Air"],
                    owner_airline="Alliance Air",
                    booking_url=outbound.booking_url,
                    is_locked=False,
                    source=SOURCE_KEY,
                    source_tier="protocol",
                )
            )
    combos.sort(key=lambda c: c.price)
    return combos[:20]
