"""
Star Air connector — public Hitit Crane IBE availability page.

Star Air's public site delegates booking to book-sdg.crane.aero.  The search
form is credential-free and submits a GET request to /ibe/availability/create,
which returns rendered HTML containing normalized flight blocks and fare cards.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_httpx_proxy_url

logger = logging.getLogger(__name__)

SOURCE_KEY = "starair_direct"
_BASE = "https://book-sdg.crane.aero"
_SEARCH_URL = f"{_BASE}/ibe/search"
_AVAILABILITY_URL = f"{_BASE}/ibe/availability/create"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

_CABIN_PARAM = {"M": "ECONOMY", "C": "BUSINESS"}


class StarAirConnectorClient:
    """Star Air public Crane IBE connector."""

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
                search_page = await client.get(_SEARCH_URL)
                action, hidden = _extract_search_form(search_page.text)
                if not action:
                    logger.warning("Star Air search form not found")
                    return _empty_response(req)
                params = _build_params(req, hidden)
                resp = await client.get(action, params=params, headers={"Referer": str(search_page.url)})
                resp.raise_for_status()
                offers = _parse_response(resp.text, req, str(resp.url))
        except Exception as exc:
            logger.warning("Star Air %s→%s failed gracefully: %s", req.origin, req.destination, exc)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Star Air %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"starair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_starair_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers,
            total_results=len(offers),
        )


def _empty_response(req: FlightSearchRequest) -> FlightSearchResponse:
    return FlightSearchResponse(
        search_id=f"fs_starair_{hashlib.md5(f'{req.origin}{req.destination}{req.date_from}'.encode()).hexdigest()[:12]}",
        origin=req.origin,
        destination=req.destination,
        currency=req.currency,
        offers=[],
        total_results=0,
    )


def _extract_search_form(html: str) -> tuple[str, dict[str, str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    form = soup.select_one("form#availabilityForm")
    if not form:
        return "", {}
    action = str(form.get("action") or "")
    if action.startswith("/"):
        action = f"{_BASE}{action}"
    hidden: dict[str, str] = {}
    for name in ("_sid", "_cid"):
        field = form.select_one(f'input[name="{name}"]')
        if field and field.get("value"):
            hidden[name] = str(field.get("value"))
    return action, hidden


def _build_params(req: FlightSearchRequest, hidden: dict[str, str]) -> dict[str, str]:
    params = dict(hidden)
    params.update(
        {
            "tripType": "ONE_WAY",
            "inlineRadioOptions": "ONE_WAY",
            "flightRequestList[0].depPort": req.origin,
            "flightRequestList[0].arrPort": req.destination,
            "flightRequestList[0].date": req.date_from.strftime("%d %b %Y"),
            "passengerQuantities[0].passengerType": "ADLT",
            "passengerQuantities[0].quantity": str(req.adults or 1),
            "passengerQuantities[1].passengerType": "CHLD",
            "passengerQuantities[1].quantity": str(req.children or 0),
            "passengerQuantities[2].passengerType": "INFT",
            "passengerQuantities[2].quantity": str(req.infants or 0),
            "promotionCode": "",
            "accountCode": "",
            "cabinClass": _CABIN_PARAM.get(req.cabin_class or "", ""),
        }
    )
    return params


def _parse_response(html: str, req: FlightSearchRequest, booking_url: str = "") -> list[FlightOffer]:
    if not html or re.search(r"(permission denied|access denied|captcha)", html, re.I):
        return []
    soup = BeautifulSoup(html, "html.parser")
    offers: list[FlightOffer] = []
    for flight_idx, block in enumerate(soup.select(".js-scheduled-flight")):
        try:
            offers.extend(_parse_flight_block(block, req, flight_idx, booking_url or _AVAILABILITY_URL))
        except Exception:
            logger.debug("Star Air skipped malformed flight block", exc_info=True)
            continue
    return offers


def _parse_flight_block(block: Any, req: FlightSearchRequest, flight_idx: int, booking_url: str) -> list[FlightOffer]:
    text = " ".join(block.get_text(" ", strip=True).split())
    flight_no = _first_match(r"\b(S5[-\s]?\d{2,4})\b", text)
    duration_text = _first_match(r"\b(\d{1,2}\s*h\s*\d{1,2}\s*m)\b", text)
    dep_match = re.search(
        r"(\d{1,2}:\d{2}\s*(?:AM|PM)?)\s+[^()]*\(([A-Z]{3})\)\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})",
        text,
        re.I,
    )
    arr_match = None
    if dep_match:
        arr_match = re.search(
            r"\b(\d{1,2}:\d{2}\s*(?:AM|PM)?)\s+[^()]*\(([A-Z]{3})\)\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})",
            text[dep_match.end():],
            re.I,
        )
    if not dep_match or not arr_match:
        return []
    origin = dep_match.group(2).upper()
    destination = arr_match.group(2).upper()
    if origin != req.origin or destination != req.destination:
        return []

    departure = _parse_datetime(dep_match.group(3), dep_match.group(1))
    arrival = _parse_datetime(arr_match.group(3), arr_match.group(1), fallback_ampm=dep_match.group(1))
    duration = _parse_duration(duration_text) or max(0, int((arrival - departure).total_seconds()))
    fare_nodes = block.select(".fare-item.js-fare-item-selector")
    if not fare_nodes:
        fare_nodes = block.select(".fare-item")

    parsed_offers: list[FlightOffer] = []
    seen: set[tuple[str, float]] = set()
    for fare_idx, fare_node in enumerate(fare_nodes):
        fare_text = " ".join(fare_node.get_text(" ", strip=True).split())
        fare = _parse_fare(fare_text)
        if not fare:
            continue
        cabin, price, currency, seats = fare
        if req.cabin_class == "M" and cabin != "economy":
            continue
        if req.cabin_class == "C" and cabin != "business":
            continue
        key = (cabin, price)
        if key in seen:
            continue
        seen.add(key)
        fid = hashlib.md5(
            f"{SOURCE_KEY}{req.origin}{req.destination}{req.date_from}{flight_idx}{fare_idx}{price}{cabin}".encode()
        ).hexdigest()[:12]
        segment = FlightSegment(
            airline="S5",
            airline_name="Star Air",
            flight_no=flight_no.replace(" ", "-") if flight_no else "",
            origin=origin,
            destination=destination,
            departure=departure,
            arrival=arrival,
            duration_seconds=duration,
            cabin_class=cabin,
        )
        parsed_offers.append(
            FlightOffer(
                id=f"off_starair_{fid}",
                price=price,
                currency=currency,
                price_formatted=f"{currency} {price:,.0f}",
                outbound=FlightRoute(
                    segments=[segment],
                    total_duration_seconds=duration,
                    stopovers=0,
                ),
                airlines=["Star Air"],
                owner_airline="Star Air",
                availability_seats=seats,
                booking_url=booking_url,
                is_locked=False,
                source=SOURCE_KEY,
                source_tier="protocol",
            )
        )
    return parsed_offers


def _first_match(pattern: str, text: str) -> str:
    match = re.search(pattern, text, re.I)
    return match.group(1).strip() if match else ""


def _parse_datetime(date_text: str, time_text: str, fallback_ampm: str = "") -> datetime:
    time_text = time_text.strip().upper()
    if not re.search(r"\b(AM|PM)\b", time_text) and re.search(r"\b(AM|PM)\b", fallback_ampm, re.I):
        time_text = f"{time_text} {re.search(r'(AM|PM)', fallback_ampm, re.I).group(1).upper()}"
    for fmt in ("%d %b %Y %I:%M %p", "%d %b %Y %H:%M"):
        try:
            return datetime.strptime(f"{date_text} {time_text}", fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported Star Air datetime: {date_text} {time_text}")


def _parse_duration(text: str) -> int:
    match = re.search(r"(\d{1,2})\s*h\s*(\d{1,2})\s*m", text or "", re.I)
    if not match:
        return 0
    return (int(match.group(1)) * 60 + int(match.group(2))) * 60


def _parse_fare(text: str) -> tuple[str, float, str, int | None] | None:
    match = re.search(r"\b([A-Z]{3})\s*([0-9][0-9,]*(?:\.\d+)?)", text)
    if not match:
        return None
    currency = match.group(1).upper()
    price = float(match.group(2).replace(",", ""))
    if price <= 0:
        return None
    cabin = "business" if "BUSINESS" in text.upper() else "economy"
    seats_match = re.search(r"Last\s+(\d+)\s+Seats?", text, re.I)
    seats = int(seats_match.group(1)) if seats_match else None
    return cabin, price, currency, seats


def _combine_round_trips(ob: list[FlightOffer], ib: list[FlightOffer]) -> list[FlightOffer]:
    combos: list[FlightOffer] = []
    for outbound in ob[:15]:
        for inbound in ib[:10]:
            price = round(outbound.price + inbound.price, 2)
            cid = hashlib.md5(f"{outbound.id}_{inbound.id}".encode()).hexdigest()[:12]
            combos.append(
                FlightOffer(
                    id=f"rt_starair_{cid}",
                    price=price,
                    currency=outbound.currency,
                    price_formatted=f"{outbound.currency} {price:,.0f}",
                    outbound=outbound.outbound,
                    inbound=inbound.outbound,
                    airlines=["Star Air"],
                    owner_airline="Star Air",
                    booking_url=outbound.booking_url,
                    is_locked=False,
                    source=SOURCE_KEY,
                    source_tier="protocol",
                )
            )
    combos.sort(key=lambda c: c.price)
    return combos[:20]
