"""
Cleartrip connector — India's leading OTA (Flipkart/Walmart-owned).

Covers all Indian domestic + international airlines. 261+ results per search.
Often has OTA-exclusive fares cheaper than airline websites.

Strategy:
  GET /flight/search/v2 — public JSON endpoint, just needs a cookie init
  from the homepage first (Akamai bot-manager cookies).
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date as date_type, datetime
from urllib.parse import quote

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

_BASE = "https://www.cleartrip.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# Indian airport codes (domestic detection heuristic)
_INDIAN_CODES = {
    "DEL", "BOM", "BLR", "HYD", "MAA", "CCU", "COK", "GOI", "AMD", "PNQ",
    "JAI", "LKO", "PAT", "GAU", "IXC", "SXR", "ATQ", "VNS", "NAG", "IDR",
    "BBI", "IXR", "IXB", "IXA", "DED", "VTZ", "TRZ", "CJB", "IXM", "IXJ",
    "RPR", "GAY", "IMF", "JLR", "KLH", "HBX", "HSR", "NMI",
}


def _same_country(origin: str, dest: str) -> bool:
    return origin in _INDIAN_CODES and dest in _INDIAN_CODES


def _parse_ct_time(time_str: str, fallback_date) -> datetime:
    """Parse Cleartrip time like '2026-04-01T19:55:00.000+05:30'."""
    if not time_str:
        return datetime(fallback_date.year, fallback_date.month, fallback_date.day)
    try:
        clean = time_str.split(".")[0] if "." in time_str else time_str.split("+")[0]
        return datetime.fromisoformat(clean)
    except (ValueError, IndexError):
        return datetime(fallback_date.year, fallback_date.month, fallback_date.day)


class CleartripConnectorClient:
    """Cleartrip — India's leading OTA flight search."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        offers: list[FlightOffer] = []

        try:
            async with httpx.AsyncClient(
                headers=_HEADERS, follow_redirects=True, timeout=self.timeout,
                proxy=get_httpx_proxy_url(),) as client:
                # Step 1: Cookie init — hit homepage for Akamai cookies
                await client.get(f"{_BASE}/flights")
                offers = await self._search(client, req)
        except Exception as e:
            logger.error("Cleartrip %s→%s failed: %s", req.origin, req.destination, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "Cleartrip %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"cleartrip{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_ct_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "INR",
            offers=offers,
            total_results=len(offers),
        )

    async def _search(
        self,
        client: httpx.AsyncClient,
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        is_intl = not _same_country(req.origin, req.destination)
        date_str = req.date_from.strftime("%d/%m/%Y")
        date_encoded = quote(date_str, safe="")

        search_url = (
            f"{_BASE}/flight/search/v2"
            f"?from={req.origin}&source_header={req.origin}"
            f"&to={req.destination}&destination_header={req.destination}"
            f"&depart_date={date_encoded}"
            f"&class=Economy"
            f"&adults={req.adults or 1}"
            f"&childs={req.children or 0}"
            f"&infants={req.infants or 0}"
            f"&mobileApp=true"
            f"&intl={'y' if is_intl else 'n'}"
            f"&responseType=json"
        )
        if req.return_from:
            return_date = quote(req.return_from.strftime("%d/%m/%Y"), safe="")
            search_url += f"&return_date={return_date}&trip_type=roundtrip"

        resp = await client.get(
            search_url,
            headers={
                "Accept": "application/json",
                "Referer": f"{_BASE}/flights",
            },
        )
        resp.raise_for_status()
        return _parse_response(resp.json(), req)


def _parse_response(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse Cleartrip search v2 response.

    Structure:
      cards.J1[]  — flight cards with travelOptionId, summary (dep/arr/duration)
      fares{}     — keyed by fareId → pricing.totalPricing.totalPrice
      flights{}   — keyed by flight ID → detailed flight info
      subTravelOptions{} — maps travel option → fareIds
    """
    offers: list[FlightOffer] = []
    cards_by_journey = data.get("cards", {})
    cards = cards_by_journey.get("J1", [])
    fares_map = data.get("fares", {})
    flights_map = data.get("flights", {})
    sub_options = data.get("subTravelOptions", {})

    if req.return_from:
        return _parse_round_trip_response(data, req, fares_map, flights_map)

    for card in cards:
        try:
            travel_id = card.get("travelOptionId", "")
            route, airline_codes = _build_route_from_card(
                card,
                flights_map,
                req.date_from,
                req.origin,
                req.destination,
            )

            # Get cheapest fare for this card via subTravelOptions
            price = 0.0
            currency = "INR"
            sto_ids = card.get("subTravelOptionIds", [])
            for sto_id in sto_ids:
                sto = sub_options.get(sto_id, {})
                cheapest_fid = sto.get("cheapestFareId", "")
                if cheapest_fid and cheapest_fid in fares_map:
                    fare = fares_map[cheapest_fid]
                    tp = fare.get("pricing", {}).get("totalPricing", {})
                    price = tp.get("totalPrice", 0)
                    break
                fare_ids = sto.get("fareIds", [])
                if fare_ids and fare_ids[0] in fares_map:
                    fare = fares_map[fare_ids[0]]
                    tp = fare.get("pricing", {}).get("totalPricing", {})
                    price = tp.get("totalPrice", 0)
                    break

            if price <= 0:
                continue

            h = hashlib.md5(f"ct_{travel_id}_{price}".encode()).hexdigest()[:10]
            is_intl = not _same_country(req.origin, req.destination)

            offers.append(FlightOffer(
                id=f"ct_{h}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"INR {price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=airline_codes,
                owner_airline=airline_codes[0],
                source="cleartrip_ota",
                source_tier="free",
                is_locked=False,
                booking_url=(
                    f"https://www.cleartrip.com/flights/results"
                    f"?adults={req.adults or 1}&childs={req.children or 0}"
                    f"&infants={req.infants or 0}&class=Economy"
                    f"&depart_date={req.date_from.strftime('%d/%m/%Y')}"
                    f"&from={req.origin}&to={req.destination}"
                    f"&intl={'y' if is_intl else 'n'}"
                ),
            ))

        except Exception as e:
            logger.debug("Cleartrip parse card failed: %s", e)
            continue

    return offers


def _parse_round_trip_response(
    data: dict,
    req: FlightSearchRequest,
    fares_map: dict,
    flights_map: dict,
) -> list[FlightOffer]:
    offers: list[FlightOffer] = []
    outbound_cards = {
        card.get("travelOptionId", ""): card
        for card in data.get("cards", {}).get("J1", [])
        if card.get("travelOptionId")
    }
    inbound_cards = {
        card.get("travelOptionId", ""): card
        for card in data.get("cards", {}).get("J2", [])
        if card.get("travelOptionId")
    }
    pair_to_fares = data.get("travelOptionIdsToFareIdsMap", {})
    is_intl = not _same_country(req.origin, req.destination)

    for pair_key, fare_ids in pair_to_fares.items():
        try:
            outbound_id, inbound_id = _split_pair_key(pair_key)
            outbound_card = outbound_cards.get(outbound_id)
            inbound_card = inbound_cards.get(inbound_id)
            if not outbound_card or not inbound_card:
                continue

            fare_id = next((fare_id for fare_id in fare_ids if fare_id in fares_map), None)
            if not fare_id:
                continue

            fare = fares_map[fare_id]
            total_pricing = fare.get("pricing", {}).get("totalPricing", {})
            price = total_pricing.get("totalPrice", 0)
            if price <= 0:
                continue

            outbound_route, outbound_airlines = _build_route_from_card(
                outbound_card,
                flights_map,
                req.date_from,
                req.origin,
                req.destination,
            )
            inbound_route, inbound_airlines = _build_route_from_card(
                inbound_card,
                flights_map,
                req.return_from or req.date_from,
                req.destination,
                req.origin,
            )
            airline_codes = list(dict.fromkeys(outbound_airlines + inbound_airlines)) or ["??"]

            h = hashlib.md5(f"ct_rt_{outbound_id}_{inbound_id}_{price}".encode()).hexdigest()[:10]
            offers.append(FlightOffer(
                id=f"ct_{h}",
                price=round(price, 2),
                currency="INR",
                price_formatted=f"INR {price:,.0f}",
                outbound=outbound_route,
                inbound=inbound_route,
                airlines=airline_codes,
                owner_airline=airline_codes[0],
                source="cleartrip_ota",
                source_tier="free",
                is_locked=False,
                booking_url=(
                    f"https://www.cleartrip.com/flights/results"
                    f"?adults={req.adults or 1}&childs={req.children or 0}"
                    f"&infants={req.infants or 0}&class=Economy"
                    f"&depart_date={req.date_from.strftime('%d/%m/%Y')}"
                    f"&return_date={(req.return_from or req.date_from).strftime('%d/%m/%Y')}"
                    f"&trip_type=roundtrip"
                    f"&from={req.origin}&to={req.destination}"
                    f"&intl={'y' if is_intl else 'n'}"
                ),
            ))
        except Exception as e:
            logger.debug("Cleartrip round-trip parse pair failed: %s", e)
            continue

    return offers


def _split_pair_key(pair_key: str) -> tuple[str, str]:
    trimmed = pair_key.strip()
    if trimmed.startswith("[") and trimmed.endswith("]"):
        trimmed = trimmed[1:-1]
    parts = trimmed.split(", ", 1)
    if len(parts) != 2:
        raise ValueError(f"Unexpected round-trip key: {pair_key}")
    return parts[0], parts[1]


def _build_route_from_card(
    card: dict,
    flights_map: dict,
    fallback_date: date_type,
    default_origin: str,
    default_destination: str,
) -> tuple[FlightRoute, list[str]]:
    travel_id = card.get("travelOptionId", "")
    summary = card.get("summary", {})

    first_dep = summary.get("firstDeparture", {})
    last_arr = summary.get("lastArrival", {})

    dep_airport = first_dep.get("airport", {})
    arr_airport = last_arr.get("airport", {})

    dep_code = dep_airport.get("code", default_origin)
    arr_code = arr_airport.get("code", default_destination)
    dep_time_str = dep_airport.get("time", "")
    arr_time_str = arr_airport.get("time", "")
    dep_airline = first_dep.get("airlineCode", "")

    dep_dt = _parse_ct_time(dep_time_str, fallback_date)
    arr_dt = _parse_ct_time(arr_time_str, fallback_date)

    duration = summary.get("totalDuration", {})
    dur_secs = (duration.get("hh", 0) * 3600) + (duration.get("mm", 0) * 60)
    stops = summary.get("stops", 0)

    flight_infos = summary.get("flights", [])
    segments: list[FlightSegment] = []
    airline_codes: list[str] = []
    for fi in flight_infos:
        ac = fi.get("airlineCode", "")
        fn = fi.get("flightNumber", "")
        if ac:
            airline_codes.append(ac)
        detail = None
        for fk, fv in flights_map.items():
            if fk.startswith(f"{ac}-{fn}-"):
                detail = fv
                break

        if detail:
            seg_dep = detail.get("departure", {}).get("airport", {})
            seg_arr = detail.get("arrival", {}).get("airport", {})
            seg_dep_dt = _parse_ct_time(seg_dep.get("time", ""), fallback_date)
            seg_arr_dt = _parse_ct_time(seg_arr.get("time", ""), fallback_date)
            seg_dur = detail.get("duration", {})
            seg_dur_secs = (seg_dur.get("hh", 0) * 3600) + (seg_dur.get("mm", 0) * 60)
            segments.append(FlightSegment(
                airline=ac,
                airline_name=ac,
                flight_no=f"{ac}{fn}",
                origin=seg_dep.get("code", dep_code),
                destination=seg_arr.get("code", arr_code),
                departure=seg_dep_dt,
                arrival=seg_arr_dt,
                duration_seconds=seg_dur_secs,
            ))
        else:
            segments.append(FlightSegment(
                airline=ac,
                airline_name=ac,
                flight_no=f"{ac}{fn}",
                origin=dep_code,
                destination=arr_code,
                departure=dep_dt,
                arrival=arr_dt,
            ))

    if not segments:
        parts = travel_id.split("-")
        code = parts[0] + parts[1] if len(parts) >= 2 else ""
        segments = [FlightSegment(
            airline=dep_airline,
            airline_name=dep_airline,
            flight_no=code,
            origin=dep_code,
            destination=arr_code,
            departure=dep_dt,
            arrival=arr_dt,
        )]
        if dep_airline:
            airline_codes.append(dep_airline)

    route = FlightRoute(
        segments=segments,
        total_duration_seconds=dur_secs,
        stopovers=stops,
    )
    airline_codes = list(dict.fromkeys(code for code in airline_codes if code))
    if not airline_codes and dep_airline:
        airline_codes = [dep_airline]
    return route, airline_codes
