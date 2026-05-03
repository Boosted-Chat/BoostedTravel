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
from datetime import datetime
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
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
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
                        req.date_from.isoformat() if hasattr(req.date_from, "isoformat") else str(req.date_from),
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

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        """Fetch live bag/seat pricing for Cleartrip (OTA). Returns None - ancillary_ref.py fallback."""
        return {
            "checked_bag_note": "baggage depends on airline – full-service carriers include 15–25 kg; LCCs charge add-on from ~INR 1,000",
            "bags_note": "personal item / hand bag typically free; cabin bag varies by airline",
            "seat_note": "seat selection varies by airline; skip for free random seat",
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

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        offers: list[FlightOffer] = []

        try:
            async with httpx.AsyncClient(
                headers=_HEADERS, follow_redirects=True, timeout=self.timeout,
                proxy=get_httpx_proxy_url(),) as client:
                # Step 1: Cookie init — hit homepage for Akamai cookies
                await client.get(f"{_BASE}/flights")

                # Step 2: Search via v2 GET endpoint
                is_intl = not _same_country(req.origin, req.destination)
                date_str = req.date_from.strftime("%d/%m/%Y")
                date_encoded = quote(date_str, safe="")
                _ct_cabin = {"M": "Economy", "W": "Premium Economy", "C": "Business", "F": "First"}.get(req.cabin_class, "Economy") if req.cabin_class else "Economy"

                search_url = (
                    f"{_BASE}/flight/search/v2"
                    f"?from={req.origin}&source_header={req.origin}"
                    f"&to={req.destination}&destination_header={req.destination}"
                    f"&depart_date={date_encoded}"
                    f"&class={_ct_cabin}"
                    f"&adults={req.adults or 1}"
                    f"&childs={req.children or 0}"
                    f"&infants={req.infants or 0}"
                    f"&mobileApp=true"
                    f"&intl={'y' if is_intl else 'n'}"
                    f"&responseType=json"
                )

                resp = await client.get(
                    search_url,
                    headers={
                        "Accept": "application/json",
                        "Referer": f"{_BASE}/flights",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                offers = _parse_response(data, req)
        except Exception as e:
            logger.error("Cleartrip %s→%s failed: %s", req.origin, req.destination, e)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info(
            "Cleartrip %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"cleartrip{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_ct_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "INR",
            offers=offers,
            total_results=len(offers),
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
                    id=f"rt_ct_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]


def _parse_response(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
    """Parse Cleartrip search v2 response.

    Structure:
      cards.J1[]  — flight cards with travelOptionId, summary (dep/arr/duration)
      fares{}     — keyed by fareId → pricing.totalPricing.totalPrice
      flights{}   — keyed by flight ID → detailed flight info
      subTravelOptions{} — maps travel option → fareIds
    """
    offers: list[FlightOffer] = []
    cards = data.get("cards", {}).get("J1", [])
    fares_map = data.get("fares", {})
    flights_map = data.get("flights", {})
    sub_options = data.get("subTravelOptions", {})

    for card in cards:
        try:
            travel_id = card.get("travelOptionId", "")
            summary = card.get("summary", {})

            first_dep = summary.get("firstDeparture", {})
            last_arr = summary.get("lastArrival", {})

            dep_airport = first_dep.get("airport", {})
            arr_airport = last_arr.get("airport", {})

            dep_code = dep_airport.get("code", req.origin)
            arr_code = arr_airport.get("code", req.destination)
            dep_time_str = dep_airport.get("time", "")
            arr_time_str = arr_airport.get("time", "")
            dep_airline = first_dep.get("airlineCode", "")

            dep_dt = _parse_ct_time(dep_time_str, req.date_from)
            arr_dt = _parse_ct_time(arr_time_str, req.date_from)

            duration = summary.get("totalDuration", {})
            dur_secs = (duration.get("hh", 0) * 3600) + (duration.get("mm", 0) * 60)
            stops = summary.get("stops", 0)

            # Build segments from flight info
            flight_infos = summary.get("flights", [])
            segments = []
            for fi in flight_infos:
                ac = fi.get("airlineCode", "")
                fn = fi.get("flightNumber", "")
                detail = None
                for fk, fv in flights_map.items():
                    if fk.startswith(f"{ac}-{fn}-"):
                        detail = fv
                        break

                if detail:
                    seg_dep = detail.get("departure", {}).get("airport", {})
                    seg_arr = detail.get("arrival", {}).get("airport", {})
                    seg_dep_dt = _parse_ct_time(seg_dep.get("time", ""), req.date_from)
                    seg_arr_dt = _parse_ct_time(seg_arr.get("time", ""), req.date_from)
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

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=dur_secs,
                stopovers=stops,
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

            airline_codes = list({fi.get("airlineCode", "") for fi in flight_infos if fi.get("airlineCode")})
            if not airline_codes:
                airline_codes = [dep_airline] if dep_airline else ["??"]

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
                    f"&depart_date={req.date_from.strftime('%Y-%m-%d')}"
                    f"&from={req.origin}&to={req.destination}"
                    f"&intl={'y' if is_intl else 'n'}"
                ),
            ))

        except Exception as e:
            logger.debug("Cleartrip parse card failed: %s", e)
            continue

    return offers
