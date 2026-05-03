"""
Rehlat connector — direct httpx API calls (no browser).

Rehlat is a Middle-Eastern OTA focused on GCC/MENA markets (Kuwait,
Saudi Arabia, UAE, Bahrain, Oman, Egypt, India).

Strategy:
1.  POST Flights/GetSearchToken to obtain a session token.
2.  POST flights/SearchForm to register the search.
3.  POST apiva/{supplier} for each supplier to get results.
4.  Parse results into FlightOffers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from datetime import datetime
from typing import Any

import httpx

from .browser import get_httpx_proxy_url
from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_API_BASE = "https://api.rehlat.com/v1/Rehlat/"
_B2C_BASE = "https://www.rehlat.com/"
_AUTH = "Basic RWxmSDNDUDJVNU91bUI1MHhlOng="
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)

# Suppliers that return flight results via the apiva endpoint.
# GA00001 is the primary GDS aggregator. Others are airline-specific
# LCC connectors that may return results for certain routes.
_SUPPLIERS = [
    "GA00001", "SA00003", "VI00004", "LCC0001", "LCC0002", "PD00001",
    "FL00009", "FD00019", "UA60016", "JDA000R5", "AAO0025",
    "LCC000PK", "LCC000AT", "LCC000AG", "LCC000FF", "LCC000JQ",
    "LCC000FR", "LCC000OF", "LCC000AB", "LCC000AZ", "LCC000UF",
    "LCC000SP", "LCC000SRPJ", "LCC000GH", "LCC000BT", "LCC000RA",
    "LCC000VY", "LCC000TRNSV", "LCC000U2", "LCC000UX", "LCC000HKR",
    "LCC000VRL",
]

_API_HEADERS = {
    "User-Agent": _UA,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": _AUTH,
    "Origin": "https://www.rehlat.com",
    "Referer": "https://www.rehlat.com/",
}

_B2C_HEADERS = {
    "User-Agent": _UA,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.rehlat.com",
    "Referer": "https://www.rehlat.com/en",
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


class RehlatConnectorClient:
    """Rehlat — Middle East OTA, direct httpx API."""

    def __init__(self, timeout: float = 30.0):
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
                logger.debug("Ancillary fetch timed out for %s→%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s→%s: %s", anc_origin, anc_dest, _anc_err)

        return ob_result

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
                        "REHLAT %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"rehlat{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_reh_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
            except Exception as e:
                logger.warning("REHLAT attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self, req: FlightSearchRequest
    ) -> list[FlightOffer] | None:
        now = datetime.now()
        dep_str = req.date_from.strftime("%Y%m%d")
        is_rt = req.return_from is not None
        trip_type = "RoundTrip" if is_rt else "OneWay"
        ret_str = req.return_from.strftime("%Y%m%d") if is_rt else "00010101"
        key = f"{now.strftime('%Y%m%d%H%M%S')}_{random.randint(10000000, 99999999):08d}"

        async with httpx.AsyncClient(
            verify=False, follow_redirects=True, timeout=self.timeout,
            proxy=get_httpx_proxy_url(),
        ) as client:
            # Step 1: Get search token
            try:
                r = await client.post(
                    _B2C_BASE + "Flights/GetSearchToken",
                    json={"dat": now.strftime(
                        "%a %b %d %Y %H:%M:%S GMT+0100"
                    )},
                    headers=_B2C_HEADERS,
                )
                token = r.text.strip().strip('"')
            except Exception as e:
                logger.warning("REHLAT: token fetch failed: %s", e)
                return {
            "checked_bag": "baggage depends on airline – full-service carriers include 20–30 kg; LCCs charge add-on from ~30 AED/SAR",
            "bags_note": "personal item / hand bag typically free; cabin bag varies by airline",
            "seat_note": "seat selection varies by airline; skip for free random seat",
        }

            # Build search payload
            payload = {
                "TripType": trip_type,
                "Segments": [{
                    "From": req.origin,
                    "To": req.destination,
                    "DepartureDate": dep_str,
                    "ReturnDate": ret_str,
                }],
                "Adults": req.adults or 1,
                "Children": req.children or 0,
                "Infant": 0,
                "Refundable": "00",
                "PreferredAirline": "",
                "Class": {"M": "Y", "W": "W", "C": "C", "F": "F"}.get(req.cabin_class, "Y") if req.cabin_class else "Y",
                "NonStop": None,
                "TotalPax": (req.adults or 1) + (req.children or 0),
                "TotalSeats": (req.adults or 1) + (req.children or 0),
                "Url": f"https://www.rehlat.com//en/cheap-flights/searchresults/?triptype={trip_type}",
                "Currency": "KWD",
                "ClientCode": "B2C",
                "IsMystifly": False,
                "UtmSource": None,
                "UrlReferrer": "",
                "Sector": "I",
                "FromCountry": "",
                "ToCountry": "",
                "Device": "Desktop",
                "Language": "en",
                "DeviceDetail": "Windows NT 6.1; Win64; x64",
                "DeviceOS": "Windows",
                "DeviceCategory": "Desktop",
                "LoginUserEmail": None,
                "Key": key,
                "SessionKey": key,
                "SearchTokenKey": token,
            }
            if is_rt:
                payload["Segments"].append(1)

            # Step 2: Register search session
            try:
                r = await client.post(
                    _B2C_BASE + "flights/SearchForm",
                    json=payload,
                    headers=_B2C_HEADERS,
                )
            except Exception as e:
                logger.warning("REHLAT: SearchForm failed: %s", e)

            # Step 3: Query all suppliers in parallel
            offers: list[FlightOffer] = []

            async def _query_supplier(supplier: str) -> list[FlightOffer]:
                try:
                    r = await client.post(
                        _API_BASE + f"apiva/{supplier}",
                        json=payload,
                        headers=_API_HEADERS,
                    )
                    if not r.text or len(r.text) < 10:
                        return []
                    data = json.loads(r.text)
                    if not isinstance(data, dict):
                        return []
                    results = data.get("results")
                    if not isinstance(results, list) or not results:
                        return []
                    return _parse_results(results, req)
                except Exception:
                    return []

            tasks = [_query_supplier(s) for s in _SUPPLIERS]
            batches = await asyncio.gather(*tasks)
            for batch in batches:
                offers.extend(batch)

            # Deduplicate by sorting key (airline+flight+time)
            seen: set[str] = set()
            unique: list[FlightOffer] = []
            for o in offers:
                sk = o.id
                if sk not in seen:
                    seen.add(sk)
                    unique.append(o)

            return unique

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        return {
            "checked_bag": "baggage depends on airline – full-service carriers include 20–30 kg; LCCs charge add-on from ~30 AED/SAR",
            "bags_note": "personal item / hand bag typically free; cabin bag varies by airline",
            "seat_note": "seat selection varies by airline; skip for free random seat",
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
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from == 0.0:
                offer.bags_price["carry_on"] = 0.0
            if checked_from == 0.0:
                offer.bags_price["checked_bag"] = 0.0

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
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
                    id=f"rt_reh_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

def _parse_results(
    results: list[dict], req: FlightSearchRequest
) -> list[FlightOffer]:
    """Parse Rehlat apiva results into FlightOffers."""
    offers: list[FlightOffer] = []
    currency = req.currency or "KWD"

    for item in results:
        try:
            if not isinstance(item, dict):
                continue

            tp = item.get("totalPriceInfo")
            if not isinstance(tp, dict):
                continue

            price = tp.get("totalAmountwithMarkUp", 0)
            if not isinstance(price, (int, float)) or price <= 0:
                continue

            eff_cur = tp.get("effectiveCurrency")
            if isinstance(eff_cur, str) and len(eff_cur) == 3:
                currency = eff_cur

            # Outbound segments
            ob_details = item.get("outBoundFlightDetails", [])
            if not isinstance(ob_details, list) or not ob_details:
                continue
            ob_segs = _parse_segments(ob_details)
            if not ob_segs:
                continue

            total_dur = sum(
                int(s.get("jrnyTm") or 0) for s in ob_details
                if isinstance(s, dict)
            )
            outbound = FlightRoute(
                segments=ob_segs,
                total_duration_seconds=total_dur * 60,
                stopovers=max(0, len(ob_segs) - 1),
            )

            # Inbound segments (round trip)
            inbound = None
            ib_details = item.get("inBoundFlightDetails", [])
            if isinstance(ib_details, list) and ib_details:
                ib_segs = _parse_segments(ib_details)
                if ib_segs:
                    ib_dur = sum(
                        int(s.get("jrnyTm") or 0) for s in ib_details
                        if isinstance(s, dict)
                    )
                    inbound = FlightRoute(
                        segments=ib_segs,
                        total_duration_seconds=ib_dur * 60,
                        stopovers=max(0, len(ib_segs) - 1),
                    )

            # Airlines
            ua = item.get("uniqueAirline", [])
            airlines: list[str] = []
            if isinstance(ua, list):
                for a in ua:
                    if isinstance(a, dict):
                        code = a.get("airlineCode", "")
                        if code:
                            airlines.append(code)
            if not airlines:
                airlines = list({s.airline for s in ob_segs if s.airline})

            booking_url = item.get("deeplink") or "https://www.rehlat.com/"

            sk = item.get("sortkingKey", "")
            h = hashlib.md5(
                f"reh{req.origin}{req.destination}{price}{sk}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"off_reh_{h}",
                price=round(price, 2),
                currency=currency,
                outbound=outbound,
                inbound=inbound,
                airlines=airlines,
                owner_airline=airlines[0] if airlines else "Rehlat",
                source="rehlat",
                source_tier="ota",
                booking_url=booking_url,
            ))
        except Exception as e:
            logger.debug("REHLAT: skipped result: %s", e)
            continue

    return offers


def _parse_segments(details: list[dict]) -> list[FlightSegment]:
    """Parse outBoundFlightDetails / inBoundFlightDetails into segments."""
    segments: list[FlightSegment] = []
    for seg in details:
        if not isinstance(seg, dict):
            continue
        airline = seg.get("mAirV") or seg.get("airV") or ""
        flt_num = seg.get("fltNum") or ""
        origin = seg.get("startAirp") or ""
        dest = seg.get("endAirp") or ""
        dep = seg.get("deptDateTime") or ""
        arr = seg.get("arrDateTime") or ""

        if not origin or not dest:
            continue

        segments.append(FlightSegment(
            airline=airline,
            flight_no=f"{airline}{flt_num}" if flt_num else airline,
            origin=origin,
            destination=dest,
            departure=_parse_dt(dep),
            arrival=_parse_dt(arr),
        ))
    return segments
