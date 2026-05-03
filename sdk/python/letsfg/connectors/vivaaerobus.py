"""
VivaAerobus direct API scraper — zero auth, pure httpx.

VivaAerobus (IATA: VB) is Mexico's largest ultra-low-cost carrier.
Website: www.vivaaerobus.com — English at /en-us.

Strategy (discovered Mar 2026):
The lowfares calendar API is open — requires only a static x-api-key header.
POST api.vivaaerobus.com/web/vb/v1/availability/lowfares
Returns 7 days of lowest fares as structured JSON. No browser needed.

Note: the full /web/v1/availability/search endpoint IS Akamai-protected (403),
but the lowfares endpoint works fine with plain httpx.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from curl_cffi.requests import AsyncSession

from .browser import get_curl_cffi_proxies
from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_API_BASE = "https://api.vivaaerobus.com"
_API_KEY = "zasqyJdSc92MhWMxYu6vW3hqhxLuDwKog3mqoYkf"
_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://www.vivaaerobus.com",
    "Referer": "https://www.vivaaerobus.com/",
    "x-api-key": _API_KEY,
    "X-Channel": "web",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}

_http_client: AsyncSession | None = None


def _get_client() -> AsyncSession:
    global _http_client
    if _http_client is None:
        _http_client = AsyncSession(impersonate="chrome131", headers=_HEADERS, timeout=30, proxies=get_curl_cffi_proxies())
    return _http_client


class VivaAerobusConnectorClient:
    """VivaAerobus scraper — pure direct API, zero auth, ~0.5s searches."""

    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout

    async def close(self):
        global _http_client
        if _http_client:
            _http_client.close()
            _http_client = None

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
                        req.date_from.isoformat(), req.adults, ob_result.currency,
                    ),
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s→%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s→%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        """Fetch bag/seat pricing for VivaAerobus. Returns None — the lowfares
        calendar API exposes base fare only; full availability is Akamai-protected."""
        return None

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_price")
        anc_currency = ancillary.get("currency", "MXN")
        for offer in offers:
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["carry_on"] = bags_from
            if checked_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["checked_bag"] = checked_from


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        client = _get_client()

        start_date = req.date_from.strftime("%Y-%m-%d")
        end_date = (req.date_from + timedelta(days=6)).strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1

        routes = [{
            "startDate": start_date,
            "endDate": end_date,
            "origin": {"code": req.origin, "type": "Airport"},
            "destination": {"code": req.destination, "type": "Airport"},
        }]
        # Add return route for RT searches
        if req.return_from:
            ret_start = req.return_from.strftime("%Y-%m-%d")
            ret_end = (req.return_from + timedelta(days=6)).strftime("%Y-%m-%d")
            routes.append({
                "startDate": ret_start,
                "endDate": ret_end,
                "origin": {"code": req.destination, "type": "Airport"},
                "destination": {"code": req.origin, "type": "Airport"},
            })

        body = {
            "currencyCode": req.currency or "USD",
            "promoCode": None,
            "bookingType": None,
            "referralCode": "",
            "passengers": [{"code": "ADT", "count": adults}],
            "routes": routes,
            "sessionID": str(uuid.uuid4()),
            "language": "en-US",
        }

        logger.info("VivaAerobus API: %s→%s %s–%s%s", req.origin, req.destination,
                     start_date, end_date, f" RT→{req.return_from}" if req.return_from else "")

        try:
            resp = await client.post(f"{_API_BASE}/web/vb/v1/availability/lowfares", json=body, headers=_HEADERS)
            elapsed = time.monotonic() - t0

            if resp.status_code != 200:
                logger.warning("VivaAerobus API HTTP %d: %s", resp.status_code, resp.text[:300])
                return self._empty(req)

            api_json = resp.json()
            outbound_offers = self._parse_lowfares(api_json, req)

            # Parse return leg + build combos
            if req.return_from and outbound_offers:
                inbound_offers = self._parse_lowfares_return(api_json, req)
                if inbound_offers:
                    combos = self._build_rt_combos(outbound_offers, inbound_offers, req)
                    if combos:
                        outbound_offers = combos + outbound_offers

            if outbound_offers:
                return self._build_response(outbound_offers, req, elapsed)
            return self._empty(req)

        except Exception as e:
            logger.error("VivaAerobus API error: %s", e)
            return self._empty(req)

    @staticmethod
    def _extract_lowfare_conditions(fare: dict) -> dict[str, str]:
        conditions = {
            "fare_upgrade_note": "Lowfares search exposes base fare only; no baggage or seat pricing",
            "carry_on": "carry-on bag add-on from ~MXN 280 (10 kg); personal item (under seat) only on base fare",
            "checked_bag": "checked bag add-on from ~MXN 400 (25 kg); not included on base VC fare",
            "seat": "seat selection from ~MXN 150; included in higher fare bundles",
        }
        fare_family = str(fare.get("fareProductClass") or "").strip()
        if fare_family:
            conditions["fare_family"] = fare_family
        return conditions

    def _parse_lowfares_return(self, api_json: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse return leg lowfares from the API response (second route in response)."""
        data = api_json.get("data", {})
        low_fares_list = data.get("lowFares", [])
        currency = data.get("currencyCode", req.currency)
        # Return fares: filter for reverse direction (dest→origin)
        offers: list[FlightOffer] = []
        for fare in (low_fares_list if isinstance(low_fares_list, list) else []):
            if not isinstance(fare, dict):
                continue
            origin_obj = fare.get("origin", {})
            dest_obj = fare.get("destination", {})
            origin_code = origin_obj.get("code", "") if isinstance(origin_obj, dict) else ""
            dest_code = dest_obj.get("code", "") if isinstance(dest_obj, dict) else ""
            # Only take return direction fares
            if origin_code != req.destination or dest_code != req.origin:
                continue
            dep_date = fare.get("departureDate", "")
            fare_obj = fare.get("fare", {})
            fare_with_tua = fare.get("fareWithTua", {})
            price = (fare_with_tua.get("amount") if fare_with_tua else None) or fare_obj.get("amount")
            if price is None or price <= 0:
                continue
            dep_dt = self._parse_dt(dep_date)
            segment = FlightSegment(
                airline=fare.get("carrierCode", "VB"),
                airline_name="VivaAerobus",
                flight_no="",
                origin=origin_code,
                destination=dest_code,
                departure=dep_dt,
                arrival=dep_dt,
                cabin_class=fare.get("fareProductClass", "M"),
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
            offer_key = f"vb_{origin_code}{dest_code}_{dep_date}_{price}"
            conditions = self._extract_lowfare_conditions(fare)
            offers.append(FlightOffer(
                id=f"vb_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                price=round(float(price), 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=[fare.get("carrierCode", "VB")],
                owner_airline="VB",
                conditions=conditions,
                booking_url=self._build_booking_url(req),
                is_locked=False,
                source="vivaaerobus_direct",
                source_tier="free",
            ))
        return offers

    def _build_rt_combos(
        self,
        outbound: list[FlightOffer],
        inbound: list[FlightOffer],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Combine outbound × inbound into RT offers."""
        def merge_conditions(outbound_conditions: dict[str, str], inbound_conditions: dict[str, str]) -> dict[str, str]:
            merged = dict(outbound_conditions or {})
            for key, value in (inbound_conditions or {}).items():
                if value in (None, ""):
                    continue
                existing = merged.get(key)
                if existing is None:
                    merged[key] = value
                    continue
                if existing == value:
                    continue
                merged.pop(key, None)
                if key in (outbound_conditions or {}):
                    merged[f"outbound_{key}"] = outbound_conditions[key]
                merged[f"inbound_{key}"] = value
            return merged

        combos: list[FlightOffer] = []
        for ob in outbound[:15]:
            for ib in inbound[:10]:
                price = round(ob.price + ib.price, 2)
                combo_key = f"vb_rt_{ob.id}_{ib.id}"
                combos.append(FlightOffer(
                    id=f"vb_{hashlib.md5(combo_key.encode()).hexdigest()[:12]}",
                    price=price,
                    currency=ob.currency,
                    price_formatted=f"{price:.2f} {ob.currency}",
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    airlines=list(set(ob.airlines + ib.airlines)),
                    owner_airline="VB",
                    conditions=merge_conditions(ob.conditions, ib.conditions),
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="vivaaerobus_direct",
                    source_tier="free",
                ))
        combos.sort(key=lambda o: o.price)
        return combos[:50]

    # ------------------------------------------------------------------ #
    #  Lowfares API parsing                                                #
    # ------------------------------------------------------------------ #

    def _parse_lowfares(self, api_json: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the lowfares API response into FlightOffer objects."""
        data = api_json.get("data", {})
        low_fares = data.get("lowFares", [])
        currency = data.get("currencyCode", req.currency)
        if not low_fares:
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for fare in low_fares:
            if not isinstance(fare, dict):
                continue
            dep_date = fare.get("departureDate", "")
            fare_obj = fare.get("fare", {})
            fare_with_tua = fare.get("fareWithTua", {})
            # Prefer fareWithTua (includes taxes) over base fare
            price = (fare_with_tua.get("amount") if fare_with_tua else None) or fare_obj.get("amount")
            if price is None or price <= 0:
                continue

            origin_obj = fare.get("origin", {})
            dest_obj = fare.get("destination", {})
            origin_code = origin_obj.get("code", req.origin) if isinstance(origin_obj, dict) else req.origin
            dest_code = dest_obj.get("code", req.destination) if isinstance(dest_obj, dict) else req.destination
            origin_name = origin_obj.get("name", "") if isinstance(origin_obj, dict) else ""
            dest_name = dest_obj.get("name", "") if isinstance(dest_obj, dict) else ""
            carrier = fare.get("carrierCode", "VB")
            avail = fare.get("availableCount")
            fare_class = fare.get("fareProductClass", "")
            conditions = self._extract_lowfare_conditions(fare)

            # Build a segment for the date (VB calendar shows one fare per day)
            dep_dt = self._parse_dt(dep_date)
            segment = FlightSegment(
                airline=carrier,
                airline_name="VivaAerobus",
                flight_no="",
                origin=origin_code,
                destination=dest_code,
                origin_city=origin_name,
                destination_city=dest_name,
                departure=dep_dt,
                arrival=dep_dt,
                cabin_class=fare_class or "M",
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)

            offer_key = f"vb_{origin_code}{dest_code}_{dep_date}_{price}"
            offer = FlightOffer(
                id=f"vb_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                price=round(float(price), 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=[carrier],
                owner_airline="VB",
                availability_seats=avail,
                conditions=conditions,
                booking_url=booking_url,
                is_locked=False,
                source="vivaaerobus_direct",
                source_tier="free",
            )
            offers.append(offer)

        return offers

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("VivaAerobus %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"vivaaerobus{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S",
                     "%Y-%m-%d", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y%m%d")
        adults = getattr(req, "adults", 1) or 1
        return (
            f"https://www.vivaaerobus.com/en-us/book/options?itineraryCode="
            f"{req.origin}_{req.destination}_{dep}&passengers=A{adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"vivaaerobus{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )


    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req,
    ) -> list[FlightOffer]:
        def merge_conditions(outbound_conditions: dict[str, str], inbound_conditions: dict[str, str]) -> dict[str, str]:
            merged = dict(outbound_conditions or {})
            for key, value in (inbound_conditions or {}).items():
                if value in (None, ""):
                    continue
                existing = merged.get(key)
                if existing is None:
                    merged[key] = value
                    continue
                if existing == value:
                    continue
                merged.pop(key, None)
                if key in (outbound_conditions or {}):
                    merged[f"outbound_{key}"] = outbound_conditions[key]
                merged[f"inbound_{key}"] = value
            return merged

        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_viva_{cid}", price=price, currency=o.currency,
                    price_formatted=f"{price:.2f} {o.currency}",
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    conditions=merge_conditions(o.conditions, i.conditions),
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
