"""
Vietnam Airlines (VN) -- EveryMundo sputnik grouped-routes API connector.

Uses the airTRFX fare-finder API to retrieve published fares across Vietnam Airlines's network.
Markets: US, VN, JP, KR, AU, GB, FR, DE, TH, SG.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date, datetime, time as dt_time, timedelta
from typing import Optional

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_httpx_proxy_url
from .airline_routes import city_match_set

logger = logging.getLogger(__name__)

_HOME_URL = "https://www.vietnamairlines.com/en"
_API_URL = "https://openair-california.airtrfx.com/airfare-sputnik-service/v3/vn/fares/grouped-routes"
_API_KEY = "HeQpRjsFI5xlAaSx2onkjc1HTK0ukqA1IrVvd5fvaMhNtzLTxInTpeYB1MK93pah"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://mm-prerendering-static-prod.airtrfx.com",
    "Referer": "https://mm-prerendering-static-prod.airtrfx.com/",
    "em-api-key": _API_KEY,
}

_MARKETS = ["US", "VN", "JP", "KR", "AU", "GB", "FR", "DE", "TH", "SG"]


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    return value


def _build_route(origin, destination, travel_date, cabin_class="economy"):
    departure_dt = datetime.combine(travel_date, dt_time(0, 0))
    segment = FlightSegment(
        airline="VN",
        airline_name="Vietnam Airlines",
        flight_no="",
        origin=origin,
        destination=destination,
        origin_city="",
        destination_city="",
        departure=departure_dt,
        arrival=departure_dt,
        duration_seconds=0,
        cabin_class=cabin_class,
    )
    return FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)


class VietnamAirlinesConnectorClient:
    def __init__(self, timeout=35.0):
        self.timeout = timeout
        self._http = None

    async def _client(self):
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout,
                headers=_HEADERS,
                follow_redirects=True,
                proxy=get_httpx_proxy_url(),)
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result


    async def _search_ow(self, req):
        started = time.monotonic()
        offers = []

        try:
            payload = self._build_payload(req)
            cards = await self._fetch_cards(payload)
            offers = self._build_offers(cards, req)
        except Exception as exc:
            logger.warning("Vietnam Airlines search failed for %s->%s: %s", req.origin, req.destination, exc)

        _td = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
        offers = [o for o in offers if o.outbound and o.outbound.segments and o.outbound.segments[0].departure.date() == _td]
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        logger.info(
            "Vietnam Airlines %s->%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), time.monotonic() - started,
        )

        search_hash = hashlib.md5(
            f"vietnamairlines{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    def _build_payload(self, req):
        outbound = _as_date(req.date_from)
        inbound = _as_date(req.return_from) if req.return_from else None
        today = date.today()
        start = min(today, outbound)
        end = max(inbound or outbound, start + timedelta(days=90))

        return {
            "markets": _MARKETS,
            "languageCode": "en",
            "dataExpirationWindow": "2d",
            "datePattern": "dd MMM yy (E)",
            "outputCurrencies": ["USD"],
            "departure": {"start": start.isoformat(), "end": end.isoformat()},
            "budget": {"maximum": None},
            "passengers": {"adults": max(1, req.adults or 1)},
            "travelClasses": [{"M": "ECONOMY", "W": "PREMIUM_ECONOMY", "C": "BUSINESS", "F": "FIRST"}.get(req.cabin_class or "M", "ECONOMY")],
            "flightType": "ROUND_TRIP" if req.return_from else "ONE_WAY",
            "flexibleDates": True,
            "faresPerRoute": "10",
            "trfxRoutes": True,
            "routesLimit": 200,
            "sorting": [{"popularity": "DESC"}],
            "airlineCode": "vn",
        }

    async def _fetch_cards(self, payload):
        client = await self._client()
        response = await client.post(_API_URL, json=payload)
        response.raise_for_status()

        data = response.json()
        cards = []
        for route in data:
            for fare in route.get("fares") or []:
                departure_value = fare.get("departureDate")
                if not departure_value:
                    continue
                departure_date = datetime.strptime(departure_value[:10], "%Y-%m-%d").date()

                return_value = fare.get("returnDate")
                return_date = None
                if return_value:
                    return_date = datetime.strptime(return_value[:10], "%Y-%m-%d").date()

                cards.append({
                    "origin": (fare.get("origin") or route.get("origin") or "").upper(),
                    "destination": (fare.get("destination") or route.get("destination") or "").upper(),
                    "origin_city": fare.get("originCity") or route.get("originCity") or "",
                    "destination_city": fare.get("destinationCity") or route.get("destinationCity") or "",
                    "departure_date": departure_date,
                    "return_date": return_date,
                    "currency": fare.get("currencyCode") or "USD",
                    "price": round(float(fare.get("totalPrice") or fare.get("usdTotalPrice") or 0.0), 2),
                    "trip_type": (fare.get("flightType") or "ROUND_TRIP").lower().replace("_", "-"),
                    "cabin": fare.get("farenetTravelClass") or fare.get("travelClass") or "Economy",
                })

        return cards

    def _build_offers(self, cards, req):
        offers = []
        valid_origins = city_match_set(req.origin)
        valid_dests = city_match_set(req.destination)

        for card in cards:
            if card["origin"] not in valid_origins or card["destination"] not in valid_dests:
                continue
            if card["price"] <= 0:
                continue
            _vn_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")

            outbound = _build_route(req.origin, req.destination, card["departure_date"], _vn_cabin)
            inbound = None
            if card.get("return_date"):
                inbound = _build_route(req.destination, req.origin, card["return_date"], _vn_cabin)

            price = round(card["price"], 2)
            currency = card.get("currency") or "USD"
            return_token = f"_{card['return_date'].isoformat()}" if card.get("return_date") else ""
            offer_hash = hashlib.md5(
                f"vn_{req.origin}_{req.destination}_{card['departure_date'].isoformat()}{return_token}_{price}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"vn_{offer_hash}",
                price=price,
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=outbound,
                inbound=inbound,
                airlines=["Vietnam Airlines"],
                owner_airline="VN",
                booking_url=_HOME_URL,
                is_locked=False,
                source="vietnamairlines_direct",
                source_tier="free",
                conditions={
                    "trip_type": card.get("trip_type", "round-trip"),
                    "cabin": str(card.get("cabin") or "Economy"),
                    "fare_note": "Promo fare from Vietnam Airlines embedded fare module",
                },
            ))

        return offers


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
                    id=f"rt_viet_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
