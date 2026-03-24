"""
SriLankan Airlines (UL) -- EveryMundo sputnik grouped-routes API connector.

SriLankan Airlines (IATA: UL) is the flag carrier of Sri Lanka.
Hub at Colombo Bandaranaike (CMB) with routes to India, Middle East,
Southeast Asia, East Asia, Europe, and North America.

Strategy (direct API — no browser required):
  1. POST to airTRFX sputnik grouped-routes with EM-API-Key header
  2. Parse fare response → FlightOffer objects
  3. Construct booking URL for srilankan.com
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date, datetime, time as dt_time, timedelta

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_HOME_URL = "https://www.srilankan.com/en_uk/"
_API_URL = (
    "https://openair-california.airtrfx.com"
    "/airfare-sputnik-service/v3/ul/fares/grouped-routes"
)
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

_MARKETS = ["US", "GB", "LK", "IN", "AE", "SG", "AU", "JP", "DE", "FR"]


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    return date.today() + timedelta(days=30)


def _build_route(origin, destination, travel_date):
    departure_dt = datetime.combine(travel_date, dt_time(0, 0))
    segment = FlightSegment(
        airline="UL",
        airline_name="SriLankan Airlines",
        flight_no="",
        origin=origin,
        destination=destination,
        origin_city="",
        destination_city="",
        departure=departure_dt,
        arrival=departure_dt,
        duration_seconds=0,
        cabin_class="economy",
    )
    return FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)


class SrilankanConnectorClient:
    """SriLankan Airlines (UL) — EveryMundo sputnik grouped-routes API."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: httpx.AsyncClient | None = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout,
                headers=_HEADERS,
                follow_redirects=True,
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        started = time.monotonic()
        offers: list[FlightOffer] = []

        try:
            payload = self._build_payload(req)
            cards = await self._fetch_cards(payload)
            offers = self._build_offers(cards, req)
        except Exception as exc:
            logger.warning(
                "SriLankan Airlines search failed for %s->%s: %s",
                req.origin, req.destination, exc,
            )

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        logger.info(
            "SriLankan Airlines %s->%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), time.monotonic() - started,
        )

        search_hash = hashlib.md5(
            f"ul{req.origin}{req.destination}{req.date_from}{req.return_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    def _build_payload(self, req: FlightSearchRequest) -> dict:
        outbound = _as_date(req.date_from)
        inbound = _as_date(req.return_from) if req.return_from else None
        start = outbound - timedelta(days=1)
        end = inbound + timedelta(days=3) if inbound else outbound + timedelta(days=3)

        return {
            "markets": _MARKETS,
            "languageCode": "en",
            "dataExpirationWindow": "2d",
            "outputCurrencies": ["USD"],
            "departure": {"start": start.isoformat(), "end": end.isoformat()},
            "passengers": {"adults": max(1, req.adults or 1)},
            "travelClasses": ["ECONOMY"],
            "flightType": "ROUND_TRIP" if req.return_from else "ONE_WAY",
            "flexibleDates": True,
            "faresPerRoute": "10",
            "trfxRoutes": True,
            "routesLimit": 200,
            "sorting": [{"popularity": "DESC"}],
            "airlineCode": "ul",
        }

    async def _fetch_cards(self, payload: dict) -> list[dict]:
        client = await self._client()
        response = await client.post(_API_URL, json=payload)
        response.raise_for_status()

        data = response.json()
        cards: list[dict] = []
        for route in data:
            for fare in route.get("fares") or []:
                departure_value = fare.get("departureDate")
                if not departure_value:
                    continue
                departure_date = datetime.strptime(
                    departure_value[:10], "%Y-%m-%d"
                ).date()

                return_value = fare.get("returnDate")
                return_date = None
                if return_value:
                    return_date = datetime.strptime(
                        return_value[:10], "%Y-%m-%d"
                    ).date()

                cards.append(
                    {
                        "origin": (
                            fare.get("origin") or route.get("origin") or ""
                        ).upper(),
                        "destination": (
                            fare.get("destination") or route.get("destination") or ""
                        ).upper(),
                        "origin_city": fare.get("originCity")
                        or route.get("originCity")
                        or "",
                        "destination_city": fare.get("destinationCity")
                        or route.get("destinationCity")
                        or "",
                        "departure_date": departure_date,
                        "return_date": return_date,
                        "currency": fare.get("currencyCode") or "USD",
                        "price": round(
                            float(
                                fare.get("totalPrice")
                                or fare.get("usdTotalPrice")
                                or 0.0
                            ),
                            2,
                        ),
                        "trip_type": (fare.get("flightType") or "ONE_WAY")
                        .lower()
                        .replace("_", "-"),
                        "cabin": fare.get("farenetTravelClass")
                        or fare.get("travelClass")
                        or "Economy",
                    }
                )

        return cards

    def _build_offers(
        self, cards: list[dict], req: FlightSearchRequest
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        for card in cards:
            if card["origin"] != req.origin or card["destination"] != req.destination:
                continue
            if card["price"] <= 0:
                continue

            outbound = _build_route(
                req.origin, req.destination, card["departure_date"]
            )
            inbound = None
            if card.get("return_date"):
                inbound = _build_route(
                    req.destination, req.origin, card["return_date"]
                )

            price = round(card["price"], 2)
            currency = card.get("currency") or "USD"
            return_token = (
                f"_{card['return_date'].isoformat()}" if card.get("return_date") else ""
            )
            offer_hash = hashlib.md5(
                f"ul_{req.origin}_{req.destination}_{card['departure_date'].isoformat()}{return_token}_{price}".encode()
            ).hexdigest()[:12]

            offers.append(
                FlightOffer(
                    id=f"ul_{offer_hash}",
                    price=price,
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=outbound,
                    inbound=inbound,
                    airlines=["SriLankan Airlines"],
                    owner_airline="UL",
                    booking_url=_HOME_URL,
                    is_locked=False,
                    source="srilankan_direct",
                    source_tier="free",
                    conditions={
                        "trip_type": card.get("trip_type", "one-way"),
                        "cabin": str(card.get("cabin") or "Economy"),
                        "fare_note": "Fare from SriLankan Airlines EveryMundo module",
                    },
                )
            )

        return offers
