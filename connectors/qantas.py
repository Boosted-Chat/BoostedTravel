"""
Qantas Airways connector — market-pricing GraphQL API.

Qantas (IATA: QF) — SYD/MEL hubs, oneworld member.

Strategy:
  Qantas exposes a public market-pricing GraphQL API at
  api.qantas.com/market-pricing/mpp-graphql/v1/graphql.

  1. Token: POST api.qantas.com/bff/web-token/mpp-graphql → Bearer token
     (no auth required, empty body, token valid ~1 h).
  2. Search: POST graphql endpoint with GetFlightDeals operation.
     Returns deal-level fares per route with travel date windows.
     bestOffer=false returns all available date windows for the route.

  Works via plain httpx — no browser or cookies needed.
  Returns fare deals (not individual flight segments).
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
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

logger = logging.getLogger(__name__)

_TOKEN_URL = "https://api.qantas.com/bff/web-token/mpp-graphql"
_GQL_URL = "https://api.qantas.com/market-pricing/mpp-graphql/v1/graphql"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Origin": "https://www.qantas.com",
    "Referer": "https://www.qantas.com/",
}

_GQL_QUERY = """query GetFlightDeals($input: FlightDealFilterInput!) {
    flightDeals(input: $input) {
      data {
        offer {
          aifFormatted
          travelStart
          travelEnd
          fareFamily
          symbol
          currency
          saleData {
            sale { name iconCode iconName }
            saleName saleStart saleEnd
          }
        }
        market {
          tripType
          tripType_i18n
          cityPairCabin {
            travelClass
            travelClass_i18n
            originAirport { originAirport originName }
            destinationAirport { destinationAirport destinationName }
          }
        }
      }
    }
  }"""

# Token cache (module-level singleton)
_token: Optional[str] = None
_token_expires: float = 0.0


class QantasConnectorClient:
    """Qantas — market-pricing GraphQL fare deals."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True,
                proxy=get_httpx_proxy_url(),)
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def _get_token(self) -> str:
        global _token, _token_expires
        now = time.time()
        if _token and now < _token_expires - 60:
            return _token
        client = await self._client()
        resp = await client.post(_TOKEN_URL)
        resp.raise_for_status()
        data = resp.json()
        _token = data["access_token"]
        # expires_at is in millis
        _token_expires = data.get("expires_at", 0) / 1000.0
        logger.debug("Qantas token refreshed, expires at %s", _token_expires)
        return _token

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        offers: list[FlightOffer] = []
        try:
            token = await self._get_token()
            client = await self._client()
            body = {
                "operationName": "GetFlightDeals",
                "variables": {
                    "input": {
                        "departureAirports": [req.origin],
                        "bestOffer": False,
                        "arrivalAirports": [
                            {"airportCode": req.destination, "travelClass": "ECONOMY"},
                        ],
                    }
                },
                "query": _GQL_QUERY,
            }
            resp = await client.post(
                _GQL_URL, json=body,
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status_code == 200:
                offers = self._parse(resp.json(), req)
            else:
                logger.warning("Qantas GQL %d: %s", resp.status_code, resp.text[:200])
        except Exception as e:
            logger.error("Qantas API error: %s", e)

        offers.sort(key=lambda o: o.price)
        elapsed = time.monotonic() - t0
        logger.info(
            "Qantas %s→%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        return FlightSearchResponse(
            search_id=f"qf_{req.origin}{req.destination}_{int(t0)}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "AUD",
            offers=offers,
            total_results=len(offers),
        )

    def _parse(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        deals = (
            data.get("data", {})
            .get("flightDeals", {})
            .get("data", [])
        )
        target = req.date_from.strftime("%Y-%m-%d")
        # Normalise to date object regardless of whether date_from is date or datetime
        target_date = (
            req.date_from.date()
            if isinstance(req.date_from, datetime)
            else req.date_from
        )

        for deal in deals:
            offer_data = deal.get("offer", {})
            market = deal.get("market", {})
            cp = market.get("cityPairCabin", {})

            origin = cp.get("originAirport", {}).get("originAirport", req.origin)
            dest = cp.get("destinationAirport", {}).get("destinationAirport", req.destination)

            price_str = offer_data.get("aifFormatted", "0")
            try:
                price = float(price_str.replace(",", ""))
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue

            currency = offer_data.get("currency", "AUD")
            travel_start = offer_data.get("travelStart", "")
            travel_end = offer_data.get("travelEnd", "")
            trip_type = market.get("tripType", "RETURN")
            fare_family = offer_data.get("fareFamily", "")

            # Match: requested date falls within the deal's travel window
            if travel_start and travel_end:
                try:
                    ts = datetime.strptime(travel_start, "%Y-%m-%d").date()
                    te = datetime.strptime(travel_end, "%Y-%m-%d").date()
                    if not (ts <= target_date <= te):
                        continue
                except ValueError:
                    continue

            dep_dt = datetime.strptime(target, "%Y-%m-%d").replace(hour=8)
            seg = FlightSegment(
                airline="QF",
                airline_name="Qantas",
                flight_no="QF",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=dep_dt,
            )
            route = FlightRoute(
                segments=[seg], total_duration_seconds=0, stopovers=0,
            )

            # RT deals: add placeholder inbound route (price already includes return)
            ib_route = None
            if trip_type == "RETURN" and req.return_from:
                ret_dt = datetime.combine(req.return_from, datetime.min.time()) if not isinstance(req.return_from, datetime) else req.return_from
                ib_seg = FlightSegment(
                    airline="QF", airline_name="Qantas", flight_no="QF",
                    origin=dest, destination=origin,
                    departure=ret_dt, arrival=ret_dt,
                )
                ib_route = FlightRoute(segments=[ib_seg], total_duration_seconds=0, stopovers=0)

            key = f"qf_{origin}{dest}{target}{price}{fare_family}"
            oid = hashlib.md5(key.encode()).hexdigest()[:12]

            booking_url = (
                f"https://www.qantas.com/en-gb/book/flights?"
                f"from={origin}&to={dest}"
                f"&departure={target.replace('-', '')}"
                f"&adults={req.adults or 1}"
            )
            if req.return_from:
                ret_str = req.return_from.strftime("%Y%m%d") if hasattr(req.return_from, "strftime") else str(req.return_from).replace("-", "")
                booking_url += f"&return={ret_str}"

            offers.append(FlightOffer(
                id=f"qf_rt_{oid}" if ib_route else f"qf_{oid}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:,.2f} {currency}",
                outbound=route,
                inbound=ib_route,
                airlines=["Qantas"],
                owner_airline="QF",
                conditions={
                    "price_type": "deal_fare",
                    "trip_type": trip_type,
                    "fare_family": fare_family,
                    "travel_window": f"{travel_start} to {travel_end}",
                },
                booking_url=booking_url,
                source="qantas_direct",
            ))

        return offers
