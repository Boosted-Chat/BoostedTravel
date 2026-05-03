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




import asyncio

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

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
_TOKEN_URL = "https://api.qantas.com/bff/web-token/mpp-graphql"
_GQL_URL = "https://api.qantas.com/market-pricing/mpp-graphql/v1/graphql"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
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
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        # Qantas Economy includes bag on most fares; Economy Saver/Sale may not.
        return {
            "bags_note": "Economy Classic/Flex: 1×23 kg included. Economy Saver/Sale (cheapest): no bag, first bag from AUD 35. Carry-on 7 kg included.",
            "seat_note": "Seat selection: free at check-in. Preferred/Upfront from AUD 25.",
            "bags_from": None,
            "currency": currency,
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
            if bags_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["carry_on"] = bags_from
            if checked_from is not None and offer.currency.upper() == anc_currency.upper():
                offer.bags_price["checked_bag"] = checked_from

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
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
                            {"airportCode": req.destination, "travelClass": {"M": "ECONOMY", "W": "PREMIUM_ECONOMY", "C": "BUSINESS", "F": "FIRST"}.get(req.cabin_class or "M", "ECONOMY")},
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
                logger.info("Qantas: GraphQL deals API returned fare windows without schedule times; suppressing offers")
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
        logger.info("Qantas: fare-window parsing is disabled until real schedule times are available")
        return []


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
                    id=f"rt_qant_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
