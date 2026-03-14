"""
Biman Bangladesh Airlines direct API connector — pure httpx, no browser needed.

Biman (IATA: BG) is the flag carrier of Bangladesh, based in Dhaka (DAC).
Operates 30+ routes from DAC to the Middle East, South Asia, Europe, and beyond.

Strategy (httpx, no browser):
  booking.biman-airlines.com is a Sabre Digital Experience (DX) platform
  backed by a public GraphQL API at /api/graphql.

  A single POST with operation ``bookingAirSearch`` returns branded fare
  results.  The only required custom header is ``x-sabre-storefront: BGDX``.
  No session token, no cookies, no CloudFlare bypass needed.

  Response contains branded fare families (Economy Saver → Business) with
  per-brand pricing, segments, and seat availability.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Optional

import httpx

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_API_URL = "https://booking.biman-airlines.com/api/graphql"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-sabre-storefront": "BGDX",
    "Origin": "https://booking.biman-airlines.com",
    "Referer": "https://booking.biman-airlines.com/dx/BGDX/",
}

_SEARCH_QUERY = (
    "query bookingAirSearch($airSearchInput: CustomAirSearchInput) {"
    " bookingAirSearch(airSearchInput: $airSearchInput) {"
    " originalResponse __typename } }"
)


class BimanConnectorClient:
    """Biman Bangladesh Airlines scraper — pure httpx GraphQL calls."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

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
        t0 = time.monotonic()
        client = await self._client()

        date_str = req.date_from.strftime("%Y-%m-%d")

        passengers: dict[str, int] = {"ADT": req.adults or 1}
        if req.children:
            passengers["C11"] = req.children
        if req.infants:
            passengers["INF"] = req.infants

        itinerary_parts = [
            {
                "from": {"useNearbyLocations": False, "code": req.origin},
                "to": {"useNearbyLocations": False, "code": req.destination},
                "when": {"date": date_str},
            }
        ]

        if req.return_from:
            itinerary_parts.append({
                "from": {"useNearbyLocations": False, "code": req.destination},
                "to": {"useNearbyLocations": False, "code": req.origin},
                "when": {"date": req.return_from.strftime("%Y-%m-%d")},
            })

        cabin = "Economy"
        if req.cabin_class == "C":
            cabin = "Business"

        variables = {
            "airSearchInput": {
                "cabinClass": cabin,
                "awardBooking": False,
                "promoCodes": [],
                "searchType": "BRANDED",
                "itineraryParts": itinerary_parts,
                "passengers": passengers,
                "pointOfSale": "BD",
            }
        }

        payload = {
            "query": _SEARCH_QUERY,
            "variables": variables,
        }

        try:
            resp = await client.post(_API_URL, json=payload)
        except httpx.TimeoutException:
            logger.warning("Biman API timed out %s→%s", req.origin, req.destination)
            return self._empty(req)
        except Exception as e:
            logger.error("Biman API error: %s", e)
            return self._empty(req)

        elapsed = time.monotonic() - t0

        if resp.status_code != 200:
            logger.warning("Biman API %d: %s", resp.status_code, resp.text[:300])
            return self._empty(req)

        try:
            body = resp.json()
        except Exception:
            logger.warning("Biman non-JSON response")
            return self._empty(req)

        # GraphQL wraps the result
        data = body
        if "data" in body and "bookingAirSearch" in body["data"]:
            inner = body["data"]["bookingAirSearch"]
            data = inner.get("originalResponse", inner)

        offers = self._parse(data, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        logger.info(
            "Biman %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"biman{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=data.get("currency", req.currency or "BDT"),
            offers=offers,
            total_results=len(offers),
        )

    # ── Parsing ──────────────────────────────────────────────────────────────

    def _parse(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Sabre DX branded search response into FlightOffers."""
        offers: list[FlightOffer] = []
        currency = data.get("currency", "BDT")

        # Build a map of brandId -> English label
        brand_labels: dict[str, str] = {}
        for ff in data.get("fareFamilies", []):
            bid = ff.get("brandId", "")
            for lbl in ff.get("brandLabel", []):
                if lbl.get("languageId") == "en_US":
                    brand_labels[bid] = lbl.get("marketingText", bid)
                    break
            if bid not in brand_labels:
                brand_labels[bid] = bid

        # unbundledOffers: list of groups (one per itinerary direction).
        # Each group is a list of brand offers with segments and pricing.
        for group in data.get("unbundledOffers", []):
            if not isinstance(group, list):
                group = [group]
            for uo in group:
                if not isinstance(uo, dict):
                    continue
                route = self._parse_route(uo)
                if not route:
                    continue

                brand_id = uo.get("brandId", "")
                total = self._extract_price(uo.get("total", {}))
                if total <= 0:
                    continue

                seats = uo.get("seatsRemaining", {}).get("count")
                cabin = uo.get("cabinClass", "Economy")
                flight_key = "_".join(s.flight_no for s in route.segments)
                dep_str = route.segments[0].departure.isoformat() if route.segments else ""

                offer_id = f"bg_{hashlib.md5(f'{flight_key}_{dep_str}_{brand_id}_{total}'.encode()).hexdigest()[:12]}"

                offers.append(FlightOffer(
                    id=offer_id,
                    price=total,
                    currency=currency,
                    price_formatted=f"{total:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=list({s.airline for s in route.segments}),
                    owner_airline="BG",
                    availability_seats=seats,
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source="biman_direct",
                    source_tier="free",
                ))

        # If no unbundled offers, fall back to brandedResults
        if not offers:
            offers = self._parse_branded_results(data, currency, req)

        return offers

    def _parse_branded_results(
        self, data: dict, currency: str, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Fallback: parse brandedResults.itineraryPartBrands."""
        offers: list[FlightOffer] = []

        branded = data.get("brandedResults", {})
        for ipb in branded.get("itineraryPartBrands", []):
            # ipb can be a dict or a list (nested structure)
            if isinstance(ipb, list):
                brand_offers = ipb
            else:
                brand_offers = ipb.get("brandOffers", [])
            for bo in brand_offers:
                if bo.get("soldout", False):
                    continue

                total = self._extract_price(bo.get("total", {}))
                if total <= 0:
                    continue

                brand_id = bo.get("brandId", "")
                seats = bo.get("seatsRemaining", {}).get("count")
                cabin = bo.get("cabinClass", "Economy")

                offer_id = f"bg_{hashlib.md5(f'{brand_id}_{total}'.encode()).hexdigest()[:12]}"

                offers.append(FlightOffer(
                    id=offer_id,
                    price=total,
                    currency=currency,
                    price_formatted=f"{total:.2f} {currency}",
                    outbound=FlightRoute(segments=[], total_duration_seconds=0, stopovers=0),
                    inbound=None,
                    airlines=["Biman Bangladesh Airlines"],
                    owner_airline="BG",
                    availability_seats=seats,
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source="biman_direct",
                    source_tier="free",
                ))

        return offers

    def _parse_route(self, offer: dict) -> Optional[FlightRoute]:
        """Parse itineraryPart from an unbundled offer into a FlightRoute."""
        parts = offer.get("itineraryPart", [])
        segments: list[FlightSegment] = []

        for part in parts:
            for seg_data in part.get("segments", []):
                seg = self._parse_segment(seg_data)
                if seg:
                    segments.append(seg)

        if not segments:
            return None

        total_dur = 0
        for s in segments:
            total_dur += s.duration_seconds
        # If individual durations are zero, compute from first dep to last arr
        if total_dur == 0 and len(segments) >= 1:
            delta = (segments[-1].arrival - segments[0].departure).total_seconds()
            total_dur = max(int(delta), 0)

        return FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(len(segments) - 1, 0),
        )

    def _parse_segment(self, seg: dict) -> Optional[FlightSegment]:
        fl = seg.get("flight", {})
        airline_code = fl.get("airlineCode") or fl.get("operatingAirlineCode") or "BG"
        flight_num = fl.get("flightNumber", "")
        flight_no = f"{airline_code}{flight_num}" if flight_num else ""

        dep_dt = self._parse_dt(seg.get("departure"))
        arr_dt = self._parse_dt(seg.get("arrival"))

        duration = seg.get("duration", 0) * 60  # duration is in minutes
        if duration <= 0 and dep_dt.year > 2000 and arr_dt.year > 2000:
            duration = max(int((arr_dt - dep_dt).total_seconds()), 0)

        equipment = seg.get("equipment", "")
        cabin = seg.get("cabinClass", "economy")
        dep_terminal = fl.get("departureTerminal", "")
        arr_terminal = fl.get("arrivalTerminal", "")

        return FlightSegment(
            airline=airline_code,
            airline_name=self._airline_name(airline_code),
            flight_no=flight_no,
            origin=seg.get("origin", ""),
            destination=seg.get("destination", ""),
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=duration,
            cabin_class=cabin.lower() if cabin else "economy",
            aircraft=equipment,
        )

    @staticmethod
    def _extract_price(price_obj: dict) -> float:
        """Extract price from Sabre DX alternatives structure."""
        alts = price_obj.get("alternatives", [])
        if alts and alts[0]:
            return float(alts[0][0].get("amount", 0))
        return 0.0

    @staticmethod
    def _airline_name(code: str) -> str:
        return {
            "BG": "Biman Bangladesh Airlines",
        }.get(code, code)

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s[:19], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        return (
            f"https://booking.biman-airlines.com/dx/BGDX/#/flight-selection"
            f"?from={req.origin}&to={req.destination}"
            f"&date={req.date_from.strftime('%Y-%m-%d')}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"biman{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "BDT",
            offers=[],
            total_results=0,
        )
