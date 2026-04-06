"""
SalamAir direct API connector — curl_cffi for TLS fingerprint bypass.

SalamAir (IATA: OV) is an Omani low-cost carrier based in Muscat, operating
60+ routes from Muscat to the Middle East, South Asia, Africa, and Europe.

Strategy (curl_cffi required — api.salamair.com fingerprints TLS):
  The booking.salamair.com React SPA calls a REST API at api.salamair.com.
  We replicate those exact calls:
    1. POST /api/session → get JWT session token (X-Session-Token header)
    2. GET  /api/flights?TripType=1&OriginStationCode=..&DestinationStationCode=..
       &DepartureDate=YYYY-MM-DD&AdultCount=N&... → full flight+fare data

  The TripType=1 parameter is REQUIRED — without it the API returns empty
  flights despite a 200 status code.

  Routes endpoint: GET /api/resources/routes?culture=en-US → full route map.

Response structure (per market date):
  trips[].markets[].flights[]:
    segments[].carrierCode/flightNumber/departureDate/arrivalDate/originCode/destinationCode
    segments[].legs[].aircraftType/flightTime
    fares[].fareTypeName ("Lite"/"Flexi"/"Business")
    fares[].fareInfos[].fareWithTaxes / seatsAvailable
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

from curl_cffi import requests as creq

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import get_curl_cffi_proxies

logger = logging.getLogger(__name__)

_API_BASE = "https://api.salamair.com"
_BOOKING_ORIGIN = "https://booking.salamair.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
    "cache-control": "no-store",
    "Culture": "en-US",
    "Origin": _BOOKING_ORIGIN,
    "Referer": f"{_BOOKING_ORIGIN}/",
}


class SalamAirConnectorClient:
    """SalamAir scraper — curl_cffi calls to api.salamair.com REST API."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout

    async def close(self):
        pass

    def _search_sync(self, req: FlightSearchRequest) -> dict | None:
        """Run session + search synchronously via curl_cffi."""
        sess = creq.Session(impersonate="chrome131", proxies=get_curl_cffi_proxies())
        hdrs = dict(_HEADERS)
        to = int(self.timeout)

        # 1. Get session token
        try:
            resp = sess.post(f"{_API_BASE}/api/session", headers=hdrs, timeout=to)
        except Exception as e:
            logger.error("SalamAir session error: %s", e)
            return None

        token = resp.headers.get("x-session-token", "")
        if not token:
            logger.warning("SalamAir session failed: %d", resp.status_code)
            return None

        # 2. Search flights
        date_str = req.date_from.strftime("%Y-%m-%d")
        params = {
            "TripType": "1",
            "OriginStationCode": req.origin,
            "DestinationStationCode": req.destination,
            "DepartureDate": date_str,
            "AdultCount": str(req.adults or 1),
            "ChildCount": str(req.children or 0),
            "InfantCount": str(req.infants or 0),
            "extraCount": "0",
            "days": "0",
        }
        if req.currency and req.currency != "EUR":
            params["currencyCode"] = req.currency

        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{_API_BASE}/api/flights?{qs}"

        try:
            hdrs["X-Session-Token"] = token
            resp = sess.get(url, headers=hdrs, timeout=to)
        except Exception as e:
            logger.error("SalamAir search error: %s", e)
            return None

        if resp.status_code != 200:
            logger.warning("SalamAir search %d: %s", resp.status_code, resp.text[:200])
            return None

        try:
            return resp.json()
        except Exception:
            logger.warning("SalamAir non-JSON response")
            return None

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        data = await asyncio.get_event_loop().run_in_executor(
            None, self._search_sync, req
        )
        if not data:
            return self._empty(req)

        offers = self._parse_trips(data, req)
        elapsed = time.monotonic() - t0

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        logger.info(
            "SalamAir %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"salamair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "OMR"),
            offers=offers,
            total_results=len(offers),
        )

    # ── Parsing ──────────────────────────────────────────────────────────────

    def _parse_trips(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        target_date = req.date_from.strftime("%Y-%m-%d")

        for trip in data.get("trips", []):
            currency = trip.get("currencyCode") or req.currency or "OMR"

            for market in trip.get("markets", []):
                market_date = (market.get("date") or "")[:10]
                if market_date != target_date:
                    continue

                for flight in market.get("flights") or []:
                    flight_offers = self._parse_flight(flight, currency, req)
                    offers.extend(flight_offers)

        return offers

    def _parse_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse one flight entry into offers (one per fare type)."""
        segments = self._parse_segments(flight)
        if not segments:
            return []

        total_duration = 0
        for seg in segments:
            total_duration += seg.duration_seconds

        stopovers = max(0, len(segments) - 1)

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_duration,
            stopovers=stopovers,
        )

        flight_key = "_".join(s.flight_no for s in segments)
        dep_str = segments[0].departure.isoformat() if segments else ""

        offers: list[FlightOffer] = []
        fares = flight.get("fares") or []

        # Pick best fare per fare type
        for fare in fares:
            if not fare.get("available", False):
                continue

            fare_name = fare.get("fareTypeName", "unknown")
            fare_infos = fare.get("fareInfos") or []

            # Sum up fare across all passenger types
            total_price = 0.0
            seats_avail: Optional[int] = None
            for fi in fare_infos:
                price = fi.get("fareWithTaxes") or fi.get("baseFareWithTaxes") or 0
                total_price += float(price)
                sa = fi.get("seatsAvailable")
                if sa is not None:
                    seats_avail = min(seats_avail, sa) if seats_avail is not None else sa

            if total_price <= 0:
                continue

            total_price = round(total_price, 2)

            offer_id = f"ov_{hashlib.md5(f'{flight_key}_{dep_str}_{fare_name}_{total_price}'.encode()).hexdigest()[:12]}"

            offers.append(FlightOffer(
                id=offer_id,
                price=total_price,
                currency=currency,
                price_formatted=f"{total_price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["SalamAir"],
                owner_airline="OV",
                availability_seats=seats_avail,
                booking_url=self._booking_url(req),
                is_locked=False,
                source="salamair_direct",
                source_tier="free",
            ))

        # If no fare entries, use market lowestFare as fallback
        if not offers:
            lowest = flight.get("lowestFare") or 0
            if lowest and float(lowest) > 0:
                offer_id = f"ov_{hashlib.md5(f'{flight_key}_{dep_str}_{lowest}'.encode()).hexdigest()[:12]}"
                offers.append(FlightOffer(
                    id=offer_id,
                    price=round(float(lowest), 2),
                    currency=currency,
                    price_formatted=f"{float(lowest):.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=["SalamAir"],
                    owner_airline="OV",
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source="salamair_direct",
                    source_tier="free",
                ))

        return offers

    def _parse_segments(self, flight: dict) -> list[FlightSegment]:
        segments: list[FlightSegment] = []

        for seg in flight.get("segments") or []:
            carrier = seg.get("carrierCode") or "OV"
            fnum = seg.get("flightNumber") or ""
            flight_no = f"{carrier}{fnum}" if fnum and not fnum.startswith(carrier) else fnum

            dep_dt = self._parse_dt(seg.get("departureDate"))
            arr_dt = self._parse_dt(seg.get("arrivalDate"))

            duration = int(seg.get("flightTime", 0) * 60)  # flightTime is minutes
            if duration <= 0 and dep_dt.year > 2000 and arr_dt.year > 2000:
                delta = arr_dt - dep_dt
                duration = max(int(delta.total_seconds()), 0)

            # Aircraft from legs
            aircraft = ""
            legs = seg.get("legs") or []
            if legs:
                aircraft = legs[0].get("aircraftDescription") or legs[0].get("aircraftType") or ""

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="SalamAir",
                flight_no=flight_no,
                origin=seg.get("originCode") or "",
                destination=seg.get("destinationCode") or "",
                origin_city=seg.get("originCity") or "",
                destination_city=seg.get("destinationCity") or "",
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=duration,
                cabin_class="economy",
                aircraft=aircraft,
            ))

        return segments

    @staticmethod
    def _parse_dt(s) -> datetime:
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
        d = req.date_from.strftime("%Y%m%d")
        trip = "roundtrip" if req.return_from else "oneway"
        url = (
            f"{_BOOKING_ORIGIN}/en/search?tripType={trip}"
            f"&origin={req.origin}&destination={req.destination}"
            f"&departureDate={d}&adult={req.adults or 1}"
            f"&child={req.children or 0}&infant={req.infants or 0}"
        )
        if req.return_from:
            url += f"&returnDate={req.return_from.strftime('%Y%m%d')}"
        return url

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"salamair{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "OMR",
            offers=[],
            total_results=0,
        )
