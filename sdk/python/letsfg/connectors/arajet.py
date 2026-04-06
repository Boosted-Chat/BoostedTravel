"""
Arajet connector — Dominican Republic ULCC.

Arajet (IATA: DM) is the Dominican Republic's first ultra-low-cost carrier.
Hub at SDQ (Santo Domingo). Rapidly growing, serving Caribbean + Americas.
28 destinations in Colombia, Mexico, Central America, Canada, US.

Strategy:
  Pure httpx — Arajet exposes a Radixx PSS calendar API at
  /pss/calendar?origin=X&destination=Y&month=YYYY-MM
  Returns fareProducts per day with baseAmount + totalAmount in USD.
  Fare classes: E (Economy), S (Sale), H (High season).
  Variants: BAS (basic), CLS (classic), COM (comfort), EXT (extra).
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

_BASE = "https://www.arajet.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer": "https://www.arajet.com/",
}

# Fare variant labels derived from Radixx fare codes
_VARIANT_LABELS = {
    "BAS": "Basic",
    "CLS": "Classic",
    "COM": "Comfort",
    "EXT": "Extra",
}


class ArajetConnectorClient:
    """Arajet — Radixx PSS calendar API."""

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

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        client = await self._client()
        target = req.date_from.strftime("%Y-%m-%d")
        month = req.date_from.strftime("%Y-%m")

        try:
            resp = await client.get(
                f"{_BASE}/pss/calendar",
                params={"origin": req.origin, "destination": req.destination, "month": month},
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Arajet calendar %s→%s: %s", req.origin, req.destination, exc)
            return self._empty(req)

        offers = self._parse(data, req, target)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Arajet %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"arajet{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers,
            total_results=len(offers),
        )

    # ------------------------------------------------------------------

    def _parse(self, data: dict, req: FlightSearchRequest, target: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        for month_block in data.get("months", {}).get("items", []):
            for week in month_block.get("weeks", {}).get("items", []):
                for day in week.get("days", {}).get("items", []):
                    if day.get("description") != target:
                        continue
                    fps = day.get("fareProducts")
                    if not fps:
                        continue
                    seen: set[str] = set()
                    for fp in fps.get("items", []):
                        for price_item in fp.get("prices", {}).get("items", []):
                            total = price_item.get("totalAmount", {})
                            base = price_item.get("baseAmount", {})
                            amount = total.get("value", 0)
                            currency = total.get("currency", {}).get("code", "USD")
                            if amount <= 0:
                                continue
                            code = price_item.get("code", "")
                            dedup = f"{code}_{amount}"
                            if dedup in seen:
                                continue
                            seen.add(dedup)

                            cabin = self._cabin_label(fp.get("code", "E"), code)
                            offers.append(self._build_offer(
                                req, target, amount, currency, cabin, code,
                            ))
        return offers

    @staticmethod
    def _cabin_label(fare_class: str, code: str) -> str:
        variant = ""
        for k, v in _VARIANT_LABELS.items():
            if k in code.upper():
                variant = v
                break
        cls_map = {"E": "Economy", "S": "Economy", "H": "Economy"}
        cls = cls_map.get(fare_class, "Economy")
        return f"{cls} {variant}".strip() if variant else cls

    def _build_offer(
        self, req: FlightSearchRequest, target: str,
        total: float, currency: str, cabin: str, code: str,
    ) -> FlightOffer:
        dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
        seg = FlightSegment(
            airline="Arajet", flight_no="", origin=req.origin,
            destination=req.destination, departure=dep_dt, arrival=dep_dt,
            duration_seconds=0,
        )
        route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
        oid = hashlib.md5(
            f"dm_{req.origin}{req.destination}{target}{total}{code}".encode()
        ).hexdigest()[:12]
        return FlightOffer(
            id=f"dm_{oid}",
            price=round(total, 2),
            currency=currency,
            price_formatted=f"{total:.2f} {currency} ({cabin})",
            outbound=route,
            inbound=None,
            airlines=["Arajet"],
            owner_airline="DM",
            conditions={"cabin": cabin},
            booking_url=(
                f"https://www.arajet.com/en-us/booking/select"
                f"?origin={req.origin}&destination={req.destination}"
                f"&date={target}&adt={req.adults or 1}"
            ),
            is_locked=False,
            source="arajet_direct",
            source_tier="free",
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id="fs_empty", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
