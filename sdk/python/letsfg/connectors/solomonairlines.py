"""
Solomon Airlines connector — Next.js RSC route pages via requests.

Solomon Airlines (IATA: IE) is the national airline of the Solomon Islands.
Hub at Honiara (HIR) with routes to Brisbane, Nadi, Port Vila, and
domestic Solomon Islands destinations.

Strategy:
  1. Resolve IATA codes to city slugs and destination-country category
  2. Fetch route page: www.flysolomons.com/explore/destinations/{category}/{o}-to-{d}
  3. Extract initialPage.fares from RSC self.__next_f.push() inline scripts
  4. Parse Fare objects: fareFamily, price, currency, tripType
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime

import requests as _req

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.flysolomons.com"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# IATA → URL city slug
_IATA_TO_SLUG: dict[str, str] = {
    # Solomon Islands
    "HIR": "honiara", "GZO": "gizo", "MUA": "munda",
    "IRA": "arona", "SCZ": "santa-cruz", "AFT": "atoifi",
    "AKS": "auki", "BAS": "balalae", "BNY": "bellona",
    "LML": "lomlom", "PRS": "parasi", "RNL": "rennell",
    "NAZ": "santa-ana", "RUA": "suavanao",
    # Australia
    "BNE": "brisbane", "SYD": "sydney", "MEL": "melbourne",
    "ADL": "adelaide", "PER": "perth", "CBR": "canberra",
    "DRW": "darwin", "CNS": "cairns", "MKY": "mackay",
    "TSV": "townsville",
    # New Zealand
    "AKL": "auckland", "CHC": "christchurch", "WLG": "wellington",
    "DUD": "dunedin",
    # Fiji
    "NAN": "nadi",
    # Vanuatu
    "VLI": "port-vila", "SON": "santo",
    # Papua New Guinea
    "POM": "port-moresby",
}

# Destination IATA → URL country-category segment
_DEST_CATEGORY: dict[str, str] = {
    # Solomon Islands (all domestic + main hub)
    "HIR": "flights-to-solomon-islands",
    "GZO": "flights-to-solomon-islands",
    "MUA": "flights-to-solomon-islands",
    "IRA": "flights-to-solomon-islands",
    "SCZ": "flights-to-solomon-islands",
    "AFT": "flights-to-solomon-islands",
    "AKS": "flights-to-solomon-islands",
    "BAS": "flights-to-solomon-islands",
    "BNY": "flights-to-solomon-islands",
    "LML": "flights-to-solomon-islands",
    "PRS": "flights-to-solomon-islands",
    "RNL": "flights-to-solomon-islands",
    "NAZ": "flights-to-solomon-islands",
    "RUA": "flights-to-solomon-islands",
    # Australia
    "BNE": "flights-to-australia",
    "SYD": "flights-to-australia",
    "MEL": "flights-to-australia",
    "ADL": "flights-to-australia",
    "PER": "flights-to-australia",
    "CBR": "flights-to-australia",
    "DRW": "flights-to-australia",
    "CNS": "flights-to-australia",
    "MKY": "flights-to-australia",
    "TSV": "flights-to-australia",
    # New Zealand
    "AKL": "flights-to-new-zealand",
    "CHC": "flights-to-new-zealand",
    "WLG": "flights-to-new-zealand",
    "DUD": "flights-to-new-zealand",
    # Vanuatu
    "VLI": "flights-to-vanuatu",
    "SON": "flights-to-vanuatu",
}


class SolomonAirlinesConnectorClient:
    """Solomon Airlines (IE) — Next.js RSC route fare pages via requests."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={
                "origin": req.destination, "destination": req.origin,
                "date_from": req.return_from, "return_from": None,
            })
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        dest_category = _DEST_CATEGORY.get(req.destination)
        if not origin_slug or not dest_slug or not dest_category:
            logger.warning("Solomon Airlines: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/explore/destinations/{dest_category}/{origin_slug}-to-{dest_slug}"
        logger.info("Solomon Airlines: fetching %s", url)

        try:
            html = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync, url
            )
        except Exception as e:
            logger.error("Solomon Airlines fetch error: %s", e)
            return self._empty(req)

        if not html:
            return self._empty(req)

        offers = self._extract_offers(html, req)

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info(
            "Solomon Airlines %s->%s: %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"solomonairlines{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "AUD",
            offers=offers,
            total_results=len(offers),
        )

    def _fetch_sync(self, url: str) -> str | None:
        try:
            r = _req.get(url, headers=_HEADERS, timeout=int(self.timeout))
            if r.status_code != 200:
                logger.warning("Solomon Airlines: %s returned %d", url, r.status_code)
                return None
            return r.text
        except Exception as e:
            logger.warning("Solomon Airlines fetch error: %s", e)
            return None

    def _extract_offers(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract fares from RSC self.__next_f.push() scripts → initialPage.fares."""
        # Extract all RSC chunks
        rsc_chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)

        fares: list[dict] = []
        for chunk in rsc_chunks:
            try:
                decoded = json.loads(f'"{chunk}"')
            except Exception:
                decoded = chunk
            if '"initialPage"' not in decoded:
                continue
            # Find the fares array within initialPage
            m = re.search(r'"fares"\s*:\s*(\[.*?\])', decoded, re.DOTALL)
            if m:
                try:
                    fares = json.loads(m.group(1))
                    break
                except Exception:
                    pass

        if not fares:
            logger.info("Solomon Airlines: no initialPage.fares found at %s->%s",
                        req.origin, req.destination)
            return []

        dep_dt = datetime.combine(req.date_from, datetime.min.time()) if hasattr(req.date_from, 'year') else datetime(2000, 1, 1)
        offers: list[FlightOffer] = []

        for fare in fares:
            if not isinstance(fare, dict):
                continue
            price = fare.get("price")
            currency = fare.get("currency", "AUD")
            fare_family = fare.get("fareFamily", "Economy")
            if not price or float(price) <= 0:
                continue

            price_f = round(float(price), 2)
            cabin = "business" if fare_family.lower() == "business" else "economy"

            seg = FlightSegment(
                airline="IE",
                airline_name="Solomon Airlines",
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=cabin,
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            fid = hashlib.md5(
                f"ie_{req.origin}{req.destination}{price_f}{fare_family}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"ie_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=f"{price_f:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Solomon Airlines"],
                owner_airline="IE",
                booking_url=(
                    f"https://www.flysolomons.com/book/"
                    f"?from={req.origin}&to={req.destination}"
                    f"&adultCount={req.adults or 1}"
                    f"&tripType={'ROUND_TRIP' if req.return_from else 'ONE_WAY'}"
                ),
                is_locked=False,
                source="solomonairlines_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req,
    ) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for o in ob[:10]:
            for i in ib[:5]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_solo_{cid}", price=price, currency=o.currency,
                    price_formatted=f"{price:.2f} {o.currency}",
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"solomonairlines{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="AUD",
            offers=[],
            total_results=0,
        )
