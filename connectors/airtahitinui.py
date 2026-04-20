"""
Air Tahiti Nui connector — book.airtahitinui.com (Amadeus e-Retail).

Air Tahiti Nui (IATA: TN) is the flag carrier of French Polynesia.
Hub at Papeete Faa'a (PPT) with routes to Los Angeles, Auckland,
Tokyo, Paris, and Seattle.

Status: booking engine discovered at book.airtahitinui.com (Amadeus e-Retail).
  - Old domains (flights/booking.airtahitinui.com): DNS dead
  - New booking engine: book.airtahitinui.com/plnext/AirTahitiNuiDX/
  Connector returns empty; fare scraping TBD (Amadeus SPA needs Playwright).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime

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

_BASE = "https://book.airtahitinui.com"
_BOOKING_URL = "https://book.airtahitinui.com/plnext/AirTahitiNuiDX/Override.action?LANGUAGE=US&SO_SITE_MARKET_ID=AU&SITE=A03OA03O#/FDCS"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Static slug mapping for Air Tahiti Nui destinations
_IATA_TO_SLUG: dict[str, str] = {
    # French Polynesia
    "PPT": "papeete", "BOB": "bora-bora", "MOZ": "moorea",
    "RGI": "rangiroa", "FAC": "fakarava",
    # USA
    "LAX": "los-angeles", "SEA": "seattle",
    # New Zealand
    "AKL": "auckland",
    # Japan
    "NRT": "tokyo",
    # France
    "CDG": "paris",
    # Cook Islands
    "RAR": "rarotonga",
}


class AirTahitiNuiConnectorClient:
    """Air Tahiti Nui (TN) — booking engine at book.airtahitinui.com; fare scraping TBD."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return await self._search_ow(req)

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Booking engine at book.airtahitinui.com (Amadeus e-Retail SPA).
        # Fare scraping not yet implemented — needs Playwright + API interception.
        logger.debug("AirTahitiNui: fare scraping TBD (Amadeus SPA), returning empty")
        return self._empty(req)

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"airtahitinui{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="XPF",
            offers=[],
            total_results=0,
        )
