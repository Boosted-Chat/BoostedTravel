"""
Aircalin connector — www.aircalin.com.

Aircalin (IATA: SB) is the flag carrier of New Caledonia.
Hub at Nouméa Tontouta (NOU) with routes to Australia, New Zealand,
Japan, Fiji, Vanuatu, Wallis & Futuna, and French Polynesia.

Status: fare data source unavailable as of 2026.
  - flights.aircalin.com: permanently offline (connection reset)
  - book.aircalin.com/plnext/FPCaircalinDX/: Cloudflare WAF blocked
  - www.aircalin.com: Drupal CMS with no structured fare data
  Connector returns empty gracefully until a new data source is found.
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

_BASE = "https://www.aircalin.com"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Static slug mapping for Aircalin destinations
_IATA_TO_SLUG: dict[str, str] = {
    # New Caledonia
    "NOU": "noumea", "ILP": "isle-of-pines", "LIF": "lifou",
    "MEE": "mare", "UVE": "ouvea",
    # Australia
    "SYD": "sydney", "MEL": "melbourne", "BNE": "brisbane",
    # New Zealand
    "AKL": "auckland",
    # Japan
    "NRT": "tokyo", "KIX": "osaka",
    # Pacific Islands
    "NAN": "nadi", "VLI": "port-vila",
    "WLS": "wallis", "FUT": "futuna",
    "PPT": "papeete",
}


class AircalinConnectorClient:
    """Aircalin (SB) — data source unavailable; returns empty."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return await self._search_ow(req)

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # flights.aircalin.com is permanently offline; book.aircalin.com/plnext/
        # Datalex IBE is Cloudflare WAF-blocked. No accessible fare source.
        logger.debug("Aircalin: data source unavailable, returning empty")
        return self._empty(req)

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"aircalin{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="XPF",
            offers=[],
            total_results=0,
        )
