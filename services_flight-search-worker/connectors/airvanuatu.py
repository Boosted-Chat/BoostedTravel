"""
Air Vanuatu connector — www.airvanuatu.com.

Air Vanuatu (IATA: NF) is the flag carrier of Vanuatu.
Hub at Port Vila (VLI) with routes to Australia, New Zealand,
Fiji, New Caledonia, and domestic Vanuatu destinations.

Status: fare data source unavailable as of 2026.
  - flights.airvanuatu.com: DNS dead
  - fo-syd.ttinteractive.com/Zenith/FrontOffice/AirVanuatu/: Cloudflare WAF blocks
    all requests (403) even with curl_cffi impersonation
  - www.airvanuatu.com: works but has no structured fare data
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

_BASE = "https://www.airvanuatu.com"
_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Static slug mapping for Air Vanuatu destinations
_IATA_TO_SLUG: dict[str, str] = {
    # Vanuatu
    "VLI": "port-vila", "SON": "santo", "TGH": "tanna",
    # Australia
    "SYD": "sydney", "MEL": "melbourne", "BNE": "brisbane",
    # New Zealand
    "AKL": "auckland",
    # Pacific
    "NAN": "nadi", "NOU": "noumea", "HIR": "honiara",
}


class AirVanuatuConnectorClient:
    """Air Vanuatu (NF) — data source unavailable; returns empty."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return await self._search_ow(req)

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # flights.airvanuatu.com DNS dead; fo-syd.ttinteractive.com Zenith is
        # Cloudflare WAF-blocked (403). No accessible fare source.
        logger.debug("AirVanuatu: data source unavailable, returning empty")
        return self._empty(req)

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"airvanuatu{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="AUD",
            offers=[],
            total_results=0,
        )
