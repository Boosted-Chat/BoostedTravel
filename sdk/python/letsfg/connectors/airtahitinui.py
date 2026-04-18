"""
Air Tahiti Nui connector — www.airtahitinui.com.

Air Tahiti Nui (IATA: TN) is the flag carrier of French Polynesia.
Hub at Papeete Faa'a (PPT) with routes to Los Angeles, Auckland,
Tokyo, Paris, and Seattle.

Status: fare data source unavailable as of 2026.
  - flights.airtahitinui.com: DNS dead
  - booking.airtahitinui.com: DNS dead
  - www.airtahitinui.com: Drupal CMS with no structured fare data or booking widget
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

_BASE = "https://www.airtahitinui.com"
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
    """Air Tahiti Nui (TN) — data source unavailable; returns empty."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return await self._search_ow(req)

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # flights.airtahitinui.com DNS dead; www.airtahitinui.com is a Drupal CMS
        # with no booking widget or structured fare data accessible.
        logger.debug("AirTahitiNui: data source unavailable, returning empty")
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
