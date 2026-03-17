"""Air India connector — BLOCKED.

Air India (IATA: AI) — DEL/BOM hubs, Star Alliance, Tata Group.

Blocked reason:
  - All httpx requests to airindia.com return RemoteProtocolError
    (HTTP/2 stream resets on every endpoint).
  - Website uses shadow DOM and lazy-loaded web components — no visible
    form inputs discoverable via Playwright.
  - Countries API works in browser (api.airindia.com/cbiz-uam/v1/common/countries)
    but no flight search endpoints are accessible.
  - Probed extensively: 2026-03-16.
"""

from __future__ import annotations

import logging

from models.flights import FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class AirIndiaConnectorClient:
    """Air India — BLOCKED (HTTP/2 stream resets, shadow DOM)."""

    def __init__(self, timeout: float = 25.0):
        pass

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        logger.debug("Air India connector blocked — HTTP/2 stream resets")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="INR", offers=[], total_results=0,
        )
