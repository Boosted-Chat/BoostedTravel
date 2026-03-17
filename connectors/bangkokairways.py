"""Bangkok Airways connector — BLOCKED.

Bangkok Airways (IATA: PG) — BKK/USM hubs.

Blocked reason:
  - All httpx requests return 403 (WAF protection).
  - Menu API works in Playwright (webapi.bangkokair.com/menuNavigation)
    but no visible form inputs on the booking page.
  - No accessible flight search or pricing API endpoints found.
  - Probed extensively: 2026-03-16.
"""

from __future__ import annotations

import logging

from models.flights import FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class BangkokAirwaysConnectorClient:
    """Bangkok Airways — BLOCKED (WAF, no visible form)."""

    def __init__(self, timeout: float = 20.0):
        pass

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        logger.debug("Bangkok Airways connector blocked — WAF protection")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="THB", offers=[], total_results=0,
        )
