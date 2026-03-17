"""Garuda Indonesia connector — BLOCKED.

Garuda Indonesia (IATA: GA) — CGK/DPS hubs, SkyTeam member.

Blocked reason:
  - React SPA with no accessible flight search API.
  - Direct search URL (/sg/en/search?ori=CGK&dst=DPS&...) loads CMS content
    search page, not flight results.
  - All booking API endpoints (web-api.garuda-indonesia.com) blocked by CORS
    when called from page.evaluate.
  - Form fill unreliable: radio buttons resolve to multiple elements,
    autocomplete dropdowns timeout.
  - Airport API works (POST airport-api.garuda-indonesia.com/ga/revamp/v1.0/data/airport/)
    but no flight pricing endpoints are accessible.
  - Probed extensively: 2026-03-16.
"""

from __future__ import annotations

import logging

from models.flights import FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class GarudaConnectorClient:
    """Garuda Indonesia — BLOCKED (no accessible flight search API)."""

    def __init__(self, timeout: float = 20.0):
        pass

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        logger.debug("Garuda connector blocked — no accessible API")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="IDR", offers=[], total_results=0,
        )
