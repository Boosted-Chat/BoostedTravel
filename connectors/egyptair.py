"""EgyptAir connector — BLOCKED.

EgyptAir (IATA: MS) — CAI hub, Star Alliance.

Blocked reason:
  - Booking URL returns "Page Not Found".
  - Website is SharePoint-based with Firebase analytics.
  - No accessible flight search or pricing API endpoints found.
  - Probed extensively: 2026-03-16.
"""

from __future__ import annotations

import logging

from models.flights import FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class EgyptAirConnectorClient:
    """EgyptAir — BLOCKED (page not found, SharePoint)."""

    def __init__(self, timeout: float = 20.0):
        pass

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        logger.debug("EgyptAir connector blocked — no accessible booking page")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="EGP", offers=[], total_results=0,
        )
