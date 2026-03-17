"""Japan Airlines connector — BLOCKED.

Japan Airlines (IATA: JL) — NRT/HND hubs, oneworld member.

Blocked reason:
  - No JSON API for flight search. Booking is a traditional HTML form POST
    to book-i.jal.co.jp/JLInt/dyn/air/booking/availability with hidden fields
    (SITE, LANGUAGE, COUNTRY_SITE, DEVICE_TYPE, FLOW_MODE, etc.).
  - Route page at /jp/en/inter/route/ returns 52KB HTML (route map, no fare data).
  - No lowfare calendar or search API endpoints found.
  - Probed extensively: 2026-03-16.
"""

from __future__ import annotations

import logging

from models.flights import FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class JapanAirlinesConnectorClient:
    """Japan Airlines — BLOCKED (traditional form POST, no API)."""

    def __init__(self, timeout: float = 20.0):
        pass

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        logger.debug("JAL connector blocked — traditional form POST, no API")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="JPY", offers=[], total_results=0,
        )
