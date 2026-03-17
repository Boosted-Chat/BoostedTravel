"""Qantas Airways connector — BLOCKED.

Qantas (IATA: QF) — SYD/MEL hubs, oneworld member.

Blocked reason:
  - Market-pricing API (api.qantas.com/market-pricing/mpp/v1/market/supported-routes)
    returns 403 via httpx AND curl_cffi. Only accessible during live browser sessions
    with specific cookies/tokens.
  - page.evaluate(fetch) blocked by CORS (separate api.qantas.com domain).
  - Route search API works (flight/routesearch/v1/airports) but returns only
    airport/route data, no pricing.
  - Search form submits redirect to /en-au/book/flights with zero capturable API calls.
  - Probed extensively: 2026-03-16.
"""

from __future__ import annotations

import logging

from models.flights import FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class QantasConnectorClient:
    """Qantas — BLOCKED (no accessible pricing API)."""

    def __init__(self, timeout: float = 25.0):
        pass

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        logger.debug("Qantas connector blocked — no accessible pricing API")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="AUD", offers=[], total_results=0,
        )
