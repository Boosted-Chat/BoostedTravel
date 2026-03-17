"""ITA Airways connector — BLOCKED.

ITA Airways (IATA: AZ) — FCO/MXP hubs, SkyTeam member.

Blocked reason:
  - Fare teaser API (POST /service/api/lhg-fare-teaser/fareteaser/offers/{city})
    requires DecisionId session token (Adobe Target-like). Returns
    {"error": "Invalid DecisionId"} without valid browser session.
  - Required headers discovered: X-Portal-Site: XX, X-Portal: ITA,
    X-Portal-Language: en, X-Portal-Countryid, X-Portal-Taxonomy — but
    DecisionId is still needed.
  - All other fare endpoints (/fareteaser/destinations, /origins, /config,
    /calendar, /lowestfares) return 403 (Cloudflare).
  - Service APIs work (catalogs/calendar, catalogs/airportnames, core/booking)
    but contain no pricing data.
  - Probed extensively: 2026-03-16.
"""

from __future__ import annotations

import logging

from models.flights import FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class ITAAirwaysConnectorClient:
    """ITA Airways — BLOCKED (DecisionId requirement, Cloudflare)."""

    def __init__(self, timeout: float = 20.0):
        pass

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        logger.debug("ITA Airways connector blocked — DecisionId required")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="EUR", offers=[], total_results=0,
        )
