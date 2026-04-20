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

from ..models.flights import FlightOffer, FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class EgyptAirConnectorClient:
    """EgyptAir — BLOCKED (page not found, SharePoint)."""

    def __init__(self, timeout: float = 20.0):
        pass

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        logger.debug("EgyptAir connector blocked — no accessible booking page")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="EGP", offers=[], total_results=0,
        )


    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req,
    ) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_egyp_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
