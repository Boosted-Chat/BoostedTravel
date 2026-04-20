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

from ..models.flights import FlightOffer, FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


class JapanAirlinesConnectorClient:
    """Japan Airlines — BLOCKED (traditional form POST, no API)."""

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
        logger.debug("JAL connector blocked — traditional form POST, no API")
        return FlightSearchResponse(
            search_id="fs_blocked", origin=req.origin, destination=req.destination,
            currency="JPY", offers=[], total_results=0,
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
                    id=f"rt_jal_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
