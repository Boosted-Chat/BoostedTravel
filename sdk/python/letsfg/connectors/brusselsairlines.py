"""
Brussels Airlines curl_cffi connector -- extracts lowest fares from
lufthansa.com shared flight pages via structured JSON-LD data.

Brussels Airlines (IATA: SN) is Belgium's flag carrier based in Brussels.
Part of Lufthansa Group. Operates European and African flights from
BRU (Brussels) hub. Default currency EUR.

Strategy: Same as Lufthansa -- uses shared lufthansa.com/xx/en/flights/
pages which cover all LH Group airline routes including Brussels Airlines.
"""

from __future__ import annotations

from .lhgroup_base import LHGroupBaseConnector


class BrusselsAirlinesConnectorClient(LHGroupBaseConnector):
    """Brussels Airlines connector -- LH Group shared flight page extraction."""

    AIRLINE_CODE = "SN"
    AIRLINE_NAME = "Brussels Airlines"
    SOURCE_KEY = "brusselsairlines_direct"
    DEFAULT_CURRENCY = "EUR"
    BOOKING_URL_TEMPLATE = (
        "https://www.brusselsairlines.com/be/en/book/offers/flights?"
        "origin={origin}&destination={destination}"
        "&outbound-date={date}"
        "&adults={adults}&children={children}"
        "&infants={infants}&cabin-class=economy&trip-type=ONE_WAY"
    )
