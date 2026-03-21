"""
Swiss International Air Lines curl_cffi connector -- extracts lowest fares
from lufthansa.com shared flight pages via structured JSON-LD data.

Swiss (IATA: LX) is Switzerland's flag carrier based in Zurich. Part of
Lufthansa Group. Operates European and long-haul flights from ZRH (Zurich)
and GVA (Geneva) hubs. Default currency CHF.

Strategy: Same as Lufthansa -- uses shared lufthansa.com/xx/en/flights/
pages which cover all LH Group airline routes including Swiss.
"""

from __future__ import annotations

from connectors.lhgroup_base import LHGroupBaseConnector


class SwissConnectorClient(LHGroupBaseConnector):
    """Swiss connector -- LH Group shared flight page extraction."""

    AIRLINE_CODE = "LX"
    AIRLINE_NAME = "Swiss"
    SOURCE_KEY = "swiss_direct"
    DEFAULT_CURRENCY = "CHF"
    BOOKING_URL_TEMPLATE = (
        "https://www.swiss.com/ch/en/book/offers/flights?"
        "origin={origin}&destination={destination}"
        "&outbound-date={date}"
        "&adults={adults}&children={children}"
        "&infants={infants}&cabin-class=economy&trip-type=ONE_WAY"
    )
