"""
Austrian Airlines curl_cffi connector -- extracts lowest fares from
lufthansa.com shared flight pages via structured JSON-LD data.

Austrian Airlines (IATA: OS) is Austria's flag carrier based in Vienna.
Part of Lufthansa Group. Operates European and long-haul flights from
VIE (Vienna) hub. Default currency EUR.

Strategy: Same as Lufthansa -- uses shared lufthansa.com/xx/en/flights/
pages which cover all LH Group airline routes including Austrian.
"""

from __future__ import annotations

from .lhgroup_base import LHGroupBaseConnector


class AustrianConnectorClient(LHGroupBaseConnector):
    """Austrian Airlines connector -- LH Group shared flight page extraction."""

    AIRLINE_CODE = "OS"
    AIRLINE_NAME = "Austrian Airlines"
    SOURCE_KEY = "austrian_direct"
    DEFAULT_CURRENCY = "EUR"
    BOOKING_URL_TEMPLATE = (
        "https://www.lufthansa.com/xx/en/flight-search?"
        "origin={origin}&destination={destination}"
        "&outbound-date={date}"
        "&adults={adults}&children={children}"
        "&infants={infants}&cabin-class=economy&trip-type=ONE_WAY"
    )
