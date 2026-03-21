"""
Discover Airlines curl_cffi connector -- extracts flight schedules and fares
from lufthansa.com SEO flight pages via structured JSON-LD data.

Discover Airlines (IATA: 4Y, formerly Eurowings Discover) is a Lufthansa Group
leisure carrier based in Frankfurt. Operates medium/long-haul flights to
holiday destinations in Europe, Caribbean, Africa, and the Americas.
Main hub: FRA (Frankfurt), secondary: MUC (Munich). Default currency EUR.

Strategy:
  Same as other LH Group connectors — the lufthansa.com flight pages
  contain JSON-LD with 4Y-operated flights (verified via probe).
"""

from __future__ import annotations

from connectors.lhgroup_base import LHGroupBaseConnector


class DiscoverConnectorClient(LHGroupBaseConnector):
    """Discover Airlines connector -- LH page JSON-LD extraction via curl_cffi."""

    AIRLINE_CODE = "4Y"
    AIRLINE_NAME = "Discover Airlines"
    SOURCE_KEY = "discover_direct"
    DEFAULT_CURRENCY = "EUR"
    BOOKING_URL_TEMPLATE = (
        "https://www.lufthansa.com/xx/en/flight-search?"
        "origin={origin}&destination={destination}"
        "&outbound-date={date}"
        "&adults={adults}&children={children}"
        "&infants={infants}&cabin-class=economy&trip-type=ONE_WAY"
    )
