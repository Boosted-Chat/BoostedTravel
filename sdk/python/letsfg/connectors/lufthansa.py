"""
Lufthansa curl_cffi connector -- extracts flight schedules and lowest fares
from lufthansa.com SEO flight pages via structured JSON-LD data.

Lufthansa (IATA: LH) is Germany's flag carrier based in Frankfurt and Munich.
Operates short/medium/long-haul flights across Europe, Americas, Asia, Africa,
and the Middle East. Main hubs: FRA (Frankfurt), MUC (Munich). Default currency EUR.

Strategy:
1. Map IATA codes to URL city slugs used by lufthansa.com/xx/en/flights/
2. Fetch the flight page: lufthansa.com/xx/en/flights/flight-{origin}-{dest}
3. Extract JSON-LD: Flight entries (schedules) + Product entry (lowest price)
4. Build FlightOffers with schedule data and the route-level lowest price
"""

from __future__ import annotations

from connectors.lhgroup_base import LHGroupBaseConnector


class LufthansaConnectorClient(LHGroupBaseConnector):
    """Lufthansa connector -- flight page JSON-LD extraction via curl_cffi."""

    AIRLINE_CODE = "LH"
    AIRLINE_NAME = "Lufthansa"
    SOURCE_KEY = "lufthansa_direct"
    DEFAULT_CURRENCY = "EUR"
    BOOKING_URL_TEMPLATE = (
        "https://www.lufthansa.com/xx/en/flight-search?"
        "origin={origin}&destination={destination}"
        "&outbound-date={date}"
        "&adults={adults}&children={children}"
        "&infants={infants}&cabin-class=economy&trip-type=ONE_WAY"
    )
