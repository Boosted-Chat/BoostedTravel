"""
LetsFG — Agent-native flight search & booking SDK.

Search hundreds of airlines via the LetsFG cloud engine. Connect a card once at
letsfg.co/connect -- nothing is charged -- then search instantly.

Quick start (CLI):
    letsfg auth               # one-time card connect, opens a browser
    letsfg search WAW BCN 2026-07-15

Programmatic search (free, requires Bearer token):
    from letsfg.local import search_local
    import asyncio
    result = asyncio.run(search_local("SHA", "CTU", "2026-03-20"))

Full API (search + book, requires API key). No unlock step — it was retired
2026-09-08 and unlock() now raises:
    import os
    from letsfg import LetsFG
    bt = LetsFG(api_key=os.environ["LETSFG_API_KEY"])
    flights = bt.search("GDN", "BER", "2026-03-03")
    bt.book(flights.offers[0].id, passengers=[{...}], contact_email="you@example.com",
            search_id=flights.search_id)
"""

from letsfg.client import (
    LetsFG,
    LetsFGError,
    AuthenticationError,
    PaymentRequiredError,
    OfferExpiredError,
    ValidationError,
    ErrorCode,
    ErrorCategory,
)
from letsfg.models import (
    FlightOffer,
    FlightSearchResult,
    FlightSegment,
    FlightRoute,
    UnlockResult,
    BookingResult,
    Passenger,
    AgentProfile,
)
from letsfg.models.flights import PublicFlightOffer, to_public_offer

__version__ = "2026.5.98"
__all__ = [
    "LetsFG",
    "LetsFGError",
    "AuthenticationError",
    "PaymentRequiredError",
    "OfferExpiredError",
    "ValidationError",
    "ErrorCode",
    "ErrorCategory",
    "FlightOffer",
    "FlightSearchResult",
    "FlightSegment",
    "FlightRoute",
    "UnlockResult",
    "BookingResult",
    "Passenger",
    "AgentProfile",
    "PublicFlightOffer",
    "to_public_offer",
]
