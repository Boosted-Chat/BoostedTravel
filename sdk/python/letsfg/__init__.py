"""
LetsFG — Agent-native flight search & booking SDK.

200 airline connectors run locally + enterprise GDS/NDC APIs via backend.

**GitHub Star Required!**
Before using, you must:
1. Star https://github.com/LetsFG/LetsFG
2. Run: letsfg star --github <your-username>

Local search (FREE with star):
    from letsfg.local import search_local
    result = await search_local("SHA", "CTU", "2026-03-20")

Full API (search + unlock + book):
    from letsfg import LetsFG
    bt = LetsFG(api_key="trav_...")
    flights = bt.search("GDN", "BER", "2026-03-03")
    bt.unlock(flights.offers[0].id)
    bt.book(flights.offers[0].id, passenger={...})
"""

from letsfg.client import (
    LetsFG,
    LetsFGError,
    BoostedTravel,
    BoostedTravelError,
    AuthenticationError,
    PaymentRequiredError,
    OfferExpiredError,
    ValidationError,
    ErrorCode,
    ErrorCategory,
)
from letsfg.config import StarRequiredError
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

__version__ = "2026.4.47"
__all__ = [
    "LetsFG",
    "LetsFGError",
    "BoostedTravel",      # deprecated alias
    "BoostedTravelError", # deprecated alias,
    "AuthenticationError",
    "PaymentRequiredError",
    "OfferExpiredError",
    "ValidationError",
    "StarRequiredError",
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
    "get_system_profile",
    "configure_max_browsers",
]

# Lazy imports for system/concurrency utilities
def get_system_profile():
    """Detect system resources (RAM, CPU) and return optimal concurrency settings."""
    from letsfg.system_info import get_system_profile as _get
    return _get()

def configure_max_browsers(n: int):
    """Set max concurrent browser processes (1-32). Call before search_local()."""
    from letsfg.connectors.browser import configure_max_browsers as _cfg
    _cfg(n)
