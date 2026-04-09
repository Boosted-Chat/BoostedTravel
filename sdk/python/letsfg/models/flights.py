"""Flight search request/response models — multi-provider."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


# ── Request ──────────────────────────────────────────────────────────────────

class FlightSearchRequest(BaseModel):
    """Parameters for a flight search — FREE for agents."""

    origin: str = Field(
        ...,
        description="IATA code of departure airport/city (e.g. 'PRG', 'LON')",
        min_length=2,
        max_length=4,
    )
    destination: str = Field(
        ...,
        description="IATA code of arrival airport/city (e.g. 'BCN', 'NYC')",
        min_length=2,
        max_length=4,
    )
    date_from: date = Field(..., description="Departure date")
    date_to: Optional[date] = Field(None, description="Latest departure date")
    return_from: Optional[date] = Field(None, description="Return date (omit for one-way)")
    return_to: Optional[date] = Field(None, description="Latest return date")
    adults: int = Field(1, ge=1, le=9)
    children: int = Field(0, ge=0, le=9)
    infants: int = Field(0, ge=0, le=9)
    cabin_class: Optional[str] = Field(
        None,
        description="M (economy), W (premium economy), C (business), F (first)",
        pattern=r"^[MWCF]$",
    )
    max_stopovers: int = Field(2, ge=0, le=4, description="Max connections per direction")
    currency: str = Field("EUR", min_length=3, max_length=3)
    locale: str = Field("en", description="Language for city/airport names")
    limit: int = Field(50, ge=1, le=200, description="Max results to return")
    sort: str = Field("price", description="Sort by: price, duration, departure_time")
    departure_time_from: Optional[str] = Field(
        None,
        description="Earliest departure time HH:MM (e.g. '06:00' for morning flights)",
        pattern=r"^([01]?[0-9]|2[0-3]):[0-5][0-9]$",
    )
    departure_time_to: Optional[str] = Field(
        None,
        description="Latest departure time HH:MM (e.g. '14:00' for flights before 2pm)",
        pattern=r"^([01]?[0-9]|2[0-3]):[0-5][0-9]$",
    )

    @field_validator("origin", "destination")
    @classmethod
    def validate_iata_code(cls, v: str) -> str:
        v = v.strip().upper()
        if not re.fullmatch(r"[A-Z]{2,4}", v):
            raise ValueError(f"Invalid IATA code '{v}': must be 2-4 letters (e.g. 'LON', 'PRG')")
        return v

    @field_validator("date_from")
    @classmethod
    def validate_date_not_past(cls, v: date) -> date:
        if v < date.today():
            raise ValueError(f"date_from ({v}) cannot be in the past")
        return v


# ── Response ─────────────────────────────────────────────────────────────────

class FlightSegment(BaseModel):
    """A single flight leg (e.g., PRG→FRA)."""

    airline: str = Field(..., description="Operating carrier IATA code")
    airline_name: str = ""
    flight_no: str = ""
    origin: str = Field(..., description="Departure IATA")
    destination: str = Field(..., description="Arrival IATA")
    origin_city: str = ""
    destination_city: str = ""
    departure: datetime
    arrival: datetime
    duration_seconds: int = 0
    cabin_class: str = "economy"
    aircraft: str = ""


class FlightRoute(BaseModel):
    """One direction (outbound or return) composed of segments."""

    segments: list[FlightSegment] = []
    total_duration_seconds: int = 0
    stopovers: int = 0


class FlightOffer(BaseModel):
    """
    A single flight offer.

    A single flight offer with full itinerary, pricing, and booking details.
    """

    id: str = Field(..., description="Unique offer ID")
    price: float
    currency: str = "EUR"
    price_formatted: str = ""
    outbound: FlightRoute
    inbound: Optional[FlightRoute] = None
    airlines: list[str] = Field(default_factory=list, description="All airlines in itinerary")
    owner_airline: str = Field("", description="Validating carrier")
    bags_price: dict[str, Any] = Field(default_factory=dict, description="Baggage pricing")
    availability_seats: Optional[int] = None
    conditions: dict[str, str] = Field(default_factory=dict, description="Refund/change policies")
    source: str = Field("", description="Provider source tag (e.g. 'duffel', 'amadeus', 'kiwi', 'travelpayouts')")
    source_tier: str = Field(
        "paid",
        description=(
            "Data source cost tier: "
            "'free' = cached/aggregated data (Travelpayouts), "
            "'low' = affordable API (Kiwi Tequila), "
            "'paid' = GDS/NDC providers (Duffel, Amadeus), "
            "'protocol' = LCC direct via Agent Interaction Protocol (Ryanair, Wizzair)"
        ),
    )
    is_locked: bool = Field(False, description="Whether booking details require unlock")
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    booking_url: str = Field("", description="Only available after unlock")
    price_normalized: Optional[float] = Field(None, description="Price converted to the search currency for sorting")


class AirlineSummary(BaseModel):
    """Cheapest offer summary for one airline."""
    airline_code: str
    airline_name: str = ""
    cheapest_price: float
    currency: str = "EUR"
    offer_count: int
    cheapest_offer_id: str = ""
    sample_route: str = Field("", description="e.g. KRK→WAW→BER")


class FlightSearchResponse(BaseModel):
    """Full response from a flight search — always FREE."""

    search_id: str = ""
    offer_request_id: str = Field("", description="Offer request ID (for booking flow)")
    passenger_ids: list[str] = Field(
        default_factory=list,
        description="Passenger IDs from the offer request — REQUIRED for booking. "
        "Map these 1:1 to your passengers when calling POST /bookings/book.",
    )

    origin: str
    destination: str
    currency: str = "EUR"
    offers: list[FlightOffer] = []
    total_results: int = 0
    airlines_summary: list[AirlineSummary] = Field(
        default_factory=list,
        description="Cheapest offer per airline — quick overview of all options.",
    )
    search_params: dict = Field(default_factory=dict)
    source_tiers: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Breakdown of which source tiers were used in this search. "
            "Keys are tier names, values describe the data source. "
            "Example: {'paid': 'Duffel (NDC), Amadeus (GDS)', 'low': 'Kiwi Tequila', 'free': 'Travelpayouts'}"
        ),
    )
    pricing_note: str = Field(
        default="Search is free. Booking is free. No hidden fees.",
        description="Pricing transparency for agents",
    )
