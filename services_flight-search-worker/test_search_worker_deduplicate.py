import copy
import unittest
from datetime import datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SDK_PYTHON_ROOT = PROJECT_ROOT / "sdk" / "python"
if str(SDK_PYTHON_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_PYTHON_ROOT))

from letsfg.connectors.combo_engine import build_combos
from letsfg.models.flights import FlightOffer, FlightRoute, FlightSegment
from search_worker import RT_CAPABLE_CONNECTORS, _deduplicate


def _make_round_trip_offer(return_flight_no: str) -> dict:
    return {
        "source": "skyscanner_meta",
        "airlines": ["Ryanair"],
        "price": 199.0,
        "currency": "PLN",
        "booking_url": "https://example.com/checkout",
        "outbound": {
            "segments": [
                {
                    "flight_no": "FR123",
                    "origin": "WAW",
                    "destination": "BCN",
                    "departure": "2026-05-01T07:00:00",
                    "arrival": "2026-05-01T10:00:00",
                }
            ]
        },
        "inbound": {
            "segments": [
                {
                    "flight_no": return_flight_no,
                    "origin": "BCN",
                    "destination": "WAW",
                    "departure": "2026-05-06T18:00:00",
                    "arrival": "2026-05-06T21:00:00",
                }
            ]
        },
    }


def _make_one_way_offer(source: str, flight_no: str, origin: str, destination: str, departure: datetime, arrival: datetime) -> FlightOffer:
    return FlightOffer(
        id=f"{source}-{flight_no}",
        price=49.0,
        currency="EUR",
        outbound=FlightRoute(
            segments=[
                FlightSegment(
                    airline="FR",
                    airline_name="Ryanair",
                    flight_no=flight_no,
                    origin=origin,
                    destination=destination,
                    departure=departure,
                    arrival=arrival,
                )
            ],
            total_duration_seconds=int((arrival - departure).total_seconds()),
            stopovers=0,
        ),
        airlines=["Ryanair"],
        owner_airline="Ryanair",
        source=source,
        booking_url=f"https://example.com/{source}/{flight_no}",
    )


class DeduplicateOffersTest(unittest.TestCase):
    def test_google_is_treated_as_round_trip_capable(self) -> None:
        self.assertIn("serpapi_google", RT_CAPABLE_CONNECTORS)

    def test_preserves_distinct_round_trip_returns(self) -> None:
        offers = [
            _make_round_trip_offer("FR456"),
            _make_round_trip_offer("FR789"),
        ]

        self.assertEqual(len(_deduplicate(offers)), 2)

    def test_collapses_exact_duplicate_itinerary(self) -> None:
        offer = _make_round_trip_offer("FR456")
        offers = [offer, copy.deepcopy(offer)]

        self.assertEqual(len(_deduplicate(offers)), 1)

    def test_collapses_same_itinerary_with_different_booking_urls(self) -> None:
        first = _make_round_trip_offer("FR456")
        second = copy.deepcopy(first)
        first["source"] = "momondo_meta"
        second["source"] = "momondo_meta"
        first["booking_url"] = "https://www.momondo.com/flight-search/WAW-BCN/2026-05-01"
        second["booking_url"] = "https://www.momondo.com/flight-search/WMI-BCN/2026-05-01"

        self.assertEqual(len(_deduplicate([first, second])), 1)

    def test_build_combos_respects_requested_max(self) -> None:
        outbound = [
            _make_one_way_offer(
                source=f"out_{index}",
                flight_no=f"FR{100 + index}",
                origin="WAW",
                destination="BCN",
                departure=datetime(2026, 5, 1, 6 + (index % 6), 0),
                arrival=datetime(2026, 5, 1, 9 + (index % 6), 0),
            )
            for index in range(13)
        ]
        returns = [
            _make_one_way_offer(
                source=f"ret_{index}",
                flight_no=f"FR{300 + index}",
                origin="BCN",
                destination="WAW",
                departure=datetime(2026, 5, 6, 12 + (index % 6), 0),
                arrival=datetime(2026, 5, 6, 15 + (index % 6), 0),
            )
            for index in range(13)
        ]

        self.assertEqual(len(build_combos(outbound, returns, "EUR")), 150)
        self.assertEqual(len(build_combos(outbound, returns, "EUR", max_combos=160)), 160)


if __name__ == "__main__":
    unittest.main()