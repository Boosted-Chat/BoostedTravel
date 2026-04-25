import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SDK_PYTHON_ROOT = PROJECT_ROOT / "sdk" / "python"
SERVICE_ROOT = PROJECT_ROOT / "services_connector-worker"

for path in (SDK_PYTHON_ROOT, SERVICE_ROOT):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from connector_patches.serpapi_google import SerpApiGoogleConnectorClient
from letsfg.models.flights import FlightSearchRequest


class GoogleFlightsCityResolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = SerpApiGoogleConnectorClient()

    def test_candidate_airports_prefers_primary_for_ambiguous_city(self) -> None:
        self.assertEqual(
            self.client._candidate_airports("LON")[:4],
            ["LON", "LHR", "LCY", "LGW"],
        )

    def test_build_filters_accepts_shared_city_codes_not_in_old_map(self) -> None:
        req = FlightSearchRequest(
            origin="CHI",
            destination="YTO",
            date_from=date(2026, 5, 1),
            adults=1,
            currency="USD",
        )

        filters = self.client._build_filters(req)

        self.assertEqual(len(filters.flight_segments), 1)

    def test_build_filters_accepts_round_trip_city_codes(self) -> None:
        req = FlightSearchRequest(
            origin="WAW",
            destination="YMQ",
            date_from=date(2026, 5, 1),
            return_from=date(2026, 5, 6),
            adults=1,
            currency="USD",
        )

        filters = self.client._build_filters(req)

        self.assertEqual(len(filters.flight_segments), 2)

    def test_search_sync_retries_once_on_empty_results(self) -> None:
        req = FlightSearchRequest(
            origin="WAW",
            destination="NYC",
            date_from=date(2026, 5, 1),
            return_from=date(2026, 5, 6),
            adults=1,
            currency="USD",
            limit=10,
        )
        search_instance = MagicMock()
        search_instance.search.side_effect = [[], []]

        with patch("connector_patches.serpapi_google.SearchFlights", return_value=search_instance):
            response = self.client._search_sync(req)

        self.assertEqual(response.total_results, 0)
        self.assertEqual(search_instance.search.call_count, 2)


if __name__ == "__main__":
    unittest.main()