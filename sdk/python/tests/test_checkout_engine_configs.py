import sys
import unittest
from pathlib import Path

from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SDK_PYTHON_ROOT = PROJECT_ROOT / "sdk" / "python"
if str(SDK_PYTHON_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_PYTHON_ROOT))

from letsfg.client import _get_bookable_connector
from letsfg.connectors.checkout_engine import AIRLINE_CONFIGS, GenericCheckoutEngine


TARGET_SOURCES = (
    "itaairways_direct",
    "mea_direct",
    "aircairo_direct",
    "aireuropa_direct",
)

DEFAULT_GENERIC_SOURCES = (
    "ryanair_direct",
    "britishairways_direct",
    "traveloka_ota",
)

AIRASIA_EXTRAS_HTML = """
<html>
    <head><title>Choose extras | AirAsia</title></head>
    <body>
        <div class="Panel__MainWrapper">
            <div>Fare summary</div>
            <div>Base fare</div>
            <div>MYR</div>
            <div>200.00</div>
            <div>Total amount</div>
            <div>MYR</div>
            <div>412.82</div>
        </div>
        <div>
            <div>Baggage</div>
            <div>1 x 7 kg Carry-on baggage (Included)</div>
            <div>Checked baggage</div>
            <div>15 kg</div>
            <div>77.82</div>
        </div>
        <div role="radio">Premium Coverage MYR 12.00</div>
        <button>Meal Combo MYR 18.00</button>
        <button>Fast Pass MYR 30.00</button>
        <button>Value Pack MYR 110.00</button>
        <button>Airport Lounge MYR 55.00</button>
    </body>
</html>
"""

AIRASIA_SEATS_HTML = """
<html>
    <head><title>Seat map | AirAsia</title></head>
    <body>
        <div class="Panel__MainWrapper">
            <div>Fare summary</div>
            <div>Base fare</div>
            <div>MYR</div>
            <div>200.00</div>
            <div>Total amount</div>
            <div>MYR</div>
            <div>245.00</div>
        </div>
        <button>Standard seat MYR 20.00</button>
        <button>Hot seat MYR 45.00</button>
    </body>
</html>
"""


class CheckoutEngineConfigTest(unittest.TestCase):
    def test_target_sources_are_registered_for_generic_checkout(self) -> None:
        for source in TARGET_SOURCES:
            with self.subTest(source=source):
                config = AIRLINE_CONFIGS.get(source)
                self.assertIsNotNone(config)
                self.assertEqual(config.details_extractor_handler, "_extract_generic_visible_checkout_details")
                self.assertIsNotNone(_get_bookable_connector(source))

    def test_base_configs_default_to_generic_checkout_details(self) -> None:
        for source in DEFAULT_GENERIC_SOURCES:
            with self.subTest(source=source):
                config = AIRLINE_CONFIGS.get(source)
                self.assertIsNotNone(config)
                self.assertEqual(config.details_extractor_handler, "_extract_generic_visible_checkout_details")

    def test_airasia_family_keeps_custom_details_extractor(self) -> None:
        for source in ("airasia_direct", "airasiax_direct"):
            with self.subTest(source=source):
                config = AIRLINE_CONFIGS.get(source)
                self.assertIsNotNone(config)
                self.assertEqual(config.details_extractor_handler, "_extract_airasia_checkout_details")

    def test_merge_checkout_details_preserves_structured_add_ons(self) -> None:
        engine = GenericCheckoutEngine()
        merged = engine._merge_checkout_details(
            {
                "checkout_page": "extras",
                "available_add_ons": {
                    "baggage": [
                        {
                            "label": "20kg checked bag",
                            "currency": "EUR",
                            "amount": 25.0,
                            "type": "baggage",
                        }
                    ]
                },
                "visible_price_options": [
                    {
                        "label": "20kg checked bag",
                        "currency": "EUR",
                        "amount": 25.0,
                    }
                ],
            },
            {
                "checkout_page": "seats",
                "available_add_ons": {
                    "baggage": [
                        {
                            "label": "20kg checked bag",
                            "currency": "EUR",
                            "amount": 25.0,
                            "type": "baggage",
                        },
                        {
                            "label": "32kg checked bag",
                            "currency": "EUR",
                            "amount": 40.0,
                            "type": "baggage",
                        },
                    ],
                    "seat_selection": [
                        {
                            "label": "Standard seat",
                            "currency": "EUR",
                            "amount": 9.0,
                            "type": "seat_selection",
                        }
                    ],
                },
                "visible_price_options": [
                    {
                        "label": "20kg checked bag",
                        "currency": "EUR",
                        "amount": 25.0,
                    },
                    {
                        "label": "Standard seat",
                        "currency": "EUR",
                        "amount": 9.0,
                    },
                ],
                "price_breakdown": [
                    {
                        "label": "Base fare",
                        "currency": "EUR",
                        "amount": 120.0,
                        "type": "breakdown",
                    },
                    {
                        "label": "Standard seat",
                        "currency": "EUR",
                        "amount": 9.0,
                        "type": "seat_selection",
                    },
                ],
            },
        )

        self.assertEqual(merged["checkout_page"], "seats")
        self.assertEqual(len(merged["available_add_ons"]["baggage"]), 2)
        self.assertEqual(merged["available_add_ons"]["seat_selection"][0]["amount"], 9.0)
        self.assertEqual(len(merged["visible_price_options"]), 2)
        self.assertEqual(len(merged["price_breakdown"]), 2)

    def test_infer_checkout_page_prefers_search_surface_over_extras_copy(self) -> None:
        engine = GenericCheckoutEngine()
        checkout_page = engine._infer_checkout_page(
            {"checkout_page": "extras"},
            {
                "current_url": "https://www.traveloka.com/en-id/flight/fullsearch?ap=CGK.DPS",
                "page_title": "One Way: CGK -> DPS, 15 Jun 2026",
                "body_snippet": "Baggage included on selected fares",
            },
        )

        self.assertEqual(checkout_page, "select_flight")
        self.assertEqual(engine._checkout_step_for_page(checkout_page), "page_loaded")

    def test_infer_checkout_page_detects_payment_surface(self) -> None:
        engine = GenericCheckoutEngine()
        checkout_page = engine._infer_checkout_page(
            {},
            {
                "current_url": "https://carrier.example.com/checkout/review-and-pay",
                "page_title": "Review and Pay | Example Air",
                "body_snippet": "Payment method Card number Billing address",
            },
        )

        self.assertEqual(checkout_page, "payment")
        self.assertEqual(engine._checkout_step_for_page(checkout_page), "payment_page_reached")


class AirAsiaCheckoutDetailsExtractionTest(unittest.IsolatedAsyncioTestCase):
    async def _extract(self, source: str, html: str) -> dict:
        engine = GenericCheckoutEngine()
        config = AIRLINE_CONFIGS[source]
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.set_content(html)
            details = await engine._extract_airasia_checkout_details(page, config, default_currency="MYR")
            await browser.close()
        return details

    async def test_airasia_family_checkout_extractor_captures_clean_add_on_truth(self) -> None:
        for source in ("airasia_direct", "airasiax_direct"):
            with self.subTest(source=source):
                extras = await self._extract(source, AIRASIA_EXTRAS_HTML)
                seats = await self._extract(source, AIRASIA_SEATS_HTML)

                baggage = {item["label"]: item for item in extras["available_add_ons"]["baggage"]}
                self.assertEqual(set(baggage), {"1 x 7 kg Carry-on baggage", "15 kg checked baggage"})
                self.assertEqual(baggage["1 x 7 kg Carry-on baggage"]["type"], "cabin_bag")
                self.assertEqual(baggage["15 kg checked baggage"]["type"], "checked_bag")
                self.assertEqual(baggage["15 kg checked baggage"]["amount"], 77.82)

                seat_selection = {item["label"]: item for item in seats["available_add_ons"]["seat_selection"]}
                self.assertEqual(set(seat_selection), {"Standard seat", "Hot seat"})
                self.assertEqual(seat_selection["Standard seat"]["amount"], 20.0)
                self.assertEqual(seat_selection["Hot seat"]["amount"], 45.0)

                self.assertTrue(any(item["label"] == "Premium Coverage" and item["amount"] == 12.0 for item in extras["available_add_ons"]["insurance"]))
                self.assertTrue(any(item["label"] == "Meal Combo" and item["amount"] == 18.0 for item in extras["available_add_ons"]["meals"]))
                self.assertTrue(any(item["label"] == "Fast Pass" and item["amount"] == 30.0 for item in extras["available_add_ons"]["priority"]))
                self.assertTrue(any(item["label"] == "Value Pack" and item["amount"] == 110.0 for item in extras["available_add_ons"]["packages"]))
                self.assertTrue(any(item["label"] == "Airport Lounge" and item["amount"] == 55.0 for item in extras["available_add_ons"]["extras"]))

                self.assertEqual(
                    extras["baggage_pricing_observation"],
                    "Numeric baggage pricing is visible when the AirAsia baggage selector is open.",
                )
                self.assertEqual(
                    seats["seat_selection_observation"],
                    "Numeric seat-selection pricing is visible on the AirAsia seat-selection surface.",
                )


if __name__ == "__main__":
    unittest.main()