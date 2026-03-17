"""
Config-driven checkout engine — covers 79 airline connectors.

Instead of writing 79 individual Playwright scripts, this engine runs ONE
generic checkout flow parametrised by airline-specific selector configs.

All airlines follow the same basic checkout pattern:
  1. Navigate to booking URL
  2. Dismiss cookie/overlay banners
  3. Select flights (by departure time)
  4. Select fare tier
  5. Bypass login / continue as guest
  6. Fill passenger details
  7. Skip extras (bags, insurance, priority)
  8. Skip seat selection
  9. STOP at payment page → screenshot + URL for manual completion

The differences between airlines are:
  - CSS selectors for each element
  - Anti-bot setup (Kasada, Akamai, Cloudflare, PerimeterX)
  - Pre-navigation requirements (homepage pre-load for Kasada, etc.)
  - Quirks (storage cleanup, iframe payment, PRM declarations, etc.)

This module exports:
  - AirlineCheckoutConfig: dataclass with all per-airline selectors/settings
  - AIRLINE_CONFIGS: dict mapping source_tag → AirlineCheckoutConfig
  - GenericCheckoutEngine: the unified engine
"""

from __future__ import annotations

import base64
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from typing import Optional

from .booking_base import (
    CheckoutProgress,
    CHECKOUT_STEPS,
    FAKE_PASSENGER,
    dismiss_overlays,
    safe_click,
    safe_click_first,
    safe_fill,
    safe_fill_first,
    take_screenshot_b64,
    verify_checkout_token,
)

logger = logging.getLogger(__name__)


# ── Airline checkout config ──────────────────────────────────────────────

@dataclass
class AirlineCheckoutConfig:
    """Per-airline configuration for the generic checkout engine."""

    # Identity
    airline_name: str
    source_tag: str

    # Pre-navigation
    homepage_url: str = ""             # Load this BEFORE booking URL (Kasada init, etc.)
    homepage_wait_ms: int = 3000       # Wait after homepage load
    clear_storage_keep: list[str] = field(default_factory=list)  # localStorage prefixes to KEEP

    # Navigation
    goto_timeout: int = 30000          # ms — initial page.goto() timeout

    # Proxy (residential proxy for anti-bot bypass)
    use_proxy: bool = False            # Enable residential proxy for this airline
    use_chrome_channel: bool = False   # Use installed Chrome instead of Playwright Chromium

    # CDP Chrome mode (Kasada bypass — launch real Chrome as subprocess, connect via CDP)
    use_cdp_chrome: bool = False       # Launch real Chrome + CDP instead of Playwright
    cdp_port: int = 9448               # CDP debugging port (unique per airline)
    cdp_user_data_dir: str = ""        # Custom user data dir name (default: .{source_tag}_chrome_data)

    # Custom checkout handler (method name on GenericCheckoutEngine, e.g. "_wizzair_checkout")
    custom_checkout_handler: str = ""

    # Anti-bot
    service_workers: str = ""          # "block" | "" — block SW for cleaner interception
    disable_cache: bool = False        # CDP Network.setCacheDisabled
    locale: str = "en-GB"
    locale_pool: list[str] = field(default_factory=list)  # Random locale from pool
    timezone: str = "Europe/London"
    timezone_pool: list[str] = field(default_factory=list)

    # Cookie/overlay dismissal — scoped to cookie/consent containers to avoid clicking nav buttons
    cookie_selectors: list[str] = field(default_factory=lambda: [
        "#onetrust-accept-btn-handler",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "[class*='cookie'] button:has-text('Accept')",
        "[class*='cookie'] button:has-text('OK')",
        "[class*='cookie'] button:has-text('Agree')",
        "[id*='cookie'] button",
        "[class*='consent'] button:has-text('Accept')",
        "[id*='consent'] button:has-text('Accept')",
        "[class*='gdpr'] button",
        "button:has-text('Accept all cookies')",
        "button:has-text('Accept All Cookies')",
        "button:has-text('Yes, I agree')",
    ])

    # Flight selection
    flight_cards_selector: str = "[data-ref*='flight-card'], flight-card, [class*='flight-card'], [data-test*='flight'], [class*='flight-select'], [class*='flight-row']"
    flight_cards_timeout: int = 8000
    first_flight_selectors: list[str] = field(default_factory=lambda: [
        "flight-card:first-child",
        "[class*='flight-card']:first-child",
        "[data-ref*='flight-card']:first-child",
        "[data-test*='flight']:first-child",
        "[class*='flight-select']:first-child",
    ])
    flight_ancestor_tag: str = "flight-card"  # For xpath ancestor climb

    # Fare selection
    fare_selectors: list[str] = field(default_factory=lambda: [
        "[data-ref*='fare-card--regular'] button",
        "button:has-text('Regular')",
        "button:has-text('Value')",
        "button:has-text('Standard')",
        "button:has-text('BASIC')",
        "button:has-text('Economy')",
        "[class*='fare-card']:first-child button:has-text('Select')",
        "[class*='fare-selector'] button:first-child",
        "fare-card:first-child button",
        "button:has-text('Select'):first-child",
    ])
    fare_upsell_decline: list[str] = field(default_factory=lambda: [
        "button:has-text('No, thanks')",
        "button:has-text('Continue with Regular')",
        "button:has-text('Continue with Standard')",
        "button:has-text('Not now')",
        "button:has-text('No thanks')",
    ])
    # Wizzair-style multi-step fare: keep clicking "Continue for" until passenger form appears
    fare_loop_enabled: bool = False
    fare_loop_selectors: list[str] = field(default_factory=list)
    fare_loop_done_selector: str = ""  # If this appears, fare selection is complete

    # Login bypass
    login_skip_selectors: list[str] = field(default_factory=lambda: [
        "button:has-text('Log in later')",
        "button:has-text('Continue as guest')",
        "button:has-text('Not now')",
        "button:has-text('Skip')",
        "button:has-text('No thanks')",
        "[data-ref='login-gate__skip']",
        "[data-test*='guest'] button",
    ])

    # Passenger form — name fields
    passenger_form_selector: str = "input[name*='name'], [class*='passenger-form'], [data-testid*='passenger'], pax-passenger"
    passenger_form_timeout: int = 8000

    # Title: "dropdown" | "select" | "none"
    title_mode: str = "dropdown"
    title_dropdown_selectors: list[str] = field(default_factory=lambda: [
        "button[data-ref='title-toggle']",
        "[class*='dropdown'] button:has-text('Title')",
    ])
    title_select_selector: str = "select[name*='title'], [data-testid*='title'] select"

    first_name_selectors: list[str] = field(default_factory=lambda: [
        "input[name*='name'][name*='first']",
        "input[data-ref*='first-name']",
        "input[data-test*='first-name']",
        "input[data-test='passenger-first-name-0']",
        "input[name*='firstName']",
        "input[data-testid*='first-name']",
        "input[placeholder*='First name' i]",
    ])
    last_name_selectors: list[str] = field(default_factory=lambda: [
        "input[name*='name'][name*='last']",
        "input[data-ref*='last-name']",
        "input[data-test*='last-name']",
        "input[data-test='passenger-last-name-0']",
        "input[name*='lastName']",
        "input[data-testid*='last-name']",
        "input[placeholder*='Last name' i]",
    ])

    # Gender selection
    gender_enabled: bool = False
    gender_selectors_male: list[str] = field(default_factory=lambda: [
        "label:has-text('Male')",
        "label:has-text('Mr')",
        "label[data-test='passenger-gender-0-male']",
        "[data-test='passenger-0-gender-selectormale']",
    ])
    gender_selectors_female: list[str] = field(default_factory=lambda: [
        "label:has-text('Female')",
        "label:has-text('Ms')",
        "label:has-text('Mrs')",
        "label[data-test='passenger-gender-0-female']",
        "[data-test='passenger-0-gender-selectorfemale']",
    ])

    # Date of birth (some airlines require it)
    dob_enabled: bool = False
    dob_day_selectors: list[str] = field(default_factory=lambda: [
        "input[data-test*='birth-day']",
        "input[placeholder*='DD']",
        "input[name*='day']",
    ])
    dob_month_selectors: list[str] = field(default_factory=lambda: [
        "input[data-test*='birth-month']",
        "input[placeholder*='MM']",
        "input[name*='month']",
    ])
    dob_year_selectors: list[str] = field(default_factory=lambda: [
        "input[data-test*='birth-year']",
        "input[placeholder*='YYYY']",
        "input[name*='year']",
    ])
    dob_strip_leading_zero: bool = False  # Wizzair wants "5" not "05" for day

    # Nationality (some airlines require it)
    nationality_enabled: bool = False
    nationality_selectors: list[str] = field(default_factory=list)
    nationality_dropdown_item: str = "[class*='dropdown'] [class*='item']:first-child"

    # Contact info
    email_selectors: list[str] = field(default_factory=lambda: [
        "input[data-test*='email']",
        "input[data-test*='contact-email']",
        "input[name*='email']",
        "input[data-testid*='email']",
        "input[type='email']",
    ])
    phone_selectors: list[str] = field(default_factory=lambda: [
        "input[data-test*='phone']",
        "input[name*='phone']",
        "input[data-testid*='phone']",
        "input[type='tel']",
    ])

    # Passenger continue button
    passenger_continue_selectors: list[str] = field(default_factory=lambda: [
        "button[data-test='passengers-continue-btn']",
        "[data-test*='continue'] button",
        "[data-testid*='continue'] button",
        "[class*='passenger'] button:has-text('Continue')",
        "[class*='pax'] button:has-text('Continue')",
        "form button[type='submit']",
        "button:has-text('Continue to')",
        "button:has-text('Next step')",
    ])

    # Wizzair-style extras on passengers page (baggage checkbox, PRM, etc.)
    pre_extras_hooks: list[dict] = field(default_factory=list)
    # Format: [{"action": "click"|"check"|"escape", "selectors": [...], "desc": "..."}]

    # Skip extras (bags, insurance, priority)
    extras_rounds: int = 3  # How many times to try skipping
    extras_skip_selectors: list[str] = field(default_factory=lambda: [
        "button:has-text('Continue without')",
        "button:has-text('No thanks')",
        "button:has-text('No, thanks')",
        "button:has-text('OK, got it')",
        "button:has-text('Not interested')",
        "button:has-text('I don\\'t need')",
        "button:has-text('No hold luggage')",
        "button:has-text('Skip to payment')",
        "button:has-text('Continue to payment')",
        "[data-test*='extras-skip'] button",
        "[data-test*='continue-without'] button",
    ])

    # Skip seats
    seats_skip_selectors: list[str] = field(default_factory=lambda: [
        "button:has-text('No thanks')",
        "button:has-text('Not now')",
        "button:has-text('Continue without')",
        "button:has-text('OK, pick seats later')",
        "button:has-text('Skip seat selection')",
        "button:has-text('Skip')",
        "button:has-text('Assign random seats')",
        "[data-ref*='seats-action__button--later']",
        "[data-test*='skip-seat']",
        "[data-test*='seat-selection-decline']",
    ])
    seats_confirm_selectors: list[str] = field(default_factory=lambda: [
        "[data-ref*='seats'] button:has-text('OK')",
        "[class*='seat'] button:has-text('OK')",
        "[class*='modal'] button:has-text('Yes')",
        "[class*='dialog'] button:has-text('Continue')",
    ])

    # Price extraction on payment page
    price_selectors: list[str] = field(default_factory=lambda: [
        "[class*='total'] [class*='price']",
        "[data-test*='total-price']",
        "[data-ref*='total']",
        "[class*='total-price']",
        "[data-testid*='total']",
        "[class*='summary'] [class*='amount']",
        "[class*='summary-price']",
        "[class*='summary'] [class*='price']",
    ])


# ── Airline configs ──────────────────────────────────────────────────────
# Each entry maps a source_tag to its AirlineCheckoutConfig.

def _base_cfg(airline_name: str, source_tag: str, **overrides) -> AirlineCheckoutConfig:
    """Create a config with defaults + overrides."""
    return AirlineCheckoutConfig(airline_name=airline_name, source_tag=source_tag, **overrides)


AIRLINE_CONFIGS: dict[str, AirlineCheckoutConfig] = {}


def _register(cfg: AirlineCheckoutConfig):
    AIRLINE_CONFIGS[cfg.source_tag] = cfg


# ─── European LCCs ──────────────────────────────────────────────────────

_register(_base_cfg("Ryanair", "ryanair_direct",
    service_workers="block",
    disable_cache=True,
    homepage_url="https://www.ryanair.com/gb/en",
    homepage_wait_ms=3000,
    cookie_selectors=[
        "button[data-ref='cookie.accept-all']",
        "#cookie-preferences button:has-text('Accept')",
        "#cookie-preferences button:has-text('Yes')",
        "#cookie-preferences button",
        "#onetrust-accept-btn-handler",
        "[class*='cookie'] button:has-text('Accept')",
    ],
    flight_cards_selector="button.flight-card-summary__select-btn, button[data-ref='regular-price-select'], flight-card, [class*='flight-card']",
    first_flight_selectors=[
        "button[data-ref='regular-price-select']",
        "button.flight-card-summary__select-btn",
        "flight-card:first-child button:has-text('Select')",
    ],
    flight_ancestor_tag="flight-card",
    fare_selectors=[
        "[data-ref*='fare-card--regular'] button",
        "fare-card:first-child button",
        "button:has-text('Regular')",
        "button:has-text('Value')",
        "[class*='fare-card']:first-child button:has-text('Select')",
        "button:has-text('Continue with Regular')",
    ],
    fare_upsell_decline=[
        "button:has-text('No, thanks')",
        "button:has-text('Continue with Regular')",
    ],
    login_skip_selectors=[
        "button:has-text('Log in later')",
        "button:has-text('Continue as guest')",
        "[data-ref='login-gate__skip']",
        "button:has-text('Not now')",
    ],
    title_mode="dropdown",
    title_dropdown_selectors=[
        "button[data-ref='title-toggle']",
        "[class*='dropdown'] button:has-text('Title')",
    ],
))

_register(_base_cfg("Wizz Air", "wizzair_api",
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9446,
    cdp_user_data_dir=".wizzair_chrome_data",
    custom_checkout_handler="_wizzair_checkout",
    homepage_url="https://wizzair.com/en-gb",
    homepage_wait_ms=5000,
    clear_storage_keep=["kpsdk", "_kas"],
    locale_pool=["en-GB", "en-US", "en-IE"],
    timezone_pool=["Europe/Warsaw", "Europe/London", "Europe/Budapest"],
    cookie_selectors=[
        "button[data-test='cookie-policy-button-accept']",
        "[class*='cookie'] button:has-text('Accept')",
        "[data-test='modal-close']",
        "button[class*='close']",
    ],
    flight_cards_selector="[data-test*='flight'], [class*='flight-select'], [class*='flight-row']",
    flight_cards_timeout=20000,
    first_flight_selectors=[
        "[data-test*='flight']:first-child",
        "[class*='flight-select']:first-child",
        "[class*='flight-row']:first-child",
    ],
    fare_loop_enabled=True,
    fare_loop_selectors=[
        "button:has-text('Continue for')",
        "button[data-test='booking-flight-select-continue-btn']",
        "button:has-text('No, thanks')",
        "button:has-text('Not now')",
    ],
    fare_loop_done_selector="input[data-test='passenger-first-name-0']",
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('No, thanks')",
        "button:has-text('Not now')",
        "[data-test*='login-modal'] button:has-text('Later')",
        "[class*='modal'] button:has-text('Continue')",
    ],
    passenger_form_selector="input[data-test='passenger-first-name-0'], input[name*='firstName'], [class*='passenger-form']",
    first_name_selectors=[
        "input[data-test='passenger-first-name-0']",
        "input[data-test*='first-name']",
        "input[name*='firstName']",
        "input[placeholder*='First name' i]",
    ],
    last_name_selectors=[
        "input[data-test='passenger-last-name-0']",
        "input[data-test*='last-name']",
        "input[name*='lastName']",
        "input[placeholder*='Last name' i]",
    ],
    gender_enabled=True,
    dob_enabled=True,
    dob_strip_leading_zero=True,
    nationality_enabled=True,
    nationality_selectors=[
        "input[data-test*='nationality']",
        "[data-test*='nationality'] input",
    ],
    nationality_dropdown_item="[class*='dropdown'] [class*='item']:first-child",
    email_selectors=[
        "input[data-test*='contact-email']",
        "input[data-test*='email']",
        "input[name*='email']",
        "input[type='email']",
    ],
    phone_selectors=[
        "input[data-test*='phone']",
        "input[name*='phone']",
        "input[type='tel']",
    ],
    passenger_continue_selectors=[
        "button[data-test='passengers-continue-btn']",
        "button:has-text('Continue')",
        "button:has-text('Next')",
    ],
    pre_extras_hooks=[
        {"action": "click", "selectors": [
            "label[data-test='checkbox-label-no-checked-in-baggage']",
            "input[name='no-checked-in-baggage']",
        ], "desc": "no checked bag"},
        {"action": "click", "selectors": [
            "button[data-test='add-wizz-priority']",
        ], "desc": "cabin bag priority hack"},
        {"action": "escape", "selectors": [".dialog-container"], "desc": "dismiss priority dialog"},
        {"action": "click", "selectors": [
            "[data-test='common-prm-card'] label:has-text('No')",
        ], "desc": "PRM declaration No"},
    ],
    extras_rounds=5,
    extras_skip_selectors=[
        "button:has-text('No, thanks')",
        "button:has-text('Continue')",
        "button:has-text('Skip')",
        "button:has-text('I don\\'t need')",
        "button:has-text('Next')",
        "[data-test*='cabin-bag-no']",
        "[data-test*='skip']",
    ],
    seats_skip_selectors=[
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without seats')",
        "button:has-text('No, thanks')",
        "button:has-text('Skip')",
        "button[data-test*='skip-seat']",
        "[data-test*='seat-selection-decline']",
        "button:has-text('Continue')",
    ],
))

_register(_base_cfg("easyJet", "easyjet_direct",
    goto_timeout=60000,
    cookie_selectors=[
        "#ensCloseBanner",
        "button:has-text('Accept all cookies')",
        "[class*='cookie-banner'] button",
        "button:has-text('Accept')",
        "button:has-text('Agree')",
        "button:has-text('Got it')",
        "button:has-text('OK')",
        "[class*='cookie'] button",
    ],
    flight_cards_selector="[class*='flight-grid'], [class*='flight-card'], [data-testid*='flight']",
    first_flight_selectors=[
        "[class*='flight-card']:first-child",
        "[data-testid*='flight']:first-child",
        "button:has-text('Select'):first-child",
    ],
    fare_selectors=[
        "button:has-text('Standard')",
        "button:has-text('Continue')",
        "[class*='fare'] button:first-child",
        "button:has-text('Select')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Skip')",
        "button:has-text('No thanks')",
        "[data-testid*='guest'] button",
    ],
    title_mode="select",
    title_select_selector="select[name*='title'], [data-testid*='title'] select",
    first_name_selectors=[
        "input[name*='firstName']",
        "input[data-testid*='first-name']",
        "input[placeholder*='First name' i]",
    ],
    last_name_selectors=[
        "input[name*='lastName']",
        "input[data-testid*='last-name']",
        "input[placeholder*='Last name' i]",
    ],
    extras_rounds=5,
    seats_skip_selectors=[
        "button:has-text('Skip')",
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Assign random seats')",
    ],
))

_register(_base_cfg("Vueling", "vueling_direct",
    flight_cards_selector="[class*='flight-row'], [class*='flight-card'], [class*='FlightCard']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Optima')",
        "button:has-text('Select')",
        "[class*='fare'] button:first-child",
    ],
    title_mode="select",
    title_select_selector="select[name*='title'], select[id*='title']",
))

_register(_base_cfg("Volotea", "volotea_direct",
    flight_cards_selector="[class*='flight'], [class*='outbound']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Select')",
        "[class*='fare'] button:first-child",
    ],
))

_register(_base_cfg("Eurowings", "eurowings_direct",
    flight_cards_selector="[class*='flight-card'], [class*='flight-row']",
    fare_selectors=[
        "button:has-text('SMART')",
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Transavia", "transavia_direct",
    flight_cards_selector="[class*='flight'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Light')",
        "[class*='fare'] button:first-child",
    ],
))

_register(_base_cfg("Norwegian", "norwegian_api",
    flight_cards_selector="[class*='flight'], [data-testid*='flight']",
    fare_selectors=[
        "button:has-text('LowFare')",
        "button:has-text('Select')",
        "[class*='fare-card']:first-child button",
    ],
))

_register(_base_cfg("Pegasus", "pegasus_direct",
    cookie_selectors=[
        "#cookie-popup-with-overlay button:has-text('Accept')",
        "#cookie-popup-with-overlay button",
        "[class*='cookie-popup'] button:has-text('Accept')",
        "[class*='cookie'] button",
    ],
    flight_cards_selector="[class*='flight-detail'], [class*='flight-row'], [class*='flight-list'] button",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Essentials')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Smartwings", "smartwings_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Light')",
        "button:has-text('Select')",
        "[class*='fare'] button:first-child",
    ],
))

_register(_base_cfg("Condor", "condor_direct",
    goto_timeout=60000,
    flight_cards_selector="button:has-text('Book Now'), [class*='flight-result'], [class*='flight-card']",
    first_flight_selectors=[
        "button:has-text('Book Now')",
    ],
    fare_selectors=[
        "button:has-text('Economy Light')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("SunExpress", "sunexpress_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('SunEco')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("LOT Polish", "lot_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy Saver')",
        "button:has-text('Light')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Jet2", "jet2_direct",
    flight_cards_selector="[class*='flight-result'], [class*='flight-card']",
    fare_selectors=[
        "button:has-text('Select')",
        "[class*='fare'] button:first-child",
    ],
))

_register(_base_cfg("airBaltic", "airbaltic_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy Green')",
        "button:has-text('Select')",
    ],
))

# ─── US airlines ─────────────────────────────────────────────────────────

_register(_base_cfg("Southwest", "southwest_direct",
    flight_cards_selector="[class*='air-booking-select'], [id*='outbound']",
    first_flight_selectors=[
        "[class*='air-booking-select-detail']:first-child button",
        "button:has-text('Wanna Get Away'):first-child",
    ],
    fare_selectors=[
        "button:has-text('Wanna Get Away')",
        "[class*='fare-button']:first-child",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as Guest')",
        "button:has-text('Continue Without')",
        "button:has-text('Skip')",
    ],
))

_register(_base_cfg("Frontier", "frontier_direct",
    flight_cards_selector="[class*='flight-row'], [class*='flight-card']",
    fare_selectors=[
        "button:has-text('The Works')",
        "button:has-text('The Perks')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Spirit", "spirit_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Bare Fare')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("JetBlue", "jetblue_direct",
    flight_cards_selector="button.cb-fare-card, [class*='cb-fare-card'], [class*='cb-alternate-date']",
    first_flight_selectors=[
        "button.cb-fare-card",
        "[class*='cb-fare-card']:first-child",
        "button:has-text('Core')",
        "button:has-text('Blue')",
    ],
    fare_selectors=[
        "button.cb-fare-card",
        "button:has-text('Core')",
        "button:has-text('Blue Basic')",
        "button:has-text('Blue')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Allegiant", "allegiant_direct",
    flight_cards_selector="[class*='flight-card'], [class*='FlightCard']",
    fare_selectors=[
        "button:has-text('Select')",
        "[class*='fare'] button:first-child",
    ],
))

_register(_base_cfg("Alaska Airlines", "alaska_direct",
    flight_cards_selector="[class*='flight-result'], [class*='flight-card']",
    fare_selectors=[
        "button:has-text('Saver')",
        "button:has-text('Main')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Avelo", "avelo_direct",
    goto_timeout=60000,
    use_proxy=True,
    use_chrome_channel=True,
    homepage_url="https://www.aveloair.com",
    homepage_wait_ms=3000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Breeze", "breeze_direct",
    flight_cards_selector="button:has-text('Compare Bundles'), button:has-text('Trip Details'), [class*='flight'], [class*='result']",
    first_flight_selectors=[
        "button:has-text('Compare Bundles')",
        "button:has-text('Trip Details')",
    ],
    fare_selectors=[
        "button:has-text('Nice')",
        "button:has-text('Nicer')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Hawaiian", "hawaiian_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Main Cabin')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Sun Country", "suncountry_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Best')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Flair", "flair_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Select')",
        "[class*='fare'] button:first-child",
    ],
))

_register(_base_cfg("WestJet", "westjet_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Econo')",
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
))

# ─── Latin American airlines ────────────────────────────────────────────

_register(_base_cfg("Avianca", "avianca_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Azul", "azul_direct",
    flight_cards_selector="[class*='flight'], [class*='v5-result']",
    fare_selectors=[
        "button:has-text('Azul')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("GOL", "gol_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Light')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("LATAM", "latam_direct",
    flight_cards_selector="[class*='cardFlight'], [class*='WrapperCardHeader'], button:has-text('Flight recommended')",
    first_flight_selectors=[
        "[class*='WrapperCardHeader-sc']:first-child",
        "[class*='cardFlight'] button:first-child",
        "button:has-text('Flight recommended')",
    ],
    fare_selectors=[
        "button:has-text('Light')",
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Copa", "copa_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy Basic')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Flybondi", "flybondi_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("JetSMART", "jetsmart_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Light')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Volaris", "volaris_direct",
    flight_cards_selector="button:has-text('Reserva ahora'), button:has-text('Book Now'), [class*='flight'], [class*='result']",
    first_flight_selectors=[
        "button:has-text('Reserva ahora')",
        "button:has-text('Book Now')",
        "button:has-text('Book now')",
    ],
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("VivaAerobus", "vivaaerobus_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Viva')",
        "button:has-text('Zero')",
        "button:has-text('Select')",
    ],
))

# ─── Middle East airlines ───────────────────────────────────────────────

_register(_base_cfg("Air Arabia", "airarabia_direct",
    flight_cards_selector="[class*='flight'], [class*='fare']",
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Value')",
    ],
    dob_enabled=True,
))

_register(_base_cfg("flydubai", "flydubai_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Light')",
        "button:has-text('Select')",
    ],
))
# flydubai also emits results with "flydubai_api" source tag
AIRLINE_CONFIGS["flydubai_api"] = AIRLINE_CONFIGS["flydubai_direct"]

_register(_base_cfg("Flynas", "flynas_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Jazeera", "jazeera_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Light')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("SalamAir", "salamair_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

# ─── Asian airlines ─────────────────────────────────────────────────────

_register(_base_cfg("AirAsia", "airasia_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [data-testid*='flight']",
    fare_selectors=[
        "button:has-text('Value Pack')",
        "button:has-text('Select')",
    ],
    dob_enabled=True,
    gender_enabled=True,
))

_register(_base_cfg("Cebu Pacific", "cebupacific_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Go Basic')",
        "button:has-text('Select')",
    ],
    dob_enabled=True,
))

_register(_base_cfg("VietJet", "vietjet_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Eco')",
        "button:has-text('Promo')",
        "button:has-text('Select')",
    ],
    dob_enabled=True,
    gender_enabled=True,
))

_register(_base_cfg("IndiGo", "indigo_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Saver')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("SpiceJet", "spicejet_direct_api",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Spice Value')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Akasa Air", "akasa_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Saver')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Air India Express", "airindiaexpress_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Saver')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Batik Air", "batikair_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Scoot", "scoot_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Fly')",
        "button:has-text('Select')",
    ],
    dob_enabled=True,
))

_register(_base_cfg("Jetstar", "jetstar_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Starter')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Nok Air", "nokair_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
    dob_enabled=True,
))

_register(_base_cfg("Peach", "peach_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Simple Peach')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Jeju Air", "jejuair_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Fly')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("T'way Air", "twayair_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("9 Air", "9air_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Lucky Air", "luckyair_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Spring Airlines", "spring_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Malaysia Airlines", "malaysia_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Lite')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("ZIPAIR", "zipair_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('ZIP Full')",
        "button:has-text('Select')",
    ],
))

# ─── African airlines ───────────────────────────────────────────────────

_register(_base_cfg("Air Peace", "airpeace_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("FlySafair", "flysafair_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

# ─── Bangladeshi airlines ───────────────────────────────────────────────

_register(_base_cfg("Biman Bangladesh", "biman_direct",
    goto_timeout=90000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("US-Bangla", "usbangla_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

# ─── Full-service carriers (deep-link capable) ──────────────────────────

_register(_base_cfg("Cathay Pacific", "cathay_direct",
    goto_timeout=90000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy Light')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("ANA", "nh_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

# ─── Full-service carriers (manual booking only — generic homepage URL) ─

_register(_base_cfg("American Airlines", "american_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result'], .slice",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Delta", "delta_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Basic Economy')",
        "button:has-text('Main Cabin')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("United", "united_direct",
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Basic Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Emirates", "emirates_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy Saver')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Etihad", "etihad_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy Saver')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Qatar Airways", "qatar_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Singapore Airlines", "singapore_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy Lite')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Turkish Airlines", "turkish_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('ecoFly')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Thai Airways", "thai_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Korean Air", "korean_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Porter", "porter_scraper",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
))

# ─── Meta-search aggregators ────────────────────────────────────────────

_register(_base_cfg("Kiwi.com", "kiwi_connector",
    # Kiwi booking URLs go straight to checkout — no flight/fare selection
    # The URL is an opaque session token from their GraphQL API
    # Checkout lands on Kiwi's own payment page (not airline direct)
    cookie_selectors=[
        "button[data-test='CookiesPopup-Accept']",
        "button:has-text('Accept')",
        "button:has-text('Accept all')",
        "[class*='cookie'] button",
        "button:has-text('Got it')",
        "button:has-text('OK')",
    ],
    # Kiwi skips flight/fare selection — booking URL lands on passenger form
    flight_cards_selector="[data-test='BookingPassengerRow'], [class*='PassengerForm'], [data-test*='passenger']",
    flight_cards_timeout=20000,
    first_flight_selectors=[],   # No flight cards to click — already selected
    fare_selectors=[],           # No fare to pick — already selected
    fare_upsell_decline=[
        "button:has-text('No, thanks')",
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as a guest')",
        "button:has-text('Continue as guest')",
        "button:has-text('Skip')",
        "button:has-text('No thanks')",
        "[data-test='SocialLogin-GuestButton']",
        "[data-test*='guest'] button",
    ],
    # Kiwi passenger form
    passenger_form_selector="[data-test='BookingPassengerRow'], input[name*='firstName'], [data-test*='passenger']",
    passenger_form_timeout=20000,
    title_mode="select",
    title_select_selector="select[name*='title'], [data-test*='Title'] select",
    first_name_selectors=[
        "input[name*='firstName']",
        "input[data-test*='firstName']",
        "[data-test='BookingPassenger-FirstName'] input",
        "input[placeholder*='First name' i]",
        "input[placeholder*='Given name' i]",
    ],
    last_name_selectors=[
        "input[name*='lastName']",
        "input[data-test*='lastName']",
        "[data-test='BookingPassenger-LastName'] input",
        "input[placeholder*='Last name' i]",
        "input[placeholder*='Family name' i]",
    ],
    gender_enabled=True,
    gender_selectors_male=[
        "[data-test*='gender'] label:has-text('Male')",
        "label:has-text('Male')",
        "[data-test*='Gender-male']",
    ],
    gender_selectors_female=[
        "[data-test*='gender'] label:has-text('Female')",
        "label:has-text('Female')",
        "[data-test*='Gender-female']",
    ],
    dob_enabled=True,
    dob_day_selectors=[
        "input[name*='birthDay']",
        "[data-test*='BirthDay'] input",
        "input[placeholder*='DD']",
    ],
    dob_month_selectors=[
        "input[name*='birthMonth']",
        "[data-test*='BirthMonth'] input",
        "select[name*='birthMonth']",
        "input[placeholder*='MM']",
    ],
    dob_year_selectors=[
        "input[name*='birthYear']",
        "[data-test*='BirthYear'] input",
        "input[placeholder*='YYYY']",
    ],
    nationality_enabled=True,
    nationality_selectors=[
        "input[name*='nationality']",
        "[data-test*='Nationality'] input",
        "input[placeholder*='Nationali' i]",
    ],
    email_selectors=[
        "input[name*='email']",
        "input[data-test*='contact-email']",
        "[data-test='contact-email'] input",
        "input[type='email']",
    ],
    phone_selectors=[
        "input[name*='phone']",
        "input[data-test*='contact-phone']",
        "[data-test='contact-phone'] input",
        "input[type='tel']",
    ],
    passenger_continue_selectors=[
        "button[data-test='StepControls-passengers-next']",
        "button:has-text('Continue')",
        "button:has-text('Next')",
        "[data-test*='continue'] button",
    ],
    extras_rounds=4,
    extras_skip_selectors=[
        "button:has-text('No, thanks')",
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Continue')",
        "button:has-text('Skip')",
        "button:has-text('Next')",
        "[data-test*='skip'] button",
        "[data-test*='decline'] button",
        "button[data-test='StepControls-baggage-next']",
        "button[data-test='StepControls-extras-next']",
    ],
    seats_skip_selectors=[
        "button:has-text('Skip')",
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "[data-test*='seats-skip']",
        "button[data-test='StepControls-seating-next']",
        "button:has-text('Continue')",
    ],
    price_selectors=[
        "[data-test='TotalPrice']",
        "[data-test*='total-price']",
        "[class*='TotalPrice']",
        "[class*='total-price']",
        "[class*='summary'] [class*='price']",
        "[data-test*='Price']",
    ],
))


# ─── Coverage Expansion — EveryMundo / httpx connectors ──────────────────
# These connectors have booking_url pointing to airline fare pages.
# The checkout engine navigates to that URL and proceeds through the
# standard airline booking flow.

_register(_base_cfg("Aegean Airlines", "aegean_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('GoLight')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept All')",
        "[class*='cookie'] button:has-text('Accept')",
    ],
))

_register(_base_cfg("Icelandair", "icelandair_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='journey']",
    fare_selectors=[
        "button:has-text('Economy Light')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
    ],
))

_register(_base_cfg("Air Canada", "aircanada_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
    ],
))

_register(_base_cfg("Finnair", "finnair_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Light')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
    ],
))

_register(_base_cfg("TAP Air Portugal", "tap_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Discount')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
    ],
))

_register(_base_cfg("SAS", "sas_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('SAS Go Light')",
        "button:has-text('SAS Go')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
    ],
))

_register(_base_cfg("Wingo", "wingo_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Sky Airline", "skyairline_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Arajet", "arajet_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Ethiopian Airlines", "ethiopian_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Kenya Airways", "kenyaairways_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Royal Air Maroc", "royalairmaroc_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Philippine Airlines", "philippineairlines_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("South African Airways", "saa_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Aer Lingus", "aerlingus_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Saver')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
    ],
))

_register(_base_cfg("Air New Zealand", "airnewzealand_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Seat')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
    ],
))

_register(_base_cfg("Virgin Australia", "virginaustralia_direct",
    # VA booking URLs go to the Virgin Australia booking page
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='fare']",
    fare_selectors=[
        "button:has-text('Choice')",
        "button:has-text('Getaway')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
    ],
))

# SpiceJet has dual source tags in engine (spicejet_direct vs spicejet_direct_api)
_register(_base_cfg("SpiceJet", "spicejet_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Book')",
    ],
))

# ─── Blocked airline stubs — redirect to manual booking URL ─────────────
# These connectors are blocked (no accessible API) but still registered in
# the engine. Their checkout configs exist so the engine doesn't error when
# queried — they cleanly return the booking URL for manual completion.

_register(_base_cfg("Air India", "airindia_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Qantas", "qantas_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("EgyptAir", "egyptair_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Japan Airlines", "jal_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Garuda Indonesia", "garuda_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("Bangkok Airways", "bangkokairways_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("ITA Airways", "itaairways_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

# ─── OTA / Aggregator connectors ────────────────────────────────────────
# OTAs have their own booking flows—checkout configs handle navigation
# through their specific checkout UIs (passenger forms, payment page).

_register(_base_cfg("Google Flights (SerpAPI)", "serpapi_google_ota",
    # SerpAPI returns Google Flights deep links → lands on airline checkout
    # or OTA checkout. The engine navigates the intermediary.
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='offer']",
    fare_selectors=["button:has-text('Select')"],
    cookie_selectors=[
        "button:has-text('Accept all')",
        "button:has-text('I agree')",
    ],
))

_register(_base_cfg("Traveloka", "traveloka_ota",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result'], [data-testid*='flight']",
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Book')",
    ],
    cookie_selectors=[
        "button:has-text('Accept')",
        "[class*='cookie'] button",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Skip')",
        "button:has-text('Later')",
        "[class*='close']",
    ],
))

_register(_base_cfg("Cleartrip", "cleartrip_ota",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Book')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Skip')",
        "[class*='close']",
    ],
))

_register(_base_cfg("Despegar", "despegar_ota",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='cluster']",
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Seleccionar')",
        "button:has-text('Comprar')",
    ],
    cookie_selectors=[
        "button:has-text('Accept')",
        "button:has-text('Aceptar')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Continuar sin cuenta')",
        "[class*='close']",
    ],
))

_register(_base_cfg("Wego", "wego_ota",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='deal']",
    fare_selectors=[
        "button:has-text('View Deal')",
        "button:has-text('Select')",
        "button:has-text('Book')",
    ],
    cookie_selectors=[
        "button:has-text('Accept')",
        "[class*='cookie'] button",
    ],
))

# ─── Source tag aliases ──────────────────────────────────────────────────
# Some connectors use different source tags in engine.py vs checkout_engine.
# Register aliases so checkout lookups work for both tags.

# Norwegian: engine registers "norwegian_direct", checkout has "norwegian_api"
AIRLINE_CONFIGS["norwegian_direct"] = AIRLINE_CONFIGS["norwegian_api"]

# Porter: engine registers "porter_direct", checkout has "porter_scraper"
AIRLINE_CONFIGS["porter_direct"] = AIRLINE_CONFIGS["porter_scraper"]

# Wizzair: engine registers "wizzair_direct", checkout has "wizzair_api"
AIRLINE_CONFIGS["wizzair_direct"] = AIRLINE_CONFIGS["wizzair_api"]


# ── Generic Checkout Engine ──────────────────────────────────────────────

class GenericCheckoutEngine:
    """
    Config-driven checkout engine — parametrised by AirlineCheckoutConfig.

    Drives the standard airline checkout flow using Playwright:
      page_loaded → flights_selected → fare_selected → login_bypassed →
      passengers_filled → extras_skipped → seats_skipped → payment_page_reached

    Never submits payment. Returns CheckoutProgress with screenshot + URL.
    """

    async def run(
        self,
        config: AirlineCheckoutConfig,
        offer: dict,
        passengers: list[dict],
        checkout_token: str,
        api_key: str,
        *,
        base_url: str | None = None,
        headless: bool = False,
    ) -> CheckoutProgress:
        t0 = time.monotonic()
        booking_url = offer.get("booking_url", "")
        offer_id = offer.get("id", "")

        # ── Verify checkout token ────────────────────────────────────
        try:
            verification = verify_checkout_token(offer_id, checkout_token, api_key, base_url)
            if not verification.get("valid"):
                return CheckoutProgress(
                    status="failed", airline=config.airline_name, source=config.source_tag,
                    offer_id=offer_id, booking_url=booking_url,
                    message="Checkout token invalid or expired. Call unlock() first.",
                )
        except Exception as e:
            return CheckoutProgress(
                status="failed", airline=config.airline_name, source=config.source_tag,
                offer_id=offer_id, booking_url=booking_url,
                message=f"Token verification failed: {e}",
            )

        if not booking_url:
            return CheckoutProgress(
                status="failed", airline=config.airline_name, source=config.source_tag,
                offer_id=offer_id, message="No booking URL available for this offer.",
            )

        # ── Launch browser ───────────────────────────────────────────
        from playwright.async_api import async_playwright
        import subprocess as _sp

        pw = await async_playwright().start()
        _chrome_proc = None  # CDP Chrome subprocess (if any)

        if config.use_cdp_chrome:
            # CDP mode: launch real Chrome as subprocess, connect via CDP.
            # This bypasses Kasada KPSDK — Playwright automation hooks are NOT
            # injected into the Chrome binary, so KPSDK JS runs naturally.
            from .browser import find_chrome, stealth_popen_kwargs
            chrome_path = find_chrome()
            _udd_name = config.cdp_user_data_dir or f".{config.source_tag}_chrome_data"
            _user_data_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                _udd_name,
            )
            os.makedirs(_user_data_dir, exist_ok=True)
            vp = random.choice([(1366, 768), (1440, 900), (1920, 1080)])
            cdp_args = [
                chrome_path,
                f"--remote-debugging-port={config.cdp_port}",
                f"--user-data-dir={_user_data_dir}",
                f"--window-size={vp[0]},{vp[1]}",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "about:blank",
            ]
            logger.info("%s checkout: launching CDP Chrome on port %d", config.airline_name, config.cdp_port)
            _chrome_proc = _sp.Popen(cdp_args, **stealth_popen_kwargs())
            import asyncio as _aio
            await _aio.sleep(3.0)  # give Chrome time to start CDP server
            try:
                browser = await pw.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{config.cdp_port}"
                )
            except Exception as cdp_err:
                logger.warning("%s checkout: CDP connect failed: %s", config.airline_name, cdp_err)
                _chrome_proc.terminate()
                _chrome_proc = None
                await pw.stop()
                return CheckoutProgress(
                    status="failed", airline=config.airline_name, source=config.source_tag,
                    offer_id=offer_id, booking_url=booking_url,
                    message=f"CDP Chrome launch failed: {cdp_err}",
                    elapsed_seconds=time.monotonic() - t0,
                )
        else:
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1440,900",
            ]

            # Proxy support (Decodo / residential proxy for anti-bot bypass)
            launch_kwargs: dict = {"headless": headless, "args": launch_args}
            if config.use_chrome_channel:
                launch_kwargs["channel"] = "chrome"
            if config.use_proxy:
                proxy_server = os.environ.get("DECODO_PROXY_SERVER", "")
                proxy_user = os.environ.get("DECODO_PROXY_USER", "")
                proxy_pass = os.environ.get("DECODO_PROXY_PASS", "")
                if proxy_server:
                    launch_kwargs["proxy"] = {
                        "server": proxy_server,
                        "username": proxy_user,
                        "password": proxy_pass,
                    }
                    logger.info("%s checkout: using proxy %s", config.airline_name, proxy_server)

            browser = await pw.chromium.launch(**launch_kwargs)

        # Track browser PID for guaranteed cleanup on cancellation
        _browser_pid = None
        try:
            _browser_pid = browser._impl_obj._browser_process.pid
        except Exception:
            pass
        if _chrome_proc:
            _browser_pid = _chrome_proc.pid

        def _force_kill_browser():
            """Synchronous kill — works even when asyncio is cancelled."""
            if _chrome_proc:
                try:
                    _chrome_proc.terminate()
                    _chrome_proc.wait(timeout=5)
                except Exception:
                    try:
                        _sp.run(["taskkill", "/F", "/T", "/PID", str(_chrome_proc.pid)],
                                capture_output=True, timeout=5)
                    except Exception:
                        pass
            elif _browser_pid:
                try:
                    _sp.run(["taskkill", "/F", "/T", "/PID", str(_browser_pid)],
                            capture_output=True, timeout=5)
                except Exception:
                    pass

        locale = random.choice(config.locale_pool) if config.locale_pool else config.locale
        tz = random.choice(config.timezone_pool) if config.timezone_pool else config.timezone

        ctx_kwargs = {
            "viewport": {"width": random.choice([1366, 1440, 1920]), "height": random.choice([768, 900, 1080])},
            "locale": locale,
            "timezone_id": tz,
        }
        if config.service_workers:
            ctx_kwargs["service_workers"] = config.service_workers

        if config.use_cdp_chrome and hasattr(browser, "contexts") and browser.contexts:
            # CDP mode: reuse the existing context from the connected Chrome
            context = browser.contexts[0]
        else:
            context = await browser.new_context(**ctx_kwargs)

        try:
            # Stealth (skip for CDP Chrome — it's already a real browser)
            if config.use_cdp_chrome:
                page = await context.new_page()
            else:
                try:
                    from playwright_stealth import stealth_async
                    page = await context.new_page()
                    await stealth_async(page)
                except ImportError:
                    page = await context.new_page()

            # CDP cache disable
            if config.disable_cache:
                try:
                    cdp = await context.new_cdp_session(page)
                    await cdp.send("Network.setCacheDisabled", {"cacheDisabled": True})
                except Exception:
                    pass

            step = "started"
            pax = passengers[0] if passengers else FAKE_PASSENGER

            # ── Homepage pre-load (Kasada, etc.) ─────────────────────
            if config.homepage_url:
                logger.info("%s checkout: loading homepage %s", config.airline_name, config.homepage_url)
                await page.goto(config.homepage_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
                await page.wait_for_timeout(config.homepage_wait_ms)
                await self._dismiss_cookies(page, config)

                # Storage cleanup (keep anti-bot tokens)
                if config.clear_storage_keep:
                    keep_prefixes = config.clear_storage_keep
                    await page.evaluate(f"""() => {{
                        try {{ sessionStorage.clear(); }} catch {{}}
                        try {{
                            const dominated = Object.keys(localStorage).filter(
                                k => !{keep_prefixes}.some(p => k.startsWith(p))
                            );
                            dominated.forEach(k => localStorage.removeItem(k));
                        }} catch {{}}
                    }}""")

            # ── Step 1: Navigate to booking page ─────────────────────

            # Check for custom checkout handler (e.g. WizzAir needs Vue SPA injection)
            if config.custom_checkout_handler:
                handler = getattr(self, config.custom_checkout_handler, None)
                if handler:
                    result = await handler(page, config, offer, offer_id, booking_url, passengers, t0)
                    if result is not None:
                        return result
                    # If handler returned None, fall through to generic flow
                else:
                    logger.warning("%s checkout: custom handler '%s' not found, using generic flow",
                                   config.airline_name, config.custom_checkout_handler)

            logger.info("%s checkout: navigating to %s", config.airline_name, booking_url)
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
            except Exception as nav_err:
                # Some SPAs return HTTP errors but still render via JS — continue if page loaded
                logger.warning("%s checkout: goto error (%s) — continuing", config.airline_name, str(nav_err)[:100])
            await page.wait_for_timeout(2000 if not config.homepage_url else 3000)
            await self._dismiss_cookies(page, config)

            # Guard against SPA redirects (e.g. Ryanair → check-in page)
            if booking_url.split("?")[0] not in page.url:
                logger.warning("%s checkout: page redirected to %s — retrying", config.airline_name, page.url[:120])
                try:
                    await page.goto(booking_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
                except Exception:
                    pass
                await page.wait_for_timeout(3000)
                await self._dismiss_cookies(page, config)

            step = "page_loaded"

            # ── Step 2: Select flights ───────────────────────────────
            try:
                await page.wait_for_selector(config.flight_cards_selector, timeout=config.flight_cards_timeout)
            except Exception:
                logger.warning("%s checkout: flight cards not visible", config.airline_name)
                # Debug: screenshot + page URL + visible button count
                try:
                    cur_url = page.url
                    vis_btns = await page.locator("button:visible").count()
                    logger.warning("%s debug: url=%s visible_buttons=%d", config.airline_name, cur_url[:120], vis_btns)
                    await page.screenshot(path=f"_checkout_screenshots/_debug_{config.source_tag}.png")
                except Exception:
                    pass

            await self._dismiss_cookies(page, config)

            # Match by departure time
            outbound = offer.get("outbound", {})
            segments = outbound.get("segments", []) if isinstance(outbound, dict) else []
            flight_clicked = False
            if segments:
                dep = segments[0].get("departure", "")
                if dep and len(dep) >= 16:
                    dep_time = dep[11:16]
                    try:
                        card = page.locator(f"text='{dep_time}'").first
                        if await card.is_visible(timeout=2000):
                            # Try clicking parent flight card
                            if config.flight_ancestor_tag:
                                try:
                                    parent = card.locator(f"xpath=ancestor::{config.flight_ancestor_tag}").first
                                    await parent.click()
                                    flight_clicked = True
                                except Exception:
                                    pass
                            if not flight_clicked:
                                await card.click()
                                flight_clicked = True
                    except Exception:
                        pass

            if not flight_clicked:
                await safe_click_first(page, config.first_flight_selectors, timeout=3000, desc="first flight")

            await page.wait_for_timeout(1500)
            step = "flights_selected"

            # ── Step 3: Select fare ──────────────────────────────────
            if config.fare_loop_enabled:
                # Wizzair-style multi-step fare selection
                for _ in range(10):
                    await page.wait_for_timeout(2500)
                    if config.fare_loop_done_selector:
                        try:
                            if await page.locator(config.fare_loop_done_selector).count() > 0:
                                break
                        except Exception:
                            pass
                    for sel in config.fare_loop_selectors:
                        await safe_click(page, sel, timeout=2000, desc="fare loop")
                    await self._dismiss_cookies(page, config)
            else:
                if await safe_click_first(page, config.fare_selectors, timeout=3000, desc="select fare"):
                    await page.wait_for_timeout(1000)
                    await safe_click_first(page, config.fare_upsell_decline, timeout=1500, desc="decline upsell")

            step = "fare_selected"
            await page.wait_for_timeout(1000)
            await self._dismiss_cookies(page, config)

            # ── Step 4: Skip login ───────────────────────────────────
            await safe_click_first(page, config.login_skip_selectors, timeout=2000, desc="skip login")
            await page.wait_for_timeout(1500)
            await self._dismiss_cookies(page, config)
            step = "login_bypassed"

            # ── Step 5: Fill passenger details ───────────────────────
            try:
                await page.wait_for_selector(config.passenger_form_selector, timeout=config.passenger_form_timeout)
            except Exception:
                pass

            # Title
            title_text = "Mr" if pax.get("gender", "m") == "m" else "Ms"
            if config.title_mode == "dropdown":
                if await safe_click_first(page, config.title_dropdown_selectors, timeout=2000, desc="title dropdown"):
                    await page.wait_for_timeout(500)
                    await safe_click(page, f"button:has-text('{title_text}')", timeout=2000)
            elif config.title_mode == "select":
                try:
                    await page.select_option(config.title_select_selector, label=title_text, timeout=2000)
                except Exception:
                    await safe_click(page, f"button:has-text('{title_text}')", timeout=1500, desc=f"title {title_text}")

            # First name
            await safe_fill_first(page, config.first_name_selectors, pax.get("given_name", "Test"))

            # Last name
            await safe_fill_first(page, config.last_name_selectors, pax.get("family_name", "Traveler"))

            # Gender (if required)
            if config.gender_enabled:
                gender = pax.get("gender", "m")
                sels = config.gender_selectors_male if gender == "m" else config.gender_selectors_female
                await safe_click_first(page, sels, timeout=2000, desc=f"gender {gender}")

            # Date of birth (if required)
            if config.dob_enabled:
                dob = pax.get("born_on", "1990-06-15")
                parts = dob.split("-")
                if len(parts) == 3:
                    year, month, day = parts
                    if config.dob_strip_leading_zero:
                        day = day.lstrip("0") or day
                        month = month.lstrip("0") or month
                    await safe_fill_first(page, config.dob_day_selectors, day)
                    await safe_fill_first(page, config.dob_month_selectors, month)
                    await safe_fill_first(page, config.dob_year_selectors, year)

            # Nationality (if required)
            if config.nationality_enabled:
                for sel in config.nationality_selectors:
                    if await safe_fill(page, sel, "GB"):
                        await page.wait_for_timeout(500)
                        try:
                            await page.locator(config.nationality_dropdown_item).first.click(timeout=2000)
                        except Exception:
                            pass
                        break

            # Email
            await safe_fill_first(page, config.email_selectors, pax.get("email", "test@example.com"))

            # Phone
            await safe_fill_first(page, config.phone_selectors, pax.get("phone_number", "+441234567890"))

            step = "passengers_filled"

            # Pre-extras hooks (Wizzair baggage checkbox, PRM, etc.)
            for hook in config.pre_extras_hooks:
                action = hook.get("action", "click")
                sels = hook.get("selectors", [])
                desc = hook.get("desc", "")
                if action == "click":
                    await safe_click_first(page, sels, timeout=2000, desc=desc)
                elif action == "escape":
                    for sel in sels:
                        try:
                            if await page.locator(sel).first.is_visible(timeout=1000):
                                await page.keyboard.press("Escape")
                        except Exception:
                            pass
                elif action == "check":
                    for sel in sels:
                        try:
                            el = page.locator(sel).first
                            if await el.is_visible(timeout=1500):
                                await el.check()
                        except Exception:
                            pass

            # Continue past passengers
            await safe_click_first(page, config.passenger_continue_selectors, timeout=2000, desc="continue after passengers")
            await page.wait_for_timeout(1500)
            await self._dismiss_cookies(page, config)

            # ── Step 6: Skip extras ──────────────────────────────────
            for _round in range(config.extras_rounds):
                await self._dismiss_cookies(page, config)
                # Fast combined probe: any extras button visible?
                if not config.extras_skip_selectors:
                    break
                combined = page.locator(config.extras_skip_selectors[0])
                for sel in config.extras_skip_selectors[1:]:
                    combined = combined.or_(page.locator(sel))
                try:
                    if not await combined.first.is_visible(timeout=1500):
                        break  # No extras buttons, bail all rounds
                except Exception:
                    break
                # Something visible — click each matching selector individually
                for sel in config.extras_skip_selectors:
                    try:
                        el = page.locator(sel).first
                        if await el.is_visible(timeout=300):
                            await el.click()
                            await page.wait_for_timeout(300)
                    except Exception:
                        pass
                await page.wait_for_timeout(1000)

            step = "extras_skipped"

            # ── Step 7: Skip seats ───────────────────────────────────
            await safe_click_first(page, config.seats_skip_selectors, timeout=2000, desc="skip seats")
            await page.wait_for_timeout(1000)
            await safe_click_first(page, config.seats_confirm_selectors, timeout=1500, desc="confirm skip seats")

            step = "seats_skipped"
            await page.wait_for_timeout(1000)
            await self._dismiss_cookies(page, config)

            # ── Step 8: Payment page — STOP HERE ─────────────────────
            step = "payment_page_reached"
            screenshot = await take_screenshot_b64(page)

            # Extract displayed price
            page_price = offer.get("price", 0.0)
            for sel in config.price_selectors:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=2000):
                        text = await el.text_content()
                        if text:
                            nums = re.findall(r"[\d,.]+", text)
                            if nums:
                                page_price = float(nums[-1].replace(",", ""))
                        break
                except Exception:
                    continue

            elapsed = time.monotonic() - t0
            return CheckoutProgress(
                status="payment_page_reached",
                step=step,
                step_index=8,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                total_price=page_price,
                currency=offer.get("currency", "EUR"),
                booking_url=booking_url,
                screenshot_b64=screenshot,
                message=(
                    f"{config.airline_name} checkout complete — reached payment page in {elapsed:.0f}s. "
                    f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                    f"Payment NOT submitted (safe mode). "
                    f"Complete manually at: {booking_url}"
                ),
                can_complete_manually=True,
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            logger.error("%s checkout error: %s", config.airline_name, e, exc_info=True)
            screenshot = ""
            try:
                screenshot = await take_screenshot_b64(page)
            except Exception:
                pass
            return CheckoutProgress(
                status="error",
                step=step,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                booking_url=booking_url,
                screenshot_b64=screenshot,
                message=f"Checkout error at step '{step}': {e}",
                elapsed_seconds=time.monotonic() - t0,
            )
        finally:
            # Graceful close, then force-kill as fallback
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass
            try:
                await pw.stop()
            except Exception:
                pass
            # Synchronous kill — guarantees browser dies even on CancelledError
            _force_kill_browser()

    async def _wizzair_checkout(self, page, config, offer, offer_id, booking_url, passengers, t0):
        """WizzAir custom checkout: API search + Vuex injection + Vue Router navigation.

        WizzAir's Vue 2 SPA has Kasada KPSDK anti-bot + route guards that block
        direct page.goto().  We bypass by:
        1. Load homepage (KPSDK initialises from cached Chrome profile)
        2. Call search API via fetch() — KPSDK injects challenge headers
        3. Inject results into Vuex store
        4. Remove Vue Router guards
        5. Navigate by route name → VUE renders flight selection
        6. Click flight → fare → Continue → passengers → payment
        """
        import re as _re

        pax = passengers[0] if passengers else FAKE_PASSENGER
        step = "init"

        # Extract origin/dest/date from booking_url or offer
        origin = offer.get("outbound", {}).get("segments", [{}])[0].get("origin", "")
        dest = offer.get("outbound", {}).get("segments", [{}])[0].get("destination", "")
        dep_date = offer.get("outbound", {}).get("segments", [{}])[0].get("departure", "")[:10]

        if not origin or not dest or not dep_date:
            # Parse from booking URL: .../BUD/LTN/2026-04-16/...
            parts = booking_url.rstrip("/").split("/")
            for i, p in enumerate(parts):
                if _re.match(r"^[A-Z]{3}$", p) and i + 1 < len(parts) and _re.match(r"^[A-Z]{3}$", parts[i + 1]):
                    origin, dest = p, parts[i + 1]
                    if i + 2 < len(parts) and _re.match(r"^\d{4}-\d{2}-\d{2}$", parts[i + 2]):
                        dep_date = parts[i + 2]
                    break

        if not all([origin, dest, dep_date]):
            logger.warning("WizzAir checkout: could not extract route from offer/URL")
            return None  # fall through to generic

        # Detect API version from page content
        content = await page.content()
        m = _re.search(r'be\.wizzair\.com/(\d+\.\d+\.\d+)/', content)
        api_version = m.group(1) if m else "28.2.0"
        logger.info("WizzAir checkout: %s→%s on %s, API v%s", origin, dest, dep_date, api_version)

        # Dismiss cookies
        try:
            await page.evaluate("() => { try { UC_UI.acceptAllConsents(); } catch {} }")
        except Exception:
            pass
        await self._dismiss_cookies(page, config)

        # ── Search API + Vuex injection + guard bypass + navigate ─────
        result = await page.evaluate("""async (params) => {
            const {origin, dest, depDate, version} = params;
            const store = document.querySelector('#app').__vue__.$store;
            const router = document.querySelector('#app').__vue__.$router;
            const Vue = document.querySelector('#app').__vue__.$root.constructor;
            const log = [];

            // 1. Search API
            const rvt = (document.cookie.split('; ').find(c => c.startsWith('RequestVerificationToken=')) || '').split('=').slice(1).join('=');
            const resp = await fetch(`https://be.wizzair.com/${version}/Api/search/search`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json;charset=UTF-8', 'X-RequestVerificationToken': rvt},
                body: JSON.stringify({
                    flightList: [{departureStation: origin, arrivalStation: dest, departureDate: depDate}],
                    adultCount: 1, childCount: 0, infantCount: 0,
                    wdc: true, isFlightChange: false, isSeniorOrStudent: false,
                    rescueFareCode: '', priceType: 'regular',
                }),
                credentials: 'include',
            });
            if (resp.status !== 200) return {error: 'search_api_' + resp.status};
            const data = await resp.json();
            log.push('flights:' + (data.outboundFlights?.length || 0));

            // 2. Vuex state injection
            const stations = store.state.resources?.stations || [];
            const depStn = stations.find(s => s.iata === origin);
            const arrStn = stations.find(s => s.iata === dest);
            store.state.search.departureStation = depStn;
            store.state.search.arrivalStation = arrStn;
            store.state.search.departureDate = depDate;
            store.state.search.returnDate = null;
            store.state.search.isLoaded = true;
            store.state.search.currencyCode = data.currencyCode;
            store.state.search.isDomestic = data.isDomestic || false;
            try { store.commit('search/setPassengers', {adultCount: 1, childCount: 0, infantCount: 0}); } catch(e) {}
            if (!store.state.search.outbound) {
                Vue.set(store.state.search, 'outbound', {flights: [], bundles: [], fees: {}});
            }
            Vue.set(store.state.search.outbound, 'flights', data.outboundFlights || []);
            Vue.set(store.state.search.outbound, 'bundles', data.outboundBundles || []);

            // 3. Remove ALL route guards
            router.beforeHooks = [];
            router.resolveHooks = [];
            const target = router.resolve({
                name: 'select-flight',
                params: {locale: 'en-gb', departureStationIata: origin, arrivalStationIata: dest,
                         departureDate: depDate, returnDate: '', adult: '1', child: '0', infant: '0'}
            });
            if (target.route?.matched) {
                for (const rec of target.route.matched) rec.beforeEnter = null;
            }

            // 4. Navigate to flight selection
            try {
                await router.push({
                    name: 'select-flight',
                    params: {locale: 'en-gb', departureStationIata: origin, arrivalStationIata: dest,
                             departureDate: depDate, returnDate: '', adult: '1', child: '0', infant: '0'}
                });
                log.push('nav:ok');
            } catch(e) {
                log.push('nav:' + e.message?.slice(0, 80));
            }

            return {log, flights: data.outboundFlights?.length || 0};
        }""", {"origin": origin, "dest": dest, "depDate": dep_date, "version": api_version})

        if result.get("error"):
            logger.warning("WizzAir checkout: %s", result["error"])
            return CheckoutProgress(
                status="failed", airline=config.airline_name, source=config.source_tag,
                offer_id=offer_id, booking_url=booking_url,
                message=f"WizzAir search API failed: {result['error']}",
                elapsed_seconds=time.monotonic() - t0,
            )

        logger.info("WizzAir checkout: %d flights, nav: %s", result.get("flights", 0), result.get("log", []))
        step = "flights_loaded"

        # ── Wait for flight cards to render ──────────────────────────
        await page.wait_for_timeout(5000)

        # Click first flight card
        await page.evaluate("""() => {
            const card = document.querySelector('[data-test="flight-select-flight0"]')
                || document.querySelector('[data-test*="flight-select-flight"]');
            if (card) card.click();
        }""")
        await page.wait_for_timeout(3000)

        # Select cheapest fare
        await page.evaluate("""() => {
            const btns = document.querySelectorAll('[data-test="select-fare"]');
            if (btns.length > 0) btns[0].click();
        }""")
        await page.wait_for_timeout(3000)
        step = "fare_selected"

        # Click Continue
        await page.evaluate("""() => {
            const btns = [...document.querySelectorAll('button')].filter(b =>
                b.innerText.trim().toLowerCase().startsWith('continue'));
            if (btns.length > 0) btns[0].click();
        }""")
        await page.wait_for_timeout(6000)

        # Navigate to passengers (guard bypass)
        await page.evaluate("""async () => {
            const router = document.querySelector('#app').__vue__.$router;
            router.beforeHooks = [];
            router.resolveHooks = [];
            const target = router.resolve({name: 'passengers', params: {locale: 'en-gb'}});
            if (target.route?.matched) {
                for (const rec of target.route.matched) rec.beforeEnter = null;
            }
            await router.push({name: 'passengers', params: {locale: 'en-gb'}});
        }""")
        await page.wait_for_timeout(5000)
        step = "passengers_page"

        # Fill passenger form
        given_name = pax.get("given_name", "Test")
        family_name = pax.get("family_name", "Traveler")
        await page.evaluate("""(params) => {
            function fill(sel, val) {
                const el = document.querySelector(sel);
                if (!el) return;
                const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                setter.call(el, val);
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                el.dispatchEvent(new Event('blur', {bubbles: true}));
            }
            fill('[data-test="passenger-first-name-0"]', params.first);
            fill('[data-test="passenger-last-name-0"]', params.last);
            const male = [...document.querySelectorAll('input[name="gender0"]')].find(r => r.value === 'male');
            if (male) male.click();
        }""", {"first": given_name, "last": family_name})
        await page.wait_for_timeout(2000)
        step = "passengers_filled"

        # Navigate to payment (skip seats/services via route)
        await page.evaluate("""async () => {
            const router = document.querySelector('#app').__vue__.$router;
            router.beforeHooks = [];
            router.resolveHooks = [];
            const target = router.resolve({name: 'payment', params: {locale: 'en-gb'}});
            if (target.route?.matched) {
                for (const rec of target.route.matched) rec.beforeEnter = null;
            }
            await router.push({name: 'payment', params: {locale: 'en-gb'}});
        }""")
        await page.wait_for_timeout(5000)
        step = "payment_page_reached"

        # Extract price
        page_price = offer.get("price", 0.0)
        try:
            text = await page.evaluate("""() => {
                const el = document.querySelector('[data-test*="total-price"], [class*="total"]');
                return el?.innerText || '';
            }""")
            if text:
                import re as _re2
                nums = _re2.findall(r"[\d,.]+", text)
                if nums:
                    page_price = float(nums[-1].replace(",", ""))
        except Exception:
            pass

        screenshot = await take_screenshot_b64(page)
        elapsed = time.monotonic() - t0

        return CheckoutProgress(
            status="payment_page_reached",
            step=step,
            step_index=8,
            airline=config.airline_name,
            source=config.source_tag,
            offer_id=offer_id,
            total_price=page_price,
            currency=offer.get("currency", "EUR"),
            booking_url=booking_url,
            screenshot_b64=screenshot,
            message=(
                f"Wizz Air checkout complete — reached payment page in {elapsed:.0f}s. "
                f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                f"Payment NOT submitted (safe mode). "
                f"Complete manually at: {booking_url}"
            ),
            can_complete_manually=True,
            elapsed_seconds=elapsed,
        )

    async def _dismiss_cookies(self, page, config: AirlineCheckoutConfig) -> None:
        """Dismiss cookie banners using airline-specific selectors (fast combined check)."""
        if not config.cookie_selectors:
            return
        try:
            combined = page.locator(config.cookie_selectors[0])
            for sel in config.cookie_selectors[1:]:
                combined = combined.or_(page.locator(sel))
            btn = combined.first
            if await btn.is_visible(timeout=800):
                await btn.click(force=True)
                await page.wait_for_timeout(500)
        except Exception:
            pass
        # Fallback: remove any remaining blocking overlays via JS
        try:
            await page.evaluate("""() => {
                for (const sel of ['#cookie-preferences', '#onetrust-consent-sdk',
                    '#CybotCookiebotDialog', '[class*="cookie-popup"]',
                    '[class*="cookie-overlay"]', '[class*="consent-banner"]']) {
                    const el = document.querySelector(sel);
                    if (el) el.remove();
                }
            }""")
        except Exception:
            pass
