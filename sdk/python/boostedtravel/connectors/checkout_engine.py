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
    safe_fill,
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
    homepage_wait_ms: int = 5000       # Wait after homepage load
    clear_storage_keep: list[str] = field(default_factory=list)  # localStorage prefixes to KEEP

    # Anti-bot
    service_workers: str = ""          # "block" | "" — block SW for cleaner interception
    disable_cache: bool = False        # CDP Network.setCacheDisabled
    locale: str = "en-GB"
    locale_pool: list[str] = field(default_factory=list)  # Random locale from pool
    timezone: str = "Europe/London"
    timezone_pool: list[str] = field(default_factory=list)

    # Cookie/overlay dismissal (tried in order)
    cookie_selectors: list[str] = field(default_factory=lambda: [
        "button:has-text('Accept')",
        "button:has-text('Accept All')",
        "button:has-text('Accept all cookies')",
        "button:has-text('Agree')",
        "button:has-text('Yes, I agree')",
        "button:has-text('Got it')",
        "button:has-text('OK')",
        "[class*='cookie'] button",
        "[id*='cookie'] button",
    ])

    # Flight selection
    flight_cards_selector: str = "[data-ref*='flight-card'], flight-card, [class*='flight-card'], [data-test*='flight'], [class*='flight-select'], [class*='flight-row']"
    flight_cards_timeout: int = 15000
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
    passenger_form_timeout: int = 15000

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
        "button:has-text('Continue')",
        "button:has-text('Next')",
        "button[data-test='passengers-continue-btn']",
        "[data-test*='continue'] button",
        "[data-testid*='continue'] button",
    ])

    # Wizzair-style extras on passengers page (baggage checkbox, PRM, etc.)
    pre_extras_hooks: list[dict] = field(default_factory=list)
    # Format: [{"action": "click"|"check"|"escape", "selectors": [...], "desc": "..."}]

    # Skip extras (bags, insurance, priority)
    extras_rounds: int = 4  # How many times to try skipping
    extras_skip_selectors: list[str] = field(default_factory=lambda: [
        "button:has-text('Continue without')",
        "button:has-text('No thanks')",
        "button:has-text('No, thanks')",
        "button:has-text('OK, got it')",
        "button:has-text('Continue')",
        "button:has-text('Not interested')",
        "button:has-text('Skip')",
        "button:has-text('I don\\'t need')",
        "button:has-text('Next')",
        "button:has-text('No hold luggage')",
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
        "button:has-text('OK')",
        "button:has-text('Yes')",
        "button:has-text('Continue')",
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
    cookie_selectors=[
        "button[data-ref='cookie.accept-all']",
        "button:has-text('Accept')",
        "button:has-text('Agree')",
        "button:has-text('Yes, I agree')",
        "[class*='cookie'] button",
        "button:has-text('Got it')",
        "button:has-text('OK')",
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
    homepage_url="https://wizzair.com/en-gb",
    homepage_wait_ms=5000,
    clear_storage_keep=["kpsdk", "_kas"],
    locale_pool=["en-GB", "en-US", "en-IE"],
    timezone_pool=["Europe/Warsaw", "Europe/London", "Europe/Budapest"],
    cookie_selectors=[
        "button[data-test='cookie-policy-button-accept']",
        "[class*='cookie'] button",
        "button:has-text('Accept')",
        "button:has-text('Got it')",
        "[data-test='modal-close']",
        "button[class*='close']",
        "button:has-text('No, thanks')",
        "button:has-text('Not now')",
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
    flight_cards_selector="[class*='flight-detail'], [class*='flight-row']",
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
    flight_cards_selector="[class*='flight-result'], [class*='flight-card']",
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
    flight_cards_selector="[class*='flight-card'], [class*='result']",
    fare_selectors=[
        "button:has-text('Bare Fare')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("JetBlue", "jetblue_direct",
    flight_cards_selector="[class*='flight-card'], [data-testid*='flight']",
    fare_selectors=[
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
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("Breeze", "breeze_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
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
    flight_cards_selector="[class*='flight-card'], [class*='result']",
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
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
))

_register(_base_cfg("VivaAerobus", "vivaaerobus_direct",
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
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

_register(_base_cfg("US-Bangla", "usbangla_direct",
    flight_cards_selector="[class*='flight'], [class*='result']",
    fare_selectors=["button:has-text('Select')"],
))

# ─── Full-service carriers (deep-link capable) ──────────────────────────

_register(_base_cfg("Cathay Pacific", "cathay_direct",
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
                    message="Checkout token invalid or expired. Call unlock() first ($1 fee).",
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

        pw = await async_playwright().start()
        launch_args = ["--disable-blink-features=AutomationControlled"]
        browser = await pw.chromium.launch(headless=False, args=launch_args)

        locale = random.choice(config.locale_pool) if config.locale_pool else config.locale
        tz = random.choice(config.timezone_pool) if config.timezone_pool else config.timezone

        ctx_kwargs = {
            "viewport": {"width": random.choice([1366, 1440, 1920]), "height": random.choice([768, 900, 1080])},
            "locale": locale,
            "timezone_id": tz,
        }
        if config.service_workers:
            ctx_kwargs["service_workers"] = config.service_workers

        context = await browser.new_context(**ctx_kwargs)

        try:
            # Stealth
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
                await page.goto(config.homepage_url, wait_until="domcontentloaded", timeout=30000)
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
            logger.info("%s checkout: navigating to %s", config.airline_name, booking_url)
            await page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000 if not config.homepage_url else 3000)
            await self._dismiss_cookies(page, config)
            step = "page_loaded"

            # ── Step 2: Select flights ───────────────────────────────
            try:
                await page.wait_for_selector(config.flight_cards_selector, timeout=config.flight_cards_timeout)
            except Exception:
                logger.warning("%s checkout: flight cards not visible", config.airline_name)

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
                        if await card.is_visible(timeout=3000):
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
                for sel in config.first_flight_selectors:
                    if await safe_click(page, sel, timeout=3000, desc="first flight"):
                        break

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
                for sel in config.fare_selectors:
                    if await safe_click(page, sel, timeout=5000, desc="select fare"):
                        await page.wait_for_timeout(1000)
                        for upsell in config.fare_upsell_decline:
                            await safe_click(page, upsell, timeout=3000, desc="decline upsell")
                        break

            step = "fare_selected"
            await page.wait_for_timeout(1500)
            await self._dismiss_cookies(page, config)

            # ── Step 4: Skip login ───────────────────────────────────
            for sel in config.login_skip_selectors:
                if await safe_click(page, sel, timeout=5000, desc="skip login"):
                    break
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
                for sel in config.title_dropdown_selectors:
                    if await safe_click(page, sel, timeout=5000, desc="title dropdown"):
                        await page.wait_for_timeout(500)
                        await safe_click(page, f"button:has-text('{title_text}')", timeout=3000)
                        break
            elif config.title_mode == "select":
                try:
                    await page.select_option(config.title_select_selector, label=title_text, timeout=3000)
                except Exception:
                    await safe_click(page, f"button:has-text('{title_text}')", timeout=2000, desc=f"title {title_text}")

            # First name
            for sel in config.first_name_selectors:
                if await safe_fill(page, sel, pax.get("given_name", "Test")):
                    break

            # Last name
            for sel in config.last_name_selectors:
                if await safe_fill(page, sel, pax.get("family_name", "Traveler")):
                    break

            # Gender (if required)
            if config.gender_enabled:
                gender = pax.get("gender", "m")
                sels = config.gender_selectors_male if gender == "m" else config.gender_selectors_female
                for sel in sels:
                    if await safe_click(page, sel, timeout=3000, desc=f"gender {gender}"):
                        break

            # Date of birth (if required)
            if config.dob_enabled:
                dob = pax.get("born_on", "1990-06-15")
                parts = dob.split("-")
                if len(parts) == 3:
                    year, month, day = parts
                    if config.dob_strip_leading_zero:
                        day = day.lstrip("0") or day
                        month = month.lstrip("0") or month
                    for sel in config.dob_day_selectors:
                        if await safe_fill(page, sel, day):
                            break
                    for sel in config.dob_month_selectors:
                        if await safe_fill(page, sel, month):
                            break
                    for sel in config.dob_year_selectors:
                        if await safe_fill(page, sel, year):
                            break

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
            for sel in config.email_selectors:
                if await safe_fill(page, sel, pax.get("email", "test@example.com")):
                    break

            # Phone
            for sel in config.phone_selectors:
                if await safe_fill(page, sel, pax.get("phone_number", "+441234567890")):
                    break

            step = "passengers_filled"

            # Pre-extras hooks (Wizzair baggage checkbox, PRM, etc.)
            for hook in config.pre_extras_hooks:
                action = hook.get("action", "click")
                sels = hook.get("selectors", [])
                desc = hook.get("desc", "")
                if action == "click":
                    for sel in sels:
                        if await safe_click(page, sel, timeout=3000, desc=desc):
                            break
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
                            if await el.is_visible(timeout=2000):
                                await el.check()
                        except Exception:
                            pass

            # Continue past passengers
            for sel in config.passenger_continue_selectors:
                if await safe_click(page, sel, timeout=5000, desc="continue after passengers"):
                    break
            await page.wait_for_timeout(2000)
            await self._dismiss_cookies(page, config)

            # ── Step 6: Skip extras ──────────────────────────────────
            for _ in range(config.extras_rounds):
                await self._dismiss_cookies(page, config)
                for sel in config.extras_skip_selectors:
                    await safe_click(page, sel, timeout=2000, desc="skip extras")
                await page.wait_for_timeout(1000)

            step = "extras_skipped"

            # ── Step 7: Skip seats ───────────────────────────────────
            for sel in config.seats_skip_selectors:
                if await safe_click(page, sel, timeout=4000, desc="skip seats"):
                    break
            await page.wait_for_timeout(1000)
            for sel in config.seats_confirm_selectors:
                await safe_click(page, sel, timeout=3000, desc="confirm skip seats")

            step = "seats_skipped"
            await page.wait_for_timeout(2000)
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
            await context.close()
            await browser.close()
            await pw.stop()

    async def _dismiss_cookies(self, page, config: AirlineCheckoutConfig) -> None:
        """Dismiss cookie banners using airline-specific selectors."""
        for selector in config.cookie_selectors:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=600):
                    await btn.click()
                    await page.wait_for_timeout(300)
            except Exception:
                continue
