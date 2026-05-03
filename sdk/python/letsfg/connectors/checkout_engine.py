"""
Config-driven checkout engine — covers 79 airline connectors.

Instead of writing 79 individual Playwright scripts, this engine runs ONE
generic checkout flow parametrised by airline-specific selector configs.

All airlines follow the same basic checkout pattern:
  1. Navigate to booking URL
  2. Dismiss cookie/overlay banners
  3. Select flights (by departure time)
  4. Select fare tier

            # First name
            await safe_fill_first(page, config.first_name_selectors, pax.get("given_name", "Test"))

            # Last name
            await safe_fill_first(page, config.last_name_selectors, pax.get("family_name", "Traveler"))

            # Gender (if required)
            if config.gender_enabled:
                gender = pax.get("gender", "m")
                sels = config.gender_selectors_male if gender == "m" else config.gender_selectors_female
                await safe_click_first(page, sels, timeout=2000, desc=f"gender {gender}")

This module exports:
  - AirlineCheckoutConfig: dataclass with all per-airline selectors/settings
  - AIRLINE_CONFIGS: dict mapping source_tag → AirlineCheckoutConfig
  - GenericCheckoutEngine: the unified engine
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import re
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
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
    safe_type_first,
    take_screenshot_b64,
    verify_checkout_token,
)

logger = logging.getLogger(__name__)


def _extract_hhmm(value: object) -> str:
    if isinstance(value, datetime):
        return value.strftime("%H:%M")
    text = str(value or "").strip()
    if len(text) >= 16:
        return text[11:16]
    if len(text) >= 5 and text[2] == ":" and text[:2].isdigit() and text[3:5].isdigit():
        return text[:5]
    return ""


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
    details_extractor_handler: str = ""  # method on GenericCheckoutEngine that extracts add-on/breakdown data from the current page

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
    dob_single_input_selectors: list[str] = field(default_factory=list)

    # Nationality (some airlines require it)
    nationality_enabled: bool = False
    nationality_selectors: list[str] = field(default_factory=list)
    nationality_fill_value: str = "GB"
    nationality_dropdown_item: str = "[class*='dropdown'] [class*='item']:first-child"

    # Travel document / contact accordions used by some checkout flows
    document_number_selectors: list[str] = field(default_factory=list)
    document_expiry_selectors: list[str] = field(default_factory=list)
    issuance_country_selectors: list[str] = field(default_factory=list)
    issuance_country_fill_value: str = ""
    issuance_country_dropdown_item: str = "[class*='dropdown'] [class*='item']:first-child"
    contact_section_expand_selectors: list[str] = field(default_factory=list)
    contact_first_name_selectors: list[str] = field(default_factory=list)
    contact_last_name_selectors: list[str] = field(default_factory=list)

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
    confirm_email_selectors: list[str] = field(default_factory=list)
    phone_type_selectors: list[str] = field(default_factory=list)
    phone_type_option_selectors: list[str] = field(default_factory=list)
    phone_country_code_selectors: list[str] = field(default_factory=list)
    phone_country_code_value: str = ""
    phone_country_code_dropdown_item: str = "[class*='dropdown'] [class*='item']:first-child"
    consent_checkbox_selectors: list[str] = field(default_factory=list)
    phone_digits_only: bool = False
    phone_grouping: list[int] = field(default_factory=list)
    phone_type_delay_ms: int = 0
    phone_local_digits_count: int = 0
    pre_passenger_continue_settle_ms: int = 0
    post_passenger_transition_timeout_ms: int = 1500
    passenger_continue_retries: int = 0

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
    overrides.setdefault("details_extractor_handler", "_extract_generic_visible_checkout_details")
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

# Spirit Airlines shut down May 2, 2026 — removed

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
    custom_checkout_handler="_jetsmart_checkout",
    use_cdp_chrome=True,
    cdp_port=9513,
    cdp_user_data_dir="chrome-cdp-jetsmart",
    flight_cards_timeout=20000,
    flight_cards_selector="div[class*='rounded-lg'][class*='sm:grid']:has-text('Vuelo directo'):has-text('Tarifa desde'), div[class*='rounded-lg'][class*='sm:grid']:has-text('Direct flight'):has-text('Fare from')",
    first_flight_selectors=[
        "div[class*='rounded-lg'][class*='sm:grid']:has-text('Vuelo directo'):has-text('Tarifa desde')",
        "div[class*='rounded-lg'][class*='sm:grid']:has-text('Direct flight'):has-text('Fare from')",
    ],
    fare_selectors=[
        "div[class*='cursor-pointer']:has-text('Tarifa desde')",
        "div[class*='cursor-pointer']:has-text('Fare from')",
        "div[class*='cursor-pointer']:has-text('Club de descuentos')",
        "div[class*='cursor-pointer']:has-text('Discount club')",
    ],
))

_register(_base_cfg("Volaris", "volaris_direct",
    custom_checkout_handler="_volaris_checkout",
    details_extractor_handler="_extract_volaris_checkout_details",
    use_chrome_channel=True,
    service_workers="block",
    flight_cards_timeout=20000,
    locale_pool=["en-US", "es-MX", "en-GB", "es-US"],
    timezone_pool=[
        "America/Mexico_City",
        "America/Cancun",
        "America/Tijuana",
        "America/Chicago",
        "America/Los_Angeles",
    ],
    flight_cards_selector="mbs-flight-lists .flightItem, .flightLists .flightItem, .flightItem .flightFares a[role='button'], a.panel-open[role='button']",
    first_flight_selectors=[
        ".flightItem .flightFares a[role='button']",
        "a.panel-open[role='button']",
        "mbs-flight-lists .flightItem:first-child a[role='button']",
    ],
    fare_selectors=[
        "mbs-flight-fares button.btn-select",
        "button:has-text('Seleccionar')",
        "button:has-text('Basic')",
        "button:has-text('Select')",
    ],
    fare_upsell_decline=[
        "button:has-text('Mantener Zero')",
        "button:has-text('Mantener Básica')",
        "button:has-text('Keep Zero')",
        "button:has-text('Keep Basic')",
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

_register(_base_cfg("Middle East Airlines", "mea_direct",
    goto_timeout=60000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='journey'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Skip')",
        "button:has-text('Not now')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Continue')",
        "button:has-text('Skip')",
    ],
    seats_skip_selectors=[
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without seats')",
        "button:has-text('No thanks')",
        "button:has-text('Skip')",
    ],
    details_extractor_handler="_extract_generic_visible_checkout_details",
))

_register(_base_cfg("Air Cairo", "aircairo_direct",
    goto_timeout=60000,
    use_proxy=True,
    use_cdp_chrome=True,
    cdp_port=9487,
    cdp_user_data_dir=".aircairo_chrome_data",
    homepage_url="https://online.aircairo.com/booking?lang=en-GB",
    homepage_wait_ms=4000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='journey'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Choose')",
        "button:has-text('Economy')",
    ],
    login_skip_selectors=[
        "button:has-text('Fill passenger details')",
        "button:has-text('Continue as guest')",
        "button:has-text('Skip')",
        "button:has-text('Not now')",
    ],
    title_mode="dropdown",
    title_dropdown_selectors=[
        "mat-select[id*='title']",
        "[role='combobox'][id*='title']",
    ],
    passenger_form_selector="input[placeholder*='first name' i], input[placeholder*='last name' i], input[placeholder*='email' i], input[placeholder='Day / Month / Year']",
    first_name_selectors=[
        "input[id*='PersonalInfofirstName']",
        "input[placeholder='Enter a first name']",
        "input[placeholder*='first name' i]",
    ],
    last_name_selectors=[
        "input[id*='PersonalInfolastName']",
        "input[placeholder='Enter a last name']",
        "input[placeholder*='last name' i]",
    ],
    gender_enabled=True,
    gender_selectors_male=[
        "button[role='radio']:has-text('Male')",
        "button:has-text('Male')",
    ],
    gender_selectors_female=[
        "button[role='radio']:has-text('Female')",
        "button:has-text('Female')",
    ],
    dob_enabled=True,
    dob_single_input_selectors=[
        "input[id*='PersonalInfodob']",
        "xpath=(//input[@placeholder='Day / Month / Year'])[1]",
    ],
    nationality_enabled=True,
    nationality_selectors=[
        "input[placeholder='Nationality']",
        "input[id*='nationality']",
    ],
    nationality_fill_value="British",
    nationality_dropdown_item="mat-option:has-text('British'), [role='option']:has-text('British')",
    document_number_selectors=[
        "input[placeholder='Your document number']",
        "input[id*='documentNumber']",
    ],
    document_expiry_selectors=[
        "input[id*='expiryDate']",
        "xpath=(//input[@placeholder='Day / Month / Year'])[2]",
    ],
    issuance_country_selectors=[
        "input[placeholder='Government']",
        "input[id*='issuanceCountry']",
    ],
    issuance_country_fill_value="United Kingdom",
    issuance_country_dropdown_item="mat-option:has-text('United Kingdom'), [role='option']:has-text('United Kingdom')",
    email_selectors=[
        "input[placeholder='Enter an email address']",
        "input[id*='emailItem-0email']",
        "input[type='email']",
    ],
    confirm_email_selectors=[
        "input[placeholder='Confirm an email address']",
        "input[id*='confirmedEmail']",
    ],
    phone_type_selectors=[
        "mat-select[id*='phoneType']",
        "[role='combobox'][id*='phoneType']",
    ],
    phone_type_option_selectors=[
        "mat-option:has-text('Personal')",
        "[role='option']:has-text('Personal')",
    ],
    phone_country_code_selectors=[
        "input[placeholder='Enter a country calling code']",
        "input[id*='phoneCountryCode']",
    ],
    phone_country_code_value="44",
    phone_country_code_dropdown_item="mat-option:has-text('+44'), [role='option']:has-text('+44'), mat-option:has-text('44'), [role='option']:has-text('44')",
    phone_selectors=[
        "input[placeholder='Enter a mobile phone']",
        "input[id*='phoneItem-0phone']",
        "input[type='tel']",
    ],
    phone_digits_only=True,
    phone_local_digits_count=10,
    consent_checkbox_selectors=[
        "input[type='checkbox']",
        "label[for='gdprConsent-input']",
        "#gdprConsent-input",
        "label:has-text('I understand and accept')",
    ],
    passenger_continue_selectors=[
        "button:has-text('Confirm')",
        "button:has-text('Continue')",
        "button:has-text('Next')",
    ],
    post_passenger_transition_timeout_ms=8000,
    passenger_continue_retries=1,
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Continue')",
        "button:has-text('Skip')",
    ],
    seats_skip_selectors=[
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without seats')",
        "button:has-text('No thanks')",
        "button:has-text('Skip')",
    ],
    details_extractor_handler="_extract_aircairo_checkout_details",
))

# ─── Asian airlines ─────────────────────────────────────────────────────

_register(_base_cfg("AirAsia", "airasia_direct",
    use_chrome_channel=True,
    use_proxy=True,
    service_workers="block",
    locale_pool=["en-GB", "en-US", "en-MY", "en-SG"],
    timezone_pool=[
        "Asia/Kuala_Lumpur",
        "Asia/Singapore",
        "Asia/Bangkok",
        "Asia/Jakarta",
        "Asia/Manila",
    ],
    cookie_selectors=[
        "#airasia-phoenix-modal-close-button",
        "button[aria-label='Close modal button']",
        "button:has-text('Accept all cookies')",
        "button:has-text('Accept All Cookies')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
    ],
    flight_cards_selector="[class*='JourneyPriceCTA'], [class*='flight'], [class*='result'], [data-testid*='flight']",
    first_flight_selectors=[
        "[class*='JourneyPriceCTA'] a:has-text('Select')",
        "[class*='JourneyPriceCTA'] [class*='Button__ButtonContainer']:has-text('Select')",
        "main [class*='JourneyPriceCTA'] a",
    ],
    fare_selectors=[],
    passenger_form_selector="input[placeholder*='First/Given name' i], input[placeholder*='Family name/Surname' i], input[placeholder='DD/MM/YYYY'], input[placeholder='Email']",
    title_mode="select",
    title_select_selector="select, [class*='SelectItemsMobile__Select']",
    login_skip_selectors=[
        "#guestButton",
        "a:has-text('Continue as guest')",
        "text=Continue as guest",
        "button:has-text('Continue as guest')",
    ],
    dob_enabled=True,
    gender_enabled=True,
    gender_selectors_male=[
        "text=Male",
        "label:has-text('Male')",
    ],
    gender_selectors_female=[
        "text=Female",
        "label:has-text('Female')",
    ],
    dob_single_input_selectors=[
        "xpath=(//input[@placeholder='DD/MM/YYYY'])[1]",
    ],
    document_number_selectors=[
        "input[placeholder*='Passport/ID number' i]",
    ],
    document_expiry_selectors=[
        "xpath=(//input[@placeholder='DD/MM/YYYY'])[2]",
    ],
    contact_section_expand_selectors=[
        "text=Contact details",
    ],
    contact_first_name_selectors=[
        "xpath=(//input[@placeholder='First/Given name'])[2]",
        "xpath=(//input[contains(@placeholder, 'First/Given name')])[2]",
    ],
    contact_last_name_selectors=[
        "xpath=(//input[@placeholder='Family name/Surname'])[2]",
        "xpath=(//input[contains(@placeholder, 'Family name/Surname')])[2]",
    ],
    first_name_selectors=[
        "input[placeholder*='First/Given name' i]",
        "input[placeholder*='First name' i]",
        "input[name*='name'][name*='first']",
        "input[data-ref*='first-name']",
        "input[data-test*='first-name']",
        "input[data-test='passenger-first-name-0']",
        "input[name*='firstName']",
        "input[data-testid*='first-name']",
    ],
    last_name_selectors=[
        "input[placeholder*='Family name/Surname' i]",
        "input[placeholder*='Last name' i]",
        "input[name*='name'][name*='last']",
        "input[data-ref*='last-name']",
        "input[data-test*='last-name']",
        "input[data-test='passenger-last-name-0']",
        "input[name*='lastName']",
        "input[data-testid*='last-name']",
    ],
    email_selectors=[
        "input[placeholder='Email']",
        "#emailId",
        "input[data-test*='email']",
        "input[data-test*='contact-email']",
        "input[name*='email']",
        "input[data-testid*='email']",
        "input[type='email']",
    ],
    phone_selectors=[
        "input[placeholder='512 345 678']",
        "input[data-test*='phone']",
        "input[name*='phone']",
        "input[data-testid*='phone']",
        "input[type='tel']",
    ],
    phone_digits_only=True,
    phone_grouping=[3, 3, 3],
    phone_type_delay_ms=80,
    phone_local_digits_count=9,
    pre_passenger_continue_settle_ms=1000,
    post_passenger_transition_timeout_ms=9000,
    passenger_continue_retries=1,
    passenger_continue_selectors=[
        "a:has-text('Continue to Add-Ons')",
        "button:has-text('Continue to Add-Ons')",
        "text=Continue",
        "button:has-text('Continue')",
        "button[data-test='passengers-continue-btn']",
        "[data-test*='continue'] button",
        "[data-testid*='continue'] button",
        "[class*='passenger'] button:has-text('Continue')",
        "[class*='pax'] button:has-text('Continue')",
        "form button[type='submit']",
        "button:has-text('Continue to')",
        "button:has-text('Next step')",
    ],
    price_selectors=[
        "[class*='Panel__BottomHeaderWrapper']",
        "[class*='Panel__MainWrapper'] [class*='Panel__BottomHeaderWrapper']",
        "[class*='Panel__MainWrapper']",
        "[class*='total'] [class*='price']",
        "[data-test*='total-price']",
        "[data-ref*='total']",
        "[class*='total-price']",
        "[data-testid*='total']",
        "[class*='summary'] [class*='amount']",
        "[class*='summary-price']",
        "[class*='summary'] [class*='price']",
    ],
    details_extractor_handler="_extract_airasia_checkout_details",
))
AIRLINE_CONFIGS["airasiax_direct"] = AIRLINE_CONFIGS["airasia_direct"]

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
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9444,
    cdp_user_data_dir=".jetstar_chrome_data",
    custom_checkout_handler="_jetstar_checkout",
    homepage_url="https://booking.jetstar.com/au/en/booking",
    homepage_wait_ms=3000,
    flight_cards_selector="div.price-select[role='button'], [class*='price-select'], [class*='flight-card'], [class*='result']",
    first_flight_selectors=[
        "div.price-select[role='button']",
        "[class*='price-select'][role='button']",
        "[class*='price-select']",
    ],
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Starter')",
    ],
    seats_skip_selectors=[
        "button:has-text('Skip seats for this flight')",
        "button:has-text('Continue to extras')",
        "button:has-text('I don\'t mind where I sit')",
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

_register(_base_cfg("PLAY", "play_direct",
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Play Light')",
        "button:has-text('Play')",
        "button:has-text('Select')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
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

# SpiceJet uses the canonical engine source tag for search and checkout.
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
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='journey'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Classic')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Skip')",
        "button:has-text('Not now')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Continue')",
        "button:has-text('Skip')",
    ],
    seats_skip_selectors=[
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without seats')",
        "button:has-text('No thanks')",
        "button:has-text('Skip')",
    ],
    details_extractor_handler="_extract_generic_visible_checkout_details",
))

_register(_base_cfg("Air Europa", "aireuropa_direct",
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9498,
    cdp_user_data_dir=".aireuropa_chrome_data",
    homepage_url="https://www.aireuropa.com/en/flights",
    homepage_wait_ms=4000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='journey'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Lite')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Skip')",
        "button:has-text('Not now')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Continue')",
        "button:has-text('Skip')",
    ],
    seats_skip_selectors=[
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without seats')",
        "button:has-text('No thanks')",
        "button:has-text('Skip')",
    ],
    details_extractor_handler="_extract_generic_visible_checkout_details",
))

# ─── Batch 7: BA, KLM, Air France, Iberia, Iberia Express, Virgin Atlantic ──

_register(_base_cfg("British Airways", "britishairways_direct",
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9460,
    homepage_url="https://www.britishairways.com",
    homepage_wait_ms=4000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='journey'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Economy')",
        "button:has-text('Euro Traveller')",
        "button:has-text('World Traveller')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept All Cookies')",
        "button:has-text('Accept all cookies')",
        "button:has-text('Accept')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Not now')",
        "button:has-text('Skip')",
        "button:has-text('Log in later')",
        "a:has-text('Continue as guest')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
        "button:has-text('Continue to payment')",
        "button:has-text('No, thanks')",
    ],
    seats_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
    ],
))

_register(_base_cfg("KLM", "klm_direct",
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9461,
    homepage_url="https://www.klm.nl",
    homepage_wait_ms=4000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound'], [class*='journey']",
    fare_selectors=[
        "button:has-text('Economy Light')",
        "button:has-text('Economy Standard')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept all cookies')",
        "button:has-text('Accept')",
        "[class*='cookie'] button:has-text('Accept')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Not now')",
        "button:has-text('Skip')",
        "button:has-text('Log in later')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
        "button:has-text('Continue to payment')",
    ],
    seats_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
    ],
))

_register(_base_cfg("Air France", "airfrance_direct",
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9462,
    homepage_url="https://wwws.airfrance.nl",
    homepage_wait_ms=4000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound'], [class*='journey']",
    fare_selectors=[
        "button:has-text('Economy Light')",
        "button:has-text('Economy Standard')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept all cookies')",
        "button:has-text('Accept')",
        "[class*='cookie'] button:has-text('Accept')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Not now')",
        "button:has-text('Skip')",
        "button:has-text('Log in later')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
        "button:has-text('Continue to payment')",
    ],
    seats_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
    ],
))

_register(_base_cfg("Iberia", "iberia_direct",
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9463,
    homepage_url="https://www.iberia.com",
    homepage_wait_ms=4000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound'], [class*='journey']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Aceptar todas las cookies')",
        "button:has-text('Accept All Cookies')",
        "button:has-text('Accept')",
        "[class*='cookie'] button:has-text('Accept')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Continuar sin registrarse')",
        "button:has-text('Not now')",
        "button:has-text('Skip')",
        "button:has-text('Log in later')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('No, gracias')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
        "button:has-text('Continue to payment')",
    ],
    seats_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('No, gracias')",
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
    ],
))

_register(_base_cfg("Iberia Express", "iberiaexpress_direct",
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9464,
    homepage_url="https://www.iberiaexpress.com",
    homepage_wait_ms=4000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound'], [class*='journey']",
    fare_selectors=[
        "button:has-text('Basic')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Aceptar todas las cookies')",
        "button:has-text('Accept All Cookies')",
        "button:has-text('Accept')",
        "[class*='cookie'] button:has-text('Accept')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Continuar sin registrarse')",
        "button:has-text('Not now')",
        "button:has-text('Skip')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('No, gracias')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
        "button:has-text('Continue to payment')",
    ],
    seats_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('No, gracias')",
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
    ],
))

_register(_base_cfg("Virgin Atlantic", "virginatlantic_direct",
    goto_timeout=60000,
    use_cdp_chrome=True,
    cdp_port=9465,
    homepage_url="https://www.virginatlantic.com",
    homepage_wait_ms=4000,
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='bound'], [class*='journey']",
    fare_selectors=[
        "button:has-text('Economy Light')",
        "button:has-text('Economy Classic')",
        "button:has-text('Economy')",
        "button:has-text('Select')",
        "button:has-text('Choose')",
    ],
    cookie_selectors=[
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept All Cookies')",
        "button:has-text('Accept all cookies')",
        "button:has-text('Accept')",
    ],
    login_skip_selectors=[
        "button:has-text('Continue as guest')",
        "button:has-text('Not now')",
        "button:has-text('Skip')",
        "button:has-text('Log in later')",
    ],
    extras_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
        "button:has-text('Continue to payment')",
    ],
    seats_skip_selectors=[
        "button:has-text('No thanks')",
        "button:has-text('Skip seat selection')",
        "button:has-text('Continue without')",
        "button:has-text('Skip')",
    ],
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

    async def _choose_dropdown_option(
        self,
        page,
        open_selectors: list[str],
        option_text: str = "",
        *,
        option_selectors: list[str] | None = None,
        desc: str = "dropdown",
    ) -> bool:
        if not open_selectors:
            return False
        if not await safe_click_first(page, open_selectors, timeout=2000, desc=desc):
            return False
        await page.wait_for_timeout(400)

        selectors = list(option_selectors or [])
        if option_text:
            selectors.extend([
                f"mat-option:has-text('{option_text}')",
                f"[role='option']:has-text('{option_text}')",
                f"button:has-text('{option_text}')",
                f"label:has-text('{option_text}')",
            ])
        if selectors and await safe_click_first(page, selectors, timeout=2000, desc=f"{desc} option"):
            await page.wait_for_timeout(300)
            return True
        return False

    async def _fill_autocomplete_field(
        self,
        page,
        selectors: list[str],
        value: str,
        dropdown_selector: str = "",
        *,
        desc: str = "autocomplete",
    ) -> bool:
        if not selectors or not value:
            return False
        if not await safe_type_first(page, selectors, value, timeout=2000, delay_ms=40):
            return False
        await page.wait_for_timeout(500)
        if dropdown_selector and await safe_click(page, dropdown_selector, timeout=2000, desc=desc):
            await page.wait_for_timeout(300)
            return True
        try:
            await page.keyboard.press("ArrowDown")
            await page.wait_for_timeout(200)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(300)
        except Exception:
            pass
        return True

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
        captured_details: dict = {}

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

        pw = None
        _chrome_proc = None  # CDP Chrome subprocess (if any)

        if config.use_cdp_chrome:
            # CDP mode: launch real Chrome as subprocess, connect via CDP.
            # This bypasses Kasada KPSDK — Playwright automation hooks are NOT
            # injected into the Chrome binary, so KPSDK JS runs naturally.
            from .browser import launch_cdp_chrome
            pw = await async_playwright().start()
            _udd_name = config.cdp_user_data_dir or f".{config.source_tag}_chrome_data"
            _user_data_dir = os.path.join(
                os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")),
                _udd_name,
            )
            os.makedirs(_user_data_dir, exist_ok=True)
            _fallback_user_data_dir = f"{_user_data_dir}_fresh"
            vp = random.choice([(1366, 768), (1440, 900), (1920, 1080)])

            async def _connect_cdp(user_data_dir: str, *, label: str):
                nonlocal _chrome_proc
                # Try to reuse an already-running Chrome on this port first.
                # This is critical for connectors like Air Cairo where the
                # search connector has already launched Chrome with valid
                # session cookies — a fresh Chrome would be anti-bot blocked.
                try:
                    browser_existing = await pw.chromium.connect_over_cdp(
                        f"http://127.0.0.1:{config.cdp_port}",
                        timeout=2000,
                    )
                    logger.info(
                        "%s checkout: attached to existing Chrome on port %d",
                        config.airline_name,
                        config.cdp_port,
                    )
                    return browser_existing
                except Exception:
                    pass
                logger.info(
                    "%s checkout: launching CDP Chrome on port %d using %s",
                    config.airline_name,
                    config.cdp_port,
                    label,
                )
                _chrome_proc = await launch_cdp_chrome(
                    config.cdp_port,
                    user_data_dir,
                    use_proxy=config.use_proxy,
                    extra_args=[f"--window-size={vp[0]},{vp[1]}"],
                    startup_wait=3.0,
                )
                return await pw.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{config.cdp_port}"
                )

            def _stop_cdp_proc() -> None:
                nonlocal _chrome_proc
                if not _chrome_proc:
                    return
                try:
                    _chrome_proc.terminate()
                    _chrome_proc.wait(timeout=5)
                except Exception:
                    try:
                        _chrome_proc.kill()
                    except Exception:
                        pass
                _chrome_proc = None

            try:
                browser = await _connect_cdp(_user_data_dir, label="persistent profile")
            except Exception as cdp_err:
                logger.warning(
                    "%s checkout: CDP connect failed with persistent profile: %s",
                    config.airline_name,
                    cdp_err,
                )
                _stop_cdp_proc()
                try:
                    if os.path.isdir(_fallback_user_data_dir):
                        shutil.rmtree(_fallback_user_data_dir, ignore_errors=True)
                    os.makedirs(_fallback_user_data_dir, exist_ok=True)
                    browser = await _connect_cdp(_fallback_user_data_dir, label="fresh fallback profile")
                except Exception as fallback_err:
                    logger.warning(
                        "%s checkout: CDP connect failed with fresh fallback profile: %s",
                        config.airline_name,
                        fallback_err,
                    )
                    _stop_cdp_proc()
                    await pw.stop()
                    return CheckoutProgress(
                        status="failed", airline=config.airline_name, source=config.source_tag,
                        offer_id=offer_id, booking_url=booking_url,
                        message=(
                            "CDP Chrome launch failed: "
                            f"persistent profile error: {cdp_err}; "
                            f"fresh-profile fallback error: {fallback_err}"
                        ),
                        elapsed_seconds=time.monotonic() - t0,
                    )
        else:
            pw = await async_playwright().start()
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1440,900",
            ]

            # Residential proxy support for anti-bot bypass
            launch_kwargs: dict = {"headless": headless, "args": launch_args}
            if config.use_chrome_channel:
                launch_kwargs["channel"] = "chrome"
            if config.use_proxy:
                from letsfg.connectors.browser import get_default_proxy, patchright_bandwidth_args
                proxy_dict = get_default_proxy()
                if proxy_dict:
                    launch_kwargs["proxy"] = proxy_dict
                    launch_args.extend(patchright_bandwidth_args())
                    logger.info("%s checkout: using proxy %s", config.airline_name, proxy_dict.get("server", ""))

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
                if config.source_tag == "aireuropa_direct":
                    try:
                        from .browser import inject_stealth_js

                        await inject_stealth_js(page)
                    except Exception:
                        pass
            else:
                try:
                    from playwright_stealth import stealth_async
                    page = await context.new_page()
                    await stealth_async(page)
                except ImportError:
                    page = await context.new_page()

            # Auto-block heavy resources when using proxy (saves bandwidth)
            from .browser import auto_block_if_proxied

            # CDP cache disable
            if config.disable_cache:
                try:
                    cdp = await context.new_cdp_session(page)
                    await cdp.send("Network.setCacheDisabled", {"cacheDisabled": True})
                except Exception:
                    pass

            pax = passengers[0] if passengers else FAKE_PASSENGER

            # ── Homepage pre-load (Kasada, etc.) ─────────────────────
            if config.homepage_url:
                logger.info("%s checkout: loading homepage %s", config.airline_name, config.homepage_url)
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
            step = "started"  # guard: ensures step is always defined if an exception escapes a custom handler

            # Check for custom checkout handler (e.g. WizzAir needs Vue SPA injection)
            custom_page_prepared = False
            if config.custom_checkout_handler:
                handler = getattr(self, config.custom_checkout_handler, None)
                if handler:
                    result = await handler(page, config, offer, offer_id, booking_url, passengers, t0)
                    if isinstance(result, CheckoutProgress):
                        return result
                    if result == "prepared":
                        custom_page_prepared = True
                    # If handler returned None, fall through to generic flow
                else:
                    logger.warning("%s checkout: custom handler '%s' not found, using generic flow",
                                   config.airline_name, config.custom_checkout_handler)

            if custom_page_prepared:
                logger.info("%s checkout: custom handler prepared the checkout surface", config.airline_name)
            else:
                logger.info("%s checkout: navigating to %s", config.airline_name, booking_url)
                try:
                    await page.goto(booking_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
                except Exception as nav_err:
                    # Some SPAs return HTTP errors but still render via JS — continue if page loaded
                    logger.warning("%s checkout: goto error (%s) — continuing", config.airline_name, str(nav_err)[:100])
                await page.wait_for_timeout(2000 if not config.homepage_url else 3000)
            await self._dismiss_cookies(page, config)

            # Guard against SPA redirects (e.g. Ryanair → check-in page)
            if not custom_page_prepared and booking_url.split("?")[0] not in page.url:
                logger.warning("%s checkout: page redirected to %s — retrying", config.airline_name, page.url[:120])
                try:
                    await page.goto(booking_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
                except Exception:
                    pass
                await page.wait_for_timeout(3000)
                await self._dismiss_cookies(page, config)

            if config.source_tag == "aireuropa_direct":
                await self._prepare_aireuropa_checkout_results(page, offer)
            elif config.source_tag == "airasia_direct":
                await self._prepare_airasia_results(page, booking_url)
            elif config.source_tag == "volaris_direct":
                await self._prepare_volaris_checkout_results(page, offer)

            step = "page_loaded"

            security_gate = await self._detect_security_gate(page)
            if security_gate:
                screenshot = await take_screenshot_b64(page)
                elapsed = time.monotonic() - t0
                blocked_details = self._merge_checkout_details(
                    captured_details,
                    {
                        "blocker": "anti_bot_verification",
                        "checkout_page": "blocked",
                        "current_url": security_gate.get("current_url") or booking_url,
                        "page_title": security_gate.get("page_title") or "",
                        "security_gate_observation": "Automation was blocked by a CAPTCHA or browser verification gate on the reachable airline surface.",
                    },
                )
                return CheckoutProgress(
                    status="in_progress",
                    step=step,
                    step_index=CHECKOUT_STEPS.index(step) if step in CHECKOUT_STEPS else 0,
                    airline=config.airline_name,
                    source=config.source_tag,
                    offer_id=offer_id,
                    total_price=offer.get("price", 0.0),
                    currency=offer.get("currency", "EUR"),
                    booking_url=security_gate.get("current_url") or booking_url,
                    screenshot_b64=screenshot,
                    message=(
                        f"{config.airline_name} checkout is blocked by CAPTCHA or anti-bot verification. "
                        "Manual verification is required before automation can continue."
                    ),
                    can_complete_manually=bool(security_gate.get("current_url") or booking_url),
                    elapsed_seconds=elapsed,
                    details=blocked_details,
                )

            # ── Step 2: Select flights ───────────────────────────────
            try:
                await page.wait_for_selector(config.flight_cards_selector, timeout=config.flight_cards_timeout)
            except Exception:
                if config.source_tag == "airasia_direct":
                    await self._prepare_airasia_results(page, booking_url)
                    try:
                        await page.wait_for_selector(config.flight_cards_selector, timeout=5000)
                    except Exception:
                        pass
                elif config.source_tag == "volaris_direct":
                    await self._prepare_volaris_checkout_results(page, offer)
                    try:
                        await page.wait_for_selector(config.flight_cards_selector, timeout=5000)
                    except Exception:
                        pass
            try:
                await page.wait_for_selector(config.flight_cards_selector, timeout=1000)
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
                dep_time = _extract_hhmm(dep)
                if dep_time:
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
                fare_clicked = False
                if config.source_tag == "aireuropa_direct":
                    fare_clicked = await self._click_aireuropa_inline_fare(page)
                elif config.source_tag == "aircairo_direct":
                    fare_clicked = await self._click_aircairo_fare(page)
                if not fare_clicked:
                    fare_clicked = await safe_click_first(page, config.fare_selectors, timeout=3000, desc="select fare")
                if fare_clicked:
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
                await self._choose_dropdown_option(page, config.title_dropdown_selectors, title_text, desc="title dropdown")
            elif config.title_mode == "select":
                try:
                    await page.select_option(config.title_select_selector, label=title_text, timeout=2000)
                except Exception:
                    await self._choose_dropdown_option(page, [config.title_select_selector], title_text, desc=f"title {title_text}")

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
                    if config.dob_single_input_selectors:
                        await safe_fill_first(page, config.dob_single_input_selectors, f"{day}/{month}/{year}")
                    else:
                        if config.dob_strip_leading_zero:
                            day = day.lstrip("0") or day
                            month = month.lstrip("0") or month
                        await safe_fill_first(page, config.dob_day_selectors, day)
                        await safe_fill_first(page, config.dob_month_selectors, month)
                        await safe_fill_first(page, config.dob_year_selectors, year)

            # Nationality (if required)
            if config.nationality_enabled:
                await self._fill_autocomplete_field(
                    page,
                    config.nationality_selectors,
                    config.nationality_fill_value or "GB",
                    config.nationality_dropdown_item,
                    desc="nationality",
                )

            # Travel document fields (if required)
            if config.document_number_selectors:
                document_number = (
                    pax.get("document_number")
                    or pax.get("passport_number")
                    or "X1234567"
                )
                await safe_fill_first(page, config.document_number_selectors, document_number)

            if config.document_expiry_selectors:
                document_expiry = (
                    pax.get("document_expiry")
                    or pax.get("passport_expiry")
                    or "2030-06-15"
                )
                document_expiry = self._format_checkout_date(document_expiry, default="15/06/2030")
                await safe_fill_first(page, config.document_expiry_selectors, document_expiry)

            if config.issuance_country_selectors:
                await self._fill_autocomplete_field(
                    page,
                    config.issuance_country_selectors,
                    config.issuance_country_fill_value or config.nationality_fill_value or "GB",
                    config.issuance_country_dropdown_item,
                    desc="issuance country",
                )

            if config.contact_section_expand_selectors:
                await safe_click_first(page, config.contact_section_expand_selectors, timeout=2000, desc="expand contact details")
                await page.wait_for_timeout(500)

            if config.contact_first_name_selectors:
                await safe_fill_first(page, config.contact_first_name_selectors, pax.get("given_name", "Test"))

            if config.contact_last_name_selectors:
                await safe_fill_first(page, config.contact_last_name_selectors, pax.get("family_name", "Traveler"))

            # Email
            await safe_fill_first(page, config.email_selectors, pax.get("email", "test@example.com"))

            if config.confirm_email_selectors:
                await safe_fill_first(page, config.confirm_email_selectors, pax.get("email", "test@example.com"))

            if config.phone_type_selectors:
                await self._choose_dropdown_option(
                    page,
                    config.phone_type_selectors,
                    option_selectors=config.phone_type_option_selectors,
                    desc="phone type",
                )

            if config.phone_country_code_selectors:
                await self._fill_autocomplete_field(
                    page,
                    config.phone_country_code_selectors,
                    config.phone_country_code_value,
                    config.phone_country_code_dropdown_item,
                    desc="phone country code",
                )

            # Phone
            phone_value = pax.get("phone_number", "+441234567890")
            if config.phone_digits_only:
                digits_only = re.sub(r"\D", "", phone_value)
                if digits_only:
                    phone_value = digits_only
            if config.phone_local_digits_count > 0 and len(phone_value) > config.phone_local_digits_count:
                phone_value = phone_value[-config.phone_local_digits_count:]
            if config.phone_grouping and phone_value:
                grouped_parts: list[str] = []
                offset = 0
                for group_size in config.phone_grouping:
                    part = phone_value[offset:offset + group_size]
                    if not part:
                        break
                    grouped_parts.append(part)
                    offset += group_size
                remainder = phone_value[offset:]
                if remainder:
                    grouped_parts.append(remainder)
                phone_value = " ".join(grouped_parts)
            if config.phone_type_delay_ms > 0:
                await safe_type_first(
                    page,
                    config.phone_selectors,
                    phone_value,
                    delay_ms=config.phone_type_delay_ms,
                    blur=True,
                )
            else:
                await safe_fill_first(page, config.phone_selectors, phone_value)

            if config.consent_checkbox_selectors:
                await safe_click_first(page, config.consent_checkbox_selectors, timeout=2000, desc="accept consent")
                await page.wait_for_timeout(300)

            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_checkout_details(page, config, offer.get("currency", "EUR")),
            )

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
            if config.pre_passenger_continue_settle_ms > 0:
                await page.wait_for_timeout(config.pre_passenger_continue_settle_ms)
            for attempt in range(max(config.passenger_continue_retries, 0) + 1):
                await safe_click_first(page, config.passenger_continue_selectors, timeout=2000, desc="continue after passengers")
                transition_wait_remaining = max(config.post_passenger_transition_timeout_ms, 0)
                transition_snapshot = None
                transition_page = ""
                transition_url = ""
                while transition_wait_remaining > 0:
                    wait_chunk = min(1000, transition_wait_remaining)
                    await page.wait_for_timeout(wait_chunk)
                    transition_wait_remaining -= wait_chunk
                    transition_snapshot = await self._snapshot_checkout_page(page)
                    transition_page = self._infer_checkout_page(captured_details, transition_snapshot)
                    transition_url = (transition_snapshot.get("current_url") or "").lower()
                    if "cashier.airasia.com/payment" in transition_url or transition_page not in {"passengers", "guest_details"}:
                        break
                if "cashier.airasia.com/payment" in transition_url or transition_page not in {"passengers", "guest_details"}:
                    break
                if attempt < config.passenger_continue_retries:
                    await page.wait_for_timeout(800)
            await self._dismiss_cookies(page, config)

            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_checkout_details(page, config, offer.get("currency", "EUR")),
            )

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

            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_checkout_details(page, config, offer.get("currency", "EUR")),
            )

            step = "extras_skipped"

            # ── Step 7: Skip seats ───────────────────────────────────
            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_checkout_details(page, config, offer.get("currency", "EUR")),
            )
            await safe_click_first(page, config.seats_skip_selectors, timeout=2000, desc="skip seats")
            await page.wait_for_timeout(1000)
            await safe_click_first(page, config.seats_confirm_selectors, timeout=1500, desc="confirm skip seats")

            step = "seats_skipped"
            await page.wait_for_timeout(1000)
            await self._dismiss_cookies(page, config)

            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_checkout_details(page, config, offer.get("currency", "EUR")),
            )

            # ── Step 8: Verify final page state before claiming success ──────
            screenshot = await take_screenshot_b64(page)
            final_snapshot = await self._snapshot_checkout_page(page)
            final_checkout_page = self._infer_checkout_page(captured_details, final_snapshot)
            if final_checkout_page:
                captured_details = self._merge_checkout_details(
                    captured_details,
                    {"checkout_page": final_checkout_page},
                )

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

            display_total = captured_details.get("display_total")
            if isinstance(display_total, dict) and isinstance(display_total.get("amount"), (int, float)):
                page_price = float(display_total["amount"])

            elapsed = time.monotonic() - t0
            if final_checkout_page != "payment":
                blocker_code = "payment_page_not_reached"
                status_message = (
                    f"{config.airline_name} checkout did not reach payment. "
                    f"Current surface looks like '{(final_checkout_page or 'unknown').replace('_', ' ')}'. "
                    f"Visible price: {page_price} {offer.get('currency', 'EUR')}."
                )
                if final_checkout_page == "blocked":
                    blocker_code = "anti_bot_verification"
                    status_message = (
                        f"{config.airline_name} checkout is blocked by CAPTCHA or anti-bot verification. "
                        "Manual verification is required before automation can continue."
                    )
                step = self._checkout_step_for_page(final_checkout_page)
                blocker_details = self._merge_checkout_details(
                    captured_details,
                    {
                        "blocker": blocker_code,
                        "checkout_page": final_checkout_page or "unknown",
                        "current_url": final_snapshot.get("current_url") or booking_url,
                        "page_title": final_snapshot.get("page_title") or "",
                    },
                )
                return CheckoutProgress(
                    status="in_progress",
                    step=step,
                    step_index=CHECKOUT_STEPS.index(step) if step in CHECKOUT_STEPS else 0,
                    airline=config.airline_name,
                    source=config.source_tag,
                    offer_id=offer_id,
                    total_price=page_price,
                    currency=offer.get("currency", "EUR"),
                    booking_url=final_snapshot.get("current_url") or booking_url,
                    screenshot_b64=screenshot,
                    message=status_message,
                    can_complete_manually=bool(booking_url),
                    elapsed_seconds=elapsed,
                    details=blocker_details,
                )

            step = "payment_page_reached"
            current_url = final_snapshot.get("current_url") or booking_url
            return CheckoutProgress(
                status="payment_page_reached",
                step=step,
                step_index=8,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                total_price=page_price,
                currency=offer.get("currency", "EUR"),
                booking_url=current_url,
                screenshot_b64=screenshot,
                message=(
                    f"{config.airline_name} checkout complete — reached payment page in {elapsed:.0f}s. "
                    f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                    f"Payment NOT submitted (safe mode). "
                    f"Complete manually at: {current_url}"
                ),
                can_complete_manually=bool(current_url),
                elapsed_seconds=elapsed,
                details=captured_details,
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
                details=captured_details,
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

    async def _detect_security_gate(self, page) -> dict | None:
        snapshot = await self._snapshot_checkout_page(page)
        title = str(snapshot.get("page_title") or "").strip().lower()
        body = str(snapshot.get("body_snippet") or "").strip().lower()
        combined = f"{title} {body}"
        tokens = (
            "pardon our interruption",
            "made us think you were a bot",
            "captcha below",
            "i am human",
            "hcaptcha",
            "incident id",
            "performing security verification",
            "verifies you are not a bot",
            "checking your browser",
            "just a moment",
            "please wait while we verify",
        )
        if any(token in combined for token in tokens):
            return snapshot
        try:
            challenge_visible = await page.evaluate(
                """() => Boolean(
                    document.querySelector("iframe[src*='hcaptcha'], iframe[title*='hCaptcha'], .h-captcha, [data-hcaptcha-response], #challenge-running")
                )"""
            )
            if challenge_visible:
                return snapshot
        except Exception:
            pass
        return None

    @staticmethod
    def _format_checkout_date(value: str, *, default: str) -> str:
        text = (value or default).strip()
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", text):
            return text
        match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
        if match:
            year, month, day = match.groups()
            return f"{day}/{month}/{year}"
        return default

    async def _snapshot_checkout_page(self, page) -> dict:
        title = ""
        body = ""
        try:
            title = await page.title()
        except Exception:
            pass
        try:
            body = await page.evaluate(
                """() => {
                    const root = document.body;
                    if (!root) return '';
                    return (root.innerText || root.textContent || '').slice(0, 2400);
                }"""
            )
        except Exception:
            pass
        return {
            "current_url": page.url,
            "page_title": title,
            "body_snippet": " ".join(str(body).split())[:1200],
        }

    @staticmethod
    def _infer_checkout_page(details: dict, snapshot: dict) -> str:
        details = details or {}
        snapshot = snapshot or {}
        checkout_page = str(details.get("checkout_page") or "").strip().lower()
        if checkout_page == "guest_details":
            checkout_page = "passengers"
        current_url = str(snapshot.get("current_url") or "").strip().lower()
        title = str(snapshot.get("page_title") or "").strip().lower()
        body = str(snapshot.get("body_snippet") or "").strip().lower()
        combined = " ".join(part for part in (title, body) if part)
        security_terms = (
            "pardon our interruption",
            "made us think you were a bot",
            "captcha below",
            "i am human",
            "hcaptcha",
            "incident id",
            "performing security verification",
            "verifies you are not a bot",
            "checking your browser",
            "just a moment",
            "please wait while we verify",
        )
        if any(term in combined for term in security_terms):
            return "blocked"
        passenger_url_terms = ("/traveler", "/traveller", "/passenger", "guest-details")
        passenger_form_terms = (
            "enter your information",
            "personal information",
            "passport details",
            "date of birth",
            "document number",
            "confirm email",
            "remember passenger information",
        )
        if any(term in current_url for term in passenger_url_terms) or any(term in combined for term in passenger_form_terms):
            return "passengers"
        checkout_url_terms = ("/checkout", "guest-details", "add-ons", "addons", "seat", "payment")
        on_checkout_surface = any(term in current_url for term in checkout_url_terms)

        payment_url_terms = ("/payment", "payment", "/review", "review-and-pay", "review-and-book")
        payment_text_terms = (
            "review & pay",
            "review and pay",
            "secure payment",
            "payment details",
            "billing address",
            "card number",
            "cvv",
            "pay now",
            "complete booking",
        )
        home_text_terms = (
            "search flights",
            "flight deals",
            "manage your reservation",
            "before flying",
            "promotional code",
        )
        has_payment_signal = any(term in current_url for term in payment_url_terms) or any(term in combined for term in payment_text_terms)
        if checkout_page == "payment":
            if has_payment_signal or not any(term in combined for term in home_text_terms):
                return "payment"
        if any(term in current_url for term in payment_url_terms) or any(term in combined for term in payment_text_terms):
            return "payment"

        passenger_terms = ("passenger details", "traveller details", "traveler details", "guest details", "contact details")
        seat_terms = ("seat map", "select seat", "choose your seat", "hot seat")
        extras_terms = ("baggage", "checked bag", "bags", "add-ons", "add ons", "extras")

        if on_checkout_surface and checkout_page:
            return checkout_page

        if on_checkout_surface and any(term in combined for term in passenger_terms):
            return "passengers"

        if on_checkout_surface and any(term in combined for term in seat_terms):
            return "seats"

        if on_checkout_surface and any(term in combined for term in extras_terms):
            return "extras"

        search_url_terms = ("/search", "fullsearch", "flight-search", "select-flight", "results", "/availability", "booking/availability")
        search_text_terms = (
            "select cheap flights",
            "select flight",
            "choose flight",
            "flight selection",
            "search results",
            "departure flights",
            "departing flights",
            "returning flights",
            "flight results",
            "search flights",
            "trip type",
            "promotional code",
        )
        if any(term in current_url for term in search_url_terms) or any(term in combined for term in search_text_terms):
            return "select_flight"

        if checkout_page:
            return checkout_page

        if any(term in combined for term in passenger_terms):
            return "passengers"

        if any(term in combined for term in seat_terms):
            return "seats"

        if any(term in combined for term in extras_terms):
            return "extras"

        return ""

    @staticmethod
    def _checkout_step_for_page(checkout_page: str) -> str:
        mapping = {
            "payment": "payment_page_reached",
            "seats": "extras_skipped",
            "extras": "passengers_filled",
            "passengers": "login_bypassed",
            "guest_details": "login_bypassed",
            "blocked": "page_loaded",
            "select_flight": "page_loaded",
        }
        return mapping.get((checkout_page or "").strip().lower(), "started")

    @staticmethod
    def _dedupe_checkout_detail_items(items: list[dict], limit: int = 12) -> list[dict]:
        seen = set()
        deduped: list[dict] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or item.get("text") or "").strip().lower()
            amount = item.get("amount")
            try:
                amount = round(float(amount), 2) if amount is not None else None
            except Exception:
                amount = None
            key = (
                label,
                str(item.get("type") or "").strip().lower(),
                str(item.get("currency") or "").strip().upper(),
                amount,
                bool(item.get("included")),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
            if len(deduped) >= limit:
                break
        return deduped

    def _merge_checkout_details(self, existing: dict, extracted: dict) -> dict:
        existing = existing or {}
        extracted = extracted or {}
        merged = dict(existing)

        structured_keys = {"available_add_ons", "price_breakdown", "visible_price_options"}
        for key, value in extracted.items():
            if key in structured_keys or value in (None, "", [], {}):
                continue
            merged[key] = value

        for key in ("price_breakdown", "visible_price_options"):
            combined: list[dict] = []
            if isinstance(existing.get(key), list):
                combined.extend(existing[key])
            if isinstance(extracted.get(key), list):
                combined.extend(extracted[key])
            if combined:
                limit = 20 if key == "visible_price_options" else 12
                merged[key] = self._dedupe_checkout_detail_items(combined, limit=limit)

        existing_add_ons = existing.get("available_add_ons") if isinstance(existing.get("available_add_ons"), dict) else {}
        extracted_add_ons = extracted.get("available_add_ons") if isinstance(extracted.get("available_add_ons"), dict) else {}
        merged_add_ons: dict[str, list[dict]] = {}
        for category in sorted(set(existing_add_ons) | set(extracted_add_ons)):
            combined: list[dict] = []
            if isinstance(existing_add_ons.get(category), list):
                combined.extend(existing_add_ons[category])
            if isinstance(extracted_add_ons.get(category), list):
                combined.extend(extracted_add_ons[category])
            if combined:
                merged_add_ons[category] = self._dedupe_checkout_detail_items(combined, limit=12)
        if merged_add_ons:
            merged["available_add_ons"] = merged_add_ons

        return merged

    async def _prepare_aireuropa_checkout_results(self, page, offer: dict) -> None:
        try:
            from letsfg.connectors.aireuropa import AirEuropaConnectorClient, _dismiss_overlays
            from letsfg.models.flights import FlightSearchRequest
        except Exception as exc:
            logger.debug("Air Europa checkout: helper imports unavailable: %s", exc)
            return

        try:
            current_url = page.url.lower()
            body_text = await page.evaluate(
                "() => (document.body && document.body.innerText || '').replace(/\\s+/g, ' ').trim()"
            )
        except Exception:
            return

        if "aireuropa.com" not in current_url:
            return
        if not re.search(r"trip type|search flights|manage your reservation|welcome to air europa", body_text, re.IGNORECASE):
            return

        segments = ((offer.get("outbound") or {}).get("segments") or []) if isinstance(offer.get("outbound"), dict) else []
        segment = segments[0] if segments else {}
        origin = str(segment.get("origin") or "").strip().upper()
        destination = str(segment.get("destination") or "").strip().upper()
        departure_value = str(segment.get("departure") or "").strip()
        departure_date = departure_value.split("T", 1)[0] if departure_value else ""
        if not origin or not destination or not departure_date:
            return

        try:
            req_date = datetime.strptime(departure_date, "%Y-%m-%d").date()
        except ValueError:
            return

        req = FlightSearchRequest(
            origin=origin,
            destination=destination,
            date_from=req_date,
            adults=1,
            children=0,
            infants=0,
            cabin_class="M",
            currency=str(offer.get("currency") or "EUR"),
            limit=1,
        )

        logger.info("Air Europa checkout: redirected to homepage, replaying search widget for %s→%s", origin, destination)
        client = AirEuropaConnectorClient(timeout=60)
        try:
            await page.goto(client.HOMEPAGE, wait_until="domcontentloaded", timeout=25000)
            await asyncio.sleep(2.0)
        except Exception as exc:
            logger.debug("Air Europa checkout: clean homepage reset failed: %s", exc)
        await _dismiss_overlays(page)
        await page.evaluate("""() => {
            document.querySelectorAll('.cdk-overlay-backdrop, .cdk-overlay-dark-backdrop').forEach(el => el.remove());
        }""")
        await asyncio.sleep(0.5)

        await page.evaluate("""() => {
            const radios = document.querySelectorAll('mat-radio-button, input[type="radio"]');
            for (const r of radios) {
                const t = (r.textContent || r.parentElement?.textContent || '').trim().toLowerCase();
                if (t.includes('one way') || t.includes('one-way') || t.includes('solo ida')) {
                    r.click();
                    return;
                }
            }
            const ms = document.querySelector('common-select.way-trip mat-select, [class*="trip-type"] mat-select');
            if (ms) ms.click();
        }""")
        await asyncio.sleep(0.8)
        await page.evaluate("""() => {
            const opts = document.querySelectorAll('mat-option, [role="option"]');
            for (const o of opts) {
                const t = (o.textContent || '').trim().toLowerCase();
                if (t.includes('one way') || t.includes('one-way') || t.includes('solo ida')) {
                    o.click();
                    return;
                }
            }
        }""")
        await asyncio.sleep(1.0)

        if not await client._fill_airport(page, "input#departure", origin):
            return
        await asyncio.sleep(1.0)

        if not await client._fill_airport(page, "input#arrival", destination):
            return
        await asyncio.sleep(1.0)

        if not await client._fill_date(page, req):
            return

        await page.evaluate("""() => {
            const btn = document.querySelector("button[data-testid='btn-searcher-submit-flights'], button[data-test='btn-searcher-submit-flights'], button[data-jto='btn-searcher-submit-flights']");
            if (btn && btn.offsetHeight > 0) { btn.click(); return; }
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = (b.textContent || '').trim().toLowerCase();
                if ((t === 'search flights' || t.includes('search flights') || t === 'search' || t.includes('search')) && b.offsetHeight > 0) {
                    b.click();
                    return;
                }
            }
        }""")
        await asyncio.sleep(4.0)

        try:
            error_modal = page.locator("text=Sorry, there was an error").first
            if await error_modal.count() > 0 and await error_modal.is_visible(timeout=1500):
                logger.info("Air Europa checkout: retrying after blocking error modal")
                await safe_click_first(
                    page,
                    [
                        "button:has-text('Try again')",
                        "button[aria-label='Close']",
                        "button:has-text('Close')",
                    ],
                    timeout=2500,
                    desc="Air Europa error modal",
                )
                await asyncio.sleep(1.5)
                await page.evaluate("""() => {
                    const btn = document.querySelector("button[data-testid='btn-searcher-submit-flights'], button[data-test='btn-searcher-submit-flights'], button[data-jto='btn-searcher-submit-flights']");
                    if (btn && btn.offsetHeight > 0) { btn.click(); return; }
                    const btns = document.querySelectorAll('button');
                    for (const b of btns) {
                        const t = (b.textContent || '').trim().toLowerCase();
                        if ((t === 'search flights' || t.includes('search flights') || t === 'search' || t.includes('search')) && b.offsetHeight > 0) {
                            b.click();
                            return;
                        }
                    }
                }""")
                await asyncio.sleep(4.0)
        except Exception:
            pass

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if await page.locator("[class*='flight'], [class*='result'], [class*='journey'], [class*='bound']").count() > 0:
                    break
            except Exception:
                pass
            if any(marker in page.url.lower() for marker in ("availability", "result", "booking", "select")):
                await asyncio.sleep(2.0)
                break
            await asyncio.sleep(1.0)

        await _dismiss_overlays(page)

    async def _prepare_jetsmart_checkout_results(self, page, config: AirlineCheckoutConfig, offer: dict) -> bool:
        """Navigate to the JetSMART flight-selection page.

        Fast path: navigate directly to the offer's booking_url (the /select?... URL).
        Fallback: replay the homepage search form.
        Returns True once flight cards are visible.
        """
        segments = ((offer.get("outbound") or {}).get("segments") or []) if isinstance(offer.get("outbound"), dict) else []
        if not segments:
            logger.warning("JetSMART checkout: offer has no outbound segments")
            return False

        origin = str(segments[0].get("origin") or "").strip().upper()
        destination = str(segments[-1].get("destination") or "").strip().upper()
        departure_value = str(segments[0].get("departure") or "").strip()
        departure_date = re.split(r"[T ]", departure_value, maxsplit=1)[0] if departure_value else ""
        if not origin or not destination or not departure_date:
            logger.warning("JetSMART checkout: missing origin/destination/date in offer segments")
            return False

        try:
            target_date = datetime.strptime(departure_date, "%Y-%m-%d").date()
        except ValueError:
            logger.warning("JetSMART checkout: invalid departure date '%s'", departure_date)
            return False

        try:
            from .browser import auto_block_if_proxied
            await auto_block_if_proxied(page)
        except Exception:
            pass

        try:
            from playwright_stealth import stealth_async
            await stealth_async(page)
        except Exception:
            pass

        async def _wait_for_cards(deadline_seconds: int = 25) -> bool:
            deadline = time.monotonic() + deadline_seconds
            while time.monotonic() < deadline:
                await self._dismiss_cookies(page, config)
                try:
                    from . import jetsmart as _js_mod
                    await _js_mod.JetSmartConnectorClient(timeout=30.0)._remove_overlays(page)
                except Exception:
                    pass
                try:
                    cur = page.url.lower()
                    if "chrome-error://" in cur:
                        return False
                    if await page.locator(config.flight_cards_selector).count() > 0:
                        return True
                except Exception:
                    pass
                await page.wait_for_timeout(500)
            return False

        # ── Fast path: navigate directly to the offer's booking URL ─────
        direct_url = str(offer.get("booking_url") or "").strip()
        if direct_url:
            logger.info("JetSMART checkout: navigating directly to booking URL for %s→%s", origin, destination)
            try:
                await page.goto(direct_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
            except Exception as nav_err:
                logger.warning("JetSMART checkout: direct URL goto failed (%s)", str(nav_err)[:100])
            await page.wait_for_timeout(2000)
            if await _wait_for_cards(25):
                return True

            # GeoIP redirect may have stripped query params from the select URL.
            # Re-navigate using the GeoIP-detected market but with the original route params.
            geoip_url = page.url
            geoip_match = re.search(r"jetsmart\.com/([a-z]{2}/[a-z]{2})", geoip_url)
            if geoip_match:
                geoip_market = geoip_match.group(1)
                retry_url = (
                    f"https://jetsmart.com/{geoip_market}/select"
                    f"?origin={origin}&destination={destination}"
                    f"&departure={departure_date}&adults=1&children=0"
                )
                logger.info("JetSMART checkout: retrying with GeoIP market %s: %s", geoip_market, retry_url)
                try:
                    await page.goto(retry_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
                except Exception as nav_err2:
                    logger.warning("JetSMART checkout: GeoIP retry goto failed (%s)", str(nav_err2)[:100])
                # Wait for "Tarifa desde" to actually appear (SPA may take 30-60s to render)
                try:
                    await page.wait_for_function(
                        "() => document.body.innerText.includes('Tarifa desde') || document.body.innerText.includes('Fare from')",
                        timeout=60000,
                    )
                    await page.wait_for_timeout(1500)
                except Exception:
                    await page.wait_for_timeout(3000)
                if await _wait_for_cards(10):
                    return True
                # DEBUG: dump page text to understand what's showing
                try:
                    _dbg_text = await page.evaluate("() => document.body.innerText.slice(0, 1000)")
                    logger.warning("JetSMART DEBUG geoip-select page text: %s", _dbg_text[:500])
                except Exception:
                    pass

            logger.warning("JetSMART checkout: direct URL did not surface flight cards (url=%s), trying homepage form fill", page.url[:120])

        # ── Fallback: homepage form fill ────────────────────────────────
        from . import jetsmart as jetsmart_module

        helper = jetsmart_module.JetSmartConnectorClient(timeout=60.0)
        market = jetsmart_module._MARKET_MAP.get(origin, "cl")
        homepage = f"https://jetsmart.com/{market}/en"

        month_names_es = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo",
            6: "Junio", 7: "Julio", 8: "Agosto", 9: "Septiembre",
            10: "Octubre", 11: "Noviembre", 12: "Diciembre",
        }
        month_names_en = {
            1: "January", 2: "February", 3: "March", 4: "April", 5: "May",
            6: "June", 7: "July", 8: "August", 9: "September",
            10: "October", 11: "November", 12: "December",
        }
        month_es = f"{month_names_es[target_date.month]} {target_date.year}"
        month_en = f"{month_names_en[target_date.month]} {target_date.year}"

        logger.info("JetSMART checkout: replaying homepage search for %s→%s", origin, destination)

        try:
            await page.goto(homepage, wait_until="domcontentloaded", timeout=config.goto_timeout)
        except Exception as nav_err:
            logger.warning("JetSMART checkout: homepage goto failed (%s)", str(nav_err)[:100])
            return False

        await page.wait_for_timeout(3000)
        await helper._remove_overlays(page)
        await page.wait_for_timeout(500)

        # Now that homepage session cookies are established, retry the select URL
        # (it failed earlier because there were no session cookies yet)
        _select_url = (
            f"https://jetsmart.com/{market}/en/select"
            f"?origin={origin}&destination={destination}"
            f"&departure={departure_date}&adults=1&children=0"
        )
        logger.warning("JetSMART checkout: retrying select URL with session: %s", _select_url[:120])
        try:
            await page.goto(_select_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
            await page.wait_for_timeout(3000)
            if await _wait_for_cards(30):
                logger.warning("JetSMART checkout: select URL worked with session cookies!")
                return True
            _page_text = await page.evaluate("() => document.body.innerText.slice(0, 300)")
            logger.warning("JetSMART checkout: select URL page text: %s", _page_text[:200])
        except Exception as _sel_err:
            logger.warning("JetSMART checkout: select URL retry failed: %s", str(_sel_err)[:100])

        # Re-navigate to homepage for form fill
        try:
            await page.goto(homepage, wait_until="domcontentloaded", timeout=config.goto_timeout)
        except Exception:
            pass
        await page.wait_for_timeout(3000)
        await helper._remove_overlays(page)
        await page.wait_for_timeout(500)

        # Set one-way mode with retry + explicit debug
        for _ow_attempt in range(3):
            _ow_result = await page.evaluate("""() => {
                const targets = ['Solo ida', 'One way', 'One-way', 'Ida'];
                const tw = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
                let node;
                while (node = tw.nextNode()) {
                    const t = node.textContent.trim();
                    for (const target of targets) {
                        if (t === target) {
                            node.parentElement.click();
                            return 'clicked:' + t;
                        }
                    }
                }
                // Fallback: partial match in spans/divs
                for (const el of document.querySelectorAll('div, span, label')) {
                    const t = (el.textContent || '').trim();
                    if (t === 'Solo ida' || t === 'One way') {
                        el.click();
                        return 'clicked_el:' + t;
                    }
                }
                // List all candidate text nodes for debug
                const candidates = [];
                const tw2 = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
                let n2;
                while (n2 = tw2.nextNode()) {
                    const t2 = n2.textContent.trim();
                    if (t2.length > 2 && t2.length < 20) candidates.push(t2);
                }
                return 'not_found:' + candidates.slice(0, 20).join('|');
            }""")
            logger.warning("JetSMART checkout: _set_one_way attempt %d result: %s", _ow_attempt + 1, str(_ow_result)[:120])
            if isinstance(_ow_result, str) and _ow_result.startswith("clicked"):
                break
            await page.wait_for_timeout(1000)

        await page.wait_for_timeout(800)
        await helper._remove_overlays(page)

        async def _robust_fill_airport(iata: str, is_origin: bool) -> bool:
            """Fill airport with up to 10s wait for the dropdown to appear."""
            idx = 0 if is_origin else 1
            # Debug: log all visible text inputs to verify correct indexing
            try:
                _all_inputs = await page.evaluate("""() => {
                    const vis = (el) => {
                        const s = getComputedStyle(el);
                        return s.display !== 'none' && s.visibility !== 'hidden'
                            && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
                    };
                    return Array.from(document.querySelectorAll('input'))
                        .filter(inp => vis(inp) && (!inp.type || inp.type === 'text'))
                        .map((inp, i) => ({i, ph: inp.placeholder, name: inp.name, val: (inp.value||'').slice(0,20)}));
                }""")
                logger.warning("JetSMART checkout: visible text inputs (is_origin=%s idx=%s): %s", is_origin, idx, _all_inputs)
            except Exception:
                pass
            # Focus the correct text input — prefer by placeholder, fall back to index
            focused_by = await page.evaluate("""([idx, isOrigin]) => {
                const vis = (el) => {
                    const s = getComputedStyle(el);
                    return s.display !== 'none' && s.visibility !== 'hidden'
                        && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
                };
                const destPhs = ['Destino', 'Destination', 'Llegada', 'To'];
                const origPhs = ['Origen', 'Origin', 'Salida', 'From'];
                const phs = isOrigin ? origPhs : destPhs;
                for (const ph of phs) {
                    const inp = document.querySelector('input[placeholder="' + ph + '"]');
                    if (inp && vis(inp)) { inp.focus(); inp.click(); return 'ph:' + ph; }
                }
                // Fallback: by index among visible text inputs
                const inputs = Array.from(document.querySelectorAll('input'))
                    .filter(inp => vis(inp) && (!inp.type || inp.type === 'text'));
                if (inputs[idx]) { inputs[idx].focus(); inputs[idx].click(); return 'idx:' + idx; }
                return false;
            }""", [idx, is_origin])
            logger.warning("JetSMART checkout: focused input for %s via %s", iata, focused_by)
            await page.wait_for_timeout(1200)  # let auto-open dropdown settle
            # NOTE: do NOT call _remove_overlays here — it destroys the CDK overlay
            # that powers the airport autocomplete dropdown.
            # For destination: the origin selection auto-focuses the Destino field.
            # Avoid clicking it again (that closes the auto-opened dropdown).
            # Just type the IATA code using keyboard — the focused input receives it.
            if not is_origin:
                # Check if Destino is already the active element (auto-focused after origin select)
                _active_ph = await page.evaluate("""() => {
                    const ae = document.activeElement;
                    return ae ? ae.placeholder || ae.name || ae.tagName : 'none';
                }""")
                logger.warning("JetSMART checkout: active element before typing PMC: %s", _active_ph)
                # If Destino isn't auto-focused, click it with force
                if "Destino" not in str(_active_ph) and "estino" not in str(_active_ph):
                    try:
                        await page.locator('input[placeholder="Destino"]').first.click(force=True)
                        await page.wait_for_timeout(400)
                    except Exception:
                        pass

                # Debug: Check all Destino inputs (incl. hidden), Angular root context, and app-root __ngContext__
                _dest_diag = await page.evaluate("""([iata]) => {
                    const allDest = Array.from(document.querySelectorAll('input[placeholder="Destino"], input[placeholder="Destination"]'));
                    const appRoot = document.querySelector('app-root');
                    return {
                        destInputCount: allDest.length,
                        destInputs: allDest.map(i => ({ph: i.placeholder, vis: i.offsetHeight > 0, id: i.id, cls: i.className.slice(0,60), ngCtx: !!i.__ngContext__})),
                        appRootNgCtx: appRoot ? (typeof appRoot.__ngContext__) : 'no_app_root',
                        appRootCtxLen: (appRoot && Array.isArray(appRoot.__ngContext__)) ? appRoot.__ngContext__.length : -1,
                        activeEl: document.activeElement ? (document.activeElement.placeholder || document.activeElement.tagName) : 'none',
                    };
                }""", [iata])
                logger.warning("JetSMART checkout: dest diag: %s", str(_dest_diag)[:300])

                # Simulate character-by-character typing via JS keyboard events (bypasses pointer blocks)
                _js_type_result = await page.evaluate("""([iata]) => {
                    const inp = document.querySelector('input[placeholder="Destino"]');
                    if (!inp) return 'no_destino_input';
                    inp.focus();
                    // Clear existing value
                    const nativeInputSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                    nativeInputSetter.call(inp, '');
                    inp.dispatchEvent(new Event('input', {bubbles: true}));
                    // Type each character with full keyboard + input event sequence
                    for (const char of iata) {
                        const kd = new KeyboardEvent('keydown', {key: char, code: 'Key' + char, keyCode: char.charCodeAt(0), which: char.charCodeAt(0), bubbles: true, cancelable: true});
                        inp.dispatchEvent(kd);
                        const kp = new KeyboardEvent('keypress', {key: char, code: 'Key' + char, keyCode: char.charCodeAt(0), which: char.charCodeAt(0), charCode: char.charCodeAt(0), bubbles: true, cancelable: true});
                        inp.dispatchEvent(kp);
                        nativeInputSetter.call(inp, (inp.value || '') + char);
                        const ie = new InputEvent('input', {data: char, inputType: 'insertText', bubbles: true, cancelable: false});
                        inp.dispatchEvent(ie);
                        const ku = new KeyboardEvent('keyup', {key: char, code: 'Key' + char, keyCode: char.charCodeAt(0), which: char.charCodeAt(0), bubbles: true, cancelable: true});
                        inp.dispatchEvent(ku);
                    }
                    return 'typed:' + inp.value;
                }""", [iata])
                logger.warning("JetSMART checkout: JS keyboard sim result: %s", _js_type_result)
                await asyncio.sleep(3.0)  # Give Angular time to process and render autocomplete options
            else:
                # For origin: use locator-based typing to reliably dispatch events
                try:
                    _loc = page.locator('input[placeholder="Origen"]').first
                    await _loc.click(force=True)
                    await _loc.select_text()
                    await _loc.type(iata, delay=150)
                    logger.warning("JetSMART checkout: typed %s into [placeholder=Origen]", iata)
                except Exception as _te:
                    await page.keyboard.press("Control+A")
                    await page.keyboard.press("Backspace")
                    await page.keyboard.type(iata, delay=150)
                    logger.warning("JetSMART checkout: typed %s via keyboard fallback (%s)", iata, str(_te)[:60])
            # Wait up to 10s for a matching <li> to appear in the dropdown
            try:
                await page.wait_for_function(
                    """(iata) => {
                        const vis = (el) => {
                            const s = getComputedStyle(el);
                            return s.display !== 'none' && s.visibility !== 'hidden'
                                && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
                        };
                        return Array.from(document.querySelectorAll('li'))
                            .some(li => (li.textContent || '').includes(iata));
                    }""",
                    arg=iata,
                    timeout=10000,
                )
            except Exception:
                pass  # No dropdown appeared — will fall back to Enter
            # Debug: log <li> items containing the IATA to diagnose dropdown content
            try:
                _dbg_items = await page.evaluate("""(iata) => {
                    return Array.from(document.querySelectorAll('li'))
                        .filter(li => (li.textContent || '').includes(iata))
                        .slice(0, 8)
                        .map(li => (li.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 80));
                }""", iata)
                logger.warning("JetSMART checkout: <li> items with '%s': %s", iata, _dbg_items)
            except Exception:
                pass
            # Click the closest matching <li> (no visibility guard — force-click even if hidden)
            clicked = await page.evaluate("""([iata, idx]) => {
                const vis = (el) => {
                    const s = getComputedStyle(el);
                    return s.display !== 'none' && s.visibility !== 'hidden'
                        && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
                };
                const inputs = Array.from(document.querySelectorAll('input'))
                    .filter(inp => vis(inp) && (!inp.type || inp.type === 'text'));
                const target = inputs[idx];
                const tRect = target ? target.getBoundingClientRect() : null;
                const items = Array.from(document.querySelectorAll('li'))
                    .filter(li => (li.textContent || '').includes(iata))
                    .sort((a, b) => {
                        if (!tRect) return 0;
                        const ar = a.getBoundingClientRect(), br = b.getBoundingClientRect();
                        return (Math.abs(ar.top - tRect.bottom) + Math.abs(ar.left - tRect.left))
                             - (Math.abs(br.top - tRect.bottom) + Math.abs(br.left - tRect.left));
                    });
                for (const item of items) { item.click(); return (item.textContent || '').trim().slice(0, 80); }
                return false;
            }""", [iata, idx])
            if clicked:
                logger.warning("JetSMART checkout: airport %s selected via dropdown: %s", iata, clicked)
                await page.wait_for_timeout(800)
                return True
            # Fallback: Enter key
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(600)
            logger.warning("JetSMART checkout: airport %s — pressed Enter (no dropdown)", iata)
            return True

        if not await _robust_fill_airport(origin, True):
            logger.warning("JetSMART checkout: could not fill origin %s", origin)
            return False
        # Diagnostic: confirm origin input value after _fill_airport
        _orig_val = await page.evaluate("() => (document.querySelector('input[placeholder=\"Origen\"]') || {}).value || ''")
        logger.warning("JetSMART checkout: origin input value after _fill_airport: %r", _orig_val)

        _js_city_map = {
            "SCL": "Santiago", "IQQ": "Iquique", "ANF": "Antofagasta", "CJC": "Calama",
            "PMC": "Puerto Montt", "CCP": "Concepcion", "ZCO": "Temuco", "ARI": "Arica",
            "LSC": "La Serena", "ZOS": "Osorno", "ZAL": "Valdivia",
            "LIM": "Lima", "CUZ": "Cusco", "AQP": "Arequipa",
            "EZE": "Buenos Aires", "AEP": "Buenos Aires", "COR": "Cordoba",
            "MDZ": "Mendoza", "IGR": "Iguazu", "NQN": "Neuquen", "USH": "Ushuaia",
            "ROS": "Rosario", "BRC": "Bariloche", "SLA": "Salta",
            "BOG": "Bogota", "MDE": "Medellin", "CTG": "Cartagena", "CLO": "Cali",
        }
        _dest_city = _js_city_map.get(destination, destination)

        # ── Destination fill ─────────────────────────────────────────────
        # History:
        # - CDP mouse.click(394, 221) → hits sticky nav bar (run 100)
        # - page.locator().click() → times out even with nav hidden (runs 101-102)
        # - CDP click at coords after nav-hide → y=400 (Playwright scrolled it
        #   during timeout attempt) but dropdown still didn't open
        # New hypothesis: destination dropdown AUTO-OPENS after origin selection.
        # If we then try to click Destino, we CLOSE it (toggle). After that nothing
        # appears. Run 103: check auto-open state FIRST, skip any click if open.
        # Fallback: Tab key from origin (triggers focus handler on Destino).
        await asyncio.sleep(1.5)  # wait longer for auto-open animation

        _dest_filled = False
        _nav_keys = ['Reserva', 'Beneficios', 'viaje', 'Vuelos', 'Admin', 'Oferta', 'Destinos', 'SMART']

        def _is_nav_only(sample):
            if not sample:
                return True
            return all(any(n in t for n in _nav_keys) for t in sample)

        # Step 1: Check if destination dropdown already auto-opened after origin
        _auto_open = await page.evaluate("""([iata, city]) => {
            const vis = (el) => { const s = getComputedStyle(el); return s.display !== 'none' && s.visibility !== 'hidden' && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length); };
            const allLis = Array.from(document.querySelectorAll('li')).filter(vis);
            const cpLis = allLis.filter(l => (l.className || '').includes('cursor-pointer'));
            const tgt = allLis.find(l => { const t = (l.textContent||'').replace(/\s+/g,' ').trim(); return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase()); });
            const inpR = (() => { const i = document.querySelector('input[placeholder="Destino"]'); return i ? i.getBoundingClientRect() : null; })();
            return {total: allLis.length, cpCount: cpLis.length,
                cpSample: cpLis.slice(0,8).map(l => (l.textContent||'').replace(/\s+/g,' ').trim().slice(0,35)),
                pmcFound: !!tgt,
                inpY: inpR ? inpR.top + inpR.height/2 : 0};
        }""", [destination, _dest_city])
        logger.warning("JetSMART checkout: dest AUTO-OPEN check (before any click): %s", _auto_open)

        _dropdown_open = _auto_open.get('cpCount', 0) > 0 and not _is_nav_only(_auto_open.get('cpSample', []))

        if not _dropdown_open:
            # Step 2a: Try Tab key from current focus — triggers Vue focus handler on Destino
            logger.warning("JetSMART checkout: dropdown not auto-open, trying Tab key")
            await page.keyboard.press("Tab")
            await asyncio.sleep(1.5)
            _after_tab = await page.evaluate("""([iata, city]) => {
                const vis = (el) => { const s = getComputedStyle(el); return s.display !== 'none' && s.visibility !== 'hidden' && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length); };
                const allLis = Array.from(document.querySelectorAll('li')).filter(vis);
                const cpLis = allLis.filter(l => (l.className || '').includes('cursor-pointer'));
                const tgt = allLis.find(l => { const t = (l.textContent||'').replace(/\s+/g,' ').trim(); return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase()); });
                return {total: allLis.length, cpCount: cpLis.length,
                    cpSample: cpLis.slice(0,8).map(l => (l.textContent||'').replace(/\s+/g,' ').trim().slice(0,35)),
                    pmcFound: !!tgt};
            }""", [destination, _dest_city])
            logger.warning("JetSMART checkout: dest after Tab key: %s", _after_tab)
            if _after_tab.get('cpCount', 0) > 0 and not _is_nav_only(_after_tab.get('cpSample', [])):
                _dropdown_open = True
                _auto_open = _after_tab

        if not _dropdown_open:
            # Step 2b: hide nav + CDP click at actual input coordinates
            logger.warning("JetSMART checkout: Tab didn't open dropdown, trying nav-hide + CDP click")
            _inp_coords_before = await page.evaluate("""() => {
                const i = document.querySelector('input[placeholder="Destino"]');
                if (!i) return null;
                const r = i.getBoundingClientRect();
                const cov = document.elementFromPoint(r.left + r.width/2, r.top + r.height/2);
                return {x: r.left + r.width/2, y: r.top + r.height/2, covTag: cov ? cov.tagName : 'none', covCls: cov ? (cov.className||'').slice(0,40) : ''};
            }""")
            logger.warning("JetSMART checkout: dest input coords + covering elem: %s", _inp_coords_before)
            # Hide sticky nav/header
            await page.evaluate("""() => {
                for (const n of document.querySelectorAll('header, nav, [class*="navbar"], [class*="header"]')) {
                    const r = n.getBoundingClientRect();
                    if (r.top < 10 && r.height > 20) {
                        n.dataset._savedVis = n.style.visibility;
                        n.style.visibility = 'hidden';
                    }
                }
            }""")
            await asyncio.sleep(0.2)
            if _inp_coords_before:
                await page.mouse.click(_inp_coords_before['x'], _inp_coords_before['y'])
                logger.warning("JetSMART checkout: CDP click at dest (nav hidden): %s", _inp_coords_before)
            # Restore nav
            await page.evaluate("""() => {
                for (const n of document.querySelectorAll('header, nav, [class*="navbar"], [class*="header"]')) {
                    if (n.dataset._savedVis !== undefined) {
                        n.style.visibility = n.dataset._savedVis;
                        delete n.dataset._savedVis;
                    }
                }
            }""")
            await asyncio.sleep(1.5)
            _after_hide_click = await page.evaluate("""([iata, city]) => {
                const vis = (el) => { const s = getComputedStyle(el); return s.display !== 'none' && s.visibility !== 'hidden' && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length); };
                const allLis = Array.from(document.querySelectorAll('li')).filter(vis);
                const cpLis = allLis.filter(l => (l.className || '').includes('cursor-pointer'));
                const tgt = allLis.find(l => { const t = (l.textContent||'').replace(/\s+/g,' ').trim(); return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase()); });
                return {total: allLis.length, cpCount: cpLis.length,
                    cpSample: cpLis.slice(0,8).map(l => (l.textContent||'').replace(/\s+/g,' ').trim().slice(0,35)),
                    pmcFound: !!tgt};
            }""", [destination, _dest_city])
            logger.warning("JetSMART checkout: dest after nav-hide CDP click: %s", _after_hide_click)
            if _after_hide_click.get('cpCount', 0) > 0 and not _is_nav_only(_after_hide_click.get('cpSample', [])):
                _dropdown_open = True
                _auto_open = _after_hide_click

        # Step 3: if dropdown is open, click PMC li directly
        if _dropdown_open:
            _pmc_li = await page.evaluate("""([iata, city]) => {
                const vis = (el) => { const s = getComputedStyle(el); return s.display !== 'none' && s.visibility !== 'hidden' && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length); };
                const lis = Array.from(document.querySelectorAll('li')).filter(vis);
                const tgt = lis.find(l => { const t = (l.textContent||'').replace(/\s+/g,' ').trim(); return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase()); });
                if (!tgt) return {found: false, total: lis.length};
                const r = tgt.getBoundingClientRect();
                return {found: true, x: r.left + r.width/2, y: r.top + r.height/2, h: r.height, text: (tgt.textContent||'').trim().slice(0,40)};
            }""", [destination, _dest_city])
            logger.warning("JetSMART checkout: PMC li in open dropdown: %s", _pmc_li)
            if _pmc_li.get('found') and _pmc_li.get('h', 0) > 0:
                await page.mouse.click(_pmc_li['x'], _pmc_li['y'])
                await asyncio.sleep(0.8)
                _v = await page.evaluate("() => (document.querySelector('input[placeholder=\"Destino\"]') || {}).value || ''")
                logger.warning("JetSMART checkout: destino after PMC click: %r", _v)
                if _v and _v.strip() and _v.strip().upper() not in ('PMC', ''):
                    _dest_filled = True

        # Step 4: open Destino dropdown, first try clicking PMC directly from unfiltered list,
        # then fall back to typing city name. 
        # KEY BUG (run 117): Ctrl+A+Backspace was clearing the ORIGIN field (focus stays on
        # origin after _fill_airport's LI click). Without origin committed, Destino autocomplete
        # has no route context → "No hay resultados" for any city name.
        # Fix: Remove Ctrl+A+Backspace. Open Destino dropdown and try PMC from unfiltered list.
        if not _dest_filled:
            logger.warning("JetSMART checkout: no dropdown PMC click, trying type autocomplete")
            # Log current origin value to confirm it's still set
            _orig_before_dest = await page.evaluate("() => (document.querySelector('input[placeholder=\"Origen\"]') || {}).value || ''")
            logger.warning("JetSMART checkout: origin val before dest step4: %r", _orig_before_dest)
            # DO NOT press Ctrl+A+Backspace here — that was clearing the origin field!
            # JS-click inputs[1] to open Vue's destination dropdown.
            await page.evaluate("""() => {
                const vis = (el) => !!(el.offsetWidth || el.offsetHeight);
                const inputs = Array.from(document.querySelectorAll('input'))
                    .filter(i => vis(i) && (!i.type || i.type === 'text'));
                const t = inputs[1];
                if (t) { t.focus(); t.click(); }
            }""")
            await asyncio.sleep(2.0)
            # Log unfiltered dropdown state: if PMC is here, click directly without typing
            _unfiltered = await page.evaluate("""([iata, city]) => {
                const vis = (el) => { const s = getComputedStyle(el); return s.display !== 'none' && s.visibility !== 'hidden' && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length); };
                const normalize = (s) => s.replace(/\\s+/g, ' ').trim();
                const allLis = Array.from(document.querySelectorAll('li')).filter(vis)
                    .filter(l => (l.className||'').includes('cursor-pointer'));
                const pmcLis = allLis.filter(li => {
                    const t = normalize(li.textContent||'');
                    return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase());
                });
                return {
                    cpCount: allLis.length,
                    cpSample: allLis.slice(0,8).map(l => normalize(l.textContent||'').slice(0,35)),
                    pmcFound: pmcLis.length > 0,
                    pmcText: pmcLis.map(l => normalize(l.textContent||'').slice(0,35)),
                };
            }""", [destination, _dest_city])
            logger.warning("JetSMART checkout: unfiltered dropdown state: %s", _unfiltered)

            # Strategy A: click PMC directly from the unfiltered list (no typing needed)
            if _unfiltered.get('pmcFound'):
                _clicked_pmc = await page.evaluate("""([iata, city]) => {
                    const normalize = (s) => s.replace(/\\s+/g, ' ').trim();
                    const vis = (el) => { const s = getComputedStyle(el); return s.display !== 'none' && s.visibility !== 'hidden' && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length); };
                    const inputs = Array.from(document.querySelectorAll('input'))
                        .filter(i => vis(i) && (!i.type || i.type === 'text'));
                    const destInput = inputs[1];
                    const destRect = destInput ? destInput.getBoundingClientRect() : null;
                    const allLis = Array.from(document.querySelectorAll('li')).filter(vis)
                        .filter(l => (l.className||'').includes('cursor-pointer'));
                    const pmcLis = allLis.filter(li => {
                        const t = normalize(li.textContent||'');
                        return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase());
                    }).sort((a, b) => {
                        if (!destRect) return 0;
                        const ar = a.getBoundingClientRect(), br = b.getBoundingClientRect();
                        return (Math.abs(ar.top - destRect.bottom) + Math.abs(ar.left - destRect.left))
                             - (Math.abs(br.top - destRect.bottom) + Math.abs(br.left - destRect.left));
                    });
                    const tgt = pmcLis[0];
                    if (!tgt) return false;
                    tgt.click();
                    return normalize(tgt.textContent||'').slice(0,40);
                }""", [destination, _dest_city])
                logger.warning("JetSMART checkout: direct PMC click from unfiltered list: %r", _clicked_pmc)
                await asyncio.sleep(1.5)
                _v_direct = await page.evaluate("() => (document.querySelector('input[placeholder=\"Destino\"]') || {}).value || ''")
                logger.warning("JetSMART checkout: destVal after direct PMC click: %r", _v_direct)
                if _v_direct and _v_direct.strip():
                    _dest_filled = True
            
            # Strategy B: type city name to filter (only if direct click didn't work)
            if not _dest_filled:
                logger.warning("JetSMART checkout: PMC not in unfiltered list or direct click failed, typing city name")
                await page.keyboard.type(_dest_city, delay=120)
                await asyncio.sleep(3.5)

                # Log dropdown state BEFORE clicking so we know if filter worked.
                _pre_click = await page.evaluate("""([iata, city]) => {
                    const vis = (el) => { const s = getComputedStyle(el); return s.display !== 'none' && s.visibility !== 'hidden' && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length); };
                    const normalize = (s) => s.replace(/\\s+/g, ' ').trim();
                    const allLis = Array.from(document.querySelectorAll('li')).filter(vis);
                    const pmcLis = allLis.filter(li => {
                        const t = normalize(li.textContent||'');
                        return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase());
                    }).sort((a,b) => a.getBoundingClientRect().height - b.getBoundingClientRect().height);
                    const zDivs = Array.from(document.querySelectorAll('div[class*="z-["]')).filter(d => d.offsetHeight > 0);
                    return {
                        totalLis: allLis.length,
                        pmcCount: pmcLis.length,
                        pmcHeights: pmcLis.slice(0,3).map(l => Math.round(l.getBoundingClientRect().height)),
                        zDivTexts: zDivs.slice(0,5).map(d => (d.innerText||'').replace(/\\s+/g,' ').trim().slice(0,50)),
                    };
                }""", [destination, _dest_city])
                logger.warning("JetSMART checkout: pre-CDP-click state: %s", _pre_click)

            # Find LI inside the booking dropdown container (after typing filtered it).
            # Also used if strategy A (unfiltered click) didn't work.
            if not _dest_filled:
                _li_coords = await page.evaluate("""([iata, city]) => {
                const vis = (el) => { const s = getComputedStyle(el); return s.display !== 'none' && s.visibility !== 'hidden' && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length); };
                const normalize = (s) => s.replace(/\\s+/g, ' ').trim();
                // Find the booking dropdown container first
                let container = null;
                for (const dd of Array.from(document.querySelectorAll('div'))) {
                    const cls = dd.className || '';
                    if (cls.includes('min-w-52') && cls.includes('select-none') && dd.offsetHeight > 0) {
                        container = dd; break;
                    }
                }
                const searchRoot = container || document;
                const allLis = Array.from(searchRoot.querySelectorAll('li')).filter(vis);
                const pmcLis = allLis.filter(li => {
                    const t = normalize(li.textContent||'');
                    return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase());
                }).sort((a,b) => a.getBoundingClientRect().height - b.getBoundingClientRect().height);
                const tgt = pmcLis[0];
                if (!tgt) return null;
                tgt.scrollIntoView({block: 'nearest', behavior: 'instant'});
                const r = tgt.getBoundingClientRect();
                // Find innermost interactive child (button/a/[role]) — Vue @click is often there
                const inner = tgt.querySelector('button, a, [role="option"]');
                const innerR = inner ? inner.getBoundingClientRect() : null;
                return {x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2),
                    h: Math.round(r.height), cls: (tgt.className||'').slice(0,50),
                    text: normalize(tgt.textContent||'').slice(0,40),
                    inContainer: !!container,
                    innerTag: inner ? inner.tagName : null,
                    innerText: inner ? normalize(inner.textContent||'').slice(0,30) : null,
                    innerY: innerR ? Math.round(innerR.top + innerR.height/2) : null,
                    innerHTML: tgt.innerHTML.slice(0, 200)};
            }""", [destination, _dest_city])
            logger.warning("JetSMART checkout: PMC LI info: %s", _li_coords)

            if _li_coords and _li_coords.get('y', 0) > 0:
                _li_text = (_li_coords.get('text') or '').strip()
                _inner_tag = _li_coords.get('innerTag')
                _inner_text = (_li_coords.get('innerText') or '').strip()
                _clicked = False
                # Strategy A: click the inner interactive child (button/a) — Vue @click is often there
                if _inner_tag and _inner_text:
                    try:
                        _inner_sel = f"{_inner_tag.lower()}".lower()
                        await page.locator('li').filter(has_text=_li_text).locator(_inner_sel).first.click(force=True, timeout=4000)
                        logger.warning("JetSMART checkout: inner %s force-click succeeded (text=%r)", _inner_tag, _inner_text)
                        _clicked = True
                    except Exception as _ie:
                        logger.warning("JetSMART checkout: inner %s click failed: %s", _inner_tag, _ie)
                # Strategy B: click the LI itself with force=True
                if not _clicked:
                    try:
                        await page.locator('li').filter(has_text=_li_text).first.click(force=True, timeout=5000)
                        logger.warning("JetSMART checkout: LI force-click succeeded (text=%r)", _li_text)
                        _clicked = True
                    except Exception as _lce:
                        logger.warning("JetSMART checkout: LI force-click failed: %s", _lce)
                # Strategy C: JS dispatchEvent (all 3: pointerdown+mousedown+click) on the LI
                if not _clicked:
                    await page.evaluate("""([iata, city]) => {
                        const normalize = (s) => s.replace(/\\s+/g,' ').trim();
                        const allLis = Array.from(document.querySelectorAll('li'));
                        const tgt = allLis.find(l => {
                            const t = normalize(l.textContent||'');
                            return t.includes(iata) || t.toLowerCase().includes(city.toLowerCase());
                        });
                        if (tgt) {
                            const fireOn = tgt.querySelector('button, a') || tgt;
                            ['pointerdown','mousedown','pointerup','mouseup','click'].forEach(ev =>
                                fireOn.dispatchEvent(new MouseEvent(ev, {bubbles:true, cancelable:true, view:window}))
                            );
                        }
                    }""", [destination, _dest_city])
                    logger.warning("JetSMART checkout: dispatchEvent fallback fired")
                await asyncio.sleep(1.5)
                _v2 = await page.evaluate("() => (document.querySelector('input[placeholder=\"Destino\"]') || {}).value || ''")
                _cp_after = await page.evaluate("""() => {
                    const vis = (el) => !!(el.offsetWidth || el.offsetHeight);
                    return Array.from(document.querySelectorAll('li')).filter(vis)
                        .filter(l => (l.className||'').includes('cursor-pointer')).length;
                }""")
                logger.warning("JetSMART checkout: after force-click on PMC LI: destVal=%r cpCount=%d", _v2, _cp_after)
                # Dropdown still open (cpCount>20)? Press Escape to close it.
                if _cp_after > 20:
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(0.5)
                    _v2 = await page.evaluate("() => (document.querySelector('input[placeholder=\"Destino\"]') || {}).value || ''")
                    logger.warning("JetSMART checkout: destVal after Escape: %r", _v2)
                await page.evaluate("() => window.scrollTo(0, 0)")
                await asyncio.sleep(0.3)
                if _v2 and _v2.strip():
                    _dest_filled = True
                    logger.warning("JetSMART checkout: PMC filled (cpCount=%d, val=%r)", _cp_after, _v2)
            else:
                logger.warning("JetSMART checkout: could not get PMC LI coords, trying Enter fallback")
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
                _vf2 = await page.evaluate("() => (document.querySelector('input[placeholder=\"Destino\"]') || {}).value || ''")
                if _vf2 and _vf2.strip():
                    _dest_filled = True

        if not _dest_filled:
            logger.warning("JetSMART checkout: all approaches failed, falling back to _fill_airport")
            await helper._fill_airport(page, destination, is_origin=False)
            _vf = await page.evaluate("() => (document.querySelector('input[placeholder=\"Destino\"]') || {}).value || ''")
            logger.warning("JetSMART checkout: destino after _fill_airport: %r", _vf)

        # After destination fill, check Vue component state and whether calendar auto-opened.
        # Diagnostic: click Search without a date to see what Vue validation fires.
        await asyncio.sleep(1.0)
        _cal_after_dest = await page.evaluate("""() => {
            const allDD = Array.from(document.querySelectorAll('div[class*="z-["]')).filter(d => d.offsetHeight > 0);
            const anyLarge = Array.from(document.querySelectorAll('div')).filter(d => {
                const r = d.getBoundingClientRect(); return r.width > 200 && r.height > 150 && d.offsetHeight > 0;
            });
            const hasManyDays = (el) => Array.from(el.querySelectorAll('*')).filter(d =>
                d.children.length === 0 && /^[0-9]{1,2}$/.test((d.textContent||'').trim())
                && d.offsetHeight > 0 && d.offsetHeight <= 50 && d.offsetWidth <= 50).length >= 20;
            const cal = [...allDD, ...anyLarge].find(hasManyDays);
            // Also check form input values  
            const inputs = Array.from(document.querySelectorAll('input')).filter(i => i.offsetHeight);
            const vals = inputs.slice(0,4).map(i => ({ph: i.placeholder, val: (i.value||'').slice(0,20)}));
            return {calFound: !!cal, calText: cal ? (cal.innerText||'').slice(0,60) : '', inputs: vals};
        }""")
        logger.warning("JetSMART checkout: state after dest fill: %s", _cal_after_dest)

        # Try clicking the search button WITHOUT date to observe Vue's validation error.
        # If Vue says "select date" (not "select origin/destination"), they're committed.
        _search_diag = await page.evaluate("""() => {
            const btns = Array.from(document.querySelectorAll('button, div[class*="cursor-pointer"]'));
            const buscar = btns.find(b => {
                const t = (b.innerText||b.textContent||'').trim().toLowerCase();
                return t.includes('buscar') || t.includes('search') || t === 'buscar vuelos';
            });
            if (buscar) { buscar.click(); return 'clicked:' + (buscar.innerText||'').trim().slice(0,30); }
            return 'not_found';
        }""")
        logger.warning("JetSMART checkout: search diag click: %s", _search_diag)
        await asyncio.sleep(1.0)
        _validation_msgs = await page.evaluate("""() => {
            // Find validation error messages / tooltips
            const msgs = [];
            for (const el of document.querySelectorAll('[class*="error"], [class*="invalid"], [class*="tooltip"], [class*="alert"], [role="alert"]')) {
                const t = (el.innerText||el.textContent||'').trim();
                if (t && t.length < 200 && el.offsetHeight > 0) msgs.push(t.slice(0,80));
            }
            // Also check for red text or warning colors
            const url = window.location.href;
            return {msgs: msgs.slice(0,5), url: url.slice(0,100)};
        }""")
        logger.warning("JetSMART checkout: validation after search click: %s", _validation_msgs)

        # After destination is committed in Vue, calendar often auto-opens.

        # Force-close any destinations autocomplete overlay before clicking date input.
        # After typing city name, the overlay class is 'fixed min-w-52 select-none overflow-hidd'
        # and text changes from 'Todos los destinos' to results/no-results — match by class.
        await page.evaluate("""() => {
            for (const dd of Array.from(document.querySelectorAll('div'))) {
                const cls = dd.className || '';
                if ((cls.includes('min-w-52') || cls.includes('select-none'))
                    && dd.offsetHeight > 0) {
                    dd.remove();
                }
            }
        }""")
        await asyncio.sleep(0.3)

        # Fill departure date — _fill_date handles calendar open internally
        import types as _types
        _req_mock = _types.SimpleNamespace(date_from=target_date)
        _cal_state = await page.evaluate("""() => {
            const allDD = Array.from(document.querySelectorAll('div[class*="z-["]')).filter(d => d.offsetHeight > 0);
            const cal = allDD.find(dd => {
                const cells = Array.from(dd.querySelectorAll('*')).filter(d =>
                    d.children.length === 0 && /^[0-9]{1,2}$/.test((d.textContent||'').trim())
                    && d.offsetHeight > 0 && d.offsetHeight <= 50 && d.offsetWidth <= 50);
                return cells.length >= 20;
            });
            if (!cal) return 'not_open (overlays=' + allDD.length + ' classes=' + allDD.slice(0,2).map(d=>(d.className||'').slice(0,40)).join('|') + ')';
            return 'open:' + (cal.innerText || '').replace(/\\s+/g,' ').trim().slice(0, 60);
        }""")
        logger.warning("JetSMART checkout: calendar state before _fill_date: %s", _cal_state)
        _date_ok = await helper._fill_date(page, _req_mock)

        logger.warning("JetSMART checkout: date %s fill result: %s", departure_date, _date_ok)
        if not _date_ok:
            logger.warning("JetSMART checkout: could not select departure date %s", departure_date)
            return False
        await asyncio.sleep(0.5)

        # Log form state before search
        try:
            _form_state = await page.evaluate("""() => {
                const vis = (el) => !!(el.offsetWidth || el.offsetHeight);
                return Array.from(document.querySelectorAll('input'))
                    .filter(inp => vis(inp) && (!inp.type || inp.type === 'text'))
                    .map(inp => ({ph: inp.placeholder, val: (inp.value||'').slice(0, 30)}));
            }""")
            logger.warning("JetSMART checkout: form state before search: %s", _form_state)
        except Exception:
            pass

        await helper._click_search(page)
        # Confirm the navigation to booking.jetsmart.com started; retry if not
        booking_nav_started = False
        try:
            await page.wait_for_url("**/V2/Flight**", timeout=10000)
            booking_nav_started = True
        except Exception:
            pass
        logger.warning("JetSMART checkout: after search click, url=%s booking_nav_started=%s", page.url[:100], booking_nav_started)
        if not booking_nav_started:
            # Retry with broader search button click
            try:
                await page.evaluate("""() => {
                    for (const el of document.querySelectorAll('div, button')) {
                        const cls = el.className || '';
                        if (!cls.includes('cursor-pointer') && el.tagName !== 'BUTTON') continue;
                        const text = (el.innerText || el.textContent || '').trim().toLowerCase();
                        if ((text.includes('buscar') || text.includes('search')) && text.length < 30) {
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 60 && rect.height > 30) {
                                el.click();
                                return text;
                            }
                        }
                    }
                    return null;
                }""")
            except Exception:
                pass
            await page.wait_for_timeout(3000)

        if await _wait_for_cards(45):
            return True

        logger.warning("JetSMART checkout: homepage replay did not surface results (url=%s)", page.url[:120])
        return False

    async def _jetsmart_checkout(self, page, config, offer, offer_id, booking_url, passengers, t0):
        """JetSMART full checkout: Vuelo→Equipaje→Asientos→Extras→Pasajeros→Pago."""
        pax = passengers[0] if passengers else FAKE_PASSENGER
        step = "started"
        captured_details: dict = {}
        # Ensure viewport is wide enough for md: breakpoint (Continuar button uses md:flex)
        try:
            await page.set_viewport_size({"width": 1280, "height": 800})
        except Exception:
            pass

        # ── Step 1: Navigate & get flight results ────────────────────────
        if not await self._prepare_jetsmart_checkout_results(page, config, offer):
            elapsed = time.monotonic() - t0
            return CheckoutProgress(
                status="error",
                step=step,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                booking_url=booking_url,
                message="JetSMART checkout: could not load flight selection page",
                elapsed_seconds=elapsed,
                details=captured_details,
            )
        step = "flights_loaded"

        # ── Step 2: Click "Tarifa desde" (blue regular fare, not Club) ───
        # The button cell has class including 'text-n-blue', 'border-l', 'cursor-pointer'
        try:
            clicked_tarifa = await page.evaluate("""() => {
                for (const el of document.querySelectorAll('div')) {
                    const cls = el.className || '';
                    if (!cls.includes('border-l') || !cls.includes('cursor-pointer')) continue;
                    const text = (el.innerText || el.textContent || '').trim();
                    if (text.startsWith('Tarifa desde') || (text.includes('Tarifa desde') && !text.includes('Club'))) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }""")
        except Exception:
            clicked_tarifa = True  # context destroyed = navigation happened, treat as success
        if not clicked_tarifa:
            elapsed = time.monotonic() - t0
            return CheckoutProgress(
                status="error",
                step=step,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                booking_url=booking_url,
                message="JetSMART checkout: could not click 'Tarifa desde' fare cell",
                elapsed_seconds=elapsed,
                details=captured_details,
            )
        await page.wait_for_timeout(2000)
        step = "fare_panel_opened"

        # ── Step 3: Click first "¡Lo quiero!" in the bundle sub-panel ────
        # This selects the cheapest Tarifa desde bundle and enables Continuar
        try:
            clicked_lo_quiero = await page.evaluate("""() => {
                for (const el of document.querySelectorAll('div')) {
                    const text = (el.innerText || el.textContent || '').trim();
                    if ((text === '\u00a1Lo quiero!' || text === 'Lo quiero') && (el.className || '').includes('rounded-full')) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }""")
        except Exception:
            clicked_lo_quiero = True  # context destroyed = navigation happened
        if not clicked_lo_quiero:
            try:
                await page.locator("div[class*='rounded-full']:has-text('Lo quiero')").first.click(timeout=3000)
                clicked_lo_quiero = True
            except Exception:
                pass
        if not clicked_lo_quiero:
            elapsed = time.monotonic() - t0
            return CheckoutProgress(
                status="error",
                step=step,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                booking_url=booking_url,
                message="JetSMART checkout: could not click '¡Lo quiero!' bundle selector",
                elapsed_seconds=elapsed,
                details=captured_details,
            )
        await page.wait_for_timeout(2000)
        # Dismiss Club JetSMART membership modal if it appeared after bundle selection
        try:
            await page.evaluate("""() => {
                for (const el of document.querySelectorAll('div, button, a, span')) {
                    const text = (el.innerText || el.textContent || '').trim();
                    if (text === 'No quiero ser parte' || text === "I don't want to be part") {
                        el.click(); return true;
                    }
                }
                for (const el of document.querySelectorAll('div')) {
                    const cls = el.className || '';
                    if (cls.includes('bg-[#484848]') && cls.includes('cursor-pointer')) {
                        el.click(); return true;
                    }
                }
                return false;
            }""")
        except Exception:
            pass
        await page.wait_for_timeout(500)
        # Close the bundle selection panel — target the bundle-panel Cerrar specifically
        # (has class text-[#828282], distinct from the search-edit Cerrar which has font-body text-sm)
        try:
            cerrar_clicked = await page.evaluate("""() => {
                // Prefer the bundle-panel close (text-[#828282] class)
                for (const el of document.querySelectorAll('div, button, span')) {
                    const cls = el.className || '';
                    if (!cls.includes('cursor-pointer')) continue;
                    if (!cls.includes('text-[#828282]')) continue;
                    const text = (el.innerText || el.textContent || '').trim();
                    if (text === 'Cerrar' || text.startsWith('Cerrar') || text === 'Close') {
                        el.click();
                        return true;
                    }
                }
                // Fallback: any Cerrar
                for (const el of document.querySelectorAll('div, button, span')) {
                    const cls = el.className || '';
                    if (!cls.includes('cursor-pointer')) continue;
                    const text = (el.innerText || el.textContent || '').trim();
                    if (text === 'Cerrar' || text.startsWith('Cerrar') || text === 'Close') {
                        el.click();
                        return true;
                    }
                }
                return false;
            }""")
        except Exception:
            cerrar_clicked = True  # context destroyed = navigation already happened
        if not cerrar_clicked:
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
        await page.wait_for_timeout(1500)
        step = "fare_selected"

        # Helper: dismiss the Club JetSMART membership modal (appears after bundle selection or Continuar)
        async def _dismiss_club_modal():
            try:
                result = await page.evaluate("""() => {
                    // Prefer the dark X close button (bg-[#484848]) — unambiguous modal close
                    for (const el of document.querySelectorAll('div')) {
                        const cls = el.className || '';
                        if (cls.includes('bg-[#484848]') && cls.includes('cursor-pointer')) {
                            el.click();
                            return 'close_x';
                        }
                    }
                    // 'No quiero ser parte' = "I don't want to be part of Club JetSMART"
                    for (const el of document.querySelectorAll('div, button, a, span')) {
                        const text = (el.innerText || el.textContent || '').trim();
                        if (text === 'No quiero ser parte' || text === "I don't want to be part") {
                            el.click();
                            return 'no_quiero';
                        }
                    }
                    return null;
                }""")
                if result:
                    # Wait for modal to fully close (bg-[#484848] X button disappears)
                    try:
                        await page.wait_for_function(
                            """() => {
                                for (const el of document.querySelectorAll('div')) {
                                    const cls = el.className || '';
                                    if (cls.includes('bg-[#484848]') && cls.includes('cursor-pointer')) {
                                        const rect = el.getBoundingClientRect();
                                        if (rect.width > 0 && rect.height > 0) return false;
                                    }
                                }
                                return true;
                            }""",
                            timeout=3000,
                        )
                    except Exception:
                        # Try Escape as additional push
                        try:
                            await page.keyboard.press("Escape")
                        except Exception:
                            pass
                        await page.wait_for_timeout(1000)
                    return True
            except Exception:
                pass
            # Final fallback: Escape key
            try:
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(500)
            except Exception:
                pass
            return False

        # Helper: click the active "Continuar" button using JS .click() (bypasses display:none)
        async def _click_continuar():
            # Always dismiss Club modal first
            await _dismiss_club_modal()
            for attempt in range(8):
                try:
                    result = await page.evaluate("""() => {
                        // Find the ACTIVE Continuar button: has bg-[#af272f] (red) class
                        for (const el of document.querySelectorAll('div')) {
                            const cls = el.className || '';
                            if (!cls.includes('rounded-full')) continue;
                            if (!cls.includes('bg-[#af272f]')) continue;
                            if (cls.includes('pointer-events-none')) continue;
                            const text = (el.innerText || el.textContent || '').trim();
                            if (text === 'Continuar' || text.startsWith('Continuar')) {
                                el.click();
                                return 'clicked';
                            }
                        }
                        // Fallback: any rounded-full Continuar without pointer-events-none
                        for (const el of document.querySelectorAll('div')) {
                            const cls = el.className || '';
                            if (!cls.includes('rounded-full')) continue;
                            if (cls.includes('pointer-events-none')) continue;
                            const text = (el.innerText || el.textContent || '').trim();
                            if (text === 'Continuar' || text.startsWith('Continuar')) {
                                el.click();
                                return 'fallback';
                            }
                        }
                        // Broad: any button/div with Continuar text that is clickable
                        for (const el of document.querySelectorAll('button, div, a')) {
                            const text = (el.innerText || el.textContent || '').trim();
                            if (text !== 'Continuar') continue;
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 0 && rect.height > 0) {
                                el.click();
                                return 'broad';
                            }
                        }
                        return null;
                    }""")
                    if result:
                        await page.wait_for_timeout(2500)
                        return True
                except Exception:
                    pass  # context destroyed = navigation happened, treat as success
                    await page.wait_for_timeout(500)
                    return True
                await page.wait_for_timeout(1000)
            return False

        # ── Steps 4-7: Vuelo→Equipaje→Asientos→Extras→Pasajeros ─────────
        # JetSMART is a SPA — URL never changes between steps, so we detect progress by
        # waiting for "Selecciona el Vuelo" to disappear (means we left the Vuelo step)
        # and then clicking Continuar for each subsequent step.
        jetsmart_steps = ["equipaje", "asientos", "extras", "pasajeros"]
        for step_label in jetsmart_steps:
            logger.debug("JetSMART: clicking Continuar for step '%s'", step_label)
            # Dismiss any Club modal before clicking Continuar
            await _dismiss_club_modal()
            advanced = await _click_continuar()
            # After advancing from Vuelo, wait for flight-selection content to clear
            if step_label == "equipaje":
                try:
                    await page.wait_for_function(
                        """() => !document.body.innerText.includes('Selecciona el Vuelo')""",
                        timeout=8000,
                    )
                except Exception:
                    pass  # proceed anyway
            await page.wait_for_timeout(1500)
            logger.debug("JetSMART step %s: advanced=%s url=%s", step_label, advanced, page.url[-60:])
        step = "continued_to_pasajeros"

        # ── Step 8: Fill Pasajeros form ───────────────────────────────────
        await page.wait_for_timeout(2000)
        given_name = str(pax.get("given_name") or "Test")
        family_name = str(pax.get("family_name") or "Traveler")
        email_val = "test@example.com"
        for sel, val in [
            ("input[placeholder*='ombre'], input[name*='ombre'], input[name*='first']", given_name),
            ("input[placeholder*='pellido'], input[name*='pellido'], input[name*='last']", family_name),
            ("input[type='email'], input[placeholder*='mail']", email_val),
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible(timeout=1000):
                    await el.fill(val)
            except Exception:
                pass
        await page.wait_for_timeout(500)

        # ── Step 9: Continuar to Pago ─────────────────────────────────────
        await _click_continuar()
        await page.wait_for_timeout(3000)
        step = "passengers_filled"

        # ── Step 10: Extract details and return ───────────────────────────
        screenshot = await take_screenshot_b64(page)
        try:
            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_generic_visible_checkout_details(page, config, default_currency=offer.get("currency", "PEN")),
            )
        except Exception:
            pass
        final_snapshot = await self._snapshot_checkout_page(page)
        final_page = self._infer_checkout_page(captured_details, final_snapshot)
        if final_page:
            captured_details["checkout_page"] = final_page
        elapsed = time.monotonic() - t0
        page_price = float(offer.get("price", 0.0) or 0.0)
        display_total = captured_details.get("display_total")
        if isinstance(display_total, dict) and isinstance(display_total.get("amount"), (int, float)):
            page_price = float(display_total["amount"])

        if final_page == "payment":
            return CheckoutProgress(
                status="payment_page_reached",
                step="payment_page_reached",
                step_index=8,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                total_price=page_price,
                currency=offer.get("currency", "PEN"),
                booking_url=page.url or booking_url,
                screenshot_b64=screenshot,
                message=f"JetSMART checkout reached payment in {elapsed:.0f}s. Price: {page_price} {offer.get('currency', 'PEN')}.",
                can_complete_manually=True,
                elapsed_seconds=elapsed,
                details=captured_details,
            )
        return CheckoutProgress(
            status="in_progress",
            step=step,
            step_index=CHECKOUT_STEPS.index(step) if step in CHECKOUT_STEPS else 0,
            airline=config.airline_name,
            source=config.source_tag,
            offer_id=offer_id,
            total_price=page_price,
            currency=offer.get("currency", "PEN"),
            booking_url=final_snapshot.get("current_url") or page.url or booking_url,
            screenshot_b64=screenshot,
            message=(
                f"JetSMART checkout reached '{step}'. Surface: '{final_page or 'unknown'}'. "
                f"Price: {page_price} {offer.get('currency', 'PEN')}."
            ),
            can_complete_manually=True,
            elapsed_seconds=elapsed,
            details=self._merge_checkout_details(captured_details, {
                "blocker": "payment_page_not_reached",
                "checkout_page": final_page or "unknown",
                "current_url": final_snapshot.get("current_url") or page.url or booking_url,
            }),
        )

    async def _prepare_volaris_checkout_results(self, page, offer: dict) -> None:
        try:
            current_url = page.url.lower()
        except Exception:
            return

        if "volaris.com" not in current_url:
            return

        segments = ((offer.get("outbound") or {}).get("segments") or []) if isinstance(offer.get("outbound"), dict) else []
        if not segments:
            return

        origin = str(segments[0].get("origin") or "").strip().upper()
        destination = str(segments[-1].get("destination") or "").strip().upper()
        departure_value = str(segments[0].get("departure") or "").strip()
        departure_date = departure_value.split("T", 1)[0] if departure_value else ""
        if not origin or not destination or not departure_date:
            return

        try:
            target_date = datetime.strptime(departure_date, "%Y-%m-%d").date()
        except ValueError:
            return

        logger.info("Volaris checkout: replaying proven homepage search for %s→%s", origin, destination)

        try:
            await page.goto("https://www.volaris.com/es-mx", wait_until="domcontentloaded", timeout=45000)
        except Exception:
            return

        from .volaris import VolarisConnectorClient

        helper = VolarisConnectorClient(timeout=60.0)
        await helper._dismiss_cookies(page)
        await page.wait_for_timeout(500)
        await helper._dismiss_cookies(page)

        await helper._set_one_way(page)
        await page.wait_for_timeout(500)

        if not await helper._fill_airport_field(page, "From", "Desde", origin, 0):
            return
        await page.wait_for_timeout(500)

        if not await helper._fill_airport_field(page, "To", "A", destination, 1):
            return
        await page.wait_for_timeout(500)

        request_stub = type("_VolarisRequest", (), {"date_from": target_date})()
        if not await helper._fill_date(page, request_stub):
            return
        await page.wait_for_timeout(300)

        await helper._click_search(page)
        await page.wait_for_timeout(4000)

    async def _fill_volaris_airport(self, page, selector: str, iata: str, *, desc: str) -> bool:
        if not selector or not iata:
            return False
        try:
            field = page.locator(selector).first
            if await field.count() == 0:
                return False
            await field.click(timeout=3000)
            await page.wait_for_timeout(200)
            try:
                await field.fill("")
            except Exception:
                await page.keyboard.press("Control+A")
                await page.keyboard.press("Backspace")
            await page.keyboard.type(iata, delay=80)
            await page.wait_for_timeout(1200)

            exact_option = page.locator(f"[role='option']:has([data-att='{iata.upper()}'])").first
            if await exact_option.count() > 0:
                await exact_option.click(timeout=3000)
                await page.wait_for_timeout(300)
                return True

            partial_option = page.get_by_role("option").filter(
                has_text=re.compile(re.escape(iata), re.IGNORECASE)
            ).first
            if await partial_option.count() > 0:
                await partial_option.click(timeout=3000)
                await page.wait_for_timeout(300)
                return True

            await page.keyboard.press("ArrowDown")
            await page.wait_for_timeout(150)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(300)
            return True
        except Exception as exc:
            logger.debug("Volaris checkout: failed to fill %s airport %s: %s", desc, iata, exc)
            return False

    async def _select_volaris_date(self, page, target_date) -> bool:
        day_str = target_date.strftime("%d/%m/%Y")
        if not await safe_click_first(
            page,
            [
                "#date-input-11-trigger",
                "button[id^='date-input-'][id$='-trigger']",
                "[aria-label='fc-booking-departure-date-aria-label']",
            ],
            timeout=3000,
            desc="Volaris departure date",
        ):
            return False
        await page.wait_for_timeout(500)

        for _ in range(12):
            day_button = page.locator(f"[role='gridcell'][aria-label*='{day_str}']").first
            if await day_button.count() > 0:
                await day_button.click(timeout=3000)
                await page.wait_for_timeout(300)
                await safe_click_first(
                    page,
                    ["button:has-text('Confirmar')", "button:has-text('Confirm')"],
                    timeout=2000,
                    desc="Volaris confirm date",
                )
                await page.wait_for_timeout(300)
                return True
            if not await safe_click_first(
                page,
                [
                    "[aria-label='fc-booking-date-selector-next-month']",
                    "[aria-label*='next-month']",
                    "button[aria-label*='Next month']",
                ],
                timeout=1500,
                desc="Volaris next month",
            ):
                break
            await page.wait_for_timeout(300)
        return False

    async def _click_volaris_visible_text(self, page, texts: list[str]) -> bool:
        if not texts:
            return False
        try:
            clicked = await page.evaluate(
                """(targets) => {
                    const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    const visible = (el) => {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                    };
                    const nodes = Array.from(document.querySelectorAll(
                        'button, a, label, mat-radio-button, .mat-radio-label, [role="button"], span, div'
                    ));
                    for (const target of targets) {
                        const wanted = normalize(target);
                        for (const node of nodes) {
                            const text = normalize(node.innerText || node.textContent || '');
                            if (!text || !text.includes(wanted) || !visible(node)) {
                                continue;
                            }
                            node.scrollIntoView({ block: 'center', inline: 'center' });
                            if (typeof node.click === 'function') {
                                node.click();
                            } else {
                                node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                            }
                            return true;
                        }
                    }
                    return false;
                }""",
                texts,
            )
        except Exception:
            return False
        if clicked:
            await page.wait_for_timeout(600)
            return True
        return False

    async def _click_volaris_first_visible(self, page, selectors: list[str], *, timeout: int = 3000, desc: str = "") -> bool:
        if not selectors:
            return False
        deadline = time.monotonic() + max(timeout, 250) / 1000.0
        last_error = None
        while time.monotonic() < deadline:
            for selector in selectors:
                locator = page.locator(selector)
                try:
                    count = await locator.count()
                except Exception as exc:
                    last_error = exc
                    continue
                for index in range(count):
                    candidate = locator.nth(index)
                    try:
                        if not await candidate.is_visible():
                            continue
                        await candidate.scroll_into_view_if_needed()
                        await page.wait_for_timeout(150)
                        try:
                            await candidate.click(timeout=1000)
                        except Exception:
                            handle = await candidate.element_handle()
                            if handle is None:
                                raise
                            await page.evaluate(
                                """(element) => {
                                    if (typeof element.click === 'function') {
                                        element.click();
                                        return;
                                    }
                                    element.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                                }""",
                                handle,
                            )
                        await page.wait_for_timeout(500)
                        logger.debug("Volaris checkout: clicked %s via %s", desc or "selector", selector)
                        return True
                    except Exception as exc:
                        last_error = exc
                        continue
            await page.wait_for_timeout(200)
        logger.debug("Volaris checkout: no visible selector matched for %s: %s", desc or "selector", last_error)
        return False

    async def _click_volaris_fare_select(self, page) -> bool:
        if await self._click_volaris_first_visible(
            page,
            [
                ".flightItem.faresVisibleBox button.btn-select",
                ".flightItem.faresVisibleBox button:has-text('Seleccionar')",
                ".flightItem.faresVisibleBox .mat-button-wrapper:has-text('Seleccionar')",
                ".faresVisibleBox mbs-flight-fares button.btn-select",
                ".faresVisibleBox button:has-text('Seleccionar')",
                "mbs-flight-fares button.btn-select",
            ],
            timeout=4000,
            desc="Volaris fare select",
        ):
            return True
        return await self._click_volaris_visible_text(page, ["Seleccionar"])

    async def _open_volaris_flight(self, page, dep_label: str, flight_numbers: list[str] | None = None) -> bool:
        if not dep_label and not flight_numbers:
            return False
        try:
            clicked = await page.evaluate(
                """({ depTime, flightNos }) => {
                    const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    const compact = (value) => normalize(value).replace(/\s+/g, '');
                    const items = Array.from(document.querySelectorAll('.flightItem'));
                    let best = null;
                    for (const item of items) {
                        const text = normalize(item.innerText || item.textContent || '');
                        const textCompact = compact(text);
                        let score = 0;
                        if (depTime && text.includes(normalize(depTime))) {
                            score += 2;
                        }
                        for (const flightNo of flightNos || []) {
                            const wanted = compact(flightNo);
                            if (!wanted) {
                                continue;
                            }
                            if (textCompact.includes(wanted)) {
                                score += 1;
                            }
                        }
                        if (!score) {
                            continue;
                        }
                        if (!best || score > best.score) {
                            best = { item, score };
                        }
                    }
                    if (!best) {
                        return false;
                    }
                    const opener = best.item.querySelector('a.panel-open, [role="button"].panel-open');
                    if (!opener) {
                        return false;
                    }
                    opener.scrollIntoView({ block: 'center', inline: 'center' });
                    opener.click();
                    return true;
                }""",
                {"depTime": dep_label, "flightNos": flight_numbers or []},
            )
        except Exception:
            return False
        if clicked:
            await page.wait_for_timeout(1000)
            return True
        return False

    async def _open_volaris_flight_by_time(self, page, dep_label: str) -> bool:
        return await self._open_volaris_flight(page, dep_label, [])

    async def _volaris_checkout(self, page, config, offer, offer_id, booking_url, passengers, t0):
        step = "started"
        captured_details: dict[str, Any] = {}

        try:
            logger.info("%s checkout: navigating to %s", config.airline_name, booking_url)
            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
            except Exception as nav_err:
                logger.warning("%s checkout: goto error (%s) — continuing", config.airline_name, str(nav_err)[:100])
            await page.wait_for_timeout(3000)
            await self._dismiss_cookies(page, config)

            if booking_url.split("?")[0] not in page.url:
                logger.warning("%s checkout: page redirected to %s — retrying", config.airline_name, page.url[:120])
                try:
                    await page.goto(booking_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
                except Exception:
                    pass
                await page.wait_for_timeout(3000)
                await self._dismiss_cookies(page, config)

            await self._prepare_volaris_checkout_results(page, offer)
            try:
                await page.wait_for_selector(config.flight_cards_selector, timeout=config.flight_cards_timeout)
            except Exception:
                pass
            settle_deadline = time.monotonic() + 10
            while time.monotonic() < settle_deadline:
                try:
                    if "/flight" in page.url.lower() and await page.locator(config.flight_cards_selector).count() > 0:
                        break
                except Exception:
                    pass
                await page.wait_for_timeout(500)
            await self._dismiss_cookies(page, config)

            outbound = offer.get("outbound", {}) if isinstance(offer.get("outbound"), dict) else {}
            segments = outbound.get("segments", []) if isinstance(outbound, dict) else []
            dep_label = ""
            flight_numbers: list[str] = []
            if segments:
                dep_value = str(segments[0].get("departure") or "")
                if dep_value:
                    try:
                        dep_dt = datetime.fromisoformat(dep_value.replace("Z", "+00:00"))
                        dep_label = dep_dt.strftime("%I:%M %p").lstrip("0")
                    except Exception:
                        dep_label = ""
                for segment in segments:
                    flight_no = str(segment.get("flight_no") or "").strip().upper()
                    if not flight_no:
                        continue
                    flight_numbers.append(flight_no)
                    compact_match = re.match(r"^([A-Z]{2})(\d+)$", flight_no)
                    if compact_match:
                        flight_numbers.append(f"{compact_match.group(1)} {compact_match.group(2)}")

            opened = await self._open_volaris_flight(page, dep_label, flight_numbers)
            if not opened:
                await self._click_volaris_first_visible(
                    page,
                    config.first_flight_selectors,
                    timeout=3000,
                    desc="Volaris first flight",
                )
                await page.wait_for_timeout(1000)
            step = "flights_selected"

            await self._click_volaris_fare_select(page)
            await page.wait_for_timeout(2000)
            await self._click_volaris_first_visible(
                page,
                [
                    "button:has-text('Mantener Zero')",
                    "button:has-text('Mantener Básica')",
                    "button:has-text('Keep Zero')",
                    "button:has-text('Keep Basic')",
                ],
                timeout=4000,
                desc="Volaris keep fare",
            )
            await page.wait_for_timeout(2500)
            step = "fare_selected"

            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_checkout_details(page, config, offer.get("currency", "EUR")),
            )

            await self._click_volaris_first_visible(
                page,
                [
                    "label[for='mat-radio-2-input']",
                    "mat-radio-button[value='payNow'] .mat-radio-label",
                    ".mat-radio-label:has-text('Paga ahora')",
                    "label:has-text('Paga ahora')",
                    "label:has-text('Paga después')",
                ],
                timeout=4000,
                desc="Volaris TUA choice",
            )
            await page.wait_for_timeout(1000)
            await self._click_volaris_first_visible(
                page,
                ["button:has-text('Continuar sin equipaje')"],
                timeout=4000,
                desc="Volaris skip baggage",
            )
            await page.wait_for_timeout(1200)
            await self._click_volaris_first_visible(
                page,
                ["button:has-text('Continuar a asientos')"],
                timeout=4000,
                desc="Volaris continue to seats",
            )
            await page.wait_for_timeout(5000)
            step = "extras_skipped"

            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_checkout_details(page, config, offer.get("currency", "EUR")),
            )
            seat_items = ((captured_details.get("available_add_ons") or {}).get("seat_selection") or []) if isinstance(captured_details.get("available_add_ons"), dict) else []
            if not seat_items and not captured_details.get("seat_selection_observation"):
                captured_details = self._merge_checkout_details(
                    captured_details,
                    {
                        "seat_selection_observation": "Seat-selection page reached on Volaris, but no seat-specific numeric price surfaced before skipping seats.",
                    },
                )

            await self._click_volaris_first_visible(
                page,
                [
                    "button:has-text('Selecciona asientos después')",
                    "button:has-text('Seleccionar asientos después')",
                ],
                timeout=4000,
                desc="Volaris skip seats",
            )
            await page.wait_for_timeout(2000)
            await self._click_volaris_first_visible(
                page,
                [
                    "button:has-text('Continúa a pagos')",
                    "button:has-text('Continua a pagos')",
                    "button:has-text('Continuar a pagos')",
                ],
                timeout=4000,
                desc="Volaris continue payment",
            )
            await page.wait_for_timeout(4000)
            step = "seats_skipped"

            captured_details = self._merge_checkout_details(
                captured_details,
                await self._extract_checkout_details(page, config, offer.get("currency", "EUR")),
            )

            screenshot = await take_screenshot_b64(page)
            final_snapshot = await self._snapshot_checkout_page(page)
            final_checkout_page = self._infer_checkout_page(captured_details, final_snapshot)
            body_text = ""
            try:
                body_text = await page.evaluate("() => (document.body?.innerText || '')")
            except Exception:
                body_text = ""

            if "volaris.com/payment" in page.url.lower() or re.search(
                r"informaci[oó]n de pasajero y pago|pagar mi viaje|formas de pago",
                body_text,
                re.IGNORECASE,
            ):
                final_checkout_page = "payment"

            if final_checkout_page:
                captured_details = self._merge_checkout_details(
                    captured_details,
                    {"checkout_page": final_checkout_page},
                )

            page_price = offer.get("price", 0.0)
            display_total = captured_details.get("display_total")
            if isinstance(display_total, dict) and isinstance(display_total.get("amount"), (int, float)):
                page_price = float(display_total["amount"])

            elapsed = time.monotonic() - t0
            current_url = final_snapshot.get("current_url") or booking_url
            if final_checkout_page != "payment":
                blocker_details = self._merge_checkout_details(
                    captured_details,
                    {
                        "blocker": "payment_page_not_reached",
                        "checkout_page": final_checkout_page or "unknown",
                        "current_url": current_url,
                        "page_title": final_snapshot.get("page_title") or "",
                    },
                )
                step = self._checkout_step_for_page(final_checkout_page)
                return CheckoutProgress(
                    status="in_progress",
                    step=step,
                    step_index=CHECKOUT_STEPS.index(step) if step in CHECKOUT_STEPS else 0,
                    airline=config.airline_name,
                    source=config.source_tag,
                    offer_id=offer_id,
                    total_price=page_price,
                    currency=offer.get("currency", "EUR"),
                    booking_url=current_url,
                    screenshot_b64=screenshot,
                    message=(
                        f"{config.airline_name} checkout did not reach payment. "
                        f"Current surface looks like '{(final_checkout_page or 'unknown').replace('_', ' ')}'. "
                        f"Visible price: {page_price} {offer.get('currency', 'EUR')}."
                    ),
                    can_complete_manually=bool(current_url),
                    elapsed_seconds=elapsed,
                    details=blocker_details,
                )

            return CheckoutProgress(
                status="payment_page_reached",
                step="payment_page_reached",
                step_index=8,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                total_price=page_price,
                currency=offer.get("currency", "EUR"),
                booking_url=current_url,
                screenshot_b64=screenshot,
                message=(
                    f"{config.airline_name} checkout complete — reached payment page in {elapsed:.0f}s. "
                    f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                    f"Payment NOT submitted (safe mode). "
                    f"Complete manually at: {current_url}"
                ),
                can_complete_manually=bool(current_url),
                elapsed_seconds=elapsed,
                details=captured_details,
            )
        except Exception as exc:
            screenshot = await take_screenshot_b64(page)
            return CheckoutProgress(
                status="failed",
                step=step,
                step_index=CHECKOUT_STEPS.index(step) if step in CHECKOUT_STEPS else 0,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                total_price=offer.get("price", 0.0),
                currency=offer.get("currency", "EUR"),
                booking_url=page.url or booking_url,
                screenshot_b64=screenshot,
                message=f"{config.airline_name} checkout failed: {exc}",
                can_complete_manually=bool(page.url or booking_url),
                elapsed_seconds=time.monotonic() - t0,
                details=self._merge_checkout_details(
                    captured_details,
                    {
                        "blocker": "volaris_checkout_exception",
                        "checkout_page": "unknown",
                        "current_url": page.url or booking_url,
                        "page_title": await page.title() if page else "",
                    },
                ),
            )

    async def _click_aireuropa_inline_fare(self, page) -> bool:
        try:
            result = await page.evaluate(
                """() => {
                    const visible = (el) => {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style && style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                    };
                    const textOf = (el) => ((el && (el.innerText || el.textContent)) || '').replace(/\s+/g, ' ').trim();
                    const clickElement = (el) => {
                        if (!el) return false;
                        if (typeof el.click === 'function') {
                            el.click();
                            return true;
                        }
                        el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                        return true;
                    };

                    const blocks = Array.from(document.querySelectorAll('div, section, article, li, td'))
                        .filter((el) => {
                            const text = textOf(el);
                            return visible(el)
                                && /economy|business/i.test(text)
                                && /\bfrom\b/i.test(text)
                                && /(eur|pln|\$|€|£)/i.test(text);
                        })
                        .sort((a, b) => {
                            const ar = a.getBoundingClientRect();
                            const br = b.getBoundingClientRect();
                            if (Math.abs(ar.top - br.top) > 4) return ar.top - br.top;
                            if (Math.abs(ar.left - br.left) > 4) return ar.left - br.left;
                            return (ar.width * ar.height) - (br.width * br.height);
                        });

                    const controlSelectors = [
                        'mat-radio-button',
                        '[role="radio"]',
                        'mat-checkbox',
                        '[role="checkbox"]',
                        'label',
                        'button',
                        'input[type="radio"]',
                        'input[type="checkbox"]',
                    ];

                    for (const block of blocks) {
                        const blockText = textOf(block);
                        if (!/economy|lite/i.test(blockText)) {
                            continue;
                        }
                        for (const selector of controlSelectors) {
                            const control = Array.from(block.querySelectorAll(selector)).find(visible);
                            if (control && clickElement(control)) {
                                return { clicked: true, via: selector, text: blockText.slice(0, 240) };
                            }
                        }
                        const inlineCell = Array.from(block.querySelectorAll('div, span, td')).find((el) => {
                            const text = textOf(el);
                            return visible(el) && /economy|lite/i.test(text) && /\bfrom\b/i.test(text);
                        });
                        if (inlineCell && clickElement(inlineCell)) {
                            return { clicked: true, via: 'inline-cell', text: (textOf(inlineCell) || blockText).slice(0, 240) };
                        }
                        if (clickElement(block)) {
                            return { clicked: true, via: 'block', text: blockText.slice(0, 240) };
                        }
                    }
                    return { clicked: false };
                }"""
            )
        except Exception as exc:
            logger.debug("Air Europa checkout: inline fare click failed: %s", exc)
            return False

        if result and result.get("clicked"):
            logger.info(
                "Air Europa checkout: clicked inline fare via %s (%s)",
                result.get("via") or "unknown",
                result.get("text") or "",
            )
            await page.wait_for_timeout(1500)
            return True
        return False

    async def _click_aircairo_fare(self, page) -> bool:
        try:
            await safe_click_first(
                page,
                [
                    "button.flight-card-button",
                    "button[class*='flight-card-button']",
                    "button:has-text('Economy from')",
                    "button:has-text('4 seats left')",
                ],
                timeout=2500,
                desc="Air Cairo flight option",
            )
            await page.wait_for_timeout(1500)

            fare_label = ""
            fare_choices = [
                (
                    "SUPER SAVER",
                    [
                        "label.price-card-input-label:has-text('SUPER SAVER')",
                        ".fare-family-SUPERSS label.price-card-input-label",
                        ".fare-family-SUPERSS mat-card.price-card",
                    ],
                ),
                (
                    "PROMOTIONAL",
                    [
                        "label.price-card-input-label:has-text('PROMOTIONAL')",
                        "mat-card.price-card:has-text('PROMOTIONAL')",
                    ],
                ),
                (
                    "SPECIAL",
                    [
                        "label.price-card-input-label:has-text('SPECIAL')",
                        "mat-card.price-card:has-text('SPECIAL')",
                    ],
                ),
                (
                    "ECONOMY",
                    [
                        "label.price-card-input-label:has-text('ECONOMY')",
                        "mat-card.price-card:has-text('ECONOMY')",
                    ],
                ),
                (
                    "ECO FLEX",
                    [
                        "label.price-card-input-label:has-text('ECO FLEX')",
                        "mat-card.price-card:has-text('ECO FLEX')",
                    ],
                ),
            ]

            for label, selectors in fare_choices:
                if await safe_click_first(page, selectors, timeout=2000, desc=f"Air Cairo fare {label}"):
                    fare_label = label
                    break

            if fare_label:
                logger.info("Air Cairo checkout: selected fare family %s", fare_label)
                await page.wait_for_timeout(1200)
                confirm_clicked = await safe_click_first(
                    page,
                    [
                        "button:has-text('Confirm and continue')",
                        "button[aria-label='Confirm and continue']",
                        "button:has-text('Continue')",
                    ],
                    timeout=2500,
                    desc="Air Cairo confirm fare",
                )
                if not confirm_clicked:
                    return False

                await page.wait_for_timeout(1200)

                # Air Cairo may interrupt the selected fare with an upgrade modal.
                await safe_click_first(
                    page,
                    [
                        "button:has-text('Keep Super Saver')",
                        "button:has-text('Keep Promotional')",
                        "button:has-text('Keep Special')",
                        "button:has-text('Keep Economy')",
                        "button:has-text('Keep Eco Flex')",
                        "button:has-text('Keep')",
                        "button:has-text('No thanks')",
                    ],
                    timeout=3000,
                    desc="Air Cairo keep current fare",
                )

                try:
                    await page.wait_for_function(
                        "() => !window.location.pathname.includes('/availability/') || document.body.innerText.includes('Your selection')",
                        timeout=12000,
                    )
                except Exception:
                    pass

                page_text = ""
                try:
                    page_text = await page.locator("body").inner_text(timeout=2000)
                except Exception:
                    pass

                return "/availability/" not in page.url or "Your selection" in page_text or "Fill passenger details" in page_text
            return False
        except Exception:
            return False

    async def _extract_checkout_details(self, page, config: AirlineCheckoutConfig, default_currency: str = "EUR") -> dict:
        if not config.details_extractor_handler:
            return {}
        handler = getattr(self, config.details_extractor_handler, None)
        if handler is None:
            logger.debug("%s checkout: details extractor '%s' not found", config.airline_name, config.details_extractor_handler)
            return {}
        try:
            return await handler(page, config, default_currency=default_currency)
        except Exception as exc:
            logger.debug("%s checkout: details extractor '%s' failed: %s", config.airline_name, config.details_extractor_handler, exc)
            return {}

    async def _extract_aircairo_checkout_details(self, page, config: AirlineCheckoutConfig, default_currency: str = "EUR") -> dict:
        details = await self._extract_generic_visible_checkout_details(page, config, default_currency=default_currency)
        checkout_page = str(details.get("checkout_page") or "").lower()
        current_url = (page.url or "").lower()

        if checkout_page == "select_flight":
            available_add_ons = details.get("available_add_ons") if isinstance(details.get("available_add_ons"), dict) else {}
            if isinstance(available_add_ons.get("seat_selection"), list):
                available_add_ons = dict(available_add_ons)
                available_add_ons.pop("seat_selection", None)
                if available_add_ons:
                    details["available_add_ons"] = available_add_ons
                else:
                    details.pop("available_add_ons", None)

            price_breakdown = details.get("price_breakdown") if isinstance(details.get("price_breakdown"), list) else []
            filtered_breakdown = [item for item in price_breakdown if item.get("type") != "seat_selection"]
            if filtered_breakdown:
                details["price_breakdown"] = filtered_breakdown
            else:
                details.pop("price_breakdown", None)
            return details

        if checkout_page == "extras" or "/booking/shopping-cart" in current_url:
            details = self._merge_checkout_details(
                details,
                await self._extract_aircairo_seat_samples(page, default_currency=default_currency),
            )
        elif checkout_page == "seats" or "/booking/seatmap/" in current_url:
            details = self._merge_checkout_details(
                details,
                await self._extract_aircairo_seat_prices(page, default_currency=default_currency),
            )

        return details

    async def _extract_aircairo_seat_samples(self, page, default_currency: str = "EUR") -> dict:
        current_url = (page.url or "").lower()
        page_text = ""
        try:
            page_text = (await page.locator("body").inner_text(timeout=2000)).lower()
        except Exception:
            page_text = ""

        opened_seatmap = False
        on_seatmap = "/booking/seatmap/" in current_url or "select your seat" in page_text or "seat map" in page_text
        if not on_seatmap:
            if not await safe_click_first(page, ["button:has-text('Select your seats')"], timeout=2000, desc="Air Cairo seat map"):
                return {}
            opened_seatmap = True
            try:
                await page.wait_for_function(
                    "() => window.location.pathname.includes('/seatmap/') || /select your seat|seat map/i.test(document.body.innerText || '')",
                    timeout=12000,
                )
            except Exception:
                await page.wait_for_timeout(1500)

        seat_details = await self._extract_aircairo_seat_prices(page, default_currency=default_currency)

        if opened_seatmap:
            await safe_click_first(page, ["button:has-text('Back')"], timeout=1500, desc="Air Cairo back from seats")
            await page.wait_for_timeout(800)

        return seat_details

    async def _extract_aircairo_seat_prices(self, page, default_currency: str = "EUR") -> dict:
        sample_labels = ["Seat 1A", "Seat 2A", "Seat 2B"]
        sampled_prices: list[dict] = []

        for seat_label in sample_labels:
            try:
                seat_meta = await page.evaluate(
                    r'''(targetSeat) => {
                        const seat = Array.from(document.querySelectorAll('button.seat-button')).find((el) => (el.textContent || '').includes(targetSeat));
                        if (!seat) return null;
                        const seatText = (seat.textContent || '').replace(/\s+/g, ' ').trim();
                        seat.click();
                        return { seatText };
                    }''',
                    seat_label,
                )
            except Exception:
                seat_meta = None

            if not isinstance(seat_meta, dict):
                continue

            await page.wait_for_timeout(700)

            try:
                body_text = await page.locator("body").inner_text(timeout=2000)
            except Exception:
                continue

            normalized_body = re.sub(r"\s+", " ", body_text)
            match = re.search(rf"{re.escape(seat_label)}\s+([A-Z]{{3}})\s*([\d,.]+)", normalized_body)
            if not match:
                continue

            amount = None
            try:
                amount = float(match.group(2).replace(",", ""))
            except ValueError:
                amount = None
            if amount is None:
                continue

            descriptor = re.sub(r"\s+", " ", str(seat_meta.get("seatText") or "")).strip()
            descriptor = descriptor.replace(seat_label, "", 1)
            descriptor = descriptor.replace("Press enter to get more details.", "")
            descriptor = descriptor.strip(" ,.-")
            label = f"{seat_label} - {descriptor}" if descriptor else seat_label

            sampled_prices.append(
                {
                    "label": label,
                    "text": f"{seat_label} {match.group(1)}{amount:.2f}",
                    "currency": match.group(1).upper() or (default_currency or "EUR"),
                    "amount": amount,
                    "included": False,
                    "type": "seat_selection",
                }
            )

        if not sampled_prices:
            return {
                "seat_selection_observation": "Seat map reached, but Air Cairo seat detail prices were not readable from the current surface.",
            }

        return {
            "available_add_ons": {
                "seat_selection": sampled_prices,
            },
            "price_breakdown": sampled_prices,
            "seat_selection_observation": "Sampled Air Cairo seat-map seat prices directly from the visible seat detail panels.",
        }

    async def _extract_airasia_checkout_details(self, page, config: AirlineCheckoutConfig, default_currency: str = "EUR") -> dict:
        return await page.evaluate(
            r'''(defaultCurrency) => {
                const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();

                const parseBareAmount = (text) => {
                    const match = normalize(text).match(/^([\d,]+(?:\.\d{1,2})?)$/);
                    if (!match) return null;
                    return Number(match[1].replace(/,/g, ''));
                };

                const dedupeItems = (items, limit = 12) => {
                    const seen = new Set();
                    const deduped = [];
                    for (const item of Array.isArray(items) ? items : []) {
                        if (!item || typeof item !== 'object') continue;
                        const amount = typeof item.amount === 'number' ? item.amount : '';
                        const key = [
                            normalize(item.label),
                            normalize(item.type),
                            normalize(item.currency),
                            amount,
                            Boolean(item.included),
                        ].join('|');
                        if (seen.has(key)) continue;
                        seen.add(key);
                        deduped.push(item);
                        if (deduped.length >= limit) break;
                    }
                    return deduped;
                };

                const parseMoney = (text) => {
                    const match = normalize(text).match(/([A-Z]{3})\s*([\d,]+(?:\.\d{1,2})?)/);
                    if (!match) return null;
                    return {
                        currency: match[1],
                        amount: Number(match[2].replace(/,/g, '')),
                    };
                };

                const hasPositiveMoney = (money) => Boolean(
                    money
                    && typeof money.amount === 'number'
                    && Number.isFinite(money.amount)
                    && money.amount > 0
                );

                const isVisible = (element) => {
                    if (!element || !(element instanceof Element)) return false;
                    const style = window.getComputedStyle(element);
                    if (!style || style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                        return false;
                    }
                    const rect = element.getBoundingClientRect();
                    return rect.width > 0 && rect.height > 0;
                };

                const cleanLabel = (text) => normalize(
                    String(text || '')
                        .replace(/\(Included\)/gi, ' ')
                        .replace(/View All Benefits/gi, ' ')
                        .replace(/Pre-book for the lowest price/gi, ' ')
                        .replace(/([A-Z]{3})\s*[\d,]+(?:\.\d{1,2})?/g, ' ')
                );

                const result = {};
                const bodyText = document.body?.innerText || '';
                const pageSignals = normalize(`${document.title || ''} ${location.pathname || ''} ${location.href || ''} ${bodyText}`);
                const bookingWidgetControl = Array.from(document.querySelectorAll('input, select, button')).find((el) => {
                    if (!isVisible(el)) return false;
                    const controlSignals = normalize(`${el.getAttribute('name') || ''} ${el.getAttribute('id') || ''} ${el.getAttribute('placeholder') || ''} ${el.textContent || ''}`);
                    return /departurefrom|departureto|triptype|promo|promotional code|book a flight|departure date|return date/.test(controlSignals);
                });
                const bookingLandingSurface = /\bbook a flight\b/i.test(document.title || '')
                    && /\/book-flight\b/i.test(location.pathname || '');
                const homeSearchSurface = bookingLandingSurface
                    || Boolean(bookingWidgetControl)
                    || /search flights|flight deals|manage your reservation|before flying|promotional code|trip type|book a flight|plan & book|plan and book/i.test(pageSignals);
                const paymentControl = Array.from(document.querySelectorAll("input, iframe, [data-testid], [name], [autocomplete]"))
                    .find((el) => {
                        if (!isVisible(el)) return false;
                        const text = normalize(`${el.getAttribute('name') || ''} ${el.getAttribute('autocomplete') || ''} ${el.getAttribute('data-testid') || ''} ${el.getAttribute('title') || ''} ${el.getAttribute('src') || ''}`);
                        return /card|cc-number|payment|cvv|security code|billing/i.test(text);
                    });
                if (!homeSearchSurface && ((paymentControl && isVisible(paymentControl)) || /review and pay|review & pay|pay now|secure payment|billing address|card number|cvv|security code/i.test(pageSignals))) {
                    result.checkout_page = 'payment';
                } else if (homeSearchSurface) {
                    result.checkout_page = 'select_flight';
                } else if (!homeSearchSurface && /seat map|pick a seat|select your seat|seat selection|choose your seats|quiet zone|hot seat|extra legroom|flatbed/i.test(pageSignals)) {
                    result.checkout_page = 'seats';
                } else if (/guest details|passenger|traveller details|contact details|customer details/i.test(pageSignals)) {
                    result.checkout_page = 'guest_details';
                } else if (/baggage|checked bag|checked baggage|extra bag|carry-on|carry on|insurance|meal|food and drink|priority boarding|fast pass|add extras|choose extras|bundle|package|upgrade/i.test(pageSignals)) {
                    result.checkout_page = 'extras';
                }

                const bodyLines = bodyText
                    .split(/\n+/)
                    .map(normalize)
                    .filter(Boolean);

                const parseSummaryItems = (root) => {
                    if (!root) return [];
                    const lines = (root.innerText || '')
                        .split(/\n+/)
                        .map(normalize)
                        .filter(Boolean)
                        .filter((line) => !/^fare summary$/i.test(line));
                    const items = [];
                    let pendingLabel = '';
                    for (let index = 0; index < lines.length; index += 1) {
                        const line = lines[index];
                        let moneyText = null;

                        if (/^[A-Z]{3}$/i.test(line) && index + 1 < lines.length && /^[\d,]+(?:\.\d{1,2})?$/.test(lines[index + 1])) {
                            moneyText = `${line} ${lines[index + 1]}`;
                            index += 1;
                        } else if (parseMoney(line)) {
                            moneyText = line;
                        }

                        if (moneyText && pendingLabel) {
                            const money = parseMoney(moneyText);
                            if (money) {
                                items.push({
                                    label: pendingLabel,
                                    currency: money.currency,
                                    amount: money.amount,
                                });
                            }
                            pendingLabel = '';
                            continue;
                        }

                        if (!moneyText) {
                            pendingLabel = pendingLabel ? normalize(`${pendingLabel} ${line}`) : line;
                        }
                    }
                    return items;
                };

                const fareHeading = Array.from(document.querySelectorAll('*')).find(
                    (el) => normalize(el.textContent) === 'Fare summary'
                );
                const summaryContainer = fareHeading?.closest('[class*="Panel__MainWrapper"]')
                    || fareHeading?.parentElement?.parentElement;
                const priceBreakdown = [];
                let displayTotal = null;
                if (summaryContainer) {
                    for (const item of parseSummaryItems(summaryContainer)) {
                        if (/^total amount$/i.test(item.label)) {
                            displayTotal = item;
                        } else {
                            priceBreakdown.push(item);
                        }
                    }
                }
                if (priceBreakdown.length) {
                    result.price_breakdown = priceBreakdown;
                }
                if (displayTotal) {
                    result.display_total = displayTotal;
                }

                const inferredCurrency = displayTotal?.currency
                    || priceBreakdown.find((item) => item?.currency)?.currency
                    || defaultCurrency
                    || 'MYR';

                const parseVisibleMoney = (text, { allowTrailingBare = true } = {}) => {
                    const direct = parseMoney(text);
                    if (direct) return direct;
                    if (!allowTrailingBare) return null;
                    const normalizedText = normalize(text);
                    if (!normalizedText || /^\d+\s*(kg|x|pcs?|pieces?)$/i.test(normalizedText)) {
                        return null;
                    }
                    const trailingMatch = normalizedText.match(/(?:^| )(\d[\d,]*(?:\.\d{1,2})?)$/);
                    if (!trailingMatch) return null;
                    const amount = Number(trailingMatch[1].replace(/,/g, ''));
                    if (!Number.isFinite(amount) || amount <= 0) return null;
                    return {
                        currency: inferredCurrency,
                        amount,
                    };
                };

                const createItem = (label, type, money = null, included = false) => {
                    const item = {
                        label: cleanLabel(label) || normalize(label),
                        included,
                        type,
                    };
                    if (hasPositiveMoney(money)) {
                        item.currency = money.currency || inferredCurrency;
                        item.amount = money.amount;
                    }
                    return item;
                };

                const parsePersistedCheckoutSlice = (name) => {
                    try {
                        const rawStore = sessionStorage.getItem('persist:checkout_app');
                        if (!rawStore) return null;
                        const store = JSON.parse(rawStore);
                        const rawSlice = store?.[name];
                        if (!rawSlice) return null;
                        return JSON.parse(rawSlice);
                    } catch (_error) {
                        return null;
                    }
                };

                const countMoneyTokens = (text) => (
                    normalize(text).match(/[A-Z]{3}\s*[\d,]+(?:\.\d{1,2})?/g) || []
                ).length;

                const baggageTypeFromText = (text) => {
                    const haystack = normalize(text).toLowerCase();
                    if (/checked/.test(haystack)) return 'checked_bag';
                    if (/carry[- ]?on|carry on|cabin/.test(haystack)) return 'cabin_bag';
                    return 'baggage';
                };

                const baggageTypeFromPersistedData = (text) => {
                    const haystack = normalize(text).toLowerCase();
                    if (/checkedbaggage/.test(haystack)) return 'checked_bag';
                    if (/handcarry/.test(haystack)) return 'cabin_bag';
                    return baggageTypeFromText(text);
                };

                const isGenericBaggageHeading = (label) => /^(checked baggage|(carry[- ]?on|cabin) baggage)$/i.test(normalize(label));

                const elementTexts = [];
                for (const element of document.querySelectorAll(
                    'button, label, [role="button"], [role="radio"], [data-test], [data-testid], [class*="summary"], [class*="price"], [class*="option"], [class*="extra"], [class*="seat"], [class*="bag"], [class*="bundle"], [class*="fare"], [class*="insurance"], [class*="meal"], [class*="priority"], [class*="card"]'
                )) {
                    if (!isVisible(element)) continue;
                    const text = normalize(element.innerText || element.textContent);
                    if (!text || text.length > 220) continue;
                    elementTexts.push({
                        text,
                        haystack: normalize(`${element.getAttribute('data-test') || ''} ${element.getAttribute('data-testid') || ''} ${element.className || ''} ${text}`).toLowerCase(),
                    });
                }

                const collectItems = (keywords, type, excludedKeywords = []) => {
                    const items = [];
                    for (const entry of elementTexts) {
                        if (!keywords.some((keyword) => entry.haystack.includes(keyword))) continue;
                        if (excludedKeywords.some((keyword) => entry.haystack.includes(keyword))) continue;
                        const money = parseVisibleMoney(entry.text);
                        const included = /included|free|already selected|no extra cost/i.test(entry.text);
                        if (!hasPositiveMoney(money) && !included) continue;
                        items.push(createItem(entry.text, type, money, included));
                    }
                    return items;
                };

                const collectLineItems = (keywords, type, excludedKeywords = []) => {
                    const items = [];
                    for (const line of bodyLines) {
                        const haystack = line.toLowerCase();
                        if (!keywords.some((keyword) => haystack.includes(keyword))) continue;
                        if (excludedKeywords.some((keyword) => haystack.includes(keyword))) continue;
                        if (countMoneyTokens(line) > 1) continue;
                        const money = parseVisibleMoney(line);
                        const included = /included|free|already selected|no extra cost/i.test(line);
                        if (!hasPositiveMoney(money) && !included) continue;
                        items.push(createItem(line, type, money, included));
                    }
                    return items;
                };

                const collectPairedLineItems = (keywords, type, excludedKeywords = [], inferType = null) => {
                    const items = [];
                    for (let index = 0; index < bodyLines.length; index += 1) {
                        const line = bodyLines[index];
                        const haystack = line.toLowerCase();
                        if (!keywords.some((keyword) => haystack.includes(keyword))) continue;
                        if (excludedKeywords.some((keyword) => haystack.includes(keyword))) continue;
                        if (countMoneyTokens(line) > 1) continue;
                        const included = /included|free|already selected|no extra cost/i.test(line);
                        let money = parseVisibleMoney(line);
                        if (!hasPositiveMoney(money)) {
                            const candidateLines = [bodyLines[index + 1] || '', bodyLines[index + 2] || ''];
                            for (const candidate of candidateLines) {
                                money = parseVisibleMoney(candidate);
                                if (hasPositiveMoney(money)) break;
                                const bareAmount = parseBareAmount(candidate);
                                if (bareAmount !== null) {
                                    money = {
                                        currency: inferredCurrency,
                                        amount: bareAmount,
                                    };
                                    break;
                                }
                            }
                        }
                        if (!hasPositiveMoney(money) && !included) continue;
                        const resolvedType = typeof inferType === 'function' ? inferType(line, haystack) : type;
                        items.push(createItem(line, resolvedType, money, included));
                    }
                    return items;
                };

                const availableAddOns = {};

                const specificInsurance = [];
                const seenInsurance = new Set();
                const insuranceOptions = Array.from(document.querySelectorAll('[role="radio"], [class*="InsuranceContent__RadioBoxesWrapper"]'));
                for (const option of insuranceOptions) {
                    const text = normalize(option.innerText || option.textContent);
                    const money = parseVisibleMoney(text, { allowTrailingBare: false });
                    if (!hasPositiveMoney(money)) continue;
                    let label = normalize((text.match(/^(.*?)([A-Z]{3})\s*[\d,]+(?:\.\d{1,2})?/) || [])[1] || '');
                    label = normalize(label.replace(/View All Benefits/gi, ''));
                    if (!label || seenInsurance.has(label)) continue;
                    seenInsurance.add(label);
                    specificInsurance.push({
                        label,
                        currency: money.currency,
                        amount: money.amount,
                        included: false,
                        type: 'insurance',
                    });
                }

                const baggageHeading = Array.from(document.querySelectorAll('*')).find(
                    (el) => normalize(el.textContent) === 'Baggage'
                );
                const baggageRoot = baggageHeading?.closest('[class*="CardInfo__StyledMainWrapper"]')?.parentElement?.parentElement
                    || baggageHeading?.parentElement?.parentElement;
                const baggageItems = [];
                if (baggageRoot) {
                    const baggageLines = (baggageRoot.innerText || '')
                        .split(/\n+/)
                        .map(normalize)
                        .filter(Boolean)
                        .filter((line) => /baggage/i.test(line))
                        .filter((line) => !/^Baggage$/i.test(line))
                        .filter((line) => !/^checked baggage$/i.test(line))
                        .filter((line) => !/^(carry[- ]?on|cabin) baggage$/i.test(line))
                        .filter((line) => !/\boptions\b/i.test(line))
                        .filter((line) => !/Pre-book for the lowest price/i.test(line));
                    if (baggageLines.length) {
                        baggageItems.push(...baggageLines.map((line) => ({
                            label: normalize(line.replace(/\(Included\)/i, '')),
                            included: /\(Included\)/i.test(line),
                            type: /checked/i.test(line)
                                ? 'checked_bag'
                                : /carry[- ]?on/i.test(line)
                                    ? 'cabin_bag'
                                    : 'baggage',
                        })));
                    }
                }

                const baggageModalOptions = [];
                for (let index = 0; index < bodyLines.length; index += 1) {
                    const line = bodyLines[index];
                    if (!/^\d+\s*kg$/i.test(line)) continue;

                    const candidateLines = [bodyLines[index + 1] || '', bodyLines[index + 2] || ''];
                    let money = null;
                    for (const candidate of candidateLines) {
                        money = parseMoney(candidate);
                        if (money) break;
                        const bareAmount = parseBareAmount(candidate);
                        if (bareAmount !== null) {
                            money = {
                                currency: inferredCurrency,
                                amount: bareAmount,
                            };
                            break;
                        }
                    }
                    if (!money) continue;

                    const nearbyText = bodyLines
                        .slice(Math.max(0, index - 3), Math.min(bodyLines.length, index + 4))
                        .join(' ')
                        .toLowerCase();
                    const type = /carry[- ]?on/i.test(nearbyText) && !/checked baggage/i.test(nearbyText)
                        ? 'cabin_bag'
                        : 'checked_bag';
                    baggageModalOptions.push({
                        label: `${line} ${type === 'cabin_bag' ? 'carry-on baggage' : 'checked baggage'}`,
                        currency: money.currency,
                        amount: money.amount,
                        included: false,
                        type,
                    });
                }

                const persistedAddonSelection = parsePersistedCheckoutSlice('addonSelected');
                const persistedBaggageItems = [];
                const buildPersistedBaggageLabel = (bagOption, baggageGroup) => {
                    const title = Array.isArray(bagOption?.title) ? bagOption.title[0] : null;
                    const weight = normalize(title?.weight || '');
                    const type = baggageTypeFromPersistedData(baggageGroup?.baggageType || '');
                    if (type === 'cabin_bag') {
                        return normalize(`${weight ? `1 x ${weight} ` : ''}Carry-on baggage`);
                    }
                    return normalize(`${weight ? `${weight} ` : ''}checked baggage`);
                };
                const appendPersistedBaggageGroups = (perPassengerGroups) => {
                    if (!Array.isArray(perPassengerGroups)) return;
                    for (const passengerGroups of perPassengerGroups) {
                        if (!Array.isArray(passengerGroups)) continue;
                        for (const baggageGroup of passengerGroups) {
                            if (!baggageGroup || typeof baggageGroup !== 'object') continue;
                            for (const bagOption of Array.isArray(baggageGroup.baggageList) ? baggageGroup.baggageList : []) {
                                if (!bagOption || typeof bagOption !== 'object') continue;
                                const amount = Number(bagOption.amount);
                                const money = Number.isFinite(amount) && amount > 0
                                    ? {
                                        currency: normalize(bagOption.currency) || inferredCurrency,
                                        amount,
                                    }
                                    : null;
                                const included = Boolean(bagOption.isIncluded || (bagOption.isPreSelected && amount === 0));
                                if (!hasPositiveMoney(money) && !included) continue;
                                persistedBaggageItems.push(
                                    createItem(
                                        buildPersistedBaggageLabel(bagOption, baggageGroup),
                                        baggageTypeFromPersistedData(baggageGroup.baggageType || ''),
                                        money,
                                        included,
                                    )
                                );
                            }
                        }
                    }
                };
                appendPersistedBaggageGroups(persistedAddonSelection?.baggage?.departPaxBaggages);
                appendPersistedBaggageGroups(persistedAddonSelection?.baggage?.returnPaxBaggages);

                const baggage = dedupeItems([
                    ...baggageItems,
                    ...baggageModalOptions,
                    ...persistedBaggageItems,
                    ...collectItems(['baggage', 'checked bag', 'checked baggage', 'extra bag', 'carry-on', 'carry on', 'cabin bag', 'luggage', 'sports equipment'], 'baggage', ['baggage allowance', 'pre-book for the lowest price']).map((item) => ({
                        ...item,
                        type: baggageTypeFromText(item.label),
                    })),
                    ...collectLineItems(['baggage', 'checked bag', 'checked baggage', 'extra bag', 'carry-on', 'carry on', 'cabin bag', 'luggage', 'sports equipment'], 'baggage', ['baggage allowance', 'pre-book for the lowest price']).map((item) => ({
                        ...item,
                        type: baggageTypeFromText(item.label),
                    })),
                    ...collectPairedLineItems(
                        ['baggage', 'checked bag', 'checked baggage', 'extra bag', 'carry-on', 'carry on', 'cabin bag', 'luggage', 'sports equipment'],
                        'baggage',
                        ['baggage allowance', 'pre-book for the lowest price'],
                        (line, haystack) => /checked/i.test(haystack)
                            ? 'checked_bag'
                            : /carry[- ]?on|carry on|cabin/i.test(haystack)
                                ? 'cabin_bag'
                                : 'baggage'
                    ),
                ]).filter((item, _, items) => {
                    if (!isGenericBaggageHeading(item.label)) return true;
                    return !items.some((candidate) => candidate !== item
                        && candidate.type === item.type
                        && !isGenericBaggageHeading(candidate.label)
                        && ((Number.isFinite(candidate.amount) && candidate.amount > 0) || candidate.included));
                });
                if (baggage.length) {
                    availableAddOns.baggage = baggage;
                }

                const seatSelection = dedupeItems([
                    ...collectItems(['seat selection', 'seat map', 'select your seat', 'choose your seat', 'standard seat', 'hot seat', 'quiet zone', 'extra legroom', 'flatbed'], 'seat_selection', ['skip seat', 'seat selection observation']),
                    ...collectLineItems(['seat selection', 'seat map', 'select your seat', 'choose your seat', 'standard seat', 'hot seat', 'quiet zone', 'extra legroom', 'flatbed'], 'seat_selection', ['skip seat', 'seat selection observation']),
                    ...collectPairedLineItems(['seat selection', 'seat map', 'select your seat', 'choose your seat', 'standard seat', 'hot seat', 'quiet zone', 'extra legroom', 'flatbed'], 'seat_selection', ['skip seat']),
                ]);
                if (seatSelection.length) {
                    availableAddOns.seat_selection = seatSelection;
                }

                const meals = dedupeItems([
                    ...collectItems(['meal', 'food', 'snack', 'drink', 'beverage', 'santan'], 'meals'),
                    ...collectLineItems(['meal', 'food', 'snack', 'drink', 'beverage', 'santan'], 'meals'),
                    ...collectPairedLineItems(['meal', 'food', 'snack', 'drink', 'beverage', 'santan'], 'meals'),
                ]);
                if (meals.length) {
                    availableAddOns.meals = meals;
                }

                const priority = dedupeItems([
                    ...collectItems(['priority', 'priority boarding', 'fast track', 'fast pass'], 'priority'),
                    ...collectLineItems(['priority', 'priority boarding', 'fast track', 'fast pass'], 'priority'),
                    ...collectPairedLineItems(['priority', 'priority boarding', 'fast track', 'fast pass'], 'priority'),
                ]);
                if (priority.length) {
                    availableAddOns.priority = priority;
                }

                const insurance = dedupeItems([
                    ...specificInsurance,
                    ...collectItems(['insurance'], 'insurance', ['no insurance']),
                    ...collectLineItems(['insurance'], 'insurance', ['no insurance']),
                    ...collectPairedLineItems(['insurance'], 'insurance', ['no insurance']),
                ], 8);
                if (insurance.length) {
                    availableAddOns.insurance = insurance;
                }

                const packages = dedupeItems([
                    ...collectItems(['bundle', 'package', 'upgrade', 'value pack', 'premium flex'], 'package', ['insurance']),
                    ...collectLineItems(['bundle', 'package', 'upgrade', 'value pack', 'premium flex'], 'package', ['insurance']),
                    ...collectPairedLineItems(['bundle', 'package', 'upgrade', 'value pack', 'premium flex'], 'package', ['insurance']),
                ]);
                if (packages.length) {
                    availableAddOns.packages = packages;
                }

                const extraServices = dedupeItems([
                    ...collectItems(['hotel', 'car hire', 'car rental', 'transfer', 'lounge', 'wifi', 'wi-fi', 'voucher', 'sim'], 'extras'),
                    ...collectLineItems(['hotel', 'car hire', 'car rental', 'transfer', 'lounge', 'wifi', 'wi-fi', 'voucher', 'sim'], 'extras'),
                    ...collectPairedLineItems(['hotel', 'car hire', 'car rental', 'transfer', 'lounge', 'wifi', 'wi-fi', 'voucher', 'sim'], 'extras'),
                ]);
                if (extraServices.length) {
                    availableAddOns.extras = extraServices;
                }

                if (Object.keys(availableAddOns).length) {
                    result.available_add_ons = availableAddOns;
                }

                const visiblePriceOptions = dedupeItems([
                    ...priceBreakdown,
                    ...(displayTotal ? [displayTotal] : []),
                    ...baggage,
                    ...seatSelection,
                    ...meals,
                    ...priority,
                    ...insurance,
                    ...packages,
                    ...extraServices,
                ], 20).filter((item) => hasPositiveMoney(item));
                if (visiblePriceOptions.length) {
                    result.visible_price_options = visiblePriceOptions;
                }

                const detailedBreakdown = dedupeItems([
                    ...priceBreakdown,
                    ...baggage,
                    ...seatSelection,
                    ...meals,
                    ...priority,
                    ...insurance,
                    ...packages,
                    ...extraServices,
                ], 16).filter((item) => !(displayTotal && item.amount === displayTotal.amount && item.currency === displayTotal.currency));
                if (detailedBreakdown.length) {
                    result.price_breakdown = detailedBreakdown;
                }

                const baggageNumericVisible = baggage.some((item) => hasPositiveMoney(item));
                if (baggageModalOptions.length) {
                    result.baggage_pricing_observation = 'Numeric baggage pricing is visible when the AirAsia baggage selector is open.';
                } else if (baggageNumericVisible) {
                    result.baggage_pricing_observation = 'Numeric baggage pricing is visible on the reachable AirAsia checkout surface.';
                } else if (result.checkout_page === 'extras') {
                    result.baggage_pricing_observation = 'Extras page reached, but no numeric baggage price was visible on the current AirAsia surface.';
                }

                const seatSurfaceVisible = /seat map|pick a seat|select your seat|standard seat|hot seat|quiet zone|extra legroom|flatbed/i.test(bodyText);
                const seatNumericVisible = seatSelection.some((item) => hasPositiveMoney(item));
                if (seatNumericVisible) {
                    result.seat_selection_observation = 'Numeric seat-selection pricing is visible on the AirAsia seat-selection surface.';
                } else if (result.checkout_page === 'seats') {
                    result.seat_selection_observation = 'Seat-selection page reached, but no numeric seat price was visible on the current AirAsia surface.';
                } else if (!seatSurfaceVisible && (result.checkout_page === 'guest_details' || result.checkout_page === 'payment')) {
                    result.seat_selection_observation = 'No visible seat-selection price surfaced on the reachable AirAsia guest-details/payment path.';
                }

                return result;
            }''',
            default_currency
        )

    async def _extract_volaris_checkout_details(self, page, config: AirlineCheckoutConfig, default_currency: str = "EUR") -> dict:
        return await page.evaluate(
            r'''(defaultCurrency) => {
                const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                const symbolCurrency = {
                    '€': 'EUR',
                    '£': 'GBP',
                    '$': (defaultCurrency || 'MXN').toUpperCase(),
                };
                const knownCurrencies = new Set([
                    'AED', 'ARS', 'AUD', 'BDT', 'BGN', 'BHD', 'BRL', 'CAD', 'CHF', 'CLP', 'CNY', 'COP', 'CZK',
                    'DKK', 'EGP', 'EUR', 'GBP', 'GEL', 'HKD', 'HUF', 'IDR', 'ILS', 'INR', 'JOD', 'JPY', 'KRW',
                    'KWD', 'KZT', 'MAD', 'MXN', 'MYR', 'NOK', 'NZD', 'OMR', 'PEN', 'PHP', 'PKR', 'PLN', 'QAR',
                    'RON', 'RSD', 'SAR', 'SEK', 'SGD', 'THB', 'TRY', 'TWD', 'UAH', 'USD', 'UZS', 'VND', 'ZAR'
                ]);

                const parseNumberToken = (token) => {
                    const clean = normalize(token).replace(/[^\d.,-]/g, '');
                    if (!clean) return null;

                    let normalizedNumber = clean;
                    if (normalizedNumber.includes('.') && normalizedNumber.includes(',')) {
                        if (normalizedNumber.lastIndexOf('.') > normalizedNumber.lastIndexOf(',')) {
                            normalizedNumber = normalizedNumber.replace(/,/g, '');
                        } else {
                            normalizedNumber = normalizedNumber.replace(/\./g, '').replace(',', '.');
                        }
                    } else if (normalizedNumber.includes(',')) {
                        const parts = normalizedNumber.split(',');
                        if (parts.length === 2 && parts[1].length <= 2) {
                            normalizedNumber = `${parts[0].replace(/\./g, '')}.${parts[1]}`;
                        } else {
                            normalizedNumber = normalizedNumber.replace(/,/g, '');
                        }
                    } else {
                        normalizedNumber = normalizedNumber.replace(/,/g, '');
                    }

                    const parsed = Number(normalizedNumber);
                    return Number.isFinite(parsed) ? parsed : null;
                };

                const parseAllMoney = (text) => {
                    const clean = normalize(text).replace(/\u00a0/g, ' ');
                    const values = [];

                    for (const regex of [
                        /([A-Z]{3}|[€£$])\s*([\d.,]+(?:\s*[\d.,]+)?)/gi,
                        /([\d.,]+(?:\s*[\d.,]+)?)\s*([A-Z]{3}|[€£$])\b/gi,
                    ]) {
                        let match;
                        while ((match = regex.exec(clean)) !== null) {
                            const currencyToken = regex === /([A-Z]{3}|[€£$])\s*([\d.,]+(?:\s*[\d.,]+)?)/gi ? match[1] : match[2];
                            const amountToken = regex === /([A-Z]{3}|[€£$])\s*([\d.,]+(?:\s*[\d.,]+)?)/gi ? match[2] : match[1];
                            const currencyCode = String(currencyToken || '').toUpperCase();
                            if (currencyCode.length === 3 && !knownCurrencies.has(currencyCode)) {
                                continue;
                            }
                            const amount = parseNumberToken(amountToken);
                            if (amount === null || amount <= 0) {
                                continue;
                            }
                            values.push({
                                currency: symbolCurrency[currencyToken] || currencyCode,
                                amount,
                            });
                        }
                    }

                    const seen = new Set();
                    return values.filter((item) => {
                        const key = `${item.currency}|${item.amount}`;
                        if (seen.has(key)) return false;
                        seen.add(key);
                        return true;
                    });
                };

                const isVisible = (element) => {
                    if (!element) return false;
                    const style = window.getComputedStyle(element);
                    if (!style || style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) {
                        return false;
                    }
                    return element.offsetParent !== null || style.position === 'fixed';
                };

                const cleanLabel = (text) => normalize(
                    text
                        .replace(/([A-Z]{3}|[€£$])\s*[\d.,]+(?:\s*[\d.,]+)?/gi, '')
                        .replace(/[\d.,]+(?:\s*[\d.,]+)?\s*([A-Z]{3}|[€£$])\b/gi, '')
                        .replace(/\b(desde|detalles|agregar|hasta|meses|sin intereses|keyboard_arrow_down|por pasajero|de tarifa aeroportuaria)\b/gi, '')
                );

                const dedupe = (items, limit = 20) => {
                    const seen = new Set();
                    const deduped = [];
                    for (const item of Array.isArray(items) ? items : []) {
                        if (!item || typeof item !== 'object') continue;
                        const key = [
                            normalize(item.label),
                            normalize(item.type),
                            normalize(item.currency),
                            item.amount ?? '',
                            item.base_amount ?? '',
                            item.airport_fee_amount ?? '',
                            Boolean(item.included),
                        ].join('|').toLowerCase();
                        if (!key.trim() || seen.has(key)) continue;
                        seen.add(key);
                        deduped.push(item);
                        if (deduped.length >= limit) break;
                    }
                    return deduped;
                };

                const dedupeStrings = (items, limit = 400) => {
                    const seen = new Set();
                    const deduped = [];
                    for (const item of Array.isArray(items) ? items : []) {
                        const text = normalize(item);
                        if (!text) continue;
                        const key = text.toLowerCase();
                        if (seen.has(key)) continue;
                        seen.add(key);
                        deduped.push(text);
                        if (deduped.length >= limit) break;
                    }
                    return deduped;
                };

                const rawBody = document.body?.innerText || '';
                const bodyText = normalize(rawBody).toLowerCase();
                const bodyLines = rawBody.split(/\n+/).map(normalize).filter(Boolean);
                const title = normalize(document.title || '').toLowerCase();
                const currentUrl = normalize(`${location.pathname || ''} ${location.hash || ''} ${location.href || ''}`).toLowerCase();
                const pageSignals = `${title} ${currentUrl} ${bodyText}`;

                const result = {};
                if (/pasajeros\s*&\s*pago|formas de pago|pagar mi viaje|informaci[oó]n de pasajero y pago/.test(pageSignals)) {
                    result.checkout_page = 'payment';
                } else if (/\basientos\b|event_seat|selecciona asientos|seleccionar asientos|seat map/.test(pageSignals)) {
                    result.checkout_page = 'seats';
                } else if (/equipaje de mano|equipaje documentado|continuar sin equipaje|\bequipaje\b/.test(pageSignals)) {
                    result.checkout_page = 'baggage';
                } else if (/seguro|hotel|renta de auto|car hire|car rental/.test(pageSignals)) {
                    result.checkout_page = 'extras';
                } else if (/mantener zero|mantener b[áa]sica|seleccionar|asientos disponibles|tarifa aeroportuaria/.test(pageSignals)) {
                    result.checkout_page = 'select_flight';
                }

                const elementTexts = [];
                for (const element of document.querySelectorAll(
                    'button, label, [role="button"], [data-test], [data-testid], [class*="summary"], [class*="price"], [class*="option"], [class*="extra"], [class*="seat"], [class*="bag"], [class*="fare"], [class*="card"]'
                )) {
                    if (!isVisible(element)) continue;
                    const text = normalize(element.innerText || element.textContent || element.getAttribute('aria-label') || '');
                    if (!text || text.length > 320) continue;
                    if (!/(mxn|\$|zero|b[áa]sica|plus|combo|equipaje|asiento|asientos|tarifa|pasajero|pago)/i.test(text)) continue;
                    elementTexts.push(text);
                }
                const sourceTexts = dedupeStrings([...elementTexts, ...bodyLines]);

                const packageItems = [];
                const baggageItems = [];
                const seatItems = [];
                const airportFeeItems = [];
                const perPassengerItems = [];

                for (const text of sourceTexts) {
                    const lower = text.toLowerCase();
                    const monies = parseAllMoney(text);
                    if (!monies.length) continue;

                    if (/tarifa aeroportuaria/.test(lower)) {
                        const feeMoney = monies[1] || monies[0];
                        if (feeMoney) {
                            airportFeeItems.push({
                                label: 'Tarifa aeroportuaria',
                                text,
                                currency: feeMoney.currency,
                                amount: feeMoney.amount,
                                included: false,
                                type: 'airport_fee',
                            });
                        }
                    }

                    if (/por pasajero/.test(lower)) {
                        perPassengerItems.push({
                            label: cleanLabel(text) || 'Cargo por pasajero',
                            text,
                            currency: monies[0].currency,
                            amount: monies[0].amount,
                            included: false,
                            type: 'per_passenger',
                        });
                    }

                    const fareLine = /salida -|cambiar el vuelo|precio por persona/.test(lower) && /tarifa aeroportuaria/.test(lower) && monies.length > 1;
                    const optionalPackageLine = /combo|flexibilidad|combo business/.test(lower);

                    if (fareLine || optionalPackageLine || /zero|b[áa]sica|plus|asientos disponibles/.test(lower)) {
                        if (/meses sin intereses/.test(lower) && !fareLine && !optionalPackageLine) {
                            continue;
                        }
                        const baseMoney = monies[0] || null;
                        if (!baseMoney) {
                            continue;
                        }
                        const airportFeeMoney = fareLine ? (monies[1] || null) : null;
                        const totalAmount = airportFeeMoney ? baseMoney.amount + airportFeeMoney.amount : baseMoney.amount;
                        let label = 'Package option';
                        if (fareLine) {
                            label = 'Selected fare';
                        } else if (/zero/.test(lower)) {
                            label = 'Zero';
                        } else if (/b[áa]sica/.test(lower)) {
                            label = 'Básica';
                        } else if (/plus/.test(lower)) {
                            label = 'Plus';
                        } else if (/combo/.test(lower)) {
                            label = 'Combo';
                        } else {
                            label = cleanLabel(text) || 'Package option';
                        }
                        packageItems.push({
                            label,
                            text,
                            currency: baseMoney.currency,
                            amount: totalAmount,
                            base_amount: baseMoney.amount,
                            airport_fee_amount: airportFeeMoney?.amount,
                            included: false,
                            type: 'package',
                        });
                        continue;
                    }

                    if (/equipaje de mano|equipaje documentado|\bequipaje\b|maleta/.test(lower)) {
                        baggageItems.push({
                            label: cleanLabel(text) || text,
                            text,
                            currency: monies[0].currency,
                            amount: monies[0].amount,
                            included: /incluido|gratis|sin costo/.test(lower),
                            type: 'baggage',
                        });
                        continue;
                    }

                    if (/(asiento|seat|ventana|pasillo|fila|extra espacio|est[aá]ndar|premium)/.test(lower) && !/asientos disponibles/.test(lower)) {
                        seatItems.push({
                            label: cleanLabel(text) || text,
                            text,
                            currency: monies[0].currency,
                            amount: monies[0].amount,
                            included: /incluido|gratis|sin costo/.test(lower),
                            type: 'seat_selection',
                        });
                    }
                }

                const packages = dedupe(packageItems, 12);
                const baggage = dedupe(baggageItems, 12);
                const seats = dedupe(seatItems, 12);
                const airportFees = dedupe(airportFeeItems, 8);
                const perPassenger = dedupe(perPassengerItems, 8);

                const farePackages = packages.filter((item) => Number.isFinite(item.airport_fee_amount) && item.airport_fee_amount > 0);
                if (farePackages.length) {
                    const totalCandidate = farePackages.reduce((lowest, item) => {
                        if (!lowest) return item;
                        return item.amount < lowest.amount ? item : lowest;
                    }, null);
                    if (totalCandidate) {
                        result.display_total = {
                            label: 'Total price',
                            currency: totalCandidate.currency,
                            amount: totalCandidate.amount,
                        };
                    }
                }

                if (!result.display_total) {
                    for (const text of sourceTexts) {
                        const lower = text.toLowerCase();
                        if (!/(total|pagar|resumen|monto|precio)/.test(lower) || /por pasajero/.test(lower)) continue;
                        const money = parseAllMoney(text)[0];
                        if (!money) continue;
                        result.display_total = {
                            label: 'Total price',
                            currency: money.currency,
                            amount: money.amount,
                        };
                        break;
                    }
                }

                const availableAddOns = {};
                if (packages.length) {
                    availableAddOns.packages = packages;
                }
                if (baggage.length) {
                    availableAddOns.baggage = baggage;
                }
                if (seats.length) {
                    availableAddOns.seat_selection = seats;
                }
                if (Object.keys(availableAddOns).length) {
                    result.available_add_ons = availableAddOns;
                }

                if (!seats.length && result.checkout_page === 'seats') {
                    result.seat_selection_observation = 'Seat-selection page reached on Volaris, but no seat-specific numeric price was visible on the current surface.';
                }
                if (!baggage.length && (result.checkout_page === 'baggage' || result.checkout_page === 'extras')) {
                    result.baggage_pricing_observation = 'Volaris baggage step was reached, but no numeric baggage price was visible on the current surface.';
                }

                const visiblePrices = dedupe([
                    ...packages,
                    ...baggage,
                    ...seats,
                    ...airportFees,
                    ...perPassenger,
                ], 24);
                if (visiblePrices.length) {
                    result.visible_price_options = visiblePrices;
                }

                const priceBreakdown = dedupe([
                    ...airportFees,
                    ...baggage,
                    ...seats,
                    ...perPassenger,
                ], 16).filter((item) => !(result.display_total && item.amount === result.display_total.amount && item.currency === result.display_total.currency));
                if (priceBreakdown.length) {
                    result.price_breakdown = priceBreakdown;
                }

                return result;
            }''',
            default_currency
        )

    async def _extract_generic_visible_checkout_details(self, page, config: AirlineCheckoutConfig, default_currency: str = "EUR") -> dict:
        return await page.evaluate(
            r'''(defaultCurrency) => {
                const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                const symbolCurrency = {
                    '€': 'EUR',
                    '£': 'GBP',
                    '$': defaultCurrency || 'USD',
                };
                const knownCurrencies = new Set([
                    'AED', 'ARS', 'AUD', 'BDT', 'BGN', 'BHD', 'BRL', 'CAD', 'CHF', 'CLP', 'CNY', 'COP', 'CZK',
                    'DKK', 'EGP', 'EUR', 'GBP', 'GEL', 'HKD', 'HUF', 'IDR', 'ILS', 'INR', 'JOD', 'JPY', 'KRW',
                    'KWD', 'KZT', 'MAD', 'MXN', 'MYR', 'NOK', 'NZD', 'OMR', 'PEN', 'PHP', 'PKR', 'PLN', 'QAR',
                    'RON', 'RSD', 'SAR', 'SEK', 'SGD', 'THB', 'TRY', 'TWD', 'UAH', 'USD', 'UZS', 'VND', 'ZAR'
                ]);

                const parseNumberToken = (token) => {
                    const clean = normalize(token).replace(/[^\d.,-]/g, '');
                    if (!clean) return null;

                    let normalizedNumber = clean;
                    if (normalizedNumber.includes('.') && normalizedNumber.includes(',')) {
                        if (normalizedNumber.lastIndexOf('.') > normalizedNumber.lastIndexOf(',')) {
                            normalizedNumber = normalizedNumber.replace(/,/g, '');
                        } else {
                            normalizedNumber = normalizedNumber.replace(/\./g, '').replace(',', '.');
                        }
                    } else if (normalizedNumber.includes(',')) {
                        const parts = normalizedNumber.split(',');
                        if (parts.length === 2 && parts[1].length <= 2) {
                            normalizedNumber = `${parts[0].replace(/\./g, '')}.${parts[1]}`;
                        } else {
                            normalizedNumber = normalizedNumber.replace(/,/g, '');
                        }
                    } else {
                        normalizedNumber = normalizedNumber.replace(/,/g, '');
                    }

                    const parsed = Number(normalizedNumber);
                    return Number.isFinite(parsed) ? parsed : null;
                };

                const parseMoney = (text) => {
                    const clean = normalize(text).replace(/\u00a0/g, ' ');
                    let match = clean.match(/([A-Z]{3}|[€£$])\s*([\d.,]+(?:\s*[\d.,]+)?)/i);
                    if (match) {
                        const currencyCode = match[1].toUpperCase();
                        if (currencyCode.length === 3 && !knownCurrencies.has(currencyCode)) {
                            match = null;
                        }
                    }
                    if (match) {
                        const amount = parseNumberToken(match[2]);
                        if (amount !== null) {
                            return {
                                currency: symbolCurrency[match[1]] || match[1].toUpperCase(),
                                amount,
                            };
                        }
                    }

                    match = clean.match(/([\d.,]+(?:\s*[\d.,]+)?)\s*([A-Z]{3}|[€£$])\b/i);
                    if (match) {
                        const currencyCode = match[2].toUpperCase();
                        if (currencyCode.length === 3 && !knownCurrencies.has(currencyCode)) {
                            match = null;
                        }
                    }
                    if (match) {
                        const amount = parseNumberToken(match[1]);
                        if (amount !== null) {
                            return {
                                currency: symbolCurrency[match[2]] || match[2].toUpperCase(),
                                amount,
                            };
                        }
                    }

                    return null;
                };

                const isVisible = (element) => {
                    if (!element) return false;
                    const style = window.getComputedStyle(element);
                    if (!style || style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) {
                        return false;
                    }
                    return element.offsetParent !== null || style.position === 'fixed';
                };

                const cleanLabel = (text) => normalize(
                    text
                        .replace(/([A-Z]{3}|[€£$])\s*[\d.,]+(?:\s*[\d.,]+)?/gi, '')
                        .replace(/[\d.,]+(?:\s*[\d.,]+)?\s*([A-Z]{3}|[€£$])\b/gi, '')
                        .replace(/\b(add|from|per passenger|pp|each)\b/gi, '')
                );

                const hasPositiveMoney = (money) => !!money && Number.isFinite(money.amount) && money.amount > 0;

                const dedupe = (items, limit = 12) => {
                    const seen = new Set();
                    const deduped = [];
                    for (const item of Array.isArray(items) ? items : []) {
                        if (!item || typeof item !== 'object') continue;
                        const key = [
                            normalize(item.label),
                            normalize(item.type),
                            normalize(item.currency),
                            item.amount ?? '',
                            Boolean(item.included),
                        ].join('|').toLowerCase();
                        if (!key.trim() || seen.has(key)) continue;
                        seen.add(key);
                        deduped.push(item);
                        if (deduped.length >= limit) break;
                    }
                    return deduped;
                };

                const rawBody = document.body?.innerText || '';
                const bodyText = normalize(rawBody).toLowerCase();
                const bodyLines = rawBody.split(/\n+/).map(normalize).filter(Boolean);
                const title = normalize(document.title || '').toLowerCase();
                const currentUrl = normalize(`${location.pathname || ''} ${location.hash || ''} ${location.href || ''}`).toLowerCase();
                const pageSignals = `${title} ${currentUrl} ${bodyText}`;
                const bookingWidgetControl = Array.from(document.querySelectorAll('input, select, button')).find((el) => {
                    if (!isVisible(el)) return false;
                    const controlSignals = normalize(`${el.getAttribute('name') || ''} ${el.getAttribute('id') || ''} ${el.getAttribute('placeholder') || ''} ${el.textContent || ''}`).toLowerCase();
                    return /departurefrom|departureto|triptype|promo|promotional code|book a flight|departure date|return date/.test(controlSignals);
                });
                const bookingLandingSurface = /\bbook a flight\b/.test(title) && /\/book-flight\b/.test(currentUrl);
                const homeSearchSurface = bookingLandingSurface
                    || Boolean(bookingWidgetControl)
                    || /search flights|flight deals|manage your reservation|before flying|promotional code|trip type|book a flight|plan & book|plan and book/.test(pageSignals);
                const securityGate = /pardon our interruption|made us think you were a bot|captcha below|i am human|hcaptcha|incident id|performing security verification|verifies you are not a bot|checking your browser|just a moment|please wait while we verify/.test(pageSignals)
                    || Boolean(document.querySelector("iframe[src*='hcaptcha'], iframe[title*='hCaptcha'], .h-captcha, [data-hcaptcha-response], #challenge-running"));
                const passengerControl = Array.from(document.querySelectorAll('input, select, textarea, mat-select, [role="combobox"]')).find((el) => {
                    if (!isVisible(el)) return false;
                    const controlSignals = normalize(`${el.getAttribute('name') || ''} ${el.getAttribute('id') || ''} ${el.getAttribute('placeholder') || ''} ${el.getAttribute('aria-label') || ''}`).toLowerCase();
                    return /first.?name|last.?name|dob|birth|nationality|document|passport|confirmedemail|confirm email|phone|traveler|traveller|passenger/.test(controlSignals);
                });
                const passengerSurface = Boolean(passengerControl)
                    || /enter your information|personal information|traveller details|traveler details|guest details|passenger details|passport details|date of birth|confirm email/.test(pageSignals);

                const flightSelectionSurface = /\/booking\/availability/.test(currentUrl)
                    || /flight selection/.test(title)
                    || /select flight|choose flight|departing flights|returning flights/.test(bodyText);

                const result = {};
                if (securityGate) {
                    result.checkout_page = 'blocked';
                    result.security_gate_observation = 'Automation was blocked by a CAPTCHA or browser verification gate on the reachable airline surface.';
                } else if (flightSelectionSurface || homeSearchSurface) {
                    result.checkout_page = 'select_flight';
                } else if (!homeSearchSurface && passengerSurface) {
                    result.checkout_page = 'passengers';
                } else if (/card number|billing address|secure payment|payment method|pay now|review and pay|expiry date|cvv|security code/.test(pageSignals)) {
                    result.checkout_page = 'payment';
                } else if (/seat map|select your seat|seat selection|choose your seats|pick your seat|extra legroom/.test(pageSignals)) {
                    result.checkout_page = 'seats';
                } else if (/baggage|checked bag|extra bag|carry-on|carry on|priority boarding|insurance|meal|add extras|choose extras/.test(pageSignals)) {
                    result.checkout_page = 'extras';
                } else if (/passenger|traveller details|contact details|customer details|guest details/.test(pageSignals)) {
                    result.checkout_page = 'passengers';
                }

                let displayTotal = null;
                for (const selector of [
                    "[data-test*='total']",
                    "[data-testid*='total']",
                    "[class*='total'] [class*='price']",
                    "[class*='summary'] [class*='price']",
                    "[class*='summary'] [class*='amount']",
                    "[class*='cart'] [class*='price']",
                    "[class*='payment'] [class*='price']",
                ]) {
                    const element = document.querySelector(selector);
                    if (!isVisible(element)) continue;
                    const money = parseMoney(element.innerText || element.textContent || '');
                    if (!hasPositiveMoney(money)) continue;
                    displayTotal = {
                        label: 'Total price',
                        currency: money.currency,
                        amount: money.amount,
                    };
                    break;
                }

                if (!displayTotal) {
                    for (const line of bodyLines) {
                        if (!/total|due now|to pay|amount due|grand total/i.test(line)) continue;
                        const money = parseMoney(line);
                        if (!hasPositiveMoney(money)) continue;
                        displayTotal = {
                            label: 'Total price',
                            currency: money.currency,
                            amount: money.amount,
                        };
                        break;
                    }
                }
                if (displayTotal) {
                    result.display_total = displayTotal;
                }

                const elementTexts = [];
                for (const element of document.querySelectorAll(
                    'button, label, [role="button"], [data-test], [data-testid], [class*="summary"], [class*="price"], [class*="option"], [class*="extra"], [class*="seat"], [class*="bag"], [class*="bundle"], [class*="fare"]'
                )) {
                    if (!isVisible(element)) continue;
                    const text = normalize(element.innerText || element.textContent);
                    if (!text || text.length > 180) continue;
                    elementTexts.push({
                        text,
                        haystack: normalize(`${element.getAttribute('data-test') || ''} ${element.getAttribute('data-testid') || ''} ${element.className || ''} ${text}`).toLowerCase(),
                    });
                }

                const collectItems = (keywords, type, excludedKeywords = []) => {
                    const items = [];
                    for (const entry of elementTexts) {
                        if (!keywords.some((keyword) => entry.haystack.includes(keyword))) continue;
                        if (excludedKeywords.some((keyword) => entry.haystack.includes(keyword))) continue;
                        const money = parseMoney(entry.text);
                        const included = /included|free|no extra cost|included in your fare|already selected/i.test(entry.text);
                        if (!hasPositiveMoney(money) && !included) continue;
                        items.push({
                            label: cleanLabel(entry.text) || entry.text,
                            text: entry.text,
                            currency: money?.currency || defaultCurrency || 'EUR',
                            amount: money?.amount,
                            included,
                            type,
                        });
                    }
                    return items;
                };

                const collectLineItems = (keywords, type, excludedKeywords = []) => {
                    const items = [];
                    for (const line of bodyLines) {
                        const haystack = line.toLowerCase();
                        if (!keywords.some((keyword) => haystack.includes(keyword))) continue;
                        if (excludedKeywords.some((keyword) => haystack.includes(keyword))) continue;
                        const money = parseMoney(line);
                        const included = /included|free|no extra cost|included in your fare|already selected/i.test(line);
                        if (!hasPositiveMoney(money) && !included) continue;
                        items.push({
                            label: cleanLabel(line) || line,
                            text: line,
                            currency: money?.currency || defaultCurrency || 'EUR',
                            amount: money?.amount,
                            included,
                            type,
                        });
                    }
                    return items;
                };

                const availableAddOns = {};
                const baggage = dedupe([
                    ...collectItems(['baggage', 'checked bag', 'checked baggage', 'carry-on', 'carry on', 'cabin bag', 'luggage'], 'baggage', ['baggage allowance']),
                    ...collectLineItems(['baggage', 'checked bag', 'checked baggage', 'carry-on', 'carry on', 'cabin bag', 'luggage'], 'baggage', ['baggage allowance']),
                ]);
                if (baggage.length) {
                    availableAddOns.baggage = baggage;
                }

                const seatSelection = dedupe([
                    ...collectItems(['seat selection', 'seat map', 'select your seat', 'choose your seat', 'extra legroom', 'standard seat', 'window seat', 'aisle seat', 'seat'], 'seat_selection', ['seat selection observation']),
                    ...collectLineItems(['seat selection', 'seat map', 'select your seat', 'choose your seat', 'extra legroom', 'standard seat', 'window seat', 'aisle seat'], 'seat_selection'),
                ]);
                if (seatSelection.length) {
                    availableAddOns.seat_selection = seatSelection;
                }

                const meals = dedupe([
                    ...collectItems(['meal', 'food', 'snack', 'drink'], 'meals'),
                    ...collectLineItems(['meal', 'food', 'snack', 'drink'], 'meals'),
                ]);
                if (meals.length) {
                    availableAddOns.meals = meals;
                }

                const priority = dedupe([
                    ...collectItems(['priority', 'fast track', 'priority boarding'], 'priority'),
                    ...collectLineItems(['priority', 'fast track', 'priority boarding'], 'priority'),
                ]);
                if (priority.length) {
                    availableAddOns.priority = priority;
                }

                const insurance = dedupe([
                    ...collectItems(['insurance'], 'insurance'),
                    ...collectLineItems(['insurance'], 'insurance'),
                ]);
                if (insurance.length) {
                    availableAddOns.insurance = insurance;
                }

                const packages = dedupe([
                    ...collectItems(['bundle', 'package', 'upgrade'], 'package'),
                    ...collectLineItems(['bundle', 'package', 'upgrade'], 'package'),
                ]);
                if (packages.length) {
                    availableAddOns.packages = packages;
                }

                if (Object.keys(availableAddOns).length) {
                    result.available_add_ons = availableAddOns;
                }

                if (!seatSelection.length && result.checkout_page === 'seats') {
                    result.seat_selection_observation = 'Seat-selection page reached, but no numeric seat price was visible on the current surface.';
                }
                if (!baggage.length && result.checkout_page === 'extras') {
                    result.baggage_pricing_observation = 'Extras page reached, but no numeric baggage price was visible on the current surface.';
                }

                const visiblePrices = [];
                for (const entry of [...elementTexts, ...bodyLines.map((text) => ({ text, haystack: text.toLowerCase() }))]) {
                    if (homeSearchSurface && /^from\s+[A-Z]{3}\s*[\d,]+(?:\.\d{1,2})?$/i.test(normalize(entry.text))) {
                        continue;
                    }
                    const money = parseMoney(entry.text);
                    if (!hasPositiveMoney(money)) continue;
                    visiblePrices.push({
                        label: cleanLabel(entry.text) || entry.text,
                        text: entry.text,
                        currency: money.currency,
                        amount: money.amount,
                    });
                }
                const dedupedVisiblePrices = dedupe(visiblePrices, 20);
                if (dedupedVisiblePrices.length) {
                    result.visible_price_options = dedupedVisiblePrices;
                }

                const priceBreakdown = dedupe([
                    ...collectLineItems(['fare', 'flight', 'base fare', 'tax', 'fee', 'service charge', 'admin', 'airport'], 'breakdown'),
                    ...baggage,
                    ...seatSelection,
                    ...meals,
                    ...priority,
                    ...insurance,
                ], 16).filter((item) => !(displayTotal && item.amount === displayTotal.amount && item.currency === displayTotal.currency));
                if (priceBreakdown.length) {
                    result.price_breakdown = priceBreakdown;
                }

                return result;
            }''',
            default_currency
        )

    async def _wizzair_checkout(self, page, config, offer, offer_id, booking_url, passengers, t0):
        """WizzAir custom checkout using homepage preload + SPA hash navigation.

        The older in-page fetch/Vuex injection path is no longer reliable. WizzAir's
        own SPA will still load the booking flow after Kasada initialises on the
        homepage, so this handler now drives the same route/navigation pattern used
        by the private full booker and stops safely before payment submission.
        """
        import asyncio as _aio
        import re as _re

        pax = passengers[0] if passengers else FAKE_PASSENGER
        step = "init"

        def _normalize_booking_url(raw_url: str) -> str:
            if not raw_url:
                return raw_url
            normalized = _re.sub(
                r"(booking/select-flight/[A-Z]{3}/[A-Z]{3}/\d{4}-\d{2}-\d{2})//(?=\d+/\d+/\d+(?:$|[?#]))",
                r"\1/null/",
                raw_url,
                count=1,
            )
            if normalized != raw_url:
                logger.info("WizzAir checkout: normalized one-way booking URL placeholder")
            return normalized

        booking_url = _normalize_booking_url(booking_url)

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

        async def _extract_price() -> float:
            page_price = float(offer.get("price", 0.0) or 0.0)
            for sel in [
                "[data-test='total-price']",
                "[class*='total-price']",
                "[class*='TotalPrice']",
                "[data-test*='summary'] [class*='price']",
                "[data-test*='total-price']",
                "[class*='summary-price']",
            ]:
                try:
                    el = page.locator(sel).first
                    text = await el.text_content(timeout=3000)
                    if text:
                        nums = re.findall(r"[\d,]+\.?\d*", text.replace(",", ""))
                        if nums:
                            return float(nums[-1])
                except Exception:
                    continue
            return page_price

        async def _extract_visible_checkout_details() -> dict:
            return await page.evaluate(
                r'''(defaultCurrency) => {
                    const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                    const currencyCodes = new Set([
                        'EUR', 'GBP', 'USD', 'HUF', 'PLN', 'RON', 'AED', 'SAR', 'SEK', 'NOK', 'DKK', 'CHF', 'CZK', 'BGN', 'ALL', 'GEL', 'MKD', 'UAH',
                    ]);
                    const symbolCurrency = {
                        '£': 'GBP',
                        '€': 'EUR',
                        '$': defaultCurrency || 'USD',
                    };
                    const noisePattern = /choose an outbound flight|passenger change|airport and fuel charges included in the base price|passengers seats services payment/i;

                    const parseMoney = (text) => {
                        const clean = normalize(text).replace(/\u00a0/g, ' ');
                        let match = clean.match(/([A-Z]{3}|[£€$])\s*([\d,]+(?:\.\d{1,2})?)/i);
                        if (match) {
                            const currencyToken = match[1];
                            if (!symbolCurrency[currencyToken] && !currencyCodes.has(currencyToken.toUpperCase())) {
                                return null;
                            }
                            return {
                                currency: symbolCurrency[currencyToken] || currencyToken.toUpperCase(),
                                amount: Number(match[2].replace(/,/g, '')),
                            };
                        }

                        match = clean.match(/([\d,]+(?:\.\d{1,2})?)\s*([A-Z]{3})\b/i);
                        if (match) {
                            if (!currencyCodes.has(match[2].toUpperCase())) {
                                return null;
                            }
                            return {
                                currency: match[2].toUpperCase(),
                                amount: Number(match[1].replace(/,/g, '')),
                            };
                        }

                        return null;
                    };

                    const isVisible = (element) => {
                        if (!element) return false;
                        const style = window.getComputedStyle(element);
                        if (!style || style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) {
                            return false;
                        }
                        return element.offsetParent !== null || style.position === 'fixed';
                    };

                    const cleanLabel = (text) => normalize(
                        text
                            .replace(/([A-Z]{3}|[£€$])\s*[\d,]+(?:\.\d{1,2})?/gi, '')
                            .replace(/[\d,]+(?:\.\d{1,2})?\s*([A-Z]{3})\b/gi, '')
                            .replace(/\badd\b/gi, '')
                    );

                    const hasPositiveMoney = (money) => (
                        !!money && Number.isFinite(money.amount) && money.amount > 0
                    );

                    const dedupe = (items) => {
                        const seen = new Set();
                        return items.filter((item) => {
                            const key = [item.label || '', item.amount ?? '', item.currency || '', item.text || ''].join('|').toLowerCase();
                            if (!key.trim() || seen.has(key)) {
                                return false;
                            }
                            seen.add(key);
                            return true;
                        });
                    };

                    const result = {};
                    const hash = (window.location.hash || '').toLowerCase();
                    const passengerInput = document.querySelector("input[data-test='passenger-first-name-0'], input[data-test*='first-name'], button[data-test='passengers-continue-btn']");
                    const paymentInput = document.querySelector("input[data-test*='card-number'], input[name*='cardNumber'], iframe[src*='payment'], iframe[title*='payment' i]");
                    const loginDialog = document.querySelector("[data-test='loginmodal-signin'], .dialog-container [data-test='loginmodal-signin']");
                    if (isVisible(passengerInput)) {
                        result.checkout_page = 'passengers';
                    } else if (isVisible(paymentInput)) {
                        result.checkout_page = 'payment';
                    } else if (isVisible(loginDialog)) {
                        result.checkout_page = 'login';
                    } else if (hash.includes('passengers')) {
                        result.checkout_page = 'passengers';
                    } else if (hash.includes('payment')) {
                        result.checkout_page = 'payment';
                    } else if (hash.includes('select-flight')) {
                        result.checkout_page = 'select_flight';
                    }
                    if (hash) {
                        result.route = hash;
                    }

                    let displayTotal = null;
                    for (const selector of [
                        "[data-test='total-price']",
                        "[class*='total-price']",
                        "[class*='TotalPrice']",
                        "[data-test*='summary'] [class*='price']",
                        "[class*='summary-price']",
                    ]) {
                        const element = document.querySelector(selector);
                        if (!isVisible(element)) continue;
                        const text = normalize(element.innerText || element.textContent);
                        const money = parseMoney(text);
                        if (!hasPositiveMoney(money)) continue;
                        displayTotal = {
                            label: /total/i.test(text) ? 'Total price' : cleanLabel(text) || 'Total price',
                            currency: money.currency,
                            amount: money.amount,
                        };
                        break;
                    }

                    if (!displayTotal) {
                        const lines = normalize(document.body?.innerText || '').split(/\n+/).map(normalize).filter(Boolean);
                        for (const line of lines) {
                            if (!/total/i.test(line)) continue;
                            const money = parseMoney(line);
                            if (!hasPositiveMoney(money)) continue;
                            displayTotal = {
                                label: 'Total price',
                                currency: money.currency,
                                amount: money.amount,
                            };
                            break;
                        }
                    }

                    if (displayTotal) {
                        result.display_total = displayTotal;
                    }

                    const collectKeywordItems = (keywords) => {
                        const items = [];
                        for (const element of document.querySelectorAll('button, label, [data-test], [role="button"]')) {
                            if (!isVisible(element)) continue;
                            const text = normalize(element.innerText || element.textContent);
                            if (!text || text.length > 120 || noisePattern.test(text)) continue;
                            const haystack = normalize(`${element.getAttribute('data-test') || ''} ${element.className || ''} ${text}`).toLowerCase();
                            if (!keywords.some((keyword) => haystack.includes(keyword))) continue;
                            const money = parseMoney(text);
                            const included = /included|free|no checked/i.test(text);
                            if ((!money || money.amount <= 0) && !included) continue;
                            items.push({
                                label: cleanLabel(text) || text,
                                text,
                                currency: money?.currency || defaultCurrency || 'EUR',
                                amount: money?.amount,
                                included,
                            });
                        }
                        return dedupe(items);
                    };

                    const availableAddOns = {};
                    const baggage = collectKeywordItems(['bag', 'baggage']);
                    if (baggage.length) {
                        availableAddOns.baggage = baggage.slice(0, 10);
                    }
                    const priority = collectKeywordItems(['priority']);
                    if (priority.length) {
                        availableAddOns.priority = priority.slice(0, 10);
                    }
                    const insurance = collectKeywordItems(['insurance']);
                    if (insurance.length) {
                        availableAddOns.insurance = insurance.slice(0, 10);
                    }
                    const disruption = collectKeywordItems(['disruption']);
                    if (disruption.length) {
                        availableAddOns.insurance = dedupe([...(availableAddOns.insurance || []), ...disruption]).slice(0, 10);
                    }
                    const packages = collectKeywordItems(['bundle', 'smart', 'plus', 'premium', 'flex']);
                    if (packages.length) {
                        availableAddOns.packages = packages.slice(0, 10);
                    }
                    if (Object.keys(availableAddOns).length) {
                        result.available_add_ons = availableAddOns;
                    }

                    const visiblePrices = [];
                    for (const element of document.querySelectorAll('button, label, [data-test], [role="button"]')) {
                        if (!isVisible(element)) continue;
                        const text = normalize(element.innerText || element.textContent);
                        if (!text || text.length > 100 || noisePattern.test(text)) continue;
                        const money = parseMoney(text);
                        if (!hasPositiveMoney(money)) continue;
                        visiblePrices.push({
                            label: cleanLabel(text) || text,
                            text,
                            currency: money.currency,
                            amount: money.amount,
                        });
                    }

                    const dedupedVisiblePrices = dedupe(visiblePrices).slice(0, 20);
                    if (dedupedVisiblePrices.length) {
                        result.visible_price_options = dedupedVisiblePrices;
                    }

                    const priceBreakdown = dedupe([
                        ...collectKeywordItems(['fare', 'flight', 'total', 'service', 'admin']),
                        ...collectKeywordItems(['priority', 'bag', 'baggage', 'insurance']),
                    ]).filter((item) => !(displayTotal && item.amount === displayTotal.amount && item.currency === displayTotal.currency));
                    if (priceBreakdown.length) {
                        result.price_breakdown = priceBreakdown.slice(0, 12);
                    }

                    return result;
                }''',
                str(offer.get("currency") or "EUR"),
            )

        async def _dismiss_wizz_consent() -> None:
            await safe_click_first(
                page,
                [
                    "button:has-text('Accept all')",
                    "button:has-text('Deny all')",
                    "button:has-text('Save')",
                    "#usercentrics-cmp-ui button:has-text('Accept all')",
                    "#usercentrics-cmp-ui button:has-text('Deny all')",
                ],
                timeout=2000,
                desc="Wizz consent banner",
            )
            try:
                await page.evaluate(
                    """() => {
                        for (const id of ['usercentrics-cmp-ui', 'usercentrics-root']) {
                            const node = document.getElementById(id);
                            if (node) {
                                node.remove();
                            }
                        }
                        for (const node of document.querySelectorAll('aside, .uc-embedding-container')) {
                            const text = (node.textContent || '').toLowerCase();
                            if (text.includes('privacy settings') || text.includes('accept all') || text.includes('deny all')) {
                                node.remove();
                            }
                        }
                    }"""
                )
            except Exception:
                pass
            await page.wait_for_timeout(500)

        async def _dismiss_wizz_urgency_modal() -> None:
            await safe_click_first(
                page,
                [
                    "button[data-test='continue-booking']",
                    "button:has-text('Continue booking')",
                    "button[aria-label='Close']",
                    "button:has-text('Start a new search')",
                ],
                timeout=1500,
                desc="Wizz urgency modal",
            )
            try:
                await page.evaluate(
                    """() => {
                        const targets = Array.from(document.querySelectorAll('article, [role="dialog"], .dialog-container, .modal'));
                        for (const node of targets) {
                            const text = (node.textContent || '').toLowerCase();
                            if (text.includes('your session will expire soon')) {
                                node.remove();
                            }
                        }
                    }"""
                )
            except Exception:
                pass
            await page.wait_for_timeout(300)

        async def _select_flight(route: dict | None, direction: str) -> bool:
            if not route or not route.get("segments"):
                return False

            try:
                await page.wait_for_selector("button[data-test='select-fare'], button:has-text('SELECT')", timeout=15000)
            except Exception:
                logger.warning("WizzAir checkout: fare select buttons not ready for %s", direction)

            await _dismiss_wizz_consent()
            await _dismiss_wizz_urgency_modal()

            segment = route.get("segments", [{}])[0]
            target_flight_no = str(segment.get("flight_no") or "").strip()
            target_dep = segment.get("departure")
            target_arr = segment.get("arrival")
            dep_time = _extract_hhmm(target_dep)
            arr_time = _extract_hhmm(target_arr)

            if dep_time:
                try:
                    clicked = await page.evaluate(
                        """({ depTime, arrTime }) => {
                            const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                            const isVisible = (element) => {
                                if (!element) return false;
                                const style = window.getComputedStyle(element);
                                return style.display !== 'none' && style.visibility !== 'hidden' && element.getClientRects().length > 0;
                            };
                            const buttons = Array.from(document.querySelectorAll("button[data-test='select-fare']"));
                            const match = buttons.find((button) => {
                                let node = button;
                                while (node) {
                                    const text = normalize(node.innerText || node.textContent);
                                    if (text.includes(depTime) && (!arrTime || text.includes(arrTime))) {
                                        return isVisible(button);
                                    }
                                    node = node.parentElement;
                                }
                                return false;
                            });
                            const fallback = buttons.find(isVisible);
                            const target = match || fallback;
                            if (!target) {
                                return false;
                            }
                            target.click();
                            return true;
                        }""",
                        {"depTime": dep_time, "arrTime": arr_time},
                    )
                    if clicked:
                        await page.wait_for_timeout(1000)
                        logger.info("WizzAir checkout: selected %s flight by DOM click %s", direction, dep_time)
                        return True
                except Exception:
                    pass

            if target_flight_no:
                clean_num = target_flight_no.replace("W6", "").replace(" ", "").strip()
                for text_variant in [target_flight_no, f"W6 {clean_num}", clean_num]:
                    try:
                        card = page.locator(f"text='{text_variant}'").first
                        if await card.is_visible(timeout=2000):
                            await card.click()
                            logger.info("WizzAir checkout: selected %s flight %s", direction, text_variant)
                            return True
                    except Exception:
                        continue

            if dep_time:
                xpath = (
                    "//button[@data-test='select-fare' and ancestor::*[contains(normalize-space(.), '"
                    + dep_time
                    + "')"
                    + (f" and contains(normalize-space(.), '{arr_time}')" if arr_time else "")
                    + "]]"
                )
                try:
                    match_btn = page.locator(f"xpath={xpath}").first
                    if await match_btn.count() > 0:
                        await match_btn.click(force=True)
                        logger.info("WizzAir checkout: selected %s flight by time %s", direction, dep_time)
                        return True
                except Exception:
                    pass

                for container_sel in [
                    "[data-test*='flight-card']",
                    "[class*='flight-card']",
                    "[class*='FlightCard']",
                    "[class*='flight-select'] > *",
                ]:
                    try:
                        containers = page.locator(container_sel)
                        count = min(await containers.count(), 20)
                        for index in range(count):
                            container = containers.nth(index)
                            text = ((await container.inner_text(timeout=1000)) or "").strip()
                            if dep_time not in text:
                                continue
                            if arr_time and arr_time not in text:
                                continue
                            select_btn = container.locator("button[data-test='select-fare'], button:has-text('SELECT')").first
                            if await select_btn.count() > 0:
                                await select_btn.click(force=True)
                                logger.info("WizzAir checkout: selected %s flight row by %s", direction, dep_time)
                                return True
                    except Exception:
                        continue

                try:
                    time_el = page.locator(f"text='{dep_time}'").first
                    if await time_el.is_visible(timeout=3000):
                        await time_el.click()
                        logger.info("WizzAir checkout: selected %s flight by time %s", direction, dep_time)
                        return True
                except Exception:
                    pass

            for sel in [
                "button[data-test='select-fare']",
                f"[data-test*='flight-select-{direction}'] button:has-text('SELECT')",
                "button:has-text('SELECT')",
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0:
                        await btn.click(force=True)
                        await page.wait_for_timeout(1000)
                        logger.info("WizzAir checkout: selected %s flight using fallback %s", direction, sel)
                        return True
                except Exception:
                    continue
            return False

        async def _select_basic_fare() -> bool:
            async def _choose_required_option() -> bool:
                for text_sel in ["text='No thanks'", "text='No, thanks'", "text='Skip'"]:
                    try:
                        option = page.locator(text_sel).first
                        if await option.count() == 0:
                            continue
                        try:
                            await option.click(force=True, timeout=1000)
                        except Exception:
                            clicked = await option.evaluate(
                                """(node) => {
                                    let candidate = node;
                                    for (let depth = 0; candidate && depth < 6; depth += 1, candidate = candidate.parentElement) {
                                        if (typeof candidate.click === 'function') {
                                            candidate.click();
                                            return true;
                                        }
                                    }
                                    return false;
                                }"""
                            )
                            if not clicked:
                                continue
                        await page.wait_for_timeout(800)
                        logger.info("WizzAir checkout: selected required option via %s", text_sel)
                        return True
                    except Exception:
                        continue
                try:
                    clicked = await page.evaluate(
                        """() => {
                            const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                            const isVisible = (element) => {
                                if (!element) return false;
                                const style = window.getComputedStyle(element);
                                return style.display !== 'none' && style.visibility !== 'hidden' && element.getClientRects().length > 0;
                            };
                            const bodyText = normalize(document.body?.innerText || '');
                            if (!bodyText.includes('Add Disruption Assistance') && !bodyText.includes('You have to choose one option')) {
                                return false;
                            }
                            const targets = Array.from(document.querySelectorAll('button, label, [role="radio"], [class], div, span, b, strong'));
                            for (const node of targets) {
                                const text = normalize(node.innerText || node.textContent);
                                if (!text || (text !== 'No thanks' && text !== 'No, thanks' && text !== 'Skip')) {
                                    continue;
                                }
                                let candidate = node;
                                for (let depth = 0; candidate && depth < 6; depth += 1, candidate = candidate.parentElement) {
                                    if (!isVisible(candidate) || typeof candidate.click !== 'function') {
                                        continue;
                                    }
                                    candidate.click();
                                    return true;
                                }
                            }
                            return false;
                        }"""
                    )
                    if clicked:
                        await page.wait_for_timeout(800)
                        logger.info("WizzAir checkout: selected required option via DOM fallback")
                    return bool(clicked)
                except Exception:
                    return False

            async def _click_continue_dom() -> bool:
                try:
                    clicked = await page.evaluate(
                        """() => {
                            const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                            const isVisible = (element) => {
                                if (!element) return false;
                                const style = window.getComputedStyle(element);
                                return style.display !== 'none' && style.visibility !== 'hidden' && element.getClientRects().length > 0;
                            };
                            const buttons = Array.from(document.querySelectorAll('button'));
                            const pick = (predicate) => buttons.find((button) => {
                                const text = normalize(button.innerText || button.textContent);
                                return isVisible(button) && predicate(button, text);
                            });
                            const target =
                                pick((button, text) => button.getAttribute('data-test') === 'next-btn') ||
                                pick((button, text) => button.getAttribute('data-test') === 'booking-flight-select-continue-btn') ||
                                pick((button, text) => text === 'No thanks' || text === 'No, thanks' || text === 'Not now' || text === 'Skip') ||
                                pick((button, text) => text.includes('Continue for')) ||
                                pick((button, text) => text === 'Continue');
                            if (!target) {
                                return false;
                            }
                            target.click();
                            return true;
                        }"""
                    )
                    if clicked:
                        await page.wait_for_timeout(1000)
                    return bool(clicked)
                except Exception:
                    return False

            try:
                await page.wait_for_selector(
                    "button:has-text('Continue for'), button[data-test='booking-flight-select-continue-btn'], button[data-test='next-btn']",
                    timeout=15000,
                )
            except Exception:
                logger.warning("WizzAir checkout: fare buttons not found within 15s")
                if await _click_continue_dom():
                    try:
                        return await page.locator("input[data-test='passenger-first-name-0']").count() > 0
                    except Exception:
                        return False

            for index in range(10):
                await page.wait_for_timeout(2500)
                await _dismiss_wizz_urgency_modal()
                await _choose_required_option()
                try:
                    if await page.locator("input[data-test='passenger-first-name-0']").count() > 0:
                        logger.info("WizzAir checkout: fare selection complete, passenger form appeared")
                        return True
                except Exception:
                    pass

                clicked = False
                for sel in [
                    "button[data-test='next-btn']",
                    "button[data-test='booking-flight-select-continue-btn']",
                    "button:has-text('No thanks')",
                    "button:has-text('No, thanks')",
                    "button:has-text('Not now')",
                    "button:has-text('Skip')",
                    "button:has-text('Continue for')",
                    "button:has-text('Continue')",
                ]:
                    try:
                        btn = page.locator(sel).first
                        if await btn.count() > 0:
                            text = ((await btn.text_content()) or "").strip()[:60]
                            await btn.click(force=True)
                            logger.info("WizzAir checkout: fare step %d clicked %s", index, text)
                            clicked = True
                            break
                    except Exception:
                        continue

                if not clicked:
                    if await _click_continue_dom():
                        clicked = True
                        continue
                    logger.debug("WizzAir checkout: no fare button matched on step %d", index)

            try:
                return await page.locator("input[data-test='passenger-first-name-0']").count() > 0
            except Exception:
                return False

        async def _fill_passenger_details() -> bool:
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            form_found = False
            for sel in [
                "input[data-test='passenger-first-name-0']",
                "input[data-test*='first-name']",
                "input[placeholder='First name']",
            ]:
                try:
                    await page.wait_for_selector(sel, timeout=10000)
                    form_found = True
                    break
                except Exception:
                    continue

            if not form_found:
                logger.warning("WizzAir checkout: passenger form not found")
                return False

            gender = pax.get("gender", "m")
            if gender == "f":
                for sel in [
                    "label[data-test='passenger-gender-0-female']",
                    "[data-test='passenger-0-gender-selectorfemale']",
                    "label:has-text('Ms')",
                    "label:has-text('Mrs')",
                ]:
                    if await safe_click(page, sel, timeout=3000, desc="gender female"):
                        break
            else:
                for sel in [
                    "label[data-test='passenger-gender-0-male']",
                    "[data-test='passenger-0-gender-selectormale']",
                    "label:has-text('Mr')",
                ]:
                    if await safe_click(page, sel, timeout=3000, desc="gender male"):
                        break

            await page.wait_for_timeout(300)

            name_filled = False
            for sel in [
                "input[data-test='passenger-first-name-0']",
                "input[data-test*='first-name']",
                "input[placeholder='First name']",
            ]:
                if await safe_fill(page, sel, pax.get("given_name", "Test")):
                    name_filled = True
                    break

            for sel in [
                "input[data-test='passenger-last-name-0']",
                "input[data-test*='last-name']",
                "input[placeholder='Last name']",
            ]:
                if await safe_fill(page, sel, pax.get("family_name", "Traveler")):
                    break

            dob = pax.get("born_on", "1990-06-15")
            parts = dob.split("-")
            if len(parts) == 3:
                year, month, day = parts
                for sel in [
                    "input[data-test*='birth-day']",
                    "input[placeholder*='DD']",
                ]:
                    if await safe_fill(page, sel, day.lstrip("0") or day):
                        break
                for sel in [
                    "input[data-test*='birth-month']",
                    "input[placeholder*='MM']",
                ]:
                    if await safe_fill(page, sel, month.lstrip("0") or month):
                        break
                for sel in [
                    "input[data-test*='birth-year']",
                    "input[placeholder*='YYYY']",
                ]:
                    if await safe_fill(page, sel, year):
                        break

            nationality = pax.get("nationality")
            if nationality:
                for sel in [
                    "input[data-test*='nationality']",
                    "[data-test*='nationality'] input",
                ]:
                    if await safe_fill(page, sel, nationality):
                        await page.wait_for_timeout(500)
                        try:
                            await page.locator("[class*='dropdown'] [class*='item']:first-child").first.click(timeout=2000)
                        except Exception:
                            pass
                        break

            for sel in [
                "input[data-test*='contact-email']",
                "input[data-test*='email']",
                "input[type='email']",
            ]:
                if await safe_fill(page, sel, pax.get("email", "test@example.com")):
                    break

            for sel in [
                "input[data-test*='phone']",
                "input[type='tel']",
            ]:
                if await safe_fill(page, sel, pax.get("phone_number", "+441234567890")):
                    break

            return name_filled

        async def _handle_passengers_page_extras() -> None:
            await page.wait_for_timeout(1000)

            for sel in [
                "label[data-test='checkbox-label-no-checked-in-baggage']",
                "input[name='no-checked-in-baggage']",
            ]:
                if await safe_click(page, sel, timeout=3000, desc="no checked-in bag"):
                    logger.info("WizzAir checkout: declined checked bag")
                    break

            await page.wait_for_timeout(1000)

            cabin_container = page.locator("[data-test='cabin-baggage-and-priority-boarding']")
            try:
                if await cabin_container.count() > 0:
                    checked = await cabin_container.first.get_attribute("data-checked")
                    if checked == "false":
                        prio_btn = page.locator("button[data-test='add-wizz-priority']")
                        if await prio_btn.count() > 0:
                            await prio_btn.first.scroll_into_view_if_needed()
                            await prio_btn.first.click()
                            logger.info("WizzAir checkout: clicked priority to satisfy cabin bag validation")
                            await page.wait_for_timeout(2000)
                            dialog = page.locator(".dialog-container")
                            if await dialog.count() > 0:
                                try:
                                    if await dialog.first.is_visible(timeout=1000):
                                        await page.keyboard.press("Escape")
                                        await page.wait_for_timeout(1000)
                                except Exception:
                                    pass
            except Exception:
                pass

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)

            prm_card = page.locator("[data-test='common-prm-card']")
            try:
                if await prm_card.count() > 0:
                    no_label = prm_card.locator("label").filter(has_text="No")
                    if await no_label.count() > 0:
                        await no_label.click(force=True)
                        logger.info("WizzAir checkout: PRM declaration set to No")
            except Exception:
                pass

            await page.wait_for_timeout(1000)

        async def _continue_past_passengers() -> str:
            async def _is_passengers_page() -> bool:
                current_url = page.url.lower()
                if "/passengers" in current_url:
                    return True
                for sel in [
                    "input[data-test='passenger-first-name-0']",
                    "input[data-test*='first-name']",
                    "button[data-test='passengers-continue-btn']",
                ]:
                    try:
                        locator = page.locator(sel).first
                        if await locator.count() > 0 and await locator.is_visible(timeout=1000):
                            return True
                    except Exception:
                        continue
                return False

            if not await _is_passengers_page():
                logger.warning("WizzAir checkout: passenger page not active before continue")
                return "passenger_page_not_reached"

            cont_btn = page.locator("button[data-test='passengers-continue-btn']")
            try:
                if await cont_btn.count() > 0 and await cont_btn.is_visible(timeout=3000):
                    await cont_btn.scroll_into_view_if_needed()
                    await cont_btn.click()
                else:
                    await safe_click(page, "button:has-text('Continue')", desc="continue fallback")
            except Exception:
                await safe_click(page, "button:has-text('Continue')", desc="continue fallback")

            await page.wait_for_timeout(5000)

            login_modal = page.locator("[data-test='loginmodal-signin'], .dialog-container [data-test='loginmodal-signin']")
            try:
                if await login_modal.count() > 0 and await login_modal.is_visible(timeout=2000):
                    logger.info("WizzAir checkout: login modal detected")
                    return "login_required"
            except Exception:
                pass

            dialog = page.locator(".dialog-container")
            try:
                if await dialog.count() > 0 and await dialog.first.is_visible(timeout=1000):
                    has_login = await page.evaluate("""() => {
                        const dialogRoot = document.querySelector('.dialog-container');
                        return !!(dialogRoot && dialogRoot.textContent && (
                            dialogRoot.textContent.includes('Sign in') ||
                            dialogRoot.textContent.includes('Registration') ||
                            dialogRoot.textContent.includes('Forgot your password')
                        ));
                    }""")
                    if has_login:
                        logger.info("WizzAir checkout: login dialog detected")
                        return "login_required"
            except Exception:
                pass

            current_hash = page.url.split("#")[-1].lower()
            if "/passengers" not in current_hash:
                return "continued"

            await _handle_passengers_page_extras()
            await page.wait_for_timeout(2000)

            try:
                if await cont_btn.count() > 0 and await cont_btn.is_visible(timeout=2000):
                    await cont_btn.click()
                    await page.wait_for_timeout(5000)
            except Exception:
                pass

            try:
                if await login_modal.count() > 0 and await login_modal.is_visible(timeout=2000):
                    return "login_required"
            except Exception:
                pass

            current_hash = page.url.split("#")[-1].lower()
            return "continued" if "/passengers" not in current_hash else "stuck"

        async def _skip_extras() -> None:
            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)

            for sel in [
                "button:has-text('Continue')",
                "button:has-text('No, thanks')",
                "button:has-text('Skip')",
                "button[data-test*='continue']",
                "button[data-test*='skip']",
                "button:has-text('Continue without')",
                "button:has-text('No insurance')",
                "[data-test='insurance-decline']",
            ]:
                await safe_click(page, sel, timeout=3000, desc="skip extras")
                await page.wait_for_timeout(500)

            for _ in range(3):
                await dismiss_overlays(page)
                clicked = await safe_click(
                    page,
                    "button:has-text('Continue'), button[data-test*='continue']",
                    timeout=3000,
                    desc="extras continue",
                )
                if not clicked:
                    break
                await page.wait_for_timeout(1500)

        async def _skip_seats() -> None:
            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)

            for sel in [
                "button:has-text('Skip seat selection')",
                "button:has-text('Continue without seats')",
                "button:has-text('No, thanks')",
                "button:has-text('Skip')",
                "button[data-test*='skip-seat']",
                "[data-test*='seat-selection-decline']",
                "button:has-text('Continue')",
            ]:
                if await safe_click(page, sel, timeout=3000, desc="skip seats"):
                    await page.wait_for_timeout(1000)

            for sel in [
                "button:has-text('OK')",
                "button:has-text('Continue without')",
                "[data-test='modal-confirm']",
            ]:
                await safe_click(page, sel, timeout=2000, desc="confirm skip seats")

        async def _is_payment_page() -> bool:
            current_hash = page.url.split("#")[-1].lower()
            if "payment" in current_hash:
                return True
            for sel in [
                "input[data-test*='card-number']",
                "input[name*='cardNumber']",
                "iframe[src*='payment']",
                "iframe[title*='payment' i]",
                "[data-test='total-price']",
            ]:
                try:
                    locator = page.locator(sel).first
                    if await locator.count() > 0 and await locator.is_visible(timeout=1500):
                        return True
                except Exception:
                    continue
            return False

        try:
            captured_details = {}

            try:
                await page.evaluate("() => { try { UC_UI.acceptAllConsents(); } catch {} }")
            except Exception:
                pass
            await self._dismiss_cookies(page, config)
            await _dismiss_wizz_consent()

            logger.info("WizzAir checkout: driving SPA route for %s→%s on %s", origin, dest, dep_date)
            search_loaded = _aio.Event()

            async def _on_search_response(response):
                try:
                    if "/Api/search/search" in response.url and response.status == 200:
                        search_loaded.set()
                except Exception:
                    pass

            page.on("response", _on_search_response)

            try:
                await page.goto(booking_url, wait_until="domcontentloaded", timeout=config.goto_timeout)

                try:
                    await _aio.wait_for(search_loaded.wait(), timeout=20)
                except _aio.TimeoutError:
                    logger.debug("WizzAir checkout: search API timeout, retrying after overlay dismiss")
                    await dismiss_overlays(page)
                    await self._dismiss_cookies(page, config)
                    await page.goto(booking_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
                    try:
                        await _aio.wait_for(search_loaded.wait(), timeout=15)
                    except _aio.TimeoutError:
                        logger.warning("WizzAir checkout: search API did not respond after retry")
            finally:
                try:
                    page.remove_listener("response", _on_search_response)
                except Exception:
                    pass

            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)
            await self._dismiss_cookies(page, config)
            await _dismiss_wizz_consent()
            await _dismiss_wizz_urgency_modal()
            step = "flights_loaded"

            try:
                await page.wait_for_selector(
                    "button[data-test='select-fare'], button:has-text('SELECT'), [data-test='flight-select-outbound'], [class*='flight-select'], [class*='FlightSelect'], [class*='flight-row'], [data-test*='flight-card']",
                    timeout=15000,
                )
            except Exception:
                logger.warning("WizzAir checkout: flight cards not found after SPA navigation")

            await _select_flight(offer.get("outbound", {}), "outbound")
            if offer.get("inbound"):
                await page.wait_for_timeout(1500)
                await _select_flight(offer.get("inbound", {}), "return")
            step = "flights_selected"

            if not await _select_basic_fare():
                logger.warning("WizzAir checkout: BASIC fare selection did not confirm passenger form")
            step = "fare_selected"
            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)
            await self._dismiss_cookies(page, config)
            await _dismiss_wizz_consent()
            await _dismiss_wizz_urgency_modal()

            if await _fill_passenger_details():
                step = "passengers_filled"
            await _handle_passengers_page_extras()
            captured_details = self._merge_checkout_details(captured_details, await _extract_visible_checkout_details())

            passenger_state = await _continue_past_passengers()
            if passenger_state == "passenger_page_not_reached":
                screenshot = await take_screenshot_b64(page)
                elapsed = time.monotonic() - t0
                page_price = await _extract_price()
                return CheckoutProgress(
                    status="failed",
                    step=step,
                    airline=config.airline_name,
                    source=config.source_tag,
                    offer_id=offer_id,
                    total_price=page_price,
                    currency=offer.get("currency", "EUR"),
                    booking_url=page.url or booking_url,
                    screenshot_b64=screenshot,
                    message="Wizz Air checkout did not reach passenger details after flight and fare selection.",
                    can_complete_manually=True,
                    elapsed_seconds=elapsed,
                    details=self._merge_checkout_details(captured_details, {"blocker": "passenger_page_not_reached", "checkout_page": "select-flight"}),
                )
            if passenger_state == "login_required":
                screenshot = await take_screenshot_b64(page)
                elapsed = time.monotonic() - t0
                page_price = await _extract_price()
                return CheckoutProgress(
                    status="failed",
                    step=step,
                    airline=config.airline_name,
                    source=config.source_tag,
                    offer_id=offer_id,
                    total_price=page_price,
                    currency=offer.get("currency", "EUR"),
                    booking_url=page.url or booking_url,
                    screenshot_b64=screenshot,
                    message="Wizz Air requires sign-in/registration after passenger details; checkout could not continue to payment in safe mode.",
                    can_complete_manually=True,
                    elapsed_seconds=elapsed,
                    details=self._merge_checkout_details(captured_details, {"blocker": "login_required", "login_required": True, "checkout_page": "passengers"}),
                )
            if passenger_state == "stuck":
                screenshot = await take_screenshot_b64(page)
                elapsed = time.monotonic() - t0
                page_price = await _extract_price()
                return CheckoutProgress(
                    status="failed",
                    step=step,
                    airline=config.airline_name,
                    source=config.source_tag,
                    offer_id=offer_id,
                    total_price=page_price,
                    currency=offer.get("currency", "EUR"),
                    booking_url=page.url or booking_url,
                    screenshot_b64=screenshot,
                    message="Wizz Air checkout remained on passenger details after filling the form.",
                    can_complete_manually=True,
                    elapsed_seconds=elapsed,
                    details=self._merge_checkout_details(captured_details, {"blocker": "passengers_validation", "checkout_page": "passengers"}),
                )

            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)
            await self._dismiss_cookies(page, config)

            await _skip_extras()
            step = "extras_skipped"

            await _skip_seats()
            step = "seats_skipped"
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)
            await self._dismiss_cookies(page, config)
            captured_details = self._merge_checkout_details(captured_details, await _extract_visible_checkout_details())

            if not await _is_payment_page():
                screenshot = await take_screenshot_b64(page)
                elapsed = time.monotonic() - t0
                return CheckoutProgress(
                    status="failed",
                    step=step,
                    airline=config.airline_name,
                    source=config.source_tag,
                    offer_id=offer_id,
                    booking_url=page.url or booking_url,
                    screenshot_b64=screenshot,
                    message="Wizz Air checkout advanced past extras but did not reach a detectable payment page.",
                    can_complete_manually=True,
                    elapsed_seconds=elapsed,
                    details=captured_details,
                )

            step = "payment_page_reached"
            screenshot = await take_screenshot_b64(page)
            page_price = await _extract_price()
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
                booking_url=page.url or booking_url,
                screenshot_b64=screenshot,
                message=(
                    f"Wizz Air checkout complete — reached payment page in {elapsed:.0f}s. "
                    f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                    "Payment NOT submitted (safe mode)."
                ),
                can_complete_manually=True,
                elapsed_seconds=elapsed,
                details=self._merge_checkout_details(captured_details, {"checkout_page": "payment"}),
            )
        except Exception as e:
            logger.error("Wizzair checkout error: %s", e, exc_info=True)
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
                booking_url=page.url or booking_url,
                screenshot_b64=screenshot,
                message=f"Checkout error at step '{step}': {e}",
                elapsed_seconds=time.monotonic() - t0,
            )

    async def _jetstar_checkout(self, page, config, offer, offer_id, booking_url, passengers, t0):
        """Jetstar custom checkout for Navitaire's flight → bags → seats → details flow."""
        from .browser import auto_block_if_proxied
        from . import jetstar as jetstar_module
        from .jetstar import (
            JetstarConnectorClient,
            _get_browser as _get_jetstar_browser,
        )

        pax = passengers[0] if passengers else FAKE_PASSENGER
        helper = JetstarConnectorClient()
        step = "started"
        checkout_page = page
        owns_page = False
        captured_details: dict[str, Any] = {}
        debug_info: dict[str, Any] = {}

        def _dedupe_detail_items(items: list[dict], *, limit: int = 20) -> list[dict]:
            seen: set[tuple] = set()
            deduped: list[dict] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                label = str(item.get("label") or item.get("text") or "").strip().lower()
                amount = item.get("amount")
                try:
                    amount = round(float(amount), 2) if amount is not None else None
                except Exception:
                    amount = None
                key = (
                    label,
                    str(item.get("type") or "").strip().lower(),
                    str(item.get("currency") or "").strip().upper(),
                    amount,
                    bool(item.get("included")),
                )
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(item)
                if len(deduped) >= limit:
                    break
            return deduped

        def _merge_jetstar_details(existing: dict, extracted: dict) -> dict:
            merged = self._merge_checkout_details(
                existing,
                {
                    key: value
                    for key, value in (extracted or {}).items()
                    if key not in {"available_add_ons", "price_breakdown", "visible_price_options"}
                },
            )

            for key in ("price_breakdown", "visible_price_options"):
                combined: list[dict] = []
                if isinstance(existing.get(key), list):
                    combined.extend(existing[key])
                if isinstance(extracted.get(key), list):
                    combined.extend(extracted[key])
                if combined:
                    merged[key] = _dedupe_detail_items(combined)

            existing_add_ons = existing.get("available_add_ons") if isinstance(existing.get("available_add_ons"), dict) else {}
            extracted_add_ons = extracted.get("available_add_ons") if isinstance(extracted.get("available_add_ons"), dict) else {}
            merged_add_ons: dict[str, list[dict]] = {}
            for category in sorted(set(existing_add_ons) | set(extracted_add_ons)):
                combined: list[dict] = []
                if isinstance(existing_add_ons.get(category), list):
                    combined.extend(existing_add_ons[category])
                if isinstance(extracted_add_ons.get(category), list):
                    combined.extend(extracted_add_ons[category])
                if combined:
                    merged_add_ons[category] = _dedupe_detail_items(combined, limit=12)
            if merged_add_ons:
                merged["available_add_ons"] = merged_add_ons

            return merged

        async def _body_text() -> str:
            try:
                return await page.evaluate("() => (document.body?.innerText || '')")
            except Exception:
                return ""

        async def _snapshot_details() -> dict:
            body = await _body_text()
            title = ""
            try:
                title = await page.title()
            except Exception:
                pass
            actions: list[str] = []
            try:
                actions = await page.evaluate(
                    r"""(limit) => Array.from(document.querySelectorAll('button, [role="button"], a[role="button"], input[type="submit"], a'))
                        .filter(el => {
                            const text = (el.innerText || el.textContent || el.value || '').trim();
                            const visible = !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
                            return visible && text;
                        })
                        .map(el => (el.innerText || el.textContent || el.value || '').trim().replace(/\s+/g, ' ').slice(0, 140))
                        .slice(0, limit)""",
                    20,
                )
            except Exception:
                pass
            return {
                "current_url": page.url,
                "page_title": title,
                "visible_actions": actions,
                "body_snippet": " ".join(body.split())[:1200],
            }

        async def _extract_price() -> float:
            patterns = [
                r"Your booking total\s*\$\s*([\d,]+(?:\.\d+)?)\s*([A-Z]{3})?",
                r"\$\s*([\d,]+(?:\.\d+)?)\s*(AUD|NZD|USD|EUR|GBP)",
            ]
            texts: list[str] = []
            for selector in [
                ".qa-cart",
                "[class*='cart']",
                "[class*='booking-total']",
                "[class*='summary']",
            ]:
                try:
                    el = page.locator(selector).first
                    if await el.is_visible(timeout=500):
                        text = await el.text_content()
                        if text:
                            texts.append(text)
                except Exception:
                    continue
            texts.append(await _body_text())
            for text in texts:
                for pattern in patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        try:
                            return float(match.group(1).replace(",", ""))
                        except Exception:
                            continue
            try:
                return float(offer.get("price", 0.0) or 0.0)
            except Exception:
                return 0.0

        async def _extract_jetstar_checkout_details() -> dict:
            return await page.evaluate(
                r'''(defaultCurrency) => {
                    const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                    const isVisible = (element) => !!(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));

                    const parseMoney = (text) => {
                        const normalized = normalize(text);
                        if (!normalized) return null;

                        let match = normalized.match(/\b(AUD|NZD|USD|EUR|GBP)\s*([\d,]+(?:\.\d{1,2})?)/i);
                        if (match) {
                            return {
                                currency: match[1].toUpperCase(),
                                amount: parseFloat(match[2].replace(/,/g, '')),
                            };
                        }

                        match = normalized.match(/\$\s*([\d,]+(?:\.\d{1,2})?)/);
                        if (match) {
                            return {
                                currency: (defaultCurrency || 'AUD').toUpperCase(),
                                amount: parseFloat(match[1].replace(/,/g, '')),
                            };
                        }

                        return null;
                    };

                    const cleanLabel = (text) => normalize(
                        (text || '')
                            .replace(/\b(AUD|NZD|USD|EUR|GBP)\s*[\d,]+(?:\.\d{1,2})?/gi, '')
                            .replace(/\$\s*[\d,]+(?:\.\d{1,2})?/g, '')
                            .replace(/\b(select|continue|skip)\b/gi, '')
                    );

                    const dedupe = (items, limit = 20) => {
                        const seen = new Set();
                        const result = [];
                        for (const item of items) {
                            const key = JSON.stringify([
                                normalize(item.label || item.text || '').toLowerCase(),
                                normalize(item.type || '').toLowerCase(),
                                item.currency || '',
                                typeof item.amount === 'number' ? Number(item.amount.toFixed(2)) : null,
                                !!item.included,
                            ]);
                            if (seen.has(key)) continue;
                            seen.add(key);
                            result.push(item);
                            if (result.length >= limit) break;
                        }
                        return result;
                    };

                    const result = {};
                    const bodyText = normalize(document.body?.innerText || '');
                    const currentUrl = (location.href || '').toLowerCase();
                    const title = normalize(document.title || '').toLowerCase();

                    const onSeatPage = /skip seats for this flight|i don't mind where i sit|seat map|select your seat|seat selection/i.test(bodyText)
                        || /\/booking\/seats/.test(currentUrl)
                        || /flight booking - seats/.test(title);
                    const onExtrasPage = /\/booking\/extras/.test(currentUrl)
                        || /flight booking - extras/.test(title)
                        || /continue to booking details|food and drink|travel insurance|insurance|hotels|car hire|car rental|extras/i.test(bodyText);
                    const onBaggagePage = (!onExtrasPage) && (
                        /select a checked baggage option to continue|select a carry-on baggage option to continue|more baggage options|use points plus pay/i.test(bodyText)
                        || /\/booking\/baggage/.test(currentUrl)
                        || /flight booking - baggage/.test(title)
                    );

                    if (onBaggagePage) {
                        result.checkout_page = 'baggage';
                    } else if (onSeatPage) {
                        result.checkout_page = 'seats';
                    } else if (onExtrasPage) {
                        result.checkout_page = 'extras';
                    } else if (/booking details|contact details|passenger details/i.test(bodyText)) {
                        result.checkout_page = 'booking_details';
                    } else if (/payment|review and pay|review & pay/i.test(bodyText + ' ' + currentUrl + ' ' + title)) {
                        result.checkout_page = 'payment';
                    } else if (/starter|starter plus|flex plus|bundle/i.test(bodyText)) {
                        result.checkout_page = 'bundles';
                    }

                    const summaryContainers = Array.from(document.querySelectorAll('.qa-cart, [class*="cart"], [class*="summary"], [class*="booking-total"]'))
                        .filter(isVisible);
                    const summaryItems = [];
                    for (const container of summaryContainers) {
                        const lines = normalize(container.innerText || '').split(/\n+/).map(normalize).filter(Boolean);
                        let pendingLabel = '';
                        for (const line of lines) {
                            const money = parseMoney(line);
                            if (money) {
                                const label = cleanLabel(line) || pendingLabel || 'Line item';
                                summaryItems.push({
                                    label,
                                    currency: money.currency,
                                    amount: money.amount,
                                });
                                pendingLabel = '';
                                continue;
                            }
                            if (!/^(your booking total|booking total|total)$/i.test(line)) {
                                pendingLabel = pendingLabel ? normalize(`${pendingLabel} ${line}`) : line;
                            }
                        }
                    }

                    const displayTotal = summaryItems.find((item) => /total/i.test(item.label));
                    if (displayTotal) {
                        result.display_total = displayTotal;
                    }
                    const priceBreakdown = summaryItems.filter((item) => !displayTotal || item !== displayTotal);
                    if (priceBreakdown.length) {
                        result.price_breakdown = dedupe(priceBreakdown, 12);
                    }

                    const candidateSelectors = [
                        'button',
                        '[role="button"]',
                        'a[role="button"]',
                        'label',
                        '[class*="card"]',
                        '[class*="tile"]',
                        '[class*="option"]',
                        '[class*="seat"]',
                        '[class*="bag"]',
                        '[class*="baggage"]',
                        '[class*="summary"]',
                    ];
                    const candidates = Array.from(document.querySelectorAll(candidateSelectors.join(',')));

                    const categorized = {
                        packages: [],
                        baggage: [],
                        seat_selection: [],
                        extras: [],
                    };
                    const visiblePrices = [];

                    for (const element of candidates) {
                        if (!isVisible(element)) continue;
                        const text = normalize(element.innerText || element.textContent || element.getAttribute('aria-label') || '');
                        if (!text || text.length > 180) continue;

                        const money = parseMoney(text);
                        const included = /included|free|no checked baggage|i don't mind where i sit/i.test(text);
                        if (!money && !included) continue;

                        const haystack = normalize(`${element.className || ''} ${element.getAttribute('data-test') || ''} ${element.getAttribute('aria-label') || ''} ${text}`).toLowerCase();
                        const item = {
                            label: cleanLabel(text) || text,
                            text,
                            currency: money?.currency || (defaultCurrency || 'AUD').toUpperCase(),
                            amount: money?.amount,
                            included,
                        };

                        visiblePrices.push(item);

                        const addOnNoise = /your booking total|continue to |review & pay|change search|edit flight time|fees and charges/i.test(text);

                        if (!addOnNoise && /starter|starter plus|flex|bundle/.test(haystack)) {
                            categorized.packages.push({ ...item, type: 'package' });
                        }
                        if (!addOnNoise && (/bag|baggage|carry[- ]?on|checked|\b\d+kg\b/.test(haystack))) {
                            categorized.baggage.push({
                                ...item,
                                type: /carry[- ]?on/.test(haystack) ? 'cabin_bag' : 'checked_bag',
                            });
                        }
                        if (!addOnNoise && (/up front|extra legroom|standard seat|seat selection|seat map|i don't mind where i sit/.test(haystack))) {
                            categorized.seat_selection.push({ ...item, type: 'seat_selection' });
                        }
                        if (!addOnNoise && (/food and drink|meal|insurance|hotel|car hire|car rental|transfer|lounge|wifi|wi-fi|voucher|extras/.test(haystack))) {
                            categorized.extras.push({ ...item, type: 'extra' });
                        }
                    }

                    const availableAddOns = {};
                    for (const [category, items] of Object.entries(categorized)) {
                        const deduped = dedupe(items, 10);
                        if (deduped.length) {
                            availableAddOns[category] = deduped;
                        }
                    }
                    if (Object.keys(availableAddOns).length) {
                        result.available_add_ons = availableAddOns;
                    }

                    const dedupedVisiblePrices = dedupe(visiblePrices, 20);
                    if (dedupedVisiblePrices.length) {
                        result.visible_price_options = dedupedVisiblePrices;
                    }

                    return result;
                }''',
                str(offer.get("currency") or "AUD"),
            )

        async def _capture_checkout_details(label: str) -> None:
            nonlocal captured_details
            extracted = await _extract_jetstar_checkout_details()
            if extracted:
                captured_details = _merge_jetstar_details(captured_details, extracted)
                debug_info[f"captured_details_{label}"] = {
                    key: (sorted(value.keys()) if isinstance(value, dict) else len(value) if isinstance(value, list) else value)
                    for key, value in extracted.items()
                }

        async def _result(status: str, message: str, *, allow_manual: bool = True) -> CheckoutProgress:
            screenshot = ""
            try:
                screenshot = await take_screenshot_b64(page)
            except Exception:
                pass
            details = _merge_jetstar_details(captured_details, await _snapshot_details())
            return CheckoutProgress(
                status=status,
                step=step,
                step_index=CHECKOUT_STEPS.index(step) if step in CHECKOUT_STEPS else 0,
                airline=config.airline_name,
                source=config.source_tag,
                offer_id=offer_id,
                total_price=await _extract_price(),
                currency=offer.get("currency", "AUD"),
                booking_url=page.url or booking_url,
                screenshot_b64=screenshot,
                message=message,
                can_complete_manually=allow_manual,
                elapsed_seconds=time.monotonic() - t0,
                details={**details, "jetstar_debug": debug_info},
            )

        conditions = offer.get("conditions") if isinstance(offer, dict) else {}
        fare_note = str((conditions or {}).get("fare_note") or "").strip().lower()
        cached_fare_offer = str(offer_id or "").startswith("jq_sale_") or "fare-cache" in fare_note
        if cached_fare_offer:
            debug_info["cached_fare_offer"] = True
            debug_info["fare_note"] = fare_note
            return await _result(
                "failed",
                "Jetstar checkout requires a live booking-session offer. Search fell back to Jetstar's public fare cache, so ancillaries cannot be driven from this offer.",
                allow_manual=False,
            )

        async def _click_action_card(text_fragment: str) -> bool:
            selectors = [
                ".anchor-module_buttonLink__tLcNb",
                "[class*='anchor-module_buttonLink']",
                "[class*='buttonLink']",
                "button",
                "[role='button']",
            ]
            for selector in selectors:
                try:
                    cards = page.locator(selector)
                    count = await cards.count()
                except Exception:
                    continue
                for index in range(count):
                    try:
                        card = cards.nth(index)
                        if not await card.is_visible(timeout=600):
                            continue
                        text = " ".join(((await card.inner_text(timeout=1000)) or "").split())
                        if text_fragment.lower() not in text.lower():
                            continue
                        await card.click(timeout=4000)
                        await page.wait_for_timeout(1000)
                        return True
                    except Exception:
                        continue
            return False

        async def _click_first(
            selectors: list[str], *, timeout: int, desc: str, wait_ms: int = 1500, dismiss_after: bool = True
        ) -> bool:
            clicked = await safe_click_first(page, selectors, timeout=timeout, desc=desc)
            if clicked:
                await page.wait_for_timeout(wait_ms)
                if dismiss_after:
                    await helper._dismiss_overlays(page)
            return clicked

        async def _page_has_any_text(fragments: list[str]) -> bool:
            details = await _snapshot_details()
            haystack = " ".join([
                details["page_title"],
                details["body_snippet"],
                " ".join(details["visible_actions"]),
            ]).lower()
            return any(fragment.lower() in haystack for fragment in fragments)

        async def _has_any_selector(selectors: list[str]) -> bool:
            for selector in selectors:
                try:
                    if await page.locator(selector).count() > 0:
                        return True
                except Exception:
                    continue
            return False

        async def _is_baggage_page() -> bool:
            details = await _snapshot_details()
            current_url = details["current_url"].lower()
            title = details["page_title"].lower()
            haystack = " ".join([
                current_url,
                title,
                details["body_snippet"],
                " ".join(details["visible_actions"]),
            ]).lower()

            if "select-flights" in current_url or "flight booking - flight select" in title:
                return False
            if (
                "/booking/seats" in current_url
                or "flight booking - seats" in title
                or "choose your seats" in haystack
            ):
                return False
            if "/booking/bags" in current_url or "flight booking - baggage" in title:
                return True

            return any(fragment in haystack for fragment in [
                "continue to seats",
                "no checked baggage",
                "included in starter",
                "7kg across 2 items",
            ])

        async def _wait_for_jetstar_seat_page(timeout_ms: int = 15000) -> bool:
            deadline = time.monotonic() + (timeout_ms / 1000)
            while time.monotonic() < deadline:
                details = await _snapshot_details()
                haystack = " ".join([
                    details["current_url"],
                    details["page_title"],
                    details["body_snippet"],
                    " ".join(details["visible_actions"]),
                ]).lower()
                if (
                    "flight booking - seats" in haystack
                    or "/booking/seats" in haystack
                    or "choose your seats" in haystack
                ):
                    return True
                await dismiss_overlays(page)
                await page.wait_for_timeout(1500)
            return False

        async def _scroll_jetstar_page() -> None:
            for top in (0, 500, 1100, 1700, 2300, 2900, 3600):
                try:
                    await page.evaluate("(y) => window.scrollTo(0, y)", top)
                except Exception:
                    continue
                await page.wait_for_timeout(250)
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(400)
            except Exception:
                pass

        async def _wait_for_jetstar_seat_controls(timeout_ms: int = 12000) -> bool:
            control_fragments = [
                "skip seats for this flight",
                "continue to extras",
                "you must select an option before you can continue",
                "choose seat",
                "seating options",
                "extra legroom$",
                "upfront$",
                "standard$",
            ]
            deadline = time.monotonic() + (timeout_ms / 1000)
            while time.monotonic() < deadline:
                details = await _snapshot_details()
                haystack = " ".join([
                    details["current_url"],
                    details["page_title"],
                    details["body_snippet"],
                    " ".join(details["visible_actions"]),
                ]).lower()
                if "#a320" in details["current_url"].lower() or any(fragment in haystack for fragment in control_fragments):
                    return True
                await _scroll_jetstar_page()
                await helper._dismiss_overlays(page)
                await dismiss_overlays(page)
                await page.wait_for_timeout(1200)
            return False

        async def _jetstar_seat_choice_completed() -> bool:
            details = await _snapshot_details()
            haystack = " ".join([
                details["current_url"],
                details["page_title"],
                details["body_snippet"],
                " ".join(details["visible_actions"]),
            ]).lower()
            if "/booking/extras" in details["current_url"].lower() or "flight booking - extras" in haystack:
                return True
            if await _is_booking_details_page() or await _is_payment_page():
                return True
            if "you must select an option before you can continue" in haystack:
                return False

            try:
                continue_button = page.locator("button:has-text('Continue to extras')").first
                if await continue_button.is_visible(timeout=400):
                    class_name = ((await continue_button.get_attribute("class")) or "").lower()
                    aria_disabled = ((await continue_button.get_attribute("aria-disabled")) or "").lower()
                    disabled_attr = await continue_button.get_attribute("disabled")
                    if "disabled" not in class_name and aria_disabled != "true" and disabled_attr is None:
                        return True
            except Exception:
                pass

            return False

        async def _jetstar_seat_choice_candidates() -> list[dict[str, Any]]:
            try:
                return await page.evaluate(
                    r'''() => {
                        const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                        const isVisible = (element) => !!(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));

                        const containers = Array.from(document.querySelectorAll('fieldset, section, form, div'))
                            .filter((element) => {
                                const text = normalize(element.innerText || '');
                                return text && /choose seat|i don't mind where i sit|randomly allocated at check-in|skip seats for this flight/i.test(text);
                            })
                            .slice(0, 4);

                        return containers.map((container, index) => ({
                            index: String(index),
                            tag: container.tagName.toLowerCase(),
                            text: normalize(container.innerText || '').slice(0, 400),
                            controls: Array.from(container.querySelectorAll('input, label, button, [role="radio"]'))
                                .slice(0, 10)
                                .map((element) => ({
                                    tag: element.tagName.toLowerCase(),
                                    type: element.getAttribute('type') || '',
                                    id: element.id || '',
                                    name: element.getAttribute('name') || '',
                                    value: element.getAttribute('value') || '',
                                    role: element.getAttribute('role') || '',
                                    text: normalize(element.innerText || element.textContent || element.getAttribute('aria-label') || '').slice(0, 200),
                                    ariaChecked: element.getAttribute('aria-checked') || '',
                                    checked: element instanceof HTMLInputElement ? element.checked : false,
                                    disabled: element instanceof HTMLInputElement || element instanceof HTMLButtonElement ? element.disabled : false,
                                })),
                        }));
                    }'''
                )
            except Exception:
                return []

        async def _click_jetstar_random_seat_control() -> bool:
            try:
                clicked = await page.evaluate(
                    r'''() => {
                        const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
                        const isVisible = (element) => !!(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));
                        const fire = (element, type) => {
                            element.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
                        };
                        const activate = (element) => {
                            if (!element) return false;
                            try {
                                if (typeof element.focus === 'function') {
                                    element.focus();
                                }
                            } catch (error) {
                            }
                            try {
                                fire(element, 'mousedown');
                                fire(element, 'mouseup');
                                fire(element, 'click');
                            } catch (error) {
                            }
                            try {
                                if (typeof element.click === 'function') {
                                    element.click();
                                }
                            } catch (error) {
                            }
                            return true;
                        };

                        const labelCandidates = Array.from(document.querySelectorAll('label, [role="radio"], button, div, span, a'));
                        for (const element of labelCandidates) {
                            if (!isVisible(element)) continue;
                            const text = normalize(element.innerText || element.textContent || element.getAttribute('aria-label') || '');
                            if (!text.includes("i don't mind where i sit") && !text.includes('i don’t mind where i sit')) continue;

                            const nestedInput = element.querySelector('input');
                            if (nestedInput) {
                                try {
                                    nestedInput.checked = true;
                                } catch (error) {
                                }
                                activate(nestedInput);
                                nestedInput.dispatchEvent(new Event('input', { bubbles: true }));
                                nestedInput.dispatchEvent(new Event('change', { bubbles: true }));
                                return true;
                            }

                            const htmlFor = element.getAttribute('for');
                            if (htmlFor) {
                                const target = document.getElementById(htmlFor);
                                if (target) {
                                    try {
                                        if ('checked' in target) {
                                            target.checked = true;
                                        }
                                    } catch (error) {
                                    }
                                    activate(target);
                                    target.dispatchEvent(new Event('input', { bubbles: true }));
                                    target.dispatchEvent(new Event('change', { bubbles: true }));
                                    return true;
                                }
                            }

                            const roleRadio = element.closest('[role="radio"]');
                            if (roleRadio) {
                                activate(roleRadio);
                                roleRadio.dispatchEvent(new Event('input', { bubbles: true }));
                                roleRadio.dispatchEvent(new Event('change', { bubbles: true }));
                                return true;
                            }

                            if (activate(element)) {
                                return true;
                            }
                        }

                        return false;
                    }'''
                )
                if clicked:
                    await page.wait_for_timeout(1500)
                    return True
            except Exception:
                pass
            return False

        async def _click_jetstar_skip_seats() -> bool:
            clicked = await _click_first([
                "button:has-text('Skip seats for this flight')",
            ], timeout=4000, desc="skip Jetstar seats", wait_ms=1500)
            if clicked:
                return True

            try:
                clicked = bool(await page.evaluate(
                    r'''() => {
                        const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
                        const isVisible = (element) => !!(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));
                        for (const element of Array.from(document.querySelectorAll('button, [role="button"], a, div, span'))) {
                            if (!isVisible(element)) continue;
                            const text = normalize(element.innerText || element.textContent || element.getAttribute('aria-label') || '');
                            if (text !== 'skip seats for this flight') continue;
                            const clickable = element.closest('button, [role="button"], a') || element;
                            clickable.click();
                            return true;
                        }
                        return false;
                    }'''
                ))
            except Exception:
                clicked = False

            if clicked:
                await page.wait_for_timeout(1500)
            return clicked

        async def _click_jetstar_continue_to_extras() -> bool:
            if await _is_booking_details_page() or await _is_payment_page():
                return True

            clicked = await _click_first([
                "button:has-text('Continue to extras')",
            ], timeout=4000, desc="continue Jetstar seats to extras", wait_ms=2000)
            if clicked:
                return True

            try:
                clicked = bool(await page.evaluate(
                    r'''() => {
                        const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
                        const isVisible = (element) => !!(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));
                        for (const element of Array.from(document.querySelectorAll('button, [role="button"], a, div, span'))) {
                            if (!isVisible(element)) continue;
                            const text = normalize(element.innerText || element.textContent || element.getAttribute('aria-label') || '');
                            if (text !== 'continue to extras') continue;
                            const clickable = element.closest('button, [role="button"], a') || element;
                            clickable.click();
                            return true;
                        }
                        return false;
                    }'''
                ))
            except Exception:
                clicked = False

            if clicked:
                await page.wait_for_timeout(2000)
            return clicked

        async def _click_jetstar_seat_option() -> bool:
            fragments = [
                "Don't mind where you sit",
                "Don’t mind where you sit",
                "randomly allocated at no extra cost",
                "Skip seats for this flight",
                "I don't mind where I sit",
                "I don’t mind where I sit",
            ]

            for _ in range(3):
                if await _jetstar_seat_choice_completed():
                    return True

                if await _click_jetstar_skip_seats():
                    await _click_jetstar_continue_to_extras()
                    if await _jetstar_seat_choice_completed():
                        return True

                if await _click_jetstar_random_seat_control():
                    await _click_jetstar_skip_seats()
                    await _click_jetstar_continue_to_extras()
                    if await _jetstar_seat_choice_completed():
                        return True

                selected_random_option = await _click_first([
                    "label:has-text('I don't mind where I sit')",
                    "label:has-text('I don’t mind where I sit')",
                    "text=I don't mind where I sit",
                    "text=I don’t mind where I sit",
                    "text=Seats for this flight will be randomly allocated at check-in at no extra cost.",
                ], timeout=4000, desc="select Jetstar random seat option", wait_ms=1200)

                if selected_random_option:
                    await _click_jetstar_skip_seats()
                    await _click_jetstar_continue_to_extras()
                    if await _jetstar_seat_choice_completed():
                        return True

                if (
                    await _click_action_card("Don't mind where you sit")
                    or await _click_action_card("Don’t mind where you sit")
                    or await _click_action_card("randomly allocated at no extra cost")
                ):
                    await page.wait_for_timeout(1200)
                    await _click_jetstar_skip_seats()
                    await _click_jetstar_continue_to_extras()
                    if await _jetstar_seat_choice_completed():
                        return True

                if await _click_first([
                    "button:has-text('I don't mind where I sit')",
                    "button:has-text('I don’t mind where I sit')",
                    "text=Don't mind where you sit",
                    "text=Don’t mind where you sit",
                ], timeout=3000, desc="skip Jetstar seats", wait_ms=1500):
                    await _click_jetstar_skip_seats()
                    await _click_jetstar_continue_to_extras()
                    if await _jetstar_seat_choice_completed():
                        return True

                try:
                    clicked = await page.evaluate(
                        r'''(targets) => {
                            const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                            const isVisible = (element) => !!(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));
                            const matches = (text) => targets.some(target => normalize(text).toLowerCase().includes(target.toLowerCase()));

                            const candidates = Array.from(document.querySelectorAll('button, [role="button"], label, [role="radio"], a, div, span'));
                            for (const candidate of candidates) {
                                if (!isVisible(candidate)) continue;
                                const text = normalize(candidate.innerText || candidate.textContent || candidate.getAttribute('aria-label') || '');
                                if (!text || text.length > 220 || !matches(text)) continue;
                                const clickable = candidate.closest('button, [role="button"], label, [role="radio"], a') || candidate;
                                clickable.click();
                                return text;
                            }
                            return '';
                        }''',
                        fragments,
                    )
                    if clicked:
                        await page.wait_for_timeout(1500)
                        await _click_jetstar_skip_seats()
                        await _click_jetstar_continue_to_extras()
                        if await _jetstar_seat_choice_completed():
                            return True
                except Exception:
                    pass

                try:
                    clicked_paid_seat = await page.evaluate(
                        r'''() => {
                            const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
                            const isVisible = (element) => !!(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));
                            const candidates = Array.from(document.querySelectorAll('button, [role="button"], a, label, div, span'));
                            for (const element of candidates) {
                                if (!isVisible(element)) continue;
                                const text = normalize(element.innerText || element.textContent || element.getAttribute('aria-label') || '');
                                if (!text || text.length > 120) continue;
                                if (!/(standard|upfront|extra legroom)\$\d/.test(text)) continue;
                                const clickable = element.closest('button, [role="button"], a, label') || element;
                                clickable.click();
                                return text;
                            }
                            return '';
                        }'''
                    )
                    if clicked_paid_seat:
                        await page.wait_for_timeout(1500)
                        if await _jetstar_seat_choice_completed():
                            return True
                    
                except Exception:
                    pass

                await _scroll_jetstar_page()
                await helper._dismiss_overlays(page)
                await page.wait_for_timeout(1200)

            return await _jetstar_seat_choice_completed()

        async def _wait_for_jetstar_extras_controls(timeout_ms: int = 12000) -> bool:
            control_fragments = [
                "continue to booking details",
                "continue to review",
                "continue to payment",
                "no thanks",
                "not now",
                "food and drink",
                "travel insurance",
                "hotel",
                "extras",
            ]
            deadline = time.monotonic() + (timeout_ms / 1000)
            while time.monotonic() < deadline:
                if await _is_booking_details_page() or await _is_payment_page():
                    return True
                details = await _snapshot_details()
                haystack = " ".join([
                    details["current_url"],
                    details["page_title"],
                    details["body_snippet"],
                    " ".join(details["visible_actions"]),
                ]).lower()
                if any(fragment in haystack for fragment in control_fragments):
                    return True
                await _scroll_jetstar_page()
                await helper._dismiss_overlays(page)
                await dismiss_overlays(page)
                await page.wait_for_timeout(1200)
            return False

        async def _advance_jetstar_extras() -> None:
            for attempt in range(6):
                if await _is_payment_page() or await _is_booking_details_page():
                    return

                await _scroll_jetstar_page()
                clicked = await _click_first([
                    "button:has-text('Continue to booking details')",
                    "button:has-text('Continue to review')",
                    "button:has-text('Continue to payment')",
                    "input[type='submit'][value*='Continue']",
                    "button[aria-label*='Continue']",
                    "button:has-text('Continue to extras')",
                    "button:has-text('No thanks')",
                    "button:has-text('Not now')",
                    "button:has-text('Continue')",
                ], timeout=5000, desc="advance Jetstar extras", wait_ms=3500)

                if not clicked:
                    try:
                        clicked = bool(await page.evaluate(
                            r'''() => {
                                const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
                                const isVisible = (element) => !!(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));
                                const targets = [
                                    'continue to booking details',
                                    'continue to review',
                                    'continue to payment',
                                    'continue to extras',
                                    'no thanks',
                                    'not now',
                                    'continue',
                                ];
                                const elements = Array.from(document.querySelectorAll('button, [role="button"], a, label, div, span, input[type="submit"], input[type="button"]'));
                                elements.sort((left, right) => {
                                    const leftRect = left.getBoundingClientRect();
                                    const rightRect = right.getBoundingClientRect();
                                    return rightRect.top - leftRect.top;
                                });
                                for (const element of elements) {
                                    if (!isVisible(element)) continue;
                                    const text = normalize(
                                        element.innerText
                                        || element.textContent
                                        || element.getAttribute('aria-label')
                                        || element.getAttribute('value')
                                        || ''
                                    );
                                    if (!text || text.length > 220) continue;
                                    if (!targets.some(target => text.includes(target))) continue;
                                    element.scrollIntoView({block: 'center'});
                                    const clickable = element.closest('button, [role="button"], a, label') || element;
                                    clickable.click();
                                    return true;
                                }
                                return false;
                            }'''
                        ))
                    except Exception:
                        clicked = False

                    if clicked:
                        await page.wait_for_timeout(2500)

                await _capture_checkout_details(f"extras_{attempt + 1}")

                if not clicked:
                    return

        async def _jetstar_select_candidates() -> list[dict[str, str]]:
            try:
                return await page.evaluate(
                    r"""() => Array.from(document.querySelectorAll('button'))
                        .filter(btn => ((btn.innerText || btn.textContent || '').trim().toLowerCase() === 'select'))
                        .map((btn, index) => {
                            let container = btn;
                            for (let i = 0; i < 4 && container?.parentElement; i += 1) {
                                container = container.parentElement;
                            }
                            return {
                                index: String(index),
                                text: (btn.innerText || btn.textContent || '').trim().replace(/\s+/g, ' '),
                                aria_label: (btn.getAttribute('aria-label') || '').trim(),
                                context: ((container?.innerText || '') || '').trim().replace(/\s+/g, ' ').slice(0, 400),
                            };
                        })"""
                )
            except Exception:
                return []

        async def _click_jetstar_bundle_select() -> bool:
            bundle_targets = [
                ("starter", ["starter our basic fare"]),
                ("starter_plus", ["starter plus bag + seat + meal"]),
                ("flex", ["flex extra carry-on and flexibility"]),
                ("flex_plus", ["flex plus added flex + extras"]),
            ]
            buttons = page.locator("button")
            try:
                count = await buttons.count()
            except Exception:
                return False

            for bundle_name, fragments in bundle_targets:
                for index in range(count):
                    try:
                        button = buttons.nth(index)
                        if not await button.is_visible(timeout=400):
                            continue
                        text = " ".join(((await button.inner_text(timeout=800)) or "").split()).lower()
                        if text != "select":
                            continue
                        context = await button.evaluate(
                            r"""btn => {
                                let container = btn;
                                for (let i = 0; i < 4 && container?.parentElement; i += 1) {
                                    container = container.parentElement;
                                }
                                return ((container?.innerText || '') || '').replace(/\s+/g, ' ').trim();
                            }"""
                        )
                        context_lower = context.lower()
                        if not any(fragment in context_lower for fragment in fragments):
                            continue
                        await button.scroll_into_view_if_needed()
                        await button.click(timeout=4000)
                        await page.wait_for_timeout(4000)
                        debug_info["bundle_select_name"] = bundle_name
                        debug_info["bundle_select_context"] = context[:400]
                        return True
                    except Exception:
                        continue
            return False

        async def _wait_for_baggage_page(timeout_ms: int = 12000) -> bool:
            deadline = time.monotonic() + (timeout_ms / 1000)
            while time.monotonic() < deadline:
                if await _is_baggage_page():
                    return True
                await helper._handle_deeplink_redirect(page)
                await helper._dismiss_overlays(page)
                await page.wait_for_timeout(1500)
            return False

        async def _is_booking_details_page() -> bool:
            if await _is_baggage_page():
                return False
            details = await _snapshot_details()
            state = f"{details['current_url']} {details['page_title']} {details['body_snippet']}".lower()
            if await _has_any_selector(
                config.first_name_selectors
                + config.last_name_selectors
                + config.email_selectors
                + config.phone_selectors
            ):
                return True
            if "passenger details" in state or "contact details" in state or "booking details" in state:
                return (
                    "first name" in state
                    or "last name" in state
                    or "email" in state
                    or "phone" in state
                )
            return False

        async def _is_payment_page() -> bool:
            details = await _snapshot_details()
            current_url = details["current_url"].lower()
            title = details["page_title"].lower()
            return (
                "payment" in current_url
                or "review" in current_url
                or "payment" in title
                or "review & pay" in title
                or "review and pay" in title
            )

        def _coerce_jetstar_datetime(value: Any) -> Optional[datetime]:
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                except Exception:
                    return None
            return None

        def _jetstar_segment_value(route_key: str, segment_index: int, field_name: str) -> Any:
            route = (offer or {}).get(route_key) if isinstance(offer, dict) else None
            if not isinstance(route, dict):
                return None
            segments = route.get("segments")
            if not isinstance(segments, list) or not segments:
                return None
            try:
                segment = segments[segment_index]
            except Exception:
                return None
            if not isinstance(segment, dict):
                return None
            return segment.get(field_name)

        def _jetstar_calendar_label(value: Any) -> Optional[str]:
            dt_value = _coerce_jetstar_datetime(value)
            if not dt_value:
                return None
            return f"{dt_value.strftime('%A')}, {dt_value.day} {dt_value.strftime('%B %Y')}"

        async def _load_jetstar_select_flights_via_homepage() -> None:
            origin = _jetstar_segment_value("outbound", 0, "origin")
            destination = _jetstar_segment_value("outbound", -1, "destination")
            outbound_departure = _jetstar_segment_value("outbound", 0, "departure")
            inbound_departure = _jetstar_segment_value("inbound", 0, "departure")
            if not origin or not destination or not outbound_departure:
                raise RuntimeError("Jetstar checkout is missing outbound route details for homepage submit")

            adult_count = max(len(passengers or []), 1)
            home_search_url = (
                "https://www.jetstar.com/au/en/home"
                f"?adults={adult_count}"
                f"&destination={destination}"
                "&flight-type=2"
                f"&origin={origin}"
            )
            await page.goto(home_search_url, wait_until="domcontentloaded", timeout=config.goto_timeout)
            await page.wait_for_timeout(8000)

            await page.click("#popoverButton", timeout=10000)
            await page.wait_for_timeout(1500)

            outbound_label = _jetstar_calendar_label(outbound_departure)
            inbound_label = _jetstar_calendar_label(inbound_departure)
            if inbound_label:
                await page.locator("input[type='radio'][value='Return']").check(timeout=5000)
                await page.wait_for_timeout(800)
            else:
                await page.locator("input[type='radio'][value='Oneway']").check(timeout=5000)
                await page.wait_for_timeout(800)

            if not outbound_label:
                raise RuntimeError("Jetstar checkout could not determine outbound date label")
            await page.get_by_label(outbound_label).click(timeout=10000)
            await page.wait_for_timeout(1200)

            if inbound_label:
                await page.get_by_label(inbound_label).click(timeout=10000)
                await page.wait_for_timeout(1200)

            await page.get_by_role("button", name="Search").click(timeout=10000)
            await page.wait_for_timeout(15000)
            await helper._handle_deeplink_redirect(page)
            await helper._dismiss_overlays(page)
            await page.wait_for_timeout(1500)

        async def _open_checkout_page() -> None:
            nonlocal checkout_page, owns_page, page
            jetstar_browser = await _get_jetstar_browser()
            if getattr(jetstar_browser, "contexts", None):
                jetstar_context = jetstar_browser.contexts[0]
            else:
                jetstar_context = await jetstar_browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    locale="en-AU",
                    timezone_id="Australia/Sydney",
                    service_workers="block",
                )
            checkout_page = await jetstar_context.new_page()
            owns_page = True
            await auto_block_if_proxied(checkout_page)
            page = checkout_page

            if not getattr(jetstar_module, "_warmup_done", False):
                try:
                    warmup_urls = getattr(
                        jetstar_module,
                        "_WARMUP_URLS",
                        ("https://www.jetstar.com/", "https://booking.jetstar.com/au/en"),
                    )
                    warmup_ok = True
                    for warmup_url in warmup_urls:
                        await page.goto(
                            warmup_url,
                            wait_until="domcontentloaded",
                            timeout=20000,
                        )
                        await page.wait_for_timeout(3000)
                        warmup_title = ""
                        try:
                            warmup_title = (await page.title()).lower()
                        except Exception:
                            pass
                        if any(marker in warmup_title for marker in ("challenge", "not found", "error", "processing")):
                            warmup_ok = False
                            break
                    if warmup_ok:
                        jetstar_module._warmup_done = True
                except Exception:
                    pass

        try:
            flight_loaded = False
            processing_error = False
            for session_pass in range(1, 3):
                for attempt in range(1, 5):
                    if owns_page:
                        try:
                            await checkout_page.close()
                        except Exception:
                            pass
                        owns_page = False

                    await _open_checkout_page()

                    logger.info(
                        "Jetstar checkout: navigating to %s (pass %d/2, attempt %d/4)",
                        booking_url,
                        session_pass,
                        attempt,
                    )
                    try:
                        await _load_jetstar_select_flights_via_homepage()
                    except Exception as nav_err:
                        logger.warning("Jetstar checkout: homepage search navigation error (%s) — continuing", str(nav_err)[:100])
                    if page.is_closed():
                        logger.warning(
                            "Jetstar checkout: pass %d attempt %d ended with a closed page after homepage search",
                            session_pass,
                            attempt,
                        )
                        try:
                            await jetstar_module._reset_browser()
                        except Exception:
                            pass
                        await asyncio.sleep(1.5)
                        continue
                    step = "page_loaded"

                    title = ""
                    try:
                        title = (await page.title()).lower()
                    except Exception:
                        pass
                    if "challenge" in title:
                        jetstar_module._warmup_done = False
                        logger.warning(
                            "Jetstar checkout: pass %d attempt %d hit a challenge page",
                            session_pass,
                            attempt,
                        )
                        try:
                            await jetstar_module._reset_browser()
                        except Exception:
                            pass
                        await asyncio.sleep(1.5)
                        continue

                    try:
                        await page.wait_for_selector(
                            "script#bundle-data-v2, [class*='flight-row'], "
                            "div[aria-label*='Departure'], div[aria-label*='price'], "
                            "div.price-select[role='button'], [class*='price-select']",
                            timeout=20000,
                        )
                        await page.wait_for_timeout(2000)
                    except Exception:
                        if page.is_closed():
                            logger.warning(
                                "Jetstar checkout: pass %d attempt %d page closed while waiting for result markers",
                                session_pass,
                                attempt,
                            )
                            try:
                                await jetstar_module._reset_browser()
                            except Exception:
                                pass
                            await asyncio.sleep(1.5)
                            continue

                    result_markers = False
                    for selector in [
                        "script#bundle-data-v2",
                        "[class*='flight-row']",
                        "div[aria-label*='Departure']",
                        "div[aria-label*='price']",
                        "div.price-select[role='button']",
                        "[class*='price-select']",
                    ]:
                        try:
                            locator = page.locator(selector).first
                            if await locator.count() > 0:
                                result_markers = True
                                if selector.startswith("script#"):
                                    flight_loaded = True
                                    break
                                if await locator.is_visible(timeout=1500):
                                    flight_loaded = True
                                    break
                        except Exception:
                            pass

                    if not flight_loaded:
                        for _ in range(4):
                            if page.is_closed():
                                break
                            try:
                                if await page.locator("div.price-select[role='button'], [class*='price-select']").first.is_visible(timeout=1500):
                                    flight_loaded = True
                                    break
                            except Exception:
                                pass
                            await helper._handle_deeplink_redirect(page)
                            await helper._dismiss_overlays(page)
                            if page.is_closed():
                                break
                            await page.wait_for_timeout(2500)
                        if not flight_loaded:
                            body = (await _body_text()).lower()
                            processing_error = (
                                "not found" in title
                                or "error" in title
                                or "oops! you\u2019ve landed here by mistake" in body
                                or "oops! you've landed here by mistake" in body
                            )
                            if processing_error and not result_markers:
                                logger.warning(
                                    "Jetstar checkout: pass %d attempt %d stayed on a processing/error page",
                                    session_pass,
                                    attempt,
                                )
                        if flight_loaded:
                            break

                    if flight_loaded:
                        if result_markers:
                            logger.info(
                                "Jetstar checkout: pass %d attempt %d reached search result markers",
                                session_pass,
                                attempt,
                            )
                        break

                if flight_loaded:
                    break

            if not flight_loaded:
                if processing_error:
                    return await _result("failed", "Jetstar checkout repeatedly landed on Jetstar's processing/error page.")
                return await _result("failed", "Jetstar checkout did not reach the flight-selection stage.")

            if not await _click_first([
                "div.price-select[role='button']",
                "[class*='price-select'][role='button']",
                "[class*='price-select']",
            ], timeout=5000, desc="select Jetstar flight", wait_ms=2500):
                return await _result("failed", "Jetstar checkout could not select a flight.")
            step = "flights_selected"
            debug_info["after_flight_selection"] = await _snapshot_details()
            debug_info["bundle_select_candidates_before_click"] = await _jetstar_select_candidates()
            await _capture_checkout_details("flight_selection")

            bundle_click_needed = not await _is_baggage_page()
            debug_info["bundle_click_needed"] = bundle_click_needed
            if bundle_click_needed:
                bundle_selected = await _click_jetstar_bundle_select()
                debug_info["bundle_selected"] = bundle_selected
                debug_info["after_bundle_selection"] = await _snapshot_details()
                await _capture_checkout_details("bundle_selection")
                if not bundle_selected:
                    return await _result("failed", "Jetstar checkout could not select a fare bundle.")
            step = "fare_selected"

            debug_info["continue_to_bags_visible"] = await _page_has_any_text(["continue to bags"])
            if debug_info["continue_to_bags_visible"]:
                await _click_first(
                    ["button:has-text('Continue to bags')"],
                    timeout=5000,
                    desc="continue to Jetstar bags",
                    wait_ms=4500,
                    dismiss_after=False,
                )
                debug_info["after_continue_to_bags"] = await _snapshot_details()

            debug_info["baggage_page_ready"] = await _wait_for_baggage_page()
            await _capture_checkout_details("baggage")
            debug_info["continue_to_seats_visible_before_bag_click"] = await _page_has_any_text(["continue to seats"])
            debug_info["clicked_no_checked_baggage"] = await _click_action_card("No checked baggage")
            debug_info["clicked_included_carry_on"] = (
                await _click_action_card("7kg across 2 items")
                or await _click_action_card("Included in Starter")
            )
            debug_info["continue_to_seats_visible_after_bag_click"] = await _page_has_any_text(["continue to seats"])
            if not await _click_first([
                "button:has-text('Continue to seats')",
            ], timeout=7000, desc="continue to Jetstar seats", wait_ms=4500, dismiss_after=False):
                return await _result("failed", "Jetstar checkout could not advance past baggage.")
            debug_info["seat_page_ready"] = await _wait_for_jetstar_seat_page()
            debug_info["seat_controls_ready"] = await _wait_for_jetstar_seat_controls()
            debug_info["after_continue_to_seats"] = await _snapshot_details()
            await _capture_checkout_details("seats")
            step = "extras_skipped"

            debug_info["seat_choice_candidates_before_click"] = await _jetstar_seat_choice_candidates()
            debug_info["clicked_random_seat_allocation"] = await _click_jetstar_seat_option()
            debug_info["seat_choice_candidates_after_click"] = await _jetstar_seat_choice_candidates()
            debug_info["after_seat_choice"] = await _snapshot_details()
            await _capture_checkout_details("seats_after_choice")
            await _scroll_jetstar_page()
            await _click_first([
                "button:has-text('Continue to extras')",
                "button:has-text('Continue')",
            ], timeout=8000, desc="continue from Jetstar seats", wait_ms=4000, dismiss_after=False)
            step = "seats_skipped"

            debug_info["extras_controls_ready"] = await _wait_for_jetstar_extras_controls()
            debug_info["before_extras_advance"] = await _snapshot_details()
            await _capture_checkout_details("extras")
            await _advance_jetstar_extras()
            debug_info["after_extras_advance"] = await _snapshot_details()

            if await _is_booking_details_page():
                await _capture_checkout_details("booking_details")
                title_text = "Mr" if pax.get("gender", "m") == "m" else "Ms"
                if config.title_mode == "dropdown":
                    if await safe_click_first(page, config.title_dropdown_selectors, timeout=2000, desc="Jetstar title dropdown"):
                        await page.wait_for_timeout(500)
                        await safe_click(page, f"button:has-text('{title_text}')", timeout=2000, desc=f"Jetstar title {title_text}")
                elif config.title_mode == "select":
                    try:
                        await page.select_option(config.title_select_selector, label=title_text, timeout=2000)
                    except Exception:
                        pass

                await safe_fill_first(page, config.first_name_selectors, pax.get("given_name", "Test"))
                await safe_fill_first(page, config.last_name_selectors, pax.get("family_name", "Traveler"))
                await safe_fill_first(page, config.email_selectors, pax.get("email", "test@example.com"))
                await safe_fill_first(page, config.phone_selectors, pax.get("phone_number", "+441234567890"))
                step = "passengers_filled"

                await _click_first([
                    "button:has-text('Continue to review')",
                    "button:has-text('Continue to payment')",
                    "button:has-text('Continue')",
                ], timeout=5000, desc="continue after Jetstar booking details", wait_ms=4500, dismiss_after=False)

            if await _is_payment_page():
                await _capture_checkout_details("payment")
                step = "payment_page_reached"
                return await _result(
                    "payment_page_reached",
                    (
                        f"Jetstar checkout complete — reached payment page in {time.monotonic() - t0:.0f}s. "
                        f"Price: {await _extract_price()} {offer.get('currency', 'AUD')}. "
                        "Payment NOT submitted (safe mode)."
                    ),
                )

            return await _result("failed", "Jetstar checkout advanced but stopped before the payment page.")
        except Exception as e:
            logger.error("Jetstar checkout error: %s", e, exc_info=True)
            return await _result("error", f"Jetstar checkout error at step '{step}': {e}")
        finally:
            if owns_page:
                try:
                    await checkout_page.close()
                except Exception:
                    pass

    async def _prepare_airasia_results(self, page, booking_url: str = "") -> None:
        """Wait out AirAsia WAF and clear results-page modals before selecting a flight."""
        security_rounds = 0
        select_cta_selectors = [
            "[class*='JourneyPriceCTA'] a:has-text('Select')",
            "[class*='JourneyPriceCTA'] [class*='Button__ButtonContainer']:has-text('Select')",
            "main [class*='JourneyPriceCTA'] a",
        ]
        for attempt in range(18):
            body_text = ""
            title_text = ""
            try:
                body_text = (await page.locator("body").inner_text()).lower()
            except Exception:
                pass
            try:
                title_text = (await page.title()).lower()
            except Exception:
                pass

            combined = f"{title_text} {body_text}"
            on_security_gate = any(
                token in combined
                for token in (
                    "performing security verification",
                    "verifies you are not a bot",
                    "just a moment",
                    "checking your browser",
                    "please wait while we verify",
                )
            )

            if on_security_gate:
                security_rounds += 1
                await page.wait_for_timeout(1500)
                if security_rounds % 4 == 0:
                    try:
                        if booking_url:
                            await page.goto(booking_url, wait_until="domcontentloaded", timeout=20000)
                        else:
                            await page.reload(wait_until="domcontentloaded", timeout=20000)
                    except Exception:
                        pass
                    await page.wait_for_timeout(2500)
                elif security_rounds % 2 == 0:
                    try:
                        await page.reload(wait_until="domcontentloaded", timeout=20000)
                    except Exception:
                        pass
                    await page.wait_for_timeout(2000)
                continue

            security_rounds = 0

            await safe_click_first(
                page,
                [
                    "#airasia-phoenix-modal-close-button",
                    "button[aria-label='Close modal button']",
                ],
                timeout=1000,
                desc="close AirAsia results modal",
            )

            await safe_click_first(
                page,
                ["#airasia-sso-modal-wrapper-close-button"],
                timeout=1000,
                desc="close AirAsia preselection login modal",
            )

            for selector in select_cta_selectors:
                try:
                    cta = page.locator(selector).first
                    if await cta.is_visible(timeout=1000):
                        return
                except Exception:
                    pass

            if attempt in {5, 11}:
                try:
                    if booking_url:
                        await page.goto(booking_url, wait_until="domcontentloaded", timeout=20000)
                    else:
                        await page.reload(wait_until="domcontentloaded", timeout=20000)
                except Exception:
                    pass
                await page.wait_for_timeout(2500)
                continue

            try:
                await page.mouse.wheel(0, 1200)
            except Exception:
                pass

            await page.wait_for_timeout(1000)

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
                try:
                    await btn.click(force=True)
                except Exception:
                    handle = await btn.element_handle()
                    if handle is not None:
                        await page.evaluate(
                            """(element) => {
                                if (typeof element.click === 'function') {
                                    element.click();
                                    return;
                                }
                                element.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                            }""",
                            handle,
                        )
                await page.wait_for_timeout(500)
        except Exception:
            pass
        # Fallback: remove any remaining blocking overlays via JS
        try:
            await page.evaluate("""() => {
                for (const sel of ['#cookie-preferences', '#onetrust-consent-sdk',
                    '#CybotCookiebotDialog', '[class*="cookie-popup"]',
                    '[class*="cookie-overlay"]', '[class*="consent-banner"]',
                    '#airasia-phoenix-modal',
                    '[class*="Modal__ModalWrapper"]', '[class*="Modal__Backdrop"]']) {
                    const el = document.querySelector(sel);
                    if (el) el.remove();
                }
            }""")
        except Exception:
            pass
