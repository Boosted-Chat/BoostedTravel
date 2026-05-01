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

import asyncio
import base64
import logging
import os
import random
import re
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
    nationality_dropdown_item: str = "[class*='dropdown'] [class*='item']:first-child"

    # Travel document / contact accordions used by some checkout flows
    document_number_selectors: list[str] = field(default_factory=list)
    document_expiry_selectors: list[str] = field(default_factory=list)
    contact_section_expand_selectors: list[str] = field(default_factory=list)

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
    flight_cards_selector="[class*='flight'], [class*='result'], [class*='journey'], [class*='bound']",
    fare_selectors=[
        "button:has-text('Select')",
        "button:has-text('Choose')",
        "button:has-text('Economy')",
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

# ─── Asian airlines ─────────────────────────────────────────────────────

_register(_base_cfg("AirAsia", "airasia_direct",
    flight_cards_selector="a:has-text('Select'), [class*='JourneyPriceCTA'], [class*='flight'], [class*='result'], [data-testid*='flight']",
    first_flight_selectors=[
        "#jcta-mobile a:has-text('Select')",
        "a:has-text('Select')",
        "text=Select",
    ],
    fare_selectors=[],
    login_skip_selectors=[
        "#guestButton",
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
    email_selectors=[
        "input[placeholder='Email']",
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
    passenger_continue_selectors=[
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

        pw = await async_playwright().start()
        _chrome_proc = None  # CDP Chrome subprocess (if any)

        if config.use_cdp_chrome:
            # CDP mode: launch real Chrome as subprocess, connect via CDP.
            # This bypasses Kasada KPSDK — Playwright automation hooks are NOT
            # injected into the Chrome binary, so KPSDK JS runs naturally.
            from .browser import find_chrome, stealth_popen_kwargs, bandwidth_saving_args, disable_background_networking_args
            chrome_path = find_chrome()
            _udd_name = config.cdp_user_data_dir or f".{config.source_tag}_chrome_data"
            _user_data_dir = os.path.join(
                os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")),
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
                *bandwidth_saving_args(),
                *disable_background_networking_args(),
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
            else:
                try:
                    from playwright_stealth import stealth_async
                    page = await context.new_page()
                    await stealth_async(page)
                except ImportError:
                    page = await context.new_page()

            # Auto-block heavy resources when using proxy (saves bandwidth)
            from .browser import auto_block_if_proxied
            await auto_block_if_proxied(page)

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

            if config.source_tag == "aireuropa_direct":
                await self._prepare_aireuropa_checkout_results(page, offer)

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
                for sel in config.nationality_selectors:
                    if await safe_fill(page, sel, "GB"):
                        await page.wait_for_timeout(500)
                        try:
                            await page.locator(config.nationality_dropdown_item).first.click(timeout=2000)
                        except Exception:
                            pass
                        break

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

            if config.contact_section_expand_selectors:
                await safe_click_first(page, config.contact_section_expand_selectors, timeout=2000, desc="expand contact details")
                await page.wait_for_timeout(500)

            # Email
            await safe_fill_first(page, config.email_selectors, pax.get("email", "test@example.com"))

            # Phone
            await safe_fill_first(page, config.phone_selectors, pax.get("phone_number", "+441234567890"))

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
            await safe_click_first(page, config.passenger_continue_selectors, timeout=2000, desc="continue after passengers")
            await page.wait_for_timeout(1500)
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
                step = self._checkout_step_for_page(final_checkout_page)
                blocker_details = self._merge_checkout_details(
                    captured_details,
                    {
                        "blocker": "payment_page_not_reached",
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
                    message=(
                        f"{config.airline_name} checkout did not reach payment. "
                        f"Current surface looks like '{(final_checkout_page or 'unknown').replace('_', ' ')}'. "
                        f"Visible price: {page_price} {offer.get('currency', 'EUR')}."
                    ),
                    can_complete_manually=bool(booking_url),
                    elapsed_seconds=elapsed,
                    details=blocker_details,
                )

            step = "payment_page_reached"
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
        current_url = str(snapshot.get("current_url") or "").strip().lower()
        title = str(snapshot.get("page_title") or "").strip().lower()
        body = str(snapshot.get("body_snippet") or "").strip().lower()
        combined = " ".join(part for part in (title, body) if part)

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

        search_url_terms = ("/search", "fullsearch", "flight-search", "select-flight", "results")
        search_text_terms = (
            "select cheap flights",
            "select flight",
            "choose flight",
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

        passenger_terms = ("passenger details", "traveller details", "traveler details", "guest details", "contact details")
        if any(term in combined for term in passenger_terms):
            return "passengers"

        seat_terms = ("seat map", "select seat", "choose your seat", "hot seat")
        if any(term in combined for term in seat_terms):
            return "seats"

        extras_terms = ("baggage", "checked bag", "bags", "add-ons", "add ons", "extras")
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
            const btn = document.querySelector('button.ae-btn-block.ae-btn-primary');
            if (btn && btn.offsetHeight > 0) { btn.click(); return; }
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = (b.textContent || '').trim().toLowerCase();
                if ((t === 'search' || t.includes('search')) && b.offsetHeight > 0) {
                    b.click();
                    return;
                }
            }
        }""")
        await asyncio.sleep(4.0)

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
                const homeSearchSurface = /search flights|flight deals|manage your reservation|before flying|promotional code|trip type/i.test(pageSignals);
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
                } else if (/seat map|pick a seat|select your seat|seat selection|choose your seats|quiet zone|hot seat|extra legroom|flatbed/i.test(pageSignals)) {
                    result.checkout_page = 'seats';
                } else if (/baggage|checked bag|checked baggage|extra bag|carry-on|carry on|insurance|meal|food and drink|priority boarding|fast pass|add extras|choose extras|bundle|package|upgrade/i.test(pageSignals)) {
                    result.checkout_page = 'extras';
                } else if (/guest details|passenger|traveller details|contact details|customer details/i.test(pageSignals)) {
                    result.checkout_page = 'guest_details';
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

                const countMoneyTokens = (text) => (
                    normalize(text).match(/[A-Z]{3}\s*[\d,]+(?:\.\d{1,2})?/g) || []
                ).length;

                const baggageTypeFromText = (text) => {
                    const haystack = normalize(text).toLowerCase();
                    if (/checked/.test(haystack)) return 'checked_bag';
                    if (/carry[- ]?on|carry on|cabin/.test(haystack)) return 'cabin_bag';
                    return 'baggage';
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

                const baggage = dedupeItems([
                    ...baggageItems,
                    ...baggageModalOptions,
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

                const result = {};
                if (/card number|billing address|secure payment|payment method|pay now|review and pay|expiry date|cvv|security code/.test(pageSignals)) {
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
