"""Reference table of ancillary pricing and bag allowance descriptions.

Prices are in each airline's primary trading currency.  Values are based on
published airline tariffs (Q1–Q2 2025) and should be shown as *approximate*
(prefix with "from ~").

Per-entry fields
----------------
carry_on : float | None
    0.0  = cabin/carry-on bag included in base fare.
    >0   = typical add-on price (stable enough to display).
    None = price varies by route — do NOT show a number; omit or say "varies".

carry_on_kg : int | None
    Weight limit of the included/add-on cabin bag.
    None = airline uses size-only rules (no weight limit enforced).

carry_on_note : str  (optional)
    Overrides auto-generated conditions["carry_on"] text.  Use when the
    "included" item is only a personal under-seat item, not a full cabin bag.

checked_bag : float | None
    Same conventions as carry_on but for the first checked bag.

checked_bag_kg : int
    Weight allowance of the first checked bag.

checked_bag_note : str  (optional)
    Overrides auto-generated conditions["checked_bag"] text.

seat : float | None
    Minimum seat-selection surcharge.
    0.0 = seats assigned free.
    None = highly variable.

currency : str
    ISO 4217 code.

Source: published airline tariff pages, Q1–Q2 2025.
"""
from __future__ import annotations

from typing import Any

_DEFAULT_CARRY_ON_KG = 10

# ---------------------------------------------------------------------------
# Reference table keyed by IATA carrier code
# ---------------------------------------------------------------------------
_AIRLINE_ANCILLARY: dict[str, dict[str, Any]] = {

    # ── European LCCs ───────────────────────────────────────────────────────
    "FR": {  # Ryanair
        # Personal item (under seat) is always free.
        # Cabin bag to overhead bin requires Priority Boarding add-on.
        # Checked bag price varies hugely by route/timing/weight — don't show.
        "carry_on": 0.0,
        "carry_on_kg": None,  # size-only: 40×20×25 cm, no weight check
        "carry_on_note": "1 personal item (40×20×25 cm) included; cabin bag requires Priority add-on (price varies by route)",
        "checked_bag": None,   # intentionally omitted — too variable (~€10–50+)
        "checked_bag_kg": 20,
        "checked_bag_note": "add-on; price varies by route",
        "seat": 4.0, "currency": "EUR",
    },
    "W6": {  # Wizz Air
        # Small under-seat bag always included free.
        # Larger cabin bag to overhead bin requires WIZZ Go add-on (~€12).
        "carry_on": 0.0,
        "carry_on_kg": None,  # 40×30×20 cm size limit, no weight check
        "carry_on_note": "1 small under-seat bag (40×30×20 cm) included; overhead cabin bag add-on from ~€12 (10 kg)",
        "checked_bag": 22.0, "checked_bag_kg": 20,
        "seat": 5.0, "currency": "EUR",
    },
    "U2": {  # EasyJet — STANDARD includes cabin bag to overhead bin
        "carry_on": 0.0, "carry_on_kg": 15,
        "carry_on_note": "1 cabin bag (56×45×25 cm, up to 15 kg) included",
        "checked_bag": 18.0, "checked_bag_kg": 23,
        "seat": 5.0, "currency": "EUR",
    },
    "DY": {  # Norwegian LowFare — small under-seat item only
        "carry_on": 0.0, "carry_on_kg": 10,
        "carry_on_note": "1 small under-seat bag (45×35×25 cm, up to 10 kg) included",
        "checked_bag": 35.0, "checked_bag_kg": 20,
        "seat": 8.0, "currency": "EUR",
    },
    "HV": {  # Transavia
        "carry_on": 0.0, "carry_on_kg": 10,
        "carry_on_note": "1 cabin bag (55×35×25 cm, up to 10 kg) included",
        "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 7.0, "currency": "EUR",
    },
    "VY": {  # Vueling Basic — cabin bag is add-on in cheapest fare
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 6.0, "currency": "EUR",
    },
    "LS": {  # Jet2
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 28.0, "checked_bag_kg": 22,
        "seat": 6.0, "currency": "GBP",
    },
    "EW": {  # Eurowings
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 22.0, "checked_bag_kg": 23,
        "seat": 5.0, "currency": "EUR",
    },
    "DE": {  # Condor
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "V7": {  # Volotea
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 22.0, "checked_bag_kg": 20,
        "seat": 6.0, "currency": "EUR",
    },
    "PC": {  # Pegasus
        "carry_on": 12.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 20,
        "seat": 5.0, "currency": "EUR",
    },
    "TO": {  # Transavia France
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 7.0, "currency": "EUR",
    },
    "BT": {  # airBaltic
        "carry_on": 12.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 6.0, "currency": "EUR",
    },
    "XC": {  # Corendon Airlines
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 6.0, "currency": "EUR",
    },
    "SZ": {  # SkyExpress Greece
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 20.0, "checked_bag_kg": 23,
        "seat": 5.0, "currency": "EUR",
    },
    "OG": {  # Play Airlines (Iceland)
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 20.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },
    "A3": {  # Aegean Airlines
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 20.0, "checked_bag_kg": 23,
        "seat": 7.0, "currency": "EUR",
    },
    "OA": {  # Olympic Air
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },
    "EI": {  # Aer Lingus
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 20,
        "seat": 8.0, "currency": "EUR",
    },
    "I2": {  # Iberia Express
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },
    "FI": {  # Icelandair
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "OV": {  # SalamAir
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 50.0, "checked_bag_kg": 20,
        "seat": 10.0, "currency": "OMR",
    },
    "S4": {  # Azores Airlines
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },
    "QS": {  # Smartwings / Czech Airlines
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },
    "JU": {  # Air Serbia
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },
    "CY": {  # Cyprus Airways
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },
    "XQ": {  # SunExpress
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 20,
        "seat": 6.0, "currency": "EUR",
    },
    "GL": {  # Air Greenland — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "DKK",
    },
    "FP": {  # Flair Airlines (Canada) — variable pricing
        "carry_on": None, "checked_bag": None, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "CAD",
    },
    "3O": {  # Air Arabia Maroc
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 120.0, "checked_bag_kg": 20,
        "seat": 25.0, "currency": "MAD",
    },
    "BJ": {  # Nouvelair (Tunisia)
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },

    # ── Middle East LCCs / Regional ────────────────────────────────────────
    "FZ": {  # FlyDubai
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 100.0, "checked_bag_kg": 20,
        "seat": 30.0, "currency": "AED",
    },
    "G9": {  # Air Arabia
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 80.0, "checked_bag_kg": 20,
        "seat": 25.0, "currency": "AED",
    },
    "XY": {  # Flynas
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 150.0, "checked_bag_kg": 20,
        "seat": 30.0, "currency": "SAR",
    },
    "F3": {  # Flyadeal
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 130.0, "checked_bag_kg": 20,
        "seat": 20.0, "currency": "SAR",
    },
    "J9": {  # Jazeera Airways
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 8.0, "checked_bag_kg": 20,
        "seat": 3.0, "currency": "USD",
    },
    "SM": {  # Air Cairo
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 50.0, "checked_bag_kg": 20,
        "seat": 15.0, "currency": "USD",
    },
    "WY": {  # Oman Air — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "GF": {  # Gulf Air — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "ME": {  # MEA — Middle East Airlines — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },

    # ── Asian LCCs ─────────────────────────────────────────────────────────
    "AK": {  # AirAsia Malaysia
        "carry_on": 20.0, "carry_on_kg": 7, "checked_bag": 70.0, "checked_bag_kg": 20,
        "seat": 8.0, "currency": "MYR",
    },
    "D7": {  # AirAsia X
        "carry_on": 30.0, "carry_on_kg": 7, "checked_bag": 80.0, "checked_bag_kg": 20,
        "seat": 10.0, "currency": "MYR",
    },
    "QZ": {  # Indonesia AirAsia
        "carry_on": 70000.0, "carry_on_kg": 7, "checked_bag": 200000.0, "checked_bag_kg": 20,
        "seat": 30000.0, "currency": "IDR",
    },
    "Z2": {  # Philippines AirAsia
        "carry_on": 300.0, "carry_on_kg": 7, "checked_bag": 600.0, "checked_bag_kg": 20,
        "seat": 150.0, "currency": "PHP",
    },
    "FD": {  # Thai AirAsia
        "carry_on": 200.0, "carry_on_kg": 7, "checked_bag": 500.0, "checked_bag_kg": 20,
        "seat": 100.0, "currency": "THB",
    },
    "XT": {  # Indonesia AirAsia X
        "carry_on": 80000.0, "carry_on_kg": 7, "checked_bag": 200000.0, "checked_bag_kg": 20,
        "seat": 30000.0, "currency": "IDR",
    },
    "I5": {  # Air Asia India
        "carry_on": 200.0, "carry_on_kg": 7, "checked_bag": 800.0, "checked_bag_kg": 15,
        "seat": 250.0, "currency": "INR",
    },
    "YP": {  # AirAsia Move (OTA-style)
        "carry_on": 20.0, "carry_on_kg": 7, "checked_bag": 70.0, "checked_bag_kg": 20,
        "seat": 8.0, "currency": "MYR",
    },
    "6E": {  # IndiGo
        "carry_on": 0.0, "carry_on_kg": 7,
        "carry_on_note": "1 cabin bag (55×35×25 cm, up to 7 kg) included",
        "checked_bag": 1000.0, "checked_bag_kg": 15,
        "seat": 250.0, "currency": "INR",
    },
    "SG": {  # SpiceJet
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 1200.0, "checked_bag_kg": 15,
        "seat": 200.0, "currency": "INR",
    },
    "QP": {  # Akasa Air
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 900.0, "checked_bag_kg": 15,
        "seat": 200.0, "currency": "INR",
    },
    "IX": {  # Air India Express
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 900.0, "checked_bag_kg": 15,
        "seat": 250.0, "currency": "INR",
    },
    "VJ": {  # VietJet
        "carry_on": 200000.0, "carry_on_kg": 7, "checked_bag": 400000.0, "checked_bag_kg": 20,
        "seat": 100000.0, "currency": "VND",
    },
    "5J": {  # Cebu Pacific
        "carry_on": 300.0, "carry_on_kg": 7, "checked_bag": 600.0, "checked_bag_kg": 20,
        "seat": 150.0, "currency": "PHP",
    },
    "TR": {  # Scoot
        "carry_on": 20.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 20,
        "seat": 8.0, "currency": "SGD",
    },
    "MM": {  # Peach (Japan)
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 2000.0, "checked_bag_kg": 20,
        "seat": 900.0, "currency": "JPY",
    },
    "GK": {  # Jetstar Japan
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 2000.0, "checked_bag_kg": 20,
        "seat": 900.0, "currency": "JPY",
    },
    "BC": {  # Skymark (Japan) — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "JPY",
    },
    "7C": {  # Jeju Air
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30000.0, "checked_bag_kg": 15,
        "seat": 5000.0, "currency": "KRW",
    },
    "BX": {  # Air Busan
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25000.0, "checked_bag_kg": 15,
        "seat": 5000.0, "currency": "KRW",
    },
    "LJ": {  # Jin Air
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25000.0, "checked_bag_kg": 15,
        "seat": 5000.0, "currency": "KRW",
    },
    "TW": {  # T'way Air
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25000.0, "checked_bag_kg": 15,
        "seat": 5000.0, "currency": "KRW",
    },
    "8L": {  # Lucky Air
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 100.0, "checked_bag_kg": 20,
        "seat": 30.0, "currency": "CNY",
    },
    "9C": {  # Spring Airlines
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 80.0, "checked_bag_kg": 15,
        "seat": 25.0, "currency": "CNY",
    },
    "DD": {  # Nok Air
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 400.0, "checked_bag_kg": 20,
        "seat": 100.0, "currency": "THB",
    },
    "SL": {  # Thai Lion Air
        "carry_on": 200.0, "carry_on_kg": 7, "checked_bag": 400.0, "checked_bag_kg": 20,
        "seat": 100.0, "currency": "THB",
    },
    "JT": {  # Lion Air
        "carry_on": 200000.0, "carry_on_kg": 7, "checked_bag": 200000.0, "checked_bag_kg": 20,
        "seat": 50000.0, "currency": "IDR",
    },
    "ID": {  # Batik Air
        "carry_on": 200000.0, "carry_on_kg": 7, "checked_bag": 200000.0, "checked_bag_kg": 20,
        "seat": 50000.0, "currency": "IDR",
    },
    "QG": {  # Citilink (Indonesia)
        "carry_on": 0.0, "carry_on_kg": 7,
        "carry_on_note": "1 cabin bag (7 kg) included",
        "checked_bag": 150000.0, "checked_bag_kg": 20,
        "seat": 50000.0, "currency": "IDR",
    },
    "OD": {  # Batik Air Malaysia
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 50.0, "checked_bag_kg": 20,
        "seat": 10.0, "currency": "MYR",
    },
    "UO": {  # HK Express
        "carry_on": 100.0, "carry_on_kg": 7, "checked_bag": 200.0, "checked_bag_kg": 20,
        "seat": 50.0, "currency": "HKD",
    },
    "HX": {  # Hong Kong Airlines — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "HKD",
    },
    "IT": {  # Tigerair Taiwan
        "carry_on": 250.0, "carry_on_kg": 7, "checked_bag": 600.0, "checked_bag_kg": 20,
        "seat": 150.0, "currency": "TWD",
    },
    "8B": {  # TransNusa
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 100000.0, "checked_bag_kg": 20,
        "seat": 30000.0, "currency": "IDR",
    },
    "KC": {  # FlyArystan
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 8.0, "checked_bag_kg": 20,
        "seat": 3.0, "currency": "USD",
    },
    "ZG": {  # Zipair
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 3000.0, "checked_bag_kg": 20,
        "seat": 0.0, "currency": "JPY",
    },
    "JX": {  # Starlux Airlines — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },

    # ── Americas LCCs ──────────────────────────────────────────────────────
    "F9": {  # Frontier Airlines — bag fees highly variable by route/tier
        "carry_on": None, "checked_bag": None, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "NK": {  # Spirit Airlines — all add-on fees highly variable
        "carry_on": None, "checked_bag": None, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "G4": {  # Allegiant Air — variable pricing
        "carry_on": None, "checked_bag": None, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "USD",
    },
    "WN": {  # Southwest Airlines — 2 checked bags included free, no seat fees
        "carry_on": 0.0, "carry_on_kg": None,
        "carry_on_note": "1 carry-on bag included",
        "checked_bag": 0.0, "checked_bag_kg": 23,
        "checked_bag_note": "2 checked bags (23 kg each) included free",
        "seat": 0.0, "currency": "USD",
    },
    "MX": {  # Breeze Airways — variable pricing
        "carry_on": None, "checked_bag": None, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "XP": {  # Avelo Airlines — variable pricing
        "carry_on": None, "checked_bag": None, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "USD",
    },
    "SY": {  # Sun Country — variable pricing
        "carry_on": None, "checked_bag": None, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "B6": {  # JetBlue — cabin bag included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None,
        "carry_on_note": "1 cabin bag included",
        "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "Y4": {  # Volaris — prices in MXN (domestic Mexico LCC, API returns MXN)
        "carry_on": 250.0, "carry_on_kg": 10, "checked_bag": 500.0, "checked_bag_kg": 25,
        "seat": 150.0, "currency": "MXN",
    },
    "VB": {  # VivaAerobus — prices in MXN (domestic Mexico LCC)
        "carry_on": 280.0, "carry_on_kg": 10, "checked_bag": 400.0, "checked_bag_kg": 25,
        "seat": 150.0, "currency": "MXN",
    },
    "DM": {  # Arajet
        "carry_on": 15.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 20,
        "seat": 8.0, "currency": "USD",
    },
    "P5": {  # Wingo
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 20.0, "checked_bag_kg": 20,
        "seat": 6.0, "currency": "USD",
    },
    "WW": {  # Wingo (alt code)
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 20.0, "checked_bag_kg": 20,
        "seat": 6.0, "currency": "USD",
    },
    "JA": {  # JetSmart
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "USD",
    },
    "H2": {  # Sky Airline (Chile)
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 22.0, "checked_bag_kg": 23,
        "seat": 5.0, "currency": "USD",
    },
    "FO": {  # Flybondi
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 20.0, "checked_bag_kg": 23,
        "seat": 5.0, "currency": "USD",
    },
    "G3": {  # Gol Linhas Aéreas
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 100.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "BRL",
    },
    "AD": {  # Azul Brazilian Airlines
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 100.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "BRL",
    },
    "AV": {  # Avianca — carry-on included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "CM": {  # Copa Airlines — carry-on included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 40.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "AR": {  # Aerolíneas Argentinas — 1 checked bag included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "LA": {  # LATAM — carry-on included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 40.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "JJ": {  # LATAM Brasil
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 100.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "BRL",
    },

    # ── North American FSCs ────────────────────────────────────────────────
    "DL": {  # Delta Air Lines — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "AA": {  # American Airlines — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "UA": {  # United Airlines — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "AS": {  # Alaska Airlines — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "HA": {  # Hawaiian Airlines — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "AC": {  # Air Canada — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "CAD",
    },
    "WS": {  # WestJet — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "CAD",
    },
    "PD": {  # Porter Airlines — both included
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "CAD",
    },

    # ── European FSCs ──────────────────────────────────────────────────────
    "LH": {  # Lufthansa (Light fare has no bag)
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "OS": {  # Austrian Airlines
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "SN": {  # Brussels Airlines
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "LX": {  # Swiss International
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "CHF",
    },
    "AF": {  # Air France
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "EUR",
    },
    "KL": {  # KLM
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "EUR",
    },
    "BA": {  # British Airways
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 65.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "GBP",
    },
    "IB": {  # Iberia
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "EUR",
    },
    "SK": {  # SAS
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "AY": {  # Finnair
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "TP": {  # TAP Air Portugal
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "LO": {  # LOT Polish Airlines
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 25.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "PLN",
    },
    "UX": {  # Air Europa
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "EUR",
    },
    "VS": {  # Virgin Atlantic — 1 checked bag included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 20.0, "currency": "GBP",
    },

    # ── Middle East / Gulf FSCs ────────────────────────────────────────────
    "TK": {  # Turkish Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 8, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "EK": {  # Emirates — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 25,
        "seat": 20.0, "currency": "USD",
    },
    "EY": {  # Etihad — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "QR": {  # Qatar Airways — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 20.0, "currency": "USD",
    },
    "SV": {  # Saudi Arabian Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "RJ": {  # Royal Jordanian — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "AT": {  # Royal Air Maroc — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "MS": {  # EgyptAir — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 8.0, "currency": "USD",
    },
    "KU": {  # Kuwait Airways — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },

    # ── Asian FSCs ─────────────────────────────────────────────────────────
    "SQ": {  # Singapore Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 25,
        "seat": 20.0, "currency": "SGD",
    },
    "CX": {  # Cathay Pacific — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 50.0, "currency": "HKD",
    },
    "JL": {  # Japan Airlines — bags included, free seats
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "JPY",
    },
    "NH": {  # ANA — bags included, free seats
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "JPY",
    },
    "CZ": {  # China Southern — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "CNY",
    },
    "MU": {  # China Eastern — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "CNY",
    },
    "CA": {  # Air China — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "CNY",
    },
    "HU": {  # Hainan Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "CNY",
    },
    "CI": {  # China Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "BR": {  # EVA Air — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "KE": {  # Korean Air — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "OZ": {  # Asiana Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "MH": {  # Malaysia Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 25,
        "seat": 15.0, "currency": "MYR",
    },
    "GA": {  # Garuda Indonesia — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "PR": {  # Philippine Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "VN": {  # Vietnam Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "TG": {  # Thai Airways — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 30,
        "seat": 0.0, "currency": "THB",
    },
    "AI": {  # Air India — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 25,
        "seat": 10.0, "currency": "INR",
    },
    "UL": {  # SriLankan Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "BG": {  # Biman Bangladesh — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 20,
        "seat": 5.0, "currency": "USD",
    },
    "PK": {  # PIA Pakistan — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 5.0, "currency": "USD",
    },
    "PG": {  # Bangkok Airways — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 20,
        "seat": 8.0, "currency": "THB",
    },

    # ── Oceania ────────────────────────────────────────────────────────────
    "QF": {  # Qantas — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "AUD",
    },
    "VA": {  # Virgin Australia — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "AUD",
    },
    "JQ": {  # Jetstar Australia
        "carry_on": 14.0, "carry_on_kg": 7, "checked_bag": 25.0, "checked_bag_kg": 20,
        "seat": 6.0, "currency": "AUD",
    },
    "NZ": {  # Air New Zealand — carry-on included, no weight limit
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 12.0, "currency": "NZD",
    },
    "ZL": {  # Rex (Regional Express) — bags included
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "AUD",
    },
    "FJ": {  # Fiji Airways — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "FJD",
    },
    "PX": {  # Air Niugini — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "USD",
    },
    "NF": {  # Air Vanuatu — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "USD",
    },

    # ── African ────────────────────────────────────────────────────────────
    "FA": {  # FlySafair
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 180.0, "checked_bag_kg": 20,
        "seat": 60.0, "currency": "ZAR",
    },
    "ET": {  # Ethiopian Airlines — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "KQ": {  # Kenya Airways — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "WB": {  # RwandAir — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "SA": {  # South African Airways — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "ZAR",
    },
    "LY": {  # El Al — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 8, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },
    "TS": {  # Air Transat
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 30.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "CAD",
    },
    "MK": {  # Air Mauritius — 1 bag included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "HM": {  # Air Seychelles — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "BW": {  # Caribbean Airlines — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "USD",
    },
    "TN": {  # Air Tahiti Nui — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 10.0, "currency": "EUR",
    },
    "P4": {  # Air Peace (Nigeria) — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "USD",
    },
    "BS": {  # US-Bangla Airlines — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 20,
        "seat": 5.0, "currency": "USD",
    },

    # ── Pacific / Regional ─────────────────────────────────────────────────
    "SB": {  # Air Caledonie Internationale — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "XPF",
    },
    "TL": {  # Air North (Canada) — bags included
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "CAD",
    },
    "CG": {  # PNG Air — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "USD",
    },
    "IE": {  # Solomon Airlines — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "USD",
    },
    "OL": {  # Samoa Airways — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "USD",
    },

    # ── Indian regional ────────────────────────────────────────────────────
    "9I": {  # Alliance Air India — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 15,
        "seat": 200.0, "currency": "INR",
    },

    # ── Nigerian ───────────────────────────────────────────────────────────
    "QI": {  # Ibom Air (Nigeria) — bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 5.0, "currency": "USD",
    },

    # ── European ───────────────────────────────────────────────────────────
    "4U": {  # Eurowings Discovery (formerly Germanwings) — LCC
        "carry_on": 10.0, "carry_on_kg": 10, "checked_bag": 22.0, "checked_bag_kg": 23,
        "seat": 5.0, "currency": "EUR",
    },
    "DK": {  # Star Air (Denmark) — bags included (regional/charter)
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "DKK",
    },
    "FC": {  # Link Airways (Australia) — bags included
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "AUD",
    },

    # ── Pakistani ──────────────────────────────────────────────────────────
    "9N": {  # Nine Air (Pakistan) — bags included
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 0.0, "checked_bag_kg": 20,
        "seat": 5.0, "currency": "PKR",
    },

    # ── Indonesian LCC ─────────────────────────────────────────────────────
    "RS": {  # Super Air Jet (Indonesia) — LCC
        "carry_on": 0.0, "carry_on_kg": 7,
        "carry_on_note": "1 cabin bag (7 kg) included",
        "checked_bag": 150000.0, "checked_bag_kg": 20,
        "seat": 50000.0, "currency": "IDR",
    },
    "IU": {  # Super Air Jet (Indonesia) — alternate IATA code alias
        "carry_on": 0.0, "carry_on_kg": 7,
        "carry_on_note": "1 cabin bag (7 kg) included",
        "checked_bag": 150000.0, "checked_bag_kg": 20,
        "seat": 50000.0, "currency": "IDR",
    },

    # ── Chinese LCC ────────────────────────────────────────────────────────
    "AQ": {  # 9Air (Nine Air, China) — LCC
        "carry_on": 0.0, "carry_on_kg": 7, "checked_bag": 50.0, "checked_bag_kg": 23,
        "seat": 30.0, "currency": "CNY",
    },

    # ── Central Asian / Caucasus ───────────────────────────────────────────
    "J2": {  # Azerbaijan Airlines (AZAL) — flag carrier, bags included
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "USD",
    },

    # ── German leisure / LTQ ──────────────────────────────────────────────
    "4Y": {  # Discover Airlines (Germany, formerly Eurowings Discover)
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 35.0, "checked_bag_kg": 23,
        "seat": 15.0, "currency": "EUR",
    },

    # ── Danish regional ───────────────────────────────────────────────────
    "S5": {  # Star Air (Denmark, regional turboprop) — bags included
        "carry_on": 0.0, "carry_on_kg": None, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "DKK",
    },

    # ── Pacific (Samoa) ───────────────────────────────────────────────────
    "PH": {  # Samoa Airways (alternate IATA code alias for OL)
        "carry_on": 0.0, "carry_on_kg": 10, "checked_bag": 0.0, "checked_bag_kg": 23,
        "seat": 0.0, "currency": "USD",
    },

    # ── Italy ─────────────────────────────────────────────────────────────
    "AZ": {  # ITA Airways (formerly Alitalia) — full-service flag carrier
        "carry_on": 0.0, "carry_on_kg": 8,
        "carry_on_note": "1 cabin bag (8 kg) + 1 personal item included",
        "checked_bag": 40.0, "checked_bag_kg": 23,
        "checked_bag_note": "Eco Light: add-on from ~€40; Eco and above: 1 × 23 kg included",
        "seat": 10.0, "currency": "EUR",
    },
}

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_ancillary_ref(airline_iata: str) -> dict[str, Any]:
    """Return the full ancillary reference entry for *airline_iata*, or {}."""
    return _AIRLINE_ANCILLARY.get(airline_iata.upper(), {})


# Kept for backward compatibility
def get_bags_price(airline_iata: str) -> dict[str, Any]:
    """Return numeric bags_price keys for *airline_iata* (non-None values only)."""
    ref = _AIRLINE_ANCILLARY.get(airline_iata.upper())
    if not ref:
        return {}
    result: dict[str, Any] = {}
    if ref.get("carry_on") is not None:
        result["carry_on"] = ref["carry_on"]
    if ref.get("checked_bag") is not None:
        result["checked_bag"] = ref["checked_bag"]
    if ref.get("seat") is not None:
        result["seat"] = ref["seat"]
    return result


def apply_ref_ancillaries(offer: Any) -> None:  # offer: FlightOffer (avoid circular import)
    """Enrich *offer* with bag/seat conditions from the static reference table.

    Only fills in keys that are not already set — live-parsed data (e.g. Kiwi's
    per-offer tierPrice, Qatar's fareFamilyFeatures) is never overwritten.

    Uses *offer.owner_airline* as the IATA carrier code lookup key.
    """
    iata = (offer.owner_airline or "").upper()
    ref = _AIRLINE_ANCILLARY.get(iata)
    if not ref:
        return

    curr = ref.get("currency", "EUR")

    # ── Carry-on / cabin bag ────────────────────────────────────────────────
    if "carry_on" not in offer.conditions:
        note = ref.get("carry_on_note")
        if not note:
            val = ref.get("carry_on")
            kg = ref.get("carry_on_kg", 10)
            if val is None:
                note = "cabin bag: varies by route and fare class"
            elif val == 0.0:
                kg_str = f", up to {kg} kg" if kg else ""
                note = f"1 cabin bag included{kg_str}"
            else:
                kg_str = f", {kg} kg" if kg else ""
                note = f"cabin bag add-on from ~{val:.0f} {curr}{kg_str}"
        offer.conditions["carry_on"] = note
        # Numeric for downstream aggregation
        if "carry_on" not in offer.bags_price and ref.get("carry_on") is not None:
            offer.bags_price["carry_on"] = ref["carry_on"]

    # ── Checked bag ─────────────────────────────────────────────────────────
    if "checked_bag" not in offer.conditions:
        note = ref.get("checked_bag_note")
        if not note:
            val = ref.get("checked_bag")
            kg = ref.get("checked_bag_kg", 23)
            if val is None:
                note = f"checked bag ({kg} kg): price varies by fare class"
            elif val == 0.0:
                note = f"1 checked bag ({kg} kg) included"
            else:
                note = f"checked bag add-on from ~{val:.0f} {curr} ({kg} kg)"
        offer.conditions["checked_bag"] = note
        if "checked_bag" not in offer.bags_price and ref.get("checked_bag") is not None:
            offer.bags_price["checked_bag"] = ref["checked_bag"]

    # ── Seat selection ───────────────────────────────────────────────────────
    if "seat" not in offer.conditions:
        val = ref.get("seat")
        if val is None:
            note = "seat selection: varies by fare class"
        elif val == 0.0:
            note = "seat selection: free (assigned at check-in)"
        else:
            note = f"seat selection: from ~{val:.0f} {curr}"
        offer.conditions["seat"] = note
        if "seat_selection" not in offer.bags_price and val is not None:
            offer.bags_price["seat_selection"] = val
