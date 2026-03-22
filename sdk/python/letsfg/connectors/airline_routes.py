"""
Airline route coverage — maps each connector to the countries it serves.

Used by the provider to skip connectors that cannot possibly serve a route.
For example, SpiceJet (India domestic + regional) will never have flights
from Paris to Barcelona, so we skip it entirely for European routes.

The mapping uses 2-letter ISO country codes. An airport IATA code is resolved
to its country, and we check if the airline operates in that country.

Design:
- "countries" = set of ISO country codes where the airline operates
- If BOTH origin and destination country are outside the airline's countries,
  the connector is skipped (saves a browser launch / API call)
- If EITHER endpoint is in the airline's network, we run the connector
  (the airline might connect them)
- Airlines with very broad networks (e.g. Kiwi aggregator) are marked global

This data is intentionally conservative — we'd rather run an unnecessary
connector than miss a valid route. Over time, agents can tighten the coverage
based on actual results.
"""

from __future__ import annotations

# ── IATA airport code → ISO country code ──────────────────────────────────
# This is a curated subset covering airports in our connector test routes
# plus major hubs. For unknown airports, the filter falls back to "run it".
AIRPORT_COUNTRY: dict[str, str] = {
    # Europe
    "LHR": "GB", "LGW": "GB", "STN": "GB", "LTN": "GB", "MAN": "GB", "EDI": "GB",
    "BHX": "GB", "BRS": "GB", "EMA": "GB", "GLA": "GB", "LPL": "GB", "NCL": "GB",
    "BFS": "GB", "ABZ": "GB", "EXT": "GB", "SOU": "GB", "CWL": "GB", "PIK": "GB",
    "CDG": "FR", "ORY": "FR", "NCE": "FR", "LYS": "FR", "MRS": "FR", "TLS": "FR",
    "BOD": "FR", "NTE": "FR", "BIQ": "FR",
    "FRA": "DE", "MUC": "DE", "TXL": "DE", "SXF": "DE", "BER": "DE", "HAM": "DE",
    "CGN": "DE", "DUS": "DE", "STR": "DE", "NUE": "DE", "HAJ": "DE", "DTM": "DE",
    "LEJ": "DE",
    "FCO": "IT", "MXP": "IT", "BGY": "IT", "VCE": "IT", "NAP": "IT", "BLQ": "IT",
    "PSA": "IT", "CTA": "IT", "PMO": "IT", "BRI": "IT", "CAG": "IT", "TRN": "IT",
    "BCN": "ES", "MAD": "ES", "PMI": "ES", "AGP": "ES", "ALC": "ES", "VLC": "ES",
    "SVQ": "ES", "IBZ": "ES", "TFS": "ES", "ACE": "ES", "LPA": "ES", "FUE": "ES",
    "SCQ": "ES", "BIO": "ES", "GRO": "ES", "REU": "ES", "XRY": "ES",
    "AMS": "NL", "EIN": "NL", "RTM": "NL",
    "BRU": "BE", "CRL": "BE",
    "ZRH": "CH", "GVA": "CH", "BSL": "CH",
    "VIE": "AT", "SZG": "AT", "INN": "AT", "GRZ": "AT", "LNZ": "AT",
    "LIS": "PT", "OPO": "PT", "FAO": "PT", "FNC": "PT",
    "DUB": "IE", "SNN": "IE", "ORK": "IE", "KNO": "IE",
    "ATH": "GR", "SKG": "GR", "HER": "GR", "RHO": "GR", "CFU": "GR",
    "JMK": "GR", "JTR": "GR", "CHQ": "GR", "ZTH": "GR", "KGS": "GR",
    "WAW": "PL", "KRK": "PL", "GDN": "PL", "WRO": "PL", "KTW": "PL",
    "POZ": "PL", "RZE": "PL", "SZZ": "PL", "BZG": "PL", "LUZ": "PL",
    "PRG": "CZ", "BRQ": "CZ", "OSR": "CZ",
    "BUD": "HU", "DEB": "HU",
    "OTP": "RO", "CLJ": "RO", "TSR": "RO", "IAS": "RO", "SBZ": "RO",
    "SOF": "BG", "BOJ": "BG", "VAR": "BG",
    "BEG": "RS", "INI": "RS",
    "ZAG": "HR", "SPU": "HR", "DBV": "HR", "PUY": "HR", "ZAD": "HR",
    "LJU": "SI",
    "SKP": "MK",
    "TIA": "AL",
    "HEL": "FI", "TMP": "FI", "OUL": "FI", "TKU": "FI",
    "ARN": "SE", "GOT": "SE", "MMX": "SE",
    "OSL": "NO", "BGO": "NO", "TRD": "NO", "SVG": "NO", "TOS": "NO",
    "CPH": "DK", "BLL": "DK", "AAL": "DK",
    "KEF": "IS", "RKV": "IS",
    "RIX": "LV", "VNO": "LT", "TLL": "EE",
    "KIV": "MD",
    "IEV": "UA", "KBP": "UA", "LWO": "UA", "ODS": "UA",
    "MSQ": "BY",
    "IST": "TR", "SAW": "TR", "AYT": "TR", "ADB": "TR", "ESB": "TR",
    "DLM": "TR", "BJV": "TR", "GZT": "TR", "TZX": "TR",
    "TIV": "ME", "TGD": "ME",
    "SJJ": "BA",
    # Middle East
    "DXB": "AE", "AUH": "AE", "SHJ": "AE",
    "DOH": "QA",
    "BAH": "BH",
    "KWI": "KW",
    "MCT": "OM",
    "RUH": "SA", "JED": "SA", "DMM": "SA", "MED": "SA",
    "AMM": "JO",
    "BEY": "LB",
    "TLV": "IL",
    "CAI": "EG", "HRG": "EG", "SSH": "EG", "LXR": "EG",
    # South Asia
    "DEL": "IN", "BOM": "IN", "BLR": "IN", "MAA": "IN", "HYD": "IN",
    "CCU": "IN", "GOI": "IN", "COK": "IN", "PNQ": "IN", "AMD": "IN",
    "JAI": "IN", "GAU": "IN", "IXC": "IN", "SXR": "IN", "ATQ": "IN",
    "LKO": "IN", "PAT": "IN", "BBI": "IN", "IXR": "IN", "CCJ": "IN",
    "TRV": "IN", "IXE": "IN", "VTZ": "IN", "IXB": "IN", "IMF": "IN",
    "CMB": "LK", "MLE": "MV", "KTM": "NP",
    "DAC": "BD", "CGP": "BD", "ZYL": "BD", "RJH": "BD", "SPD": "BD", "BZL": "BD",
    "ISB": "PK", "LHE": "PK", "KHI": "PK",
    # Southeast Asia
    "SIN": "SG",
    "KUL": "MY", "PEN": "MY", "LGK": "MY", "BKI": "MY", "KCH": "MY",
    "BKK": "TH", "DMK": "TH", "CNX": "TH", "HKT": "TH", "USM": "TH",
    "HDY": "TH", "CEI": "TH",
    "SGN": "VN", "HAN": "VN", "DAD": "VN", "CXR": "VN", "PQC": "VN",
    "CGK": "ID", "DPS": "ID", "SUB": "ID", "JOG": "ID", "UPG": "ID",
    "MNL": "PH", "CEB": "PH", "DVO": "PH", "ILO": "PH", "CRK": "PH",
    "RGN": "MM", "MDL": "MM",
    "PNH": "KH", "REP": "KH",
    "VTE": "LA", "LPQ": "LA",
    # East Asia
    "NRT": "JP", "HND": "JP", "KIX": "JP", "FUK": "JP", "CTS": "JP",
    "NGO": "JP", "OKA": "JP", "ITM": "JP",
    "ICN": "KR", "GMP": "KR", "CJU": "KR", "PUS": "KR", "TAE": "KR",
    "PEK": "CN", "PVG": "CN", "CAN": "CN", "SZX": "CN", "CTU": "CN",
    "KMG": "CN", "NKG": "CN", "HGH": "CN", "WUH": "CN", "XIY": "CN",
    "TSN": "CN", "CGO": "CN", "TAO": "CN", "DLC": "CN",
    "HKG": "HK", "MFM": "MO",
    "TPE": "TW", "KHH": "TW",
    # Oceania
    "SYD": "AU", "MEL": "AU", "BNE": "AU", "PER": "AU", "ADL": "AU",
    "OOL": "AU", "CBR": "AU", "CNS": "AU", "HBA": "AU", "DRW": "AU",
    "AKL": "NZ", "WLG": "NZ", "CHC": "NZ", "ZQN": "NZ",
    # North America
    "JFK": "US", "LAX": "US", "ORD": "US", "ATL": "US", "DFW": "US",
    "DEN": "US", "SFO": "US", "SEA": "US", "MIA": "US", "BOS": "US",
    "EWR": "US", "IAD": "US", "PHX": "US", "IAH": "US", "MCO": "US",
    "MSP": "US", "DTW": "US", "FLL": "US", "CLT": "US", "LAS": "US",
    "SLC": "US", "SAN": "US", "TPA": "US", "PDX": "US", "STL": "US",
    "BNA": "US", "AUS": "US", "RDU": "US", "MKE": "US", "IND": "US",
    "PIT": "US", "CMH": "US", "SAT": "US", "OAK": "US", "SJC": "US",
    "SMF": "US", "RSW": "US", "PBI": "US", "BUR": "US", "OGG": "US",
    "HNL": "US", "ANC": "US", "ABQ": "US", "ELP": "US",
    "YYZ": "CA", "YVR": "CA", "YUL": "CA", "YOW": "CA", "YYC": "CA",
    "YEG": "CA", "YHZ": "CA", "YWG": "CA", "YTZ": "CA", "YQB": "CA",
    # Mexico & Central America
    "MEX": "MX", "CUN": "MX", "GDL": "MX", "MTY": "MX", "TIJ": "MX",
    "SJD": "MX", "PVR": "MX", "MID": "MX", "BJX": "MX",
    "PTY": "PA", "SJO": "CR", "SAL": "SV", "GUA": "GT", "TGU": "HN",
    "MGA": "NI", "BZE": "BZ",
    # Caribbean
    "SXM": "SX", "PUJ": "DO", "SDQ": "DO", "STI": "DO",
    "HAV": "CU", "VRA": "CU",
    "KIN": "JM", "MBJ": "JM",
    "NAS": "BS",
    "AUA": "AW", "CUR": "CW", "BON": "BQ",
    "SJU": "PR", "BGI": "BB", "POS": "TT",
    # South America
    "GRU": "BR", "GIG": "BR", "BSB": "BR", "CNF": "BR", "SSA": "BR",
    "REC": "BR", "CWB": "BR", "POA": "BR", "FOR": "BR", "BEL": "BR",
    "VCP": "BR", "SDU": "BR", "FLN": "BR", "NAT": "BR", "MCZ": "BR",
    "EZE": "AR", "AEP": "AR", "COR": "AR", "MDZ": "AR", "IGR": "AR",
    "BRC": "AR", "SLA": "AR", "TUC": "AR", "NQN": "AR", "USH": "AR",
    "BUE": "AR",  # Buenos Aires city code
    "SCL": "CL", "IQQ": "CL", "ANF": "CL", "CCP": "CL", "PMC": "CL",
    "ZCO": "CL", "PUQ": "CL",
    "LIM": "PE", "CUZ": "PE", "AQP": "PE",
    "BOG": "CO", "MDE": "CO", "CLO": "CO", "CTG": "CO", "BAQ": "CO",
    "UIO": "EC", "GYE": "EC",
    "CCS": "VE",
    "ASU": "PY", "MVD": "UY",
    "VVI": "BO", "LPB": "BO",
    # Africa
    "JNB": "ZA", "CPT": "ZA", "DUR": "ZA", "PLZ": "ZA",
    "LOS": "NG", "ABV": "NG", "PHC": "NG",
    "NBO": "KE", "MBA": "KE",
    "ADD": "ET",
    "ACC": "GH",
    "DSS": "SN",
    "CMN": "MA", "RAK": "MA", "FEZ": "MA", "AGA": "MA", "TNG": "MA",
    "ALG": "DZ", "ORN": "DZ",
    "TUN": "TN", "NBE": "TN", "DJE": "TN",
}

# City codes that map to multiple airports in a country
CITY_COUNTRY: dict[str, str] = {
    "LON": "GB", "PAR": "FR", "ROM": "IT", "MIL": "IT", "BUE": "AR",
    "NYC": "US", "WAS": "US", "CHI": "US", "TYO": "JP", "OSA": "JP",
    "SEL": "KR", "BJS": "CN", "SHA": "CN", "BKK": "TH", "KUL": "MY",
    "REK": "IS", "MOW": "RU", "STO": "SE",
}


def get_country(iata: str) -> str | None:
    """Resolve an IATA airport or city code to its ISO country code."""
    iata = iata.upper().strip()
    return AIRPORT_COUNTRY.get(iata) or CITY_COUNTRY.get(iata)


# ── Airline → operating countries ─────────────────────────────────────────
# Each airline maps to the set of countries where it operates flights.
# The key must match the source name prefix in _DIRECT_AIRLINE_connectorS
# (e.g. "easyjet_direct" → key "easyjet").
#
# If an airline is not listed here, it will always be queried (safe default).
# If EITHER the origin or destination country is in the set, we query it.

AIRLINE_COUNTRIES: dict[str, set[str]] = {
    # ── Europe ──
    "easyjet": {
        "GB", "FR", "DE", "IT", "ES", "NL", "CH", "AT", "PT", "IE",
        "GR", "DK", "SE", "NO", "FI", "PL", "CZ", "HU", "HR", "BG",
        "RO", "RS", "ME", "MK", "IL", "EG", "MA", "TN", "TR", "IS",
    },
    "ryanair": {
        "GB", "IE", "FR", "DE", "IT", "ES", "PT", "NL", "BE", "AT",
        "CH", "PL", "CZ", "HU", "RO", "BG", "HR", "GR", "CY", "MT",
        "DK", "SE", "NO", "FI", "LT", "LV", "EE", "SK", "SI",
        "MA", "IL", "JO", "TR",
    },
    "norwegian": {
        "NO", "SE", "DK", "FI", "GB", "IE", "FR", "ES", "IT", "GR",
        "PT", "HR", "CY", "US", "TH",
    },
    "vueling": {
        "ES", "FR", "IT", "GB", "NL", "BE", "DE", "AT", "CH", "PT",
        "IE", "GR", "HR", "MA", "DZ", "IL", "SE", "NO", "DK", "PL",
        "RO", "BG", "RS", "AL", "BA",
    },
    "eurowings": {
        "DE", "AT", "CH", "ES", "IT", "GR", "HR", "PT", "FR", "GB",
        "SE", "NO", "DK", "BG", "TR", "EG", "TN", "MA",
    },
    "transavia": {
        "NL", "FR", "ES", "IT", "PT", "GR", "HR", "MA", "TN", "EG",
        "TR", "IL", "AT", "DE",
    },
    "condor": {
        "DE", "AT", "CH", "ES", "PT", "GR", "IT", "HR", "TR", "EG",
        "TN", "MA", "US", "CA", "MX", "DO", "CU", "JM", "BB", "CR",
        "KE", "TZ", "MV", "TH",
    },
    "smartwings": {
        "CZ", "ES", "IT", "GR", "FR", "PT", "TR", "EG", "TN", "BG",
        "HR", "GB", "IL", "AE", "CV",
    },
    "volotea": {
        "ES", "IT", "FR", "GR", "HR", "DE", "AT",
    },
    "play": {
        "IS", "GB", "IE", "FR", "DE", "DK", "NO", "SE", "ES", "PT",
        "NL", "BE", "PL", "US", "CA",
    },
    "sunexpress": {
        "TR", "DE", "AT", "CH", "GB", "NL", "BE", "DK", "SE", "NO",
        "FI",
    },
    "pegasus": {
        "TR", "DE", "AT", "CH", "FR", "GB", "NL", "BE", "DK", "SE",
        "NO", "IT", "ES", "GR", "BG", "RO", "UA", "GE", "AZ", "KZ",
        "AE", "BH", "KW", "QA", "SA", "EG", "TN", "MA", "IL", "LB",
        "JO", "IQ",
    },
    "jet2": {
        "GB", "ES", "PT", "GR", "TR", "IT", "FR", "HR", "CY", "MT",
        "BG", "HU", "CZ", "AT", "CH",
    },
    "wizz": {
        "PL", "HU", "RO", "BG", "GB", "IT", "DE", "AT", "FR", "NL",
        "BE", "ES", "PT", "GR", "HR", "RS", "MK", "AL", "BA", "ME",
        "MD", "UA", "GE", "AE", "SA", "BH", "KW", "QA", "OM", "JO",
        "AM", "NO", "SE", "DK", "FI", "IS", "CY", "MT", "IL", "EG",
        "MA",
    },

    # ── Americas ──
    "southwest": {"US", "MX", "PR", "JM", "BS", "DO", "CU", "AW", "CR", "BZ", "TT"},
    "spirit": {"US", "PR", "CO", "MX", "JM", "DO", "GT", "SV", "HN", "CR", "PA", "PE", "EC"},
    "frontier": {"US", "MX", "DO", "JM", "PR", "GT", "CR"},
    "allegiant": {"US", "MX", "PR", "DO"},
    "jetblue": {"US", "PR", "DO", "JM", "BS", "MX", "CR", "CO", "EC", "PE", "GB", "FR", "NL"},
    "avelo": {"US"},
    "breeze": {"US"},
    "alaska": {"US", "MX", "CA", "CR", "BZ", "GT"},
    "hawaiian": {"US", "JP", "KR", "AU", "NZ", "AS", "WS"},
    "american": {
        "US", "CA", "MX", "GB", "IE", "FR", "DE", "ES", "IT", "NL",
        "CH", "GR", "CZ", "PT", "IS", "DK", "SE", "NO", "FI", "PL",
        "HU", "HR", "JP", "KR", "CN", "HK", "IN", "AU", "NZ",
        "BR", "AR", "CL", "CO", "PE", "EC",
        "DO", "JM", "BS", "PR", "CU", "HT", "TT", "BB", "AW", "CW",
        "CR", "PA", "GT", "SV", "HN", "NI", "BZ", "GY",
        "IL", "JO", "AE", "QA", "BH",
    },
    "united": {
        "US", "CA", "MX", "GB", "IE", "FR", "DE", "ES", "IT", "NL",
        "BE", "CH", "AT", "GR", "CZ", "PT", "IS", "DK", "SE", "NO",
        "PL", "HU", "HR", "JP", "KR", "CN", "HK", "TW", "SG", "TH",
        "IN", "AU", "NZ", "PH",
        "BR", "AR", "CL", "CO", "PE", "EC",
        "DO", "JM", "BS", "PR", "CU", "HT", "TT", "AW", "CW",
        "CR", "PA", "GT", "SV", "HN", "NI", "BZ",
        "IL", "JO", "AE", "QA",
        "GU", "MH", "PW", "FM", "MP",
    },
    "delta": {
        "US", "CA", "MX", "GB", "IE", "FR", "DE", "ES", "IT", "NL",
        "BE", "CH", "AT", "GR", "CZ", "PT", "IS", "DK", "SE", "NO",
        "FI", "PL", "HU", "HR", "JP", "KR", "CN", "HK", "TW", "SG",
        "IN", "AU", "NZ", "GH", "ZA", "SN",
        "BR", "AR", "CL", "CO", "PE", "EC",
        "DO", "JM", "BS", "PR", "CU", "HT", "TT", "BB", "AW", "CW",
        "CR", "PA", "GT", "SV", "HN", "NI", "BZ",
        "IL", "AE", "QA",
        "GU", "AS",
    },
    "volaris": {"US", "MX", "GT", "SV", "HN", "CR", "NI"},
    "vivaaerobus": {"US", "MX", "CO", "CU", "DO", "PE"},
    "jetsmart": {"CL", "AR", "PE", "CO", "BR"},
    "flybondi": {"AR", "BR", "CL", "PY", "UY", "CO", "PE"},
    "flair": {"CA", "US", "MX"},
    "porter": {"CA", "US", "MX", "JM", "BS", "DO", "CU", "CR", "BB", "TT"},
    "gol": {"BR", "AR", "CL", "UY", "PY", "CO", "PE", "EC", "VE", "DO", "MX", "US"},
    "azul": {"BR", "AR", "CL", "UY", "PY", "US", "PT", "FR"},

    # ── Asia-Pacific ──
    "airasia": {
        "MY", "TH", "ID", "PH", "SG", "VN", "KH", "MM", "LA", "IN",
        "LK", "BD", "NP", "CN", "JP", "KR", "TW", "HK", "AU", "NZ",
        "SA", "AE", "TR", "EG", "KE",
    },
    "vietjet": {"VN", "TH", "KR", "JP", "TW", "IN", "SG", "MY", "ID", "PH", "KH", "AU"},
    "cebupacific": {"PH", "SG", "MY", "TH", "VN", "KR", "JP", "TW", "CN", "HK", "AU", "AE"},
    "scoot": {"SG", "TH", "MY", "ID", "VN", "PH", "KR", "JP", "TW", "CN", "HK", "IN", "AU", "GR", "DE", "GB", "FI", "SA"},
    "nokair": {"TH", "CN", "JP", "KR", "VN", "MM", "IN", "SG"},
    "jetstar": {"AU", "NZ", "JP", "SG", "VN", "ID", "TH", "MY", "PH"},
    "peach": {"JP", "KR", "TW", "CN", "HK", "TH"},
    "zipair": {"JP", "KR", "TH", "SG", "US", "CA"},
    "spring": {"CN", "JP", "KR", "TH", "KH", "MY"},
    "luckyair": {"CN", "TH", "MM", "LA", "KH", "VN", "BD"},
    "9air": {"CN"},
    "jejuair": {"KR", "JP", "TW", "PH", "VN", "TH", "SG", "MY", "GU"},
    "twayair": {"KR", "JP", "TW", "VN", "TH", "PH", "SG", "GU"},
    "batikair": {"ID", "MY", "SG", "TH", "AU"},
    "thai": {
        "TH", "JP", "KR", "CN", "HK", "TW", "SG", "MY", "ID", "VN",
        "KH", "LA", "MM", "PH", "IN", "LK", "BD", "NP", "PK", "AU",
        "GB", "DE", "FR", "IT", "CH", "AT", "DK", "SE", "NO", "BE",
        "TR",
    },
    "korean": {
        "KR", "JP", "CN", "HK", "TW", "SG", "TH", "VN", "KH", "PH",
        "MY", "ID", "IN", "LK", "BD", "NP", "MN", "UZ", "KZ", "AU",
        "US", "CA", "GB", "FR", "DE", "IT", "NL", "CH", "AT", "CZ",
        "HU", "PL", "HR", "SE", "DK", "FI", "ES", "TR", "IL", "AE",
        "RU", "NZ",
    },

    # ── Middle East / Africa / India ──
    "flydubai": {
        "AE", "SA", "BH", "KW", "QA", "OM", "IN", "PK", "LK", "BD",
        "NP", "EG", "JO", "LB", "IQ", "IR", "GE", "AM", "AZ", "KZ",
        "UZ", "TJ", "KG", "RS", "BG", "RO", "HR", "GR", "IT", "CZ",
        "PL", "AT", "TR", "TH", "LK", "ET", "KE", "TZ",
    },
    "airarabia": {
        "AE", "AF", "AM", "AT", "AZ", "BA", "BD", "BE", "BH", "CH",
        "CN", "CZ", "DE", "DZ", "EG", "ES", "ET", "FR", "GB", "GE",
        "GR", "HR", "IN", "IQ", "IR", "IT", "JO", "KE", "KG", "KH",
        "KW", "KZ", "LB", "LK", "MA", "MV", "MY", "NL", "NP", "OM",
        "PK", "PL", "QA", "RU", "SA", "SD", "SO", "SY", "TH", "TN",
        "TR", "UG", "UZ",
    },
    "flynas": {
        "SA", "AE", "BH", "KW", "QA", "OM", "EG", "JO", "LB", "IQ",
        "TR", "IN", "PK", "BD", "LK", "GE", "AZ", "MA", "TN", "ET",
        "SD", "BA", "RS", "ME", "AL",
    },
    "spicejet": {
        "IN", "AE", "SA", "OM", "TH", "HK", "BD", "BH", "KW", "LK",
        "MM", "MV", "NP", "QA",
    },
    "indigo": {
        "IN", "AE", "SA", "QA", "KW", "OM", "BH", "SG", "TH", "MY",
        "VN", "HK", "ID", "LK", "MV", "NP", "BD", "MM", "GE", "UZ",
        "KE", "TZ", "TR", "GB",
    },
    "akasa": {"IN", "SA", "QA", "KW", "BH"},
    "salamair": {
        "OM", "AE", "SA", "BH", "KW", "QA", "IN", "BD", "PK", "LK",
        "NP", "EG", "IR", "IQ", "SD", "KE", "TZ", "ET", "TH", "MY",
        "GE", "TR", "GB", "AT", "CZ", "BA", "RS",
    },
    "airindiaexpress": {"IN", "AE", "SA", "QA", "KW", "OM", "BH", "MY", "SG", "TH"},
    "usbangla": {
        "BD", "AE", "OM", "QA", "SA", "IN", "MY", "SG", "TH", "CN",
        "MV", "NP", "DE", "GB", "US",
    },
    "flysafair": {"ZA"},
    "airpeace": {"NG", "GH", "ZA", "KE", "AE", "GB"},
    "biman": {
        "BD", "AE", "SA", "QA", "KW", "OM", "IN", "NP", "TH", "MY",
        "SG", "HK", "CN", "IT", "GB", "CA", "PK",
    },
    "etihad": {
        "AE", "SA", "QA", "KW", "BH", "OM", "EG", "JO", "LB", "IQ",
        "PK", "IN", "LK", "BD", "NP", "TH", "MY", "SG", "ID", "PH",
        "CN", "JP", "KR", "AU", "GB", "IE", "FR", "DE", "IT", "ES",
        "CH", "NL", "GR", "TR", "US", "CA", "KE", "TZ", "ZA", "NG",
        "ET", "BR", "MV", "SC", "KZ", "RU",
    },
    "turkish": {
        "TR", "DE", "GB", "FR", "IT", "ES", "NL", "BE", "CH", "AT",
        "SE", "NO", "DK", "FI", "PL", "CZ", "HU", "RO", "BG", "HR",
        "RS", "BA", "ME", "MK", "AL", "GR", "CY", "PT", "IE", "IS",
        "LV", "LT", "EE", "SK", "SI", "MT", "LU", "UA", "MD", "GE",
        "AM", "AZ", "RU", "KZ", "UZ", "TM", "KG", "TJ",
        "US", "CA", "BR", "AR", "CO", "MX", "PA", "CU", "DO",
        "AE", "SA", "QA", "KW", "BH", "OM", "JO", "LB", "IQ", "IR",
        "IL", "EG", "LY", "TN", "DZ", "MA",
        "IN", "PK", "BD", "LK", "MV", "NP",
        "TH", "MY", "SG", "ID", "VN", "PH", "KH", "MM",
        "CN", "HK", "JP", "KR", "TW",
        "AU", "NZ",
        "ZA", "KE", "ET", "NG", "GH", "SN", "CI", "TZ", "MZ", "MU",
        "CM", "GA", "CG", "CD", "SD", "DJ", "SO", "MG", "RW", "UG",
    },
    "nh": {
        "JP", "US", "CA", "MX",
        "GB", "FR", "DE", "IT", "BE", "AT", "SE",
        "CN", "HK", "TW", "KR",
        "TH", "MY", "SG", "ID", "VN", "PH", "KH", "MM", "IN",
        "AU",
    },
    "emirates": {
        "AE", "SA", "QA", "KW", "BH", "OM", "EG", "JO", "LB", "IQ", "IR",
        "PK", "IN", "LK", "BD", "NP", "MV",
        "TH", "MY", "SG", "ID", "PH", "VN", "KH", "MM",
        "CN", "HK", "JP", "KR", "TW", "AU", "NZ",
        "GB", "IE", "FR", "DE", "IT", "ES", "PT", "CH", "NL", "BE",
        "AT", "SE", "NO", "DK", "FI", "PL", "CZ", "HU", "RO", "BG",
        "GR", "CY", "TR", "RU",
        "US", "CA", "MX", "BR", "AR", "CL", "CO",
        "KE", "TZ", "ZA", "NG", "ET", "GH", "UG", "MU", "SC",
        "KZ", "UZ", "AZ",
    },
    "malaysia": {
        "MY", "SG", "TH", "ID", "VN", "KH", "MM", "PH", "BN", "LA",
        "IN", "LK", "BD", "NP", "MV",
        "CN", "HK", "TW", "JP", "KR",
        "AU", "NZ",
        "AE", "SA", "QA", "BH", "OM",
        "GB", "NL", "FR", "DE", "TR",
        "US",
        "KE", "ZA",
    },
    "cathay": {
        "HK", "CN", "TW", "JP", "KR",
        "SG", "TH", "MY", "ID", "PH", "VN", "KH", "MM", "IN", "LK", "MV", "NP", "BD",
        "AU", "NZ",
        "GB", "FR", "DE", "IT", "ES", "NL", "BE",
        "US", "CA",
        "AE", "QA", "SA",
        "ZA", "KE", "SN",
    },
    "singapore": {
        "SG", "MY", "TH", "ID", "VN", "PH", "KH", "MM", "BN", "LA",
        "IN", "LK", "BD", "NP", "MV",
        "CN", "HK", "TW", "JP", "KR",
        "AU", "NZ",
        "AE", "SA", "BH", "IL",
        "GB", "FR", "DE", "IT", "ES", "NL", "CH", "DK", "SE", "TR",
        "US",
        "ZA", "KE",
    },
    # ── Wired 2026-03-20 ──────────────────────────────────────────
    "aegean": {
        "GR", "CY", "DE", "GB", "FR", "IT", "ES", "CH", "AT", "RO",
        "BG", "EG", "SA", "GE", "AM", "IL", "RU", "UA", "CZ", "PL",
        "NL", "SE",
    },
    "olympicair": {
        "GR", "CY", "DE", "GB", "FR", "IT", "ES", "CH", "AT", "RO",
        "BG", "EG", "SA", "GE", "AM", "IL", "RU", "UA", "CZ", "PL",
        "NL", "SE",
    },
    "aerlingus": {
        "IE", "GB", "US", "FR", "ES", "PT", "IT", "DE", "NL", "BE",
        "AT", "CH", "PL", "HR",
    },
    "airbaltic": {
        "LV", "LT", "EE", "FI", "SE", "NO", "DK", "DE", "AT", "CH",
        "NL", "BE", "FR", "IT", "ES", "PT", "GB", "IE", "GR", "CY",
        "TR", "GE", "AZ", "IL", "EG", "AE",
    },
    "aircanada": {
        "CA", "US", "MX", "GB", "IE", "FR", "DE", "CH", "IT", "ES",
        "PT", "NL", "BE", "AT", "DK", "SE", "NO", "JP", "KR", "CN",
        "HK", "IN", "AU", "NZ", "BR", "CO", "CR", "DO", "JM", "CU",
        "BB", "TT", "BS",
    },
    "airindia": {
        "IN", "US", "CA", "GB", "FR", "DE", "IT", "AT", "CH", "DK",
        "SE", "NO", "FI", "AE", "SA", "QA", "KW", "BH", "OM", "SG",
        "TH", "MY", "AU", "NZ", "JP", "KR", "HK", "LK", "BD", "NP",
        "KE",
    },
    "airnewzealand": {
        "NZ", "AU", "FJ", "NC", "CK", "TO", "WS", "US", "CA", "GB",
        "JP", "CN", "HK", "SG",
    },
    "arajet": {
        "DO", "CO", "MX", "GT", "SV", "HN", "CR", "PA", "CU", "JM",
        "EC", "PE", "CL", "BR", "CA",
    },
    "bangkokairways": {
        "TH", "KH", "LA", "MM", "MY", "SG", "IN", "MV", "HK", "CN",
    },
    "egyptair": {
        "EG", "SA", "AE", "KW", "QA", "BH", "OM", "JO", "LB", "IQ",
        "GB", "FR", "DE", "IT", "ES", "GR", "TR", "US", "CA", "IN",
        "TH", "CN", "JP", "KR", "ZA", "KE", "NG", "ET", "TZ", "SD",
    },
    "ethiopian": {
        "ET", "KE", "TZ", "UG", "NG", "GH", "ZA", "EG", "SA", "AE",
        "QA", "IN", "CN", "HK", "JP", "KR", "GB", "FR", "DE", "IT",
        "US", "CA", "BR", "IL", "TR",
    },
    "finnair": {
        "FI", "SE", "NO", "DK", "EE", "LV", "LT", "GB", "IE", "FR",
        "DE", "ES", "IT", "NL", "BE", "AT", "CH", "PL", "CZ", "GR",
        "HR", "PT", "US", "JP", "KR", "CN", "HK", "TH", "SG", "IN",
    },
    "garuda": {
        "ID", "SG", "MY", "TH", "AU", "JP", "KR", "CN", "HK", "NL",
        "SA", "AE",
    },
    "icelandair": {
        "IS", "US", "CA", "GB", "IE", "FR", "DE", "NL", "BE", "DK",
        "SE", "NO", "FI",
    },
    "itaairways": {
        "IT", "FR", "DE", "ES", "GB", "NL", "BE", "CH", "AT", "GR",
        "US", "BR", "AR", "JP", "IL", "EG", "TN",
    },
    "jal": {
        "JP", "US", "CA", "GB", "FR", "DE", "FI", "AU", "CN", "HK",
        "TW", "KR", "TH", "SG", "MY", "ID", "VN", "IN",
    },
    "jazeera": {
        "KW", "AE", "SA", "BH", "QA", "OM", "EG", "IN", "PK", "BD",
        "LK", "NP", "TR", "GE", "AZ",
    },
    "kenyaairways": {
        "KE", "TZ", "UG", "ET", "NG", "ZA", "EG", "SA", "AE", "IN",
        "CN", "HK", "TH", "GB", "FR", "NL", "US",
    },
    "philippineairlines": {
        "PH", "US", "CA", "JP", "KR", "CN", "HK", "TW", "SG", "AU",
        "AE", "SA", "GB",
    },
    "qantas": {
        "AU", "NZ", "US", "GB", "JP", "SG", "HK", "CN", "IN", "ID",
        "TH", "MY", "PH", "VN", "FJ", "ZA", "CL", "CA",
    },
    "royalairmaroc": {
        "MA", "FR", "ES", "IT", "DE", "GB", "NL", "BE", "PT", "US",
        "CA", "BR", "SA", "AE", "QA", "EG", "TN", "DZ", "SN", "CI",
        "GA", "CM", "ML",
    },
    "saa": {
        "ZA", "GH", "NG", "KE", "TZ", "MU", "MZ", "ZW", "NA", "GB",
        "DE", "US", "AU", "HK", "SG", "IN",
    },
    "sas": {
        "SE", "NO", "DK", "FI", "IS", "GB", "IE", "FR", "DE", "NL",
        "BE", "ES", "IT", "PT", "GR", "CH", "AT", "PL", "US", "JP",
        "CN", "HK", "TH",
    },
    "skyairline": {
        "CL", "PE", "AR", "BR", "CO", "UY",
    },
    "skyexpress": {
        "GR", "CY", "TR", "DE", "FR", "IT", "BE", "NL", "AT", "CZ",
        "HU", "PL", "GB", "IE", "AL", "AM", "GE",
    },
    "aerolineas": {
        "AR", "BR", "CL", "UY", "PY", "BO", "PE", "EC", "CO", "VE",
        "US", "MX", "CU", "DO", "ES", "IT",
    },
    "chinaairlines": {
        "US", "TW", "VN", "PH",
    },
    "flyarystan": {
        "KZ", "TR", "GE", "KG", "AE", "AZ", "UZ", "CN", "IN",
    },
    "tap": {
        "PT", "ES", "FR", "DE", "IT", "GB", "NL", "BE", "CH", "LU",
        "US", "CA", "BR", "MZ", "CV", "GW", "SN", "MA",
    },
    "virginaustralia": {
        "AU", "NZ", "US", "JP", "HK", "ID", "FJ", "TO", "WS", "VU",
    },
    "wingo": {
        "CO", "PA", "VE", "EC", "MX", "DO", "CU", "GT", "CW", "AW",
    },
    "klm": {
        "NL", "GB", "IE", "FR", "DE", "ES", "IT", "PT", "BE", "CH",
        "AT", "CZ", "PL", "HU", "RO", "BG", "HR", "RS", "SI", "SK",
        "DK", "SE", "NO", "FI", "IS", "GR", "CY", "TR",
        "US", "CA", "MX", "BR", "AR", "CL", "CO", "PE", "PA",
        "AE", "QA", "BH", "KW", "SA", "IL", "JO",
        "EG", "MA", "TN", "KE", "SN", "GH", "NG", "ZA", "ET", "TZ",
        "JP", "KR", "CN", "HK", "TW", "SG", "MY", "TH", "VN", "ID",
        "IN", "AU", "NZ",
    },
    "airfrance": {
        "FR", "NL", "GB", "IE", "DE", "ES", "IT", "PT", "BE", "CH",
        "AT", "CZ", "PL", "HU", "RO", "BG", "HR", "RS", "SI", "SK",
        "DK", "SE", "NO", "FI", "GR", "CY", "TR",
        "US", "CA", "MX", "BR", "AR", "CL", "CO", "PE",
        "AE", "QA", "SA", "IL", "JO",
        "EG", "MA", "TN", "SN", "CI", "CM", "GA", "CG", "MG",
        "JP", "KR", "CN", "HK", "SG", "TH", "VN", "IN",
    },
    "iberia": {
        "ES", "GB", "US", "FR", "DE", "IT", "PT", "NL", "BE", "CH",
        "AT", "CZ", "PL", "HU", "RO", "BG", "HR", "GR", "CY", "TR",
        "DK", "SE", "NO", "FI",
        "MX", "BR", "AR", "CL", "CO", "PE", "EC", "UY", "PA", "DO",
        "CR", "GT", "SV", "HN", "NI", "CU", "VE",
        "MA", "EG", "IL", "AE", "QA", "SA", "BH",
        "JP", "KR", "CN", "IN", "TH", "AU", "NZ",
        "ZA", "KE", "NG", "GH", "SN",
    },
    "virginatlantic": {
        "GB", "US", "BB", "JM", "AG", "GD", "LC", "TT", "BS", "DO",
        "CU", "MX", "IL", "AE", "IN", "HK", "CN",
        "ZA", "KE", "NG",
    },
    # ── British Airways (SOLR pricing feed — oneworld, IAG) ──
    "britishairways": {
        "GB", "IE", "FR", "DE", "ES", "IT", "PT", "NL", "BE", "CH",
        "AT", "CZ", "PL", "HU", "RO", "BG", "HR", "RS", "SI", "SK",
        "DK", "SE", "NO", "FI", "IS", "GR", "CY", "TR", "MT",
        "US", "CA", "MX", "BR", "AR", "CL", "CO", "PE", "CR", "PA",
        "BB", "JM", "AG", "GD", "LC", "TT", "BS", "KY", "BM", "DO",
        "AE", "QA", "BH", "KW", "SA", "OM", "IL", "JO",
        "EG", "MA", "TN", "KE", "ZA", "GH", "NG", "MU", "SC",
        "JP", "KR", "CN", "HK", "SG", "MY", "TH", "IN", "LK", "MV",
        "AU", "NZ",
    },
    # ── Iberia Express (via Iberia LD+JSON — domestic Spain + short-haul EU) ──
    "iberiaexpress": {
        "ES", "PT", "IT", "GR", "IE", "DE", "CZ", "HU", "DK",
    },
    # ── Lufthansa Group (JSON-LD from lufthansa.com flight pages) ──
    "lufthansa": {
        "DE", "AT", "CH", "BE", "NL", "LU",
        "GB", "IE", "FR", "IT", "ES", "PT", "GR", "CY", "TR", "MT",
        "PL", "CZ", "HU", "RO", "BG", "HR", "RS", "SI", "BA", "MK", "AL", "ME", "SK",
        "DK", "SE", "NO", "FI", "IS", "EE", "LV", "LT",
        "US", "CA", "MX", "BR", "AR", "CL", "CO", "PA",
        "AE", "SA", "QA", "BH", "OM", "KW", "JO", "LB", "IL", "EG",
        "JP", "KR", "CN", "HK", "SG", "TH", "IN", "PK", "LK", "MV", "BD", "NP",
        "ZA", "KE", "NG", "ET", "GH", "TZ", "MA", "TN", "DZ", "MU",
        "AU", "NZ",
    },
    "swiss": {
        "CH", "DE", "AT", "GB", "IE", "FR", "IT", "ES", "PT", "GR", "CY", "TR",
        "NL", "BE", "LU", "PL", "CZ", "HU", "RO", "BG", "HR", "RS",
        "DK", "SE", "NO", "FI", "EE", "LV", "LT",
        "US", "CA", "BR",
        "AE", "SA", "IL", "EG", "JO",
        "JP", "CN", "HK", "SG", "TH", "IN",
        "ZA", "KE",
    },
    "austrian": {
        "AT", "DE", "CH", "GB", "IE", "FR", "IT", "ES", "GR", "CY", "TR",
        "NL", "BE", "PL", "CZ", "HU", "RO", "BG", "HR", "RS", "SI", "BA", "MK", "AL", "ME", "SK",
        "DK", "SE", "NO", "FI", "EE", "LV", "LT",
        "US", "CA",
        "AE", "IL", "EG", "JO", "LB",
        "JP", "CN", "TH", "IN", "MV", "LK",
        "ZA", "KE", "ET", "MA", "TN",
    },
    "brusselsairlines": {
        "BE", "DE", "AT", "CH", "GB", "IE", "FR", "IT", "ES", "PT", "GR", "CY", "TR",
        "NL", "LU", "PL", "CZ", "HU", "RO", "BG", "HR", "RS",
        "DK", "SE", "NO", "FI", "EE", "LV", "LT",
        "US", "CA",
        "AE", "IL", "EG",
        "SN", "CI", "CM", "CD", "CG", "RW", "BJ", "BF", "GN", "ML", "TG", "GM", "SL", "LR", "KE", "UG", "ET", "GH", "NG",
    },
    "discover": {
        "DE", "ES", "PT", "GR", "IT", "HR", "TR", "CY", "MT",
        "MX", "DO", "JM", "CU", "CR", "PA",
        "CV", "MU", "SC", "KE", "TZ", "ZA", "NA", "MW",
    },
    "elal": {
        "IL",
        "GB", "FR", "DE", "IT", "ES", "GR", "CY", "AT", "CH", "BE", "NL", "CZ", "HU", "RO", "BG", "PL",
        "US",
        "TH", "IN",
        "ZA", "KE", "ET",
    },
    "saudia": {
        "SA",
        "AE", "BH", "KW", "OM", "QA", "JO", "EG", "LB", "IQ",
        "GB", "FR", "DE", "IT", "ES", "CH", "GR", "TR",
        "US", "CA",
        "IN", "PK", "BD", "LK", "MY", "ID", "PH",
        "ET", "KE", "ZA", "NG", "SD", "DJ", "MA", "TN",
        "CN", "JP", "KR",
    },
    "omanair": {
        "OM",
        "AE", "BH", "KW", "SA", "QA", "IR",
        "GB", "FR", "DE", "IT", "CH",
        "IN", "PK", "BD", "LK", "MY", "TH", "ID", "PH",
        "EG", "JO", "TZ", "KE", "ET",
        "CN",
    },
    # ── EVA Air (EveryMundo __NEXT_DATA__ — Star Alliance, TPE hub) ──
    "evaair": {
        "TW", "JP", "KR", "CN", "HK", "MO",
        "SG", "TH", "VN", "PH", "MY", "ID", "KH", "MM",
        "IN", "BD",
        "US", "CA",
        "GB", "FR", "NL", "AT", "IT",
        "AU", "NZ",
        "AE",
    },
    # ── Rex Airlines (EveryMundo __NEXT_DATA__ — Australian domestic regional) ──
    "rex": {
        "AU",
    },
    # ── Fiji Airways (EveryMundo __NEXT_DATA__ — Pacific hub at NAN) ──
    "fijiairways": {
        "FJ", "AU", "NZ", "US",
        "SG", "HK", "JP",
        "TO", "WS", "VU", "SB", "KI", "TV", "NR",
    },
    # ── Airnorth (.NET B2C — northern Australia + Dili) ──
    "airnorth": {"AU", "TL"},
    # ── Air Niugini (EveryMundo __NEXT_DATA__ — PNG flag carrier) ──
    "airniugini": {
        "PG", "AU", "SG", "HK", "JP", "PH", "FJ", "SB", "MY",
    },
    # ── Link Airways (Playwright ASP.NET — Australian regional) ──
    "linkairways": {"AU"},
    # ── PNG Air (VARS PSS — PNG domestic) ──
    "pngair": {"PG"},
    # ── I Want That Flight (AU aggregator — domestic + international from AU) ──
    "iwantthatflight": {
        "AU", "NZ", "FJ", "NC", "PF", "PG", "VU", "WS", "TO",
        "SG", "MY", "TH", "ID", "VN", "KH", "PH", "JP", "KR", "CN", "HK", "TW", "IN",
        "AE", "QA", "US", "GB", "FR", "DE", "IT", "ES", "NL",
        "CL", "MV", "LK",
    },
    # ── CDP Chrome browser connectors (Batch 5/6/7) ──
    "airchina": {
        "CN", "HK", "MO", "TW",
        "JP", "KR", "TH", "SG", "MY", "VN", "KH", "MM", "ID", "PH", "IN", "BD", "LK", "NP", "PK",
        "US", "CA", "AU",
        "GB", "FR", "DE", "IT", "ES", "RU", "SE", "DK", "GR", "AT", "CH", "NL", "BE", "HU", "PL",
        "AE", "EG", "ET", "KE", "ZA", "BR", "AR", "MX", "PE", "CU",
    },
    "chinaeastern": {
        "CN", "HK", "MO", "TW",
        "JP", "KR", "TH", "SG", "MY", "VN", "KH", "ID", "PH", "IN", "BD", "LK", "NP", "MM",
        "US", "CA", "AU", "NZ",
        "GB", "FR", "DE", "IT", "ES", "RU", "NL", "CZ",
        "AE", "EG", "ET", "KE",
    },
    "chinasouthern": {
        "CN", "HK", "MO", "TW",
        "JP", "KR", "TH", "SG", "MY", "VN", "KH", "ID", "PH", "IN", "BD", "LK", "NP", "PK", "MM",
        "US", "CA", "AU", "NZ",
        "GB", "FR", "DE", "IT", "ES", "RU", "NL", "TR",
        "AE", "EG", "ET", "KE", "NG", "ZA",
    },
    "vietnamairlines": {
        "VN",
        "JP", "KR", "TW", "CN", "HK", "TH", "SG", "MY", "KH", "LA", "MM", "ID", "PH", "IN",
        "US", "AU",
        "GB", "FR", "DE", "RU",
    },
    "asiana": {
        "KR",
        "JP", "CN", "HK", "TW", "TH", "SG", "VN", "KH", "PH", "MY", "ID", "IN", "UZ",
        "US", "CA",
        "GB", "FR", "DE", "IT", "ES", "TR", "HR", "CZ", "RU",
        "AU",
    },
    "airtransat": {
        "CA",
        "US",
        "GB", "FR", "ES", "PT", "IT", "GR", "NL", "BE", "CH", "IE",
        "MX", "CU", "DO", "JM", "HT", "CR", "PA", "CO", "SV", "HN",
    },
    "airserbia": {
        "RS",
        "DE", "FR", "IT", "ES", "GB", "NL", "BE", "CH", "AT", "GR", "HR", "SI", "ME", "BA",
        "MK", "AL", "BG", "RO", "HU", "CZ", "PL", "SE", "NO", "DK",
        "TR", "CY", "EG", "TN",
        "US",
        "RU",
    },
    "aireuropa": {
        "ES",
        "FR", "IT", "PT", "DE", "GB", "NL", "BE", "CH", "AT", "GR", "HR", "BG", "RO",
        "IL", "MA", "TN", "SN", "GM",
        "US",
        "MX", "CU", "DO", "CO", "EC", "PE", "BR", "AR", "UY", "PY", "BO", "VE", "PA", "CR", "HN", "SV",
    },
    "mea": {
        "LB",
        "AE", "SA", "KW", "QA", "BH", "OM", "JO", "IQ", "EG", "CY",
        "GB", "FR", "DE", "IT", "ES", "BE", "CH", "GR", "DK", "SE", "TR",
        "GH", "NG", "CI", "SN", "ET",
        "US", "CA", "BR",
    },
    "hainan": {
        "CN", "HK", "MO", "TW",
        "JP", "KR", "TH", "SG", "MY", "VN", "ID", "PH", "IN",
        "US", "CA", "AU",
        "GB", "FR", "DE", "IT", "ES", "RU", "BE", "CH", "AT", "CZ", "NO", "IE",
        "AE", "EG", "IL", "ET", "KE",
        "MX",
    },
    "royaljordanian": {
        "JO",
        "AE", "SA", "KW", "QA", "BH", "OM", "IQ", "LB", "EG", "SD",
        "GB", "FR", "DE", "IT", "ES", "NL", "CH", "AT", "GR", "CY", "TR", "RO",
        "US", "CA",
        "TH", "MY", "IN", "LK", "CN", "HK",
        "NG", "ET", "KE",
    },
    "kuwaitairways": {
        "KW",
        "AE", "SA", "BH", "QA", "OM", "JO", "LB", "IQ", "EG", "IR",
        "GB", "FR", "DE", "IT", "ES", "CH", "GR", "TR", "GE",
        "US",
        "IN", "PK", "BD", "LK", "TH", "MY", "PH", "SG", "CN",
        "ET", "KE", "NG",
    },
    "level": {
        "ES",
        "FR", "IT", "PT", "DE", "GB", "NL", "AT", "GR",
        "US",
        "AR", "CL", "BR",
    },
    "qatar": {
        "QA",
        "AE", "BH", "KW", "OM", "SA", "EG", "JO", "LB", "IQ",
        "GB", "FR", "DE", "IT", "ES", "CH", "NL", "GR", "TR",
        "US", "CA",
        "IN", "PK", "BD", "LK", "TH", "MY", "SG", "ID", "PH",
        "CN", "JP", "KR", "AU", "NZ",
        "KE", "TZ", "ZA", "NG", "ET",
        "BR", "MV", "SC", "KZ", "HK",
    },
    "aircalin": {
        "NC",
        "AU", "NZ", "JP", "FJ", "VU", "WF", "PF",
    },
    "traveloka": {
        "ID", "SG", "TH", "MY", "VN", "PH", "AU", "JP", "KR", "IN",
        "HK", "TW", "CN", "MM", "KH", "LA", "BN",
    },
    "wego": {
        "AE", "SA", "QA", "KW", "BH", "OM", "JO", "EG", "LB",
        "IN", "PK", "BD", "LK", "ID", "SG", "TH", "MY", "VN", "PH",
        "AU", "NZ", "JP", "KR", "HK", "TW", "CN",
        "KE", "NG", "ZA", "TR", "GB", "DE", "FR",
    },
    "webjet": {
        "AU", "NZ", "FJ", "NC", "VU", "PG", "WS", "TO",
        "SG", "TH", "ID", "MY", "PH", "VN", "JP", "KR", "IN",
        "US", "GB", "AE", "ZA", "HK", "CN", "TW",
    },
    "tiket": {
        "ID", "SG", "TH", "MY", "VN", "PH", "AU",
        "JP", "KR", "IN", "HK", "TW", "CN",
    },
}


def get_relevant_connectors(
    origin: str,
    destination: str,
    connectors: list[tuple[str, type, float]],
) -> list[tuple[str, type, float]]:
    """Filter connectors to only those that could serve the given route.

    Returns the subset of connectors whose airlines operate in at least one
    of the origin or destination countries. If country lookup fails for
    either airport, all connectors are returned (safe fallback).

    Args:
        origin: IATA airport/city code (e.g. "CDG", "LON")
        destination: IATA airport/city code
        connectors: list of (source_name, connector_class, timeout) tuples

    Returns:
        Filtered list of connector tuples
    """
    origin_country = get_country(origin)
    dest_country = get_country(destination)

    # If we can't resolve either airport, run all connectors (safe fallback)
    if not origin_country or not dest_country:
        return connectors

    relevant = []
    for source, cls, timeout in connectors:
        # Extract airline key from source name (e.g. "easyjet_direct" → "easyjet")
        airline_key = source.replace("_direct", "").replace("_connector", "")

        # Special: Wizzair is registered as "wizzair" but keyed as "wizz"
        if airline_key == "wizzair":
            airline_key = "wizz"

        countries = AIRLINE_COUNTRIES.get(airline_key)

        if countries is None:
            # Unknown airline — always include (safe default)
            relevant.append((source, cls, timeout))
        elif origin_country in countries or dest_country in countries:
            relevant.append((source, cls, timeout))
        # else: skip — neither endpoint is in this airline's network

    return relevant
