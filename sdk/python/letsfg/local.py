"""
Local flight search — runs 75 airline connectors on the user's machine.

Can be used programmatically:

    from letsfg.local import search_local
    result = await search_local("SHA", "CTU", "2026-03-20")

Or as a subprocess (used by the npm MCP server + JS SDK):

    echo '{"origin":"SHA","destination":"CTU","date_from":"2026-03-20"}' | python -m letsfg.local
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import date

from letsfg.models.flights import FlightSearchRequest

logger = logging.getLogger(__name__)


async def search_local(
    origin: str,
    destination: str,
    date_from: str,
    *,
    return_date: str | None = None,
    adults: int = 1,
    children: int = 0,
    infants: int = 0,
    cabin_class: str | None = None,
    currency: str = "EUR",
    limit: int = 50,
    max_browsers: int | None = None,
    max_stopovers: int | None = None,
    mode: str | None = None,
) -> dict:
    """
    Run all 73 local airline connectors and return results as a dict.

    This is the core local search — no API key needed, no backend.
    Connectors run on the user's machine via Playwright + httpx.

    Args:
        max_browsers: Max concurrent browser processes (1–32).
            None = auto-detect based on system RAM.
            Lower values use less memory but search slower.
            Higher values search faster but need more RAM.
        mode: Search mode. None = full (all connectors, default).
            "fast" = OTAs/aggregators + key direct airlines (~25 connectors, 20-40s).
    """
    from letsfg.connectors.engine import multi_provider

    # Apply concurrency setting before search starts
    if max_browsers is not None:
        from letsfg.connectors.browser import configure_max_browsers
        configure_max_browsers(max_browsers)

    req = FlightSearchRequest(
        origin=origin.upper(),
        destination=destination.upper(),
        date_from=date.fromisoformat(date_from),
        return_from=date.fromisoformat(return_date) if return_date else None,
        adults=adults,
        children=children,
        infants=infants,
        cabin_class=cabin_class.upper() if cabin_class else None,
        currency=currency,
        limit=limit,
        max_stopovers=max_stopovers if max_stopovers is not None else 2,
    )

    resp = await multi_provider.search_flights(req, mode=mode)
    return resp.model_dump(mode="json")


# ── Location name → IATA code mapping (for local resolve_location) ────────
# Curated subset covering major airports & cities. If a query isn't found
# here, returns empty list — the caller should try the backend as fallback.
_LOCATION_NAMES: dict[str, list[dict]] = {}

def _build_location_index() -> dict[str, list[dict]]:
    """Build a name → IATA entries index from airline_routes data."""
    from letsfg.connectors.airline_routes import AIRPORT_COUNTRY, CITY_AIRPORTS, CITY_COUNTRY

    # Airport & city name database — maps common names to IATA codes
    _AIRPORTS: dict[str, tuple[str, str, str]] = {
        # code: (name, city_name, type)
        # Europe — GB
        "LHR": ("Heathrow Airport", "London", "airport"),
        "LGW": ("Gatwick Airport", "London", "airport"),
        "STN": ("Stansted Airport", "London", "airport"),
        "LTN": ("Luton Airport", "London", "airport"),
        "LCY": ("London City Airport", "London", "airport"),
        "SEN": ("Southend Airport", "London", "airport"),
        "MAN": ("Manchester Airport", "Manchester", "airport"),
        "EDI": ("Edinburgh Airport", "Edinburgh", "airport"),
        "BHX": ("Birmingham Airport", "Birmingham", "airport"),
        "BRS": ("Bristol Airport", "Bristol", "airport"),
        "GLA": ("Glasgow Airport", "Glasgow", "airport"),
        "NCL": ("Newcastle Airport", "Newcastle", "airport"),
        "LPL": ("Liverpool Airport", "Liverpool", "airport"),
        "BFS": ("Belfast International", "Belfast", "airport"),
        # Europe — FR
        "CDG": ("Charles de Gaulle Airport", "Paris", "airport"),
        "ORY": ("Orly Airport", "Paris", "airport"),
        "NCE": ("Nice Côte d'Azur Airport", "Nice", "airport"),
        "LYS": ("Lyon-Saint Exupéry Airport", "Lyon", "airport"),
        "MRS": ("Marseille Provence Airport", "Marseille", "airport"),
        "TLS": ("Toulouse-Blagnac Airport", "Toulouse", "airport"),
        "BOD": ("Bordeaux-Mérignac Airport", "Bordeaux", "airport"),
        "NTE": ("Nantes Atlantique Airport", "Nantes", "airport"),
        # Europe — DE
        "FRA": ("Frankfurt Airport", "Frankfurt", "airport"),
        "MUC": ("Munich Airport", "Munich", "airport"),
        "BER": ("Berlin Brandenburg Airport", "Berlin", "airport"),
        "HAM": ("Hamburg Airport", "Hamburg", "airport"),
        "CGN": ("Cologne Bonn Airport", "Cologne", "airport"),
        "DUS": ("Düsseldorf Airport", "Düsseldorf", "airport"),
        "STR": ("Stuttgart Airport", "Stuttgart", "airport"),
        "NUE": ("Nuremberg Airport", "Nuremberg", "airport"),
        "HAJ": ("Hannover Airport", "Hannover", "airport"),
        "LEJ": ("Leipzig/Halle Airport", "Leipzig", "airport"),
        # Europe — IT
        "FCO": ("Fiumicino Airport", "Rome", "airport"),
        "MXP": ("Malpensa Airport", "Milan", "airport"),
        "BGY": ("Bergamo Airport", "Milan", "airport"),
        "VCE": ("Venice Marco Polo Airport", "Venice", "airport"),
        "NAP": ("Naples Airport", "Naples", "airport"),
        "BLQ": ("Bologna Airport", "Bologna", "airport"),
        "PSA": ("Pisa Airport", "Pisa", "airport"),
        "CTA": ("Catania Airport", "Catania", "airport"),
        "PMO": ("Palermo Airport", "Palermo", "airport"),
        # Europe — ES
        "BCN": ("Barcelona-El Prat Airport", "Barcelona", "airport"),
        "MAD": ("Madrid Barajas Airport", "Madrid", "airport"),
        "PMI": ("Palma de Mallorca Airport", "Palma", "airport"),
        "AGP": ("Málaga Airport", "Málaga", "airport"),
        "ALC": ("Alicante-Elche Airport", "Alicante", "airport"),
        "VLC": ("Valencia Airport", "Valencia", "airport"),
        "SVQ": ("Seville Airport", "Seville", "airport"),
        "IBZ": ("Ibiza Airport", "Ibiza", "airport"),
        "TFS": ("Tenerife South Airport", "Tenerife", "airport"),
        "LPA": ("Gran Canaria Airport", "Las Palmas", "airport"),
        # Europe — NL/BE/CH/AT/PT/IE
        "AMS": ("Schiphol Airport", "Amsterdam", "airport"),
        "EIN": ("Eindhoven Airport", "Eindhoven", "airport"),
        "BRU": ("Brussels Airport", "Brussels", "airport"),
        "CRL": ("Charleroi Airport", "Brussels", "airport"),
        "ZRH": ("Zürich Airport", "Zürich", "airport"),
        "GVA": ("Geneva Airport", "Geneva", "airport"),
        "BSL": ("Basel Airport", "Basel", "airport"),
        "VIE": ("Vienna Airport", "Vienna", "airport"),
        "SZG": ("Salzburg Airport", "Salzburg", "airport"),
        "INN": ("Innsbruck Airport", "Innsbruck", "airport"),
        "LIS": ("Lisbon Airport", "Lisbon", "airport"),
        "OPO": ("Porto Airport", "Porto", "airport"),
        "FAO": ("Faro Airport", "Faro", "airport"),
        "DUB": ("Dublin Airport", "Dublin", "airport"),
        "SNN": ("Shannon Airport", "Shannon", "airport"),
        "ORK": ("Cork Airport", "Cork", "airport"),
        # Europe — GR/PL/CZ/HU/RO/BG/HR/Nordic
        "ATH": ("Athens International Airport", "Athens", "airport"),
        "SKG": ("Thessaloniki Airport", "Thessaloniki", "airport"),
        "HER": ("Heraklion Airport", "Heraklion", "airport"),
        "WAW": ("Warsaw Chopin Airport", "Warsaw", "airport"),
        "KRK": ("Kraków Airport", "Kraków", "airport"),
        "GDN": ("Gdańsk Lech Wałęsa Airport", "Gdańsk", "airport"),
        "WRO": ("Wrocław Airport", "Wrocław", "airport"),
        "KTW": ("Katowice Airport", "Katowice", "airport"),
        "POZ": ("Poznań Airport", "Poznań", "airport"),
        "PRG": ("Prague Airport", "Prague", "airport"),
        "BUD": ("Budapest Airport", "Budapest", "airport"),
        "OTP": ("Bucharest Henri Coandă Airport", "Bucharest", "airport"),
        "CLJ": ("Cluj-Napoca Airport", "Cluj-Napoca", "airport"),
        "SOF": ("Sofia Airport", "Sofia", "airport"),
        "BEG": ("Belgrade Nikola Tesla Airport", "Belgrade", "airport"),
        "ZAG": ("Zagreb Airport", "Zagreb", "airport"),
        "SPU": ("Split Airport", "Split", "airport"),
        "DBV": ("Dubrovnik Airport", "Dubrovnik", "airport"),
        "LJU": ("Ljubljana Airport", "Ljubljana", "airport"),
        "TIA": ("Tirana Airport", "Tirana", "airport"),
        "HEL": ("Helsinki Airport", "Helsinki", "airport"),
        "ARN": ("Stockholm Arlanda Airport", "Stockholm", "airport"),
        "GOT": ("Gothenburg Landvetter Airport", "Gothenburg", "airport"),
        "OSL": ("Oslo Gardermoen Airport", "Oslo", "airport"),
        "BGO": ("Bergen Airport", "Bergen", "airport"),
        "CPH": ("Copenhagen Airport", "Copenhagen", "airport"),
        "KEF": ("Keflavík Airport", "Reykjavik", "airport"),
        "RIX": ("Riga Airport", "Riga", "airport"),
        "VNO": ("Vilnius Airport", "Vilnius", "airport"),
        "TLL": ("Tallinn Airport", "Tallinn", "airport"),
        # Europe — TR
        "IST": ("Istanbul Airport", "Istanbul", "airport"),
        "SAW": ("Sabiha Gökçen Airport", "Istanbul", "airport"),
        "AYT": ("Antalya Airport", "Antalya", "airport"),
        "ADB": ("Izmir Adnan Menderes Airport", "Izmir", "airport"),
        "ESB": ("Ankara Esenboğa Airport", "Ankara", "airport"),
        # Middle East
        "DXB": ("Dubai International Airport", "Dubai", "airport"),
        "AUH": ("Abu Dhabi Airport", "Abu Dhabi", "airport"),
        "DOH": ("Hamad International Airport", "Doha", "airport"),
        "BAH": ("Bahrain Airport", "Bahrain", "airport"),
        "KWI": ("Kuwait Airport", "Kuwait City", "airport"),
        "MCT": ("Muscat Airport", "Muscat", "airport"),
        "RUH": ("King Khalid Airport", "Riyadh", "airport"),
        "JED": ("Jeddah Airport", "Jeddah", "airport"),
        "AMM": ("Queen Alia Airport", "Amman", "airport"),
        "BEY": ("Beirut Airport", "Beirut", "airport"),
        "TLV": ("Ben Gurion Airport", "Tel Aviv", "airport"),
        "CAI": ("Cairo International Airport", "Cairo", "airport"),
        # South Asia
        "DEL": ("Indira Gandhi Airport", "Delhi", "airport"),
        "BOM": ("Chhatrapati Shivaji Airport", "Mumbai", "airport"),
        "BLR": ("Kempegowda Airport", "Bangalore", "airport"),
        "MAA": ("Chennai Airport", "Chennai", "airport"),
        "HYD": ("Rajiv Gandhi Airport", "Hyderabad", "airport"),
        "CCU": ("Netaji Subhas Chandra Bose Airport", "Kolkata", "airport"),
        "GOI": ("Goa Airport", "Goa", "airport"),
        "COK": ("Cochin Airport", "Kochi", "airport"),
        "CMB": ("Bandaranaike Airport", "Colombo", "airport"),
        "MLE": ("Velana Airport", "Malé", "airport"),
        "KTM": ("Tribhuvan Airport", "Kathmandu", "airport"),
        "DAC": ("Hazrat Shahjalal Airport", "Dhaka", "airport"),
        "ISB": ("Islamabad Airport", "Islamabad", "airport"),
        "LHE": ("Lahore Airport", "Lahore", "airport"),
        "KHI": ("Jinnah Airport", "Karachi", "airport"),
        # Southeast Asia
        "SIN": ("Changi Airport", "Singapore", "airport"),
        "KUL": ("Kuala Lumpur International Airport", "Kuala Lumpur", "airport"),
        "PEN": ("Penang Airport", "Penang", "airport"),
        "BKK": ("Suvarnabhumi Airport", "Bangkok", "airport"),
        "DMK": ("Don Mueang Airport", "Bangkok", "airport"),
        "CNX": ("Chiang Mai Airport", "Chiang Mai", "airport"),
        "HKT": ("Phuket Airport", "Phuket", "airport"),
        "SGN": ("Tan Son Nhat Airport", "Ho Chi Minh City", "airport"),
        "HAN": ("Noi Bai Airport", "Hanoi", "airport"),
        "DAD": ("Da Nang Airport", "Da Nang", "airport"),
        "CGK": ("Soekarno-Hatta Airport", "Jakarta", "airport"),
        "DPS": ("Ngurah Rai Airport", "Bali", "airport"),
        "MNL": ("Ninoy Aquino Airport", "Manila", "airport"),
        "CEB": ("Mactan-Cebu Airport", "Cebu", "airport"),
        "RGN": ("Yangon Airport", "Yangon", "airport"),
        "PNH": ("Phnom Penh Airport", "Phnom Penh", "airport"),
        # East Asia
        "NRT": ("Narita Airport", "Tokyo", "airport"),
        "HND": ("Haneda Airport", "Tokyo", "airport"),
        "KIX": ("Kansai Airport", "Osaka", "airport"),
        "FUK": ("Fukuoka Airport", "Fukuoka", "airport"),
        "CTS": ("New Chitose Airport", "Sapporo", "airport"),
        "ICN": ("Incheon Airport", "Seoul", "airport"),
        "GMP": ("Gimpo Airport", "Seoul", "airport"),
        "CJU": ("Jeju Airport", "Jeju", "airport"),
        "PUS": ("Gimhae Airport", "Busan", "airport"),
        "PEK": ("Beijing Capital Airport", "Beijing", "airport"),
        "PVG": ("Pudong Airport", "Shanghai", "airport"),
        "CAN": ("Guangzhou Baiyun Airport", "Guangzhou", "airport"),
        "SZX": ("Shenzhen Bao'an Airport", "Shenzhen", "airport"),
        "CTU": ("Chengdu Tianfu Airport", "Chengdu", "airport"),
        "HKG": ("Hong Kong International Airport", "Hong Kong", "airport"),
        "TPE": ("Taiwan Taoyuan Airport", "Taipei", "airport"),
        # Oceania
        "SYD": ("Sydney Airport", "Sydney", "airport"),
        "MEL": ("Melbourne Airport", "Melbourne", "airport"),
        "BNE": ("Brisbane Airport", "Brisbane", "airport"),
        "PER": ("Perth Airport", "Perth", "airport"),
        "ADL": ("Adelaide Airport", "Adelaide", "airport"),
        "CNS": ("Cairns Airport", "Cairns", "airport"),
        "OOL": ("Gold Coast Airport", "Gold Coast", "airport"),
        "AKL": ("Auckland Airport", "Auckland", "airport"),
        "WLG": ("Wellington Airport", "Wellington", "airport"),
        "CHC": ("Christchurch Airport", "Christchurch", "airport"),
        # North America — US
        "JFK": ("John F. Kennedy Airport", "New York", "airport"),
        "EWR": ("Newark Liberty Airport", "New York", "airport"),
        "LGA": ("LaGuardia Airport", "New York", "airport"),
        "LAX": ("Los Angeles International Airport", "Los Angeles", "airport"),
        "ORD": ("O'Hare International Airport", "Chicago", "airport"),
        "MDW": ("Midway Airport", "Chicago", "airport"),
        "ATL": ("Hartsfield-Jackson Airport", "Atlanta", "airport"),
        "DFW": ("Dallas/Fort Worth Airport", "Dallas", "airport"),
        "DEN": ("Denver International Airport", "Denver", "airport"),
        "SFO": ("San Francisco International Airport", "San Francisco", "airport"),
        "SEA": ("Seattle-Tacoma Airport", "Seattle", "airport"),
        "MIA": ("Miami International Airport", "Miami", "airport"),
        "BOS": ("Boston Logan Airport", "Boston", "airport"),
        "IAD": ("Dulles International Airport", "Washington", "airport"),
        "DCA": ("Reagan National Airport", "Washington", "airport"),
        "PHX": ("Phoenix Sky Harbor Airport", "Phoenix", "airport"),
        "IAH": ("Houston George Bush Airport", "Houston", "airport"),
        "MCO": ("Orlando International Airport", "Orlando", "airport"),
        "MSP": ("Minneapolis-St Paul Airport", "Minneapolis", "airport"),
        "DTW": ("Detroit Metro Airport", "Detroit", "airport"),
        "FLL": ("Fort Lauderdale Airport", "Fort Lauderdale", "airport"),
        "CLT": ("Charlotte Douglas Airport", "Charlotte", "airport"),
        "LAS": ("Harry Reid Airport", "Las Vegas", "airport"),
        "SLC": ("Salt Lake City Airport", "Salt Lake City", "airport"),
        "SAN": ("San Diego Airport", "San Diego", "airport"),
        "TPA": ("Tampa International Airport", "Tampa", "airport"),
        "PDX": ("Portland International Airport", "Portland", "airport"),
        "HNL": ("Honolulu Airport", "Honolulu", "airport"),
        # North America — CA
        "YYZ": ("Toronto Pearson Airport", "Toronto", "airport"),
        "YVR": ("Vancouver International Airport", "Vancouver", "airport"),
        "YUL": ("Montréal-Trudeau Airport", "Montreal", "airport"),
        "YOW": ("Ottawa Airport", "Ottawa", "airport"),
        "YYC": ("Calgary Airport", "Calgary", "airport"),
        "YEG": ("Edmonton Airport", "Edmonton", "airport"),
        # Mexico
        "MEX": ("Mexico City Airport", "Mexico City", "airport"),
        "CUN": ("Cancún Airport", "Cancún", "airport"),
        "GDL": ("Guadalajara Airport", "Guadalajara", "airport"),
        # Caribbean / Central America
        "PTY": ("Tocumen Airport", "Panama City", "airport"),
        "SJO": ("Juan Santamaría Airport", "San José", "airport"),
        "SJU": ("Luis Muñoz Marín Airport", "San Juan", "airport"),
        # South America
        "GRU": ("Guarulhos Airport", "São Paulo", "airport"),
        "GIG": ("Galeão Airport", "Rio de Janeiro", "airport"),
        "EZE": ("Ezeiza Airport", "Buenos Aires", "airport"),
        "AEP": ("Aeroparque Jorge Newbery", "Buenos Aires", "airport"),
        "SCL": ("Santiago International Airport", "Santiago", "airport"),
        "LIM": ("Jorge Chávez Airport", "Lima", "airport"),
        "BOG": ("El Dorado Airport", "Bogotá", "airport"),
        "MDE": ("José María Córdova Airport", "Medellín", "airport"),
        # Africa
        "JNB": ("O.R. Tambo Airport", "Johannesburg", "airport"),
        "CPT": ("Cape Town International Airport", "Cape Town", "airport"),
        "NBO": ("Jomo Kenyatta Airport", "Nairobi", "airport"),
        "ADD": ("Bole International Airport", "Addis Ababa", "airport"),
        "LOS": ("Murtala Muhammed Airport", "Lagos", "airport"),
        "CMN": ("Mohammed V Airport", "Casablanca", "airport"),
        "RAK": ("Marrakech Menara Airport", "Marrakech", "airport"),
    }

    _CITIES: dict[str, tuple[str, str]] = {
        # code: (city_name, country_code)
        "LON": ("London", "GB"), "PAR": ("Paris", "FR"), "ROM": ("Rome", "IT"),
        "MIL": ("Milan", "IT"), "NYC": ("New York", "US"), "WAS": ("Washington", "US"),
        "CHI": ("Chicago", "US"), "TYO": ("Tokyo", "JP"), "OSA": ("Osaka", "JP"),
        "SEL": ("Seoul", "KR"), "BJS": ("Beijing", "CN"), "SHA": ("Shanghai", "CN"),
        "BUE": ("Buenos Aires", "AR"), "BKK": ("Bangkok", "TH"),
        "KUL": ("Kuala Lumpur", "MY"), "REK": ("Reykjavik", "IS"),
        "MOW": ("Moscow", "RU"), "STO": ("Stockholm", "SE"),
    }

    idx: dict[str, list[dict]] = {}

    def _add(key: str, entry: dict):
        key = key.lower().strip()
        if key:
            idx.setdefault(key, []).append(entry)

    # Index airports
    for code, (name, city, _type) in _AIRPORTS.items():
        country = AIRPORT_COUNTRY.get(code, "")
        entry = {"iata_code": code, "name": name, "type": "airport", "city": city, "country": country}
        _add(code.lower(), entry)
        _add(name.lower(), entry)
        _add(city.lower(), entry)
        # Also index without diacritics for common queries
        for part in [name, city]:
            simplified = part.lower().replace("ü", "u").replace("ö", "o").replace("ä", "a").replace("é", "e").replace("è", "e").replace("ñ", "n").replace("ø", "o").replace("å", "a").replace("ł", "l").replace("ę", "e").replace("ą", "a").replace("ś", "s").replace("ć", "c").replace("ż", "z").replace("ź", "z").replace("ó", "o").replace("ń", "n").replace("ô", "o").replace("â", "a").replace("î", "i").replace("ã", "a").replace("í", "i").replace("ú", "u").replace("á", "a").replace("ò", "o").replace("ù", "u").replace("ç", "c").replace("ă", "a").replace("ş", "s").replace("ğ", "g").replace("ı", "i").replace("ð", "d").replace("þ", "th")
            if simplified != part.lower():
                _add(simplified, entry)

    # Index city codes
    for code, (city_name, country) in _CITIES.items():
        airports = CITY_AIRPORTS.get(code, [])
        entry = {"iata_code": code, "name": city_name, "type": "city", "country": country, "airports": airports}
        _add(code.lower(), entry)
        _add(city_name.lower(), entry)

    return idx


def _resolve_location_local(query: str) -> list[dict]:
    """Resolve a city/airport name to IATA codes using local data."""
    global _LOCATION_NAMES
    if not _LOCATION_NAMES:
        _LOCATION_NAMES = _build_location_index()

    q = query.lower().strip()
    if not q:
        return []

    # Exact match first
    results = _LOCATION_NAMES.get(q)
    if results:
        # Deduplicate by iata_code, prefer city entries first
        seen = set()
        out = []
        # Put city-type entries first
        for r in sorted(results, key=lambda x: (0 if x["type"] == "city" else 1, x["iata_code"])):
            if r["iata_code"] not in seen:
                seen.add(r["iata_code"])
                out.append(r)
        return out

    # Prefix match as fallback
    matches = []
    for key, entries in _LOCATION_NAMES.items():
        if key.startswith(q) or q in key:
            matches.extend(entries)

    # Deduplicate
    seen = set()
    out = []
    for r in sorted(matches, key=lambda x: (0 if x["type"] == "city" else 1, x["iata_code"])):
        if r["iata_code"] not in seen:
            seen.add(r["iata_code"])
            out.append(r)
    return out[:20]


def _run_checkout_local(params: dict) -> dict:
    """Run checkout locally via the checkout engine."""
    import asyncio

    offer = params.get("offer") or {}
    offer_id = params.get("offer_id", "")
    passengers = params.get("passengers")
    checkout_token = params.get("checkout_token", "")
    api_key = params.get("api_key", "")
    base_url = params.get("base_url", "https://api.letsfg.co")

    # If we only have offer_id but no full offer, return URL-only
    if not offer and not offer_id:
        return {"status": "error", "message": "offer_id or offer required"}

    try:
        from letsfg.client import LetsFG
        bt = LetsFG(api_key=api_key, base_url=base_url)
        result = bt.start_checkout_local(
            offer=offer if offer else {"id": offer_id},
            passengers=passengers,
            checkout_token=checkout_token,
        )
        return result.__dict__ if hasattr(result, "__dict__") else {"status": "error", "message": str(result)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _main() -> None:
    """Entry point for subprocess invocation: reads JSON from stdin, writes JSON to stdout."""
    import os
    import warnings

    # Suppress asyncio transport cleanup noise (Python 3.13+)
    warnings.filterwarnings("ignore", category=ResourceWarning, message=".*unclosed transport.*")
    _orig_unraisable = sys.unraisablehook
    def _quiet_unraisable(hook_args):
        try:
            if hook_args.exc_type is ValueError and "pipe" in str(hook_args.exc_value).lower():
                return
            if "transport" in str(getattr(hook_args, "object", "")):
                return
        except Exception:
            return
        _orig_unraisable(hook_args)
    sys.unraisablehook = _quiet_unraisable

    # Suppress Node.js DEP0169 warnings from Playwright subprocesses
    os.environ.setdefault("NODE_OPTIONS", "--no-deprecation")

    raw = sys.stdin.read().strip()
    if not raw:
        json.dump({"error": "No input provided. Send JSON on stdin."}, sys.stdout)
        sys.exit(1)

    try:
        params = json.loads(raw)
    except json.JSONDecodeError as e:
        json.dump({"error": f"Invalid JSON: {e}"}, sys.stdout)
        sys.exit(1)

    # System info query (used by MCP server's system_info tool)
    if params.get("__system_info"):
        from letsfg.system_info import get_system_profile
        from letsfg.connectors.browser import get_max_browsers
        profile = get_system_profile()
        profile["current_max_browsers"] = get_max_browsers()
        json.dump(profile, sys.stdout)
        return

    # Location resolution (used by MCP server's resolve_location tool)
    if params.get("__resolve_location"):
        json.dump(_resolve_location_local(params.get("query", "")), sys.stdout)
        return

    # Checkout (used by MCP server's start_checkout tool)
    if params.get("__checkout"):
        result = _run_checkout_local(params)
        json.dump(result, sys.stdout)
        return

    # Suppress noisy logs — only errors to stderr
    logging.basicConfig(
        level=logging.WARNING,
        format="%(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    async def _run():
        asyncio.get_event_loop().set_exception_handler(lambda loop, ctx: None)
        return await search_local(
            origin=params["origin"],
            destination=params["destination"],
            date_from=params["date_from"],
            return_date=params.get("return_date") or params.get("return_from"),
            adults=params.get("adults", 1),
            children=params.get("children", 0),
            infants=params.get("infants", 0),
            cabin_class=params.get("cabin_class"),
            currency=params.get("currency", "EUR"),
            limit=params.get("limit", 50),
            max_browsers=params.get("max_browsers"),
            mode=params.get("mode"),
        )

    try:
        result = asyncio.run(_run())
        json.dump(result, sys.stdout)
    except Exception as e:
        json.dump({"error": str(e)}, sys.stdout)
        sys.exit(1)


if __name__ == "__main__":
    _main()
