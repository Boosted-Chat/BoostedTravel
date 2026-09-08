"""
LetsFG CLI — Agent-native flight search & booking from terminal.

Usage (search — free, no API key):
    letsfg search GDN BER 2026-03-03
    letsfg search LON BCN 2026-04-01 --return 2026-04-08 --sort price

Usage (booking — requires API key):
    letsfg book off_xxx --passenger '{"id":"pas_xxx","given_name":"John",...}'
    letsfg register --name my-agent --email agent@example.com
    letsfg me
    letsfg locations London
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from typing import Optional

try:
    import typer
    from rich.console import Console
    from rich.table import Table
    from rich import print as rprint
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

from letsfg.client import LetsFG, LetsFGError, AuthenticationError


def _fallback_convert(amount: float, from_cur: str, to_cur: str) -> float:
    return float(amount)


app = typer.Typer(
    name="letsfg",
    help=(
        "LetsFG — Agent-native flight search & booking.\n\n"
        "Search hundreds of airlines at raw airline prices via the LetsFG cloud.\n"
        "Authenticate once with `letsfg auth` (zero-amount card setup, nothing\n"
        "charged), then search and book.\n\n"
        "Quick start: letsfg auth && letsfg search GDN BCN 2026-06-15\n"
        "Round trip:  letsfg search LON BCN 2026-04-01 --return 2026-04-08"
    ),
    no_args_is_help=True,
)
console = Console() if HAS_RICH else None


def _get_client(api_key: str | None = None, base_url: str | None = None) -> LetsFG:
    """Get a LetsFG client. Key resolution: explicit arg > LETSFG_API_KEY env > saved config."""
    url = base_url or os.environ.get("LETSFG_BASE_URL")
    bt = LetsFG(api_key=api_key, base_url=url, client_type="cli")
    return bt


def _handle_auth_error(e: LetsFGError) -> None:
    """Print helpful message when API key is invalid."""
    from letsfg.client import _saved_api_key

    env_key = os.environ.get("LETSFG_API_KEY")
    config_key = _saved_api_key()

    msg = str(e.message)
    if env_key and config_key and env_key != config_key:
        msg += ("\n\nNote: You have LETSFG_API_KEY set in your environment, but a different key\n"
                "      is saved in your config file. This may be a stale env var.\n"
                "      Try: unset LETSFG_API_KEY (or $env:LETSFG_API_KEY = '' on PowerShell)")
    elif env_key:
        msg += ("\n\nNote: You have LETSFG_API_KEY set in your environment.\n"
                "      If you recently re-registered, this may be a stale key.\n"
                "      Try: unset LETSFG_API_KEY")
    else:
        msg += "\n\nTry: letsfg register --name my-agent --email you@example.com"

    _err(msg)


def _err(msg: str):
    """Print an error and STOP.

    This used to only print, and 18 of its call sites relied on it stopping.
    They fell through into code that assumed the call had succeeded, so the
    FIRST thing a new user ever ran -- `letsfg search` before `letsfg auth` --
    printed the correct "run letsfg auth" message and then died on
    `result.get(...)` with an UnboundLocalError traceback (result was never
    assigned). Same for every other failure of that command: expired token,
    network error, 500.

    Fixed in the helper rather than at each call site on purpose: local.py's
    own comment records issue #163 coming straight back because a call site
    was patched instead of the helper it should have gone in.
    """
    if HAS_RICH:
        console.print(f"[red]Error:[/red] {msg}")
    else:
        print(f"Error: {msg}", file=sys.stderr)
    raise typer.Exit(1)


def _warn_developer_api_command(cmd: str):
    """Loud, non-fatal notice on the paid Developer API commands.

    Agents kept reaching for these because our own docs pointed here by default,
    quietly creating billing accounts nobody wanted. Searching and booking need
    `letsfg auth` and nothing else.
    """
    msg = (
        f"'{cmd}' is for the PAID Developer API and creates a billing account.\n"
        "  If you just want to search or book flights, press Ctrl+C and run: letsfg auth\n"
        "  (one step, no billing account, nothing charged)"
    )
    if HAS_RICH:
        console.print(f"[yellow]Note:[/yellow] {msg}")
    else:
        print(f"Note: {msg}", file=sys.stderr)
    raise typer.Exit(1)


def _json_out(data):
    """Print JSON output for machine consumption."""
    print(json.dumps(data, indent=2, default=str))


def _resolve_locations_with_local_fallback(bt, query: str) -> list[dict]:
    """Resolve locations via API."""
    try:
        result = bt.resolve_location(query)
        if result:
            return result
    except Exception:
        pass
    return []


# ── Airline display helpers ───────────────────────────────────────────────

_IATA_TO_AIRLINE: dict[str, str] = {
    # Alternative Airlines
    "W2": "FlexFlight",
    # Middle East / Arabian Peninsula
    "EK": "Emirates", "EY": "Etihad Airways", "QR": "Qatar Airways",
    "FZ": "flydubai", "G9": "Air Arabia", "XY": "flynas", "F3": "flyadeal",
    "WY": "Oman Air", "OV": "SalamAir", "RJ": "Royal Jordanian",
    "KU": "Kuwait Airways", "ME": "Middle East Airlines", "SV": "Saudia",
    # Europe – full-service
    "BA": "British Airways", "AF": "Air France", "LH": "Lufthansa",
    "KL": "KLM", "LX": "Swiss", "OS": "Austrian Airlines",
    "SN": "Brussels Airlines", "AY": "Finnair", "SK": "SAS",
    "TP": "TAP Air Portugal", "IB": "Iberia", "LY": "El Al",
    "TK": "Turkish Airlines", "AT": "Royal Air Maroc", "A3": "Aegean Airlines",
    "OA": "Olympic Air", "GQ": "SKY express", "EI": "Aer Lingus",
    "JU": "Air Serbia", "BT": "airBaltic", "QS": "Smartwings",
    "S4": "Azores Airlines", "CY": "Cyprus Airways", "VS": "Virgin Atlantic",
    "DE": "Condor", "4Y": "Discover Airlines", "J2": "Azerbaijan Airlines",
    # Europe – LCC
    "FR": "Ryanair", "W6": "Wizz Air", "W4": "Wizz Air Malta", "U2": "easyJet", "VY": "Vueling",
    "EW": "Eurowings", "HV": "Transavia", "PC": "Pegasus", "DY": "Norwegian",
    "I2": "Iberia Express", "V7": "Volotea", "LS": "Jet2", "FI": "Icelandair",
    "XQ": "SunExpress",
    # North America
    "AA": "American Airlines", "DL": "Delta Air Lines", "UA": "United Airlines",
    "WN": "Southwest Airlines", "AS": "Alaska Airlines", "B6": "JetBlue Airways",
    "HA": "Hawaiian Airlines", "F9": "Frontier Airlines",
    "G4": "Allegiant", "XP": "Avelo Airlines", "MX": "Breeze Airways",
    "SY": "Sun Country Airlines",
    # Canada
    "AC": "Air Canada", "WS": "WestJet", "PD": "Porter Airlines",
    "F8": "Flair Airlines", "TS": "Air Transat",
    # Latin America
    "LA": "LATAM Airlines", "AV": "Avianca", "CM": "Copa Airlines",
    "G3": "GOL", "AD": "Azul", "AR": "Aerolíneas Argentinas",
    "VB": "VivaAerobus", "Y4": "Volaris", "DM": "Arajet", "H2": "Sky Airline",
    "P5": "Wingo", "JA": "JetSMART", "FO": "Flybondi",
    # Africa
    "SA": "South African Airways", "FA": "FlySafair", "4Z": "Airlink", "5Z": "CemAir", "GE": "LIFT",
    "ET": "Ethiopian Airlines", "KQ": "Kenya Airways",
    "WB": "RwandAir", "P4": "Air Peace",
    # Asia – full-service
    "SQ": "Singapore Airlines", "CX": "Cathay Pacific", "NH": "ANA",
    "JL": "Japan Airlines", "KE": "Korean Air", "OZ": "Asiana Airlines",
    "MH": "Malaysia Airlines", "TG": "Thai Airways", "GA": "Garuda Indonesia",
    "AI": "Air India", "PK": "PIA", "UL": "SriLankan Airlines",
    "VN": "Vietnam Airlines", "PR": "Philippine Airlines",
    "CA": "Air China", "MU": "China Eastern Airlines",
    "CZ": "China Southern Airlines", "CI": "China Airlines",
    "ZH": "Shenzhen Airlines",
    "HU": "Hainan Airlines", "BR": "EVA Air", "JX": "Starlux Airlines",
    "UX": "Air Europa",
    # Asia – LCC
    "AK": "AirAsia", "FD": "Thai AirAsia", "VJ": "VietJet Air",
    "TR": "Scoot", "MM": "Peach Aviation", "ZG": "ZIPAIR",
    "7C": "Jeju Air", "TW": "T'way Air", "QG": "Citilink",
    "OD": "Batik Air", "IU": "Super Air Jet", "8B": "TransNusa",
    "QP": "Akasa Air", "IX": "Air India Express", "6E": "IndiGo",
    "SG": "SpiceJet", "PG": "Bangkok Airways", "5J": "Cebu Pacific",
    "DD": "Nok Air", "8L": "Lucky Air", "9C": "Spring Airlines", "AQ": "9 Air",
    # Pacific / Oceania
    "QF": "Qantas", "VA": "Virgin Australia", "ZL": "Rex Airlines",
    "NZ": "Air New Zealand", "FJ": "Fiji Airways", "PX": "Air Niugini",
    "TL": "Airnorth", "PH": "Samoa Airways", "CG": "PNG Air",
    "IE": "Solomon Airlines", "JQ": "Jetstar",
    # South / Southeast Asia
    "BS": "US-Bangla Airlines", "BG": "Biman Bangladesh Airlines",
    # Indian Ocean / Pacific islands
    "SB": "Aircalin", "TN": "Air Tahiti Nui", "NF": "Air Vanuatu",
    "HM": "Air Seychelles", "MK": "Air Mauritius", "GL": "Air Greenland",
    # Caribbean
    "BW": "Caribbean Airlines",
    # Central Asia
    "FS": "FlyArystan",
    # Eastern Europe / Other
    "LO": "LOT Polish Airlines", "AZ": "ITA Airways",
}
_AIRLINE_TO_IATA: dict[str, str] = {v.lower(): k for k, v in _IATA_TO_AIRLINE.items()}


def _fmt_airline(owner: str, airlines: list[str]) -> str:
    """Return 'CODE-FullName' for the Airline display column."""
    if not owner:
        owner = next((a for a in airlines if a), "")
    if not owner:
        return "-"

    # Combo offer — e.g. "Ryanair|Wizz Air" produced by combo_engine
    if "|" in owner:
        parts = [p.strip() for p in owner.split("|") if p.strip()]
        return " + ".join(_fmt_airline(p, []) for p in parts)

    # Pure IATA code (2–3 uppercase letters/digits)
    if re.fullmatch(r"[A-Z0-9]{2,3}", owner):
        code = owner
        primary_name = _IATA_TO_AIRLINE.get(code)
        
        if not primary_name:
            # Fall back to the first entry in the airlines list that differs from the code
            name = next((a for a in airlines if a and a.upper() != code), None)
            # Check if fallback is itself a IATA code
            if name and re.fullmatch(r"[A-Z0-9]{2,3}", name):
                name_mapped = _IATA_TO_AIRLINE.get(name)
                if name_mapped:
                    return f"{code}-{name_mapped}"
            return f"{code}-{name}" if name else code
        
        # primary_name exists for this code
        # Check if airlines list has an entry that's a IATA code we can also map
        secondary = next((a for a in airlines if a and a.upper() != code), None)
        if secondary and re.fullmatch(r"[A-Z0-9]{2,3}", secondary):
            secondary_mapped = _IATA_TO_AIRLINE.get(secondary)
            if secondary_mapped:
                return f"{code}-{primary_name} + {secondary}-{secondary_mapped}"
        
        return f"{code}-{primary_name}"

    # Full airline name — attempt reverse lookup for its IATA code
    code = _AIRLINE_TO_IATA.get(owner.lower())
    return f"{code}-{owner}" if code else owner


def _offer_price(offer: dict) -> float:
    """Extract comparable offer price; missing/invalid values sort last."""
    try:
        return float(offer.get("price", float("inf")))
    except (TypeError, ValueError):
        return float("inf")


def _offer_duration_seconds(offer: dict) -> int:
    """Extract comparable offer duration; missing/invalid values sort last."""
    raw = offer.get("duration_seconds")
    if raw is None:
        raw = (offer.get("outbound") or {}).get("total_duration_seconds")
    try:
        return int(raw) if raw is not None else int(1e18)
    except (TypeError, ValueError):
        return int(1e18)


def _final_sort_offers(offers: list[dict], sort: str) -> None:
    """Apply deterministic client-side sorting after merged results are fetched."""
    if sort == "duration":
        offers.sort(key=lambda o: (_offer_duration_seconds(o), _offer_price(o)))
        return
    offers.sort(key=lambda o: (_offer_price(o), _offer_duration_seconds(o)))


def _format_leg_time(leg: dict, pos: str = "dep", include_day_offset: bool = False) -> str:
    """Format a leg timestamp as HH:MM, optionally appending +n for arrival day offsets."""
    if not leg:
        return "-"

    segs = leg.get("segments") or []
    if not segs:
        return "-"

    if pos == "dep":
        dt_str = segs[0].get("departure", "")
    else:
        dt_str = segs[-1].get("arrival", "")

    if not dt_str:
        return "-"

    try:
        time_part = dt_str.split("T")[1][:5] if "T" in dt_str else dt_str[:5]
    except (IndexError, TypeError):
        return "-"

    if pos != "arr" or not include_day_offset:
        return time_part

    dep_str = segs[0].get("departure", "")
    if not dep_str or "T" not in dep_str or "T" not in dt_str:
        return time_part

    try:
        from datetime import datetime

        dep_date = datetime.strptime(dep_str.split("T")[0], "%Y-%m-%d").date()
        arr_date = datetime.strptime(dt_str.split("T")[0], "%Y-%m-%d").date()
        day_diff = (arr_date - dep_date).days
        return f"{time_part}+{day_diff}" if day_diff > 0 else time_part
    except (ValueError, IndexError, TypeError):
        return time_part


def _convert_display_price(amount: float, from_cur: str, to_cur: str, eur_rates: dict[str, float]) -> tuple[float, str]:
    """Convert display price when possible; preserve the original currency if conversion fails."""
    try:
        numeric_amount = float(amount)
    except (TypeError, ValueError):
        return amount, (from_cur or to_cur or "").upper()

    from_cur = (from_cur or "").upper()
    to_cur = (to_cur or "").upper()

    if not from_cur:
        return numeric_amount, to_cur
    if not to_cur or from_cur == to_cur:
        return numeric_amount, from_cur

    if eur_rates:
        from_rate = eur_rates.get(from_cur)
        to_rate = eur_rates.get(to_cur)
        if from_rate and to_rate:
            return round((numeric_amount / from_rate) * to_rate, 2), to_cur

    converted = round(_fallback_convert(numeric_amount, from_cur, to_cur), 2)
    if converted == round(numeric_amount, 2):
        return numeric_amount, from_cur

    return converted, to_cur


# ── Search ────────────────────────────────────────────────────────────────

@app.command()
def search(
    origin: str = typer.Argument(..., help="Departure IATA code (e.g., GDN, LON, JFK)"),
    destination: str = typer.Argument(..., help="Arrival IATA code (e.g., BER, BCN, LAX)"),
    date: str = typer.Argument(..., help="Departure date YYYY-MM-DD"),
    return_date: Optional[str] = typer.Option(None, "--return", "-r", help="Return date for round-trip"),
    adults: int = typer.Option(1, "--adults", "-a", help="Number of adults"),
    children: int = typer.Option(0, "--children", help="Number of children"),
    cabin: Optional[str] = typer.Option(None, "--cabin", "-c", help="M=economy W=premium C=business F=first"),
    currency: str = typer.Option("EUR", "--currency", help="Currency code"),
    limit: int = typer.Option(20, "--limit", "-l", help="Max results"),
    sort: str = typer.Option("price", "--sort", help="Sort: price or duration"),
    max_stops: Optional[int] = typer.Option(None, "--max-stops", "-s", help="Max stopovers (0=direct only, 1, 2). Default: no filter"),
    direct: bool = typer.Option(False, "--direct", "-d", help="Direct flights only (shortcut for --max-stops 0)"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
):
    """Search for flights — FREE. Requires auth (run `letsfg auth` once; nothing charged)."""
    from letsfg.local import search_local
    from letsfg.connectors.auth import BearerTokenError

    try:
        max_stopovers = 0 if direct else max_stops
        result = asyncio.run(search_local(
            origin=origin,
            destination=destination,
            date_from=date,
            return_date=return_date,
            adults=adults,
            children=children,
            cabin_class=cabin,
            currency=currency,
            limit=limit,
            max_stopovers=max_stopovers,
            sort=sort,
        ))
    except BearerTokenError as e:
        _err(str(e))
    except Exception as e:
        _err(f"Search failed: {e}")

    offers = result.get("offers", [])
    total = result.get("total_results", len(offers))
    search_id = result.get("search_id", "")

    # Apply a final client-side sort after cloud results are fetched.
    _final_sort_offers(offers, sort)

    offers = offers[:limit]

    if output_json:
        _json_out({"search_id": search_id, "total_results": total, "offers": offers})
        return

    if not offers:
        print(f"No flights found for {origin} → {destination} on {date}")
        return

    has_return = any(o.get("inbound") for o in offers)
    trip_label = f"{origin} ↔ {destination}" if return_date else f"{origin} → {destination}"
    date_label = f"{date} → {return_date}" if return_date else date
    print(f"\n  {total} offers  |  {trip_label}  |  {date_label}")
    if search_id:
        print(f"  search_id: {search_id}  (needed for `letsfg book`, offers expire ~15 min after search)")

    def _route_str(leg):
        if not leg:
            return "-"
        route = leg.get("route_str", "")
        if not route:
            segs = leg.get("segments", [])
            if segs:
                codes = [segs[0].get("origin", "")]
                for s in segs:
                    codes.append(s.get("destination", ""))
                route = "→".join(c for c in codes if c)
        return route or "-"

    def _dur_str(leg):
        if not leg:
            return "-"
        dur_s = leg.get("total_duration_seconds")
        if dur_s:
            h, m = divmod(dur_s // 60, 60)
            return f"{h}h {m:02d}m"
        return "-"

    def _time_str(leg, pos="dep"):
        return _format_leg_time(leg, pos=pos, include_day_offset=(pos == "arr"))

    target_currency = currency.upper()
    eur_rates = {}

    if HAS_RICH:
        table = Table(show_header=True, header_style="bold")
        table.add_column("#", style="dim", width=4)
        table.add_column("Price", justify="right", style="green")
        table.add_column("Airline")
        table.add_column("Outbound")
        table.add_column("Depart", justify="right")
        table.add_column("Arrive", justify="right")
        table.add_column("Dur", justify="right")
        table.add_column("Stops", justify="center")
        if has_return:
            table.add_column("Return")
            table.add_column("Dur", justify="right")

        for i, o in enumerate(offers, 1):
            ob = o.get("outbound", {})
            ib = o.get("inbound")
            airlines = _fmt_airline(o.get("owner_airline", ""), o.get("airlines", []))
            stops = str(ob.get("stopovers", 0))
            raw_price = o.get("price", 0)
            raw_currency = (o.get("currency", currency) or currency).upper()
            price, cur = _convert_display_price(raw_price, raw_currency, target_currency, eur_rates)
            row = [str(i), f"{cur} {price:.2f}", airlines, _route_str(ob), _time_str(ob, "dep"), _time_str(ob, "arr"), _dur_str(ob), stops]
            if has_return:
                row.append(_route_str(ib))
                row.append(_dur_str(ib))
            table.add_row(*row)
        console.print(table)

        # Print offer IDs below the table for use with `letsfg book`
        print("\n  Offer IDs (use with `letsfg book`):")
        for i, o in enumerate(offers, 1):
            offer_id = o.get("id", "")
            raw_price = o.get("price", 0)
            raw_currency = (o.get("currency", currency) or currency).upper()
            price, cur = _convert_display_price(raw_price, raw_currency, target_currency, eur_rates)
            id_str = f"  [{offer_id}]" if offer_id else ""
            unlock = o.get("unlock_url", "")
            pt = o.get("payment_token", "")
            print(f"  {i:3d}. {cur} {price:.2f} {airlines}{id_str}")
            if unlock:
                print(f"       Unlock: {unlock}")
                if pt:
                    print(f"       Poll after payment: GET https://letsfg.co/api/developers/payment-verify?token={pt}")
    else:
        for i, o in enumerate(offers, 1):
            raw_price = o.get("price", 0)
            raw_currency = (o.get("currency", currency) or currency).upper()
            price, cur = _convert_display_price(raw_price, raw_currency, target_currency, eur_rates)
            airlines = _fmt_airline(o.get("owner_airline", ""), o.get("airlines", []))
            ob = o.get("outbound", {})
            ib = o.get("inbound")
            dep = _time_str(ob, "dep")
            arr = _time_str(ob, "arr")
            ret = f"  ret: {_route_str(ib)}" if ib else ""
            offer_id = o.get("id", "")
            id_str = f"  [{offer_id}]" if offer_id else ""
            print(f"  {i:3d}. {cur} {price:.2f}  {airlines}  {_route_str(ob)} {dep}→{arr}{ret}{id_str}")
            unlock = o.get("unlock_url", "")
            pt = o.get("payment_token", "")
            if unlock:
                print(f"       Unlock: {unlock}")
                if pt:
                    print(f"       Poll after payment: GET https://letsfg.co/api/developers/payment-verify?token={pt}")

    print()


@app.command()
def auth(
    no_browser: bool = typer.Option(
        False, "--no-browser", help="Print the connect URL instead of opening a browser"
    ),
):
    """Connect a card at letsfg.co/connect. Nothing is charged.

    Registers this client (OAuth 2.1 + PKCE, loopback redirect), opens the card
    screen, and stores the token in ~/.letsfg/config.json. A PERSON approves it
    once in a browser — there is no endpoint that mints a token from card
    details, so never ask a user for a card number.

    You pay the fare only when you book, and it is held, not taken, until the
    airline confirms. The access token lasts about an hour and refreshes itself
    from the stored refresh token.

    Unrelated to `letsfg register` / `letsfg connect-payment`, which belong to the
    separate paid Developer API.
    """
    from letsfg.connectors.auth import connect_auth, BearerTokenError
    try:
        connect_auth(open_browser=not no_browser)
        print("\n  You're all set. Run: letsfg search WAW BCN 2026-07-15\n")
    except BearerTokenError as e:
        _err(str(e))
    except Exception as e:
        _err(f"Auth failed: {e}")


# ── Unlock ────────────────────────────────────────────────────────────────

@app.command()
def unlock(
    offer_id: str = typer.Argument(..., help="Offer ID from search results"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """RETIRED 2026-09-08. There is no unlock step - book directly.

    Kept as a command so an older script gets one clear sentence instead of "no such command".
    """
    _err(
        "letsfg unlock was retired on 2026-09-08 and the endpoint answers 410 Gone.\n\n"
        "There is no unlock step any more. Book directly:\n"
        "  letsfg book <offer_id> --search-id <search_id> --passenger '{...}' --email you@example.com\n\n"
        "The fare is HELD on your connected payment method and captured only once a real airline "
        "PNR exists, which is what unlock existed to protect against. If the fare moves at "
        "checkout you are asked to accept or decline it."
    )


# ── Book ──────────────────────────────────────────────────────────────────

@app.command()
def book(
    offer_id: str = typer.Argument(..., help="Offer ID from `letsfg search`"),
    search_id: Optional[str] = typer.Option(None, "--search-id", help="search_id from `letsfg search` (required unless --api-key is set, i.e. the paid Developer API flow)"),
    passenger: list[str] = typer.Option(..., "--passenger", "-p", help='JSON passenger object: \'{"given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","phone_number":"+15551234567"}\''),
    email: str = typer.Option(..., "--email", "-e", help="Contact email"),
    phone: str = typer.Option("", "--phone", help="Contact phone (used if not already in the passenger JSON)"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Book a flight from a `letsfg search`. Free — requires `letsfg auth`, nothing charged beyond the ticket price.

    Returns either a confirmed order or a direct booking link for that exact offer.
    Pass --api-key for the separate, paid Developer API flow (requires `letsfg unlock` first).
    """
    from letsfg.client import _saved_api_key

    using_developer_api = bool(api_key or os.environ.get("LETSFG_API_KEY") or _saved_api_key())

    if not using_developer_api:
        from letsfg.local import book_offer
        from letsfg.connectors.auth import BearerTokenError

        if not search_id:
            _err("--search-id is required (from your `letsfg search` results). Pass --api-key instead for the paid Developer API flow.")
            raise typer.Exit(1)

        try:
            p = json.loads(passenger[0])
        except json.JSONDecodeError:
            _err(f"Invalid JSON for passenger: {passenger[0]}")
            raise typer.Exit(1)
        if phone:
            p.setdefault("phone_number", phone)

        try:
            result = asyncio.run(book_offer(
                search_id=search_id,
                offer_id=offer_id,
                passenger=p,
                contact_email=email,
            ))
        except BearerTokenError as e:
            _err(str(e))
            raise typer.Exit(1)
        except Exception as e:
            _err(f"Booking failed: {e}")
            raise typer.Exit(1)

        if output_json:
            _json_out(result)
            return

        if result.get("booked"):
            print(f"\n  ✓ Booking confirmed!")
            print(f"    Order ID: {result.get('order_id')}")
            print(f"    Charged: {result.get('charged', 0)} {result.get('currency', '')}\n")
        else:
            print(f"\n  Could not complete a confirmed booking. Nothing was charged.")
            print(f"    Booking link: {result.get('booking_url', '(none)')}\n")
        return

    bt = _get_client(api_key, base_url)

    passengers = []
    for p_str in passenger:
        try:
            passengers.append(json.loads(p_str))
        except json.JSONDecodeError:
            _err(f"Invalid JSON for passenger: {p_str}")

    try:
        result = bt.book(
            offer_id=offer_id,
            passengers=passengers,
            contact_email=email,
            contact_phone=phone,
        )
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out({
            "booking_id": result.booking_id,
            "status": result.status,
            "booking_reference": result.booking_reference,
            "flight_price": result.flight_price,
            "service_fee": result.service_fee,
            "total_charged": result.total_charged,
            "currency": result.currency,
            "order_id": result.order_id,
        })
        return

    if result.is_confirmed:
        print(f"\n  ✓ Booking confirmed!")
        print(f"    PNR: {result.booking_reference}")
        print(f"    Flight price: {result.currency} {result.flight_price:.2f}")
        print(f"    Service fee: {result.currency} {result.service_fee:.2f} ({result.service_fee_percentage}%)")
        print(f"    Total: {result.currency} {result.total_charged:.2f}")
        print(f"    Order ID: {result.order_id}\n")
    else:
        _err(f"Booking failed: {result.details}")


# ── Locations ─────────────────────────────────────────────────────────────

@app.command()
def locations(
    query: str = typer.Argument(..., help="City or airport name to resolve"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Resolve city/airport name to IATA codes."""
    bt = _get_client(api_key, base_url)
    try:
        result = _resolve_locations_with_local_fallback(bt, query)
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    if not result:
        print(f"No locations found for '{query}'")
        return

    for loc in result:
        iata = loc.get("iata_code", loc.get("iata", "???"))
        name = loc.get("name", "")
        ltype = loc.get("type", "")
        city = loc.get("city_name", loc.get("city", ""))
        country = loc.get("country", "")
        print(f"  {iata:5s}  {name} ({ltype}) — {city}, {country}")


# ── Register ──────────────────────────────────────────────────────────────

@app.command()
def register(
    name: str = typer.Option(..., "--name", "-n", help="Agent name"),
    email: str = typer.Option(..., "--email", "-e", help="Contact email"),
    owner: str = typer.Option("", "--owner", help="Owner name"),
    description: str = typer.Option("", "--desc", help="Agent description"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """[Developer API only] Create a PAID developer account with its own billing.

    Most agents do NOT need this. To search and book flights, run `letsfg auth`
    instead — it takes one step and creates no billing account. This command
    exists for high-volume commercial integrations on the prepaid Developer API.
    """
    _warn_developer_api_command("letsfg register")
    try:
        result = LetsFG.register(
            agent_name=name,
            email=email,
            base_url=base_url,
            owner_name=owner,
            description=description,
        )
    except LetsFGError as e:
        _err(f"{e.message}")

    new_key = result.get("api_key", "")

    # Save the key to config file so it persists
    from letsfg.client import _save_config
    _save_config({
        "api_key": new_key,
        "agent_id": result.get("agent_id", ""),
        "auto_registered": False,
    })

    if output_json:
        _json_out(result)
        return

    print(f"\n  ✓ Agent registered!")
    print(f"    Agent ID: {result.get('agent_id')}")
    print(f"    API Key:  {new_key}")
    print(f"\n    Key saved to config.")

    # Warn if there's an old env var that will override the new key
    env_key = os.environ.get("LETSFG_API_KEY")
    if env_key and env_key != new_key:
        print(f"\n  ⚠️  WARNING: You have an old API key in your environment variable.")
        print(f"     The CLI will use the OLD key unless you clear it:")
        print(f"     PowerShell:  $env:LETSFG_API_KEY = ''")
        print(f"     Bash/Zsh:    unset LETSFG_API_KEY")

    print(f"\n    Next: letsfg search GDN BCN 2026-07-15\n")


# ── Recover ────────────────────────────────────────────────────────────────

@app.command()
def recover(
    email: str = typer.Option(..., "--email", "-e", help="Your registered email"),
    code: str = typer.Option("", "--code", "-c", help="6-digit recovery code (if you have one)"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Recover your API key via email verification.

    Lost your API key? Run this with your email to get a recovery code,
    then run again with the code to get a new key.

    Step 1: Request code
        letsfg recover --email you@example.com

    Step 2: Verify code (check your email)
        letsfg recover --email you@example.com --code 123456
    """
    import urllib.request
    import urllib.error

    url = base_url or os.environ.get("LETSFG_BASE_URL") or "https://letsfg.co/developers"

    if code:
        # Step 2: Verify code and get new key
        endpoint = f"{url}/api/v1/agents/recover/verify"
        body = json.dumps({"email": email, "code": code}).encode()
    else:
        # Step 1: Request recovery code
        endpoint = f"{url}/api/v1/agents/recover"
        body = json.dumps({"email": email}).encode()

    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "letsfg-cli/1.7.1",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            error_body = json.loads(e.read().decode())
            _err(error_body.get("detail", str(e)))
        except Exception:
            _err(str(e))
    except Exception as e:
        _err(str(e))

    if output_json:
        _json_out(result)
        return

    status = result.get("status", "")
    if status == "sent":
        print(f"\n  ✓ Recovery code sent!")
        print(f"    Check your email ({email}) for a 6-digit code.")
        print(f"\n    Then run:")
        print(f"    letsfg recover --email {email} --code <your-code>\n")
    elif status == "success":
        new_key = result.get("api_key", "")
        # Save the new key
        from letsfg.client import _save_config
        _save_config({
            "api_key": new_key,
            "agent_id": result.get("agent_id", ""),
            "auto_registered": False,
        })
        print(f"\n  ✓ API key recovered!")
        print(f"    Agent ID: {result.get('agent_id')}")
        print(f"    API Key:  {new_key}")
        print(f"\n    Key saved. Your previous key is now invalid.\n")
    else:
        print(f"\n  {result.get('message', 'Unknown response')}\n")


# ── Setup Payment ──────────────────────────────────────────────────────────

@app.command("connect-payment")
def connect_payment(
    token: str = typer.Option("", "--token", "-t", hidden=True, help="Retired Stripe token — ignored"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """[Developer API only] Get a link to connect a payment method to a PAID developer account.

    Most agents do NOT need this. `letsfg auth` already puts a payment method on
    file for search and booking, without a billing account.

    Replaced `letsfg setup-payment` on 2026-09-08; that name still works and lands here.
    Nothing is charged to connect, and card details never touch LetsFG — the printed link
    opens a hosted page where you save a card, Revolut Pay or Google Pay.
    """
    _warn_developer_api_command("letsfg connect-payment")
    if token:
        print("\n  Note: --token was part of the Stripe enrolment, retired 2026-09-08. Ignoring it.")
    bt = _get_client(api_key, base_url)
    try:
        result = bt.connect_payment()
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    url = result.get("connect_url", "")
    if url:
        print(f"\n  Open this in a browser to connect a payment method:\n")
        print(f"    {url}\n")
        print(f"    Nothing is charged. Re-run `letsfg me` afterwards to confirm it landed.\n")
    else:
        _err(f"Could not mint a connect link: {result.get('message', result.get('status', 'unknown'))}")


# `letsfg setup-payment` kept as an alias rather than deleted: it is in published
# READMEs and in people's scripts, and a "command not found" tells them nothing.
app.command("setup-payment", hidden=True)(connect_payment)


# ── Profile ───────────────────────────────────────────────────────────────

@app.command()
def me(
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """[Developer API only] Show your agent profile, balance and usage stats.

    Requires an API key. A PFS Bearer token has no profile — it is bound to your
    payment method, carries no balance, and search and booking are free.
    """
    bt = _get_client(api_key, base_url)
    try:
        profile = bt.me()
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out({
            "agent_id": profile.agent_id,
            "agent_name": profile.agent_name,
            "email": profile.email,
            "tier": profile.tier,
            "access_granted": profile.access_granted,
            "payment_ready": profile.payment_ready,
            "usage": profile.usage,
        })
        return

    print(f"\n  Agent: {profile.agent_name} ({profile.agent_id})")
    print(f"  Email: {profile.email}")
    print(f"  Tier:  {profile.tier}")
    access = getattr(profile, 'access_granted', False)
    print(f"  Access:  {'✓ Granted (search, unlock, book)' if access else '✗ Not granted'}")
    print(f"  Payment: {'✓ Ready' if profile.payment_ready else '—'}")
    u = profile.usage
    print(f"  Searches: {u.get('total_searches', 0)}")
    print(f"  Unlocks:  {u.get('total_unlocks', 0)}")
    print(f"  Bookings: {u.get('total_bookings', 0)}")
    print(f"  Total spent: ${u.get('total_spent_cents', 0) / 100:.2f}\n")



def main():
    app()


if __name__ == "__main__":
    main()
