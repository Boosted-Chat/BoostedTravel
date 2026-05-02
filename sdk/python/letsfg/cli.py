"""
LetsFG CLI — Agent-native flight search & booking from terminal.

Usage (search — free, no API key):
    letsfg search GDN BER 2026-03-03
    letsfg search LON BCN 2026-04-01 --return 2026-04-08 --sort price

Usage (booking — requires API key):
    letsfg unlock off_xxx
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
from letsfg.connectors.currency import fetch_rates, _fallback_convert

app = typer.Typer(
    name="letsfg",
    help=(
        "LetsFG — Agent-native flight search & booking.\n\n"
        "Search 180 airlines at raw airline prices — $20-50 cheaper than OTAs.\n"
        "Search runs locally on your machine — FREE, no API key needed.\n\n"
        "Quick start: letsfg search GDN BCN 2026-06-15\n"
        "Round trip:  letsfg search LON BCN 2026-04-01 --return 2026-04-08"
    ),
    no_args_is_help=True,
)
console = Console() if HAS_RICH else None


def _get_client(api_key: str | None = None, base_url: str | None = None) -> LetsFG:
    """Get a LetsFG client.

    Key resolution order: explicit arg > env var > config file > auto-register.
    """
    url = base_url or os.environ.get("LETSFG_BASE_URL")
    bt = LetsFG(api_key=api_key, base_url=url, client_type="cli")
    if not bt.api_key:
        _err("Could not connect to LetsFG API for auto-registration.\n"
             "You can register manually: letsfg register --name my-agent --email you@example.com")
    return bt


def _handle_auth_error(e: LetsFGError) -> None:
    """Print helpful message when API key is invalid."""
    from letsfg.client import _saved_api_key

    env_key = os.environ.get("LETSFG_API_KEY") or os.environ.get("BOOSTEDTRAVEL_API_KEY")
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
    if HAS_RICH:
        console.print(f"[red]Error:[/red] {msg}")
    else:
        print(f"Error: {msg}", file=sys.stderr)
    raise typer.Exit(1)


def _json_out(data):
    """Print JSON output for machine consumption."""
    print(json.dumps(data, indent=2, default=str))


def _resolve_locations_with_local_fallback(bt: LetsFG, query: str) -> list[dict]:
    """Resolve locations through the API, falling back to local SDK data when empty."""
    result = bt.resolve_location(query)
    if result:
        return result

    from letsfg.local import _resolve_location_local

    return _resolve_location_local(query)


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
    "HA": "Hawaiian Airlines", "F9": "Frontier Airlines", "NK": "Spirit Airlines",
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
    max_browsers: Optional[int] = typer.Option(None, "--max-browsers", "-b", help="Max concurrent browsers (1-32, default: auto-detect from RAM)"),
    mode: Optional[str] = typer.Option(None, "--mode", "-m", help="Search mode: 'fast' (OTAs + key airlines, 20-40s) or default (all 200+ connectors)"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show pipeline diagnostics (per-source offer counts at each filter stage)"),
):
    """Search for flights — FREE, no API key required. Runs 180 airline connectors on your machine."""
    import asyncio
    import logging
    import warnings
    from letsfg.local import search_local

    # In verbose mode, show INFO from engine pipeline; otherwise only errors
    if verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(name)s: %(message)s")
    else:
        logging.basicConfig(level=logging.ERROR, stream=sys.stderr, format="%(message)s")

    # Suppress asyncio transport warnings from Playwright subprocess cleanup
    warnings.filterwarnings("ignore", category=ResourceWarning, message=".*unclosed transport.*")

    # Suppress asyncio __del__ "Exception ignored" noise on Python 3.13+
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

    # Suppress Node.js DEP0169 deprecation warnings from Playwright subprocesses
    os.environ.setdefault("NODE_OPTIONS", "--no-deprecation")
    # Tag telemetry as CLI-sourced
    os.environ["LETSFG_TELEMETRY_SOURCE"] = "cli"

    # --direct is a shortcut for --max-stops 0
    effective_max_stops = 0 if direct else max_stops

    async def _run():
        asyncio.get_event_loop().set_exception_handler(lambda loop, ctx: None)
        return await search_local(
            origin=origin,
            destination=destination,
            date_from=date,
            return_date=return_date,
            adults=adults,
            children=children,
            cabin_class=cabin,
            currency=currency,
            limit=limit,
            max_browsers=max_browsers,
            max_stopovers=effective_max_stops,
            mode=mode,
        )

    try:
        result = asyncio.run(_run())
    except Exception as e:
        _err(f"Search failed: {e}")

    offers = result.get("offers", [])
    total = result.get("total_results", len(offers))

    # Apply a final local sort after all connector/provider results are merged.
    _final_sort_offers(offers, sort)

    offers = offers[:limit]

    if output_json:
        _json_out({"total_results": total, "offers": offers})
        return

    if not offers:
        print(f"No flights found for {origin} → {destination} on {date}")
        return

    has_return = any(o.get("inbound") for o in offers)
    trip_label = f"{origin} ↔ {destination}" if return_date else f"{origin} → {destination}"
    date_label = f"{date} → {return_date}" if return_date else date
    print(f"\n  {total} offers  |  {trip_label}  |  {date_label}")

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
    try:
        eur_rates = asyncio.run(fetch_rates("EUR"))
    except Exception:
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

        # Print booking URLs below the table
        print("\n  Booking URLs:")
        for i, o in enumerate(offers, 1):
            url = o.get("booking_url") or ""
            cond = o.get("conditions") or {}
            ob_url = cond.get("outbound_booking_url", "")
            ib_url = cond.get("inbound_booking_url", "")
            airlines = _fmt_airline(o.get("owner_airline", ""), o.get("airlines", []))
            raw_price = o.get("price", 0)
            raw_currency = (o.get("currency", currency) or currency).upper()
            price, cur = _convert_display_price(raw_price, raw_currency, target_currency, eur_rates)
            offer_id = o.get("id", "")
            id_str = f"  [{offer_id}]" if offer_id else ""
            if ob_url and ib_url:
                # Combo offer with separate leg URLs
                print(f"  {i:3d}. {cur} {price:.2f} {airlines}{id_str}")
                print(f"       Outbound: {ob_url}")
                print(f"       Return:   {ib_url}")
            elif url:
                print(f"  {i:3d}. {cur} {price:.2f} {airlines}{id_str}")
                print(f"       {url}")
            else:
                print(f"  {i:3d}. {cur} {price:.2f} {airlines}{id_str} — no booking URL")
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
            url = o.get("booking_url") or ""
            cond = o.get("conditions") or {}
            ob_url = cond.get("outbound_booking_url", "")
            ib_url = cond.get("inbound_booking_url", "")
            offer_id = o.get("id", "")
            id_str = f"  [{offer_id}]" if offer_id else ""
            print(f"  {i:3d}. {cur} {price:.2f}  {airlines}  {_route_str(ob)} {dep}→{arr}{ret}{id_str}")
            if ob_url and ib_url:
                print(f"       Outbound: {ob_url}")
                print(f"       Return:   {ib_url}")
            elif url:
                print(f"       {url}")

    print()


# ── Star (Link GitHub) ─────────────────────────────────────────────────────

@app.command()
def star(
    github: str = typer.Option(..., "--github", "-g", help="Your GitHub username"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", help="API key (defaults to saved key)"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Link your GitHub account — star the repo for FREE unlimited access.

    1. Star the repo:   https://github.com/LetsFG/LetsFG
    2. Run:             letsfg star --github <your-username>

    That's it — no registration needed, we handle it automatically.
    """
    # For star command, prefer config file key over env var (avoids stale env issues)
    from letsfg.client import _saved_api_key
    if not api_key:
        api_key = _saved_api_key()
    if not api_key:
        api_key = os.environ.get("LETSFG_API_KEY") or os.environ.get("BOOSTEDTRAVEL_API_KEY")

    bt = _get_client(api_key, base_url)
    try:
        result = bt.link_github(github)
    except AuthenticationError as e:
        _handle_auth_error(e)
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    status = result.get("status", "unknown")
    if status == "verified":
        print(f"\n  ✓ GitHub star verified! Unlimited access granted.")
        print(f"    Username: {result.get('github_username')}")
        print(f"\n    You're all set — search, unlock, and book for free.\n")
    elif status == "already_verified":
        print(f"\n  ✓ Already verified! You have unlimited access.")
        print(f"    Username: {result.get('github_username')}\n")
    elif status == "star_required":
        print(f"\n  ✗ Star not found for '{github}'.")
        print(f"    1. Star the repo: https://github.com/LetsFG/LetsFG")
        print(f"    2. Run this command again.\n")
    else:
        _err(f"Unexpected status: {status}")


# ── Unlock ────────────────────────────────────────────────────────────────

@app.command()
def unlock(
    offer_id: str = typer.Argument(..., help="Offer ID from search results"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Unlock a flight offer — FREE with GitHub star. Confirms price, reserves 30min."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.unlock(offer_id)
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out({
            "offer_id": result.offer_id,
            "unlock_status": result.unlock_status,
            "confirmed_price": result.confirmed_price,
            "confirmed_currency": result.confirmed_currency,
            "offer_expires_at": result.offer_expires_at,
            "payment_charged": result.payment_charged,
            "payment_amount_cents": result.payment_amount_cents,
        })
        return

    if result.is_unlocked:
        print(f"\n  ✓ Offer unlocked!")
        print(f"    Confirmed price: {result.confirmed_currency} {result.confirmed_price:.2f}")
        print(f"    Expires at: {result.offer_expires_at}")
        print(f"\n    Next: letsfg book {offer_id} --passenger '{{...}}' --email you@example.com\n")
    else:
        _err(f"Unlock failed: {result.message}")


# ── Book ──────────────────────────────────────────────────────────────────

@app.command()
def book(
    offer_id: str = typer.Argument(..., help="Offer ID (must be unlocked first)"),
    passenger: list[str] = typer.Option(..., "--passenger", "-p", help='JSON passenger object: \'{"id":"pas_xxx","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}\''),
    email: str = typer.Option(..., "--email", "-e", help="Contact email"),
    phone: str = typer.Option("", "--phone", help="Contact phone"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Book a flight — charges ticket price via Stripe. Creates real airline reservation."""
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
    """Register a new agent — get your API key.

    Note: this is optional! The CLI auto-registers on first use.
    Use this command when you want a named agent with your email attached.
    """
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
    env_key = os.environ.get("LETSFG_API_KEY") or os.environ.get("BOOSTEDTRAVEL_API_KEY")
    if env_key and env_key != new_key:
        print(f"\n  ⚠️  WARNING: You have an old API key in your environment variable.")
        print(f"     The CLI will use the OLD key unless you clear it:")
        print(f"     PowerShell:  $env:LETSFG_API_KEY = ''")
        print(f"     Bash/Zsh:    unset LETSFG_API_KEY")

    print(f"\n    Next: Star the repo and link your GitHub:")
    print(f"    1. Star https://github.com/LetsFG/LetsFG")
    print(f"    2. letsfg star --github <your-github-username>\n")


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

    url = base_url or os.environ.get("LETSFG_BASE_URL") or "https://api.letsfg.co"

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

@app.command("setup-payment")
def setup_payment(
    token: str = typer.Option("tok_visa", "--token", "-t", help="Payment token"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Set up payment method (required before booking)."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.setup_payment(token=token)
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    status = result.get("status", "unknown")
    if status == "ready":
        print(f"\n  ✓ Payment ready!")
        print(f"    You can now unlock offers and book flights.\n")
    else:
        _err(f"Payment setup failed: {result.get('message', status)}")


# ── Profile ───────────────────────────────────────────────────────────────

@app.command()
def me(
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Show your agent profile and usage stats."""
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
            "github_username": profile.github_username,
            "github_star_verified": profile.github_star_verified,
            "access_granted": profile.access_granted,
            "payment_ready": profile.payment_ready,
            "usage": profile.usage,
        })
        return

    print(f"\n  Agent: {profile.agent_name} ({profile.agent_id})")
    print(f"  Email: {profile.email}")
    print(f"  Tier:  {profile.tier}")
    gh = getattr(profile, 'github_username', '') or ''
    star_ok = getattr(profile, 'github_star_verified', False)
    if star_ok:
        print(f"  GitHub:  ✓ {gh} (star verified)")
    elif gh:
        print(f"  GitHub:  {gh} (star not yet verified — run: letsfg star --github {gh})")
    else:
        print(f"  GitHub:  Not linked — run: letsfg star --github <username>")
    access = getattr(profile, 'access_granted', False)
    print(f"  Access:  {'✓ Granted (search, unlock, book)' if access else '✗ Not granted — star the repo to unlock'}")
    print(f"  Payment: {'✓ Ready' if profile.payment_ready else '—'}")
    u = profile.usage
    print(f"  Searches: {u.get('total_searches', 0)}")
    print(f"  Unlocks:  {u.get('total_unlocks', 0)}")
    print(f"  Bookings: {u.get('total_bookings', 0)}")
    print(f"  Total spent: ${u.get('total_spent_cents', 0) / 100:.2f}\n")


@app.command("system-info")
def system_info_cmd(
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
):
    """Show system resources and recommended concurrency settings.

    Agents can use this to pick optimal --max-browsers values.
    """
    from letsfg.system_info import get_system_profile
    from letsfg.connectors.browser import get_max_browsers

    profile = get_system_profile()
    current_max = get_max_browsers()

    if output_json:
        profile["current_max_browsers"] = current_max
        _json_out(profile)
        return

    ram_total = profile["ram_total_gb"]
    ram_avail = profile["ram_available_gb"]
    print(f"\n  Platform:     {profile['platform']}")
    print(f"  CPU cores:    {profile['cpu_cores']}")
    print(f"  RAM total:    {ram_total:.1f} GB" if ram_total else "  RAM total:    unknown")
    print(f"  RAM available: {ram_avail:.1f} GB" if ram_avail else "  RAM available: unknown")
    print(f"  Tier:         {profile['tier']}")
    print(f"  Recommended max browsers: {profile['recommended_max_browsers']}")
    print(f"  Current max browsers:     {current_max}")
    print(f"\n  Override with: letsfg search-local ... --max-browsers {current_max}")
    print(f"  Or set env:   LETSFG_MAX_BROWSERS={current_max}\n")


def main():
    app()


if __name__ == "__main__":
    main()
