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

import json
import os
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

from letsfg.client import LetsFG, LetsFGError

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
    key = api_key or os.environ.get("LETSFG_API_KEY", "")
    url = base_url or os.environ.get("LETSFG_BASE_URL")
    if not key:
        _err("API key required. Set LETSFG_API_KEY or use --api-key flag.\n"
             "Register: letsfg register --name my-agent --email you@example.com")
    return LetsFG(api_key=key, base_url=url)


def _err(msg: str):
    if HAS_RICH:
        console.print(f"[red]Error:[/red] {msg}")
    else:
        print(f"Error: {msg}", file=sys.stderr)
    raise typer.Exit(1)


def _json_out(data):
    """Print JSON output for machine consumption."""
    print(json.dumps(data, indent=2, default=str))


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
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
):
    """Search for flights — FREE, no API key required. Runs 180 airline connectors on your machine."""
    import asyncio
    import logging
    import warnings
    from letsfg.local import search_local

    # Only show errors in CLI mode — suppress connector warning noise
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
        )

    try:
        result = asyncio.run(_run())
    except Exception as e:
        _err(f"Search failed: {e}")

    offers = result.get("offers", [])
    total = result.get("total_results", len(offers))

    # Sort
    if sort == "price":
        offers.sort(key=lambda o: o.get("price", float("inf")))
    elif sort == "duration":
        offers.sort(key=lambda o: (o.get("outbound", {}).get("total_duration_seconds") or float("inf")))

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
        """Extract departure (first segment) or arrival (last segment) time as HH:MM."""
        if not leg:
            return "-"
        segs = leg.get("segments", [])
        if not segs:
            return "-"
        seg = segs[0] if pos == "dep" else segs[-1]
        dt_str = seg.get("departure" if pos == "dep" else "arrival", "")
        if not dt_str:
            return "-"
        try:
            t_part = dt_str.split("T")[1] if "T" in dt_str else dt_str
            return t_part[:5]
        except (IndexError, TypeError):
            return "-"

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
            airlines = o.get("owner_airline") or ",".join(o.get("airlines", []))
            stops = str(ob.get("stopovers", 0))
            price = o.get("price", 0)
            cur = o.get("currency", currency)
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
            airlines = o.get("owner_airline") or ",".join(o.get("airlines", []))
            price = o.get("price", 0)
            cur = o.get("currency", currency)
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
            price = o.get("price", 0)
            cur = o.get("currency", currency)
            airlines = o.get("owner_airline") or ",".join(o.get("airlines", []))
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


# ── Search Local ───────────────────────────────────────────────────────────

@app.command("search-local")
def search_local_cmd(
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
    max_browsers: Optional[int] = typer.Option(None, "--max-browsers", "-b", help="Max concurrent browsers (1-32, default: auto-detect from RAM)"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
):
    """Alias for 'search'. Runs 180 airline connectors locally — FREE, no API key.

    Use --max-browsers to tune performance: lower values (2-4) for low-RAM machines, higher (12-16) for powerful ones.
    Default: auto-detected from your system RAM. Run 'letsfg system-info' to see your profile.
    """
    import asyncio
    import logging
    import os
    import sys
    import warnings
    from letsfg.local import search_local

    # Only show errors in CLI mode — suppress connector warning noise
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
            return  # Suppress errors in the hook itself during shutdown
        _orig_unraisable(hook_args)
    sys.unraisablehook = _quiet_unraisable

    # Suppress Node.js DEP0169 deprecation warnings from Playwright subprocesses
    os.environ.setdefault("NODE_OPTIONS", "--no-deprecation")

    async def _run():
        # Suppress "Future exception was never retrieved" from Playwright cleanup
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
        )

    try:
        result = asyncio.run(_run())
    except Exception as e:
        _err(f"Local search failed: {e}")

    offers = result.get("offers", [])
    total = result.get("total_results", len(offers))

    # Sort
    if sort == "price":
        offers.sort(key=lambda o: o.get("price", float("inf")))
    elif sort == "duration":
        offers.sort(key=lambda o: (o.get("outbound", {}).get("total_duration_seconds") or float("inf")))

    offers = offers[:limit]

    if output_json:
        _json_out({"total_results": total, "offers": offers})
        return

    if not offers:
        print(f"No flights found for {origin} → {destination} on {date}")
        return

    source_tiers = result.get("source_tiers", {})
    has_backend = "paid" in source_tiers
    mode_label = "LOCAL + BACKEND" if has_backend else "LOCAL only (set LETSFG_API_KEY for Amadeus/Duffel)"

    print(f"\n  {total} offers  |  {origin} → {destination}  |  {date}  |  {mode_label}")

    def _local_route(ob):
        route = ob.get("route_str", "")
        if not route:
            segs = ob.get("segments", [])
            if segs:
                codes = [segs[0].get("origin", "")]
                for s in segs:
                    codes.append(s.get("destination", ""))
                route = "→".join(c for c in codes if c)
        return route or "-"

    def _local_dur(ob):
        dur_s = ob.get("total_duration_seconds")
        if dur_s:
            h, m = divmod(dur_s // 60, 60)
            return f"{h}h {m:02d}m"
        return "-"

    def _local_time(leg, pos="dep"):
        if not leg:
            return "-"
        segs = leg.get("segments", [])
        if not segs:
            return "-"
        seg = segs[0] if pos == "dep" else segs[-1]
        dt_str = seg.get("departure" if pos == "dep" else "arrival", "")
        if not dt_str:
            return "-"
        try:
            t_part = dt_str.split("T")[1] if "T" in dt_str else dt_str
            return t_part[:5]
        except (IndexError, TypeError):
            return "-"

    if HAS_RICH:
        table = Table(show_header=True, header_style="bold")
        table.add_column("#", style="dim", width=4)
        table.add_column("Price", justify="right", style="green")
        table.add_column("Airline")
        table.add_column("Route")
        table.add_column("Depart", justify="right")
        table.add_column("Arrive", justify="right")
        table.add_column("Duration", justify="right")
        table.add_column("Stops", justify="center")

        for i, o in enumerate(offers, 1):
            ob = o.get("outbound", {})
            airlines = o.get("owner_airline") or ",".join(o.get("airlines", []))
            stops = str(ob.get("stopovers", 0))
            price = o.get("price", 0)
            cur = o.get("currency", currency)
            table.add_row(str(i), f"{cur} {price:.2f}", airlines, _local_route(ob),
                          _local_time(ob, "dep"), _local_time(ob, "arr"),
                          _local_dur(ob), stops)
        console.print(table)
    else:
        for i, o in enumerate(offers, 1):
            price = o.get("price", 0)
            cur = o.get("currency", currency)
            airlines = o.get("owner_airline") or ",".join(o.get("airlines", []))
            ob = o.get("outbound", {})
            dep = _local_time(ob, "dep")
            arr = _local_time(ob, "arr")
            print(f"  {i:3d}. {cur} {price:.2f}  {airlines}  {_local_route(ob)} {dep}→{arr}")

    print()


# ── Star (Link GitHub) ─────────────────────────────────────────────────────

@app.command()
def star(
    github: str = typer.Option(..., "--github", "-g", help="Your GitHub username"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Link your GitHub account — star the repo for FREE unlimited access.

    1. Star https://github.com/LetsFG/LetsFG
    2. Run: letsfg star --github <your-username>
    3. Done — unlimited search, unlock, and book forever.
    """
    bt = _get_client(api_key, base_url)
    try:
        result = bt.link_github(github)
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
        result = bt.resolve_location(query)
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
    """Register a new agent — get your API key."""
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

    if output_json:
        _json_out(result)
        return

    print(f"\n  ✓ Agent registered!")
    print(f"    Agent ID: {result.get('agent_id')}")
    print(f"    API Key:  {result.get('api_key')}")
    print(f"\n    Save your API key:")
    print(f"    export LETSFG_API_KEY={result.get('api_key')}")
    print(f"\n    Next: Star the repo and link your GitHub:")
    print(f"    1. Star https://github.com/LetsFG/LetsFG")
    print(f"    2. letsfg star --github <your-github-username>\n")


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
        print(f"  GitHub:  ✓ {gh} (star verified — unlimited access)")
    elif gh:
        print(f"  GitHub:  {gh} (star not verified)")
    else:
        print(f"  GitHub:  Not linked — run: letsfg star --github <username>")
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
