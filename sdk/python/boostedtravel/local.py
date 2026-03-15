"""
Local flight search — runs 73 airline connectors on the user's machine.

Can be used programmatically:

    from boostedtravel.local import search_local
    result = await search_local("SHA", "CTU", "2026-03-20")

Or as a subprocess (used by the npm MCP server + JS SDK):

    echo '{"origin":"SHA","destination":"CTU","date_from":"2026-03-20"}' | python -m boostedtravel.local
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import date

from boostedtravel.models.flights import FlightSearchRequest

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
) -> dict:
    """
    Run all 73 local airline connectors and return results as a dict.

    This is the core local search — no API key needed, no backend.
    Connectors run on the user's machine via Playwright + httpx.
    """
    from boostedtravel.connectors.engine import multi_provider

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
    )

    resp = await multi_provider.search_flights(req)
    return resp.model_dump(mode="json")


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
        )

    try:
        result = asyncio.run(_run())
        json.dump(result, sys.stdout)
    except Exception as e:
        json.dump({"error": str(e)}, sys.stdout)
        sys.exit(1)


if __name__ == "__main__":
    _main()
