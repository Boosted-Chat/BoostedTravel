"""
LetsFG Programmatic Flight Search — quickstart.

Auth is free (a zero-amount card setup — nothing is charged) and lasts 90 days.
Once authenticated, searches run via the LetsFG cloud engine across hundreds of airlines.

Run:
    pip install letsfg
    python quickstart.py
"""

from __future__ import annotations

import asyncio
from letsfg.connectors.auth import payment_auth, get_bearer_token, BearerTokenError
from letsfg.local import search_local


def ensure_auth() -> None:
    """Authenticate if no valid token exists."""
    try:
        get_bearer_token()
    except BearerTokenError:
        print("No token found — starting payment-token auth (nothing is charged)...")
        payment_auth()


async def main() -> None:
    ensure_auth()

    print("\nSearching WAW → BCN on 2026-07-15...")
    result = await search_local("WAW", "BCN", "2026-07-15", currency="EUR", limit=5)

    offers = result.get("offers", [])
    print(f"\n{result.get('total_results', len(offers))} offers. Top {len(offers)}:\n")
    for i, offer in enumerate(offers, 1):
        ob = offer.get("outbound", {})
        segs = ob.get("segments", [])
        route = "→".join(
            [segs[0]["origin"]] + [s["destination"] for s in segs]
        ) if segs else "?"
        price = offer.get("price", 0)
        currency = offer.get("currency", "EUR")
        airlines = ", ".join(offer.get("airlines", [offer.get("owner_airline", "?")]))
        print(f"  {i}. {currency} {price:.2f}  {airlines:<20}  {route}")

    if offers:
        print(f"\nUnlock offer #{1} (reveals direct booking link):")
        print(f"  letsfg book {offers[0].get('id', '<offer_id>')} --search-id <search_id> \\\n"
              f"    --passenger '{{\"given_name\":\"...\"}}' --email you@example.com\n")


if __name__ == "__main__":
    asyncio.run(main())
