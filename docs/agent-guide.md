# Building AI Agents with LetsFG

> ### ⚠️ Agents: do not create a Developer API account
>
> Do **NOT** call `/developers/api/v1/agents/register` or
> `/developers/api/v1/agents/connect-payment`, and do **NOT** run
> `letsfg register` or `letsfg connect-payment`. Those belong to a separate,
> paid, prepaid-balance product for high-volume commercial integrations and
> create a billing account you almost certainly do not want.
>
> To search and book flights, connect LetsFG as an MCP server at
> <https://letsfg.co/developers/api/mcp>. The consent step opens
> <https://letsfg.co/connect>, where a card is saved (nothing is charged).
> See <https://letsfg.co/for-agents>.

Guidelines for building autonomous AI agents that search, evaluate, and book flights. Works with OpenClaw, Perplexity Computer, Claude, Cursor, Windsurf, and any MCP-compatible agent framework.

> 🎬 **[Watch the demo](https://github.com/LetsFG/LetsFG#demo-lfg-vs-default-agent-search)** — side-by-side comparison of default agent search vs LetsFG.

## Search

All search runs server-side at letsfg.co. Connect once through the hosted MCP (the consent step saves a card at <https://letsfg.co/connect>; nothing is charged) and every call carries a card-backed token — or use a prepaid Developer API key.

```python
# PFS search — free Bearer token, server-side, 8-10 s to first results
from letsfg import LetsFG
bt = LetsFG()  # uses LETSFG_BEARER_TOKEN from environment
result = bt.search("LHR", "JFK", "2026-06-01")

# Developer API - look-to-book search, direct booking, hotels
bt = LetsFG(api_key="letsfg_...")
result = bt.search("LHR", "JFK", "2026-06-01")
```

**When to use PFS (card-backed token):** This is the agent path — search and booking. Connect LetsFG as an MCP server at `https://letsfg.co/developers/api/mcp`; approving it opens <https://letsfg.co/connect>, where the person saves a card in a 0.00 Revolut setup (any card, or Revolut Pay / Google Pay — no Revolut account needed). Nothing is charged until a booking is made. 8–10 s to first results per search. The SDK and CLI read the same token from `LETSFG_BEARER_TOKEN`.

**When to use Developer API:** Managed cloud search, billing controls, volume usage, and booking through `POST /flights/book`. Register at [letsfg.co/developers](https://letsfg.co/developers), then connect a Revolut method — nothing is charged to connect. Flight search is look-to-book: 200 searches free after every booking, then blocks of 500 for $5.00. No booking fee and no transaction fee; the margin is inside the price you saw. It is also the **only** way to reach hotels.

### Hotels

```python
lfg = LetsFG(api_key="letsfg_...")          # Bearer tokens do NOT work for hotels
city  = lfg.hotel_destinations("Warsaw")[0]
stays = lfg.search_hotels(city_id=city["Id"], city_name=city["Name"],
                          check_in="2026-11-10", check_out="2026-11-12", adults=2)
booking = lfg.book_hotel_and_wait(...)     # async: returns a job, polls to completion
print(booking["confirmation"], booking["pay_link"])
```

Only free-cancellation, pay-later rates are sold. Booking charges 5% to the card on file as a
non-refundable reservation fee; the balance goes straight to the supplier through `pay_link` by
`balance_due_by`. A card is required for hotel **search** as well as booking. Full detail:
[Hotels](hotels.md).

## Architecture

```
User request → Agent parses intent → Resolve locations → Search (local free or public prepaid)
    → Filter & rank offers → Present to user → Book
        (PFS: book directly — fare held on the card, captured only against a real PNR;
         Developer API: POST /flights/book, then poll)
```

## Agent Best Practices

1. **Always resolve locations first.** City names are ambiguous — "London" could be LHR, LGW, STN, LCY, or LTN. Use `resolve_location()` to get IATA codes, then let the user confirm if multiple options exist.

2. **Search is free on PFS.** Search multiple dates and variants freely with a Bearer token. On the Developer API you get 200 free searches after every booking you make, so ordinary search-then-book traffic costs nothing — only a long run of searching without booking eats into a paid block.

3. **Book promptly.** On PFS, offers expire ~15 minutes after search — book while it's still fresh, or search again. On the Developer API a search stays bookable for 6 hours, but fares move, so a stale offer is more likely to come back as a `price_change` question.

4. **Handle price changes gracefully.** Search prices are real-time snapshots. On BOTH lanes the fare is only *held* on the card; the hold is captured once a real PNR exists and released if the booking fails, so a moved price never turns into a surprise charge. If the fare moves at checkout you get a `price_change` question to accept or decline — PFS reports it through `get_flight_booking`, the Developer API through `GET /flights/bookings/{id}` and `POST /flights/bookings/{id}/answer`. There is no unlock step on either lane any more.

5. **Map passenger IDs correctly.** Search returns `passenger_ids` (e.g., `["pas_0", "pas_1"]`). When booking with multiple passengers, each passenger dict must include the correct `id` from this list. The first adult gets `pas_0`, second gets `pas_1`, etc.

6. **Use REAL passenger details.** Airlines send e-tickets to the contact email. Names must match the passenger's passport or government ID. Never use placeholder data.

7. **Search is async, and `completed` is not the end.** The engine returns in 8–10 s to first results. Poll `GET /api/results/<search_id>` immediately and then every 2 s — do not sleep before the first poll, or you put a floor under a search that is already faster than it.

   A search reports `status: "completed"` **before its offer set stops growing**. The split-ticket probe runs two extra connector fan-outs and merges its result in afterwards, so the cheapest itinerary on the search is often one that does not exist yet at the moment the status turns terminal. The response carries `split_ticket_pending` (and `gf_enrich_pending` for the Google Flights enrich) while that is still inbound.

   **Keep polling while either flag is true.** Stopping at `completed` is how you silently discard the cheapest offer. Bound the wait — a flag that never clears must not hang your agent — and take whatever has landed when the bound expires. The official SDKs and the MCP server do this for you. Most searches never fire the probe at all, so the flags are usually already false on the first poll and this costs nothing.

## Handling Edge Cases

```python
from letsfg import LetsFG, LetsFGError

# Retry on expired offers. bt.book() dispatches to the PFS path
# (POST /api/agent-book, starts the booking and returns a booking_ref) if a
# Bearer token is set, otherwise falls back to the Developer API (unlock
# required first) — same call either way.
def resilient_book(bt, origin, dest, date, passenger, email, max_retries=2):
    for attempt in range(max_retries + 1):
        flights = bt.search(origin, dest, date)
        if not flights.offers:
            return None

        try:
            result = bt.book(
                offer_id=flights.cheapest.id,
                passengers=[passenger],
                contact_email=email,
                search_id=flights.search_id,  # PFS path only; ignored on Developer API
            )
            return result
        except LetsFGError as e:
            if e.is_retryable and attempt < max_retries:
                import time; time.sleep(2 ** attempt)
                continue
            raise

# On the PFS path, result is the STARTED booking: {"booking_ref": "...",
# "state": "booking_in_progress"}. The ticket takes 4-11 minutes. Poll
# POST /api/agent-book/status {"booking_ref": ...} every 20-30 s until state is
# "completed" (pnr + charged_amount), "failed" (hold released, nothing charged)
# or "needs_attention" (a human at LetsFG is on it - do NOT book again).
# Never start a second booking for the same trip while one is in progress.

# Compare prices across dates intelligently
def find_cheapest_date(bt, origin, dest, dates):
    """Search multiple dates (free) and return the cheapest option."""
    best = None
    for date in dates:
        try:
            result = bt.search(origin, dest, date)
            if result.offers and (best is None or result.cheapest.price < best[1].price):
                best = (date, result.cheapest, result.passenger_ids)
        except LetsFGError:
            continue  # Skip dates with no routes
    return best  # (date, offer, passenger_ids) or None
```

## Rate Limits and Timeouts

The API has rate limits to ensure fair usage and protect airline endpoints.

| Endpoint | Rate Limit | Timeout |
|----------|-----------|--------|
| Search (MCP / Bearer token) | **10 per 10 min**, 30 per hour, 100 per day — per card on file | 180s (airline APIs can be slow) |
| Search (API) | 60 req/min per agent | 30s |
| Resolve location | 120 req/min per agent | 5s |
| Unlock | 20 req/min per agent | 15s |
| Book | 10 req/min per agent | 30s |

> **MCP search rate limit:** Searches through the hosted MCP and the Bearer-token lane are limited per card on file to **10 per 10 minutes, 30 per hour and 100 per day** (raised from 3 / 10 / 25 on 2026-09-02, see [#208](https://github.com/LetsFG/LetsFG/issues/208)). Going over returns a 429 with `retry_after_seconds`; repeated offences escalate the block (10 min → 30 min → 6 h → 24 h), so honour the value rather than retrying early. Polling `/api/results/<id>` never counts.

### Programmatic access requires a card-backed token

The letsfg.co website is for human users and is protected by Cloudflare Turnstile — plain HTTP requests or headless scripts cannot search it. Any agent or script that calls LetsFG directly must hold a **card-backed Bearer token**.

**Nothing is charged to get one.** The card is saved in a 0.00 Revolut setup — any card, or Revolut Pay / Google Pay, no Revolut account needed — and the details go to Revolut, never to LetsFG. Having a card on file is what lets your agent go all the way to booking, and it keeps automated abuse off the search engine. You pay the ticket price only when you book, and even then the money is held, not taken, until the airline confirms.

**The one way in — connect at letsfg.co/connect:**

1. Add LetsFG as an MCP server: `https://letsfg.co/developers/api/mcp` (Claude, ChatGPT, Cursor, Windsurf, Claude Code — anything that speaks remote MCP with OAuth).
2. Approve the connection. The consent step opens <https://letsfg.co/connect>, where the person adds a card or pays 0.00 with Revolut Pay / Google Pay.
3. The OAuth token you receive is card-backed. Over the MCP it is carried for you. Over raw HTTP send the same token on every request as `Authorization: Bearer <token>`.

`POST https://letsfg.co/api/agent-access/request` still answers `402` with these steps as JSON (`add_card_url`, `how`), so an agent that starts from the endpoint lands in the same place.

> **Retired 2026-09-02:** the Stripe enrolment lanes (`setup_url`, `setup_intent_id`, `payment_method_id`, `card_token`) and every token they issued. Such a token now answers `401 TOKEN_REVOKED`; `POST /api/agent-access/verify` answers `410` for a Stripe credential. Connect the card again at <https://letsfg.co/connect>. The CLI's `letsfg auth` and the SDKs' `payment_auth()` implemented that lane. `letsfg auth` now drives the connect flow itself: it registers as an OAuth client (dynamic registration), opens <https://letsfg.co/connect> for the card, and stores the access and refresh tokens in `~/.letsfg/config.json`. The SDKs also read `LETSFG_BEARER_TOKEN`.

**One card = one account.** A payment method identifies exactly one account; connecting a card that is already in use lands on the existing account. Quotas and rate limits are bucketed per card, not per token.

Once authenticated, use `POST /api/search` (natural language or structured) instead of `GET /en?q=...`.

```bash
# 1. Search with the token (NL query)
curl -X POST https://letsfg.co/api/search \
  -H "Authorization: Bearer eyJ..." \
  -H "Content-Type: application/json" \
  -d '{"query":"London to Barcelona June 15 2026"}'
# → {"search_id":"ws_abc123","status":"searching","parsed":{...}}

# 2. Poll for results
curl https://letsfg.co/api/results/ws_abc123 \
  -H "Authorization: Bearer eyJ..."
```

Full walkthrough: https://letsfg.co/for-agents

### Booking

Booking works exactly like the website checkout, on the card connected to the account: the fare plus LetsFG's markup is **held** on the card (not taken), a LetsFG booking agent buys the ticket from the seller, and the hold is captured only once a real airline PNR exists. If the booking fails, the hold is released and nothing is charged. This works for every offer a search returns, not only a subset. There is no unlock step and no separate LetsFG fee — the markup is inside the price you saw.

```bash
# Step 1 — start the booking (returns within seconds)
curl -X POST https://letsfg.co/api/agent-book \
  -H "Authorization: Bearer eyJ..." \
  -H "Content-Type: application/json" \
  -d '{"search_id":"ws_abc123","offer_id":"ws_off_...",
       "contact_email":"traveller@example.com",
       "passenger":{"given_name":"Ada","family_name":"Lovelace",
                    "born_on":"1990-04-01","gender":"f","nationality":"GB",
                    "phone_number":"+15551234567","phone_country":"US",
                    "address_line1":"1 Analytical Way","address_city":"London",
                    "address_postal":"N1 9GU","address_country":"GB"}}'
# → {"booking_ref":"eyJ...","state":"booking_in_progress"}

# Step 2 — poll until it lands (every 20-30 s; a booking takes 4-11 minutes)
curl -X POST https://letsfg.co/api/agent-book/status \
  -H "Authorization: Bearer eyJ..." \
  -H "Content-Type: application/json" \
  -d '{"booking_ref":"eyJ..."}'
# → {"state":"completed","pnr":"ABC123","charged_amount":93,"currency":"EUR"}
```

States: `booking_in_progress` (the agent is at the seller's checkout — keep waiting) → `completed` (booked; PNR and captured amount in the answer) | `failed` (not booked; hold released, nothing charged; `failure_reason` says why) | `needs_attention` (a human at LetsFG is checking it — do **not** book again; the traveller will be emailed). `updated_at_ms` says when it last moved. Only `completed` with a PNR means booked. Never start a second booking for the same trip while one is in progress — that would place a second hold.

Other answers, all with `"charged": 0`:

- `{"error":"missing_details","missing_fields":["born_on","address_city"]}` — ask the person and call again.
- `{"error":"payment_method_required","add_card_url":"https://letsfg.co/connect"}` — no card connected yet.
- `{"error":"payment_declined","add_card_url":"https://letsfg.co/connect"}` — the card refused the hold.

Use the traveller's **real** details — the seller's checkout asks for every one of them and the e-ticket goes to `contact_email`. Required: name as on the passport, date of birth, gender, nationality, phone with its country, residence address; passport number/country/expiry are optional but help with sellers that ask. One passenger per call; a group trip is one call per person. Offers expire ~15 minutes after the search — if one is gone, search again.

Over the MCP these two steps are the tools `book_flight` and `get_flight_booking`.

Handle rate limits and timeouts in production:

```python
import time
from letsfg import LetsFG, LetsFGError

bt = LetsFG()

def search_with_retry(origin, dest, date, max_retries=3):
    """Retry with exponential backoff on rate limit or timeout."""
    for attempt in range(max_retries):
        try:
            return bt.search(origin, dest, date)
        except LetsFGError as e:
            if "rate limit" in str(e).lower() or "429" in str(e):
                wait = 2 ** attempt  # 1s, 2s, 4s
                print(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif "timeout" in str(e).lower() or "504" in str(e):
                print(f"Timeout, retrying ({attempt + 1}/{max_retries})...")
                time.sleep(1)
            else:
                raise
    raise LetsFGError("Max retries exceeded")
```

## Advanced Preference Evaluation

Rather than always picking the cheapest flight, score offers by weighted criteria:

```python
def score_offer(offer, preferences=None):
    """Score a flight offer by multiple criteria (lower = better).
    
    preferences: dict with weights, e.g.:
        {"price": 0.4, "duration": 0.3, "stops": 0.2, "airline_pref": 0.1}
    """
    prefs = preferences or {"price": 0.4, "duration": 0.3, "stops": 0.2, "airline_pref": 0.1}
    preferred_airlines = {"British Airways", "Delta", "United", "Lufthansa"}
    
    # Normalize factors (0-1 scale, lower is better)
    price_score = offer.price / 2000        # Normalize against $2000 baseline
    duration_hours = offer.outbound.total_duration_seconds / 3600
    duration_score = duration_hours / 24    # Normalize against 24h baseline
    stops_score = offer.outbound.stopovers / 3  # Normalize against 3 stops
    airline_score = 0 if any(a in preferred_airlines for a in offer.airlines) else 1
    
    return (
        prefs["price"] * price_score +
        prefs["duration"] * duration_score +
        prefs["stops"] * stops_score +
        prefs["airline_pref"] * airline_score
    )

# Usage: find best offer considering multiple criteria
flights = bt.search("LHR", "JFK", "2026-06-01", limit=50)
best = min(flights.offers, key=lambda o: score_offer(o, {
    "price": 0.3,      # Price matters, but not everything
    "duration": 0.4,    # Shortest travel time is priority
    "stops": 0.2,       # Prefer direct flights
    "airline_pref": 0.1 # Slight preference for known airlines
}))
print(f"Best overall: {best.airlines[0]} ${best.price} — {best.outbound.stopovers} stops")
```

## Data Persistence for Price Tracking

For agents that track prices over time or compare across sessions:

```python
import json
from datetime import datetime
from pathlib import Path

CACHE_FILE = Path("flight_price_history.json")

def load_price_history():
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text())
    return {}

def save_search_result(origin, dest, date, result):
    """Save search results for later comparison."""
    history = load_price_history()
    key = f"{origin}-{dest}-{date}"
    if key not in history:
        history[key] = []
    history[key].append({
        "searched_at": datetime.utcnow().isoformat(),
        "cheapest_price": result.cheapest.price if result.offers else None,
        "total_offers": result.total_results,
        "airlines": list(set(a for o in result.offers[:5] for a in o.airlines)),
    })
    CACHE_FILE.write_text(json.dumps(history, indent=2))

def get_price_trend(origin, dest, date):
    """Check if prices are rising or falling for a route."""
    history = load_price_history()
    key = f"{origin}-{dest}-{date}"
    entries = history.get(key, [])
    if len(entries) < 2:
        return "insufficient_data"
    prices = [e["cheapest_price"] for e in entries if e["cheapest_price"]]
    if prices[-1] < prices[0]:
        return f"falling (${prices[0]} → ${prices[-1]})"
    return f"rising (${prices[0]} → ${prices[-1]})"
```
