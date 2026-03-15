# AGENTS.md — BoostedTravel for AI Agents

> This file is for AI agents. If you're a human, see [README.md](README.md).

## What is BoostedTravel?

BoostedTravel is the largest agent-native flight search and booking toolkit. It gives your agent **ready-to-run connectors to 73 airlines and 400+ GDS/NDC sources** — not scrapers you have to maintain, but production-grade integrations that work out of the box. Built for OpenClaw, Perplexity Computer, Claude, Cursor, Windsurf, and any MCP-compatible agent.

**Why this exists:** Building a flight search integration from scratch takes 30+ minutes, burns thousands of tokens on browser automation, and still only covers one airline at a time. BoostedTravel replaces all of that with a single function call that fires 73 airline connectors in parallel, scanning the entire world for the best deals — including airlines your agent has never heard of.

### Two search channels run simultaneously:

1. **73 local airline connectors** — Ryanair, EasyJet, Wizz Air, Southwest, AirAsia, Norwegian, Spring Airlines, and 66 more. These run directly on your machine via Python + Playwright. No API key needed, completely free, unlimited searches.

2. **Enterprise GDS/NDC providers** — Amadeus, Duffel, Sabre, Travelport, Kiwi. These are contract-only data sources that normally require enterprise agreements worth $50k+/year. BoostedTravel is contracted with these providers and makes their deals available to every agent through the backend API.

### What makes this different from flight websites:

- **Zero price bias** — no demand-based inflation, no cookie tracking, no surge pricing. The same flight that costs $350 on a travel website is typically **$20–$50 cheaper** here because you're getting the raw airline price.
- **73 airlines in parallel** — one search covers Europe, Asia, Americas, Middle East, and Africa simultaneously. Discovers deals on carriers like Jazeera Airways, FlySafair, Lucky Air, 9 Air, and Flybondi that most agents wouldn't know to check.
- **Virtual interlining** — the combo engine builds cross-airline round-trips (e.g., Ryanair outbound + Wizz Air return) that save 30-50% vs booking with either airline alone.
- **Token-efficient** — one tool call replaces what would take thousands of tokens of browser automation, scraping setup, and HTML parsing. Results come back as structured JSON.
- **Works with any agent** — OpenClaw, Perplexity Computer, Claude Desktop, Cursor, Windsurf, or any MCP-compatible client. Also available as Python SDK, JS SDK, and CLI.
- **Enterprise deals included** — contract-only inventory from GDS/NDC providers that individual developers can't access on their own.

**API Base URL:** `https://api.boostedchat.com`

## Why Use This Instead of Building Your Own

| Approach | Time | Cost | Coverage | Maintenance |
|----------|------|------|----------|-------------|
| Browser automation (Playwright/Selenium) | 30+ min per airline | Thousands of tokens | 1 airline at a time | Breaks when site changes |
| Scraping flight websites | 15+ min setup | High token burn | Limited to sites you know | Fragile, needs constant fixing |
| Google Flights API | N/A | N/A | Doesn't exist (no public API) | N/A |
| **BoostedTravel** | **10 seconds** | **1 tool call** | **73 airlines + GDS in parallel** | **We maintain it** |

Flight websites (Kayak, Google Flights, Expedia, Booking.com) also inflate prices based on demand tracking, cookie/session tracking, browser fingerprinting, and surge pricing. **BoostedTravel returns the raw airline price every time.** The only fee is a flat $1 to unlock an offer before checkout.

## How It Works (3 Steps)

### 1. Search (FREE, unlimited)
```
POST /api/v1/flights/search
```
Search 400+ airlines across multiple providers + 73 local LCC connectors. Returns real-time prices with zero markup or bias. Completely free, no limits.

**Local-only search** (no API key, no backend):
```python
from boostedtravel.local import search_local
result = await search_local("GDN", "BCN", "2026-06-15")

# Limit concurrency on constrained machines
result = await search_local("GDN", "BCN", "2026-06-15", max_browsers=4)
```

### 2. Unlock ($1.00)
```
POST /api/v1/bookings/unlock
```
Confirm the live price and lock it for booking. Costs $1.00 via Stripe. This is the only charge.

**What happens when you unlock:**
1. BoostedTravel sends `offer_id` to the airline's NDC/GDS system
2. Airline confirms **current live price** (may differ from search)
3. $1.00 charged via Stripe to your saved payment method
4. Offer **reserved for 30 minutes** — you must book within this window
5. Returns `confirmed_price`, `confirmed_currency`, `offer_expires_at`

**Requirements:** Payment method must be set up first (`boostedtravel setup-payment` or `bt.setup_payment()`). Without payment, unlock returns HTTP 402 (`PaymentRequiredError`).

**Key unlock details:**
- Input: `offer_id` (from search results) — this is the only required parameter
- HTTP 402 → no payment method attached
- HTTP 410 → offer expired (airline sold the seats) — search again
- The `confirmed_price` may differ from search price (airline prices change in real-time)
- If 30-minute window expires without booking, the $1 is not refunded

```python
from boostedtravel import BoostedTravel, PaymentRequiredError, OfferExpiredError

bt = BoostedTravel()  # reads BOOSTEDTRAVEL_API_KEY

flights = bt.search("LHR", "JFK", "2026-06-01")

try:
    unlocked = bt.unlock(flights.cheapest.id)
    print(f"Confirmed: {unlocked.confirmed_price} {unlocked.confirmed_currency}")
    print(f"Expires: {unlocked.offer_expires_at}")
except PaymentRequiredError:
    print("Run: boostedtravel setup-payment")
except OfferExpiredError:
    print("Offer expired — search again")
```

```bash
# CLI
boostedtravel unlock off_xxx
# Output: Confirmed price: EUR 189.50, Expires: 2026-06-01T15:30:00Z

# cURL
curl -X POST https://api.boostedchat.com/api/v1/bookings/unlock \
  -H "X-API-Key: trav_..." \
  -H "Content-Type: application/json" \
  -d '{"offer_id": "off_xxx"}'
# Response: {"offer_id":"off_xxx","confirmed_price":189.50,"confirmed_currency":"EUR","offer_expires_at":"..."}
```

### 3. Book (FREE after unlock)
```
POST /api/v1/bookings/book
```
Book the flight with real passenger details. **No additional charges** — booking is free after the $1 unlock.

## ⚠️ CRITICAL: Use REAL Passenger Details

When booking, you **MUST** use the real passenger's:
- **Email address** — the airline sends the e-ticket and booking confirmation here
- **Full legal name** — must match the passenger's passport or government ID exactly

Do NOT use placeholder emails, agent emails, or fake names. The booking will fail or the passenger will not receive their ticket.

## Installation & CLI Usage

### Install (Python — recommended for agents)
```bash
pip install boostedtravel
```

This gives you the `boostedtravel` CLI command:

```bash
# Register and get your API key
boostedtravel register --name my-agent --email you@example.com

# Save your key
export BOOSTEDTRAVEL_API_KEY=trav_...

# Search flights
boostedtravel search LHR JFK 2026-04-15

# Round trip
boostedtravel search LON BCN 2026-04-01 --return 2026-04-08 --sort price

# Multi-passenger: 2 adults + 1 child, business class
boostedtravel search LHR SIN 2026-06-01 --adults 2 --children 1 --cabin C

# Direct flights only
boostedtravel search JFK LHR 2026-05-01 --max-stops 0

# Resolve city to IATA codes
boostedtravel locations "New York"

# Unlock an offer ($1)
boostedtravel unlock off_xxx

# Book the flight (free after unlock)
boostedtravel book off_xxx \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --email john.doe@example.com

# Check profile & usage
boostedtravel me
```

All commands support `--json` for structured output:
```bash
boostedtravel search GDN BER 2026-03-03 --json
```

### Search Flags Reference

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--return` | `-r` | _(one-way)_ | Return date for round-trip (YYYY-MM-DD) |
| `--adults` | `-a` | `1` | Number of adults (1–9) |
| `--children` | | `0` | Number of children (2–11 years) |
| `--cabin` | `-c` | _(any)_ | `M` economy, `W` premium, `C` business, `F` first |
| `--max-stops` | `-s` | `2` | Max stopovers (0–4) |
| `--currency` | | `EUR` | Currency code |
| `--limit` | `-l` | `20` | Max results (1–100) |
| `--sort` | | `price` | `price` or `duration` |
| `--max-browsers` | `-b` | _(auto)_ | Max concurrent browsers for local search (1–32) |
| `--json` | `-j` | | JSON output for machine consumption |

### Python SDK
```python
from boostedtravel import BoostedTravel

bt = BoostedTravel(api_key="trav_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")
```

### JavaScript/TypeScript SDK + CLI
```bash
npm install -g boostedtravel
```

Same CLI commands available, plus SDK usage:
```typescript
import { BoostedTravel } from 'boostedtravel';

const bt = new BoostedTravel({ apiKey: 'trav_...' });
const flights = await bt.search('LHR', 'JFK', '2026-04-15');
console.log(`${flights.totalResults} offers`);
```

### MCP Server (Claude Desktop / Cursor / Windsurf)
```bash
npx boostedtravel-mcp
```

Add to your MCP config:
```json
{
  "mcpServers": {
    "boostedtravel": {
      "command": "npx",
      "args": ["-y", "boostedtravel-mcp"],
      "env": {
        "BOOSTEDTRAVEL_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

## CLI Commands

| Command | Description | Cost |
|---------|-------------|------|
| `boostedtravel register` | Get your API key | Free |
| `boostedtravel search <origin> <dest> <date>` | Search flights | Free |
| `boostedtravel locations <query>` | Resolve city/airport to IATA | Free |
| `boostedtravel unlock <offer_id>` | Unlock offer details | $1 |
| `boostedtravel book <offer_id>` | Book the flight | Free (after unlock) |
| `boostedtravel setup-payment` | Set up payment method | Free |
| `boostedtravel me` | View profile & usage | Free |

## Authentication — How to Use Your API Key

Every authenticated request requires the `X-API-Key` header. The SDK/CLI handles this automatically.

### Get a Key (No Auth Needed)

```bash
# CLI
boostedtravel register --name my-agent --email agent@example.com

# cURL
curl -X POST https://api.boostedchat.com/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "agent@example.com"}'

# Response: { "agent_id": "ag_xxx", "api_key": "trav_xxxxx..." }
```

### Use the Key

```bash
# Option 1: Environment variable (recommended)
export BOOSTEDTRAVEL_API_KEY=trav_...
boostedtravel search LHR JFK 2026-04-15  # reads env automatically

# Option 2: Pass directly
boostedtravel search LHR JFK 2026-04-15 --api-key trav_...

# Option 3: cURL (raw HTTP)
curl -X POST https://api.boostedchat.com/api/v1/flights/search \
  -H "X-API-Key: trav_..." \
  -H "Content-Type: application/json" \
  -d '{"origin": "LHR", "destination": "JFK", "date_from": "2026-04-15"}'
```

### Python SDK

```python
from boostedtravel import BoostedTravel

# Pass directly
bt = BoostedTravel(api_key="trav_...")

# Or from env
bt = BoostedTravel()  # reads BOOSTEDTRAVEL_API_KEY

# Register inline
creds = BoostedTravel.register("my-agent", "agent@example.com")
bt = BoostedTravel(api_key=creds["api_key"])
```

### Setup Payment (Required Before Unlock)

```bash
boostedtravel setup-payment  # opens Stripe to attach payment method
```

```python
bt.setup_payment(token="tok_visa")  # Stripe payment token
```

## Resolve Locations Before Searching

Always resolve city names to IATA codes first. City names are ambiguous — "London" could be LHR, LGW, STN, LCY, or LTN:

```python
locations = bt.resolve_location("London")
# [
#   {"iata_code": "LHR", "name": "Heathrow", "type": "airport", "city": "London"},
#   {"iata_code": "LGW", "name": "Gatwick", "type": "airport", "city": "London"},
#   {"iata_code": "LON", "name": "London", "type": "city"},
#   ...
# ]

# Use city code for all airports, or specific airport
flights = bt.search("LON", "BCN", "2026-04-01")  # all London airports
flights = bt.search("LHR", "BCN", "2026-04-01")  # Heathrow only
```

```bash
boostedtravel locations "New York"
# JFK  John F. Kennedy International Airport
# LGA  LaGuardia Airport
# EWR  Newark Liberty International Airport
# NYC  New York (all airports)
```

## Working with Search Results

Search returns offers from multiple airlines with full details — all for free:

```python
flights = bt.search("LON", "BCN", "2026-04-01", return_date="2026-04-08", limit=50)

for offer in flights.offers:
    print(f"{offer.owner_airline}: {offer.currency} {offer.price}")
    print(f"  Route: {offer.outbound.route_str}")
    print(f"  Duration: {offer.outbound.total_duration_seconds // 3600}h {(offer.outbound.total_duration_seconds % 3600) // 60}m")
    print(f"  Stops: {offer.outbound.stopovers}")
    print(f"  Refundable: {offer.conditions.get('refund_before_departure', 'unknown')}")
    print(f"  Changeable: {offer.conditions.get('change_before_departure', 'unknown')}")

# Filter: direct flights only
direct = [o for o in flights.offers if o.outbound.stopovers == 0]

# Filter: specific airline
ba = [o for o in flights.offers if "British Airways" in o.airlines]

# Filter: refundable only
refundable = [o for o in flights.offers if o.conditions.get("refund_before_departure") == "allowed"]

# Sort by duration
by_duration = sorted(flights.offers, key=lambda o: o.outbound.total_duration_seconds)

# Cheapest
print(f"Best: {flights.cheapest.price} {flights.cheapest.currency} on {flights.cheapest.owner_airline}")
```

### JSON Output Structure (CLI)

```bash
boostedtravel search LON BCN 2026-04-01 --adults 2 --json
```

```json
{
  "passenger_ids": ["pas_0", "pas_1"],
  "total_results": 47,
  "offers": [
    {
      "id": "off_xxx",
      "price": 89.50,
      "currency": "EUR",
      "airlines": ["Ryanair"],
      "owner_airline": "Ryanair",
      "route": "STN → BCN",
      "duration_seconds": 7800,
      "stopovers": 0,
      "conditions": {
        "refund_before_departure": "not_allowed",
        "change_before_departure": "allowed_with_fee"
      },
      "is_locked": false
    }
  ]
}
```

## Error Handling

The SDK raises specific exceptions for each failure mode. All errors include machine-readable `error_code` and `error_category` fields so agents can programmatically decide how to react.

### Error Categories

| Category | Meaning | Agent action |
|----------|---------|-------------|
| `transient` | Temporary failure (network, rate limit, supplier timeout) | Retry after short delay (1-5s) |
| `validation` | Bad input (invalid IATA, bad date, missing param) | Fix the request, then retry |
| `business` | Requires human decision (payment declined, fare expired) | Inform user, do not auto-retry |

### Error Codes Reference

| Error Code | Category | HTTP | Description |
|------------|----------|------|-------------|
| `SUPPLIER_TIMEOUT` | transient | 504 | Airline API didn't respond in time |
| `RATE_LIMITED` | transient | 429 | Too many requests — wait and retry |
| `SERVICE_UNAVAILABLE` | transient | 503 | Backend temporarily down |
| `NETWORK_ERROR` | transient | 0 | Client-side connection failure |
| `INVALID_IATA` | validation | 422 | Bad airport/city code — use resolve_location |
| `INVALID_DATE` | validation | 422 | Date in wrong format or in the past |
| `INVALID_PASSENGERS` | validation | 422 | Passenger data missing or malformed |
| `UNSUPPORTED_ROUTE` | validation | 422 | No providers serve this route |
| `MISSING_PARAMETER` | validation | 422 | Required field missing |
| `INVALID_PARAMETER` | validation | 422 | Field value out of range or wrong type |
| `AUTH_INVALID` | business | 401 | API key missing or invalid |
| `PAYMENT_REQUIRED` | business | 402 | No payment method (call setup_payment) |
| `PAYMENT_DECLINED` | business | 402 | Stripe charge failed |
| `OFFER_EXPIRED` | business | 410 | Offer no longer available — search again |
| `OFFER_NOT_UNLOCKED` | business | 403 | Tried to book without unlocking first |
| `FARE_CHANGED` | business | 409 | Price changed since search — re-unlock |
| `ALREADY_BOOKED` | business | 409 | Duplicate booking (idempotency_key matched) |
| `BOOKING_FAILED` | business | 500 | Booking failed at airline level |

### Exception Classes

| Exception | HTTP Code | When it happens |
|-----------|-----------|-----------------|
| `AuthenticationError` | 401 | Missing or invalid API key |
| `PaymentRequiredError` | 402 | No payment method or payment declined |
| `OfferExpiredError` | 410 | Offer no longer available (search again) |
| `ValidationError` | 422 | Bad input parameters |
| `BoostedTravelError` | any | Base class — catches all API errors |

### Using Error Codes in Agent Logic

```python
from boostedtravel import (
    BoostedTravel, BoostedTravelError,
    AuthenticationError, PaymentRequiredError, OfferExpiredError, ValidationError,
    ErrorCode, ErrorCategory,
)

bt = BoostedTravel()

try:
    flights = bt.search("LHR", "JFK", "2026-04-15")
    unlocked = bt.unlock(flights.cheapest.id)
    booking = bt.book(
        offer_id=unlocked.offer_id,
        passengers=[{"id": flights.passenger_ids[0], "given_name": "John", "family_name": "Doe",
                     "born_on": "1990-01-15", "gender": "m", "title": "mr",
                     "email": "john@example.com"}],
        contact_email="john@example.com",
        idempotency_key="booking-attempt-abc123",  # prevents double-booking on retry
    )
except BoostedTravelError as e:
    if e.is_retryable:
        # Transient error — safe to retry after delay
        print(f"Temporary error ({e.error_code}), retrying...")
    elif e.error_category == ErrorCategory.VALIDATION:
        # Bad input — fix and retry
        print(f"Fix input: {e.error_code} — {e.message}")
    else:
        # Business error — needs human decision
        print(f"Cannot proceed: {e.error_code} — {e.message}")
```

```typescript
// JavaScript/TypeScript
import { BoostedTravel, BoostedTravelError, ErrorCode, ErrorCategory } from 'boostedtravel';

try {
  const booking = await bt.book(offerId, passengers, email, '', 'booking-attempt-abc123');
} catch (e) {
  if (e instanceof BoostedTravelError) {
    if (e.isRetryable) { /* retry after delay */ }
    else if (e.errorCategory === ErrorCategory.VALIDATION) { /* fix input */ }
    else { /* escalate to human */ }
  }
}
```

## Safety & Idempotency (For AI Agents)

This section documents the safety guarantees that make BoostedTravel safe for autonomous agents to use without human supervision of every call.

### Operation Safety Classification

| Operation | Side effects | Cost | Safe to retry | Idempotent |
|-----------|-------------|------|--------------|------------|
| `search_flights` | None (read-only) | Free | Yes | Yes |
| `resolve_location` | None (read-only) | Free | Yes | Yes |
| `get_agent_profile` | None (read-only) | Free | Yes | Yes |
| `setup_payment` | Updates payment method | Free | Yes | Yes (last write wins) |
| `unlock_flight_offer` | Charges $1, reserves offer | $1 | **No** — charges again | **No** |
| `book_flight` | Creates airline reservation | Free | **Only with idempotency_key** | **With key: yes** |

### Idempotency Keys (Preventing Double-Bookings)

LLMs and MCP clients (Claude, Cursor) may retry tool calls on timeout or error. Without protection, a retried `book_flight` could create a duplicate reservation.

**Always provide `idempotency_key` when booking:**

```python
import uuid

# Generate a deterministic key per booking attempt
key = f"{offer_id}-{passenger_name}-{datetime.utcnow().strftime('%Y%m%d')}"
# Or use a random UUID stored in agent memory
key = str(uuid.uuid4())

booking = bt.book(
    offer_id=unlocked.offer_id,
    passengers=[...],
    contact_email="john@example.com",
    idempotency_key=key,
)
```

**How it works:**
- First call with key `"abc123"` → creates booking, returns `BookingResult`
- Second call with same key `"abc123"` → returns the **same** `BookingResult` (no duplicate)
- Different key `"def456"` → creates a **new** booking

### The Quote-Before-Book Pattern

BoostedTravel enforces a mandatory "quote" step (unlock) before booking:

```
search_flights (free, read-only)
    ↓
unlock_flight_offer ($1, confirms live price)
    ↓  ← agent shows confirmed price to user, gets approval
book_flight (free, creates reservation)
```

**Why this matters for agents:**
1. Search prices are snapshots — the airline may have changed the price
2. The unlock step confirms the **actual current price** with the airline
3. If the confirmed price differs from the search price, the agent should inform the user
4. The user can decide whether to proceed at the new price or search again
5. The 30-minute reservation window prevents stale bookings

### Error Recovery Patterns

```python
def safe_book(bt, origin, dest, date, passengers, email, max_retries=2):
    """Book with automatic retry on transient errors and offer expiry."""
    idempotency_key = str(uuid.uuid4())

    for attempt in range(max_retries + 1):
        flights = bt.search(origin, dest, date)
        if not flights.offers:
            return None  # No flights available

        try:
            unlocked = bt.unlock(flights.cheapest.id)

            # Show price to user if it changed significantly
            # (agent should implement this check)

            return bt.book(
                offer_id=unlocked.offer_id,
                passengers=[{**p, "id": pid} for p, pid in zip(passengers, flights.passenger_ids)],
                contact_email=email,
                idempotency_key=idempotency_key,
            )
        except OfferExpiredError:
            if attempt < max_retries:
                continue  # Search again, fresh offers
            raise
        except BoostedTravelError as e:
            if e.is_retryable and attempt < max_retries:
                import time
                time.sleep(2 ** attempt)  # exponential backoff
                continue
            raise
```

## Complete Search-to-Booking Workflow

### Python — Full Workflow with Error Handling

```python
from boostedtravel import (
    BoostedTravel, BoostedTravelError,
    PaymentRequiredError, OfferExpiredError,
)

def search_and_book(origin_city, dest_city, date, passenger_info, email):
    bt = BoostedTravel()  # reads BOOSTEDTRAVEL_API_KEY

    # Step 1: Resolve locations
    origins = bt.resolve_location(origin_city)
    dests = bt.resolve_location(dest_city)
    if not origins or not dests:
        raise ValueError(f"Could not resolve: {origin_city} or {dest_city}")
    origin_iata = origins[0]["iata_code"]
    dest_iata = dests[0]["iata_code"]

    # Step 2: Search (free, unlimited)
    flights = bt.search(origin_iata, dest_iata, date, adults=len(passenger_info), sort="price")
    if not flights.offers:
        print(f"No flights {origin_iata} → {dest_iata} on {date}")
        return None

    print(f"Found {flights.total_results} offers, cheapest: {flights.cheapest.price} {flights.cheapest.currency}")

    # Step 3: Unlock ($1) — confirms price, reserves 30min
    try:
        unlocked = bt.unlock(flights.cheapest.id)
        print(f"Confirmed: {unlocked.confirmed_currency} {unlocked.confirmed_price}")
    except PaymentRequiredError:
        print("Setup payment first: boostedtravel setup-payment")
        return None
    except OfferExpiredError:
        print("Offer expired — search again")
        return None

    # Step 4: Book (free) — map passenger_info to passenger_ids
    passengers = [{**info, "id": pid} for info, pid in zip(passenger_info, flights.passenger_ids)]

    try:
        booking = bt.book(offer_id=unlocked.offer_id, passengers=passengers, contact_email=email)
        print(f"Booked! PNR: {booking.booking_reference}")
        return booking
    except OfferExpiredError:
        print("30-minute window expired — search and unlock again")
        return None
    except BoostedTravelError as e:
        print(f"Booking failed: {e.message}")
        return None

# Example: 2 passengers
search_and_book(
    "London", "Barcelona", "2026-04-01",
    passenger_info=[
        {"given_name": "John", "family_name": "Doe", "born_on": "1990-01-15", "gender": "m", "title": "mr"},
        {"given_name": "Jane", "family_name": "Doe", "born_on": "1992-03-20", "gender": "f", "title": "ms"},
    ],
    email="john.doe@example.com",
)
```

### Bash — CLI Workflow (Production)

```bash
#!/bin/bash
set -euo pipefail
export BOOSTEDTRAVEL_API_KEY=trav_...

# Step 1: Resolve locations (with validation)
ORIGIN=$(boostedtravel locations "London" --json | jq -r '.[0].iata_code')
DEST=$(boostedtravel locations "Barcelona" --json | jq -r '.[0].iata_code')

if [ -z "$ORIGIN" ] || [ -z "$DEST" ]; then
  echo "Error: Could not resolve locations" >&2
  exit 1
fi

# Step 2: Search
RESULTS=$(boostedtravel search "$ORIGIN" "$DEST" 2026-04-01 --adults 2 --json)
OFFER=$(echo "$RESULTS" | jq -r '.offers[0].id')
TOTAL=$(echo "$RESULTS" | jq '.total_results')

if [ "$OFFER" = "null" ] || [ -z "$OFFER" ]; then
  echo "No flights found $ORIGIN → $DEST" >&2
  exit 1
fi
echo "Found $TOTAL offers, best: $OFFER"

# Step 3: Unlock ($1) — with error check
if ! boostedtravel unlock "$OFFER" --json > /dev/null 2>&1; then
  echo "Unlock failed — check payment setup (boostedtravel setup-payment)" >&2
  exit 1
fi

# Step 4: Book (free after unlock)
boostedtravel book "$OFFER" \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --passenger '{"id":"pas_1","given_name":"Jane","family_name":"Doe","born_on":"1992-03-20","gender":"f","title":"ms"}' \
  --email john.doe@example.com
```

## Minimizing Unlock Costs (Price Aggregation)

Searching is **completely free** — only unlock ($1) costs money. Smart strategies:

### Search Wide, Unlock Narrow

```python
# Compare prices across multiple dates — all FREE
dates = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
best = None
for date in dates:
    result = bt.search("LON", "BCN", date)
    if result.offers and (best is None or result.cheapest.price < best[1].price):
        best = (date, result.cheapest)

# Only unlock the winner — $1
unlocked = bt.unlock(best[1].id)
```

### Filter Before Unlocking

```python
flights = bt.search("LHR", "JFK", "2026-06-01", limit=50)

# Apply all filters BEFORE paying $1
candidates = [
    o for o in flights.offers
    if o.outbound.stopovers == 0
    and o.outbound.total_duration_seconds < 10 * 3600
    and o.conditions.get("change_before_departure") != "not_allowed"
]

if candidates:
    best = min(candidates, key=lambda o: o.price)
    unlocked = bt.unlock(best.id)  # $1 only for the best match
```

### Use the 30-Minute Window

After unlock, the price is held for 30 minutes. Use this to present options to the user, verify details, and complete the booking without re-searching.

### Cost Summary

| Action | Cost | Notes |
|--------|------|-------|
| Search | FREE | Unlimited — any route, any date, any number of searches |
| Resolve location | FREE | Unlimited |
| View offer details | FREE | Price, airline, duration, conditions — all in search |
| Unlock | $1 | Confirms price, holds 30 minutes |
| Book | FREE | After unlock — real airline PNR |

## Rate Limits and Timeouts

The API has generous limits. Search is completely free and unlimited.

| Endpoint | Rate Limit | Typical Latency | Timeout |
|----------|-----------|-----------------|----------|
| Search | 60 req/min per agent | 2-15s (depends on airline APIs) | 30s |
| Resolve location | 120 req/min per agent | <1s | 5s |
| Unlock | 20 req/min per agent | 2-5s | 15s |
| Book | 10 req/min per agent | 3-10s | 30s |

**Rate limit handling:**

```python
import time
from boostedtravel import BoostedTravel, BoostedTravelError

def search_with_retry(bt, origin, dest, date, max_retries=3):
    """Retry with exponential backoff on rate limit or timeout."""
    for attempt in range(max_retries):
        try:
            return bt.search(origin, dest, date)
        except BoostedTravelError as e:
            if "rate limit" in str(e).lower() or "429" in str(e):
                wait = 2 ** attempt  # 1s, 2s, 4s
                time.sleep(wait)
            elif "timeout" in str(e).lower() or "504" in str(e):
                time.sleep(1)  # brief pause then retry
            else:
                raise
    raise BoostedTravelError("Max retries exceeded")
```

## Building an Autonomous AI Agent

### Recommended Architecture

```
User request → Parse intent → Resolve locations → Search (free)
  → Filter & rank → Present options → Unlock best ($1) → Collect passenger details → Book (free)
```

### Best Practices

1. **Resolve locations first.** "London" = 5+ airports. Use `resolve_location()` to get IATA codes.
2. **Search liberally.** It's free. Search multiple dates, cabin classes, and airport combinations.
3. **Filter before unlocking.** Apply all preferences (airline, stops, duration, conditions) on free search results.
4. **Manage the 30-minute window.** Unlock → collect passenger details → book. If window expires, search+unlock again ($1 more).
5. **Handle price changes.** Unlock confirms the real-time airline price. It may differ slightly from search. Inform the user.
6. **Map passenger IDs.** Search returns `passenger_ids` (e.g., `["pas_0", "pas_1"]`). Each booking passenger must include the correct `id`.
7. **Use REAL details.** Airlines send e-tickets to the contact email. Names must match passport/ID.

### Retry Logic for Expired Offers

```python
from boostedtravel import (
    BoostedTravel, BoostedTravelError,
    PaymentRequiredError, OfferExpiredError,
)

def resilient_book(bt, origin, dest, date, passengers, email, max_retries=2):
    for attempt in range(max_retries + 1):
        flights = bt.search(origin, dest, date, adults=len(passengers))
        if not flights.offers:
            return None
        try:
            unlocked = bt.unlock(flights.cheapest.id)
            booking = bt.book(
                offer_id=unlocked.offer_id,
                passengers=[{**p, "id": pid} for p, pid in zip(passengers, flights.passenger_ids)],
                contact_email=email,
            )
            return booking
        except OfferExpiredError:
            if attempt < max_retries:
                continue  # search again, get fresh offers
            raise
        except PaymentRequiredError:
            raise  # can't retry this — need payment setup

def find_cheapest_date(bt, origin, dest, dates):
    """Search multiple dates (free) and return the best one."""
    best = None
    for date in dates:
        try:
            result = bt.search(origin, dest, date)
            if result.offers and (best is None or result.cheapest.price < best[1].price):
                best = (date, result.cheapest, result.passenger_ids)
        except BoostedTravelError:
            continue
    return best
```

### Advanced Preference Evaluation

Instead of always picking the cheapest, score offers by weighted criteria:

```python
def score_offer(offer, weights=None):
    """Score a flight (lower = better). Weights sum to 1.0."""
    w = weights or {"price": 0.4, "duration": 0.3, "stops": 0.2, "airline": 0.1}
    preferred = {"British Airways", "Delta", "United", "Lufthansa", "KLM"}
    
    price_norm = offer.price / 2000
    dur_norm = (offer.outbound.total_duration_seconds / 3600) / 24
    stops_norm = offer.outbound.stopovers / 3
    airline_norm = 0 if any(a in preferred for a in offer.airlines) else 1
    
    return (w["price"] * price_norm + w["duration"] * dur_norm +
            w["stops"] * stops_norm + w["airline"] * airline_norm)

# Usage
flights = bt.search("LHR", "JFK", "2026-06-01", limit=50)
best = min(flights.offers, key=lambda o: score_offer(o, {
    "price": 0.3, "duration": 0.4, "stops": 0.2, "airline": 0.1
}))
```

Adjust weights based on user preferences:
- Business traveler: `{"duration": 0.5, "stops": 0.3, "price": 0.1, "airline": 0.1}`
- Budget traveler: `{"price": 0.7, "stops": 0.15, "duration": 0.1, "airline": 0.05}`
- Comfort traveler: `{"stops": 0.4, "duration": 0.3, "airline": 0.2, "price": 0.1}`

### Data Persistence for Price Tracking

For agents that track prices over time or compare across sessions:

```python
import json
from datetime import datetime
from pathlib import Path

CACHE_FILE = Path("flight_price_history.json")

def save_search_result(origin, dest, date, result):
    """Append search result to price history."""
    history = json.loads(CACHE_FILE.read_text()) if CACHE_FILE.exists() else {}
    key = f"{origin}-{dest}-{date}"
    history.setdefault(key, []).append({
        "searched_at": datetime.utcnow().isoformat(),
        "cheapest_price": result.cheapest.price if result.offers else None,
        "total_offers": result.total_results,
    })
    CACHE_FILE.write_text(json.dumps(history, indent=2))

def get_price_trend(origin, dest, date):
    """Check if prices are rising or falling."""
    history = json.loads(CACHE_FILE.read_text()) if CACHE_FILE.exists() else {}
    prices = [e["cheapest_price"] for e in history.get(f"{origin}-{dest}-{date}", []) if e["cheapest_price"]]
    if len(prices) < 2:
        return "insufficient_data"
    return f"{'falling' if prices[-1] < prices[0] else 'rising'} (${prices[0]} → ${prices[-1]})"
```

### Scheduling Repeated Searches

For autonomous price monitoring agents:

```python
import time

def monitor_prices(bt, route_configs, interval_minutes=60, max_checks=24):
    """Periodically search routes and track price trends.
    
    route_configs: [{"origin": "LON", "dest": "BCN", "date": "2026-06-01"}, ...]
    """
    for check in range(max_checks):
        for route in route_configs:
            result = bt.search(route["origin"], route["dest"], route["date"])
            save_search_result(route["origin"], route["dest"], route["date"], result)
            trend = get_price_trend(route["origin"], route["dest"], route["date"])
            if result.offers:
                print(f"{route['origin']}→{route['dest']} {route['date']}: "
                      f"${result.cheapest.price} ({trend})")
        time.sleep(interval_minutes * 60)
```

### Complete Autonomous Agent Example

End-to-end implementation of an AI agent that autonomously searches, evaluates, and books flights based on user preferences while managing costs and edge cases:

```python
from boostedtravel import (
    BoostedTravel, BoostedTravelError,
    AuthenticationError, PaymentRequiredError, OfferExpiredError,
)
import time

class FlightAgent:
    """Autonomous flight booking agent with preference evaluation and cost management."""
    
    def __init__(self, api_key=None):
        self.bt = BoostedTravel(api_key=api_key)
    
    def resolve_city(self, city_name):
        """Resolve city name to IATA code, handling ambiguity."""
        locations = self.bt.resolve_location(city_name)
        if not locations:
            raise ValueError(f"Unknown city: {city_name}")
        # Prefer city code (covers all airports) over single airport
        for loc in locations:
            if loc.get("type") == "city":
                return loc["iata_code"]
        return locations[0]["iata_code"]
    
    def evaluate_offers(self, offers, preferences):
        """Score and rank offers by user preferences. Lower score = better.
        
        preferences: {"price": 0.4, "duration": 0.3, "stops": 0.2, "airline": 0.1}
        """
        preferred_airlines = preferences.get("preferred_airlines", set())
        weights = {
            "price": preferences.get("price", 0.4),
            "duration": preferences.get("duration", 0.3),
            "stops": preferences.get("stops", 0.2),
            "airline": preferences.get("airline", 0.1),
        }
        
        scored = []
        for offer in offers:
            price_norm = offer.price / 2000
            dur_norm = (offer.outbound.total_duration_seconds / 3600) / 24
            stops_norm = offer.outbound.stopovers / 3
            airline_norm = 0 if any(a in preferred_airlines for a in offer.airlines) else 1
            
            score = (weights["price"] * price_norm + weights["duration"] * dur_norm +
                     weights["stops"] * stops_norm + weights["airline"] * airline_norm)
            scored.append((score, offer))
        
        return sorted(scored, key=lambda x: x[0])
    
    def search_and_book(self, origin_city, dest_city, date, passengers, email,
                        preferences=None, max_retries=2):
        """Full autonomous workflow: resolve → search → evaluate → unlock → book.
        
        Returns booking result or None if no suitable flights found.
        """
        # Step 1: Resolve locations (free)
        origin = self.resolve_city(origin_city)
        dest = self.resolve_city(dest_city)
        
        for attempt in range(max_retries + 1):
            # Step 2: Search (free, unlimited)
            flights = self.bt.search(origin, dest, date, adults=len(passengers))
            if not flights.offers:
                return None
            
            # Step 3: Evaluate by preferences (not just cheapest)
            if preferences:
                ranked = self.evaluate_offers(flights.offers, preferences)
                best_offer = ranked[0][1]  # highest-scored offer
            else:
                best_offer = flights.cheapest
            
            # Step 4: Unlock ($1) — confirms live price with airline
            try:
                unlocked = self.bt.unlock(best_offer.id)
                
                # Check if confirmed price differs significantly from search
                price_diff = abs(unlocked.confirmed_price - best_offer.price)
                if price_diff > best_offer.price * 0.1:  # >10% price change
                    print(f"Warning: Price changed from {best_offer.price} to {unlocked.confirmed_price}")
                
            except OfferExpiredError:
                if attempt < max_retries:
                    time.sleep(1)
                    continue  # Search again for fresh offers
                raise
            except PaymentRequiredError:
                raise  # Can't retry — need payment setup
            
            # Step 5: Book (free after unlock) — map passenger IDs
            try:
                mapped_passengers = [
                    {**p, "id": pid}
                    for p, pid in zip(passengers, flights.passenger_ids)
                ]
                booking = self.bt.book(
                    offer_id=unlocked.offer_id,
                    passengers=mapped_passengers,
                    contact_email=email,
                )
                return booking
            except OfferExpiredError:
                if attempt < max_retries:
                    continue  # 30-min window expired, retry full flow
                raise

# Usage
agent = FlightAgent()

booking = agent.search_and_book(
    origin_city="London",
    dest_city="New York",
    date="2026-06-15",
    passengers=[
        {"given_name": "John", "family_name": "Doe", "born_on": "1990-01-15",
         "gender": "m", "title": "mr"},
    ],
    email="john@example.com",
    preferences={
        "price": 0.3, "duration": 0.4, "stops": 0.2, "airline": 0.1,
        "preferred_airlines": {"British Airways", "Delta"},
    },
)

if booking:
    print(f"Booked! PNR: {booking.booking_reference}")
```

## Get an API Key

```bash
curl -X POST https://api.boostedchat.com/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "you@example.com"}'
```

## API Discovery

| Endpoint | URL |
|----------|-----|
| OpenAPI/Swagger | https://api.boostedchat.com/docs |
| Agent discovery | https://api.boostedchat.com/.well-known/ai-plugin.json |
| Agent manifest | https://api.boostedchat.com/.well-known/agent.json |
| LLM instructions | https://api.boostedchat.com/llms.txt |

## Links

- **PyPI:** https://pypi.org/project/boostedtravel/
- **npm (JS SDK):** https://www.npmjs.com/package/boostedtravel
- **npm (MCP):** https://www.npmjs.com/package/boostedtravel-mcp
