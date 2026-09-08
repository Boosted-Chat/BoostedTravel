---
name: letsfg
description: >-
  Agent-native flight and hotel search and booking via the letsfg.co server-side engine. Returns raw
  prices from airlines and the major booking sites, with per-flight reliability history. Use when user asks to
  "find flights", "search flights", "book a flight", "compare airline prices",
  "find cheap flights", "fly from X to Y", or any flight-related travel query.
  Hotels need a card on file for search as well as booking; either credential
  reaches them. Do NOT use for car rentals or non-flight, non-hotel travel bookings.
license: MIT
metadata:
  author: LetsFG - github.com/LetsFG
  version: '1.0.0'
---

# LetsFG

Agent-native flight search and booking. Hundreds of airlines plus the major booking sites,
Compares airlines and the major booking sites in one pass, with per-flight reliability history.

**Three-step flow:** Search (free) → Unlock (free) → Book (ticket price only)

## Why Use This

- **Hundreds of airlines in parallel** — Ryanair, EasyJet, Wizz Air, Southwest, AirAsia, Norwegian, Qantas, LATAM, Spirit, Frontier, IndiGo, VietJet, and more
- **Zero price bias** — no demand inflation, no cookie tracking, no surge pricing. Raw airline prices every time
- **One tool call** — replaces thousands of tokens of browser automation, scraping, and HTML parsing
- **Structured JSON** — prices, times, durations, stops, conditions, airline names

## Setup

### Option A: MCP Server (Recommended for Claude Desktop / Cursor / VS Code)

**Remote (no install):**

```json
{
  "mcpServers": {
    "letsfg": {
      "url": "https://letsfg.co/developers/api/mcp",
      "headers": {
        "X-API-Key": "trav_your_api_key"
      }
    }
  }
}
```

**Local (stdio):**

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

### Option B: CLI

```bash
pip install letsfg
letsfg search LHR BCN 2026-06-15
```

### Option C: Python SDK

```python
from letsfg import LetsFG
bt = LetsFG(api_key="trav_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
```

### Get an API Key (Free)

```bash
letsfg register --name my-agent --email agent@example.com
```

Then connect a payment method (nothing is charged; there is no unlock step):

```bash
letsfg connect-payment   # prints a link to open in a browser
```

## Workflow

### 1. Resolve Locations First

City names are ambiguous — "London" = LHR, LGW, STN, LCY, LTN. Always resolve first:

```bash
letsfg locations "London"
# LON  London (all airports)
# LHR  Heathrow
# LGW  Gatwick
# ...
```

```python
locations = bt.resolve_location("London")
# Use city code "LON" for all airports, or specific airport "LHR"
```

### 2. Search (FREE, Unlimited)

```python
flights = bt.search("LON", "BCN", "2026-04-01")
# Round trip:
flights = bt.search("LON", "BCN", "2026-04-01", return_date="2026-04-08")
# Multi-passenger:
flights = bt.search("LHR", "SIN", "2026-06-01", adults=2, children=1, cabin_class="C")
```

```bash
letsfg search LON BCN 2026-04-01 --return 2026-04-08 --sort price --json
```

Search returns structured offers:

```json
{
  "passenger_ids": ["pas_0"],
  "total_results": 47,
  "offers": [{
    "id": "off_xxx",
    "price": 89.50,
    "currency": "EUR",
    "airlines": ["Ryanair"],
    "route": "STN → BCN",
    "duration_seconds": 7800,
    "stopovers": 0,
    "conditions": {
      "refund_before_departure": "not_allowed",
      "change_before_departure": "allowed_with_fee"
    }
  }]
}
```

### 3. Unlock — RETIRED 2026-09-08

There is no unlock step on either lane any more, and the route answers `410 Gone`. Unlock existed
to confirm a live price before charging; booking now HOLDS the fare and captures only against a
real airline PNR, so a moved price surfaces as a question to accept or decline rather than a
surprise charge. Call `book_flight` (PFS) or `POST /flights/book` (Developer API) directly.

```python
# unlock() raises LetsFGError(410) locally — it does not call the server. Book instead:
booking = bt.book_and_wait(
    offer_id=flights.cheapest.id,
    search_id=flights.search_id,
    passengers=[{"given_name": "John", "family_name": "Doe", "born_on": "1990-01-15"}],
    contact_email="you@example.com",
)
print(booking)
```

```bash
letsfg unlock off_xxx
```

**Note:** Confirmed price may differ from search price (airline prices change in real-time). Inform the user if price changed significantly.

### 4. Book (Ticket Price Only)

```python
booking = bt.book(
    offer_id=unlocked.offer_id,
    passengers=[{
        "id": flights.passenger_ids[0],
        "given_name": "John",
        "family_name": "Doe",
        "born_on": "1990-01-15",
        "gender": "m",
        "title": "mr",
        "email": "john@example.com"
    }],
    contact_email="john@example.com",
    idempotency_key="unique-booking-key-123"
)
print(f"Booked! PNR: {booking.booking_reference}")
```

## Critical Rules

1. **Use REAL passenger details** — airlines send e-tickets to the contact email. Names must match passport/ID exactly. Never use placeholder or fake data.
2. **Always provide `idempotency_key` when booking** — prevents duplicate reservations if the agent retries on timeout.
3. **Resolve locations before searching** — "New York" = JFK, LGA, EWR, NYC. Use `resolve_location()` first.
4. **Search is free** — search as many routes, dates, and cabin classes as needed.
5. **Map passenger IDs** — search returns `passenger_ids`. Each booking passenger must include the correct `id`.

## Best Practices

### Search Wide, Unlock Narrow

```python
# Compare multiple dates (all FREE)
dates = ["2026-04-01", "2026-04-02", "2026-04-03"]
best = None
for date in dates:
    result = bt.search("LON", "BCN", date)
    if result.offers and (best is None or result.cheapest.price < best[1].price):
        best = (date, result.cheapest)

# Book only the winner — there is no unlock step
booking = bt.book(best[1].id, passengers=[{...}], contact_email="you@example.com",
                  search_id=best[1].search_id)
```

### Filter Before Booking

```python
flights = bt.search("LHR", "JFK", "2026-06-01", limit=50)

candidates = [
    o for o in flights.offers
    if o.outbound.stopovers == 0
    and o.outbound.total_duration_seconds < 10 * 3600
]

if candidates:
    best = min(candidates, key=lambda o: o.price)
    booking = bt.book(best.id, passengers=[{...}], contact_email="you@example.com",
                      search_id=flights.search_id)
```

## Error Handling

| Error | Category | Action |
|-------|----------|--------|
| `SUPPLIER_TIMEOUT` (504) | Transient | Retry after 1-5s |
| `RATE_LIMITED` (429) | Transient | Wait and retry |
| `INVALID_IATA` (422) | Validation | Use `resolve_location()` to fix |
| `OFFER_EXPIRED` (410) | Business | Search again for fresh offers |
| `PAYMENT_REQUIRED` (402) | Business | Connect a card: `letsfg connect-payment` prints a link (or pay via MPP on the 402 challenge) |
| `FARE_CHANGED` (409) | Business | The fare moved — answer the `price_change` question to accept or decline it |

```python
from letsfg import LetsFG, OfferExpiredError, PaymentRequiredError

try:
    booking = bt.book(offer_id, passengers=[{...}], contact_email="you@example.com",
                      search_id=search_id)
except OfferExpiredError:
    # Airline sold the seats — search again and book from the fresh results
    flights = bt.search(origin, dest, date)
except PaymentRequiredError:
    # No card on file — connect one (or pay via MPP crypto on the 402 challenge)
    print("Connect a card:", bt.connect_payment()["connect_url"])
```

## Search Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--return` / `-r` | _(one-way)_ | Return date (YYYY-MM-DD) |
| `--adults` / `-a` | `1` | Number of adults (1–9) |
| `--children` | `0` | Children (2–11 years) |
| `--cabin` / `-c` | _(any)_ | `M` economy, `W` premium, `C` business, `F` first |
| `--max-stops` / `-s` | `2` | Max stopovers (0–4) |
| `--currency` | `EUR` | Currency code |
| `--limit` / `-l` | `20` | Max results (1–100) |
| `--sort` | `price` | `price` or `duration` |
| `--json` / `-j` | | JSON output |

## Safety

| Operation | Cost | Safe to Retry | Idempotent |
|-----------|------|---------------|------------|
| `search` | Free | Yes | Yes |
| `resolve_location` | Free | Yes | Yes |
| `unlock` | Free | No — may charge fee | No |
| `book` | Ticket price | Only with `idempotency_key` | With key: yes |

## Reference Files

Load only when needed:

| File | Load When |
|------|-----------|
| [api-reference.md](references/api-reference.md) | Need full API endpoint details, request/response schemas |
| [mcp-setup.md](references/mcp-setup.md) | Setting up MCP server for specific clients |

## Links

- **API Docs:** https://letsfg.co/developers/api/docs
- **GitHub:** https://github.com/LetsFG/LetsFG
- **PyPI:** https://pypi.org/project/letsfg/
- **npm SDK:** https://www.npmjs.com/package/letsfg
- **npm MCP:** https://www.npmjs.com/package/letsfg-mcp
