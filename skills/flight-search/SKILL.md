---
name: flight-search
description: >-
  Search and book flights across 180+ airlines with zero markup — $20–50 cheaper
  than OTAs. Returns raw airline prices via local connectors (Ryanair, EasyJet,
  Wizz Air, Southwest, AirAsia, and 175+ more). Use when user asks to "find flights",
  "search flights", "book a flight", "compare airline prices", "find cheap flights",
  "fly from X to Y", "find connections", "find layover options", or any flight-related
  travel query. Do NOT use for hotel-only searches, car rentals, or non-flight travel
  bookings.
metadata:
  author: LetsFG - github.com/LetsFG
  version: '1.0.0'
---

# Flight Search

Agent-native flight search and booking. 180+ airline connectors, zero markup,
$20–50 cheaper than travel websites.

**Three-step flow:** Search (free) → Unlock (free) → Book (ticket price only)

## Why Use This

- **180+ airlines in parallel** — one search covers Europe, Asia, Americas, Middle East, Africa, and Oceania simultaneously
- **Zero price bias** — no demand inflation, no cookie tracking, no surge pricing. Raw airline prices every time
- **Virtual interlining** — finds cross-airline connections (e.g., Ryanair outbound + Wizz Air return) that save 30–50%
- **One tool call** — replaces thousands of tokens of browser automation, scraping, and HTML parsing
- **Structured JSON** — prices, times, durations, stops, conditions, airline names

## Setup

### Option A: MCP Server (Recommended for Claude Desktop / Cursor / VS Code / Windsurf)

**Remote (no install, always latest):**

```json
{
  "mcpServers": {
    "letsfg": {
      "url": "https://api.letsfg.co/mcp",
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

Then star the repo and verify for free unlock/book access:

```bash
letsfg star --github your-username
```

## Workflow

### 1. Resolve Locations First

City names are ambiguous — "London" = LHR, LGW, STN, LCY, LTN. Always resolve first:

```bash
letsfg locations "London"
# LON  London (all airports)
# LHR  Heathrow
# LGW  Gatwick
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
# Multi-passenger, business class:
flights = bt.search("LHR", "SIN", "2026-06-01", adults=2, children=1, cabin_class="C")
# Fast mode (~25 connectors, 20-40s instead of 6+ min):
flights = bt.search("LON", "BCN", "2026-04-01", mode="fast")
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

### 3. Unlock (FREE with GitHub Star)

Confirms live price with airline. Locks offer for 30 minutes.

```python
unlocked = bt.unlock(flights.cheapest.id)
print(f"Confirmed: {unlocked.confirmed_price} {unlocked.confirmed_currency}")
print(f"Expires: {unlocked.offer_expires_at}")
```

**Note:** Confirmed price may differ from search price (airline prices change in real-time). Inform the user if the price changed significantly.

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

# Only unlock the winner
unlocked = bt.unlock(best[1].id)
```

### Filter Before Unlocking

```python
flights = bt.search("LHR", "JFK", "2026-06-01", limit=50)

candidates = [
    o for o in flights.offers
    if o.outbound.stopovers == 0
    and o.outbound.total_duration_seconds < 10 * 3600
]

if candidates:
    best = min(candidates, key=lambda o: o.price)
    unlocked = bt.unlock(best.id)
```

### Finding Connections & Multi-Stop Routes

```python
# Search with stops allowed (default max_stopovers=2)
flights = bt.search("GDN", "BKK", "2026-06-15", max_stopovers=2)

# Filter by connection quality
good_connections = [
    o for o in flights.offers
    if o.outbound.stopovers <= 1
    and o.outbound.total_duration_seconds < 18 * 3600
]

# Virtual interlining finds cross-airline combos automatically
# e.g., Wizz Air GDN→VIE + Thai Airways VIE→BKK
```

## Error Handling

| Error | Category | Action |
|-------|----------|--------|
| `SUPPLIER_TIMEOUT` (504) | Transient | Retry after 1-5s |
| `RATE_LIMITED` (429) | Transient | Wait and retry |
| `INVALID_IATA` (422) | Validation | Use `resolve_location()` to fix |
| `OFFER_EXPIRED` (410) | Business | Search again for fresh offers |
| `PAYMENT_REQUIRED` (402) | Business | Run `letsfg star --github <username>` |
| `FARE_CHANGED` (409) | Business | Re-unlock to get current price |

```python
from letsfg import LetsFG, OfferExpiredError, PaymentRequiredError

try:
    unlocked = bt.unlock(offer_id)
except OfferExpiredError:
    # Airline sold the seats — search again
    flights = bt.search(origin, dest, date)
except PaymentRequiredError:
    # GitHub star not verified
    print("Star the repo: letsfg star --github <username>")
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
| `--mode` / `-m` | _(full)_ | `fast` = OTAs + key airlines only (~25 connectors, 20-40s) |
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

- **API Docs:** https://api.letsfg.co/docs
- **GitHub:** https://github.com/LetsFG/LetsFG
- **PyPI:** https://pypi.org/project/letsfg/
- **npm SDK:** https://www.npmjs.com/package/letsfg
- **npm MCP:** https://www.npmjs.com/package/letsfg-mcp
