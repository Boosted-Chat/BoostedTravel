# CLI Reference

The `boostedtravel` CLI is available via both Python and JavaScript. Same commands, same interface.

## Install

=== "Python (recommended)"

    ```bash
    pip install boostedtravel
    ```

=== "JavaScript / TypeScript"

    ```bash
    npm install -g boostedtravel
    ```

## Commands

| Command | Description |
|---------|-------------|
| `boostedtravel register` | Create account and get API key |
| `boostedtravel search <origin> <dest> <date>` | Search flights (free, requires API key) |
| `boostedtravel search-local <origin> <dest> <date>` | Search flights locally (free, **no API key**) |
| `boostedtravel locations <query>` | Resolve city/airport to IATA codes |
| `boostedtravel unlock <offer_id>` | Unlock offer details ($1) |
| `boostedtravel book <offer_id>` | Book the flight (free after unlock) |
| `boostedtravel setup-payment` | Set up Stripe payment method |
| `boostedtravel me` | View profile & usage stats |

All commands accept `--json` for structured output and `--api-key` to override the environment variable.

## Search Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--return` | `-r` | _(one-way)_ | Return date for round-trip (YYYY-MM-DD) |
| `--adults` | `-a` | `1` | Number of adult passengers (1–9) |
| `--children` | | `0` | Number of children (2–11 years) |
| `--cabin` | `-c` | _(any)_ | Cabin class: `M` economy, `W` premium, `C` business, `F` first |
| `--max-stops` | `-s` | `2` | Maximum stopovers per direction (0–4) |
| `--currency` | | `EUR` | 3-letter currency code |
| `--limit` | `-l` | `20` | Maximum number of results (1–100) |
| `--sort` | | `price` | Sort by `price` or `duration` |
| `--json` | `-j` | | Output raw JSON (for agents/scripts) |

## Cabin Class Codes

| Code | Class | Typical Use |
|------|-------|-------------|
| `M` | Economy | Standard seating, cheapest fares |
| `W` | Premium Economy | Extra legroom, better meals, priority boarding |
| `C` | Business | Lie-flat seats on long-haul, lounge access |
| `F` | First | Top-tier service, suites on some airlines |

If omitted, the search returns all cabin classes.

## Examples

### Local Search (No API Key)

```bash
# Search 73 local airline connectors — completely free, no registration
boostedtravel search-local LHR JFK 2026-04-15

# With cabin class and JSON output
boostedtravel search-local LON BCN 2026-04-01 --cabin M --json
```

Local search queries 73 airline websites directly. No API key needed — install and search immediately.

### Basic Search

```bash
# One-way London to New York
boostedtravel search LHR JFK 2026-04-15

# Round-trip with cabin class
boostedtravel search LON BCN 2026-04-01 --return 2026-04-08 --cabin M --sort price
```

### Multi-Passenger

```bash
# Family: 2 adults + 2 children, economy
boostedtravel search LHR BCN 2026-07-15 --return 2026-07-22 --adults 2 --children 2 --cabin M

# Business trip: 3 adults, business class, direct only
boostedtravel search JFK LHR 2026-05-01 --adults 3 --cabin C --max-stops 0

# Solo first class, sorted by duration
boostedtravel search LAX NRT 2026-08-10 --return 2026-08-24 --cabin F --sort duration
```

!!! info "Passenger IDs"
    When searching with multiple passengers, the response includes `passenger_ids` (e.g., `["pas_0", "pas_1"]`). You must provide details for **each** ID when booking.

### JSON Output

```bash
# Pipe to jq for filtering
boostedtravel search LON BCN 2026-04-01 --json | jq '[.offers[] | select(.stopovers == 0)]'

# Shortest flight
boostedtravel search LON BCN 2026-04-01 --json | jq '.offers | sort_by(.duration_seconds) | .[0]'
```

### Location Resolution

```bash
boostedtravel locations "New York"
# JFK  John F. Kennedy International Airport
# LGA  LaGuardia Airport
# EWR  Newark Liberty International Airport
# NYC  New York (all airports)
```

### Full Booking Flow

```bash
#!/bin/bash
set -euo pipefail
export BOOSTEDTRAVEL_API_KEY=trav_...

# Resolve locations
ORIGIN=$(boostedtravel locations "London" --json | jq -r '.[0].iata_code')
DEST=$(boostedtravel locations "Barcelona" --json | jq -r '.[0].iata_code')

# Search
RESULTS=$(boostedtravel search "$ORIGIN" "$DEST" 2026-04-01 --adults 2 --json)
OFFER_ID=$(echo "$RESULTS" | jq -r '.offers[0].id')
echo "Best offer: $OFFER_ID"

# Unlock ($1)
boostedtravel unlock "$OFFER_ID"

# Book
boostedtravel book "$OFFER_ID" \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --passenger '{"id":"pas_1","given_name":"Jane","family_name":"Doe","born_on":"1992-03-20","gender":"f","title":"ms"}' \
  --email john.doe@example.com
```

!!! warning "Real Passenger Details Required"
    Airlines send e-tickets to the contact email. Names must match the passenger's passport or government ID. Never use placeholder data.
