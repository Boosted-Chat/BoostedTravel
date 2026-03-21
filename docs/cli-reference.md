# CLI Reference

The `letsfg` CLI is available via both Python and JavaScript. Same commands, same interface.

## Install

=== "Python (recommended)"

    ```bash
    pip install letsfg
    ```

=== "JavaScript / TypeScript"

    ```bash
    npm install -g letsfg
    ```

## Commands

| Command | Description |
|---------|-------------|
| `letsfg register` | Create account and get API key |
| `letsfg search <origin> <dest> <date>` | Search flights (free, requires API key) |
| `letsfg search-local <origin> <dest> <date>` | Search flights locally (free, **no API key**) |
| `letsfg system-info` | Show system resources & concurrency tier |
| `letsfg locations <query>` | Resolve city/airport to IATA codes |
| `letsfg unlock <offer_id>` | Unlock offer details (free) |
| `letsfg book <offer_id>` | Book the flight (free after unlock) |
| `letsfg setup-payment` | Set up Stripe payment method |
| `letsfg me` | View profile & usage stats |

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
| `--max-browsers` | `-b` | _(auto)_ | Max concurrent browsers for local search (1–32) |
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
# Search 150+ local airline connectors — completely free, no registration
letsfg search-local LHR JFK 2026-04-15

# With cabin class and JSON output
letsfg search-local LON BCN 2026-04-01 --cabin M --json

# Limit browser concurrency for constrained environments
letsfg search-local LHR BCN 2026-04-15 --max-browsers 4
```

Local search queries 150+ airline websites directly. No API key needed — install and search immediately.

### Basic Search

```bash
# One-way London to New York
letsfg search LHR JFK 2026-04-15

# Round-trip with cabin class
letsfg search LON BCN 2026-04-01 --return 2026-04-08 --cabin M --sort price
```

### Multi-Passenger

```bash
# Family: 2 adults + 2 children, economy
letsfg search LHR BCN 2026-07-15 --return 2026-07-22 --adults 2 --children 2 --cabin M

# Business trip: 3 adults, business class, direct only
letsfg search JFK LHR 2026-05-01 --adults 3 --cabin C --max-stops 0

# Solo first class, sorted by duration
letsfg search LAX NRT 2026-08-10 --return 2026-08-24 --cabin F --sort duration
```

!!! info "Passenger IDs"
    When searching with multiple passengers, the response includes `passenger_ids` (e.g., `["pas_0", "pas_1"]`). You must provide details for **each** ID when booking.

### JSON Output

```bash
# Pipe to jq for filtering
letsfg search LON BCN 2026-04-01 --json | jq '[.offers[] | select(.stopovers == 0)]'

# Shortest flight
letsfg search LON BCN 2026-04-01 --json | jq '.offers | sort_by(.duration_seconds) | .[0]'
```

### Location Resolution

```bash
letsfg locations "New York"
# JFK  John F. Kennedy International Airport
# LGA  LaGuardia Airport
# EWR  Newark Liberty International Airport
# NYC  New York (all airports)
```

### Full Booking Flow

```bash
#!/bin/bash
set -euo pipefail
export LETSFG_API_KEY=trav_...

# Resolve locations
ORIGIN=$(letsfg locations "London" --json | jq -r '.[0].iata_code')
DEST=$(letsfg locations "Barcelona" --json | jq -r '.[0].iata_code')

# Search
RESULTS=$(letsfg search "$ORIGIN" "$DEST" 2026-04-01 --adults 2 --json)
OFFER_ID=$(echo "$RESULTS" | jq -r '.offers[0].id')
echo "Best offer: $OFFER_ID"

# Unlock (free)
letsfg unlock "$OFFER_ID"

# Book
letsfg book "$OFFER_ID" \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --passenger '{"id":"pas_1","given_name":"Jane","family_name":"Doe","born_on":"1992-03-20","gender":"f","title":"ms"}' \
  --email john.doe@example.com
```

!!! warning "Real Passenger Details Required"
    Airlines send e-tickets to the contact email. Names must match the passenger's passport or government ID. Never use placeholder data.

### System Info

```bash
# Show system resources and concurrency tier
letsfg system-info

# Machine-readable output
letsfg system-info --json
```

Output includes platform, CPU cores, total/available RAM, tier name, recommended max browsers, and current setting.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `LETSFG_API_KEY` | Your agent API key (for cloud search, unlock, book) |
| `LETSFG_BASE_URL` | API URL override (default: `https://api.letsfg.co`) |
| `LETSFG_MAX_BROWSERS` | Max concurrent browser instances for local search (1–32). Auto-detected from RAM if not set. |
| `LETSFG_BROWSER_VISIBLE` | Set to `1` to show browser windows for debugging |
