# CLI Reference

> ### ⚠️ Agents: do not create a Developer API account
>
> Do **NOT** call `/developers/api/v1/agents/register` or
> `/developers/api/v1/agents/setup-payment`, and do **NOT** run
> `letsfg register` or `letsfg setup-payment`. Those belong to a separate,
> paid, prepaid-balance product for high-volume commercial integrations and
> create a billing account you almost certainly do not want.
>
> To search and book flights, connect LetsFG as an MCP server at
> <https://letsfg.co/developers/api/mcp>. The consent step opens
> <https://letsfg.co/connect>, where a card is saved (nothing is charged).
> See <https://letsfg.co/for-agents>.

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
| `letsfg register` | **[Developer API only]** Create a Developer API account + API key. Most agents want the card-backed token from the connect flow instead |
| `letsfg recover --email <email>` | Recover lost API key via email verification |
| `letsfg auth` | Connect a card at `letsfg.co/connect` and store the token. Registers itself as an OAuth client (PKCE + loopback redirect), opens a browser for a person to approve, writes `~/.letsfg/config.json`. `--no-browser` prints the URL. The old Stripe setup was retired 2026-09-02 and its tokens revoked |
| `letsfg search <origin> <dest> <date>` | Search flights via the letsfg.co server-side engine (free with Bearer token) |
| `letsfg locations <query>` | Resolve city/airport to IATA codes |
| `letsfg unlock <offer_id>` | **RETIRED 2026-09-08.** The server answers `410 Gone`; there is no unlock step on either lane. Book directly — the fare is held, not taken, and captured only against a real PNR |
| `letsfg book <offer_id>` | Book the flight. No unlock step on either lane. The fare is HELD on the connected card and captured only against a real PNR; the command returns the started booking's reference to poll (4–11 min). PFS polls `POST /api/agent-book/status`; a Developer API key polls `GET /flights/bookings/{booking_id}` |
| `letsfg setup-payment` | **RETIRED 2026-09-08 with Stripe.** The server answers `410 Gone`. Connect a Revolut method instead: `POST /agents/connect-payment` returns a one-time `connect_url` to open in a browser; nothing is charged to connect |
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

### Basic Search

```bash
# One-way London to New York
letsfg search LHR JFK 2026-04-15

# Round-trip with cabin class
letsfg search LON BCN 2026-04-01 --return 2026-04-08 --cabin M --sort price

# Direct flights only, JSON output
letsfg search LON BCN 2026-04-01 --max-stops 0 --json
```

Search runs server-side at letsfg.co. With a card-backed token in `LETSFG_BEARER_TOKEN`, search is free: 10 per 10 minutes, 30 per hour, 100 per day per card.

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
export LETSFG_API_KEY=letsfg_...

# Resolve locations
ORIGIN=$(letsfg locations "London" --json | jq -r '.[0].iata_code')
DEST=$(letsfg locations "Barcelona" --json | jq -r '.[0].iata_code')

# Search
RESULTS=$(letsfg search "$ORIGIN" "$DEST" 2026-04-01 --adults 2 --json)
OFFER_ID=$(echo "$RESULTS" | jq -r '.offers[0].id')
echo "Best offer: $OFFER_ID"

# Book (on a Bearer token: holds the fare on the connected card, returns booking_ref;
# poll POST /api/agent-book/status every 20-30 s until completed / failed)
letsfg book "$OFFER_ID" \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --passenger '{"id":"pas_1","given_name":"Jane","family_name":"Doe","born_on":"1992-03-20","gender":"f","title":"ms"}' \
  --email john.doe@example.com
```

Unlock pricing and payment mechanics follow the live public contract in Swagger.

!!! warning "Real Passenger Details Required"
    Airlines send e-tickets to the contact email. Names must match the passenger's passport or government ID. Never use placeholder data.

### Account Recovery

Lost your API key? Recover it via email verification:

```bash
# Step 1: Request a recovery code (sent to your registered email)
letsfg recover --email you@example.com

# Step 2: Enter the 6-digit code from your email
letsfg recover --email you@example.com --code 123456
```

The code expires in 15 minutes. Once verified, a new API key is issued and your previous key is invalidated.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `LETSFG_BEARER_TOKEN` | Card-backed token from the connect flow (PFS search + booking) |
| `LETSFG_API_KEY` | Developer API key for look-to-book search and booking |
| `LETSFG_BASE_URL` | API URL override (default: `https://letsfg.co/developers`) |
