# AGENTS.md — LetsFG for AI Agents

> This file is for AI agents. If you're a human, see [README.md](README.md).

## ⚠️ Read this first

**Do NOT call `/developers/api/v1/agents/register`.**
**Do NOT call `/developers/api/v1/agents/setup-payment`.**
**Do NOT run `letsfg register` or `letsfg setup-payment`.**

Those belong to a **separate, paid, prepaid-balance product** for high-volume
commercial integrations, and they create a billing account you almost certainly
do not want. Earlier versions of this file pointed there by default; that was
wrong, and agents were creating accounts nobody needed.

If you are an agent searching or booking flights — even hundreds of them — you
want **Programmatic Flight Search (PFS)**: connect LetsFG as an MCP server at
`https://letsfg.co/developers/api/mcp`, approve it (the consent step saves a card
at <https://letsfg.co/connect> — nothing charged), then search and book. No
billing account, nothing charged beyond the ticket price itself. Full guide:
<https://letsfg.co/for-agents>.

## What is LetsFG?

LetsFG is the largest agent-native flight search and booking toolkit. It gives your agent access to a server-side search engine covering hundreds of airlines — production-grade, maintained by LetsFG, not scrapers you have to keep alive. Built for OpenClaw, Perplexity Computer, Claude, Cursor, Windsurf, and any MCP-compatible agent.

**Why this exists:** Building a flight search integration from scratch takes 30+ minutes, burns thousands of tokens on browser automation, and still only covers one airline at a time. LetsFG replaces all of that with a single function call that triggers hundreds of airline connectors in parallel on our servers, scanning the entire world for the best deals — including airlines your agent has never heard of.

### How it works:

**Server-side search engine** — Ryanair, EasyJet, Wizz Air, Southwest, AirAsia, Norwegian, Qantas, LATAM, Spirit, Frontier, IndiGo, VietJet, and hundreds more. The connectors run server-side at letsfg.co. Connect once (a card saved at letsfg.co/connect — nothing is charged), then search and book: the fare is held on that card, a LetsFG booking agent buys the ticket, and you get a real airline PNR.

### What makes this different from flight websites:

- **Hundreds of airlines in parallel** — one search covers Europe, Asia, Americas, Middle East, Africa, and Oceania simultaneously. Discovers deals on carriers like Jazeera Airways, FlySafair, Lucky Air, 9 Air, and Flybondi that most agents wouldn't know to check.
- **Virtual interlining** — the server-side combo engine builds cross-airline round-trips (e.g., Ryanair outbound + Wizz Air return) that save 30-50% vs booking with either airline alone.
- **Token-efficient** — one tool call replaces what would take thousands of tokens of browser automation, scraping setup, and HTML parsing. Results come back as structured JSON.
- **Works with any agent** — OpenClaw, Perplexity Computer, Claude Desktop, Cursor, Windsurf, or any MCP-compatible client. Also available as Python SDK, JS SDK, and CLI.

**API Base URL:** `https://letsfg.co` (PFS) · `https://letsfg.co/developers/api/v1` (Developer API)

## Which access mode should I use?

| Mode | Best for | Speed | Cost |
|------|----------|-------|------|
| **CLI / SDK / MCP** (PFS payment token) | **Almost every agent.** Search + booking | 8–10 s to first results; longer to `completed`, longer again on a split | Free auth, free search, no LetsFG fee on booking |
| **Developer API** ([letsfg.co/developers](https://letsfg.co/developers)) | Business / commercial / high-volume, and the only path to hotels | 2–5 s (discover) · 8–10 s to first results (full search) | Look-to-book: 200 searches free after every booking, then $0.01. Booking through `POST /flights/book`, no booking fee, no transaction fee |

**Quick decision:**
- You are an agent that wants to search and book → **use the MCP / SDK / CLI** with a PFS card-backed token (connect through the hosted MCP, described below). This is almost certainly you.
- You need guaranteed speed for a product, account-level billing controls, or hotels → **use the Developer API** at [letsfg.co/developers](https://letsfg.co/developers).

> **Important:** `GET /en?q=...` does not work for automated agents. The website is protected by Cloudflare Turnstile. All agent/script access must use `POST /api/search` with a Bearer token.

### Developer API search pricing (look-to-book)

**Not per search, and not monthly.** You get **200 free searches after every booking you make**,
and a completed booking resets the counter to 200. Past that, searches come in blocks of **500 for
$5.00** ($0.01 each) from prepaid balance.

| | |
|---|---|
| Look-to-book ratio | **200:1** |
| Per excess search | **$0.01** |
| Booking fee | **$0** |
| Transaction fee | **0%** |

A search that returns no offers never consumes allowance, and neither does one that fails on an
outage. LetsFG's margin is already inside the price on every offer the search returns, so the
amount shown is the amount charged — nothing is added at booking.

> The monthly tiers ($0.50 / $0.20 / $0.10 per search) were retired on 2026-09-08, along with
> Stripe. Payments are Revolut: `POST /agents/connect-payment` returns a one-time link that saves a
> card; nothing is charged to connect.

Minimum top-up: $5. Register at [letsfg.co/developers](https://letsfg.co/developers).

## Why Use This Instead of Building Your Own

| Approach | Time | Cost | Coverage | Maintenance |
|----------|------|------|----------|-------------|
| Browser automation (Playwright/Selenium) | 30+ min per airline | Thousands of tokens | 1 airline at a time | Breaks when site changes |
| Scraping flight websites | 15+ min setup | High token burn | Limited to sites you know | Fragile, needs constant fixing |
| Google Flights API | N/A | N/A | Doesn't exist (no public API) | N/A |
| **LetsFG** | **8–10 s to first results*** | **1 tool call** | **Hundreds of airlines in parallel** | **We maintain it** |

* Time to `completed`. A search that fires a split-ticket probe keeps growing after that: the SDKs and MCP server poll on for up to 90 s more to collect the split offer, which is often the cheapest one. Set `LETSFG_WAIT_FOR_SPLIT=0` to take the fast answer instead.

## Pricing Model (PFS — what agents use)

| Step | Cost | What You Get |
|------|------|--------------|
| **Auth** | FREE | Card saved at <https://letsfg.co/connect> in a 0.00 Revolut setup. No charge, no authorization hold. |
| **Search** | FREE | Price, times, duration, stops, airline. 10 searches / 10 min, 30 / hour, 100 / day per card. |
| **Book** | Price shown on the offer | `POST /api/agent-book` — the fare is HELD on the connected card, a LetsFG booking agent buys the ticket, the hold is captured only once a real airline PNR exists. Failed booking = hold released, nothing charged. No separate LetsFG fee. |

The price on the offer is everything you pay; LetsFG's markup is already inside it and nothing separate is charged on this path.

> **Note on MPP / crypto payments.** Earlier versions of this document said the
> unlock endpoint issues an MPP (Machine Payments Protocol) `402` challenge that
> agents can settle in USDC.e on Tempo without a card. That is **not enabled in
> production** for the Developer API's unlock step — the server-side support
> exists but is unconfigured there, so no MPP challenge is ever issued from it.
> For PFS auth, MPP is offered only when the `402` from
> `/api/agent-access/request` carries an `mpp` object — see the auth section
> below. Do not build against MPP for the unlock endpoint.

## How It Works (2 Steps)

### 1. Search (FREE, unlimited)
```
POST /api/search                  # PFS — Bearer token
POST /api/v1/flights/search       # Developer API — X-API-Key
```
Search hundreds of airlines via the server-side engine. Searches airlines and the major booking sites together. Free; 10 searches per 10 minutes, 30 per hour, 100 per day per card (polling results never counts).

**CLI (PFS — free):**
```bash
export LETSFG_BEARER_TOKEN=eyJ...      # the card-backed token from the connect flow (see PFS auth below)
letsfg search GDN BCN 2026-06-15       # prints a search_id, needed for book
```

**Python SDK (PFS Bearer token — free):**
```python
from letsfg.local import search_local
import asyncio

result = asyncio.run(search_local("GDN", "BCN", "2026-06-15"))
print(result["search_id"], len(result["offers"]))
```

**cURL (PFS):**
```bash
curl -X POST https://letsfg.co/api/search \
  -H "Authorization: Bearer <your_token>" \
  -H "Content-Type: application/json" \
  -d '{"origin":"GDN","destination":"BCN","date_from":"2026-06-15"}'
# → {"search_id": "abc123"}
# Poll immediately, then every 2s:
curl https://letsfg.co/api/results/abc123 -H "Authorization: Bearer <your_token>"
```

### 2. Book (price shown on the offer — no separate LetsFG fee)
```
POST /api/agent-book               # PFS — Bearer token, no unlock step; starts the booking
POST /api/agent-book/status        # PFS — poll until completed / failed / needs_attention
POST /api/v1/flights/book          # Developer API — no unlock step, no fee
POST /api/v1/bookings/book         # Developer API only
```

On a PFS Bearer token, `search` → `book` → `poll` is the whole flow. There is no
unlock step. Booking works exactly like the website checkout, on the connected
card: the fare plus LetsFG's markup is **held** on the card (not taken), a LetsFG
booking agent buys the ticket from the seller, and the hold is captured only once
a real airline PNR exists. If the booking fails the hold is released and nothing
is charged. It works for every offer in the results, whichever seller it came from.

`POST /api/agent-book` returns within seconds with a `booking_ref`; the booking
itself takes **4–11 minutes**. Poll `POST /api/agent-book/status` with that
`booking_ref` every 20–30 s. One traveller per call — a group trip is one call
per person.

```bash
letsfg book ws_off_xxx --search-id ws_abc \
  --passenger '{"given_name":"Ada","family_name":"Lovelace","born_on":"1990-04-01","gender":"f","nationality":"GB","phone_number":"+15551234567","phone_country":"US","address_line1":"1 Analytical Way","address_city":"London","address_postal":"N1 9GU","address_country":"GB"}' \
  --email traveller@example.com
# → booking_ref eyJ...  (state: booking_in_progress, held: 93 EUR on visa ending 5709)
```

```python
from letsfg.local import book_offer
import asyncio, os, time, requests

token = os.environ["LETSFG_BEARER_TOKEN"]   # card-backed token from the connect flow
started = asyncio.run(book_offer(
    search_id="ws_abc",
    offer_id="ws_off_xxx",
    passenger={
        "given_name": "Ada", "family_name": "Lovelace",
        "born_on": "1990-04-01", "gender": "f", "nationality": "GB",
        "phone_number": "+15551234567", "phone_country": "US",
        "address_line1": "1 Analytical Way", "address_city": "London",
        "address_postal": "N1 9GU", "address_country": "GB",
    },
    contact_email="traveller@example.com",
))
if "missing_fields" in started:
    print("Ask for:", started["missing_fields"])        # nothing charged
else:
    ref = started["booking_ref"]                        # hold placed, agent dispatched
    while True:                                         # 4–11 minutes, poll every 20–30 s
        time.sleep(25)
        st = requests.post("https://letsfg.co/api/agent-book/status",
                           headers={"Authorization": f"Bearer {token}"},
                           json={"booking_ref": ref}).json()
        if st["state"] == "completed":
            print("PNR:", st["pnr"], st["charged_amount"], st["currency"]); break
        if st["state"] == "failed":
            print("Not booked, hold released:", st["failure_reason"]); break
        if st["state"] == "needs_attention":
            print("A human at LetsFG is checking it — do not book again"); break
```

Start response (seconds):

```json
{"ok": true, "booked": false, "state": "booking_in_progress",
 "booking_id": "agt_1788...", "booking_ref": "eyJ...",
 "held": {"amount": 93, "currency": "EUR", "card": "visa ending 5709"}, "charged": 0}
```

Status walks `booking_in_progress` → `completed` (`pnr`, `charged_amount`,
`currency`) | `failed` (`failure_reason`; the hold was released, nothing charged)
| `needs_attention` (a human at LetsFG is checking it — do **not** book again; the
traveller will be emailed). Only `completed` with a PNR means booked. Never start
a second booking for the same trip while one is in progress — that places a
second hold.

Other answers, all with `"charged": 0`: `{"error": "missing_details",
"missing_fields": [...]}` — ask the person and call again;
`{"error": "payment_method_required", "add_card_url": "https://letsfg.co/connect"}`;
`{"error": "payment_declined", "add_card_url": "https://letsfg.co/connect"}`.

**Developer API alternative (paid, unlock required first):** if you're on the
prepaid Developer API instead of a PFS Bearer token, `book` requires a prior
`unlock` (legacy) which confirms the live price and reveals
the offer for booking:

```bash
letsfg unlock off_xxx --api-key letsfg_...
# Output: Confirmed price: EUR 189.50, Fee: $3.00
letsfg book off_xxx --api-key letsfg_... --passenger '{"id":"pas_xxx",...}' --email you@example.com
```

```python
from letsfg import LetsFG

bt = LetsFG(api_key="letsfg_...")
flights = bt.search("LHR", "JFK", "2026-06-01")
unlocked = bt.unlock(flights.cheapest.id)
booked = bt.book(unlocked.offer_id, passengers=[...], contact_email="you@example.com")
```

## Installation & CLI Usage

### Install (Python — recommended for agents)
```bash
pip install letsfg
```

This gives you the `letsfg` CLI command. It reads the card-backed PFS token from
`LETSFG_BEARER_TOKEN` (or `~/.letsfg/config.json`). Get the token by connecting
through the hosted MCP — see [PFS auth](#pfs-auth--connect-a-card-at-letsfgcoconnect-cli--sdk--direct-api)
below. `letsfg auth` runs that same connect flow from the terminal and stores
the token for you.

```bash
export LETSFG_BEARER_TOKEN=eyJ...   # card-backed token from the connect flow

# Search flights — completely free after auth, prints a search_id
letsfg search LHR BCN 2026-06-15

# Round trip
letsfg search LON BCN 2026-04-01 --return 2026-04-08 --sort price

# Multi-passenger: 2 adults + 1 child, business class
letsfg search LHR SIN 2026-06-01 --adults 2 --children 1 --cabin C

# Direct flights only
letsfg search JFK LHR 2026-05-01 --max-stops 0

# Resolve city to IATA codes
letsfg locations "New York"

# Book an offer from your search (free — ticket price only, no LetsFG fee)
letsfg book off_xxx --search-id srch_abc --passenger '{"given_name":"Ada","family_name":"Lovelace","born_on":"1990-04-01","gender":"f"}' --email you@example.com
```

All commands support `--json` for structured output:
```bash
letsfg search GDN BER 2026-03-03 --json
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
| `--sort` | | `price` | `price`, `duration`, or `departure_time` |
| `--departure-from` | | _(none)_ | Earliest departure time `HH:MM` (e.g. `06:00`) |
| `--departure-to` | | _(none)_ | Latest departure time `HH:MM` (e.g. `14:00`) |
| `--json` | `-j` | | JSON output for machine consumption |

### Python SDK (PFS — free, recommended)
```python
from letsfg.local import search_local, book_offer
import asyncio

result = asyncio.run(search_local("LHR", "JFK", "2026-04-15"))
cheapest = min(result["offers"], key=lambda o: o["price"])
print(f'{result["total_results"]} offers, cheapest: {cheapest["price"]} {cheapest["currency"]}')
```

The `LetsFG` client class (`from letsfg import LetsFG`) wraps the Developer API
instead — use it only if you have a Developer API key (`api_key="letsfg_..."`).

### JavaScript/TypeScript SDK + CLI
```bash
npm install -g letsfg
```

```typescript
import { LetsFG } from 'letsfg';

// PFS (free) — the card-backed Bearer token from the connect flow
const bt = new LetsFG({ bearerToken: 'eyJ...' });
const flights = await bt.search('LHR', 'JFK', '2026-04-15');
console.log(`${flights.total_results} offers, search_id: ${flights.search_id}`);

const result = await bt.book(
  cheapestOfferId, [passenger], 'you@example.com', '', '', flights.search_id,
);
```

### MCP Server (claude.ai / Claude Code / ChatGPT / Cursor / Windsurf)

**Option A: Remote (Streamable HTTP) — no install, always latest, this is the way in**

`https://letsfg.co/developers/api/mcp`. The client runs OAuth; the consent step
opens <https://letsfg.co/connect> where the person saves a card (0.00, nothing
charged). The token the client receives is card-backed and can search and book.

```bash
# Claude Code
claude mcp add --transport http letsfg https://letsfg.co/developers/api/mcp
```

```json
// Cursor (mcp.json); Windsurf uses "serverUrl" instead of "url"
{
  "mcpServers": {
    "letsfg": { "url": "https://letsfg.co/developers/api/mcp" }
  }
}
```

claude.ai and ChatGPT: add a custom connector with that URL and approve it.

Developer API accounts can use the same endpoint with a header instead of OAuth:
`"headers": {"X-API-Key": "letsfg_your_api_key"}`.

**Option B: Local (stdio) — runs on your machine, needs a token you already hold**
```bash
npx letsfg-mcp
```

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_BEARER_TOKEN": "eyJ... (card-backed token from the connect flow)"
      }
    }
  }
}
```

The local server's `authenticate` tool returns the current connect instructions
(`add_card_url`, `how`) rather than minting a token; run `letsfg auth`, or
connect through Option A or Option B. Developer API users running the
local server can set `LETSFG_API_KEY` instead of `LETSFG_BEARER_TOKEN` —
`book_flight` dispatches automatically based on which one is present.

## CLI Commands

| Command | Description | Cost |
|---------|-------------|------|
| `letsfg auth` | Connect a card at letsfg.co/connect and store the token — self-registers, PKCE + loopback redirect. `--no-browser` prints the URL | Free |
| `letsfg search <origin> <dest> <date>` | Search flights, prints `search_id` | Free |
| `letsfg locations <query>` | Resolve city/airport to IATA | Free |
| `letsfg book <offer_id> --search-id <id>` | Start a booking: holds the fare on the connected card, prints a `booking_ref` to poll | Price shown on the offer |
| `letsfg me` | View profile & usage | Free |
| `letsfg register` | **[Paid Developer API only — most agents should not run this]** Creates a billing account | Free |
| `letsfg setup-payment` | **[Paid Developer API only — agents connect at letsfg.co/connect instead]** | Free |
| `letsfg unlock <offer_id> --api-key <key>` | **[Developer API only]** Confirm price, required before `book` on that path. Legacy | — |
| `letsfg recover --email <email>` | Recover lost Developer API key via email | Free |

## Developer API Authentication (paid, only if you need it)

Everything in this section is for the separate, prepaid Developer API. If you
just want to search and book, skip to [PFS auth](#pfs-auth--connect-a-card-at-letsfgcoconnect-cli--sdk--direct-api) below — you don't need any of this.

Every authenticated Developer API request requires the `X-API-Key` header.

```bash
# CLI
letsfg register --name my-agent --email agent@example.com

# cURL
curl -X POST https://letsfg.co/developers/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "agent@example.com"}'

# Response: { "agent_id": "ag_xxx", "api_key": "letsfg_xxxxx..." }
```

```bash
# Environment variable (recommended)
export LETSFG_API_KEY=letsfg_...
letsfg search LHR JFK 2026-04-15 --api-key $LETSFG_API_KEY

# cURL (raw HTTP)
curl -X POST https://letsfg.co/developers/api/v1/flights/search \
  -H "X-API-Key: letsfg_..." \
  -H "Content-Type: application/json" \
  -d '{"origin": "LHR", "destination": "JFK", "date_from": "2026-04-15"}'
```

```python
from letsfg import LetsFG

bt = LetsFG(api_key="letsfg_...")  # or LetsFG() to read LETSFG_API_KEY
creds = LetsFG.register("my-agent", "agent@example.com")  # register inline
```

```bash
# Connect a card once at letsfg.co/connect — nothing is charged to connect
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
letsfg locations "New York"
# JFK  John F. Kennedy International Airport
# LGA  LaGuardia Airport
# EWR  Newark Liberty International Airport
# NYC  New York (all airports)
```

## Working with Search Results

Search returns offers from multiple airlines with full details — all for free:

```python
result = asyncio.run(search_local("LON", "BCN", "2026-04-01", return_date="2026-04-08", limit=50))
offers = result["offers"]

for offer in offers:
    print(f"{offer['owner_airline']}: {offer['currency']} {offer['price']}")
    print(f"  Route: {offer['outbound']['route_str']}")
    print(f"  Stops: {offer['outbound']['stopovers']}")

# Filter: direct flights only
direct = [o for o in offers if o["outbound"]["stopovers"] == 0]

# Filter: specific airline
ba = [o for o in offers if "British Airways" in o["airlines"]]

# Filter: Starlink Wi-Fi. `starlink` is absent when unknown -- absent does NOT
# mean the flight lacks Wi-Fi. Only "confirmed_*" is a fact you may state
# outright; "likely_*" means the airline is fitting that aircraft type but has
# not finished, and anything ending "_some" has at least one leg without it.
starlink_certain = [o for o in offers if o.get("starlink") == "confirmed_all"]
starlink_any     = [o for o in offers if o.get("starlink")]

# Sort by price
by_price = sorted(offers, key=lambda o: o["price"])
cheapest = by_price[0]
print(f"Best: {cheapest['price']} {cheapest['currency']} on {cheapest['owner_airline']}")
```

### JSON Output Structure (CLI)

```bash
letsfg search LON BCN 2026-04-01 --adults 2 --json
```

```json
{
  "search_id": "srch_abc123",
  "total_results": 47,
  "offers": [
    {
      "id": "off_xxx",
      "price": 89.50,
      "currency": "EUR",
      "airlines": ["Ryanair"],
      "owner_airline": "Ryanair",
      "outbound": {
        "route_str": "STN → BCN",
        "total_duration_seconds": 7800,
        "stopovers": 0
      },
      "conditions": {
        "refund_before_departure": "not_allowed",
        "change_before_departure": "allowed_with_fee"
      }
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
| `UNSUPPORTED_ROUTE` | validation | 422 | No providers serve this route |
| `MISSING_PARAMETER` | validation | 422 | Required field missing |
| `INVALID_PARAMETER` | validation | 422 | Field value out of range or wrong type |
| `AUTH_INVALID` | business | 401 | Bearer token / API key missing or invalid |
| `PAYMENT_REQUIRED` | business | 402 | No card on file. PFS: response includes `add_card_url` (https://letsfg.co/connect) — connect the card there. Developer API: `letsfg setup-payment`. |
| `OFFER_NOT_FOUND` | business | 404 | Offer expired (~15 min after search) or unknown `offer_id`/`search_id` — search again |
| `PAYMENT_DECLINED` | business | 402 | Card refused. PFS: nothing charged, `add_card_url` points at letsfg.co/connect. Developer API: the unlock charge failed — check card details |
| `FARE_CHANGED` | business | 409 | Price changed since search (Developer API) — re-unlock |

### Using Error Codes in Agent Logic

```python
from letsfg.connectors.auth import BearerTokenError
from letsfg.local import search_local, book_offer
import asyncio

try:
    result = asyncio.run(search_local("LHR", "JFK", "2026-04-15"))
    cheapest = min(result["offers"], key=lambda o: o["price"])
    booked = asyncio.run(book_offer(
        search_id=result["search_id"], offer_id=cheapest["id"],
        passenger={...}, contact_email="you@example.com",
    ))
    if "missing_fields" in booked:
        print("Ask for:", booked["missing_fields"])  # nothing charged
    else:
        print("Started, poll /api/agent-book/status with:", booked["booking_ref"])
except BearerTokenError as e:
    print("Re-connect at https://letsfg.co/connect:", e)
```

```typescript
// JavaScript/TypeScript
import { LetsFG, LetsFGError } from 'letsfg';

const bt = new LetsFG({ bearerToken: process.env.LETSFG_BEARER_TOKEN });
try {
  const result = await bt.book(offerId, [passenger], email, '', '', searchId);
} catch (e) {
  if (e instanceof LetsFGError) { /* escalate to human, or retry if e.isRetryable */ }
}
```

## Safety & Idempotency (For AI Agents)

This section documents the safety guarantees that make LetsFG safe for autonomous agents to use without human supervision of every call.

### Operation Safety Classification

| Operation | Side effects | Cost | Safe to retry | Idempotent |
|-----------|-------------|------|--------------|------------|
| `search_flights` | None (read-only) | Free | Yes | Yes |
| `resolve_location` | None (read-only) | Free | Yes | Yes |
| `get_agent_profile` | None (read-only) | Free | Yes | Yes |
| `book_offer` (PFS) | Holds the fare and starts a real booking | Price shown on the offer | **No** — a second call places a second hold | **No** — poll `/api/agent-book/status` instead |
| `setup_payment` (Developer API) | Updates payment method | Free | Yes | Yes (last write wins) |
| `unlock` | **RETIRED 2026-09-08** — answers `410 Gone`, there is no unlock step and no fee | — | — | — |

### Don't Double-Book

LLMs and MCP clients (Claude, Cursor) may retry tool calls on timeout or error. Without protection, a retried `book` call could attempt to book the same offer twice.

**Cache the result and reuse it — do not re-book the same offer:**

```python
# Good: check if already booked before calling again
if not cached_result:
    cached_result = asyncio.run(book_offer(search_id=sid, offer_id=oid, passenger=p, contact_email=email))

# then poll POST /api/agent-book/status with cached_result["booking_ref"] —
# never call book_offer again for the same trip while one is in progress
```

### The Search-Book Pattern

```
search_flights (free, read-only)
    ↓
  Filter & rank by preference (no cost)
    ↓
book_offer (price shown on the offer — fare held on the connected card, no separate LetsFG fee)
    ↓
poll /api/agent-book/status every 20–30 s (4–11 minutes)
    ↓
  completed (PNR) | failed (hold released, nothing charged) | needs_attention
```

**Why this matters for agents:**
1. Search prices are snapshots — the airline may have changed the price by the time you book
2. Offers expire ~15 minutes after search — book promptly, or search again if it's been a while
3. Only `completed` with a PNR means booked; `failed` released the hold. Never start a second booking while one is in progress

## Complete Search-to-Book Workflow

### Python — Full Workflow with Error Handling

```python
from letsfg.connectors.auth import BearerTokenError
from letsfg.local import search_local, book_offer
import asyncio

def search_and_book(origin_iata, dest_iata, date, passenger, contact_email):
    # Step 1: Search (free, unlimited)
    result = asyncio.run(search_local(origin_iata, dest_iata, date, sort="price"))
    if not result["offers"]:
        print(f"No flights {origin_iata} → {dest_iata} on {date}")
        return None

    cheapest = min(result["offers"], key=lambda o: o["price"])
    print(f"Found {result['total_results']} offers, cheapest: {cheapest['price']} {cheapest['currency']}")

    # Step 2: Book (free — ticket price only, no LetsFG fee)
    try:
        booked = asyncio.run(book_offer(
            search_id=result["search_id"],
            offer_id=cheapest["id"],
            passenger=passenger,
            contact_email=contact_email,
        ))
    except BearerTokenError:
        print("Token expired or revoked — reconnect at https://letsfg.co/connect")
        return None

    if "missing_fields" in booked:
        print("Nothing charged. Ask the traveller for:", booked["missing_fields"])
        return None
    print(f"Started: booking_ref {booked['booking_ref']}, held {booked['held']}")
    return booked  # poll POST /api/agent-book/status until completed / failed / needs_attention

# Example
search_and_book(
    "LON", "BCN", "2026-04-01",
    passenger={"given_name": "Ada", "family_name": "Lovelace", "born_on": "1990-04-01", "gender": "f"},
    contact_email="traveller@example.com",
)
```

## Book Best Practices

Searching is free. Booking goes through `POST /api/agent-book`: the price shown
on the offer is held on the connected card and captured only once a real PNR
exists — no separate LetsFG fee. The retired unlock fee (1% of ticket, min $3) applies
only on the paid Developer API.

### Search Wide, Book Once

```python
# Compare prices across multiple dates — all FREE
dates = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
best = None
for date in dates:
    result = asyncio.run(search_local("LON", "BCN", date))
    if result["offers"]:
        cheapest = min(result["offers"], key=lambda o: o["price"])
        if best is None or cheapest["price"] < best[1]["price"]:
            best = (result["search_id"], cheapest)

# Book only the winner
booked = asyncio.run(book_offer(search_id=best[0], offer_id=best[1]["id"], passenger=p, contact_email=email))
```

### Filter Before Booking

```python
result = asyncio.run(search_local("LHR", "JFK", "2026-06-01", limit=50))

# Apply all filters BEFORE booking
candidates = [
    o for o in result["offers"]
    if o["outbound"]["stopovers"] == 0
    and o["outbound"]["total_duration_seconds"] < 10 * 3600
    and o["conditions"].get("change_before_departure") != "not_allowed"
]

if candidates:
    best = min(candidates, key=lambda o: o["price"])
    booked = asyncio.run(book_offer(search_id=result["search_id"], offer_id=best["id"], passenger=p, contact_email=email))
```

### Cost Summary

| Action | Cost | Notes |
|--------|------|-------|
| Search | FREE | Unlimited — any route, any date, any number of searches |
| Resolve location | FREE | Unlimited |
| View offer details | FREE | Price, airline, duration, conditions — all in search |
| Auth | FREE | Card saved at letsfg.co/connect (0.00 Revolut setup): no charge, no hold. The MPP lane, when offered, costs $0.01 once as verification. |
| Book | Price shown on the offer | `POST /api/agent-book` holds the fare, a LetsFG agent buys the ticket, captured only on a real PNR. Poll `/api/agent-book/status`. No separate LetsFG fee. |
| Unlock | — | **Developer API only, legacy.** Not part of the agent flow — there is no unlock step on a PFS Bearer token. |

## Rate Limits and Timeouts

The API has generous limits. Search is completely free and unlimited.

| Endpoint | Rate Limit | Typical Latency | Notes |
|----------|-----------|-----------------|-------|
| Search (PFS / Dev API full) | 60 req/min per agent | 8–10 s to first results | Async: POST returns `search_id` instantly, poll `/results/<id>` every 2 s; keep polling while `split_ticket_pending` is true |
| Search (Dev API discover) | 60 req/min per agent | 2–5 s | Synchronous, up to 20 destinations |
| Resolve location | 120 req/min per agent | <1 s | |
| Book (PFS) | 20 req/min per agent | up to 60 s | |
| Unlock (Dev API) | 20 req/min per agent | 2–5 s | |

**Rate limit handling:**

```python
import time
from letsfg.local import search_local
import asyncio

def search_with_retry(origin, dest, date, max_retries=3):
    """Retry with exponential backoff on rate limit or timeout."""
    for attempt in range(max_retries):
        try:
            return asyncio.run(search_local(origin, dest, date))
        except Exception as e:
            if "rate limit" in str(e).lower() or "429" in str(e):
                time.sleep(2 ** attempt)  # 1s, 2s, 4s
            elif "timeout" in str(e).lower() or "504" in str(e):
                time.sleep(1)
            else:
                raise
    raise RuntimeError("Max retries exceeded")
```

## Building an Autonomous AI Agent

### Recommended Architecture

```
User request → Parse intent → Resolve locations → Search (free)
  → Filter & rank → Book the best offer (fare held on the connected card)
  → Poll every 20–30 s → PNR (completed) | failed (hold released) | needs_attention
```

### Best Practices

1. **Resolve locations first.** "London" = 5+ airports. Use `resolve_location()` to get IATA codes.
2. **Search liberally.** It's free. Search multiple dates, cabin classes, and airport combinations.
3. **Filter before booking.** Apply all preferences (airline, stops, duration, conditions) on free search results, then book the winner.
4. **Book promptly.** Offers expire ~15 minutes after search — book while it's still fresh, or search again.
5. **Poll, don't re-book.** `book` returns a `booking_ref` in seconds; the booking takes 4–11 minutes. Poll `/api/agent-book/status` every 20–30 s. `failed` means the hold was released and nothing was charged; `needs_attention` means a human at LetsFG is on it — do not book again.
6. **Don't double-book.** Cache the `booking_ref` of a `book` call and reuse it. A second call places a second hold.

### Retry Logic for Expired Offers

```python
from letsfg.connectors.auth import BearerTokenError
from letsfg.local import search_local, book_offer
import asyncio

def resilient_book(origin, dest, date, passenger, contact_email, max_retries=2):
    for attempt in range(max_retries + 1):
        result = asyncio.run(search_local(origin, dest, date))
        if not result["offers"]:
            return None
        cheapest = min(result["offers"], key=lambda o: o["price"])
        try:
            return asyncio.run(book_offer(
                search_id=result["search_id"], offer_id=cheapest["id"],
                passenger=passenger, contact_email=contact_email,
            ))
        except BearerTokenError:
            raise  # can't retry this — reconnect at https://letsfg.co/connect

def find_cheapest_date(origin, dest, dates):
    """Search multiple dates (free) and return the best one."""
    best = None
    for date in dates:
        result = asyncio.run(search_local(origin, dest, date))
        if result["offers"]:
            cheapest = min(result["offers"], key=lambda o: o["price"])
            if best is None or cheapest["price"] < best[1]["price"]:
                best = (date, cheapest, result["search_id"])
    return best
```

### Advanced Preference Evaluation

Instead of always picking the cheapest, use the **open-source LetsFG ranking engine** — the exact same algorithm that runs at letsfg.co. It scores offers across 9 dimensions (price, stops, duration, departure time, arrival time, baggage, savings, comfort hours, layover quality) and selects the best offer using 12 weight profiles that adapt to trip context and purpose.

**JavaScript/TypeScript (npm: `letsfg`):**
```typescript
import { rankOffers } from 'letsfg'

const { ranked } = rankOffers(flights.offers, {
  tripPurpose: 'business',  // or 'honeymoon', 'beach', 'city_break', etc.
  wantsDirectFlight: true,
  requiresBag: true,
})

const best = ranked[0]
console.log(`Best: ${best.price} ${best.currency} — score: ${best._score.total}`)
```

The ranking source is in `sdk/js/src/ranking.ts` — inspect and fork it. For Python agents that want simple weighted scoring without the full JS engine:

```python
def score_offer(offer, weights=None):
    """Simple weighted score (lower = better). For the full 9-dimension engine, use the JS SDK."""
    w = weights or {"price": 0.4, "duration": 0.3, "stops": 0.2}
    price_norm = offer["price"] / 2000
    dur_norm = (offer["outbound"]["total_duration_seconds"] / 3600) / 24
    stops_norm = offer["outbound"]["stopovers"] / 3
    return w["price"] * price_norm + w["duration"] * dur_norm + w["stops"] * stops_norm

result = asyncio.run(search_local("LHR", "JFK", "2026-06-01", limit=50))
best = min(result["offers"], key=score_offer)
```

Adjust weights based on user preferences:
- Business traveler: `{"duration": 0.5, "stops": 0.3, "price": 0.2}`
- Budget traveler: `{"price": 0.7, "stops": 0.15, "duration": 0.15}`
- Comfort traveler: `{"stops": 0.4, "duration": 0.35, "price": 0.25}`

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
    cheapest = min(result["offers"], key=lambda o: o["price"]) if result["offers"] else None
    history.setdefault(key, []).append({
        "searched_at": datetime.utcnow().isoformat(),
        "cheapest_price": cheapest["price"] if cheapest else None,
        "total_offers": result["total_results"],
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

def monitor_prices(route_configs, interval_minutes=60, max_checks=24):
    """Periodically search routes and track price trends.

    route_configs: [{"origin": "LON", "dest": "BCN", "date": "2026-06-01"}, ...]
    """
    for check in range(max_checks):
        for route in route_configs:
            result = asyncio.run(search_local(route["origin"], route["dest"], route["date"]))
            save_search_result(route["origin"], route["dest"], route["date"], result)
            trend = get_price_trend(route["origin"], route["dest"], route["date"])
            if result["offers"]:
                cheapest = min(result["offers"], key=lambda o: o["price"])
                print(f"{route['origin']}→{route['dest']} {route['date']}: "
                      f"${cheapest['price']} ({trend})")
        time.sleep(interval_minutes * 60)
```

### Complete Autonomous Agent Example

End-to-end implementation of an AI agent that autonomously searches, evaluates, and books flights based on user preferences while managing edge cases:

```python
from letsfg.connectors.auth import BearerTokenError
from letsfg.local import search_local, book_offer
import asyncio

class FlightAgent:
    """Autonomous flight booking agent with preference evaluation."""

    def resolve_city(self, city_name):
        """Resolve city name to IATA code, handling ambiguity. Stub — wire to
        GET /api/locations or letsfg locations for a real implementation."""
        raise NotImplementedError

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
            price_norm = offer["price"] / 2000
            dur_norm = (offer["outbound"]["total_duration_seconds"] / 3600) / 24
            stops_norm = offer["outbound"]["stopovers"] / 3
            airline_norm = 0 if any(a in preferred_airlines for a in offer["airlines"]) else 1

            score = (weights["price"] * price_norm + weights["duration"] * dur_norm +
                     weights["stops"] * stops_norm + weights["airline"] * airline_norm)
            scored.append((score, offer))

        return sorted(scored, key=lambda x: x[0])

    def search_and_book(self, origin_iata, dest_iata, date, passenger, contact_email,
                         preferences=None, max_retries=2):
        """Full autonomous workflow: search → evaluate → book."""
        for attempt in range(max_retries + 1):
            # Search (free, unlimited)
            result = asyncio.run(search_local(origin_iata, dest_iata, date))
            if not result["offers"]:
                return None

            # Evaluate by preferences (not just cheapest)
            if preferences:
                ranked = self.evaluate_offers(result["offers"], preferences)
                best_offer = ranked[0][1]
            else:
                best_offer = min(result["offers"], key=lambda o: o["price"])

            # Book (free — ticket price only, no LetsFG fee)
            try:
                booked = asyncio.run(book_offer(
                    search_id=result["search_id"], offer_id=best_offer["id"],
                    passenger=passenger, contact_email=contact_email,
                ))
                return booked  # {"booking_ref": ..., "state": "booking_in_progress", "held": {...}} — poll /api/agent-book/status
            except BearerTokenError:
                raise  # can't retry — reconnect at https://letsfg.co/connect

# Usage
agent = FlightAgent()
result = agent.search_and_book(
    "LON", "NYC", "2026-06-15",
    passenger={"given_name": "Ada", "family_name": "Lovelace", "born_on": "1990-04-01", "gender": "f"},
    contact_email="traveller@example.com",
    preferences={
        "price": 0.3, "duration": 0.4, "stops": 0.2, "airline": 0.1,
        "preferred_airlines": {"British Airways", "Delta"},
    },
)
if result and "booking_ref" in result:
    print("Started — poll /api/agent-book/status with", result["booking_ref"])
elif result:
    print("Ask the traveller for:", result.get("missing_fields"))
```

## PFS Auth — connect a card at letsfg.co/connect (CLI / SDK / Direct API)

The CLI, Python SDK, JS SDK and MCP server all use the same PFS (Programmatic
Flight Search) Bearer token. It is card-backed: the card is what lets your agent
go all the way to booking, and it keeps automated abuse off the search engine.

**Nothing is charged to connect.** The card is saved through a 0.00 Revolut setup
at <https://letsfg.co/connect> — any card, or Revolut Pay / Google Pay; no Revolut
account needed; card details go to Revolut and never touch LetsFG. You pay the
ticket price only when you book, and even then the money is held, not taken,
until the airline confirms.

**The one way in:**

1. Add LetsFG as a remote MCP server: `https://letsfg.co/developers/api/mcp`
   (Claude Code: `claude mcp add --transport http letsfg https://letsfg.co/developers/api/mcp`;
   claude.ai / ChatGPT: a custom connector with that URL; Cursor / Windsurf: see
   the MCP section above).
2. Approve the connection. The OAuth consent step opens <https://letsfg.co/connect>,
   where the person adds a card or pays 0.00 with Revolut Pay / Google Pay.
3. The OAuth token you receive is card-backed. Over the MCP it is carried for you.
   Over raw HTTP send the same token on every request:

```
Authorization: Bearer eyJ...
```

Starting from the endpoint lands in the same place:

```bash
curl -s -X POST https://letsfg.co/api/agent-access/request
# → 402 {"error":"payment_method_required",
#        "add_card_url":"https://letsfg.co/connect",
#        "how":["Connect LetsFG as an MCP server (...) and approve the connection: the consent step opens https://letsfg.co/connect ...",
#               "Every tool call then carries that token. book_flight holds the fare ...; get_flight_booking reports the PNR when it lands."],
#        "charged":false, "docs":"https://letsfg.co/for-agents"}
```

**Using the token:**

```bash
# Search
curl -X POST https://letsfg.co/api/search \
  -H "Authorization: Bearer eyJ..." \
  -H "Content-Type: application/json" \
  -d '{"origin":"LHR","destination":"JFK","date_from":"2026-06-01"}'
# → {"search_id":"ws_abc123","status":"searching"}

# Poll results
curl https://letsfg.co/api/results/ws_abc123 -H "Authorization: Bearer eyJ..."

# Book — holds the fare on the connected card, returns a booking_ref in seconds
curl -X POST https://letsfg.co/api/agent-book \
  -H "Authorization: Bearer eyJ..." \
  -H "Content-Type: application/json" \
  -d '{"search_id":"ws_abc123","offer_id":"ws_off_...","contact_email":"traveller@example.com",
       "passenger":{"given_name":"Ada","family_name":"Lovelace","born_on":"1990-04-01","gender":"f",
                    "nationality":"GB","phone_number":"+15551234567","phone_country":"US",
                    "address_line1":"1 Analytical Way","address_city":"London",
                    "address_postal":"N1 9GU","address_country":"GB"}}'
# → {"ok":true,"state":"booking_in_progress","booking_ref":"eyJ...","held":{"amount":93,"currency":"EUR","card":"visa ending 5709"},"charged":0}

# Poll the booking every 20–30 s (it takes 4–11 minutes)
curl -X POST https://letsfg.co/api/agent-book/status \
  -H "Authorization: Bearer eyJ..." \
  -H "Content-Type: application/json" \
  -d '{"booking_ref":"eyJ..."}'
# → {"state":"completed","pnr":"ABC123","charged_amount":93,"currency":"EUR"}
# → {"state":"failed","failure_reason":"...","charged_amount":null}   (hold released)
# → {"state":"needs_attention"}   (a human at LetsFG is checking it — do NOT book again)
```

One traveller per call; passport fields (`passport_number`, `passport_country`,
`passport_expiry`) are optional. A missing detail answers
`{"error":"missing_details","missing_fields":[...],"charged":0}` — ask and call
again. Only `completed` with a PNR means booked.

**Retired 2026-09-02:** the Stripe enrolment lanes (`setup_url`, `setup_intent`,
`payment_method_id`, `card_token`) and the earlier Twitter/X challenge. Every
token they issued has been revoked — such a token now answers `401 TOKEN_REVOKED`,
and `POST /api/agent-access/verify` answers `410` for a Stripe credential. Connect
the card again at <https://letsfg.co/connect>. `letsfg auth` (npm and PyPI)
now drives that connect flow from the terminal — it registers itself as an
OAuth client, opens the card screen, and stores the token; the stdio server's
`authenticate` tool returns the current instructions (`add_card_url`, `how`)
rather than minting one. The CLI and SDKs otherwise read the token from
`LETSFG_BEARER_TOKEN` (or `~/.letsfg/config.json`).

**One card = one account.** A payment method identifies exactly one account.
Connecting a card that is already in use does not create a second account: it
lands on the existing one. Two agents can never hold live tokens on the same card,
so a shared or recycled card gains you nothing. Quotas and rate limits are
bucketed per card, not per token. If a token leaks, disconnect and reconnect to
rotate it.

MPP (wallet, card-free) is offered only when the `402` above carries an `mpp`
object and a `WWW-Authenticate: Payment` header; it costs one cent, once, and is
verified at `POST /api/agent-access/verify` with `Authorization: Payment`.

## API Discovery

| Endpoint | URL |
|----------|-----|
| OpenAPI/Swagger | https://letsfg.co/developers/api/docs |
| Agent discovery | https://letsfg.co/developers/api/.well-known/ai-plugin.json |
| Agent manifest | https://letsfg.co/developers/api/.well-known/agent.json |
| LLM instructions | https://letsfg.co/developers/api/llms.txt |

## Links

- **PyPI:** https://pypi.org/project/letsfg/
- **npm (JS SDK):** https://www.npmjs.com/package/letsfg
- **npm (MCP):** https://www.npmjs.com/package/letsfg-mcp
