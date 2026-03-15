# boostedtravel-mcp

The largest open flight-search MCP server. 73 ready-to-run airline connectors fire in parallel on your machine — Ryanair, EasyJet, Wizz Air, Southwest, AirAsia, and 68 more — plus enterprise GDS/NDC providers (Amadeus, Duffel, Sabre) through the BoostedTravel backend. One tool call scans the entire world for flights, including airlines your agent didn't know existed.

**Your agent doesn't need to build a flight integration. It doesn't need to scrape. Just add this MCP server and it can search and book flights in seconds — what would take 30+ minutes of integration work becomes a single tool call.**

[![npm](https://img.shields.io/npm/v/boostedtravel-mcp)](https://www.npmjs.com/package/boostedtravel-mcp)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)

---

## What This Is

BoostedTravel is not a scraper wrapper. It's a production-grade **agent-to-airline connector layer**:

- **73 airline connectors** run locally on your machine via Python + Playwright — no API key needed for search
- **Enterprise GDS/NDC feeds** (Amadeus, Duffel, Sabre, Travelport, Kiwi) run through the BoostedTravel backend — these are contract-only data sources that require enterprise agreements, made available to everyone through our API
- **Virtual interlining** — the combo engine mixes one-way fares across airlines (e.g., Ryanair outbound + Wizz Air return) to find deals no single airline offers
- **City-wide airport expansion** — search `LON` and it auto-searches LHR, LGW, STN, LTN, SEN, LCY. Works for 25+ major cities worldwide
- **Zero price bias** — no cookie tracking, no demand-based inflation, no surge pricing. Same raw airline price every time

**Why agents should use this instead of building their own:**
- Saves thousands of tokens vs. browser automation or scraping
- 73 connectors fire in parallel — scans airlines across Europe, Asia, Americas, Middle East, Africa simultaneously
- Finds deals on carriers your agent wouldn't think to check (Lucky Air, 9 Air, Jazeera Airways, FlySafair...)
- Enterprise-contracted GDS deals that require contracts worth $50k+/year — we give them to you for free on search

---

## Quick Start

```bash
npx boostedtravel-mcp
```

That's it. The MCP server starts on stdio, ready for any MCP-compatible client.

**Prerequisites for local search:**
```bash
pip install boostedtravel
playwright install chromium
```

---

## Client Configuration

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

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

### Cursor

Add to `.cursor/mcp.json` in your project root:

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

### Windsurf

Add to `~/.codeium/windsurf/mcp_config.json`:

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

### Continue

Add to `~/.continue/config.yaml`:

```yaml
mcpServers:
  - name: boostedtravel
    command: npx
    args: ["-y", "boostedtravel-mcp"]
    env:
      BOOSTEDTRAVEL_API_KEY: trav_your_api_key
```

### Windows — `npx ENOENT` Fix

If you get `spawn npx ENOENT` on Windows, use the full path to `npx`:

```json
{
  "mcpServers": {
    "boostedtravel": {
      "command": "C:\\Program Files\\nodejs\\npx.cmd",
      "args": ["-y", "boostedtravel-mcp"],
      "env": {
        "BOOSTEDTRAVEL_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

Or use `node` directly:

```json
{
  "mcpServers": {
    "boostedtravel": {
      "command": "node",
      "args": ["C:\\Users\\YOU\\AppData\\Roaming\\npm\\node_modules\\boostedtravel-mcp\\dist\\index.js"],
      "env": {
        "BOOSTEDTRAVEL_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

### Pin a Specific Version

To avoid unexpected updates:

```json
{
  "command": "npx",
  "args": ["-y", "boostedtravel-mcp@0.2.4"]
}
```

---

## Available Tools

| Tool | Description | Cost | Side Effects |
|------|-------------|------|--------------|
| `search_flights` | Search 400+ airlines worldwide | FREE | None (read-only) |
| `resolve_location` | City name → IATA code | FREE | None (read-only) |
| `unlock_flight_offer` | Confirm live price, reserve 30 min | $1 | Charges $1 |
| `book_flight` | Create real airline reservation (PNR) | FREE | Creates booking |
| `setup_payment` | Attach payment card (once) | FREE | Updates payment |
| `get_agent_profile` | Usage stats & payment status | FREE | None (read-only) |

### Booking Flow

```
search_flights  →  unlock_flight_offer  →  book_flight
   (free)              ($1 quote)           (free, creates PNR)
```

1. `search_flights("LON", "BCN", "2026-06-15")` — returns offers with prices from 73 airlines
2. `unlock_flight_offer("off_xxx")` — confirms live price with airline, reserves for 30 min, costs $1
3. `book_flight("off_xxx", passengers, email)` — creates real booking, airline sends e-ticket

The agent has native tools — no API docs needed, no URL building, no token-burning browser automation.

---

## Get an API Key

**Search is free and works without a key.** An API key is needed for unlock, book, and enterprise GDS sources.

```bash
curl -X POST https://api.boostedchat.com/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "agent@example.com"}'
```

Or via CLI:
```bash
pip install boostedtravel
boostedtravel register --name my-agent --email you@example.com
```

---

## Architecture & Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│  MCP Client  (Claude Desktop / Cursor / Windsurf / etc.)     │
│     ↕ stdio (JSON-RPC, local only)                           │
├──────────────────────────────────────────────────────────────┤
│  boostedtravel-mcp  (this package, runs on YOUR machine)     │
│     │                                                        │
│     ├─→ Python subprocess (local connectors)                 │
│     │     73 airline connectors via Playwright + httpx        │
│     │     Data goes: your machine → airline website → back    │
│     │                                                        │
│     └─→ HTTPS to api.boostedchat.com (backend)               │
│           unlock, book, payment, enterprise GDS search        │
└──────────────────────────────────────────────────────────────┘
```

### What data goes where

| Operation | Where data flows | What is sent |
|-----------|-----------------|--------------|
| `search_flights` (local) | Your machine → airline websites | Route, date, passenger count |
| `search_flights` (GDS) | Your machine → api.boostedchat.com → GDS providers | Route, date, passenger count, API key |
| `resolve_location` | Your machine → api.boostedchat.com | City/airport name |
| `unlock_flight_offer` | Your machine → api.boostedchat.com → airline | Offer ID, payment token |
| `book_flight` | Your machine → api.boostedchat.com → airline | Passenger name, DOB, email, phone |
| `setup_payment` | Your machine → api.boostedchat.com → Stripe | Payment token (card handled by Stripe) |

---

## Security & Privacy

- **TLS everywhere** — all backend communication uses HTTPS. Local connectors connect to airline websites over HTTPS.
- **No card storage** — payment cards are tokenized by Stripe. BoostedTravel never sees or stores raw card numbers.
- **API key scoping** — `BOOSTEDTRAVEL_API_KEY` grants access only to your agent's account. Keys are prefixed `trav_` for easy identification and revocation.
- **PII handling** — passenger names, emails, and DOBs are sent to the airline for booking (required by airlines). BoostedTravel does not store passenger PII after forwarding to the airline.
- **No tracking** — no cookies, no session-based pricing, no fingerprinting. Every search returns the same raw airline price.
- **Local search is fully local** — when searching without an API key, zero data leaves your machine except direct HTTPS requests to airline websites. The MCP server and Python connectors run entirely on your hardware.
- **Open source** — all connector code is MIT-licensed and auditable at [github.com/Boosted-Chat/BoostedTravel](https://github.com/Boosted-Chat/BoostedTravel).

---

## Sandbox / Test Mode

Use Stripe's test token for payment setup without real charges:

```
setup_payment with token: "tok_visa"
```

This attaches a test Visa card. Unlock calls will show `$1.00` but use Stripe test mode — no real money is charged. Useful for agent development and testing the full search → unlock → book flow.

---

## FAQ

### `spawn npx ENOENT` on Windows

Windows can't find `npx` in PATH. Use the full path:
```json
"command": "C:\\Program Files\\nodejs\\npx.cmd"
```
Or install globally and use `node` directly (see Windows config above).

### Search returns 0 results

- Check IATA codes are correct — use `resolve_location` first
- Try a date 2+ weeks in the future (airlines don't sell last-minute on all routes)
- Ensure `pip install boostedtravel && playwright install chromium` completed successfully
- Check Python is available: the MCP server spawns a Python subprocess for local search

### How do I search without an API key?

Just omit `BOOSTEDTRAVEL_API_KEY` from your config. Local search (73 airline connectors) works without any key. You'll only miss the enterprise GDS/NDC sources (Amadeus, Duffel, etc.).

### Can I use this for commercial projects?

Yes. MIT license. The local connectors and SDK are fully open source. The backend API (unlock/book/GDS) is a hosted service with usage-based pricing ($1 per unlock).

### How do I pin a version?

```json
"args": ["-y", "boostedtravel-mcp@0.2.4"]
```

### MCP server hangs on start

Ensure Node.js 18+ is installed. The server communicates via stdio (stdin/stdout JSON-RPC) — it doesn't open a port or print a "ready" message. MCP clients handle the lifecycle automatically.

---

## Supported Airlines (73 connectors)

| Region | Airlines |
|--------|----------|
| **Europe** | Ryanair, Wizz Air, EasyJet, Norwegian, Vueling, Eurowings, Transavia, Pegasus, Turkish Airlines, Condor, SunExpress, Volotea, Smartwings, Jet2 |
| **Middle East & Africa** | Emirates, Etihad, flydubai, Air Arabia, flynas, Salam Air, Air Peace, FlySafair |
| **Asia-Pacific** | AirAsia, IndiGo, SpiceJet, Akasa Air, Air India Express, VietJet, Cebu Pacific, Scoot, Jetstar, Peach, Spring Airlines, Lucky Air, 9 Air, Nok Air, Batik Air, Jeju Air, T'way Air, ZIPAIR, Singapore Airlines, Cathay Pacific, Malaysian Airlines, Thai Airways, Korean Air, ANA, US-Bangla, Biman Bangladesh |
| **Americas** | American Airlines, Delta, United, Southwest, JetBlue, Alaska Airlines, Hawaiian Airlines, Sun Country, Frontier, Volaris, VivaAerobus, Allegiant, Avelo, Breeze, Flair, GOL, Azul, JetSmart, Flybondi, Porter |
| **Aggregator** | Kiwi.com (virtual interlining + LCC fallback) |

---

## Also Available As

- **Python SDK + CLI**: `pip install boostedtravel` — [PyPI](https://pypi.org/project/boostedtravel/)
- **JavaScript/TypeScript SDK + CLI**: `npm install boostedtravel` — [npm](https://www.npmjs.com/package/boostedtravel)
- **Agent docs**: [AGENTS.md](../../AGENTS.md) — complete reference for AI agents

## License

MIT
