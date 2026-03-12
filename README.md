# BoostedTravel

Agent-native flight search & booking. 400+ airlines, 53 direct LCC connectors, virtual interlining — straight from the terminal. Built for AI agents and developers.

**API Base URL:** `https://api.boostedchat.com`

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/boostedtravel)](https://pypi.org/project/boostedtravel/)
[![npm](https://img.shields.io/npm/v/boostedtravel)](https://www.npmjs.com/package/boostedtravel)

## Why BoostedTravel?

Flight websites inflate prices with demand tracking, cookie-based pricing, and surge markup. The same flight is often **$20–$50 cheaper** through BoostedTravel — raw airline price, zero markup.

| | Google Flights / Booking.com / Expedia | **BoostedTravel** |
|---|---|---|
| Search | Free | **Free** |
| View details & price | Free (with tracking/inflation) | **Free** (no tracking) |
| Book | Ticket + hidden markup | **$1 unlock + ticket price** |
| Price goes up on repeat search? | Yes | **Never** |
| LCC coverage | Missing many low-cost carriers | **53 direct airline connectors** |

## Quick Start

```bash
pip install boostedtravel

boostedtravel register --name my-agent --email you@example.com
export BOOSTEDTRAVEL_API_KEY=trav_...

boostedtravel search LHR JFK 2026-04-15
boostedtravel search LON BCN 2026-04-01 --return 2026-04-08 --cabin M --sort price
boostedtravel unlock off_xxx
boostedtravel book off_xxx \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --email john.doe@example.com
```

All commands support `--json` for machine-readable output:

```bash
boostedtravel search GDN BER 2026-03-03 --json | jq '.offers[0]'
```

## Install

### Python (recommended)

```bash
pip install boostedtravel
```

### JavaScript / TypeScript

```bash
npm install -g boostedtravel
```

### Python SDK

```python
from boostedtravel import BoostedTravel

bt = BoostedTravel(api_key="trav_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")

unlocked = bt.unlock(flights.offers[0].id)
booking = bt.book(
    offer_id=unlocked.offer_id,
    passengers=[{"id": "pas_0", "given_name": "John", "family_name": "Doe", "born_on": "1990-01-15", "gender": "m", "title": "mr"}],
    contact_email="john.doe@example.com",
)
print(f"Booked! PNR: {booking.booking_reference}")
```

### JS SDK

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

| Command | Description |
|---------|-------------|
| `boostedtravel register` | Get your API key |
| `boostedtravel search <origin> <dest> <date>` | Search flights (free) |
| `boostedtravel locations <query>` | Resolve city/airport to IATA codes |
| `boostedtravel unlock <offer_id>` | Unlock offer details ($1) |
| `boostedtravel book <offer_id>` | Book the flight (free after unlock) |
| `boostedtravel setup-payment` | Set up payment method |
| `boostedtravel me` | View profile & usage stats |

All commands accept `--json` for structured output and `--api-key` to override the env variable.

## How It Works

1. **Search** (free) — returns offers with full details: price, airlines, duration, stopovers, conditions
2. **Unlock** ($1) — confirms live price with the airline, reserves for 30 minutes
3. **Book** (free) — creates real airline PNR, e-ticket sent to passenger email

### Two Search Modes

| Mode | What it does | Speed | Auth |
|------|-------------|-------|------|
| **Cloud search** | Queries GDS/NDC providers (Duffel, Amadeus, Sabre, Travelport, Kiwi) via backend API | 2-15s | API key |
| **Local search** | Fires 53 LCC connectors on your machine via Playwright + httpx | 5-25s | None |

Both modes run simultaneously by default. Results are merged, deduplicated, currency-normalized, and sorted.

### Virtual Interlining

The combo engine builds cross-airline round-trips by combining one-way fares from different carriers. A Ryanair outbound + Wizz Air return can save 30-50% vs booking a round-trip on either airline alone.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  AI Agents / CLI / SDK / MCP Server                 │
├──────────────────┬──────────────────────────────────┤
│  Local connectors │  Cloud API                      │
│  (53 LCC scrapers │  (Duffel, Amadeus, Sabre,       │
│   via Playwright) │   Travelport, Kiwi, etc.)       │
├──────────────────┴──────────────────────────────────┤
│            Merge + Dedup + Combo Engine              │
│            (virtual interlining, currency norm)      │
└─────────────────────────────────────────────────────┘
```

## Local LCC Connectors (53 Airlines)

The Python SDK includes 53 built-in connectors that scrape low-cost carriers directly — no API key needed for local search. Each connector uses one of three proven strategies:

| Strategy | How it works | Example airlines |
|----------|-------------|-----------------|
| **Direct API** | Reverse-engineered REST/GraphQL endpoints via `httpx`/`curl_cffi` | Ryanair, Wizz Air, Norwegian, Akasa |
| **CDP Chrome** | Real Chrome + Playwright CDP for sites with bot detection | EasyJet, Southwest, Pegasus |
| **API Interception** | Playwright page navigation + response interception | VietJet, Cebu Pacific, Lion Air |

### Supported Airlines

<details>
<summary>Full list of 53 LCC connectors</summary>

| Region | Airlines |
|--------|----------|
| **Europe** | Ryanair, Wizz Air, EasyJet, Norwegian, Vueling, Eurowings, Transavia, Pegasus, Condor, Play, SunExpress, Volotea, Smartwings, Jet2, airBaltic |
| **Middle East & Africa** | flydubai, Air Arabia, flynas, Jazeera Airways, Air Peace, FlySafair |
| **Asia-Pacific** | AirAsia, IndiGo, SpiceJet, Akasa Air, Air India Express, VietJet, Cebu Pacific, Scoot, Lion Air, Jetstar, Peach, Spring Airlines, Lucky Air, 9 Air, Nok Air, Batik Air, Jeju Air, T'way Air, ZIPAIR |
| **Americas** | Southwest, Spirit, Frontier, Volaris, VivaAerobus, Allegiant, Flair, GOL, Azul, JetSmart, Flybondi, Porter |
| **Aggregator** | Kiwi.com (virtual interlining + LCC fallback) |

</details>

### Local Search (No API Key)

```python
from boostedtravel.local import search_local

# Runs all relevant connectors on your machine — completely free
result = await search_local("GDN", "BCN", "2026-06-15")
```

```bash
# CLI local-only search
boostedtravel search-local GDN BCN 2026-06-15
```

### Shared Browser Infrastructure

All browser-based connectors share a common launcher (`connectors/browser.py`) with:

- Automatic Chrome discovery (Windows, macOS, Linux)
- Stealth headless mode (`--headless=new`) — undetectable by airline bot protection
- Off-screen window positioning to avoid stealing focus
- CDP persistent sessions for airlines that require cookie state
- `BOOSTED_BROWSER_VISIBLE=1` to show browser windows for debugging

## Error Handling

| Exception | HTTP | When |
|-----------|------|------|
| `AuthenticationError` | 401 | Missing or invalid API key |
| `PaymentRequiredError` | 402 | No payment method (call `setup-payment`) |
| `OfferExpiredError` | 410 | Offer no longer available (search again) |
| `BoostedTravelError` | any | Base class for all API errors |

## Packages

| Package | Install | What it is |
|---------|---------|------------|
| **Python SDK + CLI** | `pip install boostedtravel` | SDK + `boostedtravel` CLI + 53 local LCC connectors |
| **JS/TS SDK + CLI** | `npm install -g boostedtravel` | SDK + `boostedtravel` CLI command |
| **MCP Server** | `npx boostedtravel-mcp` | Model Context Protocol for Claude, Cursor, Windsurf |
| **Remote MCP** | `https://api.boostedchat.com/mcp` | Streamable HTTP — no install needed |

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Authentication, payment setup, search flags, cabin classes |
| [API Guide](docs/api-guide.md) | Error handling, search results, workflows, unlock details, location resolution |
| [Agent Guide](docs/agent-guide.md) | AI agent architecture, preference scoring, price tracking, rate limits |
| [Packages & SDKs](docs/packages.md) | Python SDK, JavaScript SDK, MCP Server, local connectors |
| [CLI Reference](docs/cli-reference.md) | Commands, flags, examples |
| [AGENTS.md](AGENTS.md) | Agent-specific instructions (for LLMs) |
| [CLAUDE.md](CLAUDE.md) | Codebase context for Claude |

## API Docs

- **OpenAPI/Swagger:** https://api.boostedchat.com/docs
- **Agent discovery:** https://api.boostedchat.com/.well-known/ai-plugin.json
- **Agent manifest:** https://api.boostedchat.com/.well-known/agent.json
- **LLM instructions:** https://api.boostedchat.com/llms.txt

## Links

- **PyPI:** https://pypi.org/project/boostedtravel/
- **npm (JS SDK):** https://www.npmjs.com/package/boostedtravel
- **npm (MCP):** https://www.npmjs.com/package/boostedtravel-mcp

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

See [SECURITY.md](SECURITY.md) for our security policy.

## License

[MIT](LICENSE)
