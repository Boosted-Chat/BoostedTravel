# Packages

BoostedTravel is available as a Python SDK, JavaScript SDK, MCP server, and remote MCP endpoint.

## Overview

| Package | Install | What it is |
|---------|---------|------------|
| **Python SDK + CLI** | `pip install boostedtravel` | SDK + CLI + 53 local LCC connectors |
| **JS/TS SDK + CLI** | `npm install -g boostedtravel` | SDK + `boostedtravel` CLI command |
| **MCP Server** | `npx boostedtravel-mcp` | Model Context Protocol for AI agents |
| **Remote MCP** | `https://api.boostedchat.com/mcp` | Streamable HTTP — no install needed |

## Python SDK

[![PyPI](https://img.shields.io/pypi/v/boostedtravel)](https://pypi.org/project/boostedtravel/)

```bash
pip install boostedtravel
```

Provides:

- `BoostedTravel` client class with `search()`, `unlock()`, `book()`, `me()`, `resolve_location()`, `setup_payment()`
- **53 local LCC connectors** — scrape airline websites directly on your machine (Ryanair, Wizz Air, EasyJet, Norwegian, AirAsia, IndiGo, and 47 more)
- `search_local()` — free local-only search, no API key needed
- CLI command `boostedtravel` with all operations
- Virtual interlining engine — cross-airline round-trips from one-way fares
- Shared browser infrastructure — stealth Chrome launcher, CDP sessions, anti-bot handling
- Typed response models: `FlightSearchResponse`, `UnlockResponse`, `BookingResponse`, `AgentProfile`
- Exception classes: `AuthenticationError`, `PaymentRequiredError`, `OfferExpiredError`

```python
from boostedtravel import BoostedTravel

bt = BoostedTravel(api_key="trav_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
```

### Local Search (No API Key)

```python
from boostedtravel.local import search_local

# Free, runs all relevant LCC connectors on your machine
result = await search_local("GDN", "BCN", "2026-06-15")
```

[Full Python SDK docs →](https://github.com/Boosted-Chat/BoostedTravel/tree/main/sdk/python)

## JavaScript / TypeScript SDK

[![npm](https://img.shields.io/npm/v/boostedtravel)](https://www.npmjs.com/package/boostedtravel)

```bash
npm install -g boostedtravel
```

Provides:

- `BoostedTravel` client class with `search()`, `unlock()`, `book()`, `me()`
- CLI command `boostedtravel` (same interface as Python)
- TypeScript types for all responses

```typescript
import { BoostedTravel } from 'boostedtravel';

const bt = new BoostedTravel({ apiKey: 'trav_...' });
const flights = await bt.search('LHR', 'JFK', '2026-04-15');
```

[Full JS SDK docs →](https://github.com/Boosted-Chat/BoostedTravel/tree/main/sdk/js)

## MCP Server

[![npm](https://img.shields.io/npm/v/boostedtravel-mcp)](https://www.npmjs.com/package/boostedtravel-mcp)

Model Context Protocol server for AI assistants like Claude Desktop, Cursor, and Windsurf.

### Quick Setup

```bash
npx boostedtravel-mcp
```

### Configuration

Add to your MCP config (Claude Desktop, Cursor, etc.):

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

### Remote MCP (Streamable HTTP)

If your client supports remote MCP servers, connect directly without installing anything:

```
https://api.boostedchat.com/mcp
```

### Available Tools

| Tool | Description |
|------|-------------|
| `search_flights` | Search 400+ airlines for flights |
| `get_agent_profile` | View account info and usage stats |
| `resolve_location` | Convert city names to IATA codes |
| `setup_payment` | Attach a Stripe payment method |
| `unlock_flight_offer` | Confirm price and reserve ($1) |
| `book_flight` | Create airline booking (PNR) |

[npm page →](https://www.npmjs.com/package/boostedtravel-mcp)

## API Endpoints

All packages connect to the same API:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/agents/register` | POST | Create account, get API key |
| `/api/v1/flights/search` | POST | Search flights |
| `/api/v1/flights/resolve-location` | GET | Resolve city/airport codes |
| `/api/v1/bookings/unlock` | POST | Unlock offer ($1) |
| `/api/v1/bookings/book` | POST | Book flight |
| `/api/v1/agents/setup-payment` | POST | Setup Stripe payment |
| `/api/v1/agents/me` | GET | Agent profile |

**Base URL:** `https://api.boostedchat.com`

**Interactive docs:** [api.boostedchat.com/docs](https://api.boostedchat.com/docs)
