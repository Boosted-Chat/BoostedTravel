# Packages

> ### ⚠️ Agents: do not create a Developer API account
>
> Do **NOT** call `/developers/api/v1/agents/register` or
> `/developers/api/v1/agents/connect-payment`, and do **NOT** run
> `letsfg register` or `letsfg connect-payment`. Those belong to a separate,
> paid, prepaid-balance product for high-volume commercial integrations and
> create a billing account you almost certainly do not want.
>
> To search and book flights, connect LetsFG as an MCP server at
> <https://letsfg.co/developers/api/mcp>. The consent step opens
> <https://letsfg.co/connect>, where a card is saved (nothing is charged).
> See <https://letsfg.co/for-agents>.

LetsFG is available as a Python SDK, JavaScript SDK, MCP server, and remote MCP endpoint. Works with OpenClaw, Perplexity Computer, Claude Desktop, Cursor, Windsurf, and any MCP-compatible agent.

## Overview

Every package below covers **flights and hotels**, on **one credential**: the card-backed token you get by connecting the MCP (the consent step saves a card at <https://letsfg.co/connect>, nothing is charged), or a Developer API key. Hotels additionally need a card on file for search as well as booking — which the connect step already provides — see [Hotels](hotels.md).

| Package | Install | What it is | API Key Required? |
|---------|---------|------------|-------------------|
| **Python SDK + CLI** | `pip install letsfg` | SDK + CLI, server-side search via letsfg.co | Card-backed token in `LETSFG_BEARER_TOKEN` or Developer API key |
| **JS/TS SDK + CLI** | `npm install -g letsfg` | SDK + `letsfg` CLI command | Free Bearer token or Developer API key |
| **MCP Server** | `npx letsfg-mcp` | Model Context Protocol for AI agents | Free Bearer token or Developer API key |
| **Remote MCP** | `https://letsfg.co/developers/api/mcp` | Streamable HTTP — no install needed; **the way to connect** (OAuth consent saves the card) | Nothing up front — connect and approve |
| **Smithery** | [smithery.ai/servers/letsfg](https://smithery.ai/servers/letsfg) | One-click MCP install | Free token or Developer API key |

## Python SDK

[![PyPI](https://img.shields.io/pypi/v/letsfg)](https://pypi.org/project/letsfg/)

```bash
pip install letsfg
```

Provides:

- `LetsFG` client class with `search()`, `book()`, `me()`, `resolve_location()`. `unlock()` and `setup_payment()` call routes retired on 2026-09-08 and now answer `410 Gone`
- Server-side search via letsfg.co — Ryanair, Wizz Air, EasyJet, Norwegian, AirAsia, IndiGo, Qatar Airways, LATAM, Finnair, and 190+ more
- CLI command `letsfg` with all operations (`letsfg auth` implemented the retired Stripe setup and is being migrated to the connect flow; set `LETSFG_BEARER_TOKEN` meanwhile)
- Typed response models: `FlightSearchResponse`, `UnlockResponse`, `BookingResponse`, `AgentProfile`
- Exception classes: `AuthenticationError`, `PaymentRequiredError`, `OfferExpiredError`

```python
from letsfg import LetsFG

bt = LetsFG(api_key="letsfg_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
```

[Full Python SDK docs →](https://github.com/LetsFG/LetsFG/tree/main/sdk/python)

## JavaScript / TypeScript SDK

[![npm](https://img.shields.io/npm/v/letsfg)](https://www.npmjs.com/package/letsfg)

```bash
npm install -g letsfg
```

Provides:

- `LetsFG` client class with `search()`, `book()`, `me()`. `unlock()` calls a route retired on 2026-09-08
- CLI command `letsfg` (same interface as Python)
- TypeScript types for all responses

```typescript
import { LetsFG } from 'letsfg';

const bt = new LetsFG({ apiKey: 'letsfg_...' });
const flights = await bt.search('LHR', 'JFK', '2026-04-15');
```

[Full JS SDK docs →](https://github.com/LetsFG/LetsFG/tree/main/sdk/js)

## MCP Server

[![npm](https://img.shields.io/npm/v/letsfg-mcp)](https://www.npmjs.com/package/letsfg-mcp)

Model Context Protocol server for AI assistants like Claude Desktop, Cursor, and Windsurf.

### Quick Setup

```bash
npx letsfg-mcp
```

The local MCP server connects to the letsfg.co server-side engine. It needs a credential in its environment: `LETSFG_BEARER_TOKEN` (a card-backed token from the connect flow) for free flight search and booking, or `LETSFG_API_KEY` for the Developer API. If your client supports remote MCP servers, prefer the [remote endpoint](#remote-mcp-streamable-http) below — it needs no install and no token handling.

> Either credential reaches **both** flights and hotels. If both are set, the Bearer token is used.

### Configuration

Add to your MCP config (Claude Desktop, Cursor, etc.):

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_API_KEY": "letsfg_your_api_key"
      }
    }
  }
}
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LETSFG_BEARER_TOKEN` | (none) | Card-backed token from the connect flow. Reaches **flights and hotels** |
| `LETSFG_API_KEY` | (none) | Developer API key (look-to-book search). Also reaches both; required for the account and payment tools |
| `LETSFG_BASE_URL` | `https://letsfg.co/developers` | Override the website-owned public API base |

### Remote MCP (Streamable HTTP)

If your client supports remote MCP servers, connect directly without installing anything:

```
https://letsfg.co/developers/api/mcp
```

This is the recommended way in. Point your client at the URL and approve the connection: the OAuth consent step opens <https://letsfg.co/connect>, where the person saves a card in a 0.00 Revolut setup (any card, or Revolut Pay / Google Pay — no Revolut account needed). Nothing is charged; the token your client receives is card-backed and can search and book. A Developer API key also works (`X-API-Key` header) for the paid product.

```bash
# Claude Code
claude mcp add --transport http letsfg https://letsfg.co/developers/api/mcp
```

```json
// Cursor (.cursor/mcp.json) — Windsurf uses "serverUrl" instead of "url"
{ "mcpServers": { "letsfg": { "url": "https://letsfg.co/developers/api/mcp" } } }
```

### Available Tools

**Flights**

| Tool | Description | Auth |
|------|-------------|------|
| `search_flights` | Search via the letsfg.co server-side engine | Bearer or API key |
| `resolve_location` | Convert city names to IATA codes | API key |
| `unlock_flight_offer` | **RETIRED 2026-09-08** — the route answers `410 Gone`. There is no unlock step on either lane; call `book_flight` (PFS) or `POST /flights/book` (Developer API) directly | API key |
| `book_flight` | Start the booking: the fare plus LetsFG's markup is held on the connected card, a LetsFG booking agent buys the ticket, and the hold is captured only against a real PNR. Returns a `booking_ref` in seconds; the booking takes 4–11 min | Bearer or API key |
| `get_flight_booking` | Poll a started booking every 20–30 s: `booking_in_progress` → `completed` (PNR) / `failed` (hold released) / `needs_attention` (do not book again) | Bearer |

**Hotels** — need a card on file (a search opens a real supplier session). Either credential works.

| Tool | Description | Auth |
|------|-------------|------|
| `resolve_hotel_city` | Resolve a place name to the supplier city id `search_hotels` needs. Call this first | API key |
| `search_hotels` | Search real, bookable, free-cancellation pay-later rates. Needs a card on file — a search opens a real supplier session. Takes up to a few minutes | API key + card |
| `book_hotel` | Book one rate. Charges **5% of the price as a non-refundable reservation fee**; the balance is paid to the supplier via the returned pay link by `balance_due_by`. Returns a `booking_job_id`, not a booking | API key + card |
| `get_hotel_booking` | Poll the booking job until `succeeded` or `failed`. **Never retry `book_hotel` blindly** — it books the room twice | API key |
| `cancel_hotel_booking` | Release a reservation | API key |

**Account and setup**

| Tool | Description | Auth |
|------|-------------|------|
| `authenticate` | **Retired 2026-09-02** (it drove the old Stripe setup). Get the token by connecting the remote MCP instead | none |
| `setup_payment` | **[Developer API only]** Attach a payment method to the paid prepaid account. Not how agents authenticate | API key |
| `get_agent_profile` | View account info and usage stats | API key |
| `load_resources` | Load the in-server usage guide | none |

[npm page →](https://www.npmjs.com/package/letsfg-mcp)

### Which MCP path should you use?

| Path | Search mode | Auth | Best for |
|------|-------------|------|----------|
| `https://letsfg.co/developers/api/mcp` | Server-side at letsfg.co | Approve the connection; the consent step saves a card (nothing charged) | **Recommended.** No install — flights, hotels and booking over Streamable HTTP in Claude, ChatGPT, Cursor, Windsurf |
| `npx letsfg-mcp` | Server-side at letsfg.co | `LETSFG_BEARER_TOKEN` (card-backed token) or Developer API key | Clients that cannot do remote MCP |

## API Endpoints

Public REST integrations use the letsfg.co developer API:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/agents/register` | POST | Create developer account, get API key |
| `/agents/connect-payment` | POST | Mint a one-time `connect_url` to save a Revolut method. Nothing is charged to connect |
| `/agents/top-up` | POST | Fund prepaid developer balance |
| `/agents/me` | GET | Developer profile and balance |
| `/flights/search` | POST | Search flights through the public API (consumes prepaid balance) |
| `/flights/locations/{query}` | GET | Resolve city/airport codes |
| `/flights/providers` | GET | Inspect provider mix |
| `/hotels/destinations` | POST | Resolve a place name to a supplier city id |
| `/hotels/search` | POST | Search bookable, free-cancellation hotel rates |
| `/hotels/book` | POST | Start a booking (async — returns a job id) |
| `/hotels/booking/{job_id}` | GET | Collect the booking result and pay link |
| `/hotels/cancel` | POST | Release a reservation |

**Base URL:** `https://letsfg.co/developers/api/v1`

**Interactive docs:** [letsfg.co/developers/api/docs](https://letsfg.co/developers/api/docs)

See also: [Public API overview](api-guide.md), [Onboarding and Billing](api-onboarding.md), and [Search and Results](api-search.md).
