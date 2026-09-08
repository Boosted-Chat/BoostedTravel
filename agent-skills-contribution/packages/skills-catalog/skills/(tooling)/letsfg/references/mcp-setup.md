# LetsFG MCP Server Setup

Configure the LetsFG MCP server for your AI coding agent.

## Get an API Key First

```bash
pip install letsfg
letsfg register --name my-agent --email you@example.com
# Save the trav_xxx key

# Connect a payment method (nothing is charged; there is no unlock step)
letsfg connect-payment   # prints a link to open in a browser
```

Or via cURL:

```bash
curl -X POST https://letsfg.co/developers/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "you@example.com"}'
```

## Remote MCP (Streamable HTTP) — No Install

Works with any client that supports HTTP-based MCP.

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

## Local MCP (stdio) — Runs on Your Machine

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

## Client-Specific Setup

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

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

### Cursor

Add to `.cursor/mcp.json` in your project or `~/.cursor/mcp.json` globally:

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

### VS Code (GitHub Copilot)

Add to `.vscode/mcp.json` in your workspace:

```json
{
  "servers": {
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

### Windsurf

Add to `~/.codeium/windsurf/mcp_config.json`:

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

### Claude Code

```bash
claude mcp add letsfg -- npx -y letsfg-mcp
```

Set the API key:

```bash
export LETSFG_API_KEY=trav_your_api_key
```

## Available MCP Tools

**Flights**

| Tool | Description |
|------|-------------|
| `search_flights` | Search hundreds of airlines for flights |
| `resolve_location` | Convert city names to IATA codes |
| `unlock_flight_offer` | Confirm live price and reserve for 30 min. **[Developer API only]** — on a Bearer token call `book_flight` directly |
| `book_flight` | Book with passenger details |

**Hotels** — need a card on file (a search opens a real supplier session). Either credential works.

| Tool | Description |
|------|-------------|
| `resolve_hotel_city` | Resolve a place name to the supplier city id. Call this first |
| `search_hotels` | Search bookable, free-cancellation pay-later rates. Needs a card on file — a search opens a real supplier session |
| `book_hotel` | Book one rate. Charges 5% of the price as a non-refundable reservation fee; the balance goes to the supplier via the returned pay link. Returns a `booking_job_id` |
| `get_hotel_booking` | Poll the job until `succeeded` or `failed`. Never retry `book_hotel` blindly — it books the room twice |
| `cancel_hotel_booking` | Release a reservation |

**Account and setup**

| Tool | Description |
|------|-------------|
| `authenticate` | Returns the current connect instructions (`add_card_url`, `how`) — a person adds a card at letsfg.co/connect, nothing charged |
| `setup_payment` | **RETIRED 2026-09-08 with Stripe** — answers `410 Gone`. Connect a Revolut method with `POST /agents/connect-payment` instead |
| `get_agent_profile` | Account info and usage stats |
| `load_resources` | Load the in-server usage guide |

## Verification

After setup, ask your agent: "Search for flights from London to Barcelona on June 15th 2026"

The agent should call `resolve_location` then `search_flights` and return structured results.
