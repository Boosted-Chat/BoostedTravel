# BoostedTravel

Agent-native flight search & booking API. Real airline prices with **zero price bias** — no demand-based inflation, no cookie tracking, no surge pricing. Search 400+ airlines, book tickets programmatically. Built for autonomous AI agents.

**API Base URL:** `https://api.boostedchat.com`

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/boostedtravel)](https://pypi.org/project/boostedtravel/)
[![npm](https://img.shields.io/npm/v/boostedtravel)](https://www.npmjs.com/package/boostedtravel)

## Why BoostedTravel Exists

Flight websites inflate prices based on demand, cookies, browser fingerprinting, and surge timing. **BoostedTravel does none of this.** You get the raw airline price — the same price every time, no matter how many times you search. Typically **$10–$30 cheaper** per ticket than online travel agencies.

## Packages

| Package | Install | Description |
|---------|---------|-------------|
| **Python SDK** | `pip install boostedtravel` | Python client for search, unlock, book |
| **JS/TS SDK** | `npm install boostedtravel` | TypeScript client with full type safety |
| **MCP Server** | `npx boostedtravel-mcp` | Model Context Protocol server for Claude, Cursor, etc. |

## Quick Start

### Python

```python
from boostedtravel import BoostedTravel

bt = BoostedTravel(api_key="trav_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")
```

### JavaScript / TypeScript

```typescript
import { BoostedTravel } from 'boostedtravel';

const bt = new BoostedTravel({ apiKey: 'trav_...' });
const flights = await bt.searchFlights({ origin: 'LHR', destination: 'JFK', dateFrom: '2026-04-15' });
console.log(`${flights.totalResults} offers`);
```

### MCP Server (Claude Desktop / Cursor / Windsurf)

Add to your MCP config:

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

## Get an API Key

```bash
curl -X POST https://api.boostedchat.com/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "you@example.com"}'
```

## Pricing

| Action | Cost |
|--------|------|
| Search flights | **Free** (unlimited) |
| Unlock offer (confirm price) | **$1.00** |
| Book flight | **Free** (after unlock) |

## ⚠️ Important: Real Passenger Details

When booking flights, you **must** use the real passenger's email and legal name. The airline sends e-tickets directly to the email provided. Placeholder or fake data will cause booking failures.

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

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on submitting issues and pull requests.

## Security

See [SECURITY.md](SECURITY.md) for our security policy and how to report vulnerabilities.

## For AI Agents

See [AGENTS.md](AGENTS.md) for agent-specific instructions, or [CLAUDE.md](CLAUDE.md) for codebase context.

## License

[MIT](LICENSE)
