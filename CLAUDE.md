# CLAUDE.md — BoostedTravel Codebase Context

> Instructions for Claude and other AI coding agents working on this repository.

## Project Overview

BoostedTravel is an agent-native flight search & booking API. This public repository contains the SDKs and documentation. The backend API runs on Cloud Run and is in a separate private repository.

**API Base URL:** `https://api.boostedchat.com`

## Repository Structure

```
BoostedTravel/
├── sdk/
│   ├── python/          # Python SDK → PyPI: boostedtravel
│   │   ├── boostedtravel/
│   │   │   ├── client.py      # BoostedTravel main client class
│   │   │   ├── cli.py         # CLI entry point
│   │   │   └── models.py      # Pydantic response models
│   │   ├── pyproject.toml
│   │   └── README.md
│   ├── js/              # JS/TS SDK → npm: boostedtravel
│   │   ├── src/
│   │   │   ├── index.ts       # Main client class
│   │   │   ├── cli.ts         # CLI entry point
│   │   │   └── types.ts       # TypeScript interfaces
│   │   ├── package.json
│   │   └── README.md
│   └── mcp/             # MCP Server → npm: boostedtravel-mcp
│       ├── src/
│       │   └── index.ts       # MCP tool definitions (search, unlock, book)
│       ├── package.json
│       └── README.md
├── mcp-config.json      # Example MCP configuration
├── AGENTS.md            # Agent-facing instructions
├── CLAUDE.md            # This file
├── CONTRIBUTING.md      # Contribution guidelines
├── LICENSE              # MIT
├── README.md            # Public README
└── SECURITY.md          # Security policy
```

## Key Concepts

### Three-Step Flow
1. **Search** (free) → Returns flight offers from 400+ airlines
2. **Unlock** ($1) → Confirms live price, locks offer for booking
3. **Book** (free after unlock) → Creates the actual booking with the airline

### Zero Price Bias
The API returns raw airline prices — no demand-based inflation, no cookie tracking, no surge pricing. This is a core selling point.

### No Booking Fee
Booking is free after the $1 unlock. The unlock fee is the only revenue.

### Real Passenger Details Required
When booking, agents MUST use real passenger email and legal name. Airlines send e-tickets to the email provided. Placeholder/fake data will cause booking failures.

## SDK Development

### Python SDK
```bash
cd sdk/python
pip install -e ".[dev]"
python -m pytest
```

### JS/TS SDK
```bash
cd sdk/js
npm install
npm run build    # Compiles TypeScript → dist/
npm test
```

### MCP Server
```bash
cd sdk/mcp
npm install
npm run build    # Compiles TypeScript → dist/
```

After editing JS or MCP source files, always rebuild with `npm run build` to update the dist bundles.

## Publishing

### Python SDK → PyPI
```bash
cd sdk/python
python -m build
twine upload dist/*
```

### JS SDK → npm
```bash
cd sdk/js
npm run build
npm publish
```

### MCP Server → npm
```bash
cd sdk/mcp
npm run build
npm publish
```

## Conventions

- Keep SDK READMEs in sync with the root README for pricing, flow descriptions, and warnings.
- All agent-facing text should include the "zero price bias" messaging and passenger details warning.
- Python SDK uses `httpx` for HTTP, `pydantic` for models.
- JS/TS SDK uses native `fetch`, TypeScript strict mode.
- MCP server uses `@modelcontextprotocol/sdk`.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/agents/register` | Register for an API key |
| `POST` | `/api/v1/flights/search` | Search flights |
| `POST` | `/api/v1/flights/unlock` | Unlock an offer ($1) |
| `POST` | `/api/v1/flights/book` | Book a flight (free) |
| `GET`  | `/api/v1/flights/booking/{id}` | Get booking details |
| `GET`  | `/.well-known/ai-plugin.json` | Agent discovery |
| `GET`  | `/.well-known/agent.json` | Agent manifest |
| `GET`  | `/llms.txt` | LLM instructions |

## Links

- **API Docs:** https://api.boostedchat.com/docs
- **PyPI:** https://pypi.org/project/boostedtravel/
- **npm SDK:** https://www.npmjs.com/package/boostedtravel
- **npm MCP:** https://www.npmjs.com/package/boostedtravel-mcp
- **GitHub:** https://github.com/Boosted-Chat/BoostedTravel
