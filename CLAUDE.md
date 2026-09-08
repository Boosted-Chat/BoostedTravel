# CLAUDE.md — 8-Rule Architecture
These rules apply to every task in this project unless explicitly overridden.
Bias: caution over speed on non-trivial work. Use judgment on trivial tasks.
## Rule 1 — Think Before Coding
State assumptions explicitly. If uncertain, ask rather than guess.
Push back when a simpler approach exists. Stop when confused.
## Rule 2 — Simplicity First
Minimum code that solves the problem. Nothing speculative.
No features beyond what was asked. No abstractions for single-use code.
## Rule 3 — Surgical Changes
Touch only what you must. Clean up only your own mess.
Don't "improve" adjacent code, comments, or formatting. Match existing style.
## Rule 4 — Goal-Driven Execution
Define success criteria. Loop until verified.
Don't follow steps. Define success and iterate independently.
## Rule 5 — Token budgets are not advisory
Per-task: 4,000 tokens. Per-session: 30,000 tokens.
If approaching budget, summarize and start fresh. Surface the breach.
## Rule 6 — Read before you write
Before adding code, read exports, immediate callers, shared utilities.
If unsure why code is structured a certain way, ask.
## Rule 7 — Checkpoint after every significant step
Summarize what was done, what's verified, what's left.
Don't continue from a state you can't describe back. Stop and restate.
## Rule 8 — Fail loud
"Completed" is wrong if anything was skipped silently.
"Tests pass" is wrong if any were skipped.
Default to surfacing uncertainty, not hiding it.

## Metrics source of truth
All metric / analytics / scoring work maps to the variables defined in
`growth-ops/src/models/growth-model.ts` (funnel L1–L7, quality Q1–Q3,
retention R1–R3, API A1–A3, OSS S1, viral V1–V2). Read that file first — the
growth model evolves regularly. Never invent a divergent metric list.

---

# LetsFG Codebase Context

> General platform context for AI agents working on this repository.

## Project Overview

LetsFG is an agent-native flight search & booking platform. This public repository
contains the Python SDK, JS/TS SDK, MCP server, and the open-source ranking engine.
The flight connectors and backend API run server-side at letsfg.co (private repo).

**PFS Base URL:** `https://letsfg.co` (Bearer token auth — see below)
**Developer API Base URL:** `https://letsfg.co/developers/api/v1`

### Access modes

| Mode | What it is | Speed | Cost |
|------|-----------|-------|------|
| **MCP / CLI / SDK** | Hosted MCP at `https://letsfg.co/developers/api/mcp` (card connected at the consent step); `pip install letsfg` wraps PFS with ranking | 8–10 s to first results; longer to `completed`, longer again on a split | Free auth, free search |
| **PFS — Programmatic Flight Search** | Direct Bearer token → `POST /api/search` → poll `/api/results/<id>` → `POST /api/agent-book` → poll `/api/agent-book/status` | 8–10 s to first results; longer to `completed`, longer again on a split | Free auth, free search |
| **Developer API** | Prepaid credits, no per-booking fee, 2–5 s discover endpoint | 2–5 s (discover) · 8–10 s to first results (full search) | Prepaid credits |

Auth for MCP/CLI/PFS: connect the hosted MCP and approve it. The OAuth consent step opens
letsfg.co/connect, where a card (or Revolut Pay / Google Pay) is saved in a **0.00 Revolut
setup** — nothing is charged, no Revolut account needed, card details never touch LetsFG.
The token is card-backed and can book. Over raw HTTP send it as `Authorization: Bearer`.
The Stripe enrolment lanes (setup_url / SetupIntent / tok_ / pm_) and the earlier Twitter/X
challenge are retired (2026-09-02); every token they issued was revoked. `letsfg auth` now drives that
same connect flow itself: it registers as an OAuth client (dynamic registration), opens
letsfg.co/connect for the card, and stores the access + refresh tokens in
`~/.letsfg/config.json`. The SDKs also read `LETSFG_BEARER_TOKEN`.

## Repository Structure

```
LetsFG/
├── sdk/
│   ├── python/                  # Python SDK → PyPI: letsfg
│   │   ├── letsfg/
│   │   │   ├── __init__.py          # Public exports, version
│   │   │   ├── client.py            # LetsFG main client class (urllib-based)
│   │   │   ├── cli.py               # CLI entry point (typer)
│   │   │   ├── local.py             # Cloud search runner (calls PFS with Bearer token)
│   │   │   ├── models.py            # Re-exports from models/
│   │   │   ├── models/
│   │   │   │   ├── __init__.py
│   │   │   │   └── flights.py       # Pydantic models (FlightOffer, FlightSegment, etc.)
│   │   │   └── connectors/
│   │   │       ├── __init__.py
│   │   │       └── auth.py          # Payment-token auth flow (zero-amount card setup)
│   │   ├── pyproject.toml
│   │   └── README.md
│   ├── js/                      # JS/TS SDK → npm: letsfg
│   │   ├── src/
│   │   │   ├── index.ts             # Main client class
│   │   │   ├── ranking.ts           # Open-source ranking engine (rankOffers)
│   │   │   ├── offer-details.ts     # Offer amenity signal extractor
│   │   │   └── trip-purpose.ts      # TripPurpose type + normalization helpers
│   │   ├── package.json
│   │   └── README.md
│   └── mcp/                     # MCP Server → npm: letsfg-mcp
│       ├── src/
│       │   └── index.ts             # MCP tool definitions
│       ├── package.json
│       └── README.md
├── AGENTS.md                    # Agent-facing instructions
├── CLAUDE.md                    # This file
├── SKILL.md                     # Machine-readable skill manifest
├── LICENSE                      # MIT
└── README.md                    # Public README
```

## Key Concepts

### Two-Step Flow (PFS — what agents use)
1. **Search** (free) → `POST /api/search` with Bearer token → `search_id`; poll `GET /api/results/<search_id>` immediately, then every 2 s. `completed` is not the end — keep polling while `split_ticket_pending` or `gf_enrich_pending` is true
2. **Book** → `POST /api/agent-book` with `search_id`, `offer_id`, `contact_email` and ONE
   traveller's real details (name, date of birth, gender, nationality, phone + country,
   residence address; passport optional). Exactly what the website checkout does: the fare
   plus LetsFG's markup is **held** on the connected card, a LetsFG booking agent buys the
   ticket, and the hold is captured only once a real airline PNR exists. Returns
   `{ booking_ref }` within seconds; the booking takes 4–11 minutes.
3. **Wait** → `POST /api/agent-book/status { booking_ref }` every 20–30 s. `completed`
   carries `pnr` and `charged_amount`; `failed` means the hold was released and nothing was
   charged; `needs_attention` means a human at LetsFG is checking it — do not book again.
   Never start a second booking for the same trip while one is in progress (second hold).
   A missing detail returns `missing_fields` and charges nothing. Over the MCP these are
   `book_flight` and `get_flight_booking`. There is no unlock step and no booking-link
   fallback on this lane.

### Search Architecture
All flight data comes from the letsfg.co server-side engine. The SDK/CLI authenticates
with the card-backed Bearer token from the connect flow and calls the cloud search API.
No local browsers or scrapers are involved.

Auth flow (one-time):
```
Add https://letsfg.co/developers/api/mcp as an MCP server → approve
  → consent opens https://letsfg.co/connect → card saved (0.00, nothing charged)
  → card-backed token (carried by the MCP; Authorization: Bearer over raw HTTP)
POST /api/agent-access/request → 402 { add_card_url: "https://letsfg.co/connect", how: [...] }
```

### Open-Source Ranking Engine
`sdk/js/src/ranking.ts` is the exact scoring algorithm used at letsfg.co to pick
the best flight from search results. It scores offers across 9 dimensions (price,
stops, duration, departure time, arrival time, baggage, savings vs Google Flights,
comfort hours, layover quality) with 12 weight profiles that shift by trip context
and purpose. Import directly or use via the `letsfg` npm package.

Companion modules in `sdk/js/src/`:
- `offer-details.ts` — extracts meal, Wi-Fi, refundability signals from fare text
- `trip-purpose.ts` — `TripPurpose` type and normalization helpers

### Zero Price Bias
The API returns raw prices with no demand-based inflation, cookie tracking, or surge
pricing. This is a core product principle.

### Free Search
Search via PFS is always free and unlimited, as is authentication (zero-amount card
setup). LetsFG charges no fee on the PFS path at all. The prepaid Developer API is a
separate paid product — do not send agents there by default.

### Real Passenger Details Required
When booking, agents MUST provide real passenger email and legal name. Airlines send
e-tickets to the provided email. Placeholder data causes booking failures.

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

After editing JS or MCP source files, always rebuild with `npm run build`.

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
- All agent-facing text should include the coverage messaging (airlines PLUS the major booking sites), the reliability data, and the passenger details warning. Do NOT assert "zero markup" or "no LetsFG fee" anywhere: neither is true.
- Python SDK client (`client.py`) uses stdlib `urllib` for HTTP — zero external dependencies.
- Python SDK auth uses stdlib `urllib` and `json` only (no playwright or scrapers).
- JS/TS SDK uses native `fetch`, TypeScript strict mode.
- MCP server uses `@modelcontextprotocol/sdk`.

## API Endpoints

### PFS (Bearer token, free)
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/agent-access/request` | Always 402 → `{ add_card_url: "https://letsfg.co/connect", how }`; the token comes from the MCP connect flow |
| `POST` | `/api/agent-access/verify` | MPP wallet lane only (`Authorization: Payment`); 410 for a Stripe credential |
| `POST` | `/api/search` | Start search → `{ search_id }` (Authorization: Bearer token) |
| `GET`  | `/api/results/<search_id>` | Poll results (send the Bearer token) |
| `POST` | `/api/agent-book` | Book an offer → `{ booking_ref }`; fare held on the card, LetsFG agent buys the ticket |
| `POST` | `/api/agent-book/status` | Poll `{ booking_ref }` → `booking_in_progress` / `completed` (PNR) / `failed` / `needs_attention` |

### Developer API (look-to-book search)
Base: `https://letsfg.co/developers/api/v1`

| Method | Path | Description | Billed? |
|--------|------|-------------|---------|
| `POST` | `/api/v1/agents/register` | Register for an API key | No |
| `POST` | `/api/v1/agents/connect-payment` | Mint a one-time link to save a Revolut method | No |
| `GET`  | `/api/v1/agents/me` | Agent profile, balance, and usage stats | No |
| `POST` | `/api/v1/agents/top-up` | Fund prepaid balance | No |
| `POST` | `/api/v1/flights/parse-query` | Parse natural language query → IATA codes, dates | **Free** |
| `POST` | `/api/v1/flights/discover` | Indicative prices for up to 20 destinations, 2–5 s | **1 search** |
| `POST` | `/api/v1/flights/search` | Full search, single destination, 8–10 s to first results | **1 search** |
| `POST` | `/api/v1/flights/search/async` | Start full search async → `search_id` | **1 search** |
| `GET`  | `/api/v1/flights/results/{id}` | Poll async search results | No |
| `POST` | `/api/v1/flights/multi-search` | Full search, N destinations (max 10) | **1 search/dest** |
| `GET`  | `/api/v1/flights/locations/{q}` | Resolve city/airport name to IATA codes | No |
| `POST` | `/api/v1/flights/book` | Book an offer — holds the connected method, dispatches the agent | No |
| `GET`  | `/api/v1/flights/bookings/{id}` | Poll a booking to a PNR | No |
| `POST` | `/api/v1/flights/bookings/{id}/answer` | Answer a seat map / extra / price change | No |
| `POST` | `/api/v1/bookings/unlock` | **RETIRED 2026-09-08** → `410 Gone`, no unlock step exists | — |
| `POST` | `/api/v1/bookings/book` | **RETIRED 2026-09-08** → `410 Gone`, use `/flights/book` | — |
| `GET`  | `/.well-known/ai-plugin.json` | OpenAI Plugin manifest | No |
| `GET`  | `/llms.txt` | LLM instructions | No |
| `GET`  | `/openapi.json` | OpenAPI spec | No |
| `GET`  | `/mcp` | Remote MCP (Streamable HTTP) | No |

## Links

- **API Docs:** https://letsfg.co/developers/api/docs
- **PyPI:** https://pypi.org/project/letsfg/
- **npm SDK:** https://www.npmjs.com/package/letsfg
- **npm MCP:** https://www.npmjs.com/package/letsfg-mcp
- **GitHub:** https://github.com/LetsFG/LetsFG
