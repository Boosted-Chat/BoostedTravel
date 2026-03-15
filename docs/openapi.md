# OpenAPI Reference

BoostedTravel provides a full OpenAPI 3.1 specification for the REST API.

## Interactive Documentation

- **Swagger UI:** [api.boostedchat.com/docs](https://api.boostedchat.com/docs) — try every endpoint in your browser
- **ReDoc:** [api.boostedchat.com/redoc](https://api.boostedchat.com/redoc) — clean, readable API reference

## OpenAPI Spec

The full OpenAPI specification is included in the repository:

- **YAML:** [`openapi.yaml`](https://github.com/Boosted-Chat/BoostedTravel/blob/main/openapi.yaml)

You can import this spec into any OpenAPI-compatible tool (Postman, Insomnia, Swagger Editor, etc.).

## Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/v1/agents/register` | POST | None | Create account, get API key |
| `/api/v1/agents/me` | GET | API key | Agent profile & usage stats |
| `/api/v1/agents/setup-payment` | POST | API key | Attach Stripe payment method |
| `/api/v1/flights/search` | POST | API key | Search 400+ airlines |
| `/api/v1/flights/resolve-location` | GET | API key | Resolve city/airport to IATA codes |
| `/api/v1/bookings/unlock` | POST | API key | Unlock offer ($1) |
| `/api/v1/bookings/book` | POST | API key | Book flight (free after unlock) |

**Base URL:** `https://api.boostedchat.com`

## Authentication

All endpoints except `/register` require an `X-API-Key` header:

```bash
curl -X POST https://api.boostedchat.com/api/v1/flights/search \
  -H "X-API-Key: trav_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"origin": "LHR", "destination": "JFK", "date_from": "2026-04-15"}'
```

## Agent Discovery

BoostedTravel supports standard agent discovery protocols:

| URL | Description |
|-----|-------------|
| `https://api.boostedchat.com/.well-known/ai-plugin.json` | OpenAI plugin manifest |
| `https://api.boostedchat.com/.well-known/agent.json` | Agent discovery manifest |
| `https://api.boostedchat.com/llms.txt` | LLM instructions |
| `https://api.boostedchat.com/mcp` | MCP Streamable HTTP endpoint |

## Local Search (No API Key)

The 73 local airline connectors do not use the REST API — they run directly on your machine. No API key is needed:

```bash
pip install boostedtravel
boostedtravel search-local LHR BCN 2026-04-15
```

```python
from boostedtravel.local import search_local
result = await search_local("LHR", "BCN", "2026-04-15")
```
