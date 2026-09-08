# Sandbox Environment

> ### ⚠️ Agents: do not create a Developer API account
>
> Do **NOT** call `/developers/api/v1/agents/register` or
> `/developers/api/v1/agents/setup-payment`, and do **NOT** run
> `letsfg register` or `letsfg setup-payment`. Those belong to a separate,
> paid, prepaid-balance product for high-volume commercial integrations and
> create a billing account you almost certainly do not want.
>
> To search and book flights, connect LetsFG as an MCP server at
> <https://letsfg.co/developers/api/mcp>. The consent step opens
> <https://letsfg.co/connect>, where a card is saved (nothing is charged).
> See <https://letsfg.co/for-agents>.

Test your integration without consuming prepaid balance or firing real connectors.
The sandbox mirrors the full Developer API surface — same endpoints, same request
schema, same response schema — but returns realistic fake data instantly.

## Base URL

```
https://letsfg.co/developers/api/v1/sandbox/
```

Add `sandbox/` between `v1/` and the endpoint name.

> ### The sandbox does not need a key — and there is only ever one key
>
> The sandbox is open: you can call it with **no API key at all**, so you can
> try LetsFG before adding a card. If you *do* send a key it must be a real,
> current one — an unknown key is rejected with exactly the 401 production
> returns, so a sandbox call can no longer pass while the same key fails live.
>
> There is **no separate sandbox key and no separate production key**, and
> nothing to activate to "switch on" production. It is one key, from
> <https://letsfg.co/developers>, for both. If production returns
> `401 This API key is not valid for any LetsFG account`, the key your code is
> sending is not the key on your account — almost always because it was
> rotated after your code was deployed. Rotating (and email recovery) issues a
> new key and kills the old one immediately. Copy the current value from the
> portal into your app.

| Real endpoint | Sandbox equivalent |
|---|---|
| `POST /v1/flights/search` | `POST /v1/sandbox/flights/search` |
| `POST /v1/flights/discover` | `POST /v1/sandbox/flights/discover` |
| `POST /v1/flights/multi-search` | `POST /v1/sandbox/flights/multi-search` |
| `POST /v1/flights/parse-query` | `POST /v1/sandbox/flights/parse-query` |
| `GET /v1/flights/locations/{q}` | `GET /v1/sandbox/flights/locations/{q}` |

## Sandbox search example

```bash
curl -X POST https://letsfg.co/developers/api/v1/sandbox/flights/search \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "origin": "JFK",
    "destination": "LAX",
    "date_from": "2026-07-15",
    "adults": 1,
    "currency": "USD"
  }'
```

Response is identical in structure to a live search — `offers[]`, `passenger_ids`,
`airlines_summary`, `total_results`, etc. — but `source_tier` is `"sandbox"` and
`pricing_note` confirms no charge was applied.

## Deterministic results

Sandbox results are seeded on `(origin, destination, date_from)`. The same query
always returns the same set of offers, so your tests are reproducible across runs.

## What's different

| Behaviour | Live | Sandbox |
|---|---|---|
| Connectors fired | Yes (hundreds) | No |
| Counts against your allowance | Yes (1 search) | No |
| Response time | 8–10 s to first results | < 1 s |
| `booking_url` | A letsfg.co page for that offer | Placeholder |
| `parse-query` NL accuracy | Full Gemini parse | Stub (returns missing fields) |
| `total_results` | Real count | Fake large number (~800–1 800) |

## Using `departure_time_from` / `departure_time_to`

Time-window filters work in sandbox exactly as in production — offers outside the
window are excluded before returning. Use this to verify your filter logic before
going live.

```json
{
  "origin": "JFK",
  "destination": "LAX",
  "date_from": "2026-07-15",
  "departure_time_from": "05:00",
  "departure_time_to": "11:00"
}
```

## Multi-search in sandbox

```bash
curl -X POST https://letsfg.co/developers/api/v1/sandbox/flights/multi-search \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "origin": "JFK",
    "destinations": ["LAX", "MIA", "ORD", "DFW", "SEA"],
    "date_from": "2026-07-15",
    "currency": "USD"
  }'
```

All destinations run in under a second. `charged_searches: 0` in the summary
confirms no allowance was used.

## Typical integration workflow

1. Build and test your full integration against the sandbox — iterate freely, no cost.
2. Verify that your code correctly reads `offers`, `passenger_ids`, and `airlines_summary`.
3. Confirm your `departure_time_from`/`to` filter logic returns the expected subset.
4. Check your credentials against a **free production** endpoint before you switch —
   `GET /v1/agents/me` costs nothing and either returns your account or tells you
   the key is wrong. This is the one thing the sandbox cannot confirm for you if
   you were calling it anonymously.
5. Switch to production endpoints (drop `sandbox/` from the path) when ready.
6. Credits are only consumed by production searches.

```bash
# Step 4 — free, no allowance consumed, no card needed
curl https://letsfg.co/developers/api/v1/agents/me \
  -H "X-API-Key: letsfg_your_api_key"
```
