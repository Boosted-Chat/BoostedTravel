# Async Search and Polling

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

The standard `POST /flights/search` blocks until results are ready (8–10 s to first results).
If your product needs to show a loading state while results arrive, use the async
flow: start the search immediately, then poll for updates.

## How it works

```
POST /flights/search/async   →  {search_id, poll_url, status: "pending"}
                                         ↓  poll every 5–10 s
GET  /flights/results/{id}   →  {status: "pending", ...}   ← still running
GET  /flights/results/{id}   →  {status: "pending", ...}
GET  /flights/results/{id}   →  {status: "complete", offers: [...]}  ← done
```

## Start an async search

```bash
curl -X POST https://letsfg.co/developers/api/v1/flights/search/async \
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

Response arrives in under a second:

```json
{
  "search_id": "async_a1b2c3d4e5f6...",
  "status": "pending",
  "poll_url": "https://letsfg.co/developers/api/v1/flights/results/async_a1b2c3d4...",
  "poll_interval_seconds": 2,
  "note": "Poll immediately, then every 2s. Results arrive in 8-10s to first results. Keep polling while split_ticket_pending or gf_enrich_pending is true."
}
```

The request body is identical to `POST /flights/search` — same fields, same
billing (one search counted on completion).

## Poll for results

```bash
curl https://letsfg.co/developers/api/v1/flights/results/async_a1b2c3d4e5f6 \
  -H "X-API-Key: letsfg_your_api_key"
```

While running:

```json
{
  "status": "pending",
  "origin": "JFK",
  "destination": "LAX",
  "offers": [],
  "total_results": 0,
  "progress": "Searching across hundreds of airlines…"
}
```

When complete:

```json
{
  "status": "complete",
  "origin": "JFK",
  "destination": "LAX",
  "offers": [ ... ],
  "total_results": 47,
  "passenger_ids": ["pas_0"],
  "airlines_summary": [ ... ],
  "currency": "USD"
}
```

## Status values

| Status | Meaning | Action |
|---|---|---|
| `pending` | Search is still running | Poll again in 5–10 s |
| `complete` | Results are ready | Read `offers` and `passenger_ids` |
| `error` | Search failed | Read `error` field; retry if transient |

## Expiry

Results are cached for **10 minutes** after the search completes. A 404 on
`/results/{id}` means the result expired — start a new search.

## JavaScript example (polling loop)

```ts
async function searchWithPolling(params: SearchParams, apiKey: string) {
  // Start search
  const startRes = await fetch(
    'https://letsfg.co/developers/api/v1/flights/search/async',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
      body: JSON.stringify(params),
    }
  )
  const { search_id, poll_url } = await startRes.json()

  // Poll until complete
  while (true) {
    await new Promise(r => setTimeout(r, 5_000))  // wait 5 s

    const pollRes = await fetch(poll_url, { headers: { 'X-API-Key': apiKey } })
    const data = await pollRes.json()

    if (data.status === 'complete') return data
    if (data.status === 'error') throw new Error(data.error)
    // status === 'pending' → loop
  }
}
```

## Python example

```python
import time, httpx

def search_with_polling(params: dict, api_key: str) -> dict:
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    base = "https://letsfg.co/developers/api/v1"

    # Start
    r = httpx.post(f"{base}/flights/search/async", json=params, headers=headers, timeout=10)
    r.raise_for_status()
    search_id = r.json()["search_id"]

    # Poll
    for _ in range(30):  # max ~150 s
        time.sleep(5)
        r = httpx.get(f"{base}/flights/results/{search_id}", headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data["status"] == "complete":
            return data
        if data["status"] == "error":
            raise RuntimeError(data["error"])

    raise TimeoutError("Search did not complete within 150 seconds")
```

## When to use async vs. blocking

| Use case | Recommended |
|---|---|
| Server-side batch processing | `POST /search` (blocking) — simpler, no state |
| User-facing product with loading UI | `POST /search/async` + polling |
| 20-destination discovery feed | `POST /multi-search` (parallel, single call) |
| Testing / integration | Sandbox (`/sandbox/flights/...`) |
