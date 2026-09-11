# Backend Integration

Integrate the LetsFG API into your own backend or proxy layer. All flight search runs
server-side at letsfg.co — no local connectors or browser automation required.

---

## Quick Start: FastAPI Proxy

Wrap the LetsFG API in a FastAPI service to add caching, auth, or custom logic.

### Install

```bash
pip install fastapi uvicorn httpx
```

### Server (`server.py`)

```python
import os

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="LetsFG Proxy")
LETSFG_API_KEY = os.environ["LETSFG_API_KEY"]  # never commit the key itself
LETSFG_BASE = "https://letsfg.co/developers/api/v1"


class SearchRequest(BaseModel):
    origin: str
    destination: str
    date_from: str
    return_date: str | None = None
    adults: int = 1
    children: int = 0
    currency: str = "EUR"
    limit: int = 50


@app.post("/search")
async def search(req: SearchRequest):
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            f"{LETSFG_BASE}/flights/search",
            json=req.model_dump(exclude_none=True),
            headers={"X-API-Key": LETSFG_API_KEY},
        )
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=r.text)
        return r.json()


@app.get("/health")
async def health():
    return {"status": "ok"}
```

### Run

```bash
export LETSFG_API_KEY="<your-api-key>"
uvicorn server:app --host 0.0.0.0 --port 8000
```

### Test

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"origin": "LHR", "destination": "BCN", "date_from": "2026-06-15"}'
```

---

## Calling from Node.js / Next.js

Call the LetsFG API directly or via your proxy.

### Node.js (fetch)

```javascript
const response = await fetch("https://letsfg.co/developers/api/v1/flights/search", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "X-API-Key": process.env.LETSFG_API_KEY,
  },
  body: JSON.stringify({
    origin: "LHR",
    destination: "BCN",
    date_from: "2026-06-15",
    adults: 1,
    currency: "EUR",
  }),
});

const data = await response.json();
console.log(`Found ${data.total_results} offers`);
for (const offer of data.offers.slice(0, 5)) {
  console.log(`  ${offer.price} ${offer.currency} — ${offer.airlines.join(", ")}`);
}
```

### Next.js API Route (App Router)

```typescript
// app/api/flights/route.ts
import { NextRequest, NextResponse } from "next/server";

const LETSFG_BASE = "https://letsfg.co/developers/api/v1";

export async function POST(req: NextRequest) {
  const body = await req.json();

  const res = await fetch(`${LETSFG_BASE}/flights/search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": process.env.LETSFG_API_KEY!,
    },
    body: JSON.stringify({
      origin: body.origin,
      destination: body.destination,
      date_from: body.date_from,
      return_date: body.return_date,
      adults: body.adults || 1,
      currency: body.currency || "EUR",
      limit: body.limit || 50,
    }),
  });

  const data = await res.json();
  return NextResponse.json(data);
}
```

### TypeScript Types

```typescript
interface FlightOffer {
  id: string;
  price: number;
  currency: string;
  airlines: string[];
  outbound: FlightRoute;
  inbound?: FlightRoute;
  booking_url?: string;
  source: string;
}

interface FlightRoute {
  segments: FlightSegment[];
  total_duration_seconds: number;
  stopovers: number;
}

interface FlightSegment {
  airline: string;
  flight_no: string;
  origin: string;
  destination: string;
  departure: string; // ISO datetime
  arrival: string;   // ISO datetime
}

interface SearchResult {
  search_id: string;
  total_results: number;
  offers: FlightOffer[];
  passenger_ids: string[];
}
```

---

## Async Search (Polling)

For user-facing products that need a loading state, use the async endpoint:

```typescript
async function searchWithPolling(params: object, apiKey: string) {
  const base = "https://letsfg.co/developers/api/v1";

  // Start async search
  const startRes = await fetch(`${base}/flights/search/async`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-API-Key": apiKey },
    body: JSON.stringify(params),
  });
  const { search_id } = await startRes.json();

  // Poll until complete
  while (true) {
    await new Promise(r => setTimeout(r, 5_000));
    const pollRes = await fetch(`${base}/flights/results/${search_id}`, {
      headers: { "X-API-Key": apiKey },
    });
    const data = await pollRes.json();
    if (data.status === "complete") return data;
    if (data.status === "error") throw new Error(data.error);
  }
}
```

---

## Caching

Cache search results to reduce how fast you use your search allowance. Prices are stable for 5-15 minutes.

```python
import hashlib, json, time

_cache: dict = {}

async def cached_search(api_key: str, **params) -> dict:
    key = hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()
    now = time.time()
    if key in _cache and now - _cache[key]["ts"] < 600:  # 10 min TTL
        return _cache[key]["data"]
    result = await call_letsfg_api(api_key, **params)
    _cache[key] = {"data": result, "ts": now}
    return result
```

---

## Environment Variable

Store your API key securely:

```bash
# .env
LETSFG_API_KEY=letsfg_your_api_key
```

Never hardcode API keys in source code or commit them to version control.
