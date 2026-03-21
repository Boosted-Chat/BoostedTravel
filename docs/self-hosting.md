# Self-Hosting: HTTP Endpoints for Local Connectors

Deploy LetsFG's 150+ airline connectors as HTTP endpoints on your own infrastructure. This guide covers wrapping the local search in a web server and calling it from any backend (Node.js, Next.js, Go, etc.).

---

## Quick Start: FastAPI Server

The fastest way to expose local connectors as an HTTP API.

### Install

```bash
pip install letsfg fastapi uvicorn
playwright install chromium
```

### Server (`server.py`)

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from letsfg.local import search_local

app = FastAPI(title="LetsFG Local Search")


class SearchRequest(BaseModel):
    origin: str
    destination: str
    date_from: str
    return_date: str | None = None
    adults: int = 1
    children: int = 0
    infants: int = 0
    currency: str = "EUR"
    limit: int = 50
    max_browsers: int | None = None


@app.post("/search")
async def search(req: SearchRequest):
    try:
        result = await search_local(
            origin=req.origin,
            destination=req.destination,
            date_from=req.date_from,
            return_date=req.return_date,
            adults=req.adults,
            children=req.children,
            infants=req.infants,
            currency=req.currency,
            limit=req.limit,
            max_browsers=req.max_browsers,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok"}
```

### Run

```bash
uvicorn server:app --host 0.0.0.0 --port 8000
```

### Test

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"origin": "LHR", "destination": "BCN", "date_from": "2026-06-15"}'
```

---

## Flask Alternative

If you prefer Flask (lighter, simpler):

```python
import asyncio
from flask import Flask, request, jsonify
from letsfg.local import search_local

app = Flask(__name__)


@app.route("/search", methods=["POST"])
def search():
    data = request.get_json()
    result = asyncio.run(search_local(
        origin=data["origin"],
        destination=data["destination"],
        date_from=data["date_from"],
        return_date=data.get("return_date"),
        adults=data.get("adults", 1),
        children=data.get("children", 0),
        infants=data.get("infants", 0),
        currency=data.get("currency", "EUR"),
        limit=data.get("limit", 50),
        max_browsers=data.get("max_browsers"),
    ))
    return jsonify(result)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})
```

```bash
pip install letsfg flask gunicorn
gunicorn server:app --bind 0.0.0.0:8000 --workers 2 --timeout 120
```

> **Note:** Use `--timeout 120` — searches can take 10-30 seconds depending on the number of connectors matching the route.

---

## Calling from Node.js / Next.js

Once your Python server is running, call it from any backend:

### Node.js (fetch)

```javascript
const response = await fetch("http://localhost:8000/search", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
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

const LETSFG_URL = process.env.LETSFG_URL || "http://localhost:8000";

export async function POST(req: NextRequest) {
  const body = await req.json();

  const res = await fetch(`${LETSFG_URL}/search`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
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
  elapsed_seconds: number;
}
```

---

## Deployment on Dokku

### Procfile

```
web: uvicorn server:app --host 0.0.0.0 --port $PORT --workers 2 --timeout 120
```

### Aptfile (for Playwright system deps)

Create an `Aptfile` in your repo root or use the `heroku-buildpack-apt`:

```
libnss3
libnspr4
libatk1.0-0
libatk-bridge2.0-0
libcups2
libdrm2
libdbus-1-3
libxkbcommon0
libxcomposite1
libxdamage1
libxfixes3
libxrandr2
libgbm1
libpango-1.0-0
libcairo2
libasound2
```

### Dockerfile (recommended for Dokku)

```dockerfile
FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libdbus-1-3 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 \
    libasound2 fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

COPY . .

EXPOSE 8000
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--timeout-keep-alive", "120"]
```

### requirements.txt

```
letsfg
fastapi
uvicorn
```

### Deploy

```bash
git push dokku main
```

---

## Performance Tips

### Split Connectors Across Instances

For maximum throughput, clone the LetsFG repository directly and split connectors into groups deployed on separate Dokku apps:

```
letsfg-eu (European carriers: Ryanair, EasyJet, Wizz, Norwegian, etc.)
letsfg-asia (Asian carriers: AirAsia, Cebu Pacific, IndiGo, etc.)
letsfg-americas (Americas: Southwest, JetBlue, Volaris, etc.)
letsfg-gds (GDS/NDC connectors: Amadeus, Duffel, etc.)
```

Then aggregate results in your backend:

```javascript
// Call all connector groups in parallel
const groups = ["http://letsfg-eu:8000", "http://letsfg-asia:8000", "http://letsfg-americas:8000"];

const results = await Promise.all(
  groups.map((url) =>
    fetch(`${url}/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ origin: "LHR", destination: "BKK", date_from: "2026-06-15" }),
    }).then((r) => r.json())
  )
);

// Merge and sort by price
const allOffers = results.flatMap((r) => r.offers);
allOffers.sort((a, b) => a.price - b.price);
```

### Concurrency Tuning

- **2 GB RAM VPS:** `max_browsers=2` — slow but works
- **4 GB RAM VPS:** `max_browsers=4` — good balance
- **8 GB+ RAM VPS:** `max_browsers=8` or omit (auto-detect) — fast
- Set via request param or environment variable: `LETSFG_MAX_BROWSERS=4`

### Caching

Cache search results for 5-15 minutes — flight prices don't change that frequently and it reduces load on your server:

```python
from functools import lru_cache
from datetime import datetime
import hashlib, json

_cache = {}

async def cached_search(**params):
    key = hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()
    now = datetime.now().timestamp()
    if key in _cache and now - _cache[key]["ts"] < 600:  # 10 min
        return _cache[key]["data"]
    result = await search_local(**params)
    _cache[key] = {"data": result, "ts": now}
    return result
```

### Health Checks

For Dokku/Docker health checks, the `/health` endpoint returns `{"status": "ok"}`. Configure your container orchestrator to check it:

```bash
# Dokku
dokku checks:set letsfg-search CHECKS /health
```

---

## Response Format

All endpoints return the same JSON structure:

```json
{
  "search_id": "abc123",
  "total_results": 42,
  "elapsed_seconds": 12.5,
  "offers": [
    {
      "id": "offer_xyz",
      "price": 45.99,
      "currency": "EUR",
      "airlines": ["FR"],
      "source": "ryanair",
      "outbound": {
        "segments": [
          {
            "airline": "FR",
            "flight_no": "FR1234",
            "origin": "STN",
            "destination": "BCN",
            "departure": "2026-06-15T06:30:00",
            "arrival": "2026-06-15T09:45:00"
          }
        ],
        "total_duration_seconds": 9900,
        "stopovers": 0
      },
      "booking_url": "https://www.ryanair.com/..."
    }
  ]
}
```

---

## Need Help?

We provide hands-on support for deployment and integration:

- **Email:** contact@letsfg.co
- **Response time:** Typically under 1 hour
- **GitHub Issues:** [github.com/LetsFG/LetsFG/issues](https://github.com/LetsFG/LetsFG/issues)
