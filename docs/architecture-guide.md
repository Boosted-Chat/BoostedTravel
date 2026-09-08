# Architecture & Resilience Guide

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

Deep dive into how LetsFG's server-side search engine works — connector orchestration, failure handling, caching strategies, and performance optimization. All search runs on letsfg.co infrastructure.

## Search Engine Architecture

When you call `bt.search()`, LetsFG's server-side engine fires **all** relevant data sources in parallel and merges the results. The engine never waits for one source before starting another.

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Your Application / Agent                          │
│               bt.search() / MCP tool call                            │
├──────────────────────────────────────────────────────────────────────┤
│              LetsFG Server-Side MultiProvider Engine                 │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────────┐  │
│  │ GDS/NDC APIs  │  │ Fast Connectors│ │  Airline connectors       │  │
│  │ (Amadeus,     │  │ (Ryanair,     │  │  (EasyJet, Spirit,       │  │
│  │  Duffel,      │  │  Wizzair,     │  │   Southwest, IndiGo,     │  │
│  │  Sabre, etc.) │  │  Kiwi.com)    │  │   Delta, American, ...)  │  │
│  └──────┬───────┘  └──────┬───────┘  └────────────┬──────────────┘  │
│         │                  │                        │                 │
│         └──────────────────┴────────────────────────┘                 │
│                    asyncio.gather(return_exceptions=True)             │
│                              ↓                                       │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │           Merge → Deduplicate → Normalize → Sort             │    │
│  │           Virtual Interlining (cross-airline combos)          │    │
│  │           Airline-diverse selection                           │    │
│  └──────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

### Three Source Categories

| Category | How it runs | Speed | Example sources |
|----------|-------------|-------|-----------------|
| **Cloud backend** | Single HTTP POST to LetsFG API; server queries all GDS/NDC providers | 2-10s | Amadeus, Duffel, Sabre, Travelport, Kiwi |
| **Fast connectors** | Direct HTTP API calls (no browser) | 0.5-3s | Ryanair, Wizzair, Kiwi.com |
| **Airline connectors** | Browser automation or reverse-engineered APIs | 3-30s | EasyJet, Southwest, Spirit, Delta |

All three categories fire simultaneously. Total wall-clock time equals the **slowest** source, not the sum.

If some sources fail (timeouts, bot detection, API errors), the engine continues with results from the remaining sources. The search never fails completely unless every source fails.

## Failure Handling

```
┌─ Search Request ──────────────────────────────────────────┐
│                                                            │
│  Connector A ──→ ✅ 12 offers                             │
│  Connector B ──→ ❌ TimeoutError (logged, skipped)        │
│  Connector C ──→ ✅ 3 offers                              │
│  Connector D ──→ ❌ Bot detection (logged, skipped)       │
│  Connector E ──→ ✅ 8 offers                              │
│  Backend API ──→ ✅ 45 offers (from Amadeus + Duffel)     │
│                                                            │
│  Result: 68 offers merged from 4 sources                   │
│  (2 failures logged but don't affect the response)         │
└────────────────────────────────────────────────────────────┘
```

The `FlightSearchResponse` includes a `source_tiers` field showing which sources contributed:

```python
result = bt.search("LHR", "BCN", "2026-06-01")
print(result.source_tiers)
# {"free": "ryanair_direct, easyjet_direct, vueling_direct, kiwi_connector"}
```

### Handling Incomplete Data

When a connector returns partial or malformed data:

1. **Missing prices** — Offers without valid prices are filtered out during merge
2. **Missing segments** — Offers without route information are dropped
3. **Wrong currency** — The engine normalizes all prices to the requested currency via live exchange rates
4. **Duplicate offers** — The deduplication engine identifies offers with the same route, timing, and price (within tolerance) and keeps only the best

```python
# Deduplication key: route + timing + airline
def _dedup_key(offer):
    segments = offer.outbound.segments
    return f"{segments[0].origin}-{segments[-1].destination}-" \
           f"{segments[0].departure[:16]}-{offer.owner_airline}"
```

## Caching and Rate Limit Strategy

### Rate Limit Handling

The LetsFG cloud API enforces per-agent rate limits:

| Endpoint | Rate Limit | Typical Latency |
|----------|-----------|------------------|
| Search | 60 req/min | 2-15s |
| Resolve location | 120 req/min | < 1s |
| Unlock | 20 req/min | 2-5s |
| Book | 10 req/min | 3-10s |

When rate limited (HTTP 429), use exponential backoff:

```python
import time
from letsfg import LetsFG, LetsFGError

bt = LetsFG()

def search_with_backoff(origin, dest, date, max_retries=3):
    for attempt in range(max_retries):
        try:
            return bt.search(origin, dest, date)
        except LetsFGError as e:
            if e.status_code == 429:
                wait = 2 ** attempt  # 1s, 2s, 4s
                time.sleep(wait)
            elif e.is_retryable:
                time.sleep(1)
            else:
                raise
    raise LetsFGError("Max retries exceeded")
```

### Designing a Caching Layer for Multi-User Applications

If you're building an application that serves multiple concurrent users, implement a caching layer between your users and the LetsFG API:

```python
import asyncio
import hashlib
import time
from letsfg import LetsFG

bt = LetsFG()

class FlightSearchCache:
    """In-memory cache for flight search results with TTL-based expiration."""

    def __init__(self, ttl_seconds: int = 300):
        self._cache: dict[str, tuple[float, dict]] = {}
        self._ttl = ttl_seconds
        self._locks: dict[str, asyncio.Lock] = {}

    def _cache_key(self, origin, dest, date, **kwargs):
        raw = f"{origin}:{dest}:{date}:{sorted(kwargs.items())}"
        return hashlib.md5(raw.encode()).hexdigest()

    async def search(self, origin, dest, date, **kwargs):
        key = self._cache_key(origin, dest, date, **kwargs)

        # Return cached result if fresh
        if key in self._cache:
            cached_time, cached_result = self._cache[key]
            age = time.time() - cached_time
            if age < self._ttl:
                return {**cached_result, "_cache": "hit", "_age_seconds": round(age)}

        # Deduplicate concurrent requests for the same search
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        async with self._locks[key]:
            # Double-check after acquiring lock
            if key in self._cache:
                cached_time, cached_result = self._cache[key]
                if time.time() - cached_time < self._ttl:
                    return {**cached_result, "_cache": "hit"}

            # Execute actual search (server-side at letsfg.co)
            result = await asyncio.to_thread(bt.search, origin, dest, date)
            self._cache[key] = (time.time(), result)
            return {**result, "_cache": "miss"}

    def invalidate(self, origin=None, dest=None):
        """Remove cached entries. Call when you know prices changed."""
        if origin is None and dest is None:
            self._cache.clear()
            return
        to_remove = [
            k for k, (_, r) in self._cache.items()
            if (origin and r.get("origin") == origin)
            or (dest and r.get("destination") == dest)
        ]
        for k in to_remove:
            del self._cache[k]
```

#### Cache TTL Recommendations

| Use case | Recommended TTL | Rationale |
|----------|----------------|-----------|
| Real-time price display | 2-5 minutes | Airline prices change frequently |
| Price comparison dashboard | 10-15 minutes | Good balance of freshness and performance |
| Price tracking / alerts | 30-60 minutes | Alerts don't need second-level precision |
| Historical analysis | 24 hours | Trends over days, not minutes |

**Important: there is no unlock step on either lane any more.** `POST /bookings/unlock` was retired on 2026-09-08 and answers `410 Gone`. Both lanes now work the way the PFS lane always did: booking HOLDS the fare on the connected card and the hold is captured only against a real airline PNR, so a stale cached price can never become a charge — which is the entire problem unlock existed to solve. If the fare has moved by the time the agent reaches checkout you get a `price_change` question to accept or decline, not a silent charge. Developer API: `POST /flights/book`. PFS: `book_flight` / `POST /api/agent-book`.

## Result Processing Pipeline

After all connectors return, the engine processes results through several stages:

### 1. Merge

All offers from all successful providers are collected into a single list.

### 2. Currency Normalization

Offers come back in different currencies (EUR, USD, GBP, INR, etc.). The engine converts all prices to the requested currency using live exchange rates:

```python
await self._normalize_prices(all_offers, req.currency)
# Every offer now has a price_normalized field in the target currency
```

### 3. Deduplication

The same flight can appear from multiple sources (e.g., a Ryanair flight found by both the Ryanair connector and the Kiwi.com connector). The engine deduplicates by route + time + airline, keeping the cheapest instance.

### 4. Virtual Interlining (Round-Trips)

For round-trip searches, the engine builds **cross-airline combinations** from one-way fares:

```
Outbound legs: Ryanair LHR→BCN €25, EasyJet LHR→BCN €30, Vueling LHR→BCN €35
Return legs:   Vueling BCN→LHR €28, Ryanair BCN→LHR €32, EasyJet BCN→LHR €27

Virtual interline combos:
  Ryanair out + EasyJet return = €52  ← cheapest combo
  Ryanair out + Vueling return = €53
  EasyJet out + EasyJet return = €57
  ...
```

This often finds cheaper combinations than any single airline's round-trip fare.

### 5. Airline-Diverse Selection

The final selection ensures you see the cheapest offer from each airline, not just the N cheapest overall:

```python
# Step 1: Pick cheapest per airline (guarantees diversity)
best_per_airline = {}
for offer in sorted_offers:
    airline = offer.owner_airline
    if airline not in best_per_airline:
        best_per_airline[airline] = offer

# Step 2: Fill remaining slots with overall cheapest
result = list(best_per_airline.values()) + remaining_cheapest
```

This prevents scenarios where all top results are from one airline.

## Performance Optimization

### Latency and Search Time

Full searches take 8–10 s to first results (determined by the slowest connector at letsfg.co). Use the discover endpoint for indicative pricing when 2–5 second results suffice:

```python
# Full search (8-10s to first results): best coverage, all connectors
result = bt.search("LHR", "BCN", "2026-06-01")

# Discover endpoint (2-5s): indicative prices for up to 20 destinations
# Developer API only — see api-guide.md
```

### Parallel Requests

If your application serves multiple users, fan out their searches concurrently — they run independently on separate Cloud Run instances:

```python
import asyncio
from letsfg import LetsFG

async def handle_multiple_users(requests):
    bt = LetsFG(api_key="letsfg_...")
    tasks = [
        asyncio.to_thread(bt.search, r["origin"], r["dest"], r["date"])
        for r in requests
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if not isinstance(r, Exception)]
```

### Latency Breakdown

Typical search latency by source type (all run server-side):

| Source | Latency | Notes |
|--------|---------|-------|
| Ryanair API | 0.5–1 s | Direct API call |
| Kiwi.com API | 1–2 s | GraphQL query |
| GDS providers | 2–10 s | Server queries Amadeus, Duffel, Sabre in parallel |
| LCC connectors | 3–30 s | Airline-specific integration |

Total search time equals the **maximum** of all active sources (they run in parallel), not the sum. Typical end-to-end: 8–10 s to first results to `completed`.

That terminal status is the point the *connector fan-out* is done, not the point the offer set stops changing. The split-ticket probe is dispatched after it and merges in late; `split_ticket_pending` stays true until it does. Treat `completed` as "the fast path is finished", not as "nothing more is coming".

## Monitoring and Observability

### Inspecting Search Results

```python
result = bt.search("LHR", "BCN", "2026-06-01")

# Which sources contributed
print(result.source_tiers)
# {"free": "ryanair_direct, easyjet_direct, vueling_direct", "paid": "duffel, amadeus"}

# Total offers before and after dedup
print(f"Total: {result.total_results} offers")

# Cheapest per airline
for summary in result.airlines_summary:
    print(f"  {summary.airline}: {summary.currency} {summary.cheapest_price}")
```
