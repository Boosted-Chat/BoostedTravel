# Architecture & Resilience Guide

Deep dive into how LetsFG's search engine works internally — connector orchestration, failure handling, caching strategies, and performance optimization.

## Search Engine Architecture

When you call `search_local()` or `bt.search()`, LetsFG fires **all** relevant data sources in parallel and merges the results. The engine never waits for one source before starting another.

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Your Application / Agent                          │
│           bt.search() / search_local() / MCP tool call               │
├──────────────────────────────────────────────────────────────────────┤
│                      MultiProvider Engine                            │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────────┐  │
│  │ Cloud Backend │  │ Fast Connectors│ │  180 airline connectors   │  │
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

### Parallel Execution Model

```python
# Simplified view of what happens inside the engine
results = await asyncio.gather(
    self._search_backend(req),           # Cloud GDS/NDC (if API key set)
    self._search_ryanair(req),           # Direct API
    self._search_wizzair(req),           # Direct API
    self._search_kiwi(req),              # Direct API
    self._search_connector("easyjet", req),   # Browser
    self._search_connector("spirit", req),    # Browser
    self._search_connector("southwest", req), # Browser
    # ... 70+ more connectors
    return_exceptions=True,  # ← KEY: failures don't cancel other tasks
)
```

The `return_exceptions=True` parameter is critical — it means a failed connector returns its exception as a result instead of canceling the entire `gather()`. The engine then iterates over results, collects successful offers, and logs failures:

```python
for i, result in enumerate(results):
    if isinstance(result, Exception):
        logger.warning("Provider %s failed: %s", provider_name, result)
        continue  # Skip failed provider, keep going
    if isinstance(result, FlightSearchResponse):
        all_offers.extend(result.offers)
```

**This means:** If 10 out of 150+ connectors fail (timeouts, bot detection, API errors), you still get results from the other 92. The search never fails completely unless *every* source fails.

### Route-Based Filtering

Before dispatching, the engine filters connectors by geographic relevance:

```python
filtered = get_relevant_connectors(origin, destination, all_connectors)
# LHR → BCN: runs ~15 EU connectors, skips ~60 irrelevant ones (IndiGo, AirAsia, etc.)
# BOM → DEL: runs ~8 India connectors, skips EU/US airlines
```

Each connector declares which countries it serves. The engine checks origin/destination countries against this registry and only runs connectors that could possibly have flights for the requested route. This typically reduces the number of active connectors from 150+ to 5-20, saving significant system resources.

If the engine can't determine the country for an airport code (unknown IATA), it falls back to running **all** connectors as a safety measure.

## Connector Failure Handling

### Per-Connector Resilience

Every connector implements its own multi-layer error recovery. The engine doesn't need to retry connectors — each connector handles its own failures internally.

#### Layer 1: Multi-Strategy Approach (Hybrid Connectors)

Most connectors try the fastest method first and fall back to slower methods:

```python
# Pattern used by Southwest, Frontier, Condor, JetBlue, FlyDubai, and others
async def search_flights(self, req):
    # Fast path: direct API via curl_cffi (~2-5s)
    api_result = await self._search_via_api(req)
    if api_result and api_result.total_results > 0:
        return api_result

    # Slow path: full browser automation (~10-30s)
    return await self._playwright_fallback(req)
```

Actual strategies by connector type:

| Strategy tier | Speed | When it's used | Example |
|--------------|-------|----------------|---------|
| curl_cffi direct API | 0.5-3s | First choice — reverse-engineered endpoint | Ryanair, FlyDubai, Frontier |
| Cookie-farm + API | 2-5s | API needs session cookies from browser | Southwest, Condor, Jet2 |
| Playwright API interception | 5-15s | Navigate page, capture API response | VietJet, Cebu Pacific, Spirit |
| Playwright DOM scraping | 10-30s | Last resort — parse rendered HTML | EasyJet fallback, Jetstar |

#### Layer 2: Per-Attempt Retry

Browser-based connectors retry with fresh browser contexts on connection errors:

```python
for attempt in range(1, MAX_ATTEMPTS + 1):
    try:
        result = await self._attempt_search(req)
        if result is not None:
            return result
    except Exception as e:
        if "ERR_CONNECTION" in str(e) or "Target closed" in str(e):
            await _reset_browser()  # Fresh Chrome instance
            await asyncio.sleep(2.0)
```

#### Layer 3: Hard Timeout

The engine enforces a hard deadline per connector, preventing any single connector from stalling the entire search:

```python
result = await asyncio.wait_for(
    client.search_flights(req),
    timeout=connector_timeout + 5.0,  # Hard deadline = connector's own timeout + 5s grace
)
```

If a connector exceeds its timeout (typically 30-60s), it's killed and returns an empty result. Other connectors continue normally.

### Engine-Level Failure Handling

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
result = await search_local("LHR", "BCN", "2026-06-01")
print(result["source_tiers"])
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

## Browser Concurrency Management

### The Browser Semaphore

Browser-based connectors launch real Chrome processes. Running 40+ simultaneously would crash most machines. The engine uses a **semaphore** to throttle browser concurrency:

```python
# Only N browser-based connectors can run simultaneously
# N is auto-detected from system RAM or set via LETSFG_MAX_BROWSERS
async def _search_connector_generic(self, client, req, source):
    if uses_browser:
        await acquire_browser_slot()   # Wait for available slot
    try:
        result = await client.search_flights(req)
        return result
    finally:
        if uses_browser:
            release_browser_slot()     # Free slot for next connector
            await self._cleanup_single_connector(client)  # Kill Chrome immediately
```

Non-browser connectors (direct API calls) run without throttling — they're lightweight HTTP requests.

### Auto-Scaling Based on System Resources

```python
from letsfg import get_system_profile

profile = get_system_profile()
print(profile)
# {
#   "ram_total_gb": 16.0,
#   "ram_available_gb": 10.2,
#   "cpu_cores": 8,
#   "recommended_max_browsers": 8,
#   "tier": "standard"
# }
```

| System RAM | Performance Tier | Max Browsers | Expected Search Time |
|-----------|-----------------|--------------|---------------------|
| < 2 GB | minimal | 2 | 30-60s |
| 2-4 GB | low | 3 | 20-40s |
| 4-8 GB | moderate | 5 | 15-25s |
| 8-16 GB | standard | 8 | 8-15s |
| 16-32 GB | high | 12 | 5-10s |
| 32+ GB | maximum | 16 | 3-8s |

### Manual Tuning

```python
from letsfg import configure_max_browsers

# For a CI/CD server with limited RAM
configure_max_browsers(2)

# For a dedicated search server with 32GB RAM
configure_max_browsers(16)

# Or via environment variable
# LETSFG_MAX_BROWSERS=4
```

```bash
# CLI flag
letsfg search-local LHR BCN 2026-04-15 --max-browsers 4

# Environment variable
export LETSFG_MAX_BROWSERS=4
letsfg search-local LHR BCN 2026-04-15
```

Priority: CLI flag > environment variable > auto-detect from RAM.

### Early Cleanup

The engine doesn't wait for all connectors to finish before releasing resources. Each browser-based connector's Chrome process is terminated **immediately** after it returns results:

```
Timeline:
0s   ── Launch Ryanair (API), EasyJet (browser), Spirit (browser), ...
1s   ── Ryanair returns 12 offers (API — no browser to clean up)
8s   ── EasyJet returns 5 offers → Chrome killed immediately → slot freed
9s   ── Freed slot used by Jetstar (was waiting for browser slot)
12s  ── Spirit returns 3 offers → Chrome killed → slot freed
15s  ── All connectors done → merge + dedup → response returned
```

## Caching and Rate Limit Strategy

### Cookie Farming (Connector-Level Caching)

Several connectors use **cookie farming** — they maintain cached browser cookies to avoid re-authenticating with airline websites on every search:

```python
# Pattern used by Southwest, Condor, Jet2, Flynas, T'way Air
_farmed_cookies = None
_farm_timestamp = 0.0
COOKIE_MAX_AGE = 1800  # 30 minutes

async def search_flights(self, req):
    # Fast path: use cached cookies with direct API
    if _farmed_cookies and (time.monotonic() - _farm_timestamp) < COOKIE_MAX_AGE:
        result = await self._search_via_api(req, cookies=_farmed_cookies)
        if result:
            return result

    # Slow path: launch browser, get fresh cookies
    result = await self._playwright_fallback(req)
    # Cache cookies from successful browser session for next search
    _farmed_cookies = await context.cookies()
    _farm_timestamp = time.monotonic()
    return result
```

This means the **first search** for a cookie-dependent airline takes 10-30s (browser needed), but **subsequent searches** take 2-5s (cached cookies + direct API).

### Token Caching

API-based connectors cache authentication tokens:

```python
# Pattern used by Vueling, SpiceJet, and others
_token = None
_token_expiry = 0

async def _ensure_token():
    if _token and time.monotonic() < _token_expiry:
        return _token  # Reuse cached token

    # Fetch new token (typically valid for 20 minutes)
    response = await fetch_token()
    _token = response["token"]
    _token_expiry = time.monotonic() + response.get("expires_in", 1199)
    return _token
```

### Rate Limit Handling

#### API-Level Rate Limits

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

#### Connector-Level Rate Limits

Individual airline APIs have their own rate limits. Connectors handle these internally:

- **Wizzair**: 429 responses trigger KPSDK challenge re-farming
- **Flynas**: 429 triggers a 3-second wait + retry in browser context
- **Kiwi.com**: 429 returns empty result (rate limited at GraphQL level)
- **Allegiant**: Cloudflare challenges are waited out (up to 40s)

You don't need to handle connector-level rate limits — the engine manages them transparently.

### Designing a Caching Layer for Multi-User Applications

If you're building an application that serves multiple concurrent users, implement a caching layer between your users and the LetsFG API:

```python
import asyncio
import hashlib
import time
from letsfg.local import search_local

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

            # Execute actual search
            result = await search_local(origin, dest, date, **kwargs)
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

**Important:** Always call `unlock()` before booking. The unlock step confirms the live price with the airline regardless of cache state. Cached search results are for display; unlocked prices are the source of truth.

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

### Achieving Sub-5-Second Search

For time-sensitive applications, optimize with these strategies:

#### 1. Use `sort="price"` and low `limit`

```python
# Returns as soon as enough offers are collected
result = bt.search("LHR", "BCN", "2026-06-01", sort="price", limit=5)
```

#### 2. Use local search for known LCC routes

```python
from letsfg.local import search_local

# Local connectors are faster for LCC-heavy routes
result = await search_local("LHR", "BCN", "2026-06-01", max_browsers=8)
```

#### 3. Pre-warm browser instances

For repeated searches, the first search warms up browser contexts and caches cookies. Subsequent searches are significantly faster:

| Search | Time | What happens |
|--------|------|-------------|
| First search (cold) | 10-20s | Launch Chrome, farm cookies, execute search |
| Second search (warm) | 2-5s | Reuse cookies, direct API calls |
| Third+ search (warm) | 2-5s | Cached tokens + cookies |

#### 4. Parallel user requests

If your application serves multiple users, run their searches concurrently:

```python
import asyncio
from letsfg.local import search_local

async def handle_multiple_users(requests):
    """Search for multiple users in parallel."""
    tasks = [
        search_local(r["origin"], r["dest"], r["date"])
        for r in requests
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [
        r for r in results
        if not isinstance(r, Exception)
    ]
```

#### 5. Tune `max_browsers` for your hardware

```python
from letsfg import get_system_profile, configure_max_browsers

profile = get_system_profile()
# Use the system's recommended concurrency
configure_max_browsers(profile["recommended_max_browsers"])
```

### Latency Breakdown

Typical search latency by source type:

| Source | Latency | What determines speed |
|--------|---------|----------------------|
| Ryanair API | 0.5-1s | Direct HTTP call, no browser |
| Kiwi.com API | 1-2s | GraphQL query to public API |
| Cloud backend | 2-10s | Server queries GDS providers in parallel |
| curl_cffi hybrid | 2-5s | Direct API with TLS fingerprinting |
| Browser connector (warm) | 5-10s | Reused browser context, cached cookies |
| Browser connector (cold) | 10-30s | Fresh Chrome launch, full page navigation |

The total search time is the **maximum** of all active sources (they run in parallel), not the sum.

## Monitoring and Observability

### Logging

The engine logs extensively at INFO and WARNING levels:

```python
import logging
logging.basicConfig(level=logging.INFO)

# You'll see output like:
# INFO: Route filter: LHR->BCN -- skipped 120/150 irrelevant connectors
# INFO: Launching 23 provider tasks (20 normal + 3 combo) for LHR->BCN
# INFO: ryanair_direct: 8 offers in 0.9s
# WARNING: Provider easyjet_direct failed: TimeoutError
# INFO: vueling_direct: 4 offers in 3.2s
# INFO: All 23 tasks done in 12.3s — 20 succeeded, 3 failed
# INFO: Combo engine produced 12 cross-airline offers from 8 out + 6 ret legs
# INFO: Browser cleanup: closed 6 resource(s) across 4 modules
```

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
