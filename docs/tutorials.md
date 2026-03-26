# Integration Tutorials

Practical guides for building travel applications with LetsFG in Python and JavaScript/TypeScript.

## Python: Concurrent Multi-Route Search

Search multiple routes simultaneously using `asyncio.gather`. The engine already parallelizes connectors internally, but you can also parallelize across different routes at the application level.

### Search Multiple Routes in Parallel

```python
import asyncio
from letsfg.local import search_local

async def search_multiple_routes(routes: list[dict]) -> list[dict]:
    """Search several origin-destination pairs in parallel.

    Args:
        routes: List of dicts with keys: origin, destination, date

    Returns:
        List of search results, one per route (failures excluded).
    """
    tasks = [
        search_local(r["origin"], r["destination"], r["date"])
        for r in routes
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful = []
    for route, result in zip(routes, results):
        if isinstance(result, Exception):
            print(f"  ⚠ {route['origin']}→{route['destination']}: {result}")
        else:
            successful.append(result)
    return successful

# Example: Find the cheapest weekend getaway from London
routes = [
    {"origin": "LHR", "destination": "BCN", "date": "2026-06-06"},
    {"origin": "LHR", "destination": "LIS", "date": "2026-06-06"},
    {"origin": "LHR", "destination": "AMS", "date": "2026-06-06"},
    {"origin": "LHR", "destination": "DUB", "date": "2026-06-06"},
]

results = asyncio.run(search_multiple_routes(routes))
for r in sorted(results, key=lambda x: x["offers"][0]["price"] if x["offers"] else float("inf")):
    if r["offers"]:
        cheapest = r["offers"][0]
        print(f"{r['origin']}→{r['destination']}: {cheapest['currency']} {cheapest['price']}")
```

### Multi-Date Price Calendar

Search the same route across multiple dates to find the cheapest day:

```python
import asyncio
from datetime import date, timedelta
from letsfg.local import search_local

async def price_calendar(origin: str, dest: str, start: date, days: int = 7):
    """Get the cheapest price for each day in a date range."""
    dates = [start + timedelta(days=i) for i in range(days)]

    tasks = [
        search_local(origin, dest, d.isoformat())
        for d in dates
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    calendar = []
    for d, result in zip(dates, results):
        if isinstance(result, Exception):
            calendar.append({"date": d.isoformat(), "price": None, "error": str(result)})
        elif result.get("offers"):
            cheapest = min(result["offers"], key=lambda o: o["price"])
            calendar.append({
                "date": d.isoformat(),
                "price": cheapest["price"],
                "currency": cheapest["currency"],
                "airline": cheapest.get("airlines", ["?"])[0],
            })
        else:
            calendar.append({"date": d.isoformat(), "price": None, "error": "No offers"})

    return calendar

# Find cheapest day to fly LHR → BCN next week
cal = asyncio.run(price_calendar("LHR", "BCN", date(2026, 6, 1), days=7))
for day in cal:
    if day["price"]:
        print(f"  {day['date']}: {day['currency']} {day['price']} ({day['airline']})")
    else:
        print(f"  {day['date']}: no flights")
```

---

## Python: Building a Travel Assistant

Build a backend service that uses LetsFG to answer travel queries:

```python
import asyncio
from letsfg import LetsFG

bt = LetsFG()  # Uses LETSFG_API_KEY from environment

class TravelAssistant:
    """A simple travel assistant with search, compare, and book capabilities."""

    async def find_cheapest(self, origin: str, dest: str, date: str,
                            budget: float = None, currency: str = "EUR"):
        """Search flights and optionally filter by budget."""
        result = await asyncio.to_thread(
            bt.search, origin, dest, date,
            sort="price", limit=20, currency=currency,
        )
        offers = result.get("offers", [])

        if budget:
            offers = [o for o in offers if o["price"] <= budget]

        return {
            "route": f"{origin}→{dest}",
            "date": date,
            "total_found": result.get("total_results", 0),
            "within_budget": len(offers),
            "cheapest": offers[0] if offers else None,
            "top_5": offers[:5],
        }

    async def compare_airlines(self, origin: str, dest: str, date: str):
        """Show cheapest price per airline for a route."""
        result = await asyncio.to_thread(
            bt.search, origin, dest, date,
            sort="price", limit=50,
        )

        by_airline = {}
        for offer in result.get("offers", []):
            airline = offer.get("airlines", ["Unknown"])[0]
            if airline not in by_airline or offer["price"] < by_airline[airline]["price"]:
                by_airline[airline] = {
                    "price": offer["price"],
                    "currency": offer["currency"],
                    "duration": offer.get("outbound", {}).get("total_duration_seconds"),
                    "stops": len(offer.get("outbound", {}).get("segments", [])) - 1,
                }

        return dict(sorted(by_airline.items(), key=lambda x: x[1]["price"]))

    async def unlock_and_book(self, offer_id: str, search_id: str):
        """Unlock an offer to get the live price, then return booking details."""
        unlocked = await asyncio.to_thread(bt.unlock, offer_id, search_id)
        return {
            "confirmed_price": unlocked["price"],
            "currency": unlocked["currency"],
            "booking_url": unlocked.get("booking_url"),
            "expires_at": unlocked.get("expires_at"),
        }

# Usage
assistant = TravelAssistant()
result = asyncio.run(assistant.find_cheapest("LHR", "BCN", "2026-06-01", budget=50))
print(f"Cheapest: {result['cheapest']['currency']} {result['cheapest']['price']}")
```

---

## JavaScript/TypeScript: Multi-Source Flight Search

### Basic Search with the JS SDK

```typescript
import LetsFG from "letsfg";

const bt = new LetsFG(); // Uses LETSFG_API_KEY from environment

const result = await bt.search("LHR", "BCN", "2026-06-01", {
  sort: "price",
  limit: 10,
  currency: "EUR",
});

console.log(`Found ${result.total_results} offers`);
for (const offer of result.offers) {
  console.log(`  ${offer.airlines.join("+")} — €${offer.price}`);
}
```

### Searching Multiple Sources in Parallel

LetsFG's backend already queries all GDS/NDC sources in parallel (Amadeus, Duffel, Sabre, Kiwi, and 195 airline connectors). A single `search()` call covers all sources. However, you can parallelize **multiple searches** at the application level:

```typescript
import LetsFG from "letsfg";

const bt = new LetsFG();

// Search multiple routes simultaneously
const routes = [
  { origin: "LHR", dest: "BCN", date: "2026-06-06" },
  { origin: "LHR", dest: "LIS", date: "2026-06-06" },
  { origin: "LHR", dest: "AMS", date: "2026-06-06" },
  { origin: "LHR", dest: "CDG", date: "2026-06-06" },
];

const results = await Promise.allSettled(
  routes.map((r) => bt.search(r.origin, r.dest, r.date, { sort: "price", limit: 5 }))
);

// Process results — failed searches don't block others
for (let i = 0; i < routes.length; i++) {
  const route = routes[i];
  const result = results[i];

  if (result.status === "fulfilled" && result.value.offers.length > 0) {
    const cheapest = result.value.offers[0];
    console.log(`${route.origin}→${route.dest}: ${cheapest.currency} ${cheapest.price}`);
  } else if (result.status === "rejected") {
    console.log(`${route.origin}→${route.dest}: search failed — ${result.reason}`);
  } else {
    console.log(`${route.origin}→${route.dest}: no flights found`);
  }
}
```

### Understanding GDS and NDC Sources

LetsFG aggregates from multiple distribution channels:

| Source type | What it is | Airlines covered |
|------------|------------|-----------------|
| **GDS** (Global Distribution System) | Traditional airline inventory systems | Most legacy carriers (BA, Lufthansa, Delta, United) |
| **NDC** (New Distribution Capability) | Modern direct-connect API standard | Airlines with NDC feeds (Vueling, Condor, Air Canada) |
| **LCC Direct** | LetsFG's own airline connectors | 195 airline connectors (Ryanair, EasyJet, Spirit, Southwest, Qatar, LATAM) |
| **Aggregators** | Meta-search APIs | Kiwi.com (covers 800+ airlines) |

A single `search()` call queries **all available sources** and returns merged, deduplicated results. You don't need to specify which source to query.

### Building a TypeScript Travel Agent

```typescript
import LetsFG from "letsfg";

const bt = new LetsFG();

interface SearchParams {
  origin: string;
  destination: string;
  date: string;
  budget?: number;
  currency?: string;
}

async function findCheapestFlights(params: SearchParams) {
  const { origin, destination, date, budget, currency = "EUR" } = params;

  const result = await bt.search(origin, destination, date, {
    sort: "price",
    limit: 20,
    currency,
  });

  let offers = result.offers;
  if (budget) {
    offers = offers.filter((o) => o.price <= budget);
  }

  return {
    route: `${origin}→${destination}`,
    date,
    totalFound: result.total_results,
    withinBudget: offers.length,
    cheapest: offers[0] ?? null,
    top5: offers.slice(0, 5),
    sources: result.source_tiers,
  };
}

// Price calendar: cheapest fare per day
async function priceCalendar(origin: string, dest: string, startDate: string, days = 7) {
  const dates: string[] = [];
  const start = new Date(startDate);
  for (let i = 0; i < days; i++) {
    const d = new Date(start);
    d.setDate(d.getDate() + i);
    dates.push(d.toISOString().slice(0, 10));
  }

  const results = await Promise.allSettled(
    dates.map((date) => bt.search(origin, dest, date, { sort: "price", limit: 1 }))
  );

  return dates.map((date, i) => {
    const r = results[i];
    if (r.status === "fulfilled" && r.value.offers.length > 0) {
      const offer = r.value.offers[0];
      return { date, price: offer.price, currency: offer.currency, airline: offer.airlines[0] };
    }
    return { date, price: null, currency: null, airline: null };
  });
}

// Example usage
const flights = await findCheapestFlights({
  origin: "JFK",
  destination: "CDG",
  date: "2026-07-15",
  budget: 400,
  currency: "USD",
});
console.log(`Found ${flights.withinBudget} flights under $400`);
console.log(`Cheapest: $${flights.cheapest?.price} on ${flights.cheapest?.airlines.join("+")}`);
```

---

## MCP Server: AI Agent Integration

### Claude Desktop / Cursor / Windsurf Setup

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["letsfg-mcp"],
      "env": {
        "LETSFG_API_KEY": "your-api-key"
      }
    }
  }
}
```

Once configured, the AI agent gets access to these tools:

| Tool | Description |
|------|-------------|
| `search_flights` | Search flights across all sources |
| `resolve_location` | Convert city/airport names to IATA codes |
| `unlock_flight_offer` | Lock in a live price for an offer |
| `book_flight` | Complete a booking |
| `get_agent_profile` | Check agent capabilities and limits |
| `setup_payment` | Configure payment methods |

### Agent Best Practices

```
Prompt: "Find the cheapest flight from London to Barcelona on June 1st"

Agent flow:
1. resolve_location("London")     → LHR, LGW, STN, LTN, SEN
2. resolve_location("Barcelona")  → BCN
3. search_flights("LHR", "BCN", "2026-06-01", sort="price", limit=5)
4. Present results to user
5. If user wants to book:
   unlock_flight_offer(offer_id, search_id)  → confirmed price
   book_flight(offer_id, search_id, passengers)
```

**Key patterns for AI agents:**

- **Always resolve locations first** — don't assume IATA codes from city names
- **Search with `limit`** — agents don't need 500 results, 5-20 is enough for a conversation
- **Unlock before booking** — prices can change between search and book; `unlock` confirms the live price
- **Handle partial failures gracefully** — some sources may timeout; the search still returns results from working sources

---

## Error Handling Patterns

### Python Error Hierarchy

```python
from letsfg import (
    LetsFGError,           # Base error
    AuthenticationError,    # Invalid API key (401)
    PaymentRequiredError,   # Subscription/credits needed (402)
    OfferExpiredError,      # Search result too old to book (410)
    ValidationError,        # Bad parameters (400)
)

try:
    result = bt.search("LHR", "BCN", "2026-06-01")
except AuthenticationError:
    print("Check your LETSFG_API_KEY")
except PaymentRequiredError:
    print("Subscription required for this route/volume")
except ValidationError as e:
    print(f"Bad request: {e}")
except LetsFGError as e:
    if e.is_retryable:
        # Server error or timeout — safe to retry
        time.sleep(2)
        result = bt.search("LHR", "BCN", "2026-06-01")
    else:
        raise
```

### JavaScript Error Handling

```typescript
import LetsFG, { LetsFGError } from "letsfg";

const bt = new LetsFG();

try {
  const result = await bt.search("LHR", "BCN", "2026-06-01");
} catch (err) {
  if (err instanceof LetsFGError) {
    if (err.statusCode === 401) {
      console.error("Invalid API key");
    } else if (err.statusCode === 429) {
      // Rate limited — back off and retry
      await new Promise((r) => setTimeout(r, 2000));
      const result = await bt.search("LHR", "BCN", "2026-06-01");
    } else if (err.isRetryable) {
      // Server error — retry once
      const result = await bt.search("LHR", "BCN", "2026-06-01");
    }
  }
  throw err;
}
```

### Resilient Booking Flow

The full search → unlock → book pipeline with proper error handling:

```python
import asyncio
from letsfg import LetsFG, OfferExpiredError, LetsFGError

bt = LetsFG()

async def resilient_book(origin, dest, date, passengers, max_search_retries=2):
    """Search → unlock → book with full error recovery."""

    # Step 1: Search (retryable)
    for attempt in range(max_search_retries):
        try:
            search = bt.search(origin, dest, date, sort="price", limit=5)
            break
        except LetsFGError as e:
            if e.is_retryable and attempt < max_search_retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                raise

    if not search.get("offers"):
        return {"status": "no_flights", "message": "No flights found for this route/date"}

    best = search["offers"][0]

    # Step 2: Unlock (confirms live price)
    try:
        unlocked = bt.unlock(best["id"], search["search_id"])
    except OfferExpiredError:
        # Search result too old — re-search and try again
        search = bt.search(origin, dest, date, sort="price", limit=5)
        if not search.get("offers"):
            return {"status": "expired", "message": "Offer expired and no alternatives found"}
        best = search["offers"][0]
        unlocked = bt.unlock(best["id"], search["search_id"])

    # Step 3: Book (not retryable — payment involved)
    booking = bt.book(best["id"], search["search_id"], passengers)
    return {
        "status": "booked",
        "confirmation": booking.get("confirmation_code"),
        "price": unlocked["price"],
        "currency": unlocked["currency"],
    }
```
