# Integration Tutorials

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

Practical guides for building travel applications with LetsFG in Python and JavaScript/TypeScript.

## Python: Concurrent Multi-Route Search

Search multiple routes simultaneously using `asyncio.gather`. The server-side engine already searches all airlines in parallel, but you can also parallelize across different routes at the application level.

### Search Multiple Routes in Parallel

```python
import asyncio
from letsfg import LetsFG

bt = LetsFG()  # uses LETSFG_BEARER_TOKEN or LETSFG_API_KEY from environment

async def search_multiple_routes(routes: list[dict]) -> list[dict]:
    """Search several origin-destination pairs in parallel.

    Args:
        routes: List of dicts with keys: origin, destination, date

    Returns:
        List of search results, one per route (failures excluded).
    """
    tasks = [
        asyncio.to_thread(bt.search, r["origin"], r["destination"], r["date"])
        for r in routes
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful = []
    for route, result in zip(routes, results):
        if isinstance(result, Exception):
            print(f"  {route['origin']}→{route['destination']}: {result}")
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
from letsfg import LetsFG

bt = LetsFG()

async def price_calendar(origin: str, dest: str, start: date, days: int = 7):
    """Get the cheapest price for each day in a date range."""
    dates = [start + timedelta(days=i) for i in range(days)]

    tasks = [
        asyncio.to_thread(bt.search, origin, dest, d.isoformat())
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

    async def book(self, search_id: str, offer_id: str, passengers: list[dict]):
        """Book an offer over the Developer API. There is NO unlock step since 2026-09-08.

        Written against the REST endpoints directly rather than the SDK: the SDK's book() still
        routes its Developer-API path through the retired unlock lane, and it has no polling or
        question helpers yet.

        The fare is HELD on the connected Revolut method, never taken up front, and captured only
        once a real airline PNR exists - so a fare that moved cannot become a charge for a ticket
        you did not get. The offer price already includes LetsFG's margin: no booking fee, no
        transaction fee.
        """
        import httpx

        base = "https://letsfg.co/developers/api/v1"
        headers = {"X-API-Key": os.environ["LETSFG_API_KEY"]}

        async with httpx.AsyncClient(timeout=60) as http:
            started = (await http.post(f"{base}/flights/book", headers=headers, json={
                "search_id": search_id,
                "offer_id": offer_id,
                "passengers": passengers,
                # A retry must never open a second hold on the card.
                "idempotency_key": f"{search_id}:{offer_id}",
            })).json()

            if not started.get("ok"):
                # Nothing was charged. `error` says which condition failed; `missing_fields`
                # names exactly what an airline checkout still needs from the traveller.
                return started

            booking_id = started["booking_id"]
            while True:
                await asyncio.sleep(5)
                state = (await http.get(f"{base}/flights/bookings/{booking_id}",
                                        headers=headers)).json()

                # Polling is also how LetsFG knows you are still there, which keeps a paused
                # booking alive. A `question` means the run is waiting on you - a seat map, a
                # paid extra, or a fare increase - and it expires.
                question = state.get("question")
                if question:
                    await http.post(
                        f"{base}/flights/bookings/{booking_id}/answer", headers=headers,
                        json={
                            "round": question["round"],          # echo it; a stale round is 409
                            "confirm": question["kind"] != "extra",
                            "skip": question["kind"] == "extra",
                        },
                    )
                    continue

                if state.get("terminal"):
                    # completed (pnr + charged_amount) | failed (hold released) |
                    # needs_attention (a human at LetsFG is on it - do not book again)
                    return state

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

LetsFG's backend already queries all GDS/NDC sources in parallel (Amadeus, Duffel, Sabre, Kiwi, and LetsFG's own airline connectors). A single `search()` call covers all sources. However, you can parallelize **multiple searches** at the application level:

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

LetsFG aggregates from multiple distribution channels, all server-side at letsfg.co:

| Source type | What it is | Airlines covered |
|------------|------------|-----------------|
| **GDS** (Global Distribution System) | Traditional airline inventory systems | Most legacy carriers (BA, Lufthansa, Delta, United) |
| **NDC** (New Distribution Capability) | Modern direct-connect API standard | Airlines with NDC feeds (Vueling, Condor, Air Canada) |
| **LCC Direct** | LetsFG's own airline connectors, server-side | Hundreds of airlines (Ryanair, EasyJet, Spirit, Southwest, Qatar, LATAM, and more) |
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
| `unlock_flight_offer` | **[Developer API only]** Lock in a live price for an offer. Not needed on a card-backed token |
| `book_flight` | Start the booking: fare held on the connected card, a LetsFG agent buys the ticket, hold captured only against a real PNR. Returns `booking_ref` |
| `get_flight_booking` | Poll a booking every 20–30 s until `completed` / `failed` / `needs_attention` (4–11 min) |
| `get_agent_profile` | Check agent capabilities and limits |
| `setup_payment` | **[Developer API only]** Attach a payment method to the paid account |

### Agent Best Practices

```
Prompt: "Find the cheapest flight from London to Barcelona on June 1st"

Agent flow:
1. resolve_location("London")     → LHR, LGW, STN, LTN, SEN
2. resolve_location("Barcelona")  → BCN
3. search_flights("LHR", "BCN", "2026-06-01", sort="price", limit=5)
4. Present results to user
5. If user wants to book (card-backed token):
   book_flight(search_id, offer_id, passengers, contact_email)  → booking_ref in seconds
   get_flight_booking(booking_ref) every 20-30 s            → completed + PNR (4-11 min)
   (Developer API key: unlock_flight_offer first, then book_flight)
```

**Key patterns for AI agents:**

- **Always resolve locations first** — don't assume IATA codes from city names
- **Search with `limit`** — agents don't need 500 results, 5-20 is enough for a conversation
- **No unlock on the card-backed lane** — `book_flight` holds the fare on the card and captures it only against a real PNR, so a moved price is a `failed` booking with nothing charged, never a surprise. On a Developer API key, `unlock` confirms the live price first
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

    # Step 3: Book (not retryable — payment involved). On a Bearer token this
    # STARTS the booking and returns booking_ref; poll POST /api/agent-book/status
    # every 20-30 s until completed / failed / needs_attention.
    booking = bt.book(best["id"], search["search_id"], passengers)
    return {
        "status": "booked",
        "confirmation": booking.get("confirmation_code"),
        "price": unlocked["price"],
        "currency": unlocked["currency"],
    }
```
