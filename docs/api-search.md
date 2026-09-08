# Search and Results

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

<div class="docs-callout">
  <strong>Search billing:</strong> the public developer API is <strong>look-to-book</strong>, not per call — 200 searches are free after every booking you make, and booking resets the counter. Only past that allowance do searches cost anything (blocks of 500 for $5.00 from prepaid balance). A search that returns no offers never counts.
</div>

## Endpoints you will use

| Endpoint | Method | Purpose | Billed? |
|----------|--------|---------|---------|
| `/flights/locations/{query}` | GET | Resolve a city, airport, or metro area to IATA codes | No |
| `/flights/parse-query` | POST | Parse a natural language query into search params | No |
| `/flights/search` | POST | Run a single-destination search (blocking, 8–10 s to first results) | **1 search** |
| `/flights/search/async` | POST | Start a search in background, returns search_id immediately | **1 search** |
| `/flights/results/{search_id}` | GET | Poll results of an async search | No |
| `/flights/discover` | POST | Indicative prices for up to 20 destinations — single call | **1 search** |
| `/flights/multi-search` | POST | Full search for N destinations in parallel | **1 search per destination** |
| `/flights/providers` | GET | Inspect the provider mix exposed through the public API | No |

> **Billing rule: every destination counts as one search against the allowance.**  
> `/flights/multi-search` with 10 destinations counts as 10 searches — same as calling
> `/flights/search` 10 times. There is no bundle discount.
> Use `/flights/discover` for cheap discovery (1 search total), then `/flights/search`
> on the destination you want to book. Check your balance at `GET /agents/me` before running large batches.

## Resolve locations first

Always resolve ambiguous place names before you search.

```bash
curl https://letsfg.co/developers/api/v1/flights/locations/London \
  -H "X-API-Key: letsfg_your_api_key"
```

Use metro codes such as `LON` when you want all airports in a city, or specific airport codes such as `LHR` when the integration should be strict.

## Search request fields

| Field | Required | Notes |
|-------|----------|-------|
| `origin` | Yes | IATA departure code such as `LON`, `LHR`, or `JFK` |
| `destination` | Yes | IATA arrival code such as `BCN` or `LAX` |
| `date_from` | Yes | Departure date in `YYYY-MM-DD` |
| `date_to` | No | End of an outbound date window |
| `return_from` | No | Return date for round-trips |
| `return_to` | No | End of a return date window |
| `adults` | No | `1` to `9`, default `1` |
| `children` | No | `0` to `9` |
| `infants` | No | `0` to `9` |
| `cabin_class` | No | `M`, `W`, `C`, or `F` |
| `max_stopovers` | No | `0` to `4`, default `2` |
| `currency` | No | Three-letter code, default `EUR` |
| `locale` | No | Locale code, default `en` |
| `limit` | No | `1` to `200`, default `50` |
| `sort` | No | Public API passes the value through; default `price` |
| `departure_time_from` | No | `HH:MM`, 24-hour format |
| `departure_time_to` | No | `HH:MM`, 24-hour format |
| `provider_filters` | No | Provider filter object for advanced integrations |

## Single-date search example

```bash
curl -X POST https://letsfg.co/developers/api/v1/flights/search \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "origin": "LON",
    "destination": "BCN",
    "date_from": "2026-07-15",
    "adults": 1,
    "currency": "EUR",
    "limit": 20,
    "sort": "price"
  }'
```

## JavaScript example

```ts
const response = await fetch("https://letsfg.co/developers/api/v1/flights/search", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "X-API-Key": process.env.LETSFG_API_KEY!,
  },
  body: JSON.stringify({
    origin: "LHR",
    destination: "JFK",
    date_from: "2026-07-15",
    adults: 1,
    currency: "USD",
    limit: 25,
  }),
});

if (!response.ok) {
  throw new Error(await response.text());
}

const result = await response.json();
console.log(result.total_results, result.passenger_ids);
```

## Example response

```json
{
  "passenger_ids": ["pas_0"],
  "total_results": 18,
  "offers": [
    {
      "id": "off_abc123",
      "price": 612.4,
      "currency": "USD",
      "owner_airline": "Airline Name",
      "route": "LHR -> JFK",
      "starlink": "likely_all"
    }
  ]
}
```

## Starlink Wi-Fi

Offers may carry a `starlink` field, and each segment its own. The field is
**absent when we have no information — absent does not mean the flight has no
Wi-Fi.**

| offer value | meaning |
|---|---|
| `confirmed_all` | every leg is on a subfleet the carrier has **fully** fitted |
| `confirmed_some` | at least one leg confirmed, at least one leg with none |
| `likely_all` | every leg is on a subfleet where installation is underway |
| `likely_some` | some legs on such a subfleet, others with none |

Per segment the value is `confirmed` or `likely`.

**Only `confirmed_*` may be presented to a user as a fact.** `likely_*` means the
airline is actively fitting that exact aircraft type but has not finished — as of
August 2026 United was roughly 29% of its fleet and Southwest about 300 of 800
aircraft, so a specific airframe genuinely may not have it. Phrase it as "the
airline is rolling Starlink out on this aircraft type, not guaranteed on your
flight", never "this flight has Starlink".

Anything ending in `_some` has at least one leg **without** it. Name the legs
rather than calling the itinerary a Starlink trip.

```json
{
  "id": "off_abc123",
  "price": 139.0,
  "starlink": "likely_all",
  "outbound": {
    "segments": [
      { "airline": "WN", "origin": "SEA", "destination": "LAS", "starlink": "likely" }
    ]
  }
}
```

### How it is derived

A dated airline + subfleet table, matched against the operating carrier and the
aircraft type. It is **not** a tail-number lookup, so it cannot know which
individual airframe you were assigned — that is exactly why the confirmed/likely
split exists.

Coverage is uneven by design. Carriers that operate a single type (Southwest,
airBaltic) resolve even when the aircraft type is unknown; every other carrier
needs the aircraft type, and not every source reports one. A missing field is
therefore common and carries no negative information.

## Self-transfer and split tickets

An offer may carry `self_transfer`. `"protected"` means the seller stands
behind the connection; `"unprotected"` means it does not — if the first flight
is late and the connection is missed, the onward airline has no obligation: no
rebooking, no refund, no duty of care. Surface that alongside the price.
Relaying the saving without the condition misrepresents the offer.

LetsFG also *builds* split itineraries: two separately-issued tickets through a
hub, bought from two different sellers, because no single seller offers the
combination. Those offers carry `split_ticket: "true"` and
`combo_type: "virtual_interlining"` alongside `self_transfer: "unprotected"`.

> **Lane note.** The split-ticket builder runs on the agent search lane
> (`POST https://letsfg.co/api/search` — the CLI, the Python and JS SDKs and
> the MCP server). This Developer API is served by a different backend, and
> split offers are not part of its documented contract. Treat `split_ticket`
> as a field to *honour if present*, not one to expect. `self_transfer` is
> returned by this API and should always be checked.

See the [agent guide](agent-guide.md) for the split-ticket lane.

## What to persist from results

- `passenger_ids` for any later passenger mapping or booking continuation
- `offers[].id` for the option the user chooses
- `total_results` for analytics and QA
- the request body you used, so reruns and debugging stay reproducible

## Coverage and response time

The developer API uses the same airline connector fleet as the [letsfg.co](https://letsfg.co) consumer search. The fleet includes direct airline scrapers, OTA aggregators (Kiwi, Kayak, Momondo, Skyscanner), and virtual interlining — worldwide coverage including all US domestic routes.

| Route type | Typical response time |
|------------|-----------------------|
| Europe intra-continental | 20–40 seconds |
| Transatlantic | 8–10 s to first results |
| US domestic | 8–10 s to first results |
| Asia-Pacific / Latin America | 40–80 seconds |

Response times reflect how long the relevant connectors take to return results. The API returns as soon as a useful set of offers is available — set your client timeout to at least **90 seconds** to handle the full range.

Results are cached for 5 minutes. A second search for the same route and date returns immediately.

## Inspect provider visibility

```bash
curl https://letsfg.co/developers/api/v1/flights/providers \
  -H "X-API-Key: letsfg_your_api_key"
```

Use this endpoint when you want to understand which provider families are visible to the current public API environment.

## Zero results

A `200 OK` response with `total_results: 0` means the search ran successfully but no flights were found. Common reasons:

- **Date in the past** — `date_from` must be today or later.
- **Very near-term date** — same-day or next-day departures may not yet have inventory.
- **Niche or unserved route** — some city pairs have no direct or connecting service.
- **Cabin class filter** — `cabin_class: "F"` (first class) eliminates most LCCs; try `"M"` (economy) first.

If a route returns results on [letsfg.co](https://letsfg.co) but not via the API, check that your `date_from` is in the future and your client timeout is at least 90 seconds — the search itself returns in 8–10 s to first results, but a generous ceiling costs nothing and covers a slow supplier. The same connector fleet is used for both; a second request will usually return cached results immediately.

## Departure time filters

Use `departure_time_from` and `departure_time_to` (HH:MM, 24-hour) to restrict
outbound departure to a time window. Applied server-side before results are returned.

```json
{
  "origin": "JFK",
  "destination": "LAX",
  "date_from": "2026-07-15",
  "departure_time_from": "05:00",
  "departure_time_to": "11:00"
}
```

> **Note:** Hard time filters can return zero results on thin routes. For discovery
> feeds where you always want *something* to show, search without the filter and
> sort or display by departure time client-side.

## Parse natural language queries

`POST /flights/parse-query` converts free-text input into structured search params —
free, no allowance consumed.

```bash
curl -X POST https://letsfg.co/developers/api/v1/flights/parse-query \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"query": "morning flight from New York to Chicago next Friday, 2 adults"}'
```

Returns `origin`, `destination`, `departure_date`, `adults`, `departure_time_from`,
`departure_time_to`, `cabin_class`, and a `follow_up_topics` list for any fields
that are still missing. Pass the resolved fields directly to `/flights/search`.

Use `"mode": "clarify"` to get only the list of missing fields without a full parse
— useful for building a step-by-step question flow.

## Discovery search — 20 destinations, 1 search

`POST /flights/discover` is built for discovery feeds. It checks indicative prices for up to 20 destinations
from one origin in a single API call, counted as **one search** against your allowance for the whole batch.
Results arrive in 2–5 seconds.

```bash
curl -X POST https://letsfg.co/developers/api/v1/flights/discover \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "origin": "JFK",
    "destinations": ["LAX","MIA","ORD","DFW","SEA","DEN","ATL","BOS","LAS","SFO",
                     "PDX","AUS","PHX","MSP","DTW","CLT","SAN","TPA","MCO","BNA"],
    "date_from": "2026-07-15",
    "currency": "USD"
  }'
```

Response is sorted cheapest-first:

```json
{
  "origin": "JFK",
  "results": [
    { "destination": "BOS", "price": 89.00, "currency": "USD", "found": true },
    { "destination": "CLT", "price": 112.00, "currency": "USD", "found": true },
    { "destination": "MIA", "price": 145.00, "currency": "USD", "found": true },
    ...
    { "destination": "ORD", "price": null, "found": false }
  ],
  "summary": {
    "destinations_checked": 20,
    "prices_found": 19,
    "cheapest_destination": "BOS",
    "cheapest_price": 89.00
  },
  "data_note": "Indicative prices. Run POST /flights/search on your chosen destination for final accurate pricing.",
  "billed_as": "1 search for the full batch"
}
```

**Important:** These are indicative prices, not final bookable fares.
They are useful for ranking and sorting destinations. Always run `POST /flights/search`
on the destination the user selects before showing a booking price or CTA —
the full search runs more data sources and will often find better prices.

**Recommended discovery flow:**
1. `POST /flights/discover` — rank 20 destinations, 1 search
2. Show user the top 3–5 cheapest options
3. User picks a destination
4. `POST /flights/search` on that destination — full accurate results, 1 search
5. Show offers and booking links

## Multi-destination full search

`POST /flights/multi-search` fires N destinations in parallel and returns all results
in one call. Each destination counts as one search against your allowance.

```json
{
  "origin": "JFK",
  "destinations": ["LAX", "MIA", "ORD", "DFW", "SEA", "DEN", "ATL", "BOS", "LAS", "SFO"],
  "date_from": "2026-07-15",
  "adults": 1,
  "currency": "USD"
}
```

Maximum 10 destinations per call. `summary.charged_searches` in the response
shows exactly how many searches were counted. `departure_time_from`/`to` apply
to all destinations in the batch.

## Async search with polling

For products that need a loading state while results arrive, use
`POST /flights/search/async` → `GET /flights/results/{search_id}`.
See [Async Search and Polling](api-polling.md) for the full guide and code examples.


**Poll immediately, then every 2 s.** A loop that sleeps before its first poll
puts a floor under a search that now returns in 8–10 s to first results.

On the agent lane (`letsfg.co/api/search`) a result can keep growing after it
first reports `completed`, and the response carries `split_ticket_pending` /
`gf_enrich_pending` to say so — see the
[agent guide](agent-guide.md). Those flags are not part of this API's contract;
if you see them, honour them the same way.

## Sandbox — zero-cost testing

Test your integration without consuming allowance.
See [Sandbox Environment](api-sandbox.md) for the full guide.

## Recommended production flow

1. Build and test against the [sandbox](api-sandbox.md) — free, no connectors fired.
2. Resolve city names to IATA codes with `/flights/locations/{query}`.
3. Confirm `agents/me` shows payment ready, API access enabled, and positive balance.
4. Send `POST /flights/search` (or `/search/async` for a polling UI).
5. Store `passenger_ids`, the chosen `offer.id`, and the original request metadata.
6. Handle 401, 402, 403, 409 explicitly rather than showing a generic failure.

Next: use [Errors and Limits](api-errors.md) to wire the failure states correctly.