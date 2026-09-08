# Public Developer API

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
    <strong>Scope:</strong> this section is only about the website-owned public REST API at <code>https://letsfg.co/developers/api/v1</code>. Local SDK and CLI search still stay free and do not require the paid public onboarding flow.
</div>

## When to use this API

| If you need | Use |
|-------------|-----|
| Free connector search from the repo, CLI, or Python SDK | Local search |
| Managed cloud search, account billing, browserless onboarding, and a stable public contract | Public developer API |

## Canonical URLs

| Surface | URL |
|---------|-----|
| Public API root | [https://letsfg.co/developers/api](https://letsfg.co/developers/api) |
| Public REST base | `https://letsfg.co/developers/api/v1` |
| OpenAPI JSON | [https://letsfg.co/developers/api/openapi.json](https://letsfg.co/developers/api/openapi.json) |
| Swagger UI | [https://letsfg.co/developers/api/docs](https://letsfg.co/developers/api/docs) |
| Developers page | [https://letsfg.co/en/developers](https://letsfg.co/en/developers) |

## Public search lifecycle

<div class="docs-step-strip">
    <span class="docs-step">1. Register</span>
    <span class="docs-step-arrow">/</span>
    <span class="docs-step">2. Connect a Revolut method</span>
    <span class="docs-step-arrow">/</span>
    <span class="docs-step">3. Search</span>
    <span class="docs-step-arrow">/</span>
    <span class="docs-step">4. Book</span>
    <span class="docs-step-arrow">/</span>
    <span class="docs-step">5. Poll the booking</span>
</div>

Public search is not anonymous. Requests stay blocked until the developer account has:

- an API key from `POST /agents/register`
- a Revolut payment method connected through `POST /agents/connect-payment` — open the returned
  `connect_url` once in a browser. **Nothing is charged to connect**, and a connected method is
  what opens search

Prepaid balance is **not** required to start. Flight search is look-to-book: 200 searches are free
after every booking you make, and booking resets the counter. Balance only buys extra blocks of
search past that allowance (`POST /agents/top-up`).

> **Stripe was retired on 2026-09-08.** `setup-payment`, `hosted-checkout`, `billing-portal` and the
> whole `/bookings/*` unlock-then-book lane answer `410 Gone` naming their replacement.

## What the public contract currently covers

The live public schema currently documents these groups of endpoints:

- account registration and account inspection
- Revolut payment connection (`/agents/connect-payment`), billing settings, and key rotation
- prepaid top-up, for search blocks past the free look-to-book allowance
- **flight booking**: `/flights/book`, `/flights/bookings/{id}` and `/flights/bookings/{id}/answer`
  — see [Booking flights](api-booking.md)
- NL query parsing (`/flights/parse-query` — free, Gemini-powered)
- flight search, location resolution, and provider inspection
- discovery search — indicative prices for up to 20 destinations in one call (`/flights/discover` — 1 credit)
- parallel full search for N destinations (`/flights/multi-search` — 1 credit per destination)
- async search with polling (`/flights/search/async` + `/flights/results/{id}`)
- sandbox equivalents of all flight endpoints (free, fake data, same schema — for integration testing)
- hotels: city resolution, search, asynchronous booking with polling, and cancellation (`/hotels/destinations`, `/hotels/search`, `/hotels/book`, `/hotels/booking/{job_id}`, `/hotels/cancel`) — see [Hotels](hotels.md). These require a payment method on file for **every** call, search included, and booking charges a 5% non-refundable reservation fee.

## Search activation checklist

Before you send search traffic, make sure `GET /agents/me` shows:

- `payment.connected: true` — a Revolut method is saved
- `access_granted: true`
- `developer_api.api_access_enabled: true`
- `developer_api.flight_search.searches_remaining` greater than `0`

`balance_cents` matters only once the free allowance is used up, and only then to buy another block.

## Read these pages in order

<div class="docs-resource-grid">
    <a class="docs-resource-card" href="api-onboarding/">
        <p class="docs-card-kicker">Step 1</p>
        <h3>Onboarding and billing</h3>
        <p>Register, connect a Revolut method, understand the look-to-book allowance, and rotate the key safely.</p>
    </a>

    <a class="docs-resource-card" href="api-search/">
        <p class="docs-card-kicker">Step 2</p>
        <h3>Search and results</h3>
        <p>Resolve locations, shape the search payload, inspect providers, and store the response fields that matter.</p>
    </a>

    <a class="docs-resource-card" href="api-booking/">
        <p class="docs-card-kicker">Step 3</p>
        <h3>Booking flights</h3>
        <p>Hold the fare on the connected card, dispatch the booking agent, poll to a PNR, and answer a seat map or a price change.</p>
    </a>

    <a class="docs-resource-card" href="api-errors/">
        <p class="docs-card-kicker">Step 4</p>
        <h3>Errors and limits</h3>
        <p>Map account state and request-body mistakes to the status codes your integration will actually see.</p>
    </a>

    <a class="docs-resource-card" href="api-sandbox/">
        <p class="docs-card-kicker">Testing</p>
        <h3>Sandbox environment</h3>
        <p>Test your integration for free — same schema, realistic fake data, zero credits charged.</p>
    </a>

    <a class="docs-resource-card" href="api-polling/">
        <p class="docs-card-kicker">Async</p>
        <h3>Async search and polling</h3>
        <p>Start a search and poll for results — show a loading state while the full fleet runs.</p>
    </a>

    <a class="docs-resource-card" href="openapi/">
        <p class="docs-card-kicker">Schema</p>
        <h3>OpenAPI and Swagger</h3>
        <p>Use the live website-owned schema instead of stale repository JSON or direct backend hosts.</p>
    </a>
</div>

## Minimal register-and-search example

```bash
curl -X POST https://letsfg.co/developers/api/v1/agents/register \
    -H "Content-Type: application/json" \
    -d '{"agent_name": "my-agent", "email": "you@example.com", "owner_name": "My Team"}'

curl -X POST https://letsfg.co/developers/api/v1/flights/search \
    -H "X-API-Key: letsfg_your_api_key" \
    -H "Content-Type: application/json" \
    -d '{"origin": "LHR", "destination": "JFK", "date_from": "2026-07-15", "adults": 1, "currency": "USD"}'
```

The second request succeeds only after payment is attached and prepaid balance has been funded.
