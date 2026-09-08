# OpenAPI and Swagger

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

<div class="docs-callout">
  <strong>Use the website-owned schema.</strong> The canonical public contract lives at <code>https://letsfg.co/developers/api/openapi.json</code>. Treat that as the live machine-readable surface, not the old raw repository link.
</div>

<div class="docs-action-row">
  <a href="https://letsfg.co/developers/api/openapi.json" class="docs-button docs-button--primary" target="_blank">OpenAPI JSON</a>
  <a href="https://letsfg.co/developers/api/docs" class="docs-button docs-button--ghost" target="_blank">Swagger UI</a>
  <a href="https://letsfg.co/en/developers" class="docs-button docs-button--ghost" target="_blank">Developers page</a>
</div>

## Which URL does what?

| Surface | Best for |
|---------|----------|
| `https://letsfg.co/developers/api/openapi.json` | Machine-readable schema for generators, SDK tooling, and agent discovery |
| `https://letsfg.co/developers/api/docs` | Interactive Swagger UI |
| `https://letsfg.co/developers/api/v1` | The actual REST base for requests |
| `https://letsfg.co/en/developers` | Human onboarding, hosted checkout, and account context |

## What is in the live schema?

The live public schema currently includes paths for:

- `POST /agents/register`
- `POST /agents/connect-payment`
- `POST /agents/top-up`
- `POST /agents/hosted-checkout` — **retired 2026-09-08**, answers `410 Gone`
- `POST /agents/hosted-checkout/complete` — **retired 2026-09-08**, answers `410 Gone`
- `POST /agents/setup-payment` — **retired 2026-09-08**, answers `410 Gone`
- `POST /agents/billing-portal` — **retired 2026-09-08**, answers `410 Gone`
- `POST /agents/billing-settings`
- `POST /agents/rotate-key`
- `GET /agents/me`
- `POST /flights/search`
- `GET /flights/locations/{query}`
- `GET /flights/providers`

## Fetch the schema

```bash
curl https://letsfg.co/developers/api/openapi.json | jq '.info, .servers'
```

List the documented paths:

```bash
curl https://letsfg.co/developers/api/openapi.json | jq '.paths | keys'
```

## Why this matters

- The website-owned schema matches the public proxy rules for auth and billing.
- It stays aligned with the website developer surface that agents actually use.
- Direct backend hosts are not the canonical public integration contract.

## Current public API scope

The public schema currently covers:

- developer registration and hosted onboarding
- Revolut payment connection for API-only onboarding (`/agents/connect-payment`)
- prepaid balance funding and billing settings
- flight search, location resolution, and provider inspection

<div class="docs-resource-grid">
  <a class="docs-resource-card" href="api-guide/">
    <p class="docs-card-kicker">Overview</p>
    <h3>Public API overview</h3>
    <p>Start here if you need the paid public search lifecycle and canonical URLs before reading the schema.</p>
  </a>

  <a class="docs-resource-card" href="api-onboarding/">
    <p class="docs-card-kicker">Billing</p>
    <h3>Onboarding and billing</h3>
    <p>Map the schema to the real registration, connect-payment and top-up flow.</p>
  </a>

  <a class="docs-resource-card" href="api-search/">
    <p class="docs-card-kicker">Search</p>
    <h3>Search and results</h3>
    <p>Use request-body examples and response notes that line up with the live `flights/search` contract.</p>
  </a>
</div>
