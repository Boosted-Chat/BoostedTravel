---
hide:
  - toc
---

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

<section class="docs-hero">
  <div class="docs-hero-inner">
    <p class="docs-kicker">Official LetsFG documentation</p>
    <h1>Search hundreds of airlines server-side at letsfg.co — and book. Free with a card-backed token. Move to the Developer API for direct booking, hotels and volume access.</h1>
    <p class="docs-lead">LetsFG has two paths. Connect the MCP server at <code>letsfg.co/developers/api/mcp</code> once — the consent step saves a card at <code>letsfg.co/connect</code>, nothing is charged — and search and book for free from Claude, ChatGPT, Cursor, Windsurf or the SDK. If you want account-managed access, hotels, or billing controls, register on the developer surface at letsfg.co, connect a Revolut method, and search and book with your developer key. Flight search there is look-to-book: 200 searches free after every booking.</p>
    <div class="docs-command"><span class="docs-command-prompt">$</span> pip install letsfg</div>
    <div class="docs-action-row">
      <a href="getting-started/" class="docs-button docs-button--primary">Get started</a>
      <a href="api-guide/" class="docs-button docs-button--ghost">Public API guide</a>
      <a href="https://letsfg.co/developers/api/docs" class="docs-button docs-button--ghost" target="_blank">Swagger</a>
      <a href="https://letsfg.co/en/developers" class="docs-button docs-button--ghost" target="_blank">Developers page</a>
    </div>
    <div class="docs-chip-row">
      <span class="docs-chip">Server-side search engine at letsfg.co</span>
      <span class="docs-chip">Canonical API at letsfg.co/developers/api</span>
      <span class="docs-chip">Free card-backed token, or look-to-book on the Developer API</span>
      <span class="docs-chip">CLI, SDK, and MCP</span>
    </div>
  </div>
</section>

## Two paths to LetsFG

LetsFG has two access paths — pick the one that matches your setup:

| Path | How | Speed | Search cost | Booking |
|------|-----|-------|-------------|---------|
| **MCP / SDK** (connect at [letsfg.co/connect](https://letsfg.co/connect)) | Server-side search + booking; one-time 0.00 card setup during the MCP consent → card-backed token | 8–10 s to first results; longer to `completed`, longer again on a split | Free | Fare held on the card, captured only against a real PNR; no separate LetsFG fee |
| **Developer API** ([letsfg.co/developers](https://letsfg.co/developers)) | Runs on our servers; connect a Revolut method | 2–5 s (discover) · 8–10 s to first results (full search) | Look-to-book: 200 free per booking, then $0.01 | `POST /flights/book` — fare held on the connected method, captured only against a real PNR. No booking fee, no transaction fee |

**When to choose each:**
- Use **MCP / SDK** if you want free search and booking — add `https://letsfg.co/developers/api/mcp` to your assistant and approve it once ([letsfg.co/for-agents](https://letsfg.co/for-agents)), then search and book server-side for free. `book_flight` / `POST /api/agent-book` holds the fare on the connected card and a LetsFG booking agent buys the ticket — no unlock step, no LetsFG fee on top of the price you saw.
- Use the **Developer API** if you're building a product, need hotels, or want account-level billing controls. Look-to-book search (200 free after every booking, then $0.01), results in seconds, and `POST /flights/book` with no booking fee and no transaction fee.

---

## Pick your lane

<div class="docs-mode-grid">
  <article class="docs-mode-card">
    <p class="docs-card-kicker">MCP / SDK mode</p>
    <h2>Search and book free after a one-time connect</h2>
    <p>Add the MCP server at <code>letsfg.co/developers/api/mcp</code> and approve it. The consent step opens <code>letsfg.co/connect</code>, where a card is saved in a 0.00 setup (nothing is charged). All search runs server-side at letsfg.co — no local browsers required.</p>
    <ul class="docs-check-list">
      <li><code>search_flights</code>, <code>book_flight</code> and <code>get_flight_booking</code> over the MCP; <code>bt.search()</code> / <code>bt.book()</code> with the same token in <code>LETSFG_BEARER_TOKEN</code></li>
      <li>Search is free: 10 per 10 min, 30 per hour, 100 per day per card</li>
      <li>Best for prototyping, agents, and general flight search</li>
    </ul>
    <a href="getting-started/#option-a-free-search-and-booking-with-a-card-backed-token" class="docs-text-link">Go to setup</a>
  </article>

  <article class="docs-mode-card">
    <p class="docs-card-kicker">Public developer API</p>
    <h2>Use the website-owned contract</h2>
    <p>Use this when you want managed cloud search, billing controls, the public OpenAPI contract, or hosted onboarding through letsfg.co.</p>
    <ul class="docs-check-list">
      <li>Register first and keep the returned <code>X-API-Key</code></li>
      <li>Connect a Revolut method with <code>POST /agents/connect-payment</code> — nothing is charged</li>
      <li>Search is look-to-book: 200 free after every booking, then blocks of 500 for $5.00</li>
    </ul>
    <a href="api-guide/" class="docs-text-link">Go to public API guide</a>
  </article>
</div>

<div class="docs-callout">
  <strong>Important:</strong> the public developer API is not anonymous search. Search requests are rejected until the developer account has an API key and a connected Revolut payment method. Prepaid balance is only needed once the free look-to-book allowance runs out.
</div>

## Public API onboarding

<div class="docs-step-strip">
  <span class="docs-step">1. Register</span>
  <span class="docs-step-arrow">/</span>
  <span class="docs-step">2. Connect a Revolut method</span>
  <span class="docs-step-arrow">/</span>
  <span class="docs-step">3. Top up balance</span>
  <span class="docs-step-arrow">/</span>
  <span class="docs-step">4. Search</span>
  <span class="docs-step-arrow">/</span>
  <span class="docs-step">5. Check account state</span>
</div>

The canonical public surfaces are:

- API root: [letsfg.co/developers/api](https://letsfg.co/developers/api)
- OpenAPI JSON: [letsfg.co/developers/api/openapi.json](https://letsfg.co/developers/api/openapi.json)
- Swagger UI: [letsfg.co/developers/api/docs](https://letsfg.co/developers/api/docs)

## Hotels

Hotels are live: real bookable inventory, free-cancellation and pay-later rates only, 5% charged at booking as a non-refundable reservation fee and the balance paid straight to the supplier through a pay link. They need a card on file for every call, search included — and either credential reaches them: the card-backed token from the connect step or a Developer API key. Start at [Hotels](hotels.md).

## Start from the right page

<div class="docs-resource-grid">
  <a class="docs-resource-card" href="getting-started/">
    <p class="docs-card-kicker">Start</p>
    <h3>Getting started</h3>
    <p>Install once, choose the correct mode, and run the first search without mixing local and public flows.</p>
  </a>

  <a class="docs-resource-card" href="api-guide/">
    <p class="docs-card-kicker">Contract</p>
    <h3>Public API overview</h3>
    <p>See the canonical URLs, the paid search lifecycle, and the exact pages to follow for onboarding, search, and errors.</p>
  </a>

  <a class="docs-resource-card" href="api-onboarding/">
    <p class="docs-card-kicker">Billing</p>
    <h3>Onboarding and billing</h3>
    <p>Register, connect a Revolut method, understand the look-to-book allowance, and rotate keys safely.</p>
  </a>

  <a class="docs-resource-card" href="api-search/">
    <p class="docs-card-kicker">Search</p>
    <h3>Search and results</h3>
    <p>Resolve locations, build search payloads, understand passenger IDs, and store the fields you actually need.</p>
  </a>

  <a class="docs-resource-card" href="api-errors/">
    <p class="docs-card-kicker">Ops</p>
    <h3>Errors and limits</h3>
    <p>Map account state to 401, 402, 403, 409, and 429 responses before you ship the paid API flow.</p>
  </a>

  <a class="docs-resource-card" href="openapi/">
    <p class="docs-card-kicker">Schema</p>
    <h3>OpenAPI and Swagger</h3>
    <p>Use the website-owned machine-readable schema instead of the old raw repository copy.</p>
  </a>

  <a class="docs-resource-card" href="packages/">
    <p class="docs-card-kicker">Tooling</p>
    <h3>Packages and SDKs</h3>
    <p>See what ships in the Python SDK, JS SDK, local MCP server, and remote MCP endpoint without mixing their search modes.</p>
  </a>

  <a class="docs-resource-card" href="agent-guide/">
    <p class="docs-card-kicker">Agents</p>
    <h3>AI agent guide</h3>
    <p>Build agents that use the Bearer token for free search and the Developer API when managed cloud access matters.</p>
  </a>

  <a class="docs-resource-card" href="cli-reference/">
    <p class="docs-card-kicker">CLI</p>
    <h3>CLI reference</h3>
    <p>Check command flags, environment variables, and how the CLI maps to local and public flows.</p>
  </a>
</div>
