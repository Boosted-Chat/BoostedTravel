# Windsurf — 5-Minute Quickstart

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

Connect the remote MCP server once and Cascade can search and book flights, and search and book hotels. Nothing to install.

## Option A: Remote MCP with the connect flow — **recommended**

### 1. Edit `~/.codeium/windsurf/mcp_config.json`

```json
{
  "mcpServers": {
    "letsfg": {
      "serverUrl": "https://letsfg.co/developers/api/mcp"
    }
  }
}
```

### 2. Approve the connection

Windsurf prompts you to authenticate the server. The consent step opens <https://letsfg.co/connect>, where you add a card or pay 0.00 with Revolut Pay / Google Pay. Any card works, no Revolut account is needed, and the card details go to Revolut — they never touch LetsFG.

Nothing is charged to connect. You pay the ticket price only when you book, and even then the money is held, not taken, until the airline confirms. The token Windsurf receives is card-backed and is carried on every tool call.

### 3. Restart Windsurf

### 4. Search

> Find flights from Paris to Barcelona for Easter.

Search is free: 10 per 10 minutes, 30 per hour, 100 per day per card.

---

## Option B: Local MCP server (`npx letsfg-mcp`)

Use this only if you cannot use a remote server. It needs a card-backed token in its environment — either the one issued through the connect flow above, or one minted in the terminal with **`letsfg auth`**, which opens the same card screen and writes `~/.letsfg/config.json`. (The old Stripe setup behind `letsfg auth` was retired on 2026-09-02 and its tokens revoked.)

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_BEARER_TOKEN": "your_card_backed_token"
      }
    }
  }
}
```

> **Windows `ENOENT` fix:** Replace `"npx"` with `"C:\\Program Files\\nodejs\\npx.cmd"`.

Restart Windsurf. The local server sends every request to the letsfg.co server-side engine with your token — no local browsers.

---

## Option C: Paid Developer API — *not for agents*

The same remote URL also accepts a Developer API key in an `X-API-Key` header, from the separate, prepaid product at [letsfg.co/developers](https://letsfg.co/developers). It creates a billing account and blocks search until you fund it. See [Onboarding and Billing](api-onboarding.md) if you are building a commercial integration.

---

## Use in Cascade

Cascade can chain LetsFG tools in multi-step flows:

> "Plan a trip from London to Istanbul. Find flights for April 10-15 and hotels near Sultanahmet, then book the cheapest flight for me."

Cascade will:
1. `search_flights("LON", "IST", "2026-04-10", return: "2026-04-15")`, then `get_flight_results` for the late-landing split tickets
2. `resolve_hotel_city("Istanbul")` then `search_hotels(city_id, city_name, "2026-04-10", "2026-04-15")`
   — hotels need a card on file; the connect step already saved one
3. Present both results together
4. Ask for the traveller's real details, then `book_flight` — the fare plus LetsFG's markup is held on your card, a LetsFG booking agent buys the ticket, and the hold is captured only against a real airline PNR. If it fails, the hold is released and nothing is charged
5. `get_flight_booking` every 20–30 s until `completed` with the PNR (a booking takes 4–11 minutes)

## Troubleshooting

**"payment_method_required" with an `add_card_url`** → the connection has no card yet. Open <https://letsfg.co/connect> from the consent step and add one; nothing is charged.

**"TOKEN_REVOKED"** → a token from the retired Stripe setup. Remove and re-add the server so the consent step saves the card again.

**A `402` on search** → you are on the Developer API key (Option C), not the connect flow. Remove the `X-API-Key` header to use the free lane, or connect a Revolut method with `POST /agents/connect-payment`. If the error is `search_allowance_exhausted`, book a flight (which resets the 200 free searches) or top up to buy a block.

**Tools not appearing** → Check `mcp_config.json` path and JSON validity. Restart Windsurf.

**"Cannot start Python"** -> the local server needs Node (`npx`); the remote MCP URL needs nothing installed

**Windows: `spawn npx ENOENT`** → Use full path: `"C:\\Program Files\\nodejs\\npx.cmd"`
