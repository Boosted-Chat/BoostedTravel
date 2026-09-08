# Claude Desktop — 5-Minute Quickstart

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

Connect the remote MCP server once and Claude can search and book flights, and search and book hotels. Nothing to install.

## Option A: Remote MCP with the connect flow — **recommended**

### 1. Add the connector

**claude.ai / Claude Desktop:** `Settings → Connectors → Add custom connector`, name it `LetsFG`, URL:

```
https://letsfg.co/developers/api/mcp
```

**Claude Code:**

```bash
claude mcp add --transport http letsfg https://letsfg.co/developers/api/mcp
```

### 2. Approve the connection

Click **Connect**. The consent step opens <https://letsfg.co/connect>, where you add a card or pay 0.00 with Revolut Pay / Google Pay. Any card works, no Revolut account is needed, and the card details go to Revolut — they never touch LetsFG.

Nothing is charged to connect. You pay the ticket price only when you book, and even then the money is held, not taken, until the airline confirms. The token Claude receives is card-backed; it is carried on every tool call for you.

### 3. Search

> Find me the cheapest flight from London to Barcelona next Friday

Search is free: 10 per 10 minutes, 30 per hour, 100 per day per card.

### 4. Book

> Book the 06:25 Ryanair one for Ada Lovelace

Claude asks for the traveller's real details (name as on the passport, date of birth, gender, nationality, email, phone, address), then calls `book_flight`. The fare plus LetsFG's markup is **held** on your card, a LetsFG booking agent buys the ticket from the seller, and the hold is captured only once a real airline PNR exists. If it fails, the hold is released and nothing is charged. A booking takes 4–11 minutes; Claude polls `get_flight_booking` until it reports `completed` with the PNR.

---

## Option B: Local MCP server (`npx letsfg-mcp`)

Use this only if your client cannot connect to a remote MCP server. It needs a card-backed token in its environment — either the one issued through the connect flow above, or one minted in the terminal with **`letsfg auth`**, which opens the same card screen and writes `~/.letsfg/config.json`. (The old Stripe setup behind `letsfg auth` was retired on 2026-09-02 and its tokens revoked.)

Open `Settings → Developer → Edit Config` or edit the file directly:

- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

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

Restart Claude Desktop. The local server sends every request to the letsfg.co server-side engine with your token — no local browsers.

---

## Option C: Paid Developer API — *not for agents*

The same remote URL also accepts a Developer API key (`X-API-Key` header) from the separate, prepaid product at [letsfg.co/developers](https://letsfg.co/developers). It creates a billing account and blocks search until you fund it. See [Onboarding and Billing](api-onboarding.md) if you are building a commercial integration.

---

## What you can do

| Say this | What happens |
|----------|-------------|
| "Find flights from London to Barcelona next Friday" | `search_flights` → offers with prices; `get_flight_results` collects the late-landing split tickets |
| "What's the cheapest way to get from NYC to Tokyo?" | `resolve_location` → `search_flights` |
| "Book the Ryanair one for Ada Lovelace" | `book_flight` (hold on the card, agent buys the ticket) → `get_flight_booking` until `completed` with a PNR |
| "Search hotels in Barcelona for Apr 1-5" | `resolve_hotel_city` → `search_hotels` → rooms + prices. Needs a card on file, for search as well as booking — the connect step already saved one. |
| "Am I connected?" | `get_agent_profile` → payment status and usage |

## Troubleshooting

**"payment_method_required" with an `add_card_url`** → the connection has no card yet. Open <https://letsfg.co/connect> from the consent step and add one; nothing is charged.

**"TOKEN_REVOKED"** → a token from the retired Stripe setup. Disconnect and reconnect the connector; the consent step saves the card again.

**A `402` on search** → you are on the Developer API key (Option C), not the connect flow. Remove the `X-API-Key` header to use the free lane, or connect a Revolut method with `POST /agents/connect-payment`. If the error is `search_allowance_exhausted`, book a flight (which resets the 200 free searches) or top up to buy a block.

**"Cannot start Python"** -> the local server needs Node (`npx`); the remote MCP URL needs nothing installed

**No tools showing** → Restart Claude Desktop. Check the MCP icon in the bottom-left.
