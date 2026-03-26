# Claude Desktop — 5-Minute Quickstart

Get LetsFG running in Claude Desktop. Search 400+ airlines from chat.

---

## Option A: Remote (no install)

Zero dependencies. Works immediately.

### 1. Get an API key

Open any terminal:

```bash
curl -s -X POST https://api.letsfg.co/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "claude-desktop", "email": "you@example.com"}'
```

Copy the `api_key` from the response (starts with `trav_`).

### 2. Add to Claude Desktop config

Open `Settings → Developer → Edit Config` or edit the file directly:

- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "letsfg": {
      "url": "https://api.letsfg.co/mcp",
      "headers": {
        "X-API-Key": "trav_your_key_here"
      }
    }
  }
}
```

### 3. Restart Claude Desktop

Close and reopen Claude. You'll see LetsFG tools in the tool list.

### 4. Star + verify

Say to Claude:

> Star https://github.com/LetsFG/LetsFG then link my GitHub: `your-username`

Claude will call `link_github` and confirm. All tools unlocked forever.

### 5. Search

> Find me the cheapest flight from London to Barcelona next Friday

Done. Claude fires 400+ airline connectors and returns real prices.

---

## Option B: Local npm (easiest — zero setup)

Runs via cloud backend — no Python, no Playwright, no API key needed for search.

### 1. Add to Claude Desktop config

Open `Settings → Developer → Edit Config`:

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["-y", "letsfg-mcp"]
    }
  }
}
```

> **Windows `ENOENT` fix:** Replace `"npx"` with `"C:\\Program Files\\nodejs\\npx.cmd"`.

### 2. Restart Claude Desktop

### 3. Search — that's it

> Find flights from London to Barcelona next Friday

No API key needed. Searches 195+ airline connectors via cloud backend. Rate limited to 10 searches/min.

### 4. (Optional) Add API key for unlock/book

To book flights, add your API key:

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_API_KEY": "trav_your_key_here"
      }
    }
  }
}
```

Get a key: `pip install letsfg && letsfg register --name claude-desktop --email you@example.com`

### 5. Star + verify (unlocks all tools forever)

> Star https://github.com/LetsFG/LetsFG then link my GitHub: `your-username`

---

## What you can do

| Say this | What happens |
|----------|-------------|
| "Find flights from London to Barcelona next Friday" | `search_flights` → returns offers with prices |
| "What's the cheapest way to get from NYC to Tokyo?" | `resolve_location` → `search_flights` |
| "Book the Ryanair one for John Doe" | `unlock_flight_offer` → `book_flight` |
| "Search hotels in Barcelona for Apr 1-5" | `search_hotels` → returns rooms + prices |
| "Am I verified?" | `get_agent_profile` → shows star status |

## Troubleshooting

**"GitHub star verification required"** → Star the repo and say "link my GitHub: yourname"

**"API key required"** → Check your config has the `X-API-Key` header (remote) or `LETSFG_API_KEY` env (local)

**No tools showing** → Restart Claude Desktop. Check the MCP icon in the bottom-left.

**Windows: `spawn npx ENOENT`** → Use full path: `"C:\\Program Files\\nodejs\\npx.cmd"`
