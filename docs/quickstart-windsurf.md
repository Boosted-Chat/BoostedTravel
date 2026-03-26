# Windsurf — 5-Minute Quickstart

Get LetsFG running in Windsurf (Codeium). Search and book flights from Cascade.

---

## Option A: Remote (no install)

Zero dependencies. Works immediately.

### 1. Get an API key

```bash
curl -s -X POST https://api.letsfg.co/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "windsurf", "email": "you@example.com"}'
```

### 2. Add to Windsurf MCP config

Edit `~/.codeium/windsurf/mcp_config.json`:

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

### 3. Restart Windsurf

Close and reopen Windsurf. LetsFG tools appear in the MCP panel.

### 4. Star + verify

In Cascade chat:

> Star https://github.com/LetsFG/LetsFG then link my GitHub: `your-username`

### 5. Search

> Find the cheapest flight from Amsterdam to Rome this weekend

---

## Option B: Local npm (easiest — zero setup)

No API key needed for search. Works immediately.

### 1. Edit `~/.codeium/windsurf/mcp_config.json`

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

### 2. Restart Windsurf

### 3. Search — that's it

> Find flights from Paris to Barcelona for Easter.

No API key needed. Searches 195+ airline connectors via cloud backend. Rate limited to 10 searches/min.

### 4. (Optional) Add API key for unlock/book

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

Get a key: `pip install letsfg && letsfg register --name windsurf --email you@example.com`

---

## Use in Cascade

Cascade can chain LetsFG tools in multi-step flows:

> "Plan a trip from London to Istanbul. Find flights for April 10-15 and hotels near Sultanahmet."

Cascade will:
1. `search_flights("LON", "IST", "2026-04-10", return: "2026-04-15")`
2. `search_hotels("Istanbul Sultanahmet", "2026-04-10", "2026-04-15")`
3. Present both results together

## Troubleshooting

**"GitHub star verification required"** → Star the repo and ask Cascade to call `link_github`

**Tools not appearing** → Check `mcp_config.json` path and JSON validity. Restart Windsurf.

**"API key required"** → Verify `X-API-Key` header (remote) or `LETSFG_API_KEY` env (local)

**Windows: `spawn npx ENOENT`** → Use full path: `"C:\\Program Files\\nodejs\\npx.cmd"`
