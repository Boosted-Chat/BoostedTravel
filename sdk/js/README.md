# LetsFG — Your AI agent just learned to book flights. (Node.js)

**200 airlines. Real prices. One function call.** Search 400+ airlines at raw airline prices — **$20–$50 cheaper** than Booking.com, Kayak, and other OTAs. Zero dependencies. Built for AI agents.

> **Don't want to install anything?** [**Try it on Messenger**](https://m.me/61579557368989) — search flights instantly, no setup needed.

[![GitHub stars](https://img.shields.io/github/stars/LetsFG/LetsFG?style=social)](https://github.com/LetsFG/LetsFG)
[![npm](https://img.shields.io/npm/v/letsfg)](https://www.npmjs.com/package/letsfg)

> ⭐ **[Star the repo](https://github.com/LetsFG/LetsFG) → register → unlimited access forever.** First 1,000 stars only.

## Install

```bash
npm install letsfg
```

## Quick Start (SDK)

```typescript
import { LetsFG, cheapestOffer, offerSummary } from 'letsfg';

// Register (one-time)
const creds = await LetsFG.register('my-agent', 'agent@example.com');
console.log(creds.api_key); // Save this

// Use
const bt = new LetsFG({ apiKey: 'trav_...' });

// Search — FREE
const flights = await bt.search('GDN', 'BER', '2026-03-03');
const best = cheapestOffer(flights);
console.log(offerSummary(best));

// Unlock
const unlock = await bt.unlock(best.id);

// Book
const booking = await bt.book(
  best.id,
  [{
    id: flights.passenger_ids[0],
    given_name: 'John',
    family_name: 'Doe',
    born_on: '1990-01-15',
    gender: 'm',
    title: 'mr',
    email: 'john@example.com',
  }],
  'john@example.com'
);
console.log(`PNR: ${booking.booking_reference}`);
```

## Quick Start (CLI)

```bash
export LETSFG_API_KEY=trav_...

letsfg search GDN BER 2026-03-03 --sort price

# Fast mode — OTAs + key airlines only (~25 connectors, 20-40s)
letsfg search GDN BER 2026-03-03 --mode fast
letsfg search LON BCN 2026-04-01 --json  # Machine-readable
letsfg unlock off_xxx
letsfg book off_xxx -p '{"id":"pas_xxx","given_name":"John",...}' -e john@example.com
```

## API

### `new LetsFG({ apiKey, baseUrl?, timeout? })`

### `bt.search(origin, destination, dateFrom, options?)`
### `bt.resolveLocation(query)`
### `bt.unlock(offerId)`
### `bt.book(offerId, passengers, contactEmail, contactPhone?)`
### `bt.setupPayment(token?)`
### `bt.me()`
### `LetsFG.register(agentName, email, baseUrl?, ownerName?, description?)`

### Helpers
- `offerSummary(offer)` — One-line string summary
- `cheapestOffer(result)` — Get cheapest offer from search

### `searchLocal(origin, destination, dateFrom, options?)`

Search 200 airline connectors locally (no API key needed). Requires Python + `letsfg` installed.

```typescript
import { searchLocal } from 'letsfg';

const result = await searchLocal('GDN', 'BCN', '2026-06-15');
console.log(result.total_results);

// Limit browser concurrency for constrained environments
const result2 = await searchLocal('GDN', 'BCN', '2026-06-15', { maxBrowsers: 4 });
```

### `systemInfo()`

Get system resource profile and recommended concurrency settings.

```typescript
import { systemInfo } from 'letsfg';

const info = await systemInfo();
console.log(info);
// { platform: 'win32', cpu_cores: 16, ram_total_gb: 31.2, ram_available_gb: 14.7,
//   tier: 'standard', recommended_max_browsers: 8, current_max_browsers: 8 }
```

## Zero Dependencies

Uses native `fetch` (Node 18+). No `axios`, no `node-fetch`, nothing. Safe for sandboxed environments.

## Performance Tuning

Local search auto-scales browser concurrency based on available RAM. Override with `maxBrowsers`:

```typescript
// Limit to 4 concurrent browsers
await searchLocal('LHR', 'BCN', '2026-04-15', { maxBrowsers: 4 });
```

Or set the `LETSFG_MAX_BROWSERS` environment variable globally.

## Also Available As

- **MCP Server**: `npx letsfg-mcp` — [npm](https://www.npmjs.com/package/letsfg-mcp)
- **Python SDK + CLI**: `pip install letsfg` — [PyPI](https://pypi.org/project/letsfg/)
- **Try without installing**: [Message us on Messenger](https://m.me/61579557368989)
- **GitHub**: [LetsFG/LetsFG](https://github.com/LetsFG/LetsFG)

> ⭐ **[Star the repo](https://github.com/LetsFG/LetsFG)** to unlock free access. First 1,000 stars only.

## License

MIT
