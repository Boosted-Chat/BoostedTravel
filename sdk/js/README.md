# BoostedTravel — Agent-Native Flight Search & Booking (Node.js)

Search 400+ airlines at raw airline prices — **$20-50 cheaper** than Booking.com, Kayak, and other OTAs. Zero dependencies. Built for autonomous AI agents — works with OpenClaw, Perplexity Computer, Claude, Cursor, Windsurf, and any MCP-compatible client.

> 🎥 **[Watch the demo](https://github.com/Boosted-Chat/BoostedTravel#demo-boostedtravel-vs-default-agent-search)** — side-by-side comparison of default agent search vs BoostedTravel CLI.

## Install

```bash
npm install boostedtravel
```

## Quick Start (SDK)

```typescript
import { BoostedTravel, cheapestOffer, offerSummary } from 'boostedtravel';

// Register (one-time)
const creds = await BoostedTravel.register('my-agent', 'agent@example.com');
console.log(creds.api_key); // Save this

// Use
const bt = new BoostedTravel({ apiKey: 'trav_...' });

// Search — FREE
const flights = await bt.search('GDN', 'BER', '2026-03-03');
const best = cheapestOffer(flights);
console.log(offerSummary(best));

// Unlock — $1
const unlock = await bt.unlock(best.id);

// Book — FREE after unlock
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
export BOOSTEDTRAVEL_API_KEY=trav_...

boostedtravel search GDN BER 2026-03-03 --sort price
boostedtravel search LON BCN 2026-04-01 --json  # Machine-readable
boostedtravel unlock off_xxx
boostedtravel book off_xxx -p '{"id":"pas_xxx","given_name":"John",...}' -e john@example.com
```

## API

### `new BoostedTravel({ apiKey, baseUrl?, timeout? })`

### `bt.search(origin, destination, dateFrom, options?)`
### `bt.resolveLocation(query)`
### `bt.unlock(offerId)`
### `bt.book(offerId, passengers, contactEmail, contactPhone?)`
### `bt.setupPayment(token?)`
### `bt.me()`
### `BoostedTravel.register(agentName, email, baseUrl?, ownerName?, description?)`

### Helpers
- `offerSummary(offer)` — One-line string summary
- `cheapestOffer(result)` — Get cheapest offer from search

### `searchLocal(origin, destination, dateFrom, options?)`

Search 73 airline connectors locally (no API key needed). Requires Python + `boostedtravel` installed.

```typescript
import { searchLocal } from 'boostedtravel';

const result = await searchLocal('GDN', 'BCN', '2026-06-15');
console.log(result.total_results);

// Limit browser concurrency for constrained environments
const result2 = await searchLocal('GDN', 'BCN', '2026-06-15', { maxBrowsers: 4 });
```

### `systemInfo()`

Get system resource profile and recommended concurrency settings.

```typescript
import { systemInfo } from 'boostedtravel';

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

Or set the `BOOSTEDTRAVEL_MAX_BROWSERS` environment variable globally.

## License

MIT
