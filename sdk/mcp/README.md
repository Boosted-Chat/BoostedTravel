# letsfg-mcp — Your AI agent just learned to book flights.

**Hundreds of airlines. Real prices. One tool call.** The largest open flight-search MCP server. Search is powered server-side at letsfg.co — Ryanair, EasyJet, Wizz Air, Southwest, AirAsia, Qantas, and 194 more — plus enterprise GDS/NDC providers (Amadeus, Duffel, Sabre) through the LetsFG backend.

**Your agent doesn't need to build a flight integration. Just add this MCP server.**

[![npm](https://img.shields.io/npm/v/letsfg-mcp)](https://www.npmjs.com/package/letsfg-mcp)
[![GitHub stars](https://img.shields.io/github/stars/LetsFG/LetsFG?style=social)](https://github.com/LetsFG/LetsFG)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)

---

## What This Is

LetsFG is not a scraper wrapper. It's a production-grade **agent-to-airline connector layer**:

- **Hundreds of airlines** searched server-side at letsfg.co — connect a card once at letsfg.co/connect (nothing charged) and search is free
- **Real booking** — `book_flight` holds the fare on that card, a LetsFG booking agent buys the ticket, and the hold is captured only once a real airline PNR exists
- **Enterprise GDS/NDC feeds** (Amadeus, Duffel, Sabre, Travelport, Kiwi) are included in every search — contract-only data sources made available to everyone through the LetsFG backend
- **Virtual interlining** — the combo engine mixes one-way fares across airlines (e.g., Ryanair outbound + Wizz Air return) to find deals no single airline offers
- **City-wide airport expansion** — search `LON` and it auto-searches LHR, LGW, STN, LTN, SEN, LCY. Works for 25+ major cities worldwide
- **Zero price bias** — no cookie tracking, no demand-based inflation, no surge pricing. Same raw airline price every time

**Why agents should use this instead of building their own:**
- Saves thousands of tokens vs. browser automation or scraping
- Server-side engine scans airlines across Europe, Asia, Americas, Middle East, Africa, and Oceania simultaneously
- Finds deals on carriers your agent wouldn't think to check (Lucky Air, 9 Air, Jazeera Airways, FlySafair...)
- Enterprise-contracted GDS deals that require contracts worth $50k+/year — available for free on search

---

## Quick Start

**Recommended — the hosted MCP, no install:**

```
https://letsfg.co/developers/api/mcp
```

Add it as a remote MCP server in Claude, ChatGPT, Cursor or Windsurf (or
`claude mcp add --transport http letsfg https://letsfg.co/developers/api/mcp`
in Claude Code) and approve the connection. The consent step opens
[letsfg.co/connect](https://letsfg.co/connect), where you add a card (any
card, or Revolut Pay / Google Pay) in a 0.00 Revolut setup. Nothing is
charged, no Revolut account is needed, and the card details go to Revolut,
never to LetsFG. The token is card-backed: it searches and it books, and it
is carried on every tool call for you.

**This package — the stdio server, runs on your machine:**

```bash
npx letsfg-mcp
```

It needs that same card-backed token in `LETSFG_BEARER_TOKEN`. To mint one
from the command line, run **`letsfg auth`**: it registers itself as an OAuth
client, opens letsfg.co/connect for a person to approve, and writes the token to
`~/.letsfg/config.json` (`--no-browser` prints the URL instead). This server's
own `authenticate` tool returns the current instructions rather than minting a
token, since a person has to approve in a browser either way.

> **Retired 2026-09-02:** the Stripe card setup and every token it issued
> (401 `TOKEN_REVOKED`). Reconnect at letsfg.co/connect.

---

## Client Configuration

The hosted server is the simplest option everywhere: it does the connect
flow for you and needs no token in a config file.

### Claude (claude.ai / Claude Desktop / ChatGPT)

Add a custom connector with the URL `https://letsfg.co/developers/api/mcp`
and approve it. The consent step takes you through letsfg.co/connect.

### Claude Code

```bash
claude mcp add --transport http letsfg https://letsfg.co/developers/api/mcp
```

### Cursor

Add to `.cursor/mcp.json` in your project root:

```json
{
  "mcpServers": {
    "letsfg": { "url": "https://letsfg.co/developers/api/mcp" }
  }
}
```

### Windsurf

Add to `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "letsfg": { "serverUrl": "https://letsfg.co/developers/api/mcp" }
  }
}
```

### The stdio server instead (`npx letsfg-mcp`)

Same shape in any of the files above, with the card-backed token from the
connect flow in the environment (a paid Developer API key in
`LETSFG_API_KEY` also works, on that separate product):

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_BEARER_TOKEN": "eyJ..."
      }
    }
  }
}
```

### Continue

Add to `~/.continue/config.yaml`:

```yaml
mcpServers:
  - name: letsfg
    command: npx
    args: ["-y", "letsfg-mcp"]
    env:
      LETSFG_BEARER_TOKEN: eyJ...
```

### Any MCP-Compatible Agent

Connect via remote MCP (no install, does the card connect for you):

```
https://letsfg.co/developers/api/mcp
```

Or run the stdio server with a token in `LETSFG_BEARER_TOKEN`:

```bash
npx letsfg-mcp
```

### Windows — `npx ENOENT` Fix

If you get `spawn npx ENOENT` on Windows, use the full path to `npx`:

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "C:\\Program Files\\nodejs\\npx.cmd",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_BEARER_TOKEN": "eyJ..."
      }
    }
  }
}
```

Or use `node` directly:

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "node",
      "args": ["C:\\Users\\YOU\\AppData\\Roaming\\npm\\node_modules\\letsfg-mcp\\dist\\index.js"],
      "env": {
        "LETSFG_BEARER_TOKEN": "eyJ..."
      }
    }
  }
}
```

### Pin a Specific Version

To avoid unexpected updates:

```json
{
  "command": "npx",
  "args": ["-y", "letsfg-mcp@1.0.0"]
}
```

---

## Available Tools

| Tool | Description | Cost | Side Effects |
|------|-------------|------|--------------|
| `search_flights` | Search hundreds of airlines via server-side engine | FREE | None (read-only) |
| `resolve_hotel_city` | Place name -> supplier city id | FREE | None (read-only) |
| `search_hotels` | Search bookable, free-cancellation hotel rates | Free up to 1,000/booking, then $5/1,000 — needs a card on file | Opens a supplier session |
| `book_hotel` | Start a hotel booking (async, returns a job id) | 5% now, balance to the supplier | Charges the card, books the room |
| `get_hotel_booking` | Collect the booking result and pay link | FREE | None (read-only) |
| `cancel_hotel_booking` | Release a reservation | Free until `balance_due_by`, then the hotel's ladder | Cancels the booking |
| `resolve_location` | City name → IATA code | FREE | None (read-only) |
| `book_flight` | Start a real booking: fare held on the connected card, a LetsFG agent buys the ticket, captured on a real PNR | Ticket price (LetsFG's markup is inside the price) | Places a hold, creates the booking |
| `get_flight_booking` | Poll a booking started by `book_flight` until `completed` / `failed` / `needs_attention` | FREE | None (read-only) |
| `unlock_flight_offer` | **RETIRED 2026-09-08** — the tool refuses locally and the route answers `410 Gone` | — | Call `book_flight` directly |
| `setup_payment` | **Developer API only** — attach a card to a paid prepaid account. Not how agents connect | FREE | Updates payment |
| `get_agent_profile` | Usage stats & payment status | FREE | None (read-only) |

### Booking Flow

**PFS (card-backed token from letsfg.co/connect):**

```
connect (once)  →  search_flights  →  book_flight  →  get_flight_booking (poll)
  (0.00 setup)        (free)          (hold + agent)     (PNR in 4-11 min)
```

1. `search_flights("LON", "BCN", "2026-06-15")` — server-side search returns offers from hundreds of airlines in 8–10 s to first results; collect late arrivals with `get_flight_results`
2. `book_flight(search_id, offer_id, passengers, contact_email)` — exactly what the website checkout does: the fare plus LetsFG's markup is **held** on the connected card (not taken), a LetsFG booking agent buys the ticket from the seller, and the hold is captured only once a real airline PNR exists. Returns a `booking_ref` within seconds. One traveller per call, with the details an airline checkout asks for (name, date of birth, gender, nationality, email, phone with its country, residence address; passport optional). A missing detail returns `missing_fields` and charges nothing.
3. `get_flight_booking(booking_ref)` every 20–30 s — `booking_in_progress` → `completed` (PNR, captured amount) | `failed` (hold released, nothing charged) | `needs_attention` (a human at LetsFG is checking; do not book again). A booking legitimately takes 4–11 minutes.

No unlock step, no booking-link fallback, no separate LetsFG fee. Never call
`book_flight` twice for the same trip while one is in progress — that would
place a second hold.

> The stdio server books through the same `/api/agent-book` hold + poll flow as the
> hosted MCP: `book_flight` returns a `booking_ref`, then `get_flight_booking`
> reports the outcome. It takes **one passenger per call** and refuses more rather
> than silently booking only the first. (Until 2026-09-02 this package had no
> `get_flight_booking` at all, so a stdio agent could start a booking and never
> learn whether it landed — that is fixed.)

**Developer API (look-to-book search, real booking, hotels):**

The [Developer API](https://letsfg.co/developers) books flights itself: `POST /flights/book` holds
the fare on a connected Revolut method and a LetsFG booking agent buys the ticket, exactly like
this package does. Every offer carries a `booking_url` that points at a letsfg.co page for that
offer — never a seller deep link. Search is 200 free after every booking, then $0.01. No booking
fee, no transaction fee. Use this path for volume, account-level billing, or hotels.

The agent has native tools — no API docs needed, no URL building, no token-burning browser automation.

### Response Mode (Remote MCP only)

`search_flights` and `search_hotels` accept an optional `response_mode` parameter:

| Mode | Default | What's returned | Best for |
|------|---------|----------------|----------|
| `"summary"` | ✅ | Price, airlines, route, departure, stops | Chat, quick comparisons |
| `"full"` | | Everything: segments, durations, conditions, bags, booking URLs | Deep analysis, programmatic use |

**Summary mode** saves tokens by stripping per-segment details, baggage policies, and booking conditions. Ask for `response_mode: "full"` on `search_flights` or `get_flight_results` when you need them.

```jsonc
// summary response (search_flights)
{
  "total_offers": 42,
  "offers": [
    { "id": "off_abc", "price": "€29", "airlines": ["FR"], "route": "STN→BCN", "departure": "06:15", "stops": 0 }
  ],
  "hint": "More offers are still landing — call get_flight_results before recommending."
}
```

---

## Starlink Wi-Fi

Offers may carry `starlink`: `confirmed_all` / `confirmed_some` mean the carrier
has **fully** fitted that aircraft type; `likely_all` / `likely_some` mean the
rollout on that type is underway but incomplete. Segments carry `confirmed` or
`likely`.

Only `confirmed_*` is safe to state as fact — `likely_*` is a signal, not a
promise. Anything ending `_some` has at least one leg without it. An **absent**
field means no information, **not** an absence of Wi-Fi.

Full semantics: [docs/api-search.md](https://github.com/LetsFG/LetsFG/blob/main/docs/api-search.md#starlink-wi-fi).

## Get an API Key (Developer API only)

Most agents do not need this: the card-backed token from letsfg.co/connect
already searches and books. An API key belongs to the separate, paid,
prepaid-balance Developer API. If that is what you want, register at
[letsfg.co/developers](https://letsfg.co/developers) or via CLI:

```bash
pip install letsfg
letsfg register --name my-agent --email you@example.com
```

Or directly via the API:

```bash
curl -X POST https://letsfg.co/developers/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "agent@example.com"}'
```


---

## Architecture & Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│  MCP Client  (Claude Desktop / Cursor / Windsurf / etc.)     │
│     ↕ stdio (JSON-RPC)                                       │
├──────────────────────────────────────────────────────────────┤
│  letsfg-mcp  (this package, runs on YOUR machine)            │
│     │                                                        │
│     └─→ HTTPS to letsfg.co (all search + booking)           │
│           search, book (hold → agent → PNR), GDS/NDC feeds    │
└──────────────────────────────────────────────────────────────┘
```

All search and booking run server-side at letsfg.co. No local browsers or scrapers are involved. Connect a card once at letsfg.co/connect (0.00 Revolut setup, nothing charged) and the token is card-backed for search and booking.

### What data goes where

| Operation | Where data flows | What is sent |
|-----------|-----------------|--------------|
| `search_flights` | Your machine → letsfg.co → airlines + GDS providers | Route, date, passenger count |
| `resolve_location` | Your machine → letsfg.co | City/airport name |
| `book_flight` | Your machine → letsfg.co → LetsFG booking agent → seller | Passenger name, DOB, nationality, email, phone, address; the hold goes to Revolut |
| `get_flight_booking` | Your machine → letsfg.co | booking_ref |
| `unlock_flight_offer` / `setup_payment` | Your machine → letsfg.co (Developer API only) | Offer ID / payment token |

---

## Security & Privacy

- **TLS everywhere** — all communication uses HTTPS. The server-side engine connects to airline websites over HTTPS.
- **No card storage** — the card is saved with Revolut at letsfg.co/connect. LetsFG never sees or stores raw card numbers.
- **API key scoping** — `LETSFG_API_KEY` grants access only to your agent's account. Keys are prefixed `letsfg_` for easy identification and revocation.
- **PII handling** — passenger names, emails, and DOBs are sent to the airline for booking (required by airlines). LetsFG does not store passenger PII after forwarding to the airline.
- **No tracking** — no cookies, no session-based pricing, no fingerprinting. Every search returns the same raw airline price.
- **Open source** — the SDK and MCP server code is MIT-licensed and auditable at [github.com/LetsFG/LetsFG](https://github.com/LetsFG/LetsFG).

---

## Sandbox / Test Mode

Search costs nothing, so develop against real results. `book_flight` places a
real hold on a real card; there is no test card on the PFS lane. The paid
Developer API has a keyless sandbox — see
[docs/api-sandbox.md](https://github.com/LetsFG/LetsFG/blob/main/docs/api-sandbox.md).

---

## FAQ

### `spawn npx ENOENT` on Windows

Windows can't find `npx` in PATH. Use the full path:
```json
"command": "C:\\Program Files\\nodejs\\npx.cmd"
```
Or install globally and use `node` directly (see Windows config above).

### HTTP 403 with an anti-bot challenge page (VPS / datacenter hosts)

If a tool returns `HTTP 403 … an anti-bot challenge page was returned instead of the API`,
the request never reached the search engine — something in the network path in front of
`letsfg.co` answered it, and no token can get past that. It is seen from datacenter and
VPS IPs, which is where MCP servers normally run.

**Upgrade first: `2026.5.74` and later send a declared-bot User-Agent that is not singled
out this way** (`Mozilla/5.0 (compatible; letsfg-mcp/1.3.1; +https://github.com/LetsFG/LetsFG)`).
If you pinned an older version, that is the likeliest cause.

If it still happens, override the User-Agent yourself:

```json
"env": {
  "LETSFG_BEARER_TOKEN": "eyJ...",
  "LETSFG_USER_AGENT": "Mozilla/5.0 (compatible; letsfg-mcp)"
}
```

Please also open an issue with your host and egress IP — the durable fix belongs on our
side, not in your config.

### Search returns 0 results

- Check IATA codes are correct — use `resolve_location` first
- Try a date 2+ weeks in the future (airlines don't sell last-minute on all routes)
- Connect a card through the hosted MCP if you haven't yet — a valid card-backed token is required for free search (`LETSFG_BEARER_TOKEN` for the stdio server)

### How do I get free search without a Developer API key?

Connect LetsFG as an MCP server at `https://letsfg.co/developers/api/mcp` and approve it; the consent step opens letsfg.co/connect, where you add a card in a 0.00 Revolut setup. Nothing is charged and there is no authorization hold. The token you get is card-backed and works for `POST /api/search` and `POST /api/agent-book`. Tokens from the old `letsfg auth` (Stripe) flow were revoked on 2026-09-02 — reconnect.

### Can my agent actually book, or just search?

Book. `book_flight` does what the website checkout does: the fare is held on the connected card, a LetsFG booking agent buys the ticket, and the hold is captured only once a real airline PNR exists — for every offer, not just one supplier. Poll `get_flight_booking` until `completed`; a `failed` booking releases the hold and charges nothing.

### Can I use this for commercial projects?

Yes. MIT license. The SDK, MCP server, and ranking engine are fully open source.

### MCP server hangs on start

Ensure Node.js 18+ is installed. The server communicates via stdio (stdin/stdout JSON-RPC) — it doesn't open a port or print a "ready" message. MCP clients handle the lifecycle automatically.

---

<details>
<summary><strong>Airlines covered — sample list</strong></summary>

| Region | Airlines |
|--------|----------|
| **Europe** | Ryanair, Wizz Air, EasyJet, Norwegian, Vueling, Eurowings, Transavia, Pegasus, Turkish Airlines, Condor, SunExpress, Volotea, Smartwings, Jet2, LOT Polish Airlines, Finnair, SAS, Aegean, Aer Lingus, ITA Airways, TAP Portugal, Icelandair, PLAY |
| **Middle East & Africa** | Emirates, Etihad, Qatar Airways, flydubai, Air Arabia, flynas, Salam Air, Air Peace, FlySafair, EgyptAir, Ethiopian Airlines, Kenya Airways, Royal Air Maroc, South African Airways |
| **Asia-Pacific** | AirAsia, IndiGo, SpiceJet, Akasa Air, Air India, Air India Express, VietJet, Cebu Pacific, Scoot, Jetstar, Peach, Spring Airlines, Lucky Air, 9 Air, Nok Air, Batik Air, Jeju Air, T'way Air, ZIPAIR, Singapore Airlines, Cathay Pacific, Malaysian Airlines, Thai Airways, Korean Air, ANA, JAL, Qantas, Virgin Australia, Bangkok Airways, Air New Zealand, Garuda Indonesia, Philippine Airlines, US-Bangla, Biman Bangladesh |
| **Americas** | American Airlines, Delta, United, Southwest, JetBlue, Alaska Airlines, Hawaiian Airlines, Sun Country, Frontier, Volaris, VivaAerobus, Allegiant, Avelo, Breeze, Flair, GOL, Azul, JetSmart, Flybondi, Porter, WestJet, LATAM, Copa, Avianca, Air Canada, Arajet, Wingo, Sky Airline |
| **Aggregator** | Kiwi.com (virtual interlining + LCC fallback) |

</details>

---

## Also Available As

- **JavaScript/TypeScript SDK + CLI**: `npm install letsfg` — [npm](https://www.npmjs.com/package/letsfg)
- **Python SDK + CLI**: `pip install letsfg` — [PyPI](https://pypi.org/project/letsfg/)
- **Try without installing**: [Message us on Messenger](https://m.me/61579557368989)
- **GitHub**: [LetsFG/LetsFG](https://github.com/LetsFG/LetsFG)

> ⭐ **[Star the repo](https://github.com/LetsFG/LetsFG)** — we appreciate the support.

## License

MIT

## 🏨 Hotels — new, and live

Your agent can now book hotels, not just flights. Same API key, same card on file.

```python
from letsfg import LetsFG
lfg = LetsFG()

city = lfg.hotel_destinations("Warsaw")[0]
stays = lfg.search_hotels(
    city_id=city["Id"], city_name=city["Name"],
    check_in="2026-11-10", check_out="2026-11-12", adults=2,
)

hotel = stays["hotels"][0]
offer = hotel["offers"][0]
print(hotel["name"], offer["price"], stays["currency"])
# Hotel Gromada Warszawa Centrum 669.86 PLN

booking = lfg.book_hotel_and_wait(
    session_id=stays["session_id"],
    hotel_code=hotel["hotel_code"],
    combination_id_v2=offer["combination_id_v2"],
    expected_price=offer["price"],
    expected_balance=offer["balance_to_supplier"],
    city_id=city["Id"], city_name=city["Name"],
    check_in="2026-11-10", check_out="2026-11-12",
    guests=[{"title": "Mr", "first_name": "Jan", "last_name": "Kowalski"}],
    email="guest@example.com", phone="512345678",
)
print(booking["confirmation"], booking["pay_link"])
```

### How you pay

**5% now, the rest to the hotel later.** At booking we charge 5% of the price
to your card as a reservation fee. The remaining balance is paid **directly to
the supplier** through a `pay_link` we return — we never hold it.

`balance_due_by` is the supplier's own auto-cancellation date, not a date we
invent. Miss it and the room is released.

The 5% is **non-refundable**. Cancelling before `balance_due_by` costs nothing
else; after it, the hotel's own cancellation ladder applies and can reach 100%.
That ladder ships in the booking's `terms`, so you can always see the cost before
you cancel.

### What search costs

Search is metered separately from booking, on **either** auth path (free PFS
Bearer token or Developer API key — both count against the same agent):
**the first 1,000 `search_hotels` calls since your last hotel booking are
free.** Past that, searches are billed in blocks of 1,000 for **$5**
(~$0.005/search) from your prepaid balance — refused with a 402 if the
balance can't cover the next block, never silently allowed. Book a hotel
and the count resets to zero. Resolving a city name (`resolve_hotel_city`)
is not metered, only the search call itself.

### Things worth knowing before you build

- **A card on file is required for every hotel call, including search.** That is
  unusual and it is deliberate: a hotel search opens a real session at the
  supplier, and booking blocks a real rate. We would rather refuse up front than
  let you reach the point of commitment and discover you cannot pay. The same
  card that authorises flight booking authorises hotels — there is no separate
  hotel signup.
- **Only free-cancellation, pay-later rates are sold.** Those are the rates where
  the balance can safely be settled with the supplier after booking, which is
  what makes 5%-now/rest-later work at all. You will see fewer results than a
  metasearch shows you. Every one of them can actually be booked.
- **Booking is asynchronous.** `book_hotel` returns a `booking_job_id`, not a
  booking — the real thing takes minutes. Poll `hotel_booking(job_id)` until
  `status` is `succeeded` or `failed`, or call `book_hotel_and_wait` and let the
  SDK do it. This is not ceremony: it is what makes it impossible to charge a
  card and then lose the confirmation to a timeout.
- **The fee is charged before the room is committed.** A declined card therefore
  costs nothing to unwind — no reservation exists and nothing is charged.
- **Do not retry a booking blindly.** Calling `book_hotel` twice for the same
  rate books the room twice and charges two reservation fees.
- `price` is what the guest pays. There is no wholesale figure in the response to
  quote by mistake.

### JavaScript

```javascript
import { LetsFG } from 'letsfg';
const lfg = new LetsFG({ apiKey: process.env.LETSFG_API_KEY });

const [city] = await lfg.hotelDestinations('Warsaw');
const stays = await lfg.searchHotels({
  cityId: city.Id, cityName: city.Name,
  checkIn: '2026-11-10', checkOut: '2026-11-12', adults: 2,
});

const booking = await lfg.bookHotelAndWait({ /* ...offer + guest details... */ });
console.log(booking.confirmation, booking.pay_link);
```

### MCP

Five new tools, in the order you call them: `resolve_hotel_city` →
`search_hotels` → `book_hotel` → `get_hotel_booking` → `cancel_hotel_booking`.

