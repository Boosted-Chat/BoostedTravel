# LetsFG — Your AI agent just learned to book flights. (Node.js)

**Server-side search engine. Real prices. One function call.** Search hundreds of airlines at raw airline prices — **$20–$50 cheaper** than Booking.com, Kayak, and other OTAs. Zero dependencies. Built for AI agents.

[![GitHub stars](https://img.shields.io/github/stars/LetsFG/LetsFG?style=social)](https://github.com/LetsFG/LetsFG)
[![npm](https://img.shields.io/npm/v/letsfg)](https://www.npmjs.com/package/letsfg)

## Two ways to use LetsFG

| | **CLI / SDK** (this package) | **Developer API** |
|---|---|---|
| **Search cost** | Free (card-backed token from [letsfg.co/connect](https://letsfg.co/connect), nothing charged) | Prepaid credits |
| **Booking** | `POST /api/agent-book` — fare held on your card, a LetsFG agent buys the ticket, captured only on a real PNR. Every offer. | Direct airline URL (unlock required first) |
| **Speed** | 8–10 s to first results; longer on a split | 2–5 s (discover) · 8–10 s to first results (full) |
| **Setup** | `npm install letsfg`, then connect at [letsfg.co/developers/api/mcp](https://letsfg.co/developers/api/mcp) | [letsfg.co/developers](https://letsfg.co/developers) |

> **Building a product, or need hotels?** Use the [Developer API](https://letsfg.co/developers) — look-to-book search (200 free after every booking, then $0.01), booking through `POST /flights/book`, no booking fee and no transaction fee.

## Install

```bash
npm install letsfg
```

## Getting a token

Connect LetsFG as an MCP server at `https://letsfg.co/developers/api/mcp` and
approve the connection — in Claude, ChatGPT, Cursor, Windsurf, or Claude Code
(`claude mcp add --transport http letsfg https://letsfg.co/developers/api/mcp`).
The consent step opens [letsfg.co/connect](https://letsfg.co/connect), where you
add a card (any card, or Revolut Pay / Google Pay) in a 0.00 Revolut setup.
Nothing is charged, no Revolut account is needed, and the card details go to
Revolut, never to LetsFG. The token you get back is card-backed: it searches
and it books. One card = one account; quotas are per card (10 searches per
10 min, 30 per hour, 100 per day — polling never counts).

Pass it as `bearerToken`, or set `LETSFG_BEARER_TOKEN` for the CLI.

> `letsfg auth` still runs the Stripe card setup that was retired on
> 2026-09-02 and cannot get a token today; every token issued that way was
> revoked (401 `TOKEN_REVOKED`). A connect-flow login for the CLI and SDKs is
> coming — until then, connect through the MCP.

## Quick Start (SDK)

```typescript
import { LetsFG, cheapestOffer, offerSummary } from 'letsfg';

// PFS — free. The card-backed token from the connect flow (see above).
const bt = new LetsFG({ bearerToken: 'eyJ...' });

// Search — FREE
const flights = await bt.search('GDN', 'BER', '2026-03-03');
const best = cheapestOffer(flights);
console.log(offerSummary(best));

// Book — no booking fee, no transaction fee — our margin is already in the price you saw; no unlock step. Starts the booking:
// the fare is HELD on your card and a LetsFG agent buys the ticket (4-11 min).
const result = await bt.book(
  best.id,
  [{
    given_name: 'John', family_name: 'Doe', born_on: '1990-01-15', gender: 'm',
    nationality: 'GB', phone_number: '+447700900123', phone_country: 'GB',
    address_line1: '1 Analytical Way', address_city: 'London',
    address_postal: 'N1 9GU', address_country: 'GB',
  }],
  'john@example.com',
  '',
  '',
  flights.search_id,
);
const bookingRef = result.booking_ref as string;

// Poll until it lands (every 20-30 s): completed | failed | needs_attention
let status: Record<string, unknown>;
do {
  await new Promise(r => setTimeout(r, 25_000));
  status = await (await fetch('https://letsfg.co/api/agent-book/status', {
    method: 'POST',
    headers: { Authorization: 'Bearer eyJ...', 'Content-Type': 'application/json' },
    body: JSON.stringify({ booking_ref: bookingRef }),
  })).json();
} while (status.state === 'booking_in_progress');
console.log(status); // { state: 'completed', pnr: 'ABC123', charged_amount: 93, currency: 'EUR' }
```

### How booking works

`bt.book()` posts to `POST /api/agent-book` and does exactly what the website
checkout does: the fare plus LetsFG's markup is **held** on the connected card
(not taken), a LetsFG booking agent buys the ticket from the seller, and the
hold is captured only once a real airline PNR exists. If the booking fails the
hold is released and nothing is charged. Every offer can be booked this way —
no unlock step, no booking-link fallback, no separate LetsFG fee.

The call returns within seconds with a `booking_ref`; the booking itself takes
4–11 minutes. Poll `POST /api/agent-book/status` with `{"booking_ref": ...}`
every 20–30 s (the SDK has no helper for this yet):

| `state` | Meaning |
|---|---|
| `booking_in_progress` | the agent is at the seller's checkout — keep waiting |
| `completed` | booked — `pnr`, `charged_amount`, `currency` are in the answer |
| `failed` | not booked — the hold was released, nothing charged; see `failure_reason` |
| `needs_attention` | a human at LetsFG is checking it — do **not** book again |

One traveller per call, with the details an airline checkout asks for: name,
date of birth, gender, nationality, email, phone with its country, residence
address (passport optional). A missing detail returns `missing_details` with
`missing_fields` and charges nothing. Never start a second booking for the
same trip while one is in progress — that would place a second hold.

Prefer the paid Developer API instead? Pass `apiKey` instead of `bearerToken` —
`search()`/`book()` dispatch automatically. That path needs a `searchId` (an offer
is bookable only inside the search that produced it) and has no unlock step:
`unlock()` was retired 2026-09-08, the route answers `410 Gone`, and there is no fee.

## Quick Start (CLI)

```bash
export LETSFG_BEARER_TOKEN=<your-bearer-token>  # card-backed, from the connect flow

letsfg search GDN BER 2026-03-03 --sort price
letsfg search LON BCN 2026-04-01 --json  # Machine-readable
letsfg book off_xxx --search-id srch_xxx -p '{"given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","nationality":"GB","phone_number":"+447700900123","phone_country":"GB","address_line1":"1 Analytical Way","address_city":"London","address_postal":"N1 9GU","address_country":"GB"}' -e john@example.com
# prints the booking_ref — poll POST /api/agent-book/status until completed
```

## API

### `new LetsFG({ bearerToken?, apiKey?, baseUrl?, timeout? })`

### `bt.search(origin, destination, dateFrom, options?)`
### `bt.resolveLocation(query)`
### `bt.book(offerId, passengers, contactEmail, contactPhone?, idempotencyKey?, searchId?)`
Dispatches on which credential is set: `bearerToken` → PFS booking via
`POST /api/agent-book` (pass `searchId`, one passenger with full details;
returns `booking_ref` — poll `POST /api/agent-book/status`). `apiKey` → paid
Developer API `book` (requires `searchId`, no unlock step, supports multiple
passengers and `idempotencyKey`; returns a `booking_id` to poll).
### `bt.getBooking(bookingId)` / `bt.answerBooking(...)` / `bt.bookAndWait(...)` — Developer API only
### `bt.connectPayment()` — Developer API only. Returns `connect_url`; nothing is charged
### `bt.setupPayment()` / `bt.unlock()` — **retired 2026-09-08, both throw locally**
### `bt.me()`
### `LetsFG.register(agentName, email, baseUrl?, ownerName?, description?)` — Developer API only, most agents don't need this

### Helpers
- `offerSummary(offer)` — One-line string summary
- `cheapestOffer(result)` — Get cheapest offer from search

## Starlink Wi-Fi

Offers may carry `starlink`: `confirmed_all` / `confirmed_some` mean the carrier
has **fully** fitted that aircraft type; `likely_all` / `likely_some` mean the
rollout on that type is underway but incomplete. Segments carry `confirmed` or
`likely`.

Only `confirmed_*` is safe to state as fact — `likely_*` is a signal, not a
promise. Anything ending `_some` has at least one leg without it. An **absent**
field means no information, **not** an absence of Wi-Fi.

Full semantics: [docs/api-search.md](https://github.com/LetsFG/LetsFG/blob/main/docs/api-search.md#starlink-wi-fi).

## Zero Dependencies

Uses native `fetch` (Node 18+). No `axios`, no `node-fetch`, nothing. Safe for sandboxed environments.

## Also Available As

- **MCP Server**: `npx letsfg-mcp` — [npm](https://www.npmjs.com/package/letsfg-mcp)
- **Python SDK + CLI**: `pip install letsfg` — [PyPI](https://pypi.org/project/letsfg/)
- **Try without installing**: [letsfg.co](https://letsfg.co) — search instantly in your browser
- **GitHub**: [LetsFG/LetsFG](https://github.com/LetsFG/LetsFG)

> ⭐ **[Star the repo](https://github.com/LetsFG/LetsFG)** — we appreciate the support.

## License

MIT

## 🏨 Hotels — new, and live

Your agent can book hotels as well as flights. Same card-backed token or API key, same card on file.

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
and the count resets to zero. Resolving a city name (`hotel_destinations`)
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

