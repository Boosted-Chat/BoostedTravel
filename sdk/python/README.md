# LetsFG — Your AI agent just learned to book flights.

**Server-side engine. Real prices. One function call.** Search hundreds of airlines at raw airline prices — **$20–$50 cheaper** than Booking.com, Kayak, and other OTAs.

[![GitHub stars](https://img.shields.io/github/stars/LetsFG/LetsFG?style=social)](https://github.com/LetsFG/LetsFG)
[![PyPI](https://img.shields.io/pypi/v/letsfg)](https://pypi.org/project/letsfg/)

## Two ways to use LetsFG

| | **CLI / SDK** (PFS Bearer token) | **Developer API** |
|---|---|---|
| **Search cost** | Free (card-backed token from [letsfg.co/connect](https://letsfg.co/connect), nothing charged) | Prepaid credits |
| **Booking** | `POST /api/agent-book` — fare held on your card, a LetsFG agent buys the ticket, captured only on a real PNR. Every offer. | `POST /flights/book` — the same hold-then-capture flow, no unlock step, no booking or transaction fee |
| **Speed** | 8–10 s to first results; longer on a split | 2–5 s (discover) · 8–10 s to first results (full) |
| **Setup** | `pip install letsfg`, then connect at [letsfg.co/developers/api/mcp](https://letsfg.co/developers/api/mcp) | [letsfg.co/developers](https://letsfg.co/developers) |

> **Building a product, or need hotels?** Use the [Developer API](https://letsfg.co/developers) — look-to-book search (200 free after every booking, then $0.01), booking through `POST /flights/book`, no booking fee and no transaction fee.

## Install

```bash
pip install letsfg
```

Connect a card once at [letsfg.co/connect](https://letsfg.co/connect) (nothing
charged — see Authentication), then search is free and booking runs on that card:

```bash
export LETSFG_BEARER_TOKEN=eyJ...     # the card-backed token from the connect flow
letsfg search LHR BCN 2026-06-15
```

**Search is free and booking costs the ticket price.** There is no unlock step
and no LetsFG fee on any path. The Developer API works the same way: no booking
fee and no transaction fee, with the margin already inside the offer price. Its
unlock step and 1% (min $3) fee were retired on 2026-09-08.

## Authentication

Connect LetsFG as an MCP server at `https://letsfg.co/developers/api/mcp` and
approve the connection — in Claude, ChatGPT, Cursor, Windsurf, or Claude Code
(`claude mcp add --transport http letsfg https://letsfg.co/developers/api/mcp`).
The consent step opens [letsfg.co/connect](https://letsfg.co/connect), where you
add a card (any card, or Revolut Pay / Google Pay) in a 0.00 Revolut setup.
Nothing is charged, no Revolut account is needed, and the card details go to
Revolut, never to LetsFG. The token you get back is card-backed: it searches
and it books. One card = one account; quotas are per card (10 searches per
10 min, 30 per hour, 100 per day — polling never counts).

The SDK reads the token from `LETSFG_BEARER_TOKEN` or `~/.letsfg/config.json`.

Or skip the MCP client entirely and run **`letsfg auth`**, which does the same
connect flow from the terminal: it registers itself as an OAuth client (PKCE +
loopback redirect), opens the card screen for a person to approve, and writes
the token to `~/.letsfg/config.json`. Add `--no-browser` to print the URL
instead of opening one. The access token lasts about an hour and refreshes
itself from a stored 30-day refresh token — call `ensure_bearer_token()` in a
long-lived process and it renews silently.

```bash
letsfg auth                # opens a browser
letsfg auth --no-browser   # prints the URL to open yourself
```

> **Retired 2026-09-02:** the Stripe enrolment lanes (`setup_url`,
> `setup_session_id`, `payment_method_id`, `card_token`) and every token they
> issued (401 `TOKEN_REVOKED`). `verify_payment_method()` now raises and says
> so. There is no endpoint that mints a token from card details — a person
> must approve once in a browser, so never ask a user for a card number.

```python
from letsfg import LetsFG

# Reads LETSFG_BEARER_TOKEN (or ~/.letsfg/config.json)
bt = LetsFG()
```

Prefer the paid Developer API instead? Register there and pass an `api_key` —
`search()`/`book()` dispatch automatically based on which credential is set:

```python
# Register (one-time, no auth needed) — Developer API only, most agents don't need this
creds = LetsFG.register("my-agent", "agent@example.com")
bt = LetsFG(api_key=creds["api_key"])  # or set LETSFG_API_KEY env var

# Connect a Revolut payment method (nothing is charged to connect)
# POST /agents/connect-payment returns a one-time link; open it in a browser.
# setup_payment() below calls the RETIRED Stripe route and now answers 410 Gone.
```

> Payments moved to Revolut on 2026-09-08. `setup_payment()` and the hosted-checkout lane were
> retired with Stripe and answer `410 Gone` naming the replacement:
> `POST /agents/connect-payment`, then open the returned `connect_url` once in a browser.

### Verify Your Credentials

```python
# Check that auth + payment are working
profile = bt.me()
print(f"Agent: {profile['agent_name']}")
print(f"Payment: {profile.get('payment_status', 'not set up')}")
print(f"Searches: {profile.get('search_count', 0)}")
```

### Auth Failure Recovery

```python
from letsfg.connectors.auth import BearerTokenError
from letsfg import LetsFG

try:
    bt = LetsFG()
    flights = bt.search("LHR", "JFK", "2026-04-15")
except BearerTokenError:
    print("Token missing or revoked — reconnect at https://letsfg.co/connect through the MCP")
```

## Quick Start (Python)

```python
from letsfg import LetsFG

bt = LetsFG()  # reads LETSFG_BEARER_TOKEN

# Search flights — FREE
flights = bt.search("GDN", "BER", "2026-03-03")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")

# Book — ticket price only, no LetsFG fee, no unlock step. Starts the booking:
# the fare is HELD on your card and a LetsFG agent buys the ticket (4-11 min).
result = bt.book(
    offer_id=flights.cheapest.id,
    passengers=[{
        "given_name": "John",
        "family_name": "Doe",
        "born_on": "1990-01-15",
        "gender": "m",
        "nationality": "GB",
        "phone_number": "+447700900123",
        "phone_country": "GB",
        "address_line1": "1 Analytical Way",
        "address_city": "London",
        "address_postal": "N1 9GU",
        "address_country": "GB",
    }],
    contact_email="john@example.com",
    search_id=flights.search_id,
)
booking_ref = result["booking_ref"]

# Poll until it lands (every 20-30 s): completed | failed | needs_attention
import time, requests
from letsfg.connectors.auth import get_bearer_token
while True:
    status = requests.post(
        "https://letsfg.co/api/agent-book/status",
        json={"booking_ref": booking_ref},
        headers={"Authorization": f"Bearer {get_bearer_token()}"},
    ).json()
    if status["state"] != "booking_in_progress":
        break
    time.sleep(25)
print(status)  # {"state": "completed", "pnr": "ABC123", "charged_amount": 93, "currency": "EUR"}
```

### How booking works

`book()` posts to `POST /api/agent-book` and does exactly what the website
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

## Multi-Passenger Search

Searching with multiple passengers works on both paths. Booking more than one
passenger in a single call is **Developer API only** — the free PFS `book()`
books one passenger per call.

```python
# 2 adults + 1 child, round-trip, premium economy
flights = bt.search(
    "LHR", "JFK", "2026-06-01",
    return_date="2026-06-15",
    adults=2,
    children=1,
    cabin_class="W",  # W=premium, M=economy, C=business, F=first
    sort="price",
)

# passenger_ids will be ["pas_0", "pas_1", "pas_2"]
print(f"Passenger IDs: {flights.passenger_ids}")

# Developer API: unlock, then book with details for EACH passenger
unlocked = bt.unlock(flights.cheapest.id)
booking = bt.book(
    offer_id=unlocked.offer_id,
    passengers=[
        {"id": "pas_0", "given_name": "John", "family_name": "Doe", "born_on": "1990-01-15", "gender": "m", "title": "mr"},
        {"id": "pas_1", "given_name": "Jane", "family_name": "Doe", "born_on": "1992-03-20", "gender": "f", "title": "ms"},
        {"id": "pas_2", "given_name": "Tom", "family_name": "Doe", "born_on": "2018-05-10", "gender": "m", "title": "mr"},
    ],
    contact_email="john@example.com",
)
```

## Resolve Locations

Always resolve city names to IATA codes before searching:

```python
locations = bt.resolve_location("New York")
# [{"iata_code": "JFK", "name": "John F. Kennedy", "type": "airport", "city": "New York"}, ...]

# Use in search
flights = bt.search(locations[0]["iata_code"], "LAX", "2026-04-15")
```

## Working with Search Results

```python
flights = bt.search("LON", "BCN", "2026-04-01", return_date="2026-04-08", limit=50)

# Iterate all offers
for offer in flights.offers:
    print(f"{offer.owner_airline}: {offer.currency} {offer.price}")
    print(f"  Route: {offer.outbound.route_str}")
    print(f"  Duration: {offer.outbound.total_duration_seconds // 3600}h")
    print(f"  Stops: {offer.outbound.stopovers}")
    print(f"  Refundable: {offer.conditions.get('refund_before_departure', 'unknown')}")
    print(f"  Changeable: {offer.conditions.get('change_before_departure', 'unknown')}")

# Filter: direct flights only
direct = [o for o in flights.offers if o.outbound.stopovers == 0]

# Filter: specific airline
ba = [o for o in flights.offers if "British Airways" in o.airlines]

# Filter: refundable only
refundable = [o for o in flights.offers if o.conditions.get("refund_before_departure") == "allowed"]

# Sort by duration
by_duration = sorted(flights.offers, key=lambda o: o.outbound.total_duration_seconds)

# Cheapest offer
print(f"Best: {flights.cheapest.price} {flights.cheapest.currency}")
```

## Starlink Wi-Fi

Offers may carry `starlink`: `confirmed_all` / `confirmed_some` mean the carrier
has **fully** fitted that aircraft type; `likely_all` / `likely_some` mean the
rollout on that type is underway but incomplete. Segments carry `confirmed` or
`likely`.

Only `confirmed_*` is safe to state as fact — `likely_*` is a signal, not a
promise. Anything ending `_some` has at least one leg without it. An **absent**
field means no information, **not** an absence of Wi-Fi.

Full semantics: [docs/api-search.md](https://github.com/LetsFG/LetsFG/blob/main/docs/api-search.md#starlink-wi-fi).

## Error Handling

```python
from letsfg import LetsFG, LetsFGError
from letsfg.connectors.auth import BearerTokenError

bt = LetsFG()  # reads LETSFG_BEARER_TOKEN (or ~/.letsfg/config.json)

# Handle invalid locations
try:
    flights = bt.search("INVALID", "JFK", "2026-04-15")
except LetsFGError as e:
    if e.status_code == 422:
        # Resolve the location first
        locations = bt.resolve_location("London")
        flights = bt.search(locations[0]["iata_code"], "JFK", "2026-04-15")

# Handle booking (PFS path — no unlock step)
try:
    result = bt.book(
        offer_id=flights.cheapest.id, passengers=[...],
        contact_email="...", search_id=flights.search_id,
    )
    if result.get("error") == "missing_details":
        print(f"Ask the traveller for: {result['missing_fields']}")  # nothing charged
    elif result.get("error") == "payment_method_required":
        print(f"No card connected — {result['add_card_url']}")
    else:
        print(f"Started: {result['booking_ref']} — poll /api/agent-book/status")
except BearerTokenError:
    print("Token missing or revoked — reconnect at https://letsfg.co/connect")
```

Developer API path adds `unlock()` before `book()`, and its own error modes:

```python
from letsfg import LetsFG, LetsFGError, PaymentRequiredError, OfferExpiredError

bt = LetsFG(api_key="letsfg_...")
try:
    unlocked = bt.unlock(offer_id)
    booking = bt.book(offer_id=unlocked.offer_id, passengers=[...], contact_email="...")
except PaymentRequiredError:
    print("Run bt.setup_payment() first")
except OfferExpiredError:
    print("Offer expired, or the 30-minute post-unlock window closed — search and unlock again")
except LetsFGError as e:
    print(f"API error ({e.status_code}): {e.message}")
```

| Exception | HTTP Code | Cause |
|-----------|-----------|-------|
| `AuthenticationError` | 401 | Missing or invalid API key (Developer API) |
| `BearerTokenError` | 401 | Missing, expired or revoked Bearer token — reconnect through the MCP (PFS) |
| `PaymentRequiredError` | 402 | No payment method (call `setup_payment()`, Developer API) |
| `OfferExpiredError` | 410 | Offer no longer available (Developer API) |
| `LetsFGError` | any | Base class for all API errors |

### Timeout and Retry Pattern

Full cloud search takes 8–10 s to first results (async polling). Use retry with backoff for transient errors:

```python
import time
from letsfg import LetsFG, LetsFGError

bt = LetsFG()

def search_with_retry(origin, dest, date, max_retries=3):
    """Retry with exponential backoff on rate limit or timeout."""
    for attempt in range(max_retries):
        try:
            return bt.search(origin, dest, date)
        except LetsFGError as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                wait = 2 ** attempt  # 1s, 2s, 4s
                print(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif "timeout" in str(e).lower() or "504" in str(e):
                print(f"Timeout, retrying ({attempt + 1}/{max_retries})...")
                time.sleep(1)
            else:
                raise
    raise LetsFGError("Max retries exceeded")
```

### Rate Limits

| Endpoint | Rate Limit | Typical Latency |
|----------|-----------|------------------|
| Search | No hard limit (billing is the natural governor) | 8–10 s to first results |
| Resolve location | 120 req/min | < 1 s |
| Unlock | 20 req/min | 2–5 s |
| Book | 10 req/min | 3–10 s |

## Search Wide, Book Once

Searching is free (10 per 10 min, 30 per hour, 100 per day per card). On
PFS, booking goes through `POST /api/agent-book` — the fare is held on your
card and captured only on a real PNR, ticket price only, no LetsFG fee.
Compare before booking:

```python
# Search multiple dates (free) — compare before booking
dates = ["2026-04-01", "2026-04-02", "2026-04-03"]
best = None
for date in dates:
    result = bt.search("LON", "BCN", date)
    if result.offers and (best is None or result.cheapest.price < best[1].price):
        best = (date, result)

# Book only the winner
if best:
    date, result = best
    booking = bt.book(
        offer_id=result.cheapest.id, passengers=[...],
        contact_email="...", search_id=result.search_id,
    )
```

On the Developer API the same idea applies to the look-to-book allowance: 200
searches are free after every booking, so search every candidate, then book the
winner — the booking resets the allowance.

## Quick Start (CLI)

```bash
# Token: connect once through the MCP (see Authentication), then
export LETSFG_BEARER_TOKEN=eyJ...

# Search (1 adult, one-way, economy — defaults)
letsfg search GDN BER 2026-03-03 --sort price

# Multi-passenger round trip
letsfg search LON BCN 2026-04-01 --return 2026-04-08 --adults 2 --children 1 --cabin M

# Business class, direct flights only
letsfg search JFK LHR 2026-05-01 --adults 3 --cabin C --max-stops 0

# Machine-readable output (for agents) — includes search_id, needed for book
letsfg search LON BCN 2026-04-01 --json

# Book — ticket price only, no LetsFG fee, no unlock step. Holds the fare on
# your card and starts the LetsFG booking agent; prints the booking_ref to poll.
letsfg book off_xxx --search-id srch_xxx \
  --passenger '{"given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","nationality":"GB","phone_number":"+447700900123","phone_country":"GB","address_line1":"1 Analytical Way","address_city":"London","address_postal":"N1 9GU","address_country":"GB"}' \
  --email john@example.com

# Resolve location
letsfg locations "Berlin"
```

### Search Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--return` | `-r` | _(one-way)_ | Return date YYYY-MM-DD |
| `--adults` | `-a` | `1` | Adults (1–9) |
| `--children` | | `0` | Children 2–11 years |
| `--cabin` | `-c` | _(any)_ | `M` economy, `W` premium, `C` business, `F` first |
| `--max-stops` | `-s` | `2` | Max stopovers (0–4) |
| `--currency` | | `EUR` | Currency code |
| `--limit` | `-l` | `20` | Max results (1–100) |
| `--sort` | | `price` | `price` or `duration` |
| `--json` | `-j` | | Raw JSON output |

## All CLI Commands

| Command | Description | Cost |
|---------|-------------|------|
| `auth` | Connect a card at letsfg.co/connect and store the token — self-registers, PKCE + loopback redirect, opens a browser. `--no-browser` prints the URL | FREE |
| `search` | Search flights between any two airports, prints `search_id` | FREE |
| `locations` | Resolve city name to IATA codes | FREE |
| `book` | Start a booking for an offer from your search (`--search-id` required). Fare held on your card, captured on a real PNR; poll `/api/agent-book/status` | Ticket price only, no LetsFG fee |
| `me` | Show agent profile and usage stats | FREE |
| `unlock` | **[Developer API only]** Unlock offer (confirms price, reveals booking URL). Requires `--api-key`. Legacy | — |
| `register` | **[Developer API only]** Register new Developer API key | FREE |
| `setup-payment` | **[Developer API only]** Attach payment card (required for unlock) | FREE |

Every command supports `--json` for machine-readable output.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `LETSFG_BEARER_TOKEN` | PFS Bearer token (card-backed, from the connect flow). Takes priority over `~/.letsfg/config.json`. |
| `LETSFG_API_KEY` | Developer API key (look-to-book search + booking) |
| `LETSFG_BASE_URL` | API URL override (default: `https://letsfg.co`) |

## How It Works

1. **Search** — Free. The server-side engine queries hundreds of airlines and returns real-time offers.
2. **Book** — Call `POST /api/agent-book` with your Bearer token. The fare plus LetsFG's markup is held on your connected card, a LetsFG booking agent buys the ticket from the seller, and the hold is captured only once a real airline PNR exists (4–11 minutes; poll `POST /api/agent-book/status`). A failed booking releases the hold. Ticket price only, no LetsFG fee, no unlock step.

The Developer API is a separate product with the same booking model. Search there is
**look-to-book**: 200 searches free after every booking you make, then blocks of 500 for $5.00
($0.01 each). Booking is `POST /flights/book` — the fare is held on a connected Revolut method and
captured only against a real PNR, with **no booking fee and no transaction fee**; the margin is
inside the price the search returned.

> Retired 2026-09-08: the `unlock` step (and its 1% / min $3 fee) no longer exists, and neither do
> the Stripe onboarding routes. Both answer `410 Gone` naming their replacement. See
> <https://letsfg.co/developers/api/docs>.

---

## Also Available As

- **MCP Server**: `npx letsfg-mcp` — [npm](https://www.npmjs.com/package/letsfg-mcp)
- **JS/TS SDK**: `npm install letsfg` — [npm](https://www.npmjs.com/package/letsfg)
- **Try without installing**: [letsfg.co](https://letsfg.co) — search instantly in your browser
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

