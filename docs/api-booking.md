# Booking flights

> ### ⚠️ Agents: do not create a Developer API account
>
> This page describes the **Developer API key** path. If you are an AI agent
> wanting to search and book, connect LetsFG as an MCP server at
> <https://letsfg.co/developers/api/mcp> instead — booking works the same way
> there, through `book_flight`, with no billing account.

Every offer a search returns is bookable. There is no unlock step, no separate
LetsFG fee, and no seller redirect — LetsFG buys the ticket for you.

## How the money moves

1. `POST /flights/book` **holds** the fare on your connected Revolut method. It
   is not taken.
2. A LetsFG booking agent buys the ticket from the seller.
3. The hold is **captured only once a real airline PNR exists**.
4. If the booking fails, the hold is released and nothing is charged.

That ordering is the whole safety property: a price that moves, a seller that
fails, a form that rejects a passenger — none of them can turn into a charge for
a ticket you did not get.

**The price you saw is the price you pay.** LetsFG's margin is already inside
the offer price returned by search. There is no booking fee and no transaction
fee.

A completed booking also **resets your free search allowance** to a full 200.

## 1. Search, and keep the ids

```bash
curl -X POST https://letsfg.co/developers/api/v1/flights/search \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"origin": "KRK", "destination": "BCN", "departure_date": "2026-11-14", "adults": 1}'
```

You need two things from the response: the top-level `search_id`, and the `id`
of the offer you want. An offer can only be booked inside the search that
produced it, and a search stays bookable for **6 hours**.

Each offer also carries:

- `booking_url` — a letsfg.co page for that exact offer, safe to show a person.
  It is never a seller deep link; the seller link stays server-side.
- `book_url` — the API route below.

## 2. Book

```bash
curl -X POST https://letsfg.co/developers/api/v1/flights/book \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "search_id": "srch_abc123",
    "offer_id": "off_def456",
    "idempotency_key": "your-own-unique-key",
    "contact_email": "traveller@example.com",
    "passengers": [{
      "given_name": "Adam",
      "family_name": "Kowalski",
      "born_on": "1990-04-11",
      "gender": "m",
      "email": "traveller@example.com",
      "phone_number": "+48501234567",
      "phone_country": "PL",
      "nationality": "PL",
      "address_line1": "Ulica 1",
      "address_city": "Krakow",
      "address_postal": "30-001",
      "address_country": "PL",
      "passenger_type": "adult"
    }]
  }'
```

Answers `202` in seconds — the booking itself takes 4–11 minutes:

```json
{
  "ok": true,
  "booking_id": "dev_abc123",
  "state": "authorised",
  "dispatched": true,
  "held": {"amount": 209, "currency": "EUR", "card": {"brand": "visa", "last4": "4242", "type": "card"}},
  "charged": 0,
  "offer": {"search_id": "srch_abc123", "offer_id": "off_def456", "fare": 200.0, "currency": "EUR"},
  "poll_url": "https://letsfg.co/developers/api/v1/flights/bookings/dev_abc123"
}
```

`charged` is `0` and stays `0` until a PNR exists.

### Passengers

1 to 9 per booking. The shape is identical to the MCP `book_flight` tool, field
for field, so an integration built against either lane feeds the other.

Only `given_name` is required by the schema. The server decides what a given
airline's checkout actually needs and answers `400` **before anything is
charged**, naming exactly what is missing:

```json
{
  "error": "missing_fields",
  "message": "The lead passenger is missing details an airline checkout requires: born_on, nationality. ...",
  "missing_fields": ["born_on", "nationality"]
}
```

So the safe integration is: send what you have, read `missing_fields`, ask the
traveller for those, call again. Collect passport fields if you can — they are
asked for, never required.

`passenger_type` is `adult`, `child` or `infant`. The lead passenger's email
receives the airline confirmation.

### Idempotency

Pass your own `idempotency_key`. Retrying with the same key returns the existing
booking instead of opening a second hold on the card. Use it — a network timeout
on this call is exactly the case it exists for.

## 3. Poll

```bash
curl https://letsfg.co/developers/api/v1/flights/bookings/dev_abc123 \
  -H "X-API-Key: letsfg_your_api_key"
```

```json
{
  "booking_id": "dev_abc123",
  "state": "booking_in_progress",
  "terminal": false,
  "pnr": "",
  "held_amount": 209,
  "charged_amount": null,
  "question": null,
  "message": "In progress. ..."
}
```

Poll every few seconds until `terminal` is true. **The poll is also how LetsFG
knows you are still there** — a booking paused on a question stays alive while
you are polling.

| `state` | Meaning |
|---------|---------|
| `authorised` | The hold is placed |
| `card_issued` | A single-use card was issued for this booking |
| `booking_in_progress` | The agent is on the seller's site |
| `awaiting_settlement` | Bought; settling |
| `completed` | **Done.** `pnr` and `charged_amount` are set |
| `failed` | Hold released, nothing charged. See `failure_reason` / `decline_reason` |
| `needs_attention` | Money moved but the outcome is unclear — a human at LetsFG is on it. **Do not book again.** |

`terminal` is true for the last three.

## 4. Answer a question, if one appears

While `booking_in_progress`, `question` may become non-null. There are three
kinds, and each has a window — miss it and the run continues without you.

```json
"question": {
  "kind": "price_change",
  "round": 2,
  "answer": "POST .../answer with {round, confirm:true} to accept or {round, skip:true} to decline"
}
```

| `kind` | What it is | Answer |
|--------|------------|--------|
| `seat` | The airline's seat map (only if you sent `want_seat: true`) | `{"round": N, "seats": [...]}` or `{"round": N, "skip": true}` |
| `extra` | A paid extra, e.g. baggage | `{"round": N, "confirm": true}` or `{"round": N, "skip": true}` |
| `price_change` | The fare moved while the agent was checking out | `{"round": N, "confirm": true}` to accept the new price, or `{"round": N, "skip": true}` |

```bash
curl -X POST https://letsfg.co/developers/api/v1/flights/bookings/dev_abc123/answer \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"round": 2, "confirm": true}'
```

**Echo the `round`.** A stale round is refused with `409` rather than guessed
at — that guard exists so an answer to an old question can never be applied to a
new one. Declining an extra still completes the booking, without it.

## Refusals

Nothing is charged on any refusal. Every one is JSON with an `error` you can
branch on.

| Status | `error` | What to do |
|--------|---------|------------|
| `400` | `missing_fields` | Ask the traveller for the listed fields, call again |
| `400` | `party_too_large` | Maximum 9 passengers |
| `400` | `currency_unsupported` | Search again in EUR, USD or GBP and book that offer |
| `400` | `offer_not_bookable` | Pick another offer |
| `402` | `payment_method_required` | `POST /agents/connect-payment`, open the link |
| `402` | `payment_declined` | Read `decline_reason`; the traveller's bank refused the hold |
| `404` | `search_not_found` / `offer_not_found` | The search aged out (6h) — search again |
| `409` | `package_blocked` | This traveller booked in the last 24h; package-travel rules |
| `503` | `temporarily_unavailable` | Retry shortly with the same `idempotency_key` |

## Retired

`POST /bookings/unlock` and `POST /bookings/book` were retired on 2026-09-08 and
answer `410 Gone`. There is no unlock step any more: unlock existed to confirm a
live price before charging, and the hold-then-capture flow above makes that
unnecessary — a moved price becomes a `price_change` question, not a surprise
charge.
