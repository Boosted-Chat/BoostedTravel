# LetsFG API Reference

Full endpoint details for the LetsFG flight search and booking API.

**Base URL:** `https://letsfg.co/developers/api/v1`

## Authentication

All endpoints (except `register`) require the `X-API-Key` header:

```
X-API-Key: trav_your_api_key
```

## Endpoints

### Register Agent

```
POST /api/v1/agents/register
```

No auth required.

```json
{
  "agent_name": "my-agent",
  "email": "agent@example.com"
}
```

**Response:**

```json
{
  "agent_id": "ag_xxx",
  "api_key": "trav_xxxxx..."
}
```

### Connect Payment

```
POST /api/v1/agents/connect-payment
```

```json
{
  "status": "connect_required",
  "connect_url": "https://letsfg.co/connect?dev=sess_abc123",
  "expires_in_seconds": 3600
}
```

Open `connect_url` in a browser once and save a card or Revolut Pay. **Nothing is charged to
connect.** A connected method is what opens search, and it is what bookings and top-ups are
charged to. The link lasts one hour; connecting again replaces the previous method.

> `POST /api/v1/agents/setup-payment` was retired on 2026-09-08 with Stripe and answers `410 Gone`.

### Agent Profile

```
GET /api/v1/agents/me
```

Returns agent details, search count, booking count, payment status.

### Resolve Location

```
GET /api/v1/flights/locations/{query}
```

Example: `GET /api/v1/flights/locations/London`

**Response:**

```json
[
  {"iata_code": "LON", "name": "London", "type": "city"},
  {"iata_code": "LHR", "name": "Heathrow", "type": "airport", "city": "London"},
  {"iata_code": "LGW", "name": "Gatwick", "type": "airport", "city": "London"}
]
```

### Search Flights

```
POST /api/v1/flights/search
```

```json
{
  "origin": "LHR",
  "destination": "JFK",
  "date_from": "2026-04-15",
  "adults": 1,
  "children": 0,
  "infants": 0,
  "cabin_class": "M",
  "max_stopovers": 2,
  "currency": "EUR",
  "sort": "price",
  "limit": 20
}
```

**Optional fields:** `date_to`, `return_from`, `return_to` (for round-trip), `cabin_class` (M/W/C/F).

**Response:**

```json
{
  "search_id": "sea_xxx",
  "passenger_ids": ["pas_0"],
  "total_results": 47,
  "offers": [
    {
      "id": "off_xxx",
      "price": 189.50,
      "currency": "EUR",
      "airlines": ["British Airways"],
      "owner_airline": "British Airways",
      "outbound": {
        "segments": [
          {
            "airline": "British Airways",
            "flight_no": "BA178",
            "origin": "LHR",
            "destination": "JFK",
            "departure": "2026-04-15T09:00:00",
            "arrival": "2026-04-15T12:15:00",
            "duration_seconds": 27900
          }
        ],
        "route_str": "LHR → JFK",
        "total_duration_seconds": 27900,
        "stopovers": 0
      },
      "conditions": {
        "refund_before_departure": "allowed_with_fee",
        "change_before_departure": "allowed_with_fee"
      }
    }
  ]
}
```

### Book Flight

```
POST /api/v1/flights/book
```

```json
{
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
    "passenger_type": "adult"
  }]
}
```

Answers `202` in seconds with a `booking_id`; the booking itself takes 4-11 minutes.

The connected Revolut method is **held, not charged**. A LetsFG booking agent buys the ticket and
the hold is captured **only against a real airline PNR**; a failed booking releases it. The offer
price already includes LetsFG's margin, so there is no booking fee and no transaction fee. A
completed booking also resets the free search allowance to 200.

1 to 9 passengers. Only `given_name` is required by the schema — the server answers `400
missing_fields` naming exactly what an airline checkout still needs, before anything is charged.
Always send an `idempotency_key`: a retry with the same key returns the existing booking instead of
opening a second hold.

### Poll a Booking

```
GET /api/v1/flights/bookings/{booking_id}
```

Poll until `terminal` is true. States: `authorised`, `card_issued`, `booking_in_progress`,
`awaiting_settlement`, then `completed` (with `pnr` and `charged_amount`), `failed` (hold released,
nothing charged) or `needs_attention` (a human at LetsFG is on it — do not book again).

Polling is also how LetsFG knows you are still there, which keeps a paused booking alive.

### Answer a Booking Question

```
POST /api/v1/flights/bookings/{booking_id}/answer
```

While `booking_in_progress`, `question` may carry a seat map (`kind: "seat"`), a paid extra
(`"extra"`) or a fare increase (`"price_change"`). Echo its `round` — a stale round is refused with
`409` rather than guessed at.

```json
{"round": 2, "confirm": true}
```

`{"skip": true}` declines an extra or skips seat selection; the booking still completes.

### Retired

`POST /api/v1/bookings/unlock`, `POST /api/v1/bookings/book` and
`GET /api/v1/bookings/booking/{id}` were retired on 2026-09-08 and answer `410 Gone` naming their
replacement. There is no unlock step on either lane any more.

## Rate Limits

| Endpoint | Rate Limit | Typical Latency |
|----------|-----------|-----------------|
| Search | 60 req/min | 2-15s |
| Resolve location | 120 req/min | <1s |
| Unlock | 20 req/min | 2-5s |
| Book | 10 req/min | 3-10s |

## Error Response Format

```json
{
  "error": {
    "code": "OFFER_EXPIRED",
    "category": "business",
    "message": "This offer is no longer available",
    "is_retryable": false
  }
}
```

### Error Categories

| Category | Action |
|----------|--------|
| `transient` | Retry after 1-5s (exponential backoff) |
| `validation` | Fix the request parameters |
| `business` | Inform user — needs human decision |

### Error Codes

| Code | HTTP | Category | Description |
|------|------|----------|-------------|
| `SUPPLIER_TIMEOUT` | 504 | transient | Airline API timeout |
| `RATE_LIMITED` | 429 | transient | Too many requests |
| `SERVICE_UNAVAILABLE` | 503 | transient | Backend down |
| `INVALID_IATA` | 422 | validation | Bad airport code |
| `INVALID_DATE` | 422 | validation | Bad date format or past date |
| `INVALID_PASSENGERS` | 422 | validation | Bad passenger data |
| `UNSUPPORTED_ROUTE` | 422 | validation | No providers for route |
| `AUTH_INVALID` | 401 | business | Bad API key |
| `PAYMENT_REQUIRED` | 402 | business | No payment method |
| `PAYMENT_DECLINED` | 402 | business | The connected Revolut method declined; `decline_reason` says why. Nothing was charged |
| `OFFER_EXPIRED` | 410 | business | Seats sold — search again |
| `OFFER_NOT_UNLOCKED` | 403 | business | Must unlock before booking |
| `FARE_CHANGED` | 409 | business | Price changed — re-unlock |
| `ALREADY_BOOKED` | 409 | business | Duplicate (idempotency match) |
