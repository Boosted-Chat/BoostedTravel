# LetsFG API Reference

Full endpoint details for the LetsFG flight search and booking API.

**Base URL:** `https://api.letsfg.co`

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

### Link GitHub (Star Verification)

```
POST /api/v1/agents/link-github
```

```json
{
  "github_username": "your-username"
}
```

### Setup Payment

```
POST /api/v1/agents/setup-payment
```

```json
{
  "token": "tok_visa"
}
```

Required before first booking. Card stays on file.

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

### Unlock Offer

```
POST /api/v1/bookings/unlock
```

```json
{
  "offer_id": "off_xxx"
}
```

**Response:**

```json
{
  "offer_id": "off_xxx",
  "confirmed_price": 189.50,
  "confirmed_currency": "EUR",
  "offer_expires_at": "2026-04-15T15:30:00Z"
}
```

**Errors:**
- 403 — GitHub star not verified
- 410 — Offer expired (search again)

### Book Flight

```
POST /api/v1/bookings/book
```

```json
{
  "offer_id": "off_xxx",
  "passengers": [
    {
      "id": "pas_0",
      "given_name": "John",
      "family_name": "Doe",
      "born_on": "1990-01-15",
      "gender": "m",
      "title": "mr",
      "email": "john@example.com",
      "phone_number": "+1234567890"
    }
  ],
  "contact_email": "john@example.com",
  "idempotency_key": "unique-key-123"
}
```

**Response:**

```json
{
  "booking_reference": "ABC123",
  "status": "confirmed",
  "flight_price": 189.50,
  "currency": "EUR"
}
```

**Errors:**
- 402 — Payment declined
- 403 — Offer not unlocked first
- 409 — Fare changed (re-unlock) or already booked (idempotency)
- 410 — 30-minute window expired (search + unlock again)

## Error Codes

| Error Code | Category | HTTP | Description |
|------------|----------|------|-------------|
| `SUPPLIER_TIMEOUT` | transient | 504 | Airline API didn't respond — retry |
| `RATE_LIMITED` | transient | 429 | Too many requests — wait and retry |
| `INVALID_IATA` | validation | 422 | Bad airport/city code — use resolve_location |
| `INVALID_DATE` | validation | 422 | Date in wrong format or in the past |
| `OFFER_EXPIRED` | business | 410 | Offer no longer available — search again |
| `PAYMENT_REQUIRED` | business | 402 | GitHub star not verified |
| `FARE_CHANGED` | business | 409 | Price changed — re-unlock |
| `ALREADY_BOOKED` | business | 409 | Duplicate (idempotency_key matched) |

## Discovery

| Endpoint | URL |
|----------|-----|
| OpenAPI/Swagger | https://api.letsfg.co/docs |
| Agent discovery | https://api.letsfg.co/.well-known/ai-plugin.json |
| Agent manifest | https://api.letsfg.co/.well-known/agent.json |
| LLM instructions | https://api.letsfg.co/llms.txt |
| MCP (Streamable HTTP) | https://api.letsfg.co/mcp |
