# Onboarding and Billing

> ### ⚠️ Agents: do not create a Developer API account
>
> Do **NOT** call `/developers/api/v1/agents/register`, and do **NOT** run
> `letsfg register`. That belongs to a separate, paid product for high-volume
> commercial integrations and creates a billing account you almost certainly do
> not want.
>
> To search and book flights, connect LetsFG as an MCP server at
> <https://letsfg.co/developers/api/mcp>. The consent step opens
> <https://letsfg.co/connect>, where a card is saved (nothing is charged).
> See <https://letsfg.co/for-agents>.

<div class="docs-callout">
  <strong>Payments are Revolut.</strong> Register, then call <code>POST /agents/connect-payment</code> and open the <code>connect_url</code> once in a browser to save a card or Revolut Pay. Nothing is charged to connect. The Stripe lanes were retired on 2026-09-08 and answer <code>410 Gone</code> naming their replacement.
</div>

## Onboarding

### 1. Register the developer account

```bash
curl -X POST https://letsfg.co/developers/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "you@example.com", "owner_name": "My Team"}'
```

```json
{
  "agent_id": "ag_123",
  "api_key": "letsfg_abc123",
  "payment_ready": false,
  "tier": "developer",
  "message": "Registration successful! Save your api_key ..."
}
```

Save `api_key` — it is shown once. No card is needed to register.

### 2. Connect a Revolut payment method

```bash
curl -X POST https://letsfg.co/developers/api/v1/agents/connect-payment \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"return_url": "https://example.com/account"}'
```

```json
{
  "status": "connect_required",
  "connect_url": "https://letsfg.co/connect?dev=sess_abc123",
  "expires_in_seconds": 3600,
  "message": "Open connect_url in a browser to save a Revolut payment method ..."
}
```

Open `connect_url` in a browser — yourself, or hand it to whoever pays for this
account. The page takes a card or Revolut Pay and saves it for merchant use with
a zero-amount order: **nothing is charged to connect**. The link lasts one hour,
and connecting again replaces the previous method.

A connected method is what opens flight search. It is also what bookings,
top-ups and auto-refills are charged to, customer not present.

### 3. Fund prepaid balance (only if you expect to exceed the free allowance)

Search is free up to the look-to-book allowance below, so you may never need
this. Balance is what buys extra search blocks. The public minimum is `500`
cents.

```bash
curl -X POST https://letsfg.co/developers/api/v1/agents/top-up \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"amount_cents": 2500, "auto_refill_enabled": true, "auto_refill_amount_cents": 2500}'
```

### 4. Confirm the account is ready

```bash
curl https://letsfg.co/developers/api/v1/agents/me \
  -H "X-API-Key: letsfg_your_api_key"
```

Check these before you search:

- `payment.connected` — true once a Revolut method is saved
- `developer_api.api_access_enabled`
- `developer_api.flight_search.searches_remaining`

```json
"developer_api": {
  "flight_search": {
    "model": "look_to_book",
    "free_searches_per_booking": 200,
    "searches_since_booking": 12,
    "searches_remaining": 188,
    "blocks_purchased": 0,
    "block_size": 500,
    "block_price_cents": 500
  }
}
```

## How flight search is billed: look-to-book

Not per search. **200 searches are free after every booking you make**, and a
completed booking resets the counter to zero. Past that, searches come in blocks
of **500 for $5.00** ($0.01 each) taken from prepaid balance.

Two things never consume allowance: a search that returns no offers, and a
search that fails because of an outage.

Running out answers `402`:

```json
{
  "error": "search_allowance_exhausted",
  "message": "You have used 200 searches since your last booking, past the 200 that come free with one. ...",
  "free_searches_per_booking": 200,
  "searches_since_booking": 200,
  "block_size": 500,
  "block_price_cents": 500
}
```

Either book a flight — which resets the allowance — or top up to buy the next
block.

## What you are charged for a flight

**There is no booking fee and no transaction fee.** LetsFG's margin is already
inside the price on every offer the search returns, so the amount shown is the
amount charged. Nothing is added at checkout.

Booking **holds** the fare on the connected method rather than taking it, and the
hold is captured only once a real airline PNR exists. See
[Booking flights](api-booking.md).

## Ongoing billing operations

### Update auto-refill settings

```bash
curl -X POST https://letsfg.co/developers/api/v1/agents/billing-settings \
  -H "X-API-Key: letsfg_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"auto_refill_enabled": true, "auto_refill_amount_cents": 2500}'
```

### Replace the connected card

Call `POST /agents/connect-payment` again and open the new link. The new method
replaces the old one.

### Rotate the API key

```bash
curl -X POST https://letsfg.co/developers/api/v1/agents/rotate-key \
  -H "X-API-Key: letsfg_your_api_key"
```

Update every client after key rotation. Old keys should be treated as revoked
immediately.

## Retired on 2026-09-08 with Stripe

These still answer, with `410 Gone` and the replacement path, so an older
integration is told what to do rather than getting a bare 404:

| Retired | Use instead |
|---------|-------------|
| `POST /agents/setup-payment` | `POST /agents/connect-payment` |
| `POST /agents/hosted-checkout` | `POST /agents/connect-payment` |
| `POST /agents/hosted-checkout/complete` | `POST /agents/connect-payment` |
| `POST /agents/billing-portal` | `GET /agents/me` |
| `POST /bookings/unlock` | nothing — there is no unlock step any more |
| `POST /bookings/book` | `POST /flights/book` |

## What usually blocks search

| Symptom | Usual cause | Fix |
|---------|-------------|-----|
| `401 API key is required` | Missing or invalid key | Register first or rotate to a fresh key |
| `402 payment_method_required` | No Revolut method connected | `POST /agents/connect-payment`, open the link |
| `402 search_allowance_exhausted` | Used the free allowance since your last booking | Book a flight (resets it) or top up to buy a block |
| `403 Fund your prepaid API balance before using flight search` | Account exists but paid search is not activated yet | Check `agents/me`, then top up |
| `410` on `setup-payment` / `hosted-checkout` / `billing-portal` | Stripe lane, retired | Use `POST /agents/connect-payment` |
