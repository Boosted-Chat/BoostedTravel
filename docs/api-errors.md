# Errors and Limits

> ### ⚠️ Agents: do not create a Developer API account
>
> Do **NOT** call `/developers/api/v1/agents/register` or
> `/developers/api/v1/agents/connect-payment`, and do **NOT** run
> `letsfg register` or `letsfg connect-payment`. Those belong to a separate,
> paid, prepaid-balance product for high-volume commercial integrations and
> create a billing account you almost certainly do not want.
>
> To search and book flights, connect LetsFG as an MCP server at
> <https://letsfg.co/developers/api/mcp>. The consent step opens
> <https://letsfg.co/connect>, where a card is saved (nothing is charged).
> See <https://letsfg.co/for-agents>.

<div class="docs-callout">
  <strong>Most paid API failures are state failures.</strong> In practice, the search path breaks more often because the account is not ready than because the request body is malformed. Treat <code>GET /agents/me</code> as the source of truth before you send paid search traffic.
</div>

## Status codes you should handle explicitly

| Status | Typical meaning | What to do |
|--------|-----------------|------------|
| `400` | Malformed request body or unsupported payment payload | Validate the request body and retry only after fixing it |
| `401` | Missing or invalid `X-API-Key` | Register again or rotate to a fresh key |
| `402` | `payment_method_required` — no Revolut method connected | `POST /agents/connect-payment`, open the `connect_url` |
| `402` | `search_allowance_exhausted` — the look-to-book allowance is used up | Book a flight (resets it to 200) or top up to buy a block of 500 |
| `402` | `payment_declined` on a booking — the bank refused the hold | Read `decline_reason`; nothing was charged |
| `410` | A Stripe-era route (`setup-payment`, `hosted-checkout`, `billing-portal`, `bookings/unlock`, `bookings/book`) | The body names the replacement path |
| `403` | Search access still disabled for the key | Check `agents/me`, then top up or finish onboarding |
| `409` | Billing setup conflict, often around top-up state | Re-read account state before retrying |
| `429` | Too many requests | Back off and retry later |

## Request-body limits exposed by the public contract

| Field | Limit |
|-------|-------|
| `adults` | `1` to `9` |
| `children` | `0` to `9` |
| `infants` | `0` to `9` |
| `max_stopovers` | `0` to `4` |
| `limit` | `1` to `200` |
| `amount_cents` on top-up | minimum `500` |
| `auto_refill_amount_cents` | minimum `500` when provided |
| `cabin_class` | `M`, `W`, `C`, or `F` |
| `departure_time_from` / `departure_time_to` | `HH:MM` in 24-hour format |

## Use `agents/me` as the readiness check

```bash
curl https://letsfg.co/developers/api/v1/agents/me \
  -H "X-API-Key: letsfg_your_api_key"
```

These fields matter most:

- `payment.connected` (a Revolut method is saved; `payment_ready` is the legacy alias)
- `access_granted`
- `developer_api.api_access_enabled`
- `developer_api.flight_search.searches_remaining` — what actually gates search
- `developer_api.balance_cents` — only buys blocks past the free allowance
- `developer_api.minimum_top_up_cents`
- `developer_api.auto_refill_enabled`

## Fast preflight checklist

Before the first production search for a key, assert all of the following:

- the request includes `X-API-Key`
- `payment.connected` is `true` (a Revolut method is saved)
- `developer_api.api_access_enabled` is `true`
- `developer_api.flight_search.searches_remaining` is greater than `0` (balance only matters once
  the free look-to-book allowance runs out)
- `origin`, `destination`, and `date_from` are present
- the requested `limit` is between `1` and `200`

## Retry guidance

- Retry `429` with backoff instead of hammering the endpoint.
- Retry `5xx` only after logging the original request and checking whether the account still has balance.
- Do not blindly retry `400`, `401`, `402`, `403`, or `409`; those usually need a state fix first.

## When to use local search instead

Use local LetsFG search when you want to:

- sweep many nearby dates for cheap exploration
- debug route coverage without burning paid API balance
- prototype the UX before wiring the account lifecycle

Use the public developer API when you need managed cloud search behind the website-owned contract.

Next: if you need the exact path list or machine-readable schema, use [OpenAPI and Swagger](openapi.md).