/**
 * LetsFG service fee calculation.
 *
 * Fee = max(1% of ticket price, floor in local currency equivalent to 1 EUR).
 * Applied at display time only — raw airline prices are never mutated.
 *
 * Floor rates are rough pegged values; they don't need to be live-rates —
 * the point is just that no ticket under ~100 EUR pays less than 1 EUR fee.
 */

// Approximate 1-EUR equivalents for common currencies (rounded to nearest unit)
const EUR_FLOOR: Record<string, number> = {
  EUR: 1,
  USD: 1.10,
  GBP: 0.85,
  PLN: 4.25,
  CZK: 25,
  HUF: 400,
  RON: 5,
  SEK: 11,
  NOK: 12,
  DKK: 7.5,
  CHF: 0.95,
  TRY: 36,
  AED: 4,
  SAR: 4.10,
  INR: 92,
  THB: 39,
  MYR: 5,
  SGD: 1.50,
  AUD: 1.70,
  NZD: 1.85,
  CAD: 1.50,
  MXN: 22,
  BRL: 6,
  JPY: 162,
  KRW: 1500,
  HKD: 8.6,
  ZAR: 20,
  EGP: 55,
}

/**
 * Returns the LetsFG service fee for a given ticket price + currency.
 * fee = max(price × 1%, 1 EUR in local currency)
 */
export function calculateFee(price: number, currency: string): number {
  const floor = EUR_FLOOR[currency.toUpperCase()] ?? 1.10 // default to USD-ish
  return Math.max(price * 0.01, floor)
}

/**
 * Returns the customer-facing price (ticket + fee), rounded to 2 dp.
 */
export function withFee(price: number, currency: string): number {
  return Math.round((price + calculateFee(price, currency)) * 100) / 100
}

/**
 * Formats a price for display. Uses integer for values ≥ 10, 2dp below.
 */
export function fmtPrice(amount: number, currency: string): string {
  const rounded = amount >= 10 ? Math.round(amount) : Number(amount.toFixed(2))
  return `${currency}${rounded}`
}
