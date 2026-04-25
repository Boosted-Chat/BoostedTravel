/**
 * Stripe client singleton.
 *
 * Set STRIPE_SECRET_KEY in your environment (use sk_test_... for dev,
 * sk_live_... for production).
 */

import Stripe from 'stripe'

let _stripe: Stripe | null = null

export function getStripe(): Stripe {
  if (!_stripe) {
    const key = process.env.STRIPE_SECRET_KEY
    if (!key) throw new Error('STRIPE_SECRET_KEY environment variable is not set')
    _stripe = new Stripe(key)
  }
  return _stripe
}

/**
 * Currencies where Stripe expects the amount in the base unit (no cents).
 * All others are multiplied by 100.
 */
const ZERO_DECIMAL_CURRENCIES = new Set([
  'bif', 'clp', 'djf', 'gnf', 'jpy', 'kmf', 'krw', 'mga',
  'pyg', 'rwf', 'ugx', 'vnd', 'vuv', 'xaf', 'xof', 'xpf',
])

/**
 * Convert a float amount to Stripe's expected integer (smallest currency unit).
 * e.g. EUR 1.50 → 150,  JPY 150 → 150
 */
export function toStripeAmount(amount: number, currency: string): number {
  if (ZERO_DECIMAL_CURRENCIES.has(currency.toLowerCase())) {
    return Math.round(amount)
  }
  return Math.round(amount * 100)
}
