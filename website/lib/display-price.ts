import {
  getOfferKnownTotalPrice,
  getOfferTotalWithAncillary,
  type OfferPriceAncillary,
  type OfferPriceLike,
} from './offer-pricing'
import { formatCurrencyAmount } from './user-currency'

const FX_VS_EUR: Record<string, number> = {
  AED: 4.33,
  ARS: 1350.0,
  AUD: 1.64,
  BGN: 1.96,
  BRL: 5.87,
  CAD: 1.61,
  CHF: 0.92,
  CNY: 8.05,
  CZK: 24.3,
  DKK: 7.47,
  EGP: 60.0,
  EUR: 1.0,
  GBP: 0.87,
  HKD: 9.24,
  HUF: 363.0,
  IDR: 20270.0,
  INR: 109.5,
  JPY: 188.0,
  KES: 153.0,
  KRW: 1745.0,
  KWD: 0.36,
  MXN: 20.3,
  MYR: 4.66,
  NGN: 1920.0,
  NOK: 11.0,
  NZD: 2.0,
  PHP: 70.9,
  PLN: 4.23,
  RON: 5.1,
  SAR: 4.42,
  SEK: 10.8,
  SGD: 1.5,
  THB: 37.8,
  TRY: 53.0,
  USD: 1.18,
  VND: 30500.0,
  ZAR: 19.3,
}

function normalizeCurrencyCode(currency: string | null | undefined) {
  return currency?.trim().toUpperCase() || 'EUR'
}

export function convertCurrencyAmount(amount: number, fromCurrency: string, toCurrency: string) {
  if (!Number.isFinite(amount)) {
    return amount
  }

  const from = normalizeCurrencyCode(fromCurrency)
  const to = normalizeCurrencyCode(toCurrency)

  if (from === to) {
    return Math.round(amount * 100) / 100
  }

  const fromRate = FX_VS_EUR[from]
  const toRate = FX_VS_EUR[to]

  if (!fromRate || !toRate) {
    return Math.round(amount * 100) / 100
  }

  const eurAmount = amount / fromRate
  return Math.round(eurAmount * toRate * 100) / 100
}

export function getOfferDisplayTotalPrice(offer: OfferPriceLike, displayCurrency: string) {
  return convertCurrencyAmount(getOfferKnownTotalPrice(offer), offer.currency, displayCurrency)
}

export function getOfferDisplayTotalWithAncillary(
  offer: OfferPriceLike,
  ancillary: OfferPriceAncillary | undefined,
  displayCurrency: string,
) {
  const total = getOfferTotalWithAncillary(offer, ancillary)
  if (total === null) {
    return null
  }

  return convertCurrencyAmount(total, offer.currency, displayCurrency)
}

export function formatOfferDisplayPrice(
  amount: number,
  sourceCurrency: string,
  displayCurrency: string,
  locale?: string,
) {
  return formatCurrencyAmount(
    convertCurrencyAmount(amount, sourceCurrency, displayCurrency),
    displayCurrency,
    locale,
  )
}