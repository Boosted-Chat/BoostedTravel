import { KNOWN_CURRENCY_CODES } from './user-currency'

export const LETSFG_CURRENCY_COOKIE = 'LETSFG_CURRENCY'

export const DEFAULT_SEARCH_CURRENCY = 'EUR'

export type CurrencyCode = string
export type DisplayCurrencyCode = CurrencyCode

const ALLOWED = new Set<string>(KNOWN_CURRENCY_CODES)

const PRIORITY_DISPLAY_CURRENCY_CODES: CurrencyCode[] = [
  'EUR',
  'USD',
  'GBP',
  'JPY',
  'AUD',
  'CAD',
  'CHF',
  'PLN',
  'SEK',
  'NOK',
  'DKK',
  'CZK',
  'HUF',
  'RON',
  'BRL',
  'MXN',
  'AED',
  'SAR',
  'INR',
  'SGD',
  'MYR',
  'HKD',
  'KRW',
  'THB',
  'TRY',
  'EGP',
  'NZD',
  'ZAR',
]

const DISPLAY_CURRENCY_LABELS: Record<string, string> = {
  AED: 'UAE Dirham',
  AUD: 'Australian Dollar',
  BRL: 'Brazilian Real',
  CAD: 'Canadian Dollar',
  CHF: 'Swiss Franc',
  CZK: 'Czech Koruna',
  DKK: 'Danish Krone',
  EGP: 'Egyptian Pound',
  EUR: 'Euro',
  GBP: 'British Pound',
  HKD: 'Hong Kong Dollar',
  HUF: 'Hungarian Forint',
  INR: 'Indian Rupee',
  JPY: 'Japanese Yen',
  KRW: 'South Korean Won',
  MXN: 'Mexican Peso',
  MYR: 'Malaysian Ringgit',
  NOK: 'Norwegian Krone',
  NZD: 'New Zealand Dollar',
  PLN: 'Polish Zloty',
  RON: 'Romanian Leu',
  SAR: 'Saudi Riyal',
  SEK: 'Swedish Krona',
  SGD: 'Singapore Dollar',
  THB: 'Thai Baht',
  TRY: 'Turkish Lira',
  USD: 'US Dollar',
  ZAR: 'South African Rand',
}

export const DISPLAY_CURRENCIES = Array.from(new Set([
  ...PRIORITY_DISPLAY_CURRENCY_CODES,
  ...KNOWN_CURRENCY_CODES,
]))
  .filter((code) => ALLOWED.has(code))
  .map((code) => ({
    code,
    label: DISPLAY_CURRENCY_LABELS[code] || code,
  }))

export const CURRENCY_CHANGE_EVENT = 'letsfg-currency-change'

export function normalizeCurrencyCode(raw: string | undefined | null): CurrencyCode | null {
  if (!raw || typeof raw !== 'string') return null
  const normalized = raw.trim().toUpperCase()
  return ALLOWED.has(normalized) ? normalized : null
}

export function resolveSearchCurrency(input: {
  queryParam?: string | null
  cookieValue?: string | null
  fallback?: string | null
}): CurrencyCode {
  return normalizeCurrencyCode(input.queryParam ?? undefined)
    || normalizeCurrencyCode(input.cookieValue ?? undefined)
    || normalizeCurrencyCode(input.fallback ?? undefined)
    || DEFAULT_SEARCH_CURRENCY
}

function safeDecodeURIComponent(value: string): string {
  try {
    return decodeURIComponent(value)
  } catch {
    return ''
  }
}

function readCurrencyCookie(): string | null {
  if (typeof document === 'undefined') return null
  const match = document.cookie.match(new RegExp(`(?:^|;\\s*)${LETSFG_CURRENCY_COOKIE}=([^;]*)`))
  return match?.[1] ? safeDecodeURIComponent(match[1].trim()) : null
}

export function readBrowserCurrencyPreference(fallback: CurrencyCode = DEFAULT_SEARCH_CURRENCY): CurrencyCode {
  return resolveSearchCurrency({
    cookieValue: readCurrencyCookie(),
    fallback,
  })
}

export function readBrowserSearchCurrency(fallback: CurrencyCode = DEFAULT_SEARCH_CURRENCY): CurrencyCode {
  if (typeof window === 'undefined') {
    return readBrowserCurrencyPreference(fallback)
  }

  const params = new URLSearchParams(window.location.search)
  return resolveSearchCurrency({
    queryParam: params.get('cur'),
    cookieValue: readCurrencyCookie(),
    fallback,
  })
}