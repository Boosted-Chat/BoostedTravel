export function getGoogleFlightsSavingsAmount(
  price: number,
  googleFlightsPrice: number | null | undefined,
): number | null {
  if (!Number.isFinite(price) || !Number.isFinite(googleFlightsPrice)) {
    return null
  }

  const diff = Math.round(((googleFlightsPrice as number) - price) * 100) / 100
  if (diff <= 0.005) {
    return null
  }

  return diff
}

export function formatGoogleFlightsSavings(amount: number, currency: string, locale?: string): string {
  const rounded = Math.round(amount * 100) / 100

  try {
    return new Intl.NumberFormat(locale, {
      style: 'currency',
      currency,
      minimumFractionDigits: Number.isInteger(rounded) ? 0 : 2,
      maximumFractionDigits: 2,
    }).format(rounded)
  } catch {
    const formatted = Number.isInteger(rounded)
      ? String(rounded)
      : rounded.toFixed(2).replace(/0+$/, '').replace(/\.$/, '')
    return `${formatted} ${currency}`
  }
}