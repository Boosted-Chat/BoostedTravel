import { withFee } from './pricing'

export interface OfferPriceAncillary {
  included?: boolean
  price?: number
  currency?: string
}

export interface OfferPriceAncillaries {
  cabin_bag?: OfferPriceAncillary
  checked_bag?: OfferPriceAncillary
  seat_selection?: OfferPriceAncillary
}

export interface OfferPriceLike {
  price: number
  currency: string
  source?: string
  ancillaries?: OfferPriceAncillaries
}

function isGoogleFlightsSource(source: string | undefined) {
  if (!source) {
    return false
  }

  const normalized = source.trim().toLowerCase()
  return normalized === 'google_flights' || normalized === 'serpapi_google'
}

export function hasIncludedAncillary(ancillary?: OfferPriceAncillary) {
  return ancillary?.included === true
}

export function hasPaidAncillary(ancillary?: OfferPriceAncillary) {
  return typeof ancillary?.price === 'number' && ancillary.price > 0
}

function getCompatibleAncillaryAmount(offer: OfferPriceLike, ancillary?: OfferPriceAncillary): number | null {
  if (!ancillary || ancillary.included === true) {
    return null
  }

  if (typeof ancillary.price !== 'number' || ancillary.price <= 0) {
    return null
  }

  const ancillaryCurrency = ancillary.currency || offer.currency
  if (ancillaryCurrency !== offer.currency) {
    return null
  }

  return ancillary.price
}

export function getOfferBaseTotal(offer: OfferPriceLike) {
  if (isGoogleFlightsSource(offer.source)) {
    return Math.round(offer.price * 100) / 100
  }

  return withFee(offer.price, offer.currency)
}

export function getOfferTotalWithAncillary(offer: OfferPriceLike, ancillary?: OfferPriceAncillary) {
  const ancillaryAmount = getCompatibleAncillaryAmount(offer, ancillary)
  if (ancillaryAmount === null) {
    return null
  }

  return Math.round((getOfferBaseTotal(offer) + ancillaryAmount) * 100) / 100
}

export function getOfferKnownTotalPrice(offer: OfferPriceLike) {
  let total = getOfferBaseTotal(offer)

  for (const ancillary of Object.values(offer.ancillaries || {})) {
    const ancillaryAmount = getCompatibleAncillaryAmount(offer, ancillary)
    if (ancillaryAmount !== null) {
      total += ancillaryAmount
    }
  }

  return Math.round(total * 100) / 100
}