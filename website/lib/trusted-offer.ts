import crypto from 'node:crypto'

import { cacheOffers, getCachedOffer } from './offer-cache'

const FSW_URL = process.env.FSW_URL || 'https://flight-search-worker-qryvus4jia-uc.a.run.app'
const FSW_SECRET = process.env.FSW_SECRET || ''
const OFFER_SNAPSHOT_SECRET = process.env.OFFER_SNAPSHOT_SECRET || FSW_SECRET || 'letsfg-local-offer-snapshot-secret'
const GOOGLE_FLIGHTS_SOURCES = new Set(['google_flights', 'serpapi_google'])

export interface TrustedSegment {
  airline: string
  airline_code: string
  flight_number: string
  origin: string
  destination: string
  departure_time: string
  arrival_time: string
  duration_minutes?: number
}

export interface TrustedInbound {
  origin: string
  destination: string
  departure_time: string
  arrival_time: string
  duration_minutes: number
  stops: number
  airline?: string
  airline_code?: string
  segments?: TrustedSegment[]
}

export interface TrustedBookingOption {
  leg: 'outbound' | 'inbound'
  airline: string
  airline_code: string
  booking_url: string
  price?: number
  currency?: string
  origin?: string
  destination?: string
  departure_time?: string
  arrival_time?: string
}

export interface TrustedTripLeg {
  leg: 'outbound' | 'inbound'
  airline: string
  airline_code: string
  origin: string
  destination: string
  departure_time: string
  arrival_time: string
  duration_minutes: number
  price?: number
  currency?: string
}

export interface TrustedAncillary {
  included?: boolean
  price?: number
  currency?: string
  description?: string
}

export interface TrustedAncillaries {
  cabin_bag?: TrustedAncillary
  checked_bag?: TrustedAncillary
  seat_selection?: TrustedAncillary
}

export interface TrustedOffer {
  id: string
  price: number
  google_flights_price?: number
  source?: string
  currency: string
  airline: string
  airline_code: string
  origin: string
  origin_name: string
  destination: string
  destination_name: string
  departure_time: string
  arrival_time: string
  duration_minutes: number
  stops: number
  flight_number: string
  segments?: TrustedSegment[]
  inbound?: TrustedInbound
  booking_url?: string
  is_combo?: boolean
  booking_options?: TrustedBookingOption[]
  trip_breakdown?: TrustedTripLeg[]
  ancillaries?: TrustedAncillaries
}

export interface PublicOffer extends Omit<TrustedOffer, 'booking_url' | 'booking_options'> {
  offer_ref?: string
}

const IATA_TO_NAME: Record<string, string> = {
  FR: 'Ryanair', U2: 'easyJet', W6: 'Wizz Air', W9: 'Wizz Air Malta',
  DY: 'Norwegian', VY: 'Vueling', BA: 'British Airways', LH: 'Lufthansa',
  AF: 'Air France', KL: 'KLM', IB: 'Iberia', I2: 'Iberia Express',
  TP: 'TAP Air Portugal', EK: 'Emirates', QR: 'Qatar Airways',
  TK: 'Turkish Airlines', AA: 'American Airlines', UA: 'United Airlines',
  DL: 'Delta Air Lines', AC: 'Air Canada', SQ: 'Singapore Airlines',
  CX: 'Cathay Pacific', QF: 'Qantas', EY: 'Etihad Airways',
  HV: 'Transavia', V7: 'Volotea', LS: 'Jet2', LX: 'Swiss', OS: 'Austrian',
  SN: 'Brussels Airlines', AY: 'Finnair', SK: 'SAS', FI: 'Icelandair',
  VS: 'Virgin Atlantic', A3: 'Aegean Airlines', PC: 'Pegasus Airlines',
  XQ: 'SunExpress', BT: 'airBaltic', WN: 'Southwest', B6: 'JetBlue',
  NK: 'Spirit Airlines', F9: 'Frontier Airlines', G4: 'Allegiant Air',
  AK: 'AirAsia', FZ: 'flydubai', G9: 'Air Arabia', JU: 'Air Serbia',
  RK: 'Ryanair UK', EI: 'Aer Lingus', JL: 'Japan Airlines', NH: 'ANA',
  KE: 'Korean Air', '6E': 'IndiGo', '5J': 'Cebu Pacific', LA: 'LATAM Airlines',
  EW: 'Eurowings', DE: 'Condor', '4U': 'Germanwings',
}

const DEMO_OFFER: TrustedOffer = {
  id: 'demo-offer-1',
  price: 29,
  google_flights_price: 32,
  currency: 'EUR',
  airline: 'Ryanair',
  airline_code: 'FR',
  origin: 'STN',
  origin_name: 'London Stansted',
  destination: 'BCN',
  destination_name: 'Barcelona El Prat',
  departure_time: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString().replace(/T.*/, 'T10:30:00.000Z'),
  arrival_time: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString().replace(/T.*/, 'T14:05:00.000Z'),
  duration_minutes: 215,
  stops: 0,
  flight_number: 'FR2413',
  booking_url: 'https://www.ryanair.com/select?from=STN&to=BCN&date=demo&price=29',
}

const HIGH_VALUE_DEMO: TrustedOffer = {
  id: 'demo-offer-expensive',
  price: 2499,
  google_flights_price: 2799,
  currency: 'EUR',
  airline: 'British Airways',
  airline_code: 'BA',
  origin: 'LHR',
  origin_name: 'London Heathrow',
  destination: 'JFK',
  destination_name: 'New York JFK',
  departure_time: new Date(Date.now() + 14 * 24 * 60 * 60 * 1000).toISOString().replace(/T.*/, 'T09:15:00.000Z'),
  arrival_time: new Date(Date.now() + 14 * 24 * 60 * 60 * 1000).toISOString().replace(/T.*/, 'T12:30:00.000Z'),
  duration_minutes: 435,
  stops: 0,
  flight_number: 'BA117',
  booking_url: 'https://www.britishairways.com/select?from=LHR&to=JFK&date=demo&price=2499',
}

export function extractIataFromFlightNo(flightNo: string): string {
  const match = flightNo.match(/^([A-Z]{2}|[A-Z]\d|\d[A-Z])/i)
  return match ? match[1].toUpperCase() : ''
}

function looksLikeIataCode(value: string): boolean {
  return /^[A-Z0-9]{2}$/i.test(value.trim())
}

function looksLikePlaceholderAirline(value: string): boolean {
  return /^Airline\s*#\d+$/i.test(value.trim())
}

function looksLikeCompositeAirline(value: string): boolean {
  return value.includes('|')
}

function normalizeFlightNumber(flightNumber: string, airlineCode: string): string {
  return flightNumber.trim().toUpperCase() === airlineCode.trim().toUpperCase()
    ? ''
    : flightNumber
}

function getRouteTiming(route: any, fallbackDeparture: string, fallbackArrival: string): {
  departure: string
  arrival: string
  durationMinutes: number
} {
  const segments: any[] = route?.segments || []
  const first = segments[0] || {}
  const last = segments[segments.length - 1] || first

  let departure = first.departure || first.departure_time || fallbackDeparture || ''
  let arrival = last.arrival || last.arrival_time || fallbackArrival || ''
  let durationMinutes = departure && arrival
    ? Math.round((new Date(arrival).getTime() - new Date(departure).getTime()) / 60000)
    : 0

  const routeDurationMinutes = Math.round(((route?.total_duration_seconds as number | undefined) || 0) / 60)
  const segmentDurationMinutes = segments.reduce((total: number, segment: any) => {
    const segmentMinutes = Math.round(((segment?.duration_seconds as number | undefined) || 0) / 60)
    return total + Math.max(0, segmentMinutes)
  }, 0)
  const fallbackDurationMinutes = routeDurationMinutes || segmentDurationMinutes

  if ((!arrival || durationMinutes <= 0) && departure && fallbackDurationMinutes > 0) {
    arrival = new Date(new Date(departure).getTime() + fallbackDurationMinutes * 60000).toISOString()
    durationMinutes = fallbackDurationMinutes
  }

  if ((!departure || durationMinutes <= 0) && arrival && fallbackDurationMinutes > 0) {
    departure = new Date(new Date(arrival).getTime() - fallbackDurationMinutes * 60000).toISOString()
    durationMinutes = fallbackDurationMinutes
  }

  if (durationMinutes <= 0 && fallbackDurationMinutes > 0) {
    durationMinutes = fallbackDurationMinutes
  }

  return { departure, arrival, durationMinutes }
}

function toUpperCode(value: unknown): string {
  return typeof value === 'string' ? value.trim().toUpperCase() : ''
}

function parsePriceValue(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.round(value * 100) / 100
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) {
      return Math.round(parsed * 100) / 100
    }
  }
  return undefined
}

function parseLegacyPriceAndCurrency(value: unknown): {
  price?: number
  currency?: string
} {
  if (typeof value !== 'string') {
    return {}
  }

  const normalized = value.trim()
  if (!normalized) {
    return {}
  }

  const leadingCurrencyMatch = normalized.match(/^([A-Z]{3})\s+(-?\d+(?:[.,]\d+)?)/i)
  if (leadingCurrencyMatch) {
    const [, currency, amount] = leadingCurrencyMatch
    const price = parsePriceValue(amount.replace(',', '.'))
    return {
      price,
      currency: currency.toUpperCase(),
    }
  }

  const trailingCurrencyMatch = normalized.match(/^(-?\d+(?:[.,]\d+)?)\s+([A-Z]{3})$/i)
  if (trailingCurrencyMatch) {
    const [, amount, currency] = trailingCurrencyMatch
    const price = parsePriceValue(amount.replace(',', '.'))
    return {
      price,
      currency: currency.toUpperCase(),
    }
  }

  return {
    price: parsePriceValue(normalized.replace(',', '.')),
  }
}

function parseStringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim().length > 0 ? value : undefined
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function parseBooleanLike(value: unknown): boolean | undefined {
  if (typeof value === 'boolean') {
    return value
  }

  if (typeof value !== 'string') {
    return undefined
  }

  const normalized = value.trim().toLowerCase()
  if (!normalized) {
    return undefined
  }

  if (
    normalized === 'true'
    || normalized === 'yes'
    || normalized === 'included'
    || normalized === 'free'
    || normalized === 'included_in_ticket'
    || normalized === 'included in ticket'
    || normalized === 'allowed'
    || normalized === 'available'
    || normalized.includes('included')
    || normalized.includes('free')
  ) {
    return true
  }

  if (
    normalized === 'false'
    || normalized === 'no'
    || normalized === 'not included'
    || normalized === 'paid'
    || normalized === 'extra'
    || normalized.includes('not included')
    || normalized.includes('extra fee')
  ) {
    return false
  }

  return undefined
}

function isGoogleFlightsSource(source: unknown): boolean {
  return typeof source === 'string' && GOOGLE_FLIGHTS_SOURCES.has(source.trim().toLowerCase())
}

function extractGoogleFlightsQuotedCurrency(rawOffer: any): string | undefined {
  const bookingUrl = parseStringValue(rawOffer?.booking_url)
  if (!bookingUrl) {
    return undefined
  }

  try {
    const currency = new URL(bookingUrl).searchParams.get('curr')
    return currency ? currency.trim().toUpperCase() : undefined
  } catch {
    return undefined
  }
}

type OfferSignatureSegment = {
  airline_code?: string
  flight_number?: string
  origin?: string
  destination?: string
  departure_time?: string
  arrival_time?: string
}

function getOfferSignatureSegments(offer: TrustedOffer, leg: 'outbound' | 'inbound'): OfferSignatureSegment[] {
  if (leg === 'outbound') {
    if (offer.segments?.length) {
      return offer.segments
    }

    return [{
      airline_code: offer.airline_code,
      flight_number: offer.flight_number,
      origin: offer.origin,
      destination: offer.destination,
      departure_time: offer.departure_time,
      arrival_time: offer.arrival_time,
    }]
  }

  if (!offer.inbound) {
    return []
  }

  if (offer.inbound.segments?.length) {
    return offer.inbound.segments
  }

  return [{
    airline_code: offer.inbound.airline_code || offer.airline_code,
    flight_number: '',
    origin: offer.inbound.origin,
    destination: offer.inbound.destination,
    departure_time: offer.inbound.departure_time,
    arrival_time: offer.inbound.arrival_time,
  }]
}

function getOfferSignature(offer: TrustedOffer): string | undefined {
  const outboundSegments = getOfferSignatureSegments(offer, 'outbound')
  if (outboundSegments.length === 0) {
    return undefined
  }

  const serializeSegment = (segment: OfferSignatureSegment): string => [
    toUpperCode(segment.airline_code),
    (segment.flight_number || '').trim().toUpperCase(),
    toUpperCode(segment.origin),
    toUpperCode(segment.destination),
    parseStringValue(segment.departure_time) || '',
    parseStringValue(segment.arrival_time) || '',
  ].join('|')

  const outboundSignature = outboundSegments.map(serializeSegment).join('>')
  const inboundSignature = getOfferSignatureSegments(offer, 'inbound').map(serializeSegment).join('>')
  return `${outboundSignature}||${inboundSignature}`
}

function buildGoogleFlightsMatchIndex(
  rawOffers: any[],
  offers: TrustedOffer[],
): Map<string, Map<string, number>> {
  const matchIndex = new Map<string, Map<string, number>>()

  for (const [index, rawOffer] of rawOffers.entries()) {
    if (!isGoogleFlightsSource(rawOffer?.source)) {
      continue
    }

    const normalizedOffer = offers[index]
    const signature = normalizedOffer ? getOfferSignature(normalizedOffer) : undefined
    const quotedCurrency = extractGoogleFlightsQuotedCurrency(rawOffer)
    const price = parsePriceValue(normalizedOffer?.price)
    if (!signature || !quotedCurrency || typeof price !== 'number') {
      continue
    }

    const pricesByCurrency = matchIndex.get(signature) ?? new Map<string, number>()
    const currentPrice = pricesByCurrency.get(quotedCurrency)
    if (typeof currentPrice !== 'number' || price < currentPrice) {
      pricesByCurrency.set(quotedCurrency, price)
    }
    matchIndex.set(signature, pricesByCurrency)
  }

  return matchIndex
}

export function applyGoogleFlightsBaseline(
  rawOffers: any[],
  offers: TrustedOffer[],
  explicitBaseline?: number,
): TrustedOffer[] {
  // Only compare like-for-like itineraries in the same quoted currency.
  const matchIndex = buildGoogleFlightsMatchIndex(rawOffers, offers)
  const fallbackBaseline = parsePriceValue(explicitBaseline)
  if (matchIndex.size === 0 && typeof fallbackBaseline !== 'number') {
    return offers
  }

  let changed = false
  const nextOffers = offers.map((offer, index) => {
    if (typeof offer.google_flights_price === 'number') {
      return offer
    }

    const signature = getOfferSignature(offer)
    const matchedPrice = signature
      ? matchIndex.get(signature)?.get(toUpperCode(offer.currency))
      : undefined
    const googleFlightsPrice = typeof matchedPrice === 'number' ? matchedPrice : fallbackBaseline
    if (typeof googleFlightsPrice !== 'number' || offer.google_flights_price === googleFlightsPrice) {
      return offer
    }

    changed = true
    return {
      ...offer,
      google_flights_price: googleFlightsPrice,
    }
  })

  return changed ? nextOffers : offers
}

function mergeAncillary(current: TrustedAncillary | undefined, next: TrustedAncillary): TrustedAncillary {
  const merged: TrustedAncillary = {
    included: current?.included,
    price: current?.price,
    currency: current?.currency,
    description: current?.description,
  }

  if (typeof next.included === 'boolean') {
    merged.included = next.included
  }

  if (typeof next.price === 'number') {
    if (typeof merged.price !== 'number' || next.price < merged.price) {
      merged.price = next.price
    }
  }

  if (typeof next.currency === 'string' && next.currency.trim().length > 0) {
    merged.currency = next.currency
  }

  if (typeof next.description === 'string' && next.description.trim().length > 0) {
    if (!merged.description || next.description.trim().length < merged.description.trim().length) {
      merged.description = next.description
    }
  }

  return merged
}

function parseAncillaryValue(value: unknown, defaultCurrency: string): TrustedAncillary | undefined {
  if (value == null) {
    return undefined
  }

  if (typeof value === 'boolean') {
    return { included: value }
  }

  if (typeof value === 'number') {
    const price = parsePriceValue(value)
    if (price == null) {
      return undefined
    }
    return {
      included: price <= 0,
      price,
      currency: defaultCurrency,
    }
  }

  if (typeof value === 'string') {
    const normalized = value.trim()
    if (!normalized) {
      return undefined
    }

    const parsedBoolean = parseBooleanLike(normalized)
    const legacy = parseLegacyPriceAndCurrency(normalized)
    if (typeof legacy.price === 'number') {
      return {
        included: legacy.price <= 0,
        price: legacy.price,
        currency: legacy.currency || defaultCurrency,
        description: normalized,
      }
    }

    if (typeof parsedBoolean === 'boolean') {
      return {
        included: parsedBoolean,
        description: normalized,
      }
    }

    return {
      description: normalized,
    }
  }

  if (Array.isArray(value)) {
    return value.reduce<TrustedAncillary | undefined>((current, entry) => {
      const parsed = parseAncillaryValue(entry, defaultCurrency)
      return parsed ? mergeAncillary(current, parsed) : current
    }, undefined)
  }

  if (!isRecord(value)) {
    return undefined
  }

  const directPriceCandidates = [
    value.price,
    value.amount,
    value.cost,
    value.value,
    value.total,
    value.fee,
    value.min_price,
    value.minPrice,
    value.from,
  ]

  let price: number | undefined
  let currency: string | undefined
  for (const candidate of directPriceCandidates) {
    if (typeof candidate === 'number') {
      price = parsePriceValue(candidate)
      currency = defaultCurrency
      break
    }
    if (typeof candidate === 'string') {
      const parsed = parseLegacyPriceAndCurrency(candidate)
      if (typeof parsed.price === 'number') {
        price = parsed.price
        currency = parsed.currency || defaultCurrency
        break
      }
    }
  }

  const explicitIncluded = parseBooleanLike(
    value.included
    ?? value.free
    ?? value.is_included
    ?? value.isIncluded
    ?? value.available,
  )

  const description = parseStringValue(value.label)
    || parseStringValue(value.description)
    || parseStringValue(value.text)
    || parseStringValue(value.name)
    || parseStringValue(value.summary)

  if (typeof explicitIncluded !== 'boolean' && typeof price !== 'number' && !description) {
    return undefined
  }

  return {
    included: explicitIncluded ?? (typeof price === 'number' ? price <= 0 : undefined),
    price,
    currency: currency || (typeof price === 'number' ? defaultCurrency : undefined),
    description,
  }
}

function getAncillaryTarget(key: string): keyof TrustedAncillaries | undefined {
  const normalized = key.trim().toLowerCase()
  if (!normalized) {
    return undefined
  }

  const seatLike = normalized.includes('seat') || normalized.includes('seating')
  if (seatLike) {
    return 'seat_selection'
  }

  const cabinLike = normalized.includes('cabin') || normalized.includes('carry') || normalized.includes('hand') || normalized.includes('personal')
  const bagLike = normalized.includes('bag') || normalized.includes('baggage') || normalized.includes('luggage')
  if (cabinLike && bagLike) {
    return 'cabin_bag'
  }

  const checkedLike = normalized.includes('checked') || normalized.includes('hold') || normalized.includes('drop')
  if (checkedLike && bagLike) {
    return 'checked_bag'
  }

  if (bagLike && /(^|[_\-\s])(1|20kg|23kg|25kg|32kg)([_\-\s]|$)/i.test(normalized)) {
    return 'checked_bag'
  }

  return undefined
}

function buildAncillaries(raw: any): TrustedAncillaries | undefined {
  const defaultCurrency = parseStringValue(raw.currency) || 'EUR'
  const ancillaries: TrustedAncillaries = {}

  const assign = (target: keyof TrustedAncillaries, value: unknown) => {
    const parsed = parseAncillaryValue(value, defaultCurrency)
    if (!parsed) {
      return
    }
    ancillaries[target] = mergeAncillary(ancillaries[target], parsed)
  }

  const bagsPrice = raw.bags_price ?? raw.bagsPrice
  if (isRecord(bagsPrice)) {
    for (const [key, value] of Object.entries(bagsPrice)) {
      const target = getAncillaryTarget(key) || 'checked_bag'
      assign(target, value)
    }
  }

  if (isRecord(raw.conditions)) {
    for (const [key, value] of Object.entries(raw.conditions)) {
      const target = getAncillaryTarget(key)
      if (!target) {
        continue
      }
      assign(target, value)
    }
  }

  return Object.keys(ancillaries).length > 0 ? ancillaries : undefined
}

function getRouteSummary(route: any, fallbackAirlineName: string, fallbackAirlineCode: string): Omit<TrustedTripLeg, 'leg' | 'price' | 'currency'> {
  const segments: any[] = route?.segments || []
  const first = segments[0] || {}
  const last = segments[segments.length - 1] || first
  const timing = getRouteTiming(
    route,
    first.departure || first.departure_time || '',
    last.arrival || last.arrival_time || '',
  )
  const airlineCode = (typeof first.airline === 'string' && looksLikeIataCode(first.airline)
    ? first.airline.toUpperCase()
    : extractIataFromFlightNo(first.flight_no || first.flight_number || '')) || fallbackAirlineCode
  const rawAirline = first.airline_name || first.carrier_name || first.airline || fallbackAirlineName
  const airlineName = (
    rawAirline
    && !looksLikeIataCode(rawAirline)
    && !looksLikePlaceholderAirline(rawAirline)
  ) ? rawAirline : (IATA_TO_NAME[airlineCode] || fallbackAirlineName)

  return {
    airline: airlineName,
    airline_code: airlineCode,
    origin: toUpperCode(first.origin),
    destination: toUpperCode(last.destination),
    departure_time: timing.departure,
    arrival_time: timing.arrival,
    duration_minutes: timing.durationMinutes,
  }
}

function formatDateForBookingPath(iso: string | undefined): string | undefined {
  return iso && iso.length >= 10 ? iso.slice(0, 10) : undefined
}

function formatDateForSkyscanner(iso: string | undefined): string | undefined {
  const normalized = formatDateForBookingPath(iso)
  if (!normalized) {
    return undefined
  }

  return `${normalized.slice(2, 4)}${normalized.slice(5, 7)}${normalized.slice(8, 10)}`
}

function isSimpleReturnTrip(
  leg: Omit<TrustedTripLeg, 'price' | 'currency'>,
  returnLeg: Omit<TrustedTripLeg, 'price' | 'currency'> | undefined,
): returnLeg is Omit<TrustedTripLeg, 'price' | 'currency'> {
  if (!returnLeg) {
    return false
  }

  return (
    Boolean(leg.origin)
    && Boolean(leg.destination)
    && Boolean(returnLeg.origin)
    && Boolean(returnLeg.destination)
    && returnLeg.origin.toUpperCase() === leg.destination.toUpperCase()
    && returnLeg.destination.toUpperCase() === leg.origin.toUpperCase()
  )
}

function repairKnownBookingUrl(
  url: string,
  leg: Omit<TrustedTripLeg, 'price' | 'currency'>,
  options?: { returnLeg?: Omit<TrustedTripLeg, 'price' | 'currency'> },
): string {
  if (!url) {
    return url
  }

  if (/wizzair\.com/i.test(url)) {
    try {
      const parsed = new URL(url)
      const localeMatch = parsed.pathname.match(/^\/[a-z]{2}-[a-z]{2}\b/i)
      const localePrefix = localeMatch?.[0] || '/en-gb'
      const outboundDate = formatDateForBookingPath(leg.departure_time)
      const inboundDate = formatDateForBookingPath(options?.returnLeg?.departure_time) || 'null'

      if (outboundDate) {
        parsed.hash = ''
        parsed.pathname = `${localePrefix}/booking/select-flight/${leg.origin}/${leg.destination}/${outboundDate}/${inboundDate}/1/0/0/null`
        parsed.search = ''
        return parsed.toString()
      }
    } catch {
      return url
    }
  }

  if (/skyscanner\./i.test(url)) {
    try {
      const parsed = new URL(url)
      const pathParts = parsed.pathname.split('/').filter(Boolean)
      const transportIndex = pathParts.findIndex((part) => part === 'transport')
      const listingSegment = transportIndex >= 0 && pathParts[transportIndex + 1]
        ? pathParts[transportIndex + 1]
        : 'flights'
      const outboundDate = formatDateForSkyscanner(leg.departure_time)
      const inboundDate = formatDateForSkyscanner(options?.returnLeg?.departure_time)

      if (!outboundDate) {
        return url
      }

      const nextPath = [
        'transport',
        listingSegment,
        leg.origin.toLowerCase(),
        leg.destination.toLowerCase(),
        outboundDate,
      ]

      if (inboundDate) {
        nextPath.push(inboundDate)
      }

      parsed.pathname = `/${nextPath.join('/')}/`
      parsed.search = ''
      parsed.searchParams.set('adultsv2', '1')
      parsed.searchParams.set('cabinclass', 'economy')
      parsed.searchParams.set('childrenv2', '')
      parsed.searchParams.set('outboundaltsenabled', 'false')
      parsed.searchParams.set('inboundaltsenabled', 'false')
      parsed.searchParams.set('preferdirects', 'false')
      parsed.searchParams.set('rtn', inboundDate ? '1' : '0')
      return parsed.toString()
    } catch {
      return url
    }
  }

  if (/(kayak|momondo|cheapflights)\./i.test(url)) {
    try {
      const parsed = new URL(url)
      const outboundDate = formatDateForBookingPath(leg.departure_time)
      const returnLeg = options?.returnLeg
      const inboundDate = formatDateForBookingPath(returnLeg?.departure_time)

      if (!outboundDate || !inboundDate || !isSimpleReturnTrip(leg, returnLeg)) {
        return url
      }

      if (/kayak\./i.test(parsed.hostname)) {
        parsed.pathname = `/flights/${leg.origin}-${leg.destination}/${outboundDate}/${inboundDate}`
        return parsed.toString()
      }

      if (/momondo\./i.test(parsed.hostname) || /cheapflights\./i.test(parsed.hostname)) {
        const pathParts = parsed.pathname.split('/').filter(Boolean)
        const passengerPart = pathParts[3]
        const nextPath = ['flight-search', `${leg.origin}-${leg.destination}`, outboundDate, inboundDate]
        if (passengerPart) {
          nextPath.push(passengerPart)
        }
        parsed.pathname = `/${nextPath.join('/')}`
        return parsed.toString()
      }
    } catch {
      return url
    }
  }

  if (/ryanair\.com/i.test(url)) {
    try {
      const parsed = new URL(url)
      const outboundDate = formatDateForBookingPath(leg.departure_time)
      const inboundDate = formatDateForBookingPath(options?.returnLeg?.departure_time)
      const isReturn = Boolean(inboundDate)

      if (parsed.searchParams.has('originIata')) {
        parsed.searchParams.set('originIata', leg.origin)
      }
      if (parsed.searchParams.has('destinationIata')) {
        parsed.searchParams.set('destinationIata', leg.destination)
      }
      if (outboundDate) {
        parsed.searchParams.set('dateOut', outboundDate)
      }
      if (parsed.searchParams.has('dateIn') || isReturn) {
        parsed.searchParams.set('dateIn', inboundDate || '')
      }
      if (parsed.searchParams.has('isReturn') || isReturn) {
        parsed.searchParams.set('isReturn', isReturn ? 'true' : 'false')
      }
      return parsed.toString()
    } catch {
      return url
    }
  }

  return url
}

function resolveAirlineFromRaw(raw: any, first: any): { airlineName: string; airlineCode: string } {
  const candidates = [
    first.airline_name,
    first.carrier_name,
    raw.airline,
    first.airline,
    ...((raw.airlines || []) as string[]),
    raw.owner_airline,
  ].filter((value: unknown): value is string => (
    typeof value === 'string'
    && value.trim().length > 0
    && !looksLikePlaceholderAirline(value)
    && !looksLikeCompositeAirline(value)
  ))
  const rawName: string = candidates[0] || ''
  const flightNo: string = first.flight_no || first.flight_number || ''
  const fallbackCode = raw.airline_code
    || (typeof first.airline === 'string' && looksLikeIataCode(first.airline) ? first.airline.toUpperCase() : '')
    || extractIataFromFlightNo(flightNo)
    || '??'

  if (rawName && looksLikeIataCode(rawName)) {
    const code = rawName.toUpperCase()
    return { airlineName: IATA_TO_NAME[code] || code, airlineCode: code }
  }

  return {
    airlineName: rawName || IATA_TO_NAME[fallbackCode] || 'Unknown',
    airlineCode: fallbackCode,
  }
}

function buildTripBreakdown(
  raw: any,
  outboundAirlineName: string,
  outboundAirlineCode: string,
  inbound?: TrustedInbound,
): TrustedTripLeg[] | undefined {
  const outboundSummary = getRouteSummary(raw.outbound || {}, outboundAirlineName, outboundAirlineCode)
  const conditions = raw.conditions || {}
  const outboundLegacyPricing = parseLegacyPriceAndCurrency(conditions.combo_outbound_price)
  const inboundLegacyPricing = parseLegacyPriceAndCurrency(
    conditions.combo_inbound_price || conditions.combo_return_price,
  )
  const tripBreakdown: TrustedTripLeg[] = [
    {
      leg: 'outbound',
      ...outboundSummary,
      price: parsePriceValue(conditions.outbound_price) ?? outboundLegacyPricing.price,
      currency: parseStringValue(conditions.outbound_currency)
        || outboundLegacyPricing.currency
        || raw.currency
        || 'EUR',
    },
  ]

  if (raw.inbound && inbound) {
    tripBreakdown.push({
      leg: 'inbound',
      airline: inbound.airline || outboundAirlineName,
      airline_code: inbound.airline_code || outboundAirlineCode,
      origin: inbound.origin,
      destination: inbound.destination,
      departure_time: inbound.departure_time,
      arrival_time: inbound.arrival_time,
      duration_minutes: inbound.duration_minutes,
      price: parsePriceValue(conditions.inbound_price) ?? inboundLegacyPricing.price,
      currency: parseStringValue(conditions.inbound_currency)
        || inboundLegacyPricing.currency
        || raw.currency
        || 'EUR',
    })
  }

  return tripBreakdown.length > 0 ? tripBreakdown : undefined
}

function buildBookingOptions(
  raw: any,
  tripBreakdown?: TrustedTripLeg[],
): TrustedBookingOption[] | undefined {
  const conditions = raw.conditions || {}
  const isCombo = conditions.combo_type === 'virtual_interlining'
    || (typeof raw.source === 'string' && raw.source.startsWith('combo:'))

  if (!isCombo) {
    return undefined
  }

  const options: TrustedBookingOption[] = []
  if (typeof conditions.outbound_booking_url === 'string' && conditions.outbound_booking_url) {
    const outboundLeg = tripBreakdown?.find((leg) => leg.leg === 'outbound')
    options.push({
      leg: 'outbound',
      airline: outboundLeg?.airline || 'Unknown',
      airline_code: outboundLeg?.airline_code || '??',
      booking_url: outboundLeg ? repairKnownBookingUrl(conditions.outbound_booking_url, outboundLeg) : conditions.outbound_booking_url,
      price: outboundLeg?.price,
      currency: outboundLeg?.currency,
      origin: outboundLeg?.origin,
      destination: outboundLeg?.destination,
      departure_time: outboundLeg?.departure_time,
      arrival_time: outboundLeg?.arrival_time,
    })
  }
  if (typeof conditions.inbound_booking_url === 'string' && conditions.inbound_booking_url) {
    const inboundLeg = tripBreakdown?.find((leg) => leg.leg === 'inbound')
    options.push({
      leg: 'inbound',
      airline: inboundLeg?.airline || 'Unknown',
      airline_code: inboundLeg?.airline_code || '??',
      booking_url: inboundLeg ? repairKnownBookingUrl(conditions.inbound_booking_url, inboundLeg) : conditions.inbound_booking_url,
      price: inboundLeg?.price,
      currency: inboundLeg?.currency,
      origin: inboundLeg?.origin,
      destination: inboundLeg?.destination,
      departure_time: inboundLeg?.departure_time,
      arrival_time: inboundLeg?.arrival_time,
    })
  }

  return options.length > 0 ? options : undefined
}

function hydrateTrustedOffer(offer: TrustedOffer): TrustedOffer {
  const tripBreakdown: TrustedTripLeg[] = offer.trip_breakdown?.length ? offer.trip_breakdown : [
    {
      leg: 'outbound' as const,
      airline: offer.airline,
      airline_code: offer.airline_code,
      origin: offer.origin,
      destination: offer.destination,
      departure_time: offer.departure_time,
      arrival_time: offer.arrival_time,
      duration_minutes: offer.duration_minutes,
    },
    ...(offer.inbound ? [{
      leg: 'inbound' as const,
      airline: offer.inbound.airline || offer.airline,
      airline_code: offer.inbound.airline_code || offer.airline_code,
      origin: offer.inbound.origin,
      destination: offer.inbound.destination,
      departure_time: offer.inbound.departure_time,
      arrival_time: offer.inbound.arrival_time,
      duration_minutes: offer.inbound.duration_minutes,
    }] : []),
  ]

  const bookingOptions = offer.booking_options?.map((option) => {
    const matchingLeg = tripBreakdown.find((leg) => leg.leg === option.leg)
    return matchingLeg ? {
      ...option,
      airline: option.airline || matchingLeg.airline,
      airline_code: option.airline_code || matchingLeg.airline_code,
      booking_url: repairKnownBookingUrl(option.booking_url, matchingLeg),
      price: option.price ?? matchingLeg.price,
      currency: option.currency ?? matchingLeg.currency,
      origin: option.origin ?? matchingLeg.origin,
      destination: option.destination ?? matchingLeg.destination,
      departure_time: option.departure_time ?? matchingLeg.departure_time,
      arrival_time: option.arrival_time ?? matchingLeg.arrival_time,
    } : option
  })

  const primaryLeg = tripBreakdown.find((leg) => leg.leg === 'outbound')
  const returnLeg = tripBreakdown.find((leg) => leg.leg === 'inbound')
  const repairedBookingUrl = primaryLeg && offer.booking_url
    ? repairKnownBookingUrl(offer.booking_url, primaryLeg, { returnLeg })
    : offer.booking_url

  return {
    ...offer,
    trip_breakdown: tripBreakdown,
    booking_options: bookingOptions,
    booking_url: bookingOptions?.[0]?.booking_url || repairedBookingUrl,
  }
}

function getOfferSnapshotKey(): Buffer {
  return crypto.createHash('sha256').update(OFFER_SNAPSHOT_SECRET).digest()
}

function encodeOfferSnapshot(offer: TrustedOffer): string | undefined {
  try {
    const iv = crypto.randomBytes(12)
    const cipher = crypto.createCipheriv('aes-256-gcm', getOfferSnapshotKey(), iv)
    const payload = JSON.stringify({ version: 1, offer })
    const encrypted = Buffer.concat([cipher.update(payload, 'utf8'), cipher.final()])
    const tag = cipher.getAuthTag()
    return `${iv.toString('base64url')}.${encrypted.toString('base64url')}.${tag.toString('base64url')}`
  } catch {
    return undefined
  }
}

function decodeOfferSnapshot(snapshot: string | null | undefined): TrustedOffer | null {
  if (!snapshot) {
    return null
  }

  try {
    const [ivB64, encryptedB64, tagB64] = snapshot.split('.')
    if (!ivB64 || !encryptedB64 || !tagB64) {
      return null
    }

    const decipher = crypto.createDecipheriv(
      'aes-256-gcm',
      getOfferSnapshotKey(),
      Buffer.from(ivB64, 'base64url'),
    )
    decipher.setAuthTag(Buffer.from(tagB64, 'base64url'))

    const decrypted = Buffer.concat([
      decipher.update(Buffer.from(encryptedB64, 'base64url')),
      decipher.final(),
    ]).toString('utf8')
    const parsed = JSON.parse(decrypted)
    const offer = parsed?.offer
    if (!offer || typeof offer !== 'object' || typeof offer.id !== 'string') {
      return null
    }

    return hydrateTrustedOffer(offer as TrustedOffer)
  } catch {
    return null
  }
}

function normalizeSegments(segments: any[], fallbackAirlineName: string, fallbackAirlineCode: string): TrustedSegment[] {
  return segments.map((segment: any) => {
    const departure = segment.departure || segment.departure_time || ''
    const arrival = segment.arrival || segment.arrival_time || ''
    const durationMinutes = departure && arrival
      ? Math.round((new Date(arrival).getTime() - new Date(departure).getTime()) / 60000)
      : 0
    const rawAirline = segment.airline_name || segment.carrier_name || segment.airline || fallbackAirlineName
    const airlineCode = (typeof segment.airline === 'string' && looksLikeIataCode(segment.airline)
      ? segment.airline.toUpperCase()
      : extractIataFromFlightNo(segment.flight_no || '')) || fallbackAirlineCode
    const airlineName = (
      rawAirline
      && !looksLikeIataCode(rawAirline)
      && !looksLikePlaceholderAirline(rawAirline)
    ) ? rawAirline : (IATA_TO_NAME[airlineCode] || fallbackAirlineName)

    return {
      airline: airlineName,
      airline_code: airlineCode,
      flight_number: segment.flight_no || segment.flight_number || '',
      origin: (segment.origin || '').toUpperCase(),
      destination: (segment.destination || '').toUpperCase(),
      departure_time: departure,
      arrival_time: arrival,
      duration_minutes: durationMinutes,
    }
  })
}

export function normalizeTrustedOffer(raw: any, idx: number): TrustedOffer {
  const outbound = raw.outbound || {}
  const segments: any[] = outbound.segments || []
  const first = segments[0] || {}
  const last = segments[segments.length - 1] || {}

  const origin = (first.origin || raw.origin || '').toUpperCase()
  const destination = (last.destination || raw.destination || '').toUpperCase()
  const outboundTiming = getRouteTiming(outbound, raw.departure_time || '', raw.arrival_time || '')
  const departure = outboundTiming.departure
  const arrival = outboundTiming.arrival
  const durationMinutes = outboundTiming.durationMinutes

  const { airlineName, airlineCode } = resolveAirlineFromRaw(raw, first)
  const normalizedSegments = normalizeSegments(segments, airlineName, airlineCode)

  let inbound: TrustedInbound | undefined
  const inboundRaw = raw.inbound
  if (inboundRaw && inboundRaw.segments?.length) {
    const inboundSegments: any[] = inboundRaw.segments
    const inboundFirst = inboundSegments[0] || {}
    const inboundLast = inboundSegments[inboundSegments.length - 1] || inboundFirst
    const inboundTiming = getRouteTiming(inboundRaw, '', '')
    const inboundDeparture = inboundTiming.departure
    const inboundArrival = inboundTiming.arrival
    const inboundDuration = inboundTiming.durationMinutes
    const inboundRawAirline = inboundFirst.airline_name || inboundFirst.carrier_name || inboundFirst.airline || airlineName
    const inboundAirlineCode = (typeof inboundFirst.airline === 'string' && looksLikeIataCode(inboundFirst.airline)
      ? inboundFirst.airline.toUpperCase()
      : extractIataFromFlightNo(inboundFirst.flight_no || '')) || airlineCode
    const inboundAirlineName = (
      inboundRawAirline
      && !looksLikeIataCode(inboundRawAirline)
      && !looksLikePlaceholderAirline(inboundRawAirline)
    ) ? inboundRawAirline : (IATA_TO_NAME[inboundAirlineCode] || airlineName)

    inbound = {
      origin: (inboundFirst.origin || '').toUpperCase(),
      destination: (inboundLast.destination || '').toUpperCase(),
      departure_time: inboundDeparture,
      arrival_time: inboundArrival,
      duration_minutes: inboundDuration,
      stops: inboundRaw.stopovers ?? Math.max(0, inboundSegments.length - 1),
      airline: inboundAirlineName,
      airline_code: inboundAirlineCode,
      segments: inboundSegments.length > 1 ? normalizeSegments(inboundSegments, inboundAirlineName, inboundAirlineCode) : undefined,
    }
  }

  const tripBreakdown = buildTripBreakdown(raw, airlineName, airlineCode, inbound)
  const bookingOptions = buildBookingOptions(raw, tripBreakdown)
  const isCombo = Boolean(
    raw.conditions?.combo_type === 'virtual_interlining'
    || (typeof raw.source === 'string' && raw.source.startsWith('combo:')),
  )

  return hydrateTrustedOffer({
    id: raw.id || `wo_${idx}_${Math.random().toString(36).slice(2, 8)}`,
    price: Math.round((raw.price || 0) * 100) / 100,
    google_flights_price: typeof raw.google_flights_price === 'number'
      ? Math.round(raw.google_flights_price * 100) / 100
      : undefined,
    source: parseStringValue(raw.source),
    currency: raw.currency || 'EUR',
    airline: airlineName,
    airline_code: airlineCode,
    origin,
    origin_name: raw.origin_name || first.origin_name || origin,
    destination,
    destination_name: raw.destination_name || last.destination_name || destination,
    departure_time: departure,
    arrival_time: arrival,
    duration_minutes: durationMinutes,
    stops: outbound.stopovers ?? Math.max(0, segments.length - 1),
    flight_number: normalizeFlightNumber(first.flight_no || first.flight_number || '', airlineCode),
    segments: normalizedSegments.length > 1 ? normalizedSegments : undefined,
    inbound,
    booking_url: raw.booking_url || bookingOptions?.[0]?.booking_url || undefined,
    is_combo: isCombo,
    booking_options: bookingOptions,
    trip_breakdown: tripBreakdown,
    ancillaries: buildAncillaries(raw),
  })
}

export function toPublicOffer(offer: TrustedOffer): PublicOffer {
  const { booking_url: _bookingUrl, booking_options: _bookingOptions, ...publicOffer } = offer
  return {
    ...publicOffer,
    offer_ref: encodeOfferSnapshot(offer),
  }
}

export function getDemoOffer(offerId: string): TrustedOffer | null {
  if (offerId === DEMO_OFFER.id) return DEMO_OFFER
  if (offerId === HIGH_VALUE_DEMO.id) return HIGH_VALUE_DEMO
  return null
}

export async function getTrustedOffer(
  offerId: string,
  searchId?: string | null,
  snapshotRef?: string | null,
): Promise<TrustedOffer | null> {
  const demoOffer = getDemoOffer(offerId)
  if (demoOffer) return demoOffer

  const cached = getCachedOffer<TrustedOffer>(offerId, searchId || undefined)
  if (cached) return hydrateTrustedOffer(cached)

  const snapshotOffer = decodeOfferSnapshot(snapshotRef)
  if (snapshotOffer?.id === offerId) {
    cacheOffers([snapshotOffer], searchId || undefined)
    return snapshotOffer
  }

  if (!searchId || !searchId.startsWith('ws_')) {
    return null
  }

  const response = await fetch(`${FSW_URL}/web-status/${searchId}`, {
    headers: { Authorization: `Bearer ${FSW_SECRET}` },
    signal: AbortSignal.timeout(6_000),
    cache: 'no-store',
  })

  if (!response.ok) {
    return null
  }

  const data = await response.json()
  const rawOffers: any[] = data.offers || []
  const trustedOffers = applyGoogleFlightsBaseline(
    rawOffers,
    rawOffers.map((rawOffer: any, idx: number) => normalizeTrustedOffer(rawOffer, idx)),
    data.google_flights_price,
  )
  if (trustedOffers.length > 0) {
    cacheOffers(trustedOffers, searchId)
  }

  return trustedOffers.find((offer) => offer.id === offerId) || null
}