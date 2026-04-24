import { NextRequest, NextResponse } from 'next/server'
import { getCachedOffer, cacheOffers } from '../../../../lib/offer-cache'

const FSW_URL = process.env.FSW_URL || 'https://flight-search-worker-qryvus4jia-uc.a.run.app'
const FSW_SECRET = process.env.FSW_SECRET || ''

function normalizeWebOffer(raw: any) {
  const ob = raw.outbound || {}
  const segs: any[] = ob.segments || []
  const first = segs[0] || {}
  const last = segs[segs.length - 1] || {}
  const origin = (first.origin || raw.origin || '').toUpperCase()
  const destination = (last.destination || raw.destination || '').toUpperCase()
  const departure = first.departure || first.departure_time || raw.departure_time || ''
  const arrival = last.arrival || last.arrival_time || raw.arrival_time || ''
  let durationMins = 0
  if (departure && arrival) {
    durationMins = Math.round((new Date(arrival).getTime() - new Date(departure).getTime()) / 60000)
  }
  const airlines: string[] = raw.airlines || []
  const airlineName = airlines[0] || first.airline || first.carrier_name || raw.airline || 'Unknown'
  const flightNo: string = first.flight_no || first.flight_number || ''
  const airlineCode = raw.airline_code || flightNo.replace(/\d.*/, '').toUpperCase().slice(0, 2) || '??'
  return {
    id: raw.id,
    price: Math.round((raw.price || 0) * 100) / 100,
    currency: raw.currency || 'EUR',
    airline: airlineName,
    airline_code: airlineCode,
    origin,
    origin_name: raw.origin_name || first.origin_name || origin,
    destination,
    destination_name: raw.destination_name || last.destination_name || destination,
    departure_time: departure,
    arrival_time: arrival,
    duration_minutes: durationMins,
    stops: ob.stopovers ?? Math.max(0, segs.length - 1),
    flight_number: flightNo,
    booking_url: raw.booking_url,
  }
}

// Mock offer store — in production this would be Redis/DB
// Generates a deterministic offer for any offerId

const AIRLINES = [
  { name: 'Ryanair', code: 'FR', domain: 'ryanair.com' },
  { name: 'Wizz Air', code: 'W6', domain: 'wizzair.com' },
  { name: 'EasyJet', code: 'U2', domain: 'easyjet.com' },
  { name: 'Vueling', code: 'VY', domain: 'vueling.com' },
  { name: 'British Airways', code: 'BA', domain: 'britishairways.com' },
  { name: 'Iberia', code: 'IB', domain: 'iberia.com' },
  { name: 'Norwegian', code: 'DY', domain: 'norwegian.com' },
  { name: 'TAP Portugal', code: 'TP', domain: 'flytap.com' },
]

function seededRandom(seed: number) {
  let x = Math.sin(seed) * 10000
  return x - Math.floor(x)
}

function generateOffer(offerId: string) {
  // Deterministic seed from offerId string
  let seed = 0
  for (let i = 0; i < offerId.length; i++) seed += offerId.charCodeAt(i)

  const airlineIdx = Math.floor(seededRandom(seed + 1) * AIRLINES.length)
  const airline = AIRLINES[airlineIdx]
  const price = Math.round(29 + seededRandom(seed + 2) * 280)
  const depHour = 6 + Math.floor(seededRandom(seed + 3) * 14)
  const depMin = Math.floor(seededRandom(seed + 4) * 60)
  const durationMins = 115 + Math.floor(seededRandom(seed + 5) * 180)
  const stops = seededRandom(seed + 6) > 0.65 ? 1 : 0
  const flightNum = `${airline.code}${1000 + Math.floor(seededRandom(seed + 7) * 8000)}`

  const baseDate = new Date()
  baseDate.setDate(baseDate.getDate() + 7 + Math.floor(seededRandom(seed + 8) * 30))
  baseDate.setHours(depHour, depMin, 0, 0)

  const arrDate = new Date(baseDate)
  arrDate.setMinutes(arrDate.getMinutes() + durationMins)

  return {
    id: offerId,
    price,
    currency: '€',
    airline: airline.name,
    airline_code: airline.code,
    origin: 'STN',
    origin_name: 'London Stansted',
    destination: 'BCN',
    destination_name: 'Barcelona El Prat',
    departure_time: baseDate.toISOString(),
    arrival_time: arrDate.toISOString(),
    duration_minutes: durationMins,
    stops,
    flight_number: flightNum,
    // The booking URL — revealed only after unlock in the real app
    booking_url: `https://www.${airline.domain}/select?from=STN&to=BCN&date=${baseDate.toISOString().split('T')[0]}&price=${price}`,
  }
}

// Special demo offer with fixed values for consistent testing
const DEMO_OFFER = {
  id: 'demo-offer-1',
  price: 29,
  currency: '€',
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

const HIGH_VALUE_DEMO = {
  id: 'demo-offer-expensive',
  price: 2499,
  currency: '€',
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

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ offerId: string }> }
) {
  const { offerId } = await params

  if (offerId === 'demo-offer-1') {
    return NextResponse.json(DEMO_OFFER)
  }
  if (offerId === 'demo-offer-expensive') {
    return NextResponse.json(HIGH_VALUE_DEMO)
  }

  // 1. Check module-level cache populated by /api/results/[searchId]
  //    This avoids a second FSW round-trip that could land on a different
  //    Cloud Run instance (which would have no in-memory search state).
  const cached = getCachedOffer(offerId)
  if (cached) {
    return NextResponse.json(cached)
  }

  // 2. Real offer: try to look up from FSW in-memory store
  if (offerId.startsWith('wo_') || offerId.startsWith('ws_')) {
    try {
      const res = await fetch(`${FSW_URL}/web-offer/${offerId}`, {
        headers: { 'Authorization': `Bearer ${FSW_SECRET}` },
        signal: AbortSignal.timeout(5_000),
        cache: 'no-store',
      })
      if (res.ok) {
        const raw = await res.json()
        return NextResponse.json(normalizeWebOffer(raw))
      }
    } catch (err) {
      console.error('Offer fetch error:', err)
    }

    // 3. FSW direct lookup failed — try fetching the full search result by
    //    searchId (the ?from= param) and finding the offer there.
    const from = request.nextUrl.searchParams.get('from')
    if (from && from.startsWith('ws_')) {
      try {
        const res = await fetch(`${FSW_URL}/web-status/${from}`, {
          headers: { 'Authorization': `Bearer ${FSW_SECRET}` },
          signal: AbortSignal.timeout(6_000),
          cache: 'no-store',
        })
        if (res.ok) {
          const data = await res.json()
          const offers: any[] = data.offers || []
          // Cache these for future lookups
          if (offers.length > 0) cacheOffers(offers)
          const match = offers.find((o: any) => o.id === offerId)
          if (match) return NextResponse.json(normalizeWebOffer(match))
        }
      } catch (err) {
        console.error('Offer search-fallback error:', err)
      }
    }
  }

  // 4. Last resort: deterministic mock (only for demo/dev)
  return NextResponse.json(generateOffer(offerId))
}
