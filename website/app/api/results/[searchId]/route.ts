import { NextRequest, NextResponse } from 'next/server'
import { cacheOffers } from '../../../../lib/offer-cache'
import { cacheCompletedSearchResult, getCachedSearchResult } from '../../../../lib/results-cache'
import { getOfferKnownTotalPrice } from '../../../../lib/offer-pricing'
import { getTrackedSourcePath, getTrackingSearchId, isProbeModeValue } from '../../../../lib/probe-mode'
import { applyGoogleFlightsBaseline, normalizeTrustedOffer, toPublicOffer } from '../../../../lib/trusted-offer'
import { upsertSearchSessionServer } from '../../../../lib/search-session-analytics-server'

const FSW_URL = process.env.FSW_URL || 'https://flight-search-worker-qryvus4jia-uc.a.run.app'
const FSW_SECRET = process.env.FSW_SECRET || ''

// ── FSW offer → website offer normalizer ─────────────────────────────────────
// FSW offers use the SDK format: { price, currency, airlines[], outbound: { segments[], stopovers }, inbound?, source }
// Segments have: { origin, destination, departure, arrival, flight_no, airline? }

// ── Airline resolution helpers ────────────────────────────────────────────────
// IATA_TO_NAME: subset for the most common airlines (avoids importing full map)
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

function extractIataFromFlightNo(flightNo: string): string {
  const m = flightNo.match(/^([A-Z]{2}|[A-Z]\d|\d[A-Z])/i)
  return m ? m[1].toUpperCase() : ''
}

function resolveAirlineFromRaw(raw: any, first: any): { airlineName: string; airlineCode: string } {
  const airlines: string[] = raw.airlines || []
  const rawName: string = airlines[0] || first.airline || first.carrier_name || ''
  const flightNo: string = first.flight_no || first.flight_number || ''

  // If 'name' is actually a 2-char IATA code (FSW returned codes not names)
  if (rawName && /^[A-Z0-9]{2}$/i.test(rawName.trim())) {
    const code = rawName.toUpperCase()
    const name = IATA_TO_NAME[code] || code
    return { airlineName: name, airlineCode: code }
  }

  const airlineName = rawName || 'Unknown'
  const airlineCode = raw.airline_code || extractIataFromFlightNo(flightNo) || '??'
  return { airlineName, airlineCode }
}

function normalizeOffer(raw: any, idx: number): any {
  const ob = raw.outbound || {}
  const segs: any[] = ob.segments || []
  const first = segs[0] || {}
  const last = segs[segs.length - 1] || {}

  const origin = (first.origin || '').toUpperCase()
  const destination = (last.destination || '').toUpperCase()
  const departure = first.departure || first.departure_time || ''
  const arrival = last.arrival || last.arrival_time || ''

  // Duration in minutes
  let durationMins = 0
  if (departure && arrival) {
    durationMins = Math.round((new Date(arrival).getTime() - new Date(departure).getTime()) / 60000)
  }

  const { airlineName, airlineCode } = resolveAirlineFromRaw(raw, first)

  // Offer ID: use assigned ID from FSW, or generate fallback
  const id = raw.id || `wo_${idx}_${Math.random().toString(36).slice(2, 8)}`

  // Normalize segments for the UI
  const normSegs = segs.map((s: any) => {
    const sDep = s.departure || s.departure_time || ''
    const sArr = s.arrival || s.arrival_time || ''
    const sDur = sDep && sArr
      ? Math.round((new Date(sArr).getTime() - new Date(sDep).getTime()) / 60000)
      : 0
    return {
      airline: s.airline || s.carrier_name || airlineName,
      airline_code: extractIataFromFlightNo(s.flight_no || '') || airlineCode,
      flight_number: s.flight_no || s.flight_number || '',
      origin: (s.origin || '').toUpperCase(),
      destination: (s.destination || '').toUpperCase(),
      departure_time: sDep,
      arrival_time: sArr,
      duration_minutes: sDur,
    }
  })

  // Normalize inbound leg if present (round-trip)
  let inbound: any | undefined
  const ibRaw = raw.inbound
  if (ibRaw && ibRaw.segments?.length) {
    const ibSegs: any[] = ibRaw.segments
    const ibFirst = ibSegs[0] || {}
    const ibLast = ibSegs[ibSegs.length - 1] || ibFirst
    const ibDep = ibFirst.departure || ibFirst.departure_time || ''
    const ibArr = ibLast.arrival || ibLast.arrival_time || ''
    let ibDurMins = 0
    if (ibDep && ibArr) {
      ibDurMins = Math.round((new Date(ibArr).getTime() - new Date(ibDep).getTime()) / 60000)
    }
    const ibAirlineName = ibFirst.airline || ibFirst.carrier_name || airlineName
    const ibAirlineCode = extractIataFromFlightNo(ibFirst.flight_no || '') || airlineCode
    const ibNormSegs = ibSegs.map((s: any) => ({
      airline: s.airline || s.carrier_name || ibAirlineName,
      airline_code: extractIataFromFlightNo(s.flight_no || '') || ibAirlineCode,
      flight_number: s.flight_no || s.flight_number || '',
      origin: (s.origin || '').toUpperCase(),
      destination: (s.destination || '').toUpperCase(),
      departure_time: s.departure || s.departure_time || '',
      arrival_time: s.arrival || s.arrival_time || '',
    }))
    inbound = {
      origin: (ibFirst.origin || '').toUpperCase(),
      destination: (ibLast.destination || '').toUpperCase(),
      departure_time: ibDep,
      arrival_time: ibArr,
      duration_minutes: ibDurMins,
      stops: ibRaw.stopovers ?? Math.max(0, ibSegs.length - 1),
      airline: ibAirlineName,
      airline_code: ibAirlineCode,
      segments: ibNormSegs.length > 1 ? ibNormSegs : undefined,
    }
  }

  return {
    id,
    price: Math.round((raw.price || 0) * 100) / 100,
    google_flights_price: raw.google_flights_price
      ? Math.round(raw.google_flights_price * 100) / 100
      : Math.round((raw.price || 0) * 1.12 * 100) / 100,
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
    segments: normSegs.length > 1 ? normSegs : undefined,
    inbound,
  }
}

// ── Mock data for demo search IDs ─────────────────────────────────────────────

function mockOffer(i: number, origin: string, dest: string, date: string) {
  const AIRLINES = [
    { name: 'Ryanair', code: 'FR' }, { name: 'Wizz Air', code: 'W6' },
    { name: 'EasyJet', code: 'U2' }, { name: 'Vueling', code: 'VY' },
    { name: 'British Airways', code: 'BA' }, { name: 'Iberia', code: 'IB' },
  ]
  const al = AIRLINES[i % AIRLINES.length]
  const price = 29 + i * 14 + Math.floor(Math.random() * 10)
  const dep = new Date(date || new Date(Date.now() + 7 * 86400000).toISOString())
  dep.setHours(6 + (i * 3) % 16, (i * 17) % 60)
  const dur = 90 + (i * 23) % 180
  const arr = new Date(dep.getTime() + dur * 60000)
  return {
    id: `demo_off_${i}`,
    price,
    google_flights_price: Math.round(price * 1.12),
    currency: 'EUR',
    airline: al.name,
    airline_code: al.code,
    origin: origin || 'LON',
    origin_name: 'London',
    destination: dest || 'BCN',
    destination_name: 'Barcelona',
    departure_time: dep.toISOString(),
    arrival_time: arr.toISOString(),
    duration_minutes: dur,
    stops: i % 4 === 0 ? 1 : 0,
  }
}

// ── GET /api/results/[searchId] ───────────────────────────────────────────────

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ searchId: string }> }
) {
  const { searchId } = await params
  const isProbeSearch = isProbeModeValue(request.nextUrl.searchParams.get('probe'))
  const analyticsSearchId = getTrackingSearchId(searchId, isProbeSearch) || searchId
  const analyticsSourcePath = getTrackedSourcePath(`/api/results/${searchId}`, isProbeSearch)

  // Demo stubs for UI testing
  if (searchId === 'demo-loading') {
    return NextResponse.json({
      search_id: 'demo-loading',
      status: 'searching',
      query: 'cheapest flight London to Barcelona next Friday',
      parsed: { origin: 'LON', origin_name: 'London', destination: 'BCN', destination_name: 'Barcelona',
                 date: new Date(Date.now() + 7 * 86400000).toISOString().split('T')[0] },
      progress: { checked: 47, total: 198, found: 12 },
      offers: [],
      total_results: 0,
      searched_at: new Date(Date.now() - 30000).toISOString(),
      expires_at: new Date(Date.now() + 14 * 60000).toISOString(),
    })
  }

  if (searchId === 'demo-completed') {
    const offers = Array.from({ length: 8 }, (_, i) => mockOffer(i, 'LON', 'BCN', ''))
    return NextResponse.json({
      search_id: 'demo-completed',
      status: 'completed',
      query: 'cheapest flight London to Barcelona next Friday',
      parsed: { origin: 'LON', origin_name: 'London', destination: 'BCN', destination_name: 'Barcelona' },
      offers,
      total_results: offers.length,
      searched_at: new Date(Date.now() - 2 * 60000).toISOString(),
      expires_at: new Date(Date.now() + 13 * 60000).toISOString(),
    })
  }

  // Real search: poll FSW
  if (!searchId.startsWith('ws_')) {
    return NextResponse.json({ error: 'Search not found' }, { status: 404 })
  }

  try {
    const res = await fetch(`${FSW_URL}/web-status/${searchId}`, {
      headers: { 'Authorization': `Bearer ${FSW_SECRET}` },
      signal: AbortSignal.timeout(8_000),
      cache: 'no-store',
    })

    if (res.status === 404) {
      const cachedResult = getCachedSearchResult(searchId)
      if (cachedResult) {
        if (isProbeSearch && cachedResult.status === 'completed') {
          const cachedOffers = (Array.isArray(cachedResult.offers) ? cachedResult.offers : []) as Array<{
            id?: string
            airline?: string
            price?: number
            currency?: string
            google_flights_price?: number
            stops?: number
            duration_minutes?: number
          }>
          await upsertSearchSessionServer({
            search_id: analyticsSearchId,
            source_search_id: searchId,
            is_test_search: true,
            source: 'website-results-api-cache',
            source_path: analyticsSourcePath,
            status: 'completed',
            results_count: cachedResult.total_results || cachedOffers.length,
            cheapest_price: cachedResult.cheapest_price,
            google_flights_price: cachedResult.google_flights_price,
            value: cachedResult.value,
            savings_vs_google_flights: cachedResult.savings_vs_google_flights,
            results_preview: cachedOffers.slice(0, 10).map((offer) => ({
              id: offer.id,
              airline: offer.airline,
              price: offer.price,
              currency: offer.currency,
              google_flights_price: offer.google_flights_price,
              stops: offer.stops,
              duration_minutes: offer.duration_minutes,
            })),
            event: {
              type: 'results_materialized',
              at: new Date().toISOString(),
              data: {
                offers_returned: cachedOffers.length,
                cache_hit: true,
              },
            },
          })
        }

        return NextResponse.json(cachedResult)
      }

      return NextResponse.json({
        search_id: searchId,
        status: 'expired',
        parsed: {},
        offers: [],
        total_results: 0,
      })
    }

    if (!res.ok) {
      return NextResponse.json({ error: 'Search service error' }, { status: 502 })
    }

    const data = await res.json()
    const rawOffers: any[] = data.offers || []
    const trustedOffers = applyGoogleFlightsBaseline(
      rawOffers,
      rawOffers.map((offer: any, idx: number) => normalizeTrustedOffer(offer, idx)),
      data.google_flights_price,
    )
    const normalized = trustedOffers.map((offer) => toPublicOffer(offer))
    const cheapestPrice = normalized.length > 0
      ? Math.min(...normalized.map((offer) => getOfferKnownTotalPrice(offer)))
      : undefined
    const googleFlightsPrices = normalized
      .map((offer) => typeof offer.google_flights_price === 'number' ? offer.google_flights_price : null)
      .filter((price): price is number => price !== null)
    const googleFlightsPrice = typeof data.google_flights_price === 'number'
      ? Math.round(data.google_flights_price * 100) / 100
      : (googleFlightsPrices.length > 0 ? Math.min(...googleFlightsPrices) : undefined)
    const diff = typeof googleFlightsPrice === 'number' && typeof cheapestPrice === 'number'
      ? Math.round((googleFlightsPrice - cheapestPrice) * 100) / 100
      : undefined
    const nonNegativeDiff = typeof diff === 'number'
      ? (Math.abs(diff) < 0.005 ? 0 : Math.max(0, diff))
      : undefined

    // Cache completed offers so /api/offer/[offerId] can find them without
    // needing to hit FSW again (which may route to a different instance).
    if (data.status === 'completed' && normalized.length > 0) {
      cacheOffers(trustedOffers, searchId)

      await upsertSearchSessionServer({
        search_id: analyticsSearchId,
        source_search_id: isProbeSearch ? searchId : undefined,
        is_test_search: isProbeSearch || undefined,
        source: 'website-results-api',
        source_path: analyticsSourcePath,
        status: 'completed',
        results_count: normalized.length,
        cheapest_price: cheapestPrice,
        google_flights_price: googleFlightsPrice,
        value: nonNegativeDiff,
        savings_vs_google_flights: nonNegativeDiff,
        results_preview: normalized.slice(0, 10).map((offer) => ({
          id: offer.id,
          airline: offer.airline,
          price: offer.price,
          currency: offer.currency,
          google_flights_price: offer.google_flights_price,
          stops: offer.stops,
          duration_minutes: offer.duration_minutes,
        })),
        event: {
          type: 'results_materialized',
          at: new Date().toISOString(),
          data: {
            offers_returned: normalized.length,
          },
        },
      })
    }

    const now = new Date()
    const createdAgo = data.elapsed_seconds ? data.elapsed_seconds * 1000 : 0
    const result = {
      search_id: searchId,
      status: data.status,
      query: data.query,
      parsed: {
        origin: data.origin,
        origin_name: data.origin_name || data.origin,
        destination: data.destination,
        destination_name: data.destination_name || data.destination,
        date: data.date_from,
        return_date: data.return_date || undefined,
      },
      offers: normalized,
      total_results: normalized.length,
      cheapest_price: cheapestPrice,
      google_flights_price: googleFlightsPrice,
      value: nonNegativeDiff,
      savings_vs_google_flights: nonNegativeDiff,
      elapsed_seconds: data.elapsed_seconds,
      searched_at: new Date(now.getTime() - createdAgo).toISOString(),
      expires_at: new Date(now.getTime() + (data.expires_in_seconds ?? 1200) * 1000).toISOString(),
    }

    if (result.status === 'completed') {
      cacheCompletedSearchResult({
        search_id: result.search_id,
        status: 'completed',
        query: result.query,
        parsed: result.parsed,
        offers: result.offers,
        total_results: result.total_results,
        cheapest_price: result.cheapest_price,
        google_flights_price: result.google_flights_price,
        value: result.value,
        savings_vs_google_flights: result.savings_vs_google_flights,
        searched_at: result.searched_at,
        expires_at: result.expires_at,
      })
    }

    return NextResponse.json(result)
  } catch (err) {
    console.error('Results poll error:', err)
    return NextResponse.json({ error: 'Failed to fetch search status' }, { status: 502 })
  }
}