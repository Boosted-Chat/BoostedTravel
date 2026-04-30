import { NextRequest, NextResponse } from 'next/server'
import { recordLocalSearch } from '../../lib/stats'
import { parseNLQuery } from '../../lib/searchParsing'
import { startWebSearch } from '../../../lib/fsw-search'
import { getTrackedSourcePath, isProbeModeValue } from '../../../lib/probe-mode'

// ── POST /api/search ─────────────────────────────────────────────────────────

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const isProbeSearch = isProbeModeValue(body.probe)

    let origin: string | undefined
    let originName: string | undefined
    let destination: string | undefined
    let destinationName: string | undefined
    let dateFrom: string | undefined
    let returnDate: string | undefined
    const adults = Math.max(1, parseInt(body.adults ?? '1', 10) || 1)
    const currency = (body.currency || 'EUR').toUpperCase()
    let maxStops: number | undefined
    let cabin: string | undefined

    if (body.origin && body.destination && body.date_from) {
      origin = (body.origin as string).toUpperCase().trim()
      originName = body.origin_name || origin
      destination = (body.destination as string).toUpperCase().trim()
      destinationName = body.destination_name || destination
      dateFrom = body.date_from
      returnDate = body.return_date || undefined
      if (body.max_stops !== undefined && body.max_stops !== null && body.max_stops !== '') {
        maxStops = parseInt(body.max_stops, 10)
      }
      cabin = body.cabin ? String(body.cabin).toUpperCase() : undefined
    } else if (body.query) {
      const parsed = parseNLQuery(body.query)
      origin = parsed.origin
      originName = parsed.origin_name
      destination = parsed.destination
      destinationName = parsed.destination_name
      dateFrom = parsed.date
      returnDate = parsed.return_date || undefined
      maxStops = parsed.stops
      cabin = parsed.cabin ? String(parsed.cabin).toUpperCase() : undefined
    } else {
      return NextResponse.json({ error: 'Provide either query or origin/destination/date_from' }, { status: 400 })
    }

    if (!origin || !destination) {
      return NextResponse.json({ error: 'Could not determine origin or destination.' }, { status: 400 })
    }

    recordLocalSearch()

    const { searchId, cache } = await startWebSearch({
      origin,
      destination,
      date_from: dateFrom!,
      return_date: returnDate,
      adults,
      currency,
      ...(maxStops !== undefined ? { max_stops: maxStops } : {}),
      ...(cabin ? { cabin } : {}),
    }, {
      query: typeof body.query === 'string' ? body.query : undefined,
      origin_name: originName,
      destination_name: destinationName,
      source: 'website-api-search',
      source_path: getTrackedSourcePath('/api/search', isProbeSearch),
      is_test_search: isProbeSearch,
    })

    if (!searchId) {
      return NextResponse.json({ error: 'Search service unavailable' }, { status: 502 })
    }

    return NextResponse.json({
      search_id: searchId,
      status: 'searching',
      cache,
      parsed: {
        origin,
        origin_name: originName,
        destination,
        destination_name: destinationName,
        date: dateFrom,
        return_date: returnDate,
        passengers: adults,
        ...(maxStops !== undefined ? { stops: maxStops } : {}),
        ...(cabin ? { cabin } : {}),
      },
    })
  } catch (error) {
    console.error('Search error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
