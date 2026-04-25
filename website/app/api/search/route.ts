import { NextRequest, NextResponse } from 'next/server'
import { recordLocalSearch } from '../../lib/stats'
import { parseNLQuery } from '../../lib/searchParsing'

const FSW_URL = process.env.FSW_URL || 'https://flight-search-worker-qryvus4jia-uc.a.run.app'
const FSW_SECRET = process.env.FSW_SECRET || ''
const WEBSITE_SEARCH_LIMIT = 500

// ── POST /api/search ─────────────────────────────────────────────────────────

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()

    let origin: string | undefined
    let originName: string | undefined
    let destination: string | undefined
    let destinationName: string | undefined
    let dateFrom: string | undefined
    let returnDate: string | undefined
    const adults = Math.max(1, parseInt(body.adults ?? '1', 10) || 1)
    const currency = (body.currency || 'EUR').toUpperCase()

    if (body.origin && body.destination && body.date_from) {
      origin = (body.origin as string).toUpperCase().trim()
      originName = body.origin_name || origin
      destination = (body.destination as string).toUpperCase().trim()
      destinationName = body.destination_name || destination
      dateFrom = body.date_from
      returnDate = body.return_date || undefined
    } else if (body.query) {
      const parsed = parseNLQuery(body.query)
      origin = parsed.origin
      originName = parsed.origin_name
      destination = parsed.destination
      destinationName = parsed.destination_name
      dateFrom = parsed.date
      returnDate = parsed.return_date || undefined
    } else {
      return NextResponse.json({ error: 'Provide either query or origin/destination/date_from' }, { status: 400 })
    }

    if (!origin || !destination) {
      return NextResponse.json({ error: 'Could not determine origin or destination.' }, { status: 400 })
    }

    recordLocalSearch(0)

    const fswRes = await fetch(`${FSW_URL}/web-search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${FSW_SECRET}` },
      body: JSON.stringify({ origin, destination, date_from: dateFrom, return_date: returnDate, adults, currency, limit: WEBSITE_SEARCH_LIMIT }),
      signal: AbortSignal.timeout(10_000),
    })

    if (!fswRes.ok) {
      const err = await fswRes.text()
      console.error('FSW web-search error:', fswRes.status, err)
      return NextResponse.json({ error: 'Search service unavailable' }, { status: 502 })
    }

    const { search_id } = await fswRes.json()

    return NextResponse.json({ search_id, status: 'searching', parsed: { origin, origin_name: originName, destination, destination_name: destinationName, date: dateFrom, return_date: returnDate } })
  } catch (error) {
    console.error('Search error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
