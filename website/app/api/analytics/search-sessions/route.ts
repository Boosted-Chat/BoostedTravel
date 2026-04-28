import { NextRequest, NextResponse } from 'next/server'

const ANALYTICS_API_BASE = (
  process.env.LETSFG_ANALYTICS_API_URL || 'https://letsfg-api-876385716101.us-central1.run.app'
).replace(/\/$/, '')

function debugHeaders(extra?: Record<string, string>) {
  return {
    'X-Letsfg-Analytics-Base': ANALYTICS_API_BASE,
    ...(extra || {}),
  }
}

export async function POST(request: NextRequest) {
  let bodyText = ''

  try {
    bodyText = await request.text()
    if (!bodyText) {
      return NextResponse.json({ error: 'Missing request body' }, { status: 400 })
    }

    const response = await fetch(`${ANALYTICS_API_BASE}/api/v1/analytics/search-sessions/upsert`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Origin': 'https://letsfg.co',
        'Referer': 'https://letsfg.co/',
        'User-Agent': request.headers.get('user-agent') || 'Mozilla/5.0 (compatible; LetsFG Website/1.0; +https://letsfg.co)',
      },
      body: bodyText,
      cache: 'no-store',
      signal: AbortSignal.timeout(4000),
    })

    if (!response.ok) {
      return NextResponse.json(
        { error: 'Analytics upstream failed' },
        {
          status: 502,
          headers: debugHeaders({
            'X-Letsfg-Analytics-Upstream-Status': String(response.status),
            'X-Letsfg-Analytics-Upstream-Url': response.url,
          }),
        },
      )
    }

    const data = await response.json()
    return NextResponse.json(data, {
      headers: debugHeaders({
        'X-Letsfg-Analytics-Upstream-Status': String(response.status),
        'X-Letsfg-Analytics-Upstream-Url': response.url,
      }),
    })
  } catch {
    return NextResponse.json(
      { error: 'Analytics proxy failed' },
      { status: 502, headers: debugHeaders() },
    )
  }
}