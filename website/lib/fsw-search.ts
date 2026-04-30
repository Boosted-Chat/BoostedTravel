const FSW_URL = process.env.FSW_URL || 'https://flight-search-worker-qryvus4jia-uc.a.run.app'
const FSW_SECRET = process.env.FSW_SECRET || ''
const WEBSITE_SEARCH_LIMIT = 500

import { upsertSearchSessionServer } from './search-session-analytics-server'
import { getTrackingSearchId } from './probe-mode'

export interface WebSearchParams {
  origin: string
  destination: string
  date_from: string
  return_date?: string
  adults: number
  currency: string
  max_stops?: number
  cabin?: string
}

export interface WebSearchAnalyticsContext {
  query?: string
  origin_name?: string
  destination_name?: string
  source?: string
  source_path?: string
  referrer_path?: string
  source_search_id?: string
  is_test_search?: boolean
}

export interface StartWebSearchResult {
  searchId: string | null
  cache: 'hit' | 'miss'
}

export async function startWebSearch(
  params: WebSearchParams,
  analytics?: WebSearchAnalyticsContext,
): Promise<StartWebSearchResult> {
  const startedAt = new Date().toISOString()
  const res = await fetch(`${FSW_URL}/web-search`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${FSW_SECRET}`,
    },
    body: JSON.stringify({
      origin: params.origin,
      destination: params.destination,
      date_from: params.date_from,
      return_date: params.return_date,
      adults: params.adults,
      currency: params.currency,
      limit: WEBSITE_SEARCH_LIMIT,
      ...(params.max_stops !== undefined ? { max_stops: params.max_stops } : {}),
      ...(params.cabin ? { cabin: params.cabin } : {}),
    }),
    signal: AbortSignal.timeout(10_000),
    cache: 'no-store',
  })

  if (!res.ok) {
    return { searchId: null, cache: 'miss' }
  }

  const data = await res.json()
  const searchId = typeof data.search_id === 'string' ? data.search_id : null

  if (searchId) {
    const isTestSearch = Boolean(analytics?.is_test_search)
    const analyticsSearchId = getTrackingSearchId(searchId, isTestSearch) || searchId

    await upsertSearchSessionServer({
      search_id: analyticsSearchId,
      query: analytics?.query,
      origin: params.origin,
      origin_name: analytics?.origin_name || params.origin,
      destination: params.destination,
      destination_name: analytics?.destination_name || params.destination,
      route: `${params.origin}-${params.destination}`,
      date_from: params.date_from,
      return_date: params.return_date,
      adults: params.adults,
      currency: params.currency,
      max_stops: params.max_stops,
      cabin: params.cabin,
      source: analytics?.source || 'website',
      source_path: analytics?.source_path,
      referrer_path: analytics?.referrer_path,
      source_search_id: analytics?.source_search_id || (isTestSearch ? searchId : undefined),
      is_test_search: isTestSearch || undefined,
      status: 'searching',
      cache_hit: Boolean(data.cache_hit),
      search_started_at: startedAt,
    })
  }

  return {
    searchId,
    cache: data.cache_hit ? 'hit' : 'miss',
  }
}