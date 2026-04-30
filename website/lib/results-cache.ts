import fs from 'node:fs'
import path from 'node:path'

export interface PersistedSearchResult {
  search_id: string
  status: 'completed'
  query?: string
  parsed: Record<string, unknown>
  offers: unknown[]
  total_results: number
  cheapest_price?: number
  google_flights_price?: number
  value?: number
  savings_vs_google_flights?: number
  searched_at?: string
  expires_at?: string
  stored_at: number
}

const RESULTS_TTL_MS = 30 * 24 * 60 * 60 * 1000
const RESULTS_CACHE_FILE = path.join(process.cwd(), '.next', 'cache', 'letsfg-results.json')

let cacheLoaded = false
const resultsCache = new Map<string, PersistedSearchResult>()

function loadCache(): void {
  if (cacheLoaded) return
  cacheLoaded = true

  try {
    const raw = fs.readFileSync(RESULTS_CACHE_FILE, 'utf8')
    const parsed = JSON.parse(raw) as Record<string, PersistedSearchResult>
    const now = Date.now()
    for (const [searchId, result] of Object.entries(parsed || {})) {
      if (!result || typeof result !== 'object') continue
      if (typeof result.stored_at !== 'number' || now - result.stored_at > RESULTS_TTL_MS) continue
      if (result.status !== 'completed' || !Array.isArray(result.offers)) continue
      resultsCache.set(searchId, result)
    }
  } catch {
    // Ignore missing or malformed cache.
  }
}

function persistCache(): void {
  try {
    fs.mkdirSync(path.dirname(RESULTS_CACHE_FILE), { recursive: true })
    const serialized: Record<string, PersistedSearchResult> = {}
    for (const [searchId, result] of resultsCache) {
      serialized[searchId] = result
    }
    fs.writeFileSync(RESULTS_CACHE_FILE, JSON.stringify(serialized), 'utf8')
  } catch {
    // Ignore persistence failures; runtime cache still works.
  }
}

function pruneCache(now = Date.now()): void {
  let changed = false
  for (const [searchId, result] of resultsCache) {
    if (now - result.stored_at > RESULTS_TTL_MS) {
      resultsCache.delete(searchId)
      changed = true
    }
  }
  if (changed) {
    persistCache()
  }
}

export function cacheCompletedSearchResult(result: Omit<PersistedSearchResult, 'stored_at'>): void {
  loadCache()
  resultsCache.set(result.search_id, {
    ...result,
    stored_at: Date.now(),
  })
  pruneCache()
  persistCache()
}

export function getCachedSearchResult(searchId: string): PersistedSearchResult | null {
  loadCache()
  pruneCache()
  return resultsCache.get(searchId) || null
}