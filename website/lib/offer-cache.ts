/**
 * Module-level offer cache for the Next.js server process.
 *
 * When /api/results/[searchId] fetches completed offers from FSW, it stores
 * them here. When /api/offer/[offerId] needs an offer it checks here first,
 * avoiding a second FSW round-trip that could land on a different Cloud Run
 * instance (which would have no in-memory search state).
 *
 * OFFER_TTL_MS  — individual offer lookup TTL (matches FSW _WEB_SEARCH_TTL)
 * SEARCH_TTL_MS — full search result TTL — kept much longer so that a
 *   specific results URL stays usable after FSW has expired the search.
 *   Covers: opening on another device, going back after viewing an offer, etc.
 */

const _cache = new Map<string, { offer: Record<string, unknown>; expiresAt: number }>()
const OFFER_TTL_MS = 20 * 60 * 1000   // 20 minutes
const TTL_MS = OFFER_TTL_MS            // alias kept for existing call-sites

// ── Search-result cache (keyed by searchId) ───────────────────────────────────
interface SearchResultEntry {
  result: Record<string, unknown>
  expiresAt: number
}
const _searchCache = new Map<string, SearchResultEntry>()
const SEARCH_TTL_MS = 30 * 60 * 1000  // 30 minutes

export function cacheOffers(offers: Record<string, unknown>[]): void {
  const expiresAt = Date.now() + OFFER_TTL_MS
  for (const offer of offers) {
    const id = offer.id as string | undefined
    if (id) _cache.set(id, { offer, expiresAt })
  }
  // Opportunistic cleanup of expired entries
  const now = Date.now()
  for (const [key, entry] of _cache) {
    if (now > entry.expiresAt) _cache.delete(key)
  }
}

export function getCachedOffer(offerId: string): Record<string, unknown> | null {
  const entry = _cache.get(offerId)
  if (!entry) return null
  if (Date.now() > entry.expiresAt) {
    _cache.delete(offerId)
    return null
  }
  return entry.offer
}

// ── Search-result cache API ───────────────────────────────────────────────────

/** Persist a completed search result so the URL stays usable after FSW expiry. */
export function cacheSearchResult(searchId: string, result: Record<string, unknown>): void {
  _searchCache.set(searchId, { result, expiresAt: Date.now() + SEARCH_TTL_MS })
  // Opportunistic cleanup
  const now = Date.now()
  for (const [key, entry] of _searchCache) {
    if (now > entry.expiresAt) _searchCache.delete(key)
  }
}

/** Returns the cached search result if it exists and hasn't expired. */
export function getCachedSearchResult(searchId: string): Record<string, unknown> | null {
  const entry = _searchCache.get(searchId)
  if (!entry) return null
  if (Date.now() > entry.expiresAt) {
    _searchCache.delete(searchId)
    return null
  }
  return entry.result
}
