/**
 * Module-level offer cache for the Next.js server process.
 *
 * When /api/results/[searchId] fetches completed offers from FSW, it stores
 * them here. When /api/offer/[offerId] needs an offer it checks here first,
 * avoiding a second FSW round-trip that could land on a different Cloud Run
 * instance (which would have no in-memory search state).
 *
 * TTL matches FSW's _WEB_SEARCH_TTL (20 min).
 */

const _cache = new Map<string, { offer: Record<string, unknown>; expiresAt: number }>()
const TTL_MS = 20 * 60 * 1000 // 20 minutes

export function cacheOffers(offers: Record<string, unknown>[]): void {
  const expiresAt = Date.now() + TTL_MS
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
