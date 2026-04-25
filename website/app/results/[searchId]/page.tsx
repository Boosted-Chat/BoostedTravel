import { Metadata } from 'next'
import { notFound } from 'next/navigation'
import SearchPageClient from './SearchPageClient'

// Types for our search results
interface FlightOffer {
  id: string
  price: number
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
}

interface SearchResult {
  search_id: string
  status: 'searching' | 'completed' | 'expired'
  query: string
  parsed: {
    origin?: string
    origin_name?: string
    destination?: string
    destination_name?: string
    date?: string
    return_date?: string
    passengers?: number
    cabin?: string
  }
  progress?: {
    checked: number
    total: number
    found: number
  }
  offers?: FlightOffer[]
  searched_at?: string
  expires_at?: string
}

const API_BASE = process.env.API_URL || 'http://localhost:3000'

// Single-shot fetch — used only for generateMetadata (fast, no blocking)
async function getSearchResults(searchId: string): Promise<SearchResult | null> {
  try {
    const res = await fetch(`${API_BASE}/api/results/${searchId}`, { cache: 'no-store' })
    if (!res.ok) return null
    return res.json()
  } catch {
    return null
  }
}

// Blocking poll — used by the page itself. Loops every 5 s until the search
// completes or the deadline is reached. This makes the page render full HTML
// with all offers server-side, so ChatGPT / any no-JS client reads real results.
// Humans get the same HTML with our UI on top. JS polling in SearchPageClient
// stays as a fallback but is never needed for a completed search.
async function pollUntilDone(
  searchId: string,
  maxWaitMs = 8 * 60 * 1000, // 8 min — covers full-mode searches (2–6 min)
): Promise<SearchResult | null> {
  const deadline = Date.now() + maxWaitMs
  while (true) {
    const result = await getSearchResults(searchId)
    if (!result) return null                    // 404 — search doesn't exist
    if (result.status !== 'searching') return result // completed or expired

    const remaining = deadline - Date.now()
    if (remaining <= 5_000) return result       // timed out — return as-is (JS takes over)

    await new Promise(r => setTimeout(r, 5_000))
  }
}

// Generate metadata for SEO and social sharing
export async function generateMetadata({ params }: { params: Promise<{ searchId: string }> }): Promise<Metadata> {
  const { searchId } = await params
  const result = await getSearchResults(searchId)
  
  if (!result) {
    return { title: 'Search not found — LetsFG' }
  }
  
  const { parsed, offers, status } = result
  
  if (status === 'searching') {
    return {
      title: `Searching flights ${parsed.origin || ''} → ${parsed.destination || ''} — LetsFG`,
      description: `Finding the cheapest flights. Checking 180+ airlines...`,
    }
  }
  
  if (status === 'expired') {
    return {
      title: `Search expired — LetsFG`,
      description: `These results have expired. Search again for current prices.`,
    }
  }
  
  const cheapest = offers?.[0]
  const title = cheapest 
    ? `${offers?.length} flights ${parsed.origin_name || parsed.origin} → ${parsed.destination_name || parsed.destination} from ${cheapest.currency}${cheapest.price}`
    : `Flights ${parsed.origin} → ${parsed.destination}`
  
  return {
    title: `${title} — LetsFG`,
    description: `Found ${offers?.length || 0} flights. Cheapest: ${cheapest?.currency}${cheapest?.price} on ${cheapest?.airline}. Zero markup, raw airline prices.`,
  }
}

export default async function ResultsPage({ params, searchParams }: { params: Promise<{ searchId: string }>; searchParams: Promise<{ sort?: string; filter?: string; started?: string }> }) {
  const { searchId } = await params
  const sp = await searchParams
  // Block server-side until the search completes (or 8 min timeout).
  // This ensures the HTML delivered to the client — human or bot — always
  // contains the full offer table, not a loading skeleton.
  const result = await pollUntilDone(searchId)

  if (!result) {
    notFound()
  }

  const { status, query, parsed, progress, offers, searched_at, expires_at } = result

  const isSearching = status === 'searching'
  const routeLabel = [parsed.origin_name || parsed.origin, parsed.destination_name || parsed.destination]
    .filter(Boolean)
    .join(' → ')

  const allOffers = Array.from(
    new Map((offers || []).map(o => [o.id, o])).values()
  )

  // JSON-LD for SEO (server-rendered once; not updated client-side)
  const jsonLd = isSearching
    ? {
        '@context': 'https://schema.org',
        '@type': 'SearchResultsPage',
        name: `LetsFG — Searching flights ${routeLabel || query}`,
        description: `Searching 180+ airlines. ${progress?.checked || 0} of ${progress?.total || 180} checked. ${progress?.found || 0} results found so far.`,
        url: `https://letsfg.co/results/${searchId}`,
      }
    : status === 'completed' && offers
    ? {
        '@context': 'https://schema.org',
        '@type': 'ItemList',
        name: `Flights ${routeLabel}`,
        numberOfItems: offers.length,
        itemListElement: offers.slice(0, 10).map((offer, i) => ({
          '@type': 'ListItem',
          position: i + 1,
          item: {
            '@type': 'Product',
            name: `${offer.airline} ${offer.origin}→${offer.destination}`,
            offers: {
              '@type': 'Offer',
              price: String(offer.price),
              priceCurrency: offer.currency,
              availability: 'https://schema.org/InStock',
            },
          },
        })),
      }
    : null

  return (
    <>
      {jsonLd && (
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
        />
      )}

      {/* SearchPageClient owns all dynamic rendering (searching ↔ results transition).
          It polls /api/results/{searchId} every 5 s on the client — no router.refresh()
          so SearchingTasks is never remounted and its animation state is always preserved. */}
      <SearchPageClient
        searchId={searchId}
        query={query}
        parsed={parsed}
        initialStatus={status}
        initialProgress={progress}
        initialOffers={allOffers}
        searchedAt={searched_at || sp?.started}
        expiresAt={expires_at}
      />
    </>
  )
}
