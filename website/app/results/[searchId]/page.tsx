import { Metadata } from 'next'
import { cookies, headers } from 'next/headers'
import { notFound } from 'next/navigation'
import SearchPageClient from './SearchPageClient'
import { LETSFG_CURRENCY_COOKIE, resolveSearchCurrency } from '../../../lib/currency-preference'
import { getOfferDisplayTotalPrice } from '../../../lib/display-price'
import { formatCurrencyAmount } from '../../../lib/user-currency'
import { appendProbeParam, getTrackingSearchId, isProbeModeValue } from '../../../lib/probe-mode'
import { detectPreferredCurrency } from '../../../lib/user-currency'

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

const SITE_URL = process.env.NEXT_PUBLIC_SITE_URL || 'https://letsfg.co'

function buildFallbackSearchQuery(parsed: SearchResult['parsed']): string {
  const origin = parsed.origin || parsed.origin_name
  const destination = parsed.destination || parsed.destination_name

  if (!origin || !destination) {
    return ''
  }

  const parts = [`${origin} to ${destination}`]

  if (parsed.date) parts.push(parsed.date)
  if (parsed.return_date) parts.push(`return ${parsed.return_date}`)

  return parts.join(' ').trim()
}

async function getApiBase(): Promise<string> {
  const explicitBase = process.env.API_URL?.trim()
  if (explicitBase) {
    return explicitBase.replace(/\/$/, '')
  }

  const headerList = await headers()
  const host = headerList.get('x-forwarded-host') || headerList.get('host')
  if (host) {
    const proto = headerList.get('x-forwarded-proto') || (host.includes('localhost') ? 'http' : 'https')
    return `${proto}://${host}`
  }

  return SITE_URL
}

// Single-shot fetch — used only for generateMetadata (fast, no blocking)
async function getSearchResults(searchId: string, isProbe: boolean): Promise<SearchResult | null> {
  try {
    const apiBase = await getApiBase()
    const url = new URL(`/api/results/${searchId}`, apiBase)
    appendProbeParam(url.searchParams, isProbe)
    const res = await fetch(url.toString(), { cache: 'no-store' })
    if (!res.ok) return null
    return res.json()
  } catch {
    return null
  }
}

async function resolveRequestCurrency(queryParam?: string): Promise<string> {
  const requestHeaders = await headers()
  const cookieStore = await cookies()

  return resolveSearchCurrency({
    queryParam: queryParam?.trim(),
    cookieValue: cookieStore.get(LETSFG_CURRENCY_COOKIE)?.value,
    fallback: detectPreferredCurrency(requestHeaders),
  })
}

// Generate metadata for SEO and social sharing
export async function generateMetadata({
  params,
  searchParams,
}: {
  params: Promise<{ searchId: string }>
  searchParams: Promise<{ probe?: string; cur?: string }>
}): Promise<Metadata> {
  const { searchId } = await params
  const sp = await searchParams
  const isProbe = isProbeModeValue(sp?.probe)
  const displayCurrency = await resolveRequestCurrency(sp?.cur)
  const result = await getSearchResults(searchId, isProbe)
  
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
  
  const cheapest = offers?.reduce((best, offer) => {
    if (!best) return offer
    return getOfferDisplayTotalPrice(offer, displayCurrency) < getOfferDisplayTotalPrice(best, displayCurrency) ? offer : best
  }, offers?.[0])
  const cheapestDisplayPrice = cheapest ? getOfferDisplayTotalPrice(cheapest, displayCurrency) : null
  const cheapestFormattedPrice = cheapestDisplayPrice === null ? null : formatCurrencyAmount(cheapestDisplayPrice, displayCurrency)
  const title = cheapest 
    ? `${offers?.length} flights ${parsed.origin_name || parsed.origin} → ${parsed.destination_name || parsed.destination} from ${cheapestFormattedPrice}`
    : `Flights ${parsed.origin} → ${parsed.destination}`
  
  return {
    title: `${title} — LetsFG`,
    description: `Found ${offers?.length || 0} flights. Cheapest: ${cheapestFormattedPrice} on ${cheapest?.airline}. Zero markup, raw airline prices.`,
  }
}

export default async function ResultsPage({ params, searchParams }: { params: Promise<{ searchId: string }>; searchParams: Promise<{ sort?: string; filter?: string; started?: string; probe?: string; cur?: string; q?: string }> }) {
  const { searchId } = await params
  const sp = await searchParams
  const isProbe = isProbeModeValue(sp?.probe)
  const initialCurrency = await resolveRequestCurrency(sp?.cur)
  const trackingSearchId = getTrackingSearchId(searchId, isProbe)
  // Render immediately with the current snapshot and let SearchPageClient poll.
  // Blocking here traps users on loading.tsx while the server waits.
  const result = await getSearchResults(searchId, isProbe)

  if (!result) {
    notFound()
  }

  const { status, query: resultQuery, parsed, progress, offers, searched_at, expires_at } = result
  const query = resultQuery?.trim() || sp?.q?.trim() || buildFallbackSearchQuery(parsed)

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
              price: String(Math.round(getOfferDisplayTotalPrice(offer, initialCurrency))),
              priceCurrency: initialCurrency,
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
        trackingSearchId={trackingSearchId}
        isTestSearch={isProbe}
        initialCurrency={initialCurrency}
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
