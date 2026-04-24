import { Metadata } from 'next'
import { notFound } from 'next/navigation'
import Link from 'next/link'
import Image from 'next/image'
import GlobeButton from '../../globe-button'
import ResultsSearchForm from '../ResultsSearchForm'
import SearchingTasks from './SearchingTasks'
import ResultsPanel from './ResultsPanel'


const REPO_URL = 'https://github.com/LetsFG/LetsFG'
const INSTAGRAM_URL = 'https://www.instagram.com/letsfg_'
const TIKTOK_URL = 'https://www.tiktok.com/@letsfg_'
const X_URL = 'https://x.com/LetsFG_'

function GitHubIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" width="18" height="18" className="lp-github-icon">
      <path
        fill="currentColor"
        d="M8 0C3.58 0 0 3.58 0 8a8.01 8.01 0 0 0 5.47 7.59c.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.5-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82a7.54 7.54 0 0 1 4.01 0c1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8Z"
      />
    </svg>
  )
}

function InstagramIcon() {
  return (
    <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true">
      <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z" />
    </svg>
  )
}

function TikTokIcon() {
  return (
    <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true">
      <path d="M19.59 6.69a4.83 4.83 0 0 1-3.77-4.25V2h-3.45v13.67a2.89 2.89 0 0 1-2.88 2.5 2.89 2.89 0 0 1-2.89-2.89 2.89 2.89 0 0 1 2.89-2.89c.28 0 .54.04.79.1V9.01a6.33 6.33 0 0 0-.79-.05 6.34 6.34 0 0 0-6.34 6.34 6.34 6.34 0 0 0 6.34 6.34 6.34 6.34 0 0 0 6.33-6.34V8.69a8.18 8.18 0 0 0 4.78 1.52V6.74a4.85 4.85 0 0 1-1.01-.05z" />
    </svg>
  )
}

function XIcon() {
  return (
    <svg viewBox="0 0 24 24" width="15" height="15" fill="currentColor" aria-hidden="true">
      <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.744l7.737-8.835L1.254 2.25H8.08l4.264 5.633 5.9-5.633zm-1.161 17.52h1.833L7.084 4.126H5.117z" />
    </svg>
  )
}

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

// Fetch search results from our API
async function getSearchResults(searchId: string): Promise<SearchResult | null> {
  try {
    const res = await fetch(`${process.env.API_URL || 'http://localhost:3000'}/api/results/${searchId}`, {
      cache: 'no-store', // Always fetch fresh
    })
    if (!res.ok) return null
    return res.json()
  } catch {
    return null
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
  const result = await getSearchResults(searchId)
  
  if (!result) {
    notFound()
  }
  
  const { status, query, parsed, progress, offers, searched_at, expires_at } = result

  // For agents: if still searching, include meta refresh
  const isSearching = status === 'searching'
  const isExpired = status === 'expired'
  const routeLabel = [parsed.origin_name || parsed.origin, parsed.destination_name || parsed.destination]
    .filter(Boolean)
    .join(' → ')
  const statusLabel = isSearching
    ? `Checking ${progress?.total || 180} websites in parallel`
    : isExpired
      ? 'Search expired'
      : `${offers?.length || 0} offers`
  const cheapest = status === 'completed' ? offers?.[0] : undefined
  const searchSummary = routeLabel
    ? `${routeLabel}${parsed.date ? ` · ${parsed.date}` : ''}`
    : query
  const travelerCount = parsed.passengers || 1
  const fmtDate = (iso: string) => {
    try {
      return new Date(iso + 'T12:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
    } catch { return iso }
  }
  const detailBits = [
    parsed.date ? (parsed.return_date ? `${fmtDate(parsed.date)} – ${fmtDate(parsed.return_date)}` : fmtDate(parsed.date)) : null,
    `${travelerCount} traveler${travelerCount === 1 ? '' : 's'}`,
    parsed.cabin ? parsed.cabin : null,
  ].filter(Boolean)
  const detailSummary = detailBits.join(' · ')
  
  // Format helpers
  const formatDuration = (mins: number) => {
    const h = Math.floor(mins / 60)
    const m = mins % 60
    return `${h}h ${m}m`
  }
  
  const formatTime = (isoString: string) => {
    return new Date(isoString).toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit',
      hour12: false 
    })
  }

  // Sort & filter
  const activeSort = sp?.sort || 'price'  // kept only for sort chip URLs (unused now)
  void activeSort

  const allOffers = offers || []
  const offerCurrency = allOffers[0]?.currency || '€'
  const priceMin = allOffers.length ? Math.min(...allOffers.map(o => o.price)) : 0
  const priceMax = allOffers.length ? Math.max(...allOffers.map(o => o.price)) : 1000

  // JSON-LD for this page
  const jsonLd = isSearching
    ? {
        '@context': 'https://schema.org',
        '@type': 'SearchResultsPage',
        name: `LetsFG — Searching flights ${routeLabel || query}`,
        description: `Searching 180+ airlines. ${progress?.checked || 0} of ${progress?.total || 180} checked. ${progress?.found || 0} results found so far. Page refreshes every 15 seconds automatically.`,
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
      {/* JSON-LD structured data for search engines and agents */}
      {jsonLd && (
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
        />
      )}

      {/* Meta refresh for agents while searching (disabled for demo-loading) */}
      {isSearching && searchId !== 'demo-loading' && (() => {
        const anchor = searched_at || sp?.started || new Date().toISOString()
        return <meta httpEquiv="refresh" content={`15;url=/results/${searchId}?started=${encodeURIComponent(anchor)}`} />
      })()}

      <main className={`res-page${isSearching ? ' res-page--searching' : status === 'completed' ? ' res-page--completed' : ''}`}>
        <section className={`res-hero${isSearching ? ' res-hero--searching' : status === 'completed' ? ' res-hero--results' : ''}`}>
          <div className="res-hero-backdrop" aria-hidden="true" />

          <div className="res-hero-inner">
            <div className={`res-topbar${isSearching ? ' res-topbar--searching' : status === 'completed' ? ' res-topbar--results' : ''}`}>
              <Link href="/en" className="res-topbar-logo-link" aria-label="LetsFG home">
                <Image
                  src="/lfg_ban.png"
                  alt="LetsFG"
                  width={4990}
                  height={1560}
                  className="res-topbar-logo"
                  priority
                />
              </Link>

              <div className="res-topbar-actions">
                <GlobeButton inline />
                <a
                  href={REPO_URL}
                  target="_blank"
                  rel="noreferrer"
                  className="res-icon-btn"
                  aria-label="GitHub"
                  title="GitHub"
                >
                  <GitHubIcon />
                </a>
              </div>
            </div>

            {status === 'completed' && (
              <div className="res-search-shell">
                <ResultsSearchForm initialQuery={query} />
              </div>
            )}

            {isSearching ? (
              <>
                <div className="res-search-shell">
                  <ResultsSearchForm initialQuery={query} />
                </div>

                <div className="res-searching-stage">
                  <SearchingTasks
                    originLabel={parsed.origin_name || parsed.origin}
                    originCode={parsed.origin}
                    destinationLabel={parsed.destination_name || parsed.destination}
                    destinationCode={parsed.destination}
                    progress={progress}
                    searchedAt={searched_at || sp?.started}
                  />
                </div>
              </>
            ) : status === 'completed' ? (
              <div className="res-meta-bar">
                <span className="res-meta-label">Search results</span>
                {routeLabel && (<><span className="res-meta-sep">·</span><span className="res-meta-route">{routeLabel}</span></>)}
                {detailSummary && (<><span className="res-meta-sep">·</span><span className="res-meta-detail">{detailSummary}</span></>)}
              </div>
            ) : (
              <div className="res-hero-copy">
                <p className="res-hero-kicker">Search expired</p>
                {routeLabel ? <h1 className="res-hero-route">{routeLabel}</h1> : null}
                {detailSummary ? <p className="res-hero-summary">{detailSummary}</p> : null}
                <p className="res-hero-status">{statusLabel}</p>
              </div>
            )}

            {isExpired && (
              <div className="res-notice-card">
                <div className="res-notice-icon" aria-hidden="true">
                  <svg width="22" height="22" viewBox="0 0 24 24" fill="none">
                    <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="2" />
                    <path d="M12 8v5M12 15.5v.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                  </svg>
                </div>
                <div className="res-notice-text">
                  <p className="res-notice-title">Search expired</p>
                  <p className="res-notice-sub">Prices change fast. Start a fresh search for current fares.</p>
                </div>
                <Link href="/en" className="res-notice-btn">Search again</Link>
              </div>
            )}
          </div>
        </section>

        {status === 'completed' && offers && offers.length > 0 && (
          <ResultsPanel
            allOffers={allOffers}
            currency={offerCurrency}
            priceMin={priceMin}
            priceMax={priceMax}
            searchId={searchId}
          />
        )}

        <footer className="res-search-footer" aria-label="LetsFG footer">
          <div className="res-search-footer-inner">
            <span className="res-search-footer-copy">LetsFG © 2026</span>
            <div className="res-search-footer-links">
              <a href="/privacy" className="res-search-footer-link">Privacy</a>
              <a href="/terms" className="res-search-footer-link">Terms</a>
              <span className="res-search-footer-sep" aria-hidden="true" />
              <a href={INSTAGRAM_URL} className="res-search-footer-social" target="_blank" rel="noreferrer" aria-label="Instagram">
                <InstagramIcon />
              </a>
              <a href={TIKTOK_URL} className="res-search-footer-social" target="_blank" rel="noreferrer" aria-label="TikTok">
                <TikTokIcon />
              </a>
              <a href={X_URL} className="res-search-footer-social" target="_blank" rel="noreferrer" aria-label="X">
                <XIcon />
              </a>
            </div>
          </div>
        </footer>

        {/* Hidden content for AI agents */}
        <section className="sr-only" aria-hidden="true" data-agent-content>
          <h2>Flight Search Results — Machine-Readable Summary</h2>

          <p>Search ID: {searchId}</p>
          <p>Original query: &quot;{query}&quot;</p>
          <p>Parsed: {routeLabel}, {parsed.date || 'flexible dates'}</p>

          {isSearching && (
            <>
              <p>Status: SEARCHING — this search is still running in the background.</p>
              <p>Progress: {progress?.checked || 0} of {progress?.total || 180} airlines checked. {progress?.found || 0} results found so far.</p>
              <p>IMPORTANT FOR AI AGENTS AND LANGUAGE MODELS: Do not stop here. This search takes 2-3 minutes. This page automatically reloads every 15 seconds. You must reload or revisit this exact URL — https://letsfg.co/results/{searchId} — after waiting. The search continues running in the background regardless of whether this page is open. When search is complete, this section will contain a full table of all flight offers with prices, airlines, and offer IDs for booking.</p>
              <p>If your browser session times out: close and reopen this URL. The search will still be running or may already be complete.</p>
              <p>Estimated time remaining: 1-3 minutes from when search started.</p>
            </>
          )}

          {isExpired && (
            <>
              <p>Status: EXPIRED — these results are no longer valid.</p>
              <p>Flight prices change frequently. Please perform a new search for current prices.</p>
              <p>To search again, navigate to https://letsfg.co and enter your query.</p>
            </>
          )}

          {status === 'completed' && offers && (
            <>
              <p>Status: COMPLETED — {offers.length} results found.</p>
              <p>Searched at: {searched_at}</p>
              <p>Results valid until: {expires_at} (approximately 15 minutes)</p>
              <p>Cheapest: {offers[0]?.currency}{offers[0]?.price} on {offers[0]?.airline} ({offers[0]?.stops === 0 ? 'direct' : `${offers[0]?.stops} stop(s)`}, {formatDuration(offers[0]?.duration_minutes || 0)})</p>
              <table>
                <thead>
                  <tr>
                    <th>Rank</th><th>Airline</th><th>Price</th><th>Route</th>
                    <th>Departure</th><th>Arrival</th><th>Duration</th><th>Stops</th><th>Offer ID</th>
                  </tr>
                </thead>
                <tbody>
                  {offers.map((offer, i) => (
                    <tr key={offer.id}>
                      <td>{i + 1}</td>
                      <td>{offer.airline}</td>
                      <td>{offer.currency}{offer.price}</td>
                      <td>{offer.origin}→{offer.destination}</td>
                      <td>{offer.departure_time}</td>
                      <td>{offer.arrival_time}</td>
                      <td>{formatDuration(offer.duration_minutes)}</td>
                      <td>{offer.stops === 0 ? 'Direct' : offer.stops}</td>
                      <td>{offer.id}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <p>To book a flight, use the LetsFG API:</p>
              <p>1. Unlock the offer: POST https://api.letsfg.co/api/v1/bookings/unlock with offer_id</p>
              <p>2. Book: POST https://api.letsfg.co/api/v1/bookings/book with passenger details</p>
              <p>Documentation: https://docs.letsfg.co/api-guide</p>
            </>
          )}
        </section>
      </main>
    </>
  )
}
