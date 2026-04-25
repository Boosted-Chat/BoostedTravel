'use client'

import { useEffect, useState } from 'react'
import { useTranslations } from 'next-intl'
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
const SESSION_RESULT_CACHE_LIMIT = 500

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

interface ParsedQuery {
  origin?: string
  origin_name?: string
  destination?: string
  destination_name?: string
  date?: string
  return_date?: string
  passengers?: number
  cabin?: string
}

export interface SearchPageClientProps {
  searchId: string
  query: string
  parsed: ParsedQuery
  initialStatus: 'searching' | 'completed' | 'expired'
  initialProgress?: { checked: number; total: number; found: number }
  initialOffers: FlightOffer[]
  searchedAt?: string
  expiresAt?: string
}

function dedup(offers: FlightOffer[]): FlightOffer[] {
  return Array.from(new Map(offers.map(o => [o.id, o])).values())
}

function formatDuration(mins: number) {
  const h = Math.floor(mins / 60)
  const m = mins % 60
  return `${h}h ${m}m`
}

/**
 * Client component that owns the dynamic parts of the results page.
 *
 * Architecture: the server component (page.tsx) renders JSON-LD for SEO and
 * passes initial data here. This component then polls /api/results/{searchId}
 * every 5 s on the client — NO router.refresh() involved.
 *
 * Why this matters: router.refresh() re-renders the RSC tree which can remount
 * SearchingTasks, resetting elapsed/simChecked/animation state. With client-
 * side polling, SearchingTasks is NEVER remounted during a search. The elapsed
 * counter, the flying-plane animation, and the simulated counter all run
 * uninterrupted from the moment the page loads until results appear.
 */
export default function SearchPageClient({
  searchId,
  query,
  parsed,
  initialStatus,
  initialProgress,
  initialOffers,
  searchedAt,
  expiresAt,
}: SearchPageClientProps) {
  const t = useTranslations('Results')

  const [status, setStatus] = useState(initialStatus)
  const [progress, setProgress] = useState(initialProgress)
  const [offers, setOffers] = useState(initialOffers)

  const isSearching = status === 'searching'
  const isExpired = status === 'expired'

  const CACHE_KEY = `lfg_result_${searchId}`

  // If the server returned 'searching' (search not done yet / expired on FSW),
  // immediately check sessionStorage for a previously cached completed result.
  useEffect(() => {
    if (initialStatus !== 'searching') return
    try {
      const raw = sessionStorage.getItem(CACHE_KEY)
      if (!raw) return
      const cached = JSON.parse(raw)
      if (cached.status === 'completed' && Array.isArray(cached.offers) && cached.offers.length > 0) {
        setStatus('completed')
        setOffers(dedup(cached.offers))
      }
    } catch { /* private mode or parse error — ignore */ }
  }, [searchId, initialStatus, CACHE_KEY])

  // When search completes, persist results to sessionStorage so revisiting
  // the URL is instant even if FSW has expired the search.
  useEffect(() => {
    if (status !== 'completed' || offers.length === 0) return
    try {
      sessionStorage.setItem(CACHE_KEY, JSON.stringify({
        status: 'completed',
        offers: offers.slice(0, SESSION_RESULT_CACHE_LIMIT),
      }))
    } catch { /* storage full or unavailable */ }
  }, [status, searchId, offers, CACHE_KEY])

  // Client-side poll — replaces SearchPoller + router.refresh().
  // SearchingTasks stays mounted throughout the search; its animation state is
  // never lost because we never touch the server component during the search.
  useEffect(() => {
    if (!isSearching) return

    const poll = async () => {
      try {
        const res = await fetch(`/api/results/${searchId}`, { cache: 'no-store' })
        if (!res.ok) return
        const data = await res.json()
        if (data.progress) setProgress(data.progress)
        if (data.status !== 'searching') {
          setStatus(data.status)
          if (data.offers?.length) setOffers(dedup(data.offers))
        }
      } catch {
        // Network error — silently retry next interval
      }
    }

    const id = setInterval(poll, 5000)
    return () => clearInterval(id)
  }, [searchId, isSearching])

  // Derived display strings
  const routeLabel = [
    parsed.origin_name || parsed.origin,
    parsed.destination_name || parsed.destination,
  ].filter(Boolean).join(' → ')

  const fmtDate = (iso: string) => {
    try {
      return new Date(iso + 'T12:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
    } catch { return iso }
  }

  const travelerCount = parsed.passengers || 1
  const travelerLabel = `${travelerCount} ${travelerCount === 1 ? t('traveler') : t('travelers')}`

  const detailBits = [
    parsed.date
      ? parsed.return_date
        ? `${fmtDate(parsed.date)} – ${fmtDate(parsed.return_date)}`
        : fmtDate(parsed.date)
      : null,
    travelerLabel,
    parsed.cabin ?? null,
  ].filter(Boolean)
  const detailSummary = detailBits.join(' · ')

  const statusLabel = isSearching
    ? `Checking ${progress?.total || 180} websites in parallel`
    : isExpired
    ? 'Search expired'
    : `${offers.length} offers`

  // Offer data for ResultsPanel
  const allOffers = offers
  const offerCurrency = allOffers[0]?.currency || '€'
  const priceMin = allOffers.length ? Math.min(...allOffers.map(o => o.price)) : 0
  const priceMax = allOffers.length ? Math.max(...allOffers.map(o => o.price)) : 1000

  return (
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
                  searchedAt={searchedAt}
                  searchId={searchId}
                />
              </div>
            </>
          ) : status === 'completed' ? (
            <div className="res-meta-bar">
              <span className="res-meta-label">{t('searchResults')}</span>
              {routeLabel && (
                <>
                  <span className="res-meta-sep">·</span>
                  <span className="res-meta-route">{routeLabel}</span>
                </>
              )}
              {detailSummary && (
                <>
                  <span className="res-meta-sep">·</span>
                  <span className="res-meta-detail">{detailSummary}</span>
                </>
              )}
            </div>
          ) : (
            <div className="res-hero-copy">
              <p className="res-hero-kicker">{t('searchExpired')}</p>
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
                <p className="res-notice-title">{t('expiredNoticeTitle')}</p>
                <p className="res-notice-sub">{t('expiredNoticeSub')}</p>
              </div>
              <Link href="/en" className="res-notice-btn">{t('searchAgain')}</Link>
            </div>
          )}
        </div>
      </section>

      {status === 'completed' && allOffers.length > 0 && (
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
          <span className="res-search-footer-copy">{t('copyright')}</span>
          <div className="res-search-footer-links">
            <a href="/privacy" className="res-search-footer-link">{t('privacy')}</a>
            <a href="/terms" className="res-search-footer-link">{t('terms')}</a>
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
            <p>IMPORTANT FOR AI AGENTS AND LANGUAGE MODELS: Do not stop here. This search takes 2-3 minutes. You must reload or revisit this exact URL — https://letsfg.co/results/{searchId} — after waiting. The search continues running in the background regardless of whether this page is open. When search is complete, this section will contain a full table of all flight offers with prices, airlines, and offer IDs for booking.</p>
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

        {status === 'completed' && allOffers.length > 0 && (
          <>
            <p>Status: COMPLETED — {allOffers.length} results found.</p>
            <p>Searched at: {searchedAt}</p>
            <p>Results valid until: {expiresAt} (approximately 15 minutes)</p>
            <p>Cheapest: {allOffers[0]?.currency}{allOffers[0]?.price} on {allOffers[0]?.airline} ({allOffers[0]?.stops === 0 ? 'direct' : `${allOffers[0]?.stops} stop(s)`}, {formatDuration(allOffers[0]?.duration_minutes || 0)})</p>
            <table>
              <thead>
                <tr>
                  <th>Rank</th><th>Airline</th><th>Price</th><th>Route</th>
                  <th>Departure</th><th>Arrival</th><th>Duration</th><th>Stops</th><th>Offer ID</th>
                </tr>
              </thead>
              <tbody>
                {allOffers.map((offer, i) => (
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
  )
}
