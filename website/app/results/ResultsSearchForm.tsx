'use client'

import { FormEvent, useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import {
  CURRENCY_CHANGE_EVENT,
  readBrowserSearchCurrency,
  type CurrencyCode,
} from '../../lib/currency-preference'
import { trackSearchSessionEvent } from '../../lib/search-session-analytics'

const DEMO_LOADING = false

function SearchIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true" className="lp-sf-icon" fill="none">
      <circle cx="11" cy="11" r="6.5" stroke="currentColor" strokeWidth="2.2" />
      <path d="M16 16l4 4" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" />
    </svg>
  )
}

function PlaneIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 576 512" aria-hidden="true" className="lp-sf-submit-icon" fill="currentColor">
      <path d="M372 143.9L172.7 40.2c-8-4.1-17.3-4.8-25.7-1.7l-41.1 15c-10.3 3.7-13.8 16.4-7.1 25L200.3 206.4 100.1 242.8 40 206.2c-6.2-3.8-13.8-4.5-20.7-2.1L3 210.1c-9.4 3.4-13.4 14.5-8.3 23.1l53.6 91.8c15.6 26.7 48.1 38.4 77.1 27.8l12.9-4.7 0 0 398.4-145c29.1-10.6 44-42.7 33.5-71.8s-42.7-44-71.8-33.5L372 143.9zM32.2 448c-17.7 0-32 14.3-32 32s14.3 32 32 32l512 0c17.7 0 32-14.3 32-32s-14.3-32-32-32l-512 0z"/>
    </svg>
  )
}

interface ResultsSearchFormProps {
  initialQuery?: string
  initialCurrency?: CurrencyCode
  onSearchSubmit?: (query: string) => void
  trackingSearchId?: string
  trackingSourcePath?: string
  probeMode?: boolean
}

export default function ResultsSearchForm({
  initialQuery = '',
  initialCurrency = 'EUR',
  onSearchSubmit,
  trackingSearchId,
  trackingSourcePath,
  probeMode = false,
}: ResultsSearchFormProps) {
  const router = useRouter()
  const [query, setQuery] = useState(initialQuery)
  const [prefCurrency, setPrefCurrency] = useState<CurrencyCode>(initialCurrency)
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    setQuery(initialQuery)
  }, [initialQuery])

  useEffect(() => {
    setPrefCurrency(readBrowserSearchCurrency(initialCurrency))
    const sync = () => setPrefCurrency(readBrowserSearchCurrency(initialCurrency))
    window.addEventListener(CURRENCY_CHANGE_EVENT, sync)
    return () => window.removeEventListener(CURRENCY_CHANGE_EVENT, sync)
  }, [initialCurrency])

  const handleSearch = (event: FormEvent) => {
    if (!query.trim()) {
      event.preventDefault()
      return
    }
    trackSearchSessionEvent(trackingSearchId, 'new_search_started', {
      next_query: query.trim(),
    }, {
      source: 'website-results-form',
      source_path: trackingSourcePath || (trackingSearchId ? `/results/${trackingSearchId}` : '/results'),
    }, { keepalive: true })
    onSearchSubmit?.(query.trim())
    if (DEMO_LOADING) {
      event.preventDefault()
      setIsLoading(true)
      router.push(`/results/demo-loading${probeMode ? '?probe=1' : ''}`)
      return
    }
  }

  return (
    <div className="lp-sf-wrap lp-sf-wrap--compact lp-sf-wrap--results">
      <form action="/results" method="get" onSubmit={handleSearch} className="lp-sf-form">
        {probeMode && <input type="hidden" name="probe" value="1" />}
        <input type="hidden" name="cur" value={prefCurrency} readOnly aria-hidden="true" />
        <div className="lp-sf-frame">
          <div className="lp-sf-input-wrap">
            <span className="lp-sf-leading" aria-hidden="true">
              <SearchIcon />
            </span>
            <input
              id="results-trip-query"
              name="q"
              type="text"
              className="lp-sf-input"
              placeholder="London to Barcelona next Friday"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              disabled={DEMO_LOADING && isLoading}
              autoComplete="off"
              spellCheck={false}
            />
          </div>

          <button
            type="submit"
            className="lp-sf-button"
            disabled={(DEMO_LOADING && isLoading) || !query.trim()}
            aria-label={isLoading ? 'Searching flights' : 'Search flights'}
          >
            <PlaneIcon />
          </button>
        </div>
      </form>
    </div>
  )
}