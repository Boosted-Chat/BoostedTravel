'use client'

import { FormEvent, useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'

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

export default function ResultsSearchForm({ initialQuery = '' }: { initialQuery?: string }) {
  const router = useRouter()
  const [query, setQuery] = useState(initialQuery)
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    setQuery(initialQuery)
  }, [initialQuery])

  const handleSearch = (event: FormEvent) => {
    event.preventDefault()
    if (!query.trim()) return
    setIsLoading(true)
    if (DEMO_LOADING) {
      router.push('/results/demo-loading')
      return
    }
    router.push(`/results?q=${encodeURIComponent(query.trim())}`)
  }

  return (
    <div className="lp-sf-wrap lp-sf-wrap--compact lp-sf-wrap--results">
      <form onSubmit={handleSearch} className="lp-sf-form">
        <div className="lp-sf-frame">
          <div className="lp-sf-input-wrap">
            <span className="lp-sf-leading" aria-hidden="true">
              <SearchIcon />
            </span>
            <input
              id="results-trip-query"
              type="text"
              className="lp-sf-input"
              placeholder="London to Barcelona next Friday"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              disabled={isLoading}
              autoComplete="off"
              spellCheck={false}
            />
          </div>

          <button
            type="submit"
            className="lp-sf-button"
            disabled={isLoading || !query.trim()}
            aria-label={isLoading ? 'Searching flights' : 'Search flights'}
          >
            <PlaneIcon />
          </button>
        </div>
      </form>
    </div>
  )
}