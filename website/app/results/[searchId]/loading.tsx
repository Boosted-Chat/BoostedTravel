'use client'

import { Suspense, useEffect, useState } from 'react'
import { useParams, useSearchParams } from 'next/navigation'
import Link from 'next/link'
import Image from 'next/image'
import CurrencyButton from '../../currency-button'
import GlobeButton from '../../globe-button'
import ResultsSearchForm from '../ResultsSearchForm'
import SearchingTasks from './SearchingTasks'
import { normalizeCurrencyCode } from '../../../lib/currency-preference'

// Shown immediately while [searchId]/page.tsx runs its server-side poll.
// Polls /api/results/{searchId} on the client for live progress so the
// counter and stages animate in real-time during the wait.

interface SearchInfo {
  originLabel?: string
  originCode?: string
  destinationLabel?: string
  destinationCode?: string
  progress?: { checked: number; total: number; found: number }
  searchedAt?: string
}

function LoadingInner() {
  const params = useParams()
  const searchParams = useSearchParams()
  const searchId = params.searchId as string
  const started = searchParams.get('started')
  const initialCurrency = normalizeCurrencyCode(searchParams.get('cur')) || 'EUR'
  const query = searchParams.get('q')?.trim() || ''

  const [info, setInfo] = useState<SearchInfo>({})

  // Seed the epoch from the ?started= timestamp so the simulated counter
  // knows how long the search has already been running.
  const searchedAt =
    info.searchedAt || (started ? new Date(Number(started)).toISOString() : undefined)

  useEffect(() => {
    if (!searchId) return
    let cancelled = false

    async function poll() {
      try {
        const res = await fetch(`/api/results/${searchId}`, { cache: 'no-store' })
        if (!res.ok || cancelled) return
        const data = await res.json()
        if (cancelled) return
        setInfo({
          originLabel: data.parsed?.origin_name,
          originCode: data.parsed?.origin,
          destinationLabel: data.parsed?.destination_name,
          destinationCode: data.parsed?.destination,
          progress: data.progress,
          searchedAt: data.searched_at,
        })
      } catch {
        // silently ignore — animation still runs via simulated counter
      }
    }

    poll() // immediate first fetch
    const id = setInterval(poll, 4_000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [searchId])

  return (
    <main className="res-page res-page--searching">
      <section className="res-hero res-hero--searching">
        <div className="res-hero-backdrop" aria-hidden="true" />
        <div className="res-hero-inner">
          <div className="res-topbar res-topbar--searching">
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
              <CurrencyButton
                inline
                behavior={query ? 'rerun-search' : 'persist'}
                initialCurrency={initialCurrency}
                searchQuery={query}
              />
            </div>
          </div>
          <div className="res-search-shell">
            <ResultsSearchForm initialQuery={query} initialCurrency={initialCurrency} />
          </div>
          <div className="res-searching-stage">
            <SearchingTasks
              searchId={searchId}
              originLabel={info.originLabel}
              originCode={info.originCode}
              destinationLabel={info.destinationLabel}
              destinationCode={info.destinationCode}
              progress={info.progress}
              searchedAt={searchedAt}
            />
          </div>
        </div>
      </section>
    </main>
  )
}

export default function Loading() {
  return (
    <Suspense>
      <LoadingInner />
    </Suspense>
  )
}
