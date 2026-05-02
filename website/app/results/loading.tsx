'use client'

import { Suspense } from 'react'
import { useSearchParams } from 'next/navigation'
import Link from 'next/link'
import Image from 'next/image'
import CurrencyButton from '../currency-button'
import GlobeButton from '../globe-button'
import ResultsSearchForm from './ResultsSearchForm'
import SearchingTasks from './[searchId]/SearchingTasks'
import { normalizeCurrencyCode } from '../../lib/currency-preference'
import { parseNLQuery } from '../lib/searchParsing'

// Shown immediately on client-side navigation to /results?q=...
// before the server responds — eliminates the blank-page gap.

function LoadingInner() {
  const params = useSearchParams()
  const query = params.get('q') || ''
  const initialCurrency = normalizeCurrencyCode(params.get('cur')) || 'EUR'
  const parsed = parseNLQuery(query)

  return (
    <main className="res-page res-page--searching">
      <section className="res-hero res-hero--searching">
        <div className="res-hero-backdrop" aria-hidden="true" />
        <div className="res-hero-inner">
          <div className="res-topbar res-topbar--searching">
            <Link href="/en" className="res-topbar-logo-link" aria-label="LetsFG home">
              <Image src="/lfg_ban.png" alt="LetsFG" width={4990} height={1560} className="res-topbar-logo" priority />
            </Link>
            <div className="res-topbar-actions">
              <GlobeButton inline />
              <CurrencyButton inline behavior="rerun-search" initialCurrency={initialCurrency} searchQuery={query} />
            </div>
          </div>
          <div className="res-search-shell">
            <ResultsSearchForm initialQuery={query} initialCurrency={initialCurrency} />
          </div>
          <div className="res-searching-stage">
            <SearchingTasks
              originLabel={parsed.origin_name || parsed.origin}
              originCode={parsed.origin}
              destinationLabel={parsed.destination_name || parsed.destination}
              destinationCode={parsed.destination}
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
