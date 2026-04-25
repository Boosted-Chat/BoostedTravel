'use client'

import { Suspense } from 'react'
import Link from 'next/link'
import Image from 'next/image'
import GlobeButton from '../../globe-button'
import SearchingTasks from './SearchingTasks'

/**
 * Shown immediately by Next.js while page.tsx runs its server-side polling
 * loop (pollUntilDone). Humans see the full searching animation; once the
 * server finishes, the page swaps in with completed results.
 *
 * We can't read path params or searchParams here, so SearchingTasks renders
 * with no origin/destination — it still shows the full plane animation and
 * counter with generic labels.
 */
function LoadingInner() {
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
            </div>
          </div>
          <div className="res-searching-stage">
            <SearchingTasks />
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
