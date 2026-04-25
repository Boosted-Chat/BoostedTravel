import Link from 'next/link'
import Image from 'next/image'

/**
 * Shown by Next.js App Router immediately while page.tsx is fetching search
 * data server-side. Replaces the body's dark landing-page background with the
 * searching sky gradient so there's never a black flash on navigation.
 *
 * We deliberately avoid SearchingTasks here (needs route params we don't have)
 * and show a minimal branded skeleton instead.
 */
export default function Loading() {
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
          </div>
        </div>
      </section>
    </main>
  )
}
