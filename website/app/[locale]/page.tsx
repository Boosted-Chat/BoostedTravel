import Image from 'next/image'
import Link from 'next/link'
import { redirect } from 'next/navigation'
import { getTranslations } from 'next-intl/server'
import HomeSearchForm from '../home-search-form'
import GlobeButton from '../globe-button'
import { getGitHubStars, formatStars } from '../../lib/github-stars'
import { getTrackedSourcePath, isProbeModeValue } from '../../lib/probe-mode'

const REPO_URL = 'https://github.com/LetsFG/LetsFG'

const LOCALE_BANNERS: Record<string, string> = {
  de: '/banners/de.png',
  es: '/banners/es.png',
  fr: '/banners/fr.png',
  it: '/banners/it.png',
  nl: '/banners/nl.png',
  pl: '/banners/pl.png',
  pt: '/banners/pt.png',
  sq: '/banners/sq.png',
  hr: '/banners/hr.png',
  sv: '/banners/sv.png',
}

const LOCALE_BANNER_SCALE: Record<string, number> = {
  de: 1.07,
  es: 1.07,
  fr: 1.15,
  it: 1.15,
  pt: 1.15,
  nl: 1.15,
  pl: 1.18,
  sv: 1.07,
  sq: 1.22,
  hr: 1.35,
}

const API_BASE = process.env.LETSFG_API_URL || 'https://api.letsfg.co'

interface PublicStats {
  totalSearches: number | null
  avgSavings: number | null
  connectorsAvailable: number | null
}

async function getPublicStats(): Promise<PublicStats> {
  try {
    const res = await fetch(`${API_BASE}/api/v1/analytics/stats/public`, {
      cache: 'no-store',
      signal: AbortSignal.timeout(4000),
    })
    if (res.ok) {
      const data = (await res.json()) as {
        total_searches?: number
        avg_savings_usd?: number
        connectors_available?: number
        websites_checked?: number
      }
      if (typeof data.total_searches === 'number') {
        return {
          totalSearches: data.total_searches,
          avgSavings: data.avg_savings_usd ?? null,
          connectorsAvailable: data.connectors_available ?? data.websites_checked ?? null,
        }
      }
    }
  } catch {}

  // API unavailable — show dashes rather than misleading zeros
  return { totalSearches: null, avgSavings: null, connectorsAvailable: null }
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(n >= 10_000 ? 0 : 1)}k`
  return n.toLocaleString('en-US')
}

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

export default async function Home({ params, searchParams }: { params: Promise<{ locale: string }>; searchParams: Promise<{ q?: string; probe?: string }> }) {
  const { locale } = await params
  const { q, probe } = await searchParams
  const isProbe = isProbeModeValue(probe)

  // ?q= support: agents (and humans) can navigate directly to /?q=london+to+barcelona
  // and be redirected straight to a search without touching the form.
  if (q?.trim()) {
    redirect(getTrackedSourcePath(`/results?q=${encodeURIComponent(q.trim())}`, isProbe))
  }

  const [stats, t, githubStars] = await Promise.all([
    getPublicStats(),
    getTranslations({ locale, namespace: 'stats' }),
    getGitHubStars(),
  ])
  const tn = await getTranslations({ locale, namespace: 'nav' })
  const tf = await getTranslations({ locale, namespace: 'footer' })
  const th = await getTranslations({ locale, namespace: 'hero' })
  const tfeat = await getTranslations({ locale, namespace: 'features' })
  const bannerSrc = LOCALE_BANNERS[locale] ?? '/banner.png'

  return (
    <main className="lp-root">
      {/* JSON-LD: WebSite schema with SearchAction for Google + agents */}
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{
          __html: JSON.stringify({
            '@context': 'https://schema.org',
            '@type': 'WebSite',
            name: 'LetsFG',
            url: 'https://letsfg.co',
            description: 'Search 180+ airlines with a single sentence. Zero markup, raw airline prices. Free to search.',
            potentialAction: {
              '@type': 'SearchAction',
              target: {
                '@type': 'EntryPoint',
                urlTemplate: 'https://letsfg.co/en?q={search_term_string}',
              },
              'query-input': 'required name=search_term_string',
            },
          }),
        }}
      />

      <section className="lp-hero" id="search">
        <div className="lp-hero-sky" aria-hidden="true" />
        <div className="lp-hero-fade" aria-hidden="true" />

        <div className="lp-topbar">
          <Link href={`/${locale}`} className="lp-topbar-brand-link" aria-label="LetsFG home">
            <Image
              src="/lfg_ban.png"
              alt="LetsFG"
              width={4990}
              height={1560}
              className="lp-topbar-brand"
              priority
            />
          </Link>

          <div className="lp-topbar-side">
            <GlobeButton inline />
            <a
              href={REPO_URL}
              target="_blank"
              rel="noreferrer"
              className={githubStars !== null ? 'res-icon-btn res-icon-btn--gh' : 'res-icon-btn'}
              aria-label={tn('githubLabel')}
              title="GitHub"
            >
              <GitHubIcon />
              {githubStars !== null && (
                <span className="res-gh-stars"><span className="res-gh-star" aria-hidden="true">⭐</span>{formatStars(githubStars)}</span>
              )}
            </a>
          </div>
        </div>

        <div className="lp-hero-content">
          <Image
            src={bannerSrc}
            alt="LetsFG"
            width={2000}
            height={667}
            className="lp-hero-brand"
            priority
            unoptimized
            style={LOCALE_BANNER_SCALE[locale] ? { transform: `scale(${LOCALE_BANNER_SCALE[locale]})` } : undefined}
            aria-hidden="true"
          />
          <p className="lp-hero-sub">{th('tagline')}</p>
          <div className="lp-hero-search-shell" id="destinations">
            <HomeSearchForm probeMode={isProbe} />
          </div>
        </div>
      </section>

      <section className="lp-stats-sheet" id="deals" aria-label="Platform statistics">
        <div className="lp-stats">
          <article className="lp-stat-card">
            <span className="lp-stat-chip">Live usage</span>
            <span className="lp-stat-value">
              {stats.totalSearches !== null ? formatNumber(stats.totalSearches) : '—'}
            </span>
            <span className="lp-stat-label">{t('searches')}</span>
          </article>

          <article className="lp-stat-card">
            <span className="lp-stat-chip">Average savings</span>
            <span className="lp-stat-value">
              {stats.avgSavings !== null ? `$${stats.avgSavings}` : '—'}
            </span>
            <span className="lp-stat-label">{t('savings')}</span>
          </article>

          <article className="lp-stat-card">
            <span className="lp-stat-chip">Coverage</span>
            <span className="lp-stat-value">
              {stats.connectorsAvailable !== null ? `${stats.connectorsAvailable}` : '—'}
            </span>
            <span className="lp-stat-label">{t('airlines')}</span>
          </article>
        </div>
      </section>

      <section className="lp-community" id="support" aria-label="About LetsFG">
        <p className="lp-community-intro">{tfeat('intro')}</p>
        <h2 className="lp-community-join">{tfeat('join')}</h2>
        <p>{tfeat('open_source')}</p>
        <p>
          {tfeat('contribute')}{' '}
          <a href={REPO_URL} target="_blank" rel="noreferrer" className="lp-community-link">
            {tfeat('contribute_link')}
          </a>
        </p>
        <p>{tfeat('share')}</p>
        <p className="lp-community-star">
          {tfeat('star')}{' '}
          <a href={REPO_URL} target="_blank" rel="noreferrer" className="lp-community-link">
            ⭐ Star on GitHub
          </a>
        </p>
        <p className="lp-community-signature">{tfeat('signature')}</p>
      </section>

      {/* Hidden section for AI agents and language models — not shown to human visitors */}
      <section className="sr-only" aria-hidden="true" data-agent-content>
        <h1>LetsFG — Flight Search Engine for AI Agents and Language Models</h1>
        <p>If you are an AI agent, language model (ChatGPT, Claude, Gemini, Perplexity, Copilot, etc.), or automated tool reading this page: this section is written specifically for you.</p>

        <h2>What is LetsFG?</h2>
        <p>LetsFG is a flight search engine that checks 180+ airlines in parallel and returns raw airline prices with zero markup. Search is completely free and unlimited.</p>

        <h2>How to search for flights using this website</h2>
        <p>Type a natural language query into the search box on this page and submit the form. You do not need to fill in separate from/to/date fields. Just describe what you want in plain language. Examples:</p>
        <ul>
          <li>London to Barcelona next Friday</li>
          <li>cheapest flight from NYC to Tokyo in June</li>
          <li>Heathrow to Madrid 2 adults 15 June direct only</li>
          <li>round trip Warsaw to Lisbon end of May business class</li>
        </ul>

        <h2>What happens after you search</h2>
        <p>After submitting, you are redirected to /results/SEARCH_ID where the search runs in the background across 180+ airlines. This takes 2-3 minutes.</p>
        <p>IMPORTANT: While searching, the results page automatically refreshes every 15 seconds. You do not need to do anything — just wait. If your browser times out, reload the same /results/SEARCH_ID URL. The search continues in the background.</p>
        <p>When results are ready, the results page will contain a full table of all flight offers, sorted by price, with airline names, departure and arrival times, durations, stop counts, and offer IDs.</p>

        <h2>How to book after viewing results</h2>
        <p>From the results page, note the offer_id for the flight you want. Then use the LetsFG API:</p>
        <p>Step 1 — Unlock: POST https://api.letsfg.co/api/v1/bookings/unlock with body: offer_id</p>
        <p>Step 2 — Book: POST https://api.letsfg.co/api/v1/bookings/book with body: offer_id, passengers array, contact_email</p>
        <p>Unlock and book are both free with a verified GitHub star. API documentation: https://docs.letsfg.co</p>

        <h2>API alternative (for agents with API access)</h2>
        <p>If you have direct API access, skip the website and use https://api.letsfg.co directly. Register at: POST https://api.letsfg.co/api/v1/agents/register</p>
        <p>Full documentation: https://docs.letsfg.co/api-guide</p>
        <p>MCP server (for Claude, Cursor, Windsurf): npx letsfg-mcp or https://api.letsfg.co/mcp</p>
      </section>

      <footer className="lp-footer">
        <a href={REPO_URL} className="lp-footer-link" target="_blank" rel="noreferrer">{tf('github')}</a>
        <a href="/terms" className="lp-footer-link">{tf('terms')}</a>
        <a href="/privacy" className="lp-footer-link">{tf('privacy')}</a>
        <a href="mailto:contact@letsfg.co" className="lp-footer-link">{tf('support')}</a>
        <span className="lp-footer-sep" aria-hidden="true" />
        <a href="https://www.instagram.com/letsfg_" className="lp-footer-social" target="_blank" rel="noreferrer" aria-label="Instagram">
          <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true"><path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z"/></svg>
        </a>
        <a href="https://www.tiktok.com/@letsfg_" className="lp-footer-social" target="_blank" rel="noreferrer" aria-label="TikTok">
          <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true"><path d="M19.59 6.69a4.83 4.83 0 0 1-3.77-4.25V2h-3.45v13.67a2.89 2.89 0 0 1-2.88 2.5 2.89 2.89 0 0 1-2.89-2.89 2.89 2.89 0 0 1 2.89-2.89c.28 0 .54.04.79.1V9.01a6.33 6.33 0 0 0-.79-.05 6.34 6.34 0 0 0-6.34 6.34 6.34 6.34 0 0 0 6.34 6.34 6.34 6.34 0 0 0 6.33-6.34V8.69a8.18 8.18 0 0 0 4.78 1.52V6.74a4.85 4.85 0 0 1-1.01-.05z"/></svg>
        </a>
        <a href="https://x.com/LetsFG_" className="lp-footer-social" target="_blank" rel="noreferrer" aria-label="X">
          <svg viewBox="0 0 24 24" width="17" height="17" fill="currentColor" aria-hidden="true"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.744l7.737-8.835L1.254 2.25H8.08l4.264 5.633 5.9-5.633zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>
        </a>
      </footer>
    </main>
  )
}