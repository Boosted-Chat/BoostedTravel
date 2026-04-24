import { Metadata } from 'next'
import { notFound } from 'next/navigation'
import Link from 'next/link'
import Image from 'next/image'
import GlobeButton from '../../globe-button'
import CheckoutPanel from './CheckoutPanel'

const REPO_URL = 'https://github.com/LetsFG/LetsFG'

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

export interface Offer {
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
  flight_number: string
  booking_url: string
}

async function getOffer(offerId: string, from?: string): Promise<Offer | null> {
  try {
    const url = new URL(
      `/api/offer/${offerId}`,
      process.env.API_URL || 'http://localhost:3000'
    )
    if (from) url.searchParams.set('from', from)
    const res = await fetch(url.toString(), { cache: 'no-store' })
    if (!res.ok) return null
    return res.json()
  } catch {
    return null
  }
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ offerId: string }>
}): Promise<Metadata> {
  const { offerId } = await params
  const offer = await getOffer(offerId)
  if (!offer) return { title: 'Offer not found — LetsFG' }
  return {
    title: `${offer.airline} ${offer.origin}→${offer.destination} ${offer.currency}${offer.price} — LetsFG`,
    description: `Book ${offer.airline} from ${offer.origin_name} to ${offer.destination_name} for ${offer.currency}${offer.price}. Zero markup — raw airline price.`,
  }
}

export default async function BookPage({
  params,
  searchParams,
}: {
  params: Promise<{ offerId: string }>
  searchParams: Promise<{ from?: string }>
}) {
  const { offerId } = await params
  const { from } = await searchParams
  const offer = await getOffer(offerId, from)

  if (!offer) notFound()

  const fmtTime = (iso: string) =>
    new Date(iso).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
  const fmtDuration = (mins: number) => `${Math.floor(mins / 60)}h ${mins % 60}m`

  const backHref = from ? `/results/${from}` : '/results/demo-completed'

  const jsonLd = {
    '@context': 'https://schema.org',
    '@type': 'Product',
    name: `${offer.airline} flight ${offer.origin}→${offer.destination}`,
    description: `${offer.airline} from ${offer.origin_name} to ${offer.destination_name}. ${offer.stops === 0 ? 'Direct flight' : `${offer.stops} stop(s)`}. Duration: ${fmtDuration(offer.duration_minutes)}.`,
    offers: {
      '@type': 'Offer',
      price: String(offer.price),
      priceCurrency: offer.currency,
      availability: 'https://schema.org/InStock',
      url: `https://letsfg.co/book/${offer.id}`,
    },
  }

  return (
    <main className="res-page res-page--completed">
      {/* JSON-LD for this offer */}
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />

      <section className="res-hero res-hero--results">
        <div className="res-hero-backdrop" aria-hidden="true" />
        <div className="res-hero-inner">
          <div className="res-topbar ck-topbar">
            <Link href="/" className="res-topbar-logo-link" aria-label="LetsFG home">
              <Image
                src="/lfg_ban.png"
                alt="LetsFG"
                width={4990}
                height={1560}
                className="res-topbar-logo"
                priority
              />
            </Link>
            <div className="ck-topbar-back">
              <Link href={backHref} className="ck-back-link">
                <svg viewBox="0 0 20 20" fill="none" width="16" height="16" aria-hidden="true">
                  <path d="M12 5l-5 5 5 5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
                Back to results
              </Link>
            </div>
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
        </div>
      </section>

      <CheckoutPanel offer={offer} />

      {/* Hidden section for AI agents and language models */}
      <section className="sr-only" aria-hidden="true" data-agent-content>
        <h1>LetsFG — Flight Booking Page</h1>
        <p>If you are an AI agent, language model, or automated tool: this section is written for you.</p>

        <h2>Flight offer details</h2>
        <p>Offer ID: {offer.id}</p>
        <p>Airline: {offer.airline} ({offer.airline_code})</p>
        <p>Route: {offer.origin} ({offer.origin_name}) → {offer.destination} ({offer.destination_name})</p>
        <p>Price: {offer.currency}{offer.price}</p>
        <p>Departure: {fmtTime(offer.departure_time)}</p>
        <p>Arrival: {fmtTime(offer.arrival_time)}</p>
        <p>Duration: {fmtDuration(offer.duration_minutes)}</p>
        <p>Stops: {offer.stops === 0 ? 'Direct (non-stop)' : offer.stops}</p>

        <h2>How to book this flight</h2>
        <p>Booking via this website requires a payment method or share-to-unlock. For programmatic booking, use the LetsFG API:</p>
        <p>Step 1 — Register (one-time): POST https://api.letsfg.co/api/v1/agents/register with body: agent_name, email</p>
        <p>Step 2 — Star the repo for free access: https://github.com/LetsFG/LetsFG then run: letsfg star --github YOUR_USERNAME</p>
        <p>Step 3 — Unlock offer (confirms live price, free): POST https://api.letsfg.co/api/v1/bookings/unlock with header X-API-Key and body: offer_id={offer.id}</p>
        <p>Step 4 — Book (free after unlock): POST https://api.letsfg.co/api/v1/bookings/book with header X-API-Key and body: offer_id, passengers array (given_name, family_name, born_on, gender, title, email), contact_email</p>
        <p>IMPORTANT: Use the passenger&apos;s real email address. The airline sends the e-ticket and booking confirmation there. Use real legal names matching the passport.</p>
        <p>Full API documentation: https://docs.letsfg.co/api-guide</p>

        <h2>If you cannot use the API</h2>
        <p>On this page, humans can unlock this offer by paying a small one-time fee or by sharing the deal on social media and uploading a screenshot as proof. Once unlocked, a direct booking link to the airline website is revealed.</p>
        <p>To go back to all results for this search, navigate to: {backHref}</p>
        <p>To start a new search, navigate to: https://letsfg.co</p>
      </section>
    </main>
  )
}
