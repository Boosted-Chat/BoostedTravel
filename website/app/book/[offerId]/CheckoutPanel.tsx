'use client'

import { useState, useRef, useCallback, useMemo, useEffect } from 'react'
import { useTranslations } from 'next-intl'
import { getAirlineLogoUrl } from '../../airlineLogos'
import { formatFlightTime } from '../../../lib/flight-datetime'
import { calculateFee, withFee } from '../../../lib/pricing'
import type { Offer } from './page'
import { trackSearchSessionEvent } from '../../../lib/search-session-analytics'
import { appendProbeParam, getTrackedSourcePath } from '../../../lib/probe-mode'

interface Props {
  offer: Offer
  searchId: string | null
  trackingSearchId: string | null
  isTestSearch: boolean
  offerRef: string | null
}

type CheckoutStep =
  | { type: 'checking' }           // checking unlock status on mount
  | { type: 'verifying-payment' }  // verifying Stripe session after redirect
  | { type: 'locked' }
  | { type: 'paying' }             // waiting for Stripe redirect
  | { type: 'share-select' }
  | { type: 'share-upload'; platform: Platform }     // screenshot upload
  | { type: 'share-verifying'; platform: Platform }  // verifying screenshot with AI
  | { type: 'share-rejected'; platform: Platform }
  | { type: 'unlocked'; via: 'payment' | 'share' | 'existing' }

interface Platform {
  id: string
  label: string
  instructions: string[]
}

interface BookingOption {
  leg: 'outbound' | 'inbound'
  airline: string
  airline_code: string
  booking_url: string
  booking_site?: string
  price?: number
  currency?: string
  origin?: string
  destination?: string
  departure_time?: string
  arrival_time?: string
}

type TripBreakdownLeg = NonNullable<Offer['trip_breakdown']>[number]

interface SplitBookingLeg extends TripBreakdownLeg {
  booking_url?: string
  booking_site?: string
}




const PLATFORM_ICONS: Record<string, React.ReactNode> = {
  tiktok: (
    <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true">
      <path d="M19.59 6.69a4.83 4.83 0 0 1-3.77-4.25V2h-3.45v13.67a2.89 2.89 0 0 1-2.88 2.5 2.89 2.89 0 0 1-2.89-2.89 2.89 2.89 0 0 1 2.89-2.89c.28 0 .54.04.79.1V9.01a6.33 6.33 0 0 0-.79-.05 6.34 6.34 0 0 0-6.34 6.34 6.34 6.34 0 0 0 6.34 6.34 6.34 6.34 0 0 0 6.33-6.34V8.69a8.18 8.18 0 0 0 4.78 1.52V6.75a4.85 4.85 0 0 1-1.01-.06z" />
    </svg>
  ),
  instagram: (
    <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true">
      <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z" />
    </svg>
  ),
  twitter: (
    <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true">
      <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.744l7.737-8.835L1.254 2.25H8.08l4.264 5.633 5.9-5.633zm-1.161 17.52h1.833L7.084 4.126H5.117z" />
    </svg>
  ),
  facebook: (
    <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true">
      <path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z" />
    </svg>
  ),
  message: (
    <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </svg>
  ),
  whatsapp: (
    <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true">
      <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413Z"/>
    </svg>
  ),
  telegram: (
    <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true">
      <path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/>
    </svg>
  ),
}

function fmtTime(iso: string) {
  return formatFlightTime(iso)
}

function fmtDuration(mins: number) {
  const h = Math.floor(mins / 60)
  const m = mins % 60
  return `${h}h ${m > 0 ? ` ${m}m` : ''}`
}

function fmtDate(iso: string) {
  return new Date(iso).toLocaleDateString('en-US', {
    weekday: 'short',
    day: 'numeric',
    month: 'short',
    year: 'numeric',
  })
}

function fmtFee(fee: number, currency: string) {
  return `${currency}${fee < 10 ? fee.toFixed(2) : Math.round(fee)}`
}

function fmtMoney(amount: number, currency: string) {
  return `${currency}${amount.toFixed(2).replace(/\.00$/, '')}`
}

function wait(ms: number) {
  return new Promise<void>((resolve) => {
    window.setTimeout(resolve, ms)
  })
}

const UNLOCK_TOKEN_STORAGE_PREFIX = 'lfg_unlock_token:'
const UNLOCK_TOKEN_HEADER_NAME = 'x-letsfg-unlock-token'

function getUnlockTokenStorageKey(searchId: string) {
  return `${UNLOCK_TOKEN_STORAGE_PREFIX}${searchId}`
}

function readStoredUnlockToken(searchId: string | null): string | null {
  if (!searchId) return null

  try {
    return window.localStorage.getItem(getUnlockTokenStorageKey(searchId))
  } catch {
    return null
  }
}

function persistUnlockToken(searchId: string | null, unlockToken: string | undefined) {
  if (!searchId || !unlockToken) return

  try {
    window.localStorage.setItem(getUnlockTokenStorageKey(searchId), unlockToken)
  } catch {
    // Ignore storage failures and keep the in-memory flow working.
  }
}

async function fetchLatestOfferRef(searchId: string, offerId: string, isTestSearch: boolean): Promise<string | null> {
  try {
    const params = new URLSearchParams()
    appendProbeParam(params, isTestSearch)
    const query = params.toString()
    const res = await fetch(`/api/results/${encodeURIComponent(searchId)}${query ? `?${query}` : ''}`, {
      cache: 'no-store',
      credentials: 'same-origin',
    })
    if (!res.ok) {
      return null
    }

    const data = await res.json() as {
      offers?: Array<{ id?: string; offer_ref?: string }>
    }
    const matchedOffer = data.offers?.find((candidate) => candidate.id === offerId)
    return typeof matchedOffer?.offer_ref === 'string' && matchedOffer.offer_ref.length > 0
      ? matchedOffer.offer_ref
      : null
  } catch {
    return null
  }
}

function LockIcon() {
  return (
    <svg viewBox="0 0 20 20" fill="none" width="16" height="16" aria-hidden="true">
      <rect x="4" y="9" width="12" height="9" rx="2" stroke="currentColor" strokeWidth="1.8" />
      <path d="M7 9V6a3 3 0 1 1 6 0v3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  )
}

function CheckIcon() {
  return (
    <svg viewBox="0 0 20 20" fill="none" width="18" height="18" aria-hidden="true">
      <path d="M4 10l4.5 4.5L16 6" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function ArrowIcon() {
  return (
    <svg viewBox="0 0 20 20" fill="none" width="16" height="16" aria-hidden="true">
      <path d="M4 10h12M11 5l5 5-5 5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function AirlineLogo({ code, name }: { code: string; name: string }) {
  const [failed, setFailed] = useState(false)
  if (failed) {
    return (
      <div className="ck-airline-logo ck-airline-logo--text" aria-label={name}>
        {code.slice(0, 2)}
      </div>
    )
  }
  return (
    <div className="ck-airline-logo">
      <img
        src={getAirlineLogoUrl(code)}
        alt={name}
        width={40}
        height={40}
        onError={() => setFailed(true)}
      />
    </div>
  )
}

export default function CheckoutPanel({
  offer,
  searchId,
  trackingSearchId,
  isTestSearch,
  offerRef,
}: Props) {
  const t = useTranslations('Checkout')
  const analyticsSearchId = trackingSearchId || searchId
  const checkoutSourcePath = getTrackedSourcePath(`/book/${offer.id}`, isTestSearch)
  const homeHref = isTestSearch ? 'https://letsfg.co/en?probe=1' : 'https://letsfg.co'
  const platforms = useMemo<Platform[]>(() => [
    {
      id: 'instagram',
      label: t('platform_instagram'),
      instructions: [t('instagram_step1'), t('instagram_step2'), t('instagram_step3')],
    },
    {
      id: 'tiktok',
      label: t('platform_tiktok'),
      instructions: [t('tiktok_step1'), t('tiktok_step2'), t('tiktok_step3')],
    },
    {
      id: 'twitter',
      label: t('platform_twitter'),
      instructions: [t('twitter_step1'), t('twitter_step2'), t('twitter_step3')],
    },
    {
      id: 'facebook',
      label: t('platform_facebook'),
      instructions: [t('facebook_step1'), t('facebook_step2'), t('facebook_step3')],
    },
    {
      id: 'message',
      label: t('platform_message'),
      instructions: [t('message_step1'), t('message_step2'), t('message_step3')],
    },
  ], [t])
  const fee = calculateFee(offer.price, offer.currency)
  const showShareOption = true
  const tripBreakdown = useMemo<TripBreakdownLeg[]>(() => {
    if (offer.trip_breakdown?.length) {
      return offer.trip_breakdown
    }
    if (!offer.inbound) {
      return []
    }
    return [
      {
        leg: 'outbound',
        airline: offer.airline,
        airline_code: offer.airline_code,
        origin: offer.origin,
        destination: offer.destination,
        departure_time: offer.departure_time,
        arrival_time: offer.arrival_time,
        duration_minutes: offer.duration_minutes,
      },
      {
        leg: 'inbound',
        airline: offer.inbound.airline || offer.airline,
        airline_code: offer.inbound.airline_code || offer.airline_code,
        origin: offer.inbound.origin,
        destination: offer.inbound.destination,
        departure_time: offer.inbound.departure_time,
        arrival_time: offer.inbound.arrival_time,
        duration_minutes: offer.inbound.duration_minutes,
      },
    ]
  }, [offer])
  const summaryLegs = useMemo<TripBreakdownLeg[]>(() => {
    if (tripBreakdown.length) {
      return tripBreakdown
    }
    return [
      {
        leg: 'outbound',
        airline: offer.airline,
        airline_code: offer.airline_code,
        origin: offer.origin,
        destination: offer.destination,
        departure_time: offer.departure_time,
        arrival_time: offer.arrival_time,
        duration_minutes: offer.duration_minutes,
      },
    ]
  }, [offer, tripBreakdown])
  const summaryDates = useMemo(() => {
    const seen = new Set<string>()
    const dates: string[] = []
    for (const leg of summaryLegs) {
      const label = fmtDate(leg.departure_time)
      if (!seen.has(label)) {
        seen.add(label)
        dates.push(label)
      }
    }
    return dates
  }, [summaryLegs])
  const summaryAirline = offer.is_combo && offer.inbound?.airline && offer.inbound.airline !== offer.airline
    ? `${offer.airline} + ${offer.inbound.airline}`
    : offer.airline
  const displayFlightNumber = offer.flight_number && offer.flight_number !== offer.airline_code
    ? offer.flight_number
    : ''

  // Start in 'checking' — we always verify unlock status on mount.
  const [step, setStep] = useState<CheckoutStep>({ type: 'checking' })
  const [bookingUrl, setBookingUrl] = useState<string | null>(null)
  const [bookingSite, setBookingSite] = useState<string | null>(null)
  const [bookingOptions, setBookingOptions] = useState<BookingOption[]>([])
  const [bookingLinkStatus, setBookingLinkStatus] = useState<'idle' | 'loading' | 'error'>('idle')
  const [uploadedFile, setUploadedFile] = useState<File | null>(null)
  const [previewUrl, setPreviewUrl] = useState<string | null>(null)
  const [shareError, setShareError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const bookingLinkTrackedRef = useRef(false)
  const splitBookingLegs = useMemo<SplitBookingLeg[]>(() => {
    if (tripBreakdown.length <= 1 || (!offer.is_combo && bookingOptions.length === 0)) {
      return []
    }

    const bookingOptionByLeg = new Map(bookingOptions.map((option) => [option.leg, option]))

    return tripBreakdown.map((leg) => {
      const option = bookingOptionByLeg.get(leg.leg)
      return {
        ...leg,
        price: leg.price ?? option?.price,
        currency: leg.currency ?? option?.currency ?? offer.currency,
        booking_url: option?.booking_url,
        booking_site: option?.booking_site,
      }
    })
  }, [bookingOptions, offer.currency, offer.is_combo, tripBreakdown])

  const isUnlocked = step.type === 'unlocked'
  const isLoading = step.type === 'checking' || step.type === 'verifying-payment'

  const getLegTitle = useCallback((leg: 'outbound' | 'inbound') => (
    leg === 'outbound' ? 'Flight there' : 'Flight back'
  ), [])

  const getLegButtonLabel = useCallback((leg: 'outbound' | 'inbound', airline: string) => (
    leg === 'outbound' ? `Book flight there on ${airline}` : `Book return flight on ${airline}`
  ), [])

  const getLegStops = useCallback((leg: TripBreakdownLeg | BookingOption) => (
    leg.leg === 'inbound' ? offer.inbound?.stops ?? 0 : offer.stops
  ), [offer.inbound?.stops, offer.stops])

  const getLegCityLabel = useCallback((leg: TripBreakdownLeg, endpoint: 'origin' | 'destination') => {
    const code = endpoint === 'origin' ? leg.origin : leg.destination
    if (code === offer.origin) return offer.origin_name
    if (code === offer.destination) return offer.destination_name
    return code
  }, [offer.destination, offer.destination_name, offer.origin, offer.origin_name])

  const getLockedLegButtonLabel = useCallback((leg: 'outbound' | 'inbound') => (
    leg === 'outbound' ? t('unlockOutboundBookingLink') : t('unlockReturnBookingLink')
  ), [t])

  const getLegRouteLabel = useCallback((leg: TripBreakdownLeg | BookingOption) => {
    const departureDate = leg.departure_time ? fmtDate(leg.departure_time) : ''
    const departureTime = leg.departure_time ? fmtTime(leg.departure_time) : '--:--'
    const arrivalTime = leg.arrival_time
      ? fmtTime(leg.arrival_time)
      : '--:--'
    const route = `${leg.origin || '--'} ${departureTime} -> ${leg.destination || '--'} ${arrivalTime}`
    return departureDate ? `${departureDate} · ${route}` : route
  }, [])

  const checkUnlockStatus = useCallback(async (): Promise<boolean> => {
    if (!searchId) return false

    const unlockToken = readStoredUnlockToken(searchId)

    try {
      const res = await fetch(`/api/unlock-status?searchId=${encodeURIComponent(searchId)}`, {
        cache: 'no-store',
        credentials: 'same-origin',
        headers: unlockToken ? { [UNLOCK_TOKEN_HEADER_NAME]: unlockToken } : undefined,
      })
      if (!res.ok) {
        return false
      }

      const data = await res.json() as { unlocked?: boolean }
      return data.unlocked === true
    } catch {
      return false
    }
  }, [searchId])

  const loadBookingLink = useCallback(async (): Promise<boolean> => {
    if (!searchId) return false

    setBookingLinkStatus('loading')
    try {
      const unlockToken = readStoredUnlockToken(searchId)
      // Resolve offer_ref: use the one on the offer, or fetch a fresh one if missing.
      let resolvedOfferRef = offer.offer_ref || offerRef || undefined
      if (!resolvedOfferRef) {
        const snapshotParams = new URLSearchParams({ from: searchId })
        appendProbeParam(snapshotParams, isTestSearch)
        if (offerRef) {
          snapshotParams.set('ref', offerRef)
        }
        try {
          const offerRes = await fetch(
            `/api/offer/${encodeURIComponent(offer.id)}?${snapshotParams.toString()}`,
            {
              cache: 'no-store',
              credentials: 'same-origin',
            },
          )
          if (offerRes.ok) {
            const offerData = await offerRes.json() as { offer_ref?: string }
            resolvedOfferRef = offerData.offer_ref
          }
        } catch {
          // Best-effort — proceed without offer_ref
        }
      }

      const params = new URLSearchParams({
        from: searchId,
        view: 'booking-link',
      })
      appendProbeParam(params, isTestSearch)
      if (resolvedOfferRef) {
        params.set('ref', resolvedOfferRef)
      }
      let res = await fetch(
        `/api/offer/${encodeURIComponent(offer.id)}?${params.toString()}`,
        {
          cache: 'no-store',
          credentials: 'same-origin',
          headers: unlockToken ? { [UNLOCK_TOKEN_HEADER_NAME]: unlockToken } : undefined,
        },
      )

      if (!res.ok && res.status === 404) {
        const latestOfferRef = await fetchLatestOfferRef(searchId, offer.id, isTestSearch)
        if (latestOfferRef && latestOfferRef !== resolvedOfferRef) {
          resolvedOfferRef = latestOfferRef
          const retryParams = new URLSearchParams({
            from: searchId,
            view: 'booking-link',
          })
          appendProbeParam(retryParams, isTestSearch)
          retryParams.set('ref', latestOfferRef)
          res = await fetch(
            `/api/offer/${encodeURIComponent(offer.id)}?${retryParams.toString()}`,
            {
              cache: 'no-store',
              credentials: 'same-origin',
              headers: unlockToken ? { [UNLOCK_TOKEN_HEADER_NAME]: unlockToken } : undefined,
            },
          )
        }
      }

      if (!res.ok) {
        setBookingUrl(null)
        setBookingSite(null)
        setBookingLinkStatus('error')
        return false
      }

      const data = await res.json() as {
        booking_url?: string
        booking_site?: string
        booking_site_summary?: string
        booking_options?: unknown[]
      }
      const options = Array.isArray(data.booking_options)
        ? data.booking_options.filter((option: unknown): option is BookingOption => {
            if (!option || typeof option !== 'object') return false
            const candidate = option as Record<string, unknown>
            return (
              (candidate.leg === 'outbound' || candidate.leg === 'inbound')
              && typeof candidate.airline === 'string'
              && typeof candidate.airline_code === 'string'
              && typeof candidate.booking_url === 'string'
              && candidate.booking_url.length > 0
              && (candidate.booking_site === undefined || typeof candidate.booking_site === 'string')
            )
          })
        : []
      const primaryBookingUrl = typeof data.booking_url === 'string' ? data.booking_url : ''
      const primaryBookingSite = typeof data.booking_site_summary === 'string' && data.booking_site_summary.trim().length > 0
        ? data.booking_site_summary.trim()
        : typeof data.booking_site === 'string' && data.booking_site.trim().length > 0
          ? data.booking_site.trim()
          : ''

      if (!primaryBookingUrl && options.length === 0) {
        setBookingUrl(null)
        setBookingSite(null)
        setBookingOptions([])
        setBookingLinkStatus('error')
        return false
      }

      setBookingUrl(primaryBookingUrl || options[0].booking_url)
      setBookingSite(primaryBookingSite || options[0]?.booking_site || null)
      setBookingOptions(options)
      setBookingLinkStatus('idle')
      if (!bookingLinkTrackedRef.current) {
        bookingLinkTrackedRef.current = true
        trackSearchSessionEvent(analyticsSearchId, 'booking_link_ready', {
          offer_id: offer.id,
        }, {
          source: 'website-checkout',
          source_path: checkoutSourcePath,
          is_test_search: isTestSearch || undefined,
        })
      }
      return true
    } catch {
      setBookingUrl(null)
      setBookingSite(null)
      setBookingOptions([])
      setBookingLinkStatus('error')
      return false
    }
  }, [analyticsSearchId, checkoutSourcePath, isTestSearch, offer.id, offer.offer_ref, offerRef, searchId])

  const loadUnlockedBookingLink = useCallback(async (): Promise<boolean> => {
    for (const delayMs of [0, 200, 600, 1200]) {
      if (delayMs > 0) {
        await wait(delayMs)
      }

      if (!(await checkUnlockStatus())) {
        continue
      }

      if (await loadBookingLink()) {
        return true
      }
    }

    return false
  }, [checkUnlockStatus, loadBookingLink])

  useEffect(() => {
    trackSearchSessionEvent(analyticsSearchId, 'checkout_opened', {
      offer_id: offer.id,
      airline: offer.airline,
      currency: offer.currency,
      price: offer.price,
    }, {
      source: 'website-checkout',
      source_path: checkoutSourcePath,
      is_test_search: isTestSearch || undefined,
      selected_offer_id: offer.id,
      selected_offer_airline: offer.airline,
      selected_offer_currency: offer.currency,
      selected_offer_price: offer.price,
    })
  }, [analyticsSearchId, checkoutSourcePath, isTestSearch, offer.airline, offer.currency, offer.id, offer.price])

  useEffect(() => {
    const handlePageHide = () => {
      trackSearchSessionEvent(analyticsSearchId, 'pagehide_checkout', {
        offer_id: offer.id,
        step: step.type,
      }, {
        source: 'website-checkout',
        source_path: checkoutSourcePath,
        is_test_search: isTestSearch || undefined,
      }, { beacon: true })
    }

    window.addEventListener('pagehide', handlePageHide)
    return () => window.removeEventListener('pagehide', handlePageHide)
  }, [analyticsSearchId, checkoutSourcePath, isTestSearch, offer.id, step.type])

  // ── On mount: verify payment redirect OR check stored unlock ────────────
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const stripeSession = params.get('stripe_session')

    if (stripeSession) {
      // Returned from Stripe — verify the payment server-side
      setStep({ type: 'verifying-payment' })
      fetch('/api/checkout/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ stripeSessionId: stripeSession }),
      })
        .then(r => r.json())
        .then(async (data: { unlocked: boolean; unlockToken?: string }) => {
          if (data.unlocked) {
            persistUnlockToken(searchId, data.unlockToken)
            setStep({ type: 'unlocked', via: 'payment' })
            trackSearchSessionEvent(analyticsSearchId, 'payment_verified', {
              offer_id: offer.id,
            }, {
              source: 'website-checkout',
              source_path: checkoutSourcePath,
              is_test_search: isTestSearch || undefined,
              revenue: fee,
            })
            await loadUnlockedBookingLink()
            // Clean the stripe_session param from the URL without a reload
            const url = new URL(window.location.href)
            url.searchParams.delete('stripe_session')
            window.history.replaceState({}, '', url.toString())
          } else {
            setStep({ type: 'locked' })
          }
        })
        .catch(() => setStep({ type: 'locked' }))
      return
    }

    if (!searchId) {
      setStep({ type: 'locked' })
      return
    }

    // Server-side unlock check — always authoritative
    checkUnlockStatus()
      .then(async (unlocked) => {
        if (unlocked) {
          setStep({ type: 'unlocked', via: 'existing' })
          trackSearchSessionEvent(analyticsSearchId, 'existing_unlock', {
            offer_id: offer.id,
          }, {
            source: 'website-checkout',
            source_path: checkoutSourcePath,
            is_test_search: isTestSearch || undefined,
          })
          await loadUnlockedBookingLink()
          return
        }
        setStep({ type: 'locked' })
      })
      .catch(() => setStep({ type: 'locked' }))
  }, [analyticsSearchId, checkUnlockStatus, checkoutSourcePath, fee, isTestSearch, loadUnlockedBookingLink, offer.id, searchId])

  // ── Pay via Stripe ───────────────────────────────────────────────────────
  const handlePay = useCallback(async () => {
    trackSearchSessionEvent(analyticsSearchId, 'payment_attempted', {
      offer_id: offer.id,
      airline: offer.airline,
      currency: offer.currency,
      price: offer.price,
    }, {
      source: 'website-checkout',
      source_path: checkoutSourcePath,
      is_test_search: isTestSearch || undefined,
      potential_revenue: fee,
    })
    setStep({ type: 'paying' })
    try {
      const res = await fetch('/api/checkout/create-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          offerId: offer.id,
          searchId: searchId ?? '',
          probe: isTestSearch ? '1' : undefined,
        }),
      })
      const data = await res.json()
      if (data.url) {
        window.location.href = data.url
      } else {
        setStep({ type: 'locked' })
      }
    } catch {
      setStep({ type: 'locked' })
    }
  }, [analyticsSearchId, checkoutSourcePath, fee, isTestSearch, offer, searchId])

  const handleSelectPlatform = useCallback((platform: Platform) => {
    trackSearchSessionEvent(analyticsSearchId, 'share_selected', {
      platform: platform.id,
      offer_id: offer.id,
    }, {
      source: 'website-checkout',
      source_path: checkoutSourcePath,
      is_test_search: isTestSearch || undefined,
    })
    setShareError(null)
    setStep({ type: 'share-upload', platform })
    setUploadedFile(null)
    setPreviewUrl(null)
  }, [analyticsSearchId, checkoutSourcePath, isTestSearch, offer.id])

  const handleFileChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setShareError(null)
    setUploadedFile(file)
    setPreviewUrl(URL.createObjectURL(file))
  }, [])

  const handlePaste = useCallback((e: React.ClipboardEvent | ClipboardEvent) => {
    const items = (e as React.ClipboardEvent).clipboardData?.items
      ?? (e as ClipboardEvent).clipboardData?.items
    if (!items) return
    for (const item of Array.from(items)) {
      if (item.type.startsWith('image/')) {
        const file = item.getAsFile()
        if (!file) continue
        e.preventDefault()
        setShareError(null)
        setUploadedFile(file)
        setPreviewUrl(URL.createObjectURL(file))
        break
      }
    }
  }, [])

  const handleVerify = useCallback(async () => {
    if (!uploadedFile || step.type !== 'share-upload') return
    const platform = step.platform
    trackSearchSessionEvent(analyticsSearchId, 'share_verification_submitted', {
      platform: platform.id,
      offer_id: offer.id,
    }, {
      source: 'website-checkout',
      source_path: checkoutSourcePath,
      is_test_search: isTestSearch || undefined,
    })
    setShareError(null)
    setStep({ type: 'share-verifying', platform })
    try {
      const form = new FormData()
      form.append('searchId', searchId ?? '')
      form.append('image', uploadedFile)
      const res = await fetch('/api/checkout/verify-share', {
        method: 'POST',
        credentials: 'same-origin',
        body: form,
      })
      let data: { unlocked?: boolean; error?: string; unlockToken?: string } = {}
      try {
        data = await res.json()
      } catch {
        data = {}
      }
      if (data.unlocked) {
        persistUnlockToken(searchId, data.unlockToken)
        setShareError(null)
        setStep({ type: 'unlocked', via: 'share' })
        trackSearchSessionEvent(analyticsSearchId, 'share_unlocked', {
          platform: platform.id,
          offer_id: offer.id,
        }, {
          source: 'website-checkout',
          source_path: checkoutSourcePath,
          is_test_search: isTestSearch || undefined,
        })
        await loadUnlockedBookingLink()
      } else {
        setShareError(data.error ?? null)
        setStep({ type: 'share-rejected', platform })
      }
    } catch {
      setShareError('Verification failed. Please try again.')
      setStep({ type: 'share-rejected', platform })
    }
  }, [analyticsSearchId, checkoutSourcePath, isTestSearch, loadUnlockedBookingLink, offer.id, searchId, step, uploadedFile])

  const handleRetryShare = useCallback(() => {
    setShareError(null)
    setStep({ type: 'share-select' })
    setUploadedFile(null)
    setPreviewUrl(null)
  }, [])

  // ── Global paste listener (Ctrl+V anywhere on the page) ─────────────────
  useEffect(() => {
    if (step.type !== 'share-upload') return
    const onPaste = (e: ClipboardEvent) => handlePaste(e)
    window.addEventListener('paste', onPaste)
    return () => window.removeEventListener('paste', onPaste)
  }, [step.type, handlePaste])

  return (
    <div className="ck-page">
      <div className="ck-inner">

        {/* ── Flight summary card ─────────────────────────────────────────── */}
        <div className="ck-flight-card">
          <div className="ck-flight-header">
            <AirlineLogo code={offer.airline_code} name={offer.airline} />
            <div className="ck-flight-airline">
              <span className="ck-airline-name">{summaryAirline}</span>
              {displayFlightNumber && <span className="ck-flight-num">{displayFlightNumber}</span>}
            </div>
            <div className="ck-flight-price-badge">
              <span className="ck-flight-price">{offer.currency}{Math.round(withFee(offer.price, offer.currency))}</span>
              <span className="ck-flight-price-label">{t('perPerson')}</span>
            </div>
          </div>

          <div className="ck-flight-routes">
            {summaryLegs.map((leg) => {
              const stops = getLegStops(leg)
              const durationLabel = leg.duration_minutes > 0 ? fmtDuration(leg.duration_minutes) : '--'
              const arrivalLabel = leg.duration_minutes > 0 || leg.arrival_time !== leg.departure_time
                ? fmtTime(leg.arrival_time)
                : '--:--'

              return (
                <div className="ck-flight-route-block" key={`${leg.leg}-${leg.departure_time}-${leg.arrival_time}`}>
                  {summaryLegs.length > 1 && (
                    <div className="ck-flight-route-topline">
                      <span className="ck-leg-label">{getLegTitle(leg.leg)}</span>
                      <span className="ck-flight-route-date">{fmtDate(leg.departure_time)}</span>
                    </div>
                  )}

                  <div className="ck-flight-route">
                    <div className="ck-endpoint">
                      <span className="ck-time">{fmtTime(leg.departure_time)}</span>
                      <span className="ck-iata">{leg.origin}</span>
                      <span className="ck-city">{getLegCityLabel(leg, 'origin')}</span>
                    </div>

                    <div className="ck-path">
                      <span className="ck-duration">{durationLabel}</span>
                      <div className="ck-path-line">
                        <span className="ck-path-dot" />
                        <span className="ck-path-track" />
                        {stops === 0 && <span className="ck-direct-label">Direct</span>}
                        {stops > 0 && <span className="ck-stop-dot" />}
                        <span className="ck-path-track" />
                        <span className="ck-path-dot" />
                      </div>
                      {stops > 0 && (
                        <span className="ck-stops-label">{stops} stop{stops > 1 ? 's' : ''}</span>
                      )}
                    </div>

                    <div className="ck-endpoint ck-endpoint--right">
                      <span className="ck-time">{arrivalLabel}</span>
                      <span className="ck-iata">{leg.destination}</span>
                      <span className="ck-city">{getLegCityLabel(leg, 'destination')}</span>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>

          <div className="ck-flight-meta">
            <span>{summaryDates.join(' · ')}</span>
            <span className="ck-meta-dot">·</span>
            <span>{t('onePassenger')}</span>
            <span className="ck-meta-dot">·</span>
            <span>{t('economy')}</span>
          </div>
        </div>

        {/* ── Unlocked success banner ─────────────────────────────────────── */}
        {isLoading && (
          <div className="ck-checking-banner">
            <span className="ck-spinner ck-spinner--sm" aria-hidden="true" />
            <span className="ck-checking-text">
              {step.type === 'verifying-payment' ? t('verifyingPayment') : t('checkingUnlock')}
            </span>
          </div>
        )}

        {isUnlocked && (
          <div className="ck-unlocked-banner">
            <span className="ck-unlocked-check"><CheckIcon /></span>
            <div>
              <div className="ck-unlocked-title">
                {step.via === 'share'
                  ? t('dealUnlockedShare')
                  : step.via === 'existing'
                  ? t('dealUnlockedExisting')
                  : t('dealUnlocked')}
              </div>
              <div className="ck-unlocked-sub">
                {t('bookingLinkReady')}
              </div>
              {bookingSite && (
                <div className="ck-unlocked-source">Deal from {bookingSite}</div>
              )}
            </div>
          </div>
        )}

        {/* ── Checkout card ───────────────────────────────────────────────── */}
        <div className="ck-checkout-card">

          {/* ── STEP 1: Unlock ──────────────────────────────────────────── */}
          <div className={`ck-step${isUnlocked ? ' ck-step--done' : ''}${isLoading ? ' ck-step--loading' : ''}`}>
            <div className="ck-step-label">
              <span className={`ck-step-num${isUnlocked ? ' ck-step-num--done' : ''}`}>
                {isUnlocked ? <CheckIcon /> : '1'}
              </span>
              <span className="ck-step-title">
                {isUnlocked ? t('dealUnlockedStep') : t('unlockThisDeal')}
              </span>
            </div>

            {!isUnlocked && !isLoading && (
              <div className="ck-unlock-body">
                <p className="ck-unlock-desc">
                  {t.rich('unlockDesc', { strong: (chunks) => <strong>{chunks}</strong> })}
                </p>

                {/* Pay button */}
                <button
                  className={`ck-pay-btn${step.type === 'paying' ? ' ck-pay-btn--loading' : ''}`}
                  onClick={handlePay}
                  disabled={step.type === 'paying'}
                >
                  {step.type === 'paying' ? (
                    <>
                      <span className="ck-spinner" aria-hidden="true" />
                      {t('processing')}
                    </>
                  ) : (
                    <>
                      <LockIcon />
                      {t('unlockFor', { fee: fmtFee(fee, offer.currency) })}
                    </>
                  )}
                </button>

                <div className="ck-fee-note">
                  {t('oneTime')}
                </div>

                {/* Share to unlock (only if fee < $20) */}
                {showShareOption && step.type !== 'paying' && (
                  <>
                    <div className="ck-or-divider">
                      <span>{t('shareToUnlock')}</span>
                    </div>

                    {/* Platform select */}
                    {(step.type === 'locked' || step.type === 'share-select' || step.type === 'share-rejected') && (
                      <div className="ck-share-intro">
                        <p className="ck-share-desc">
                          {t('shareDesc')}
                        </p>
                        <div className="ck-platform-grid">
                          {platforms.map(p => (
                            <button
                              key={p.id}
                              className="ck-platform-btn"
                              onClick={() => handleSelectPlatform(p)}
                            >
                              <span className="ck-platform-icon">{PLATFORM_ICONS[p.id]}</span>
                              {p.label}
                            </button>
                          ))}
                        </div>
                        {step.type === 'share-rejected' && (
                          <div className="ck-share-rejected">
                            <span>⚠</span> {shareError ?? t('screenshotInvalid')}
                          </div>
                        )}
                      </div>
                    )}

                    {/* Screenshot upload step */}
                    {step.type === 'share-upload' && (
                      <div className="ck-share-upload">
                        <div className="ck-share-platform-header">
                          <span className="ck-platform-icon">{PLATFORM_ICONS[step.platform.id]}</span>
                          <span className="ck-share-platform-name">{step.platform.label}</span>
                          <button className="ck-share-back" onClick={() => {
                            setShareError(null)
                            setStep({ type: 'share-select' })
                          }}>
                            {t('change')}
                          </button>
                        </div>
                        <ol className="ck-share-steps">
                          {step.platform.instructions.map((inst, i) => (
                            <li key={i}>{inst}</li>
                          ))}
                        </ol>

                        {/* File drop zone */}
                        <div
                          className={`ck-upload-zone${previewUrl ? ' ck-upload-zone--filled' : ''}`}
                          onClick={() => fileInputRef.current?.click()}
                          onKeyDown={e => e.key === 'Enter' && fileInputRef.current?.click()}
                          onPaste={handlePaste}
                          role="button"
                          tabIndex={0}
                          aria-label={t('uploadAriaLabel')}
                        >
                          {previewUrl ? (
                            // eslint-disable-next-line @next/next/no-img-element
                            <img src={previewUrl} alt="Screenshot preview" className="ck-upload-preview" />
                          ) : (
                            <div className="ck-upload-prompt">
                              <svg viewBox="0 0 24 24" fill="none" width="28" height="28" aria-hidden="true">
                                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M17 8l-5-5-5 5M12 3v12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                              </svg>
                              <span>{t('uploadLabel')}</span>
                              <span className="ck-upload-hint">{t('uploadHint')}</span>
                            </div>
                          )}
                        </div>

                        <input
                          ref={fileInputRef}
                          type="file"
                          accept="image/*"
                          className="ck-file-input"
                          onChange={handleFileChange}
                          aria-label="Upload share screenshot"
                        />

                        <button
                          className="ck-verify-btn"
                          onClick={handleVerify}
                          disabled={!uploadedFile}
                        >
                          {t('submitVerification')}
                        </button>
                      </div>
                    )}

                    {/* Verifying */}
                    {step.type === 'share-verifying' && (
                      <div className="ck-share-verifying">
                        <span className="ck-spinner ck-spinner--lg" aria-hidden="true" />
                        <div>
                          <div className="ck-verifying-title">{t('verifyingTitle')}</div>
                          <div className="ck-verifying-sub">{t('verifySub')}</div>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </div>
            )}
          </div>

          <div className="ck-step-divider" />

          {/* ── STEP 2: Book ticket ─────────────────────────────────────── */}
          <div className={`ck-step${isUnlocked ? '' : ' ck-step--locked-section'}`}>
            <div className="ck-step-label">
              <span className={`ck-step-num${isUnlocked ? ' ck-step-num--active' : ''}`}>2</span>
              <span className="ck-step-title">{t('bookTicket')}</span>
            </div>

            <div className="ck-book-body">
              <div className="ck-price-breakdown">
                <div className="ck-price-row">
                  <span className="ck-price-label">{t('airlineTicket')}</span>
                  <span className="ck-price-value">{offer.currency}{offer.price}</span>
                </div>
                <div className="ck-price-row">
                  <span className="ck-price-label">{t('letsfgFee')}</span>
                  <span className="ck-price-value">{fmtFee(calculateFee(offer.price, offer.currency), offer.currency)}</span>
                </div>
                <div className="ck-price-row ck-price-row--total">
                  <span className="ck-price-label">{t('total')}</span>
                  <span className="ck-price-value">{offer.currency}{Math.round(withFee(offer.price, offer.currency))}</span>
                </div>
              </div>

              {splitBookingLegs.length > 0 ? (
                <div className="ck-book-actions">
                  {splitBookingLegs.map((leg) => {
                    const legPrice = typeof leg.price === 'number' ? leg.price : null
                    const hasBookingUrl = typeof leg.booking_url === 'string' && leg.booking_url.length > 0

                    return (
                      <div className="ck-book-action-card" key={`${leg.leg}-${leg.airline}-${leg.departure_time}`}>
                        <div className="ck-book-action-meta">
                          <div className="ck-book-action-copy">
                            <span className="ck-book-action-title">{getLegTitle(leg.leg)}</span>
                            <span className="ck-leg-airline">{leg.airline}</span>
                            <span className="ck-book-action-subtitle">{getLegRouteLabel(leg)}</span>
                            {leg.booking_site && (
                              <span className="ck-book-action-site">Book via {leg.booking_site}</span>
                            )}
                          </div>
                          <span className={`ck-book-action-price${legPrice !== null ? '' : ' ck-leg-price--muted'}`}>
                            {legPrice !== null ? fmtMoney(legPrice, leg.currency || offer.currency) : 'Included in total'}
                          </span>
                        </div>

                        {isUnlocked ? (
                          hasBookingUrl ? (
                            <a
                              href={leg.booking_url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="ck-book-btn ck-book-btn--active"
                              onClick={() => trackSearchSessionEvent(analyticsSearchId, 'booking_link_opened', {
                                offer_id: offer.id,
                                airline: leg.airline,
                                leg: leg.leg,
                              }, {
                                source: 'website-checkout',
                                source_path: checkoutSourcePath,
                                is_test_search: isTestSearch || undefined,
                                decision: 'booking_link_opened',
                              }, { keepalive: true })}
                            >
                              {getLegButtonLabel(leg.leg, leg.airline)}
                              <ArrowIcon />
                            </a>
                          ) : (
                            <button className="ck-book-btn ck-book-btn--locked" disabled aria-disabled="true">
                              {bookingLinkStatus === 'loading' ? t('processing') : getLegButtonLabel(leg.leg, leg.airline)}
                            </button>
                          )
                        ) : (
                          <button className="ck-book-btn ck-book-btn--locked" disabled aria-disabled="true">
                            <LockIcon />
                            {getLockedLegButtonLabel(leg.leg)}
                          </button>
                        )}
                      </div>
                    )
                  })}
                </div>
              ) : tripBreakdown.length > 1 && (
                <div className="ck-leg-breakdown">
                  {tripBreakdown.map((leg) => (
                    <div className="ck-leg-row" key={`${leg.leg}-${leg.airline}-${leg.departure_time}`}>
                      <div className="ck-leg-copy">
                        <span className="ck-leg-label">{getLegTitle(leg.leg)}</span>
                        <span className="ck-leg-airline">{leg.airline}</span>
                        <span className="ck-leg-route">{getLegRouteLabel(leg)}</span>
                      </div>
                      <div className="ck-leg-price-wrap">
                        <span className={`ck-leg-price${typeof leg.price === 'number' ? '' : ' ck-leg-price--muted'}`}>
                          {typeof leg.price === 'number'
                            ? fmtMoney(leg.price, leg.currency || offer.currency)
                            : 'Included in total'}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {splitBookingLegs.length > 0 ? null : isUnlocked && bookingOptions.length > 0 ? (
                <div className="ck-book-actions">
                  {bookingOptions.map((option) => (
                    <div className="ck-book-action-card" key={`${option.leg}-${option.airline}-${option.booking_url}`}>
                      {(option.origin || option.destination) && (
                        <div className="ck-book-action-meta">
                          <div className="ck-book-action-copy">
                            <span className="ck-book-action-title">{getLegTitle(option.leg)}</span>
                            <span className="ck-book-action-subtitle">{getLegRouteLabel(option)}</span>
                            {option.booking_site && (
                              <span className="ck-book-action-site">Book via {option.booking_site}</span>
                            )}
                          </div>
                          {typeof option.price === 'number' && (
                            <span className="ck-book-action-price">{fmtMoney(option.price, option.currency || offer.currency)}</span>
                          )}
                        </div>
                      )}
                      <a
                        href={option.booking_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="ck-book-btn ck-book-btn--active"
                        onClick={() => trackSearchSessionEvent(analyticsSearchId, 'booking_link_opened', {
                          offer_id: offer.id,
                          airline: option.airline,
                          leg: option.leg,
                        }, {
                          source: 'website-checkout',
                          source_path: checkoutSourcePath,
                          is_test_search: isTestSearch || undefined,
                          decision: 'booking_link_opened',
                        }, { keepalive: true })}
                      >
                        {getLegButtonLabel(option.leg, option.airline)}
                        <ArrowIcon />
                      </a>
                    </div>
                  ))}
                </div>
              ) : isUnlocked && bookingUrl ? (
                  <a
                  href={bookingUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="ck-book-btn ck-book-btn--active"
                    onClick={() => trackSearchSessionEvent(analyticsSearchId, 'booking_link_opened', {
                      offer_id: offer.id,
                      airline: offer.airline,
                    }, {
                      source: 'website-checkout',
                      source_path: checkoutSourcePath,
                      is_test_search: isTestSearch || undefined,
                      decision: 'booking_link_opened',
                    }, { keepalive: true })}
                >
                  {t('bookOn', { airline: offer.airline })}
                  <ArrowIcon />
                </a>
              ) : isUnlocked ? (
                <>
                  {tripBreakdown.length > 1 ? (
                    <div className="ck-book-actions">
                      {tripBreakdown.map((leg) => (
                        <button key={`${leg.leg}-${leg.airline}`} className="ck-book-btn ck-book-btn--locked" disabled aria-disabled="true">
                          {bookingLinkStatus === 'loading' ? t('processing') : getLegButtonLabel(leg.leg, leg.airline)}
                        </button>
                      ))}
                    </div>
                  ) : (
                    <button className="ck-book-btn ck-book-btn--locked" disabled aria-disabled="true">
                      {bookingLinkStatus === 'loading' ? t('processing') : t('bookOn', { airline: summaryAirline })}
                    </button>
                  )}
                  <div className="ck-book-locked-note">
                    {bookingLinkStatus === 'error' ? t('unlockFirst') : t('processing')}
                  </div>
                </>
              ) : splitBookingLegs.length > 0 ? null : (
                <>
                  {tripBreakdown.length > 1 ? (
                    <div className="ck-book-actions">
                      {tripBreakdown.map((leg) => (
                        <button key={`${leg.leg}-${leg.airline}`} className="ck-book-btn ck-book-btn--locked" disabled aria-disabled="true">
                          <LockIcon />
                          {getLockedLegButtonLabel(leg.leg)}
                        </button>
                      ))}
                    </div>
                  ) : (
                    <button className="ck-book-btn ck-book-btn--locked" disabled aria-disabled="true">
                      <LockIcon />
                      {t('unlockBookingLink')}
                    </button>
                  )}
                  <div className="ck-book-locked-note">
                    {t('unlockFirst')}
                  </div>
                </>
              )}

              <div className="ck-guarantee-row">
                <span className="ck-guarantee-item">
                  <CheckIcon /> {t('rawAirlinePrice')}
                </span>
                <span className="ck-guarantee-item">
                  <CheckIcon /> {t('secureCheckout')}
                </span>
                <span className="ck-guarantee-item">
                  <CheckIcon /> {t('noHiddenFees')}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* ── Trust footer ────────────────────────────────────────────────── */}
        <div className="ck-trust-footer">
          <a
            href={homeHref}
            target="_blank"
            rel="noreferrer"
            className="ck-trust-link ck-trust-brand"
            onClick={() => trackSearchSessionEvent(analyticsSearchId, 'navigate_home', {}, {
              source: 'website-checkout',
              source_path: checkoutSourcePath,
              is_test_search: isTestSearch || undefined,
            }, { keepalive: true })}
          >LetsFG</a>
          <span className="ck-meta-dot">·</span>
          <a href="https://instagram.com/letsfg_" target="_blank" rel="noreferrer" className="ck-trust-link">Instagram</a>
          <span className="ck-meta-dot">·</span>
          <a href="https://www.tiktok.com/@letsfg_" target="_blank" rel="noreferrer" className="ck-trust-link">TikTok</a>
          <span className="ck-meta-dot">·</span>
          <a href="https://twitter.com/LetsFG_" target="_blank" rel="noreferrer" className="ck-trust-link">Twitter / X</a>
        </div>

      </div>
    </div>
  )
}
