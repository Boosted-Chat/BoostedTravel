'use client'

import { useState, useMemo, useCallback, useEffect } from 'react'
import { useLocale, useTranslations } from 'next-intl'
import { getAirlineCodeFromName, getAirlineLogoUrl, getAirlineNameFromCode, looksLikeIataCode } from '../../airlineLogos'
import { extractFlightClockMinutes, formatFlightDateCompact, formatFlightTime } from '../../../lib/flight-datetime'
import { formatGoogleFlightsSavings, getGoogleFlightsSavingsAmount } from '../../../lib/google-flights-savings'
import { trackSearchSessionEvent } from '../../../lib/search-session-analytics'
import {
  getOfferKnownTotalPrice,
  getOfferTotalWithAncillary,
  hasIncludedAncillary,
  hasPaidAncillary,
} from '../../../lib/offer-pricing'
import { appendProbeParam, getTrackedSourcePath } from '../../../lib/probe-mode'

// ── Types ─────────────────────────────────────────────────────────────────────
interface FlightSegment {
  airline?: string
  airline_code?: string
  origin: string
  origin_name: string
  destination: string
  destination_name: string
  departure_time: string
  arrival_time: string
  flight_number: string
  duration_minutes: number
  layover_minutes: number
}

interface InboundLeg {
  origin: string
  destination: string
  departure_time: string
  arrival_time: string
  duration_minutes: number
  stops: number
  airline?: string
  airline_code?: string
  segments?: FlightSegment[]
}

interface OfferAncillary {
  included?: boolean
  price?: number
  currency?: string
  description?: string
}

interface OfferAncillaries {
  cabin_bag?: OfferAncillary
  checked_bag?: OfferAncillary
  seat_selection?: OfferAncillary
}

interface FlightOffer {
  id: string
  price: number
  google_flights_price?: number
  offer_ref?: string
  currency: string
  airline: string
  airline_code: string
  flight_number?: string
  is_combo?: boolean
  origin: string
  origin_name: string
  destination: string
  destination_name: string
  departure_time: string
  arrival_time: string
  duration_minutes: number
  stops: number
  segments?: FlightSegment[]
  inbound?: InboundLeg
  ancillaries?: OfferAncillaries
}

interface SourceMetaResponse {
  booking_site?: string
  booking_site_summary?: string
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtDuration(mins: number) {
  return `${Math.floor(mins / 60)}h ${mins % 60}m`
}

function isoToMins(iso: string) {
  return extractFlightClockMinutes(iso)
}

function minsToLabel(m: number) {
  return `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`
}

function extractIataFromFlightNo(flightNo: string) {
  const match = flightNo.match(/^([A-Z]{2}|[A-Z]\d|\d[A-Z])/i)
  return match ? match[1].toUpperCase() : ''
}

interface OfferCarrier {
  name: string
  code: string
}

interface RouteStop {
  code: string
  name: string
}

function isMeaningfulCarrierName(value: string) {
  if (!value) return false
  const normalized = value.trim()
  if (!normalized || normalized === '??') return false
  return normalized.toLowerCase() !== 'unknown'
}

function resolveCarrier(name: unknown, code: unknown, flightNumber?: unknown): OfferCarrier | null {
  const normalizedName = typeof name === 'string' ? name.trim() : ''
  const normalizedCode = typeof code === 'string' ? code.trim().toUpperCase() : ''
  const flightCode = typeof flightNumber === 'string' ? extractIataFromFlightNo(flightNumber) : ''
  const codeFromField = looksLikeIataCode(normalizedCode) ? normalizedCode : ''
  const codeFromName = looksLikeIataCode(normalizedName) ? normalizedName.toUpperCase() : ''
  const inferredCode = getAirlineCodeFromName(normalizedName) || getAirlineCodeFromName(normalizedCode)
  const resolvedCode = codeFromField || flightCode || codeFromName || inferredCode || ''
  const resolvedName = isMeaningfulCarrierName(normalizedName) && !looksLikeIataCode(normalizedName)
    ? normalizedName
    : (resolvedCode ? getAirlineNameFromCode(resolvedCode) || resolvedCode : '')

  if (!isMeaningfulCarrierName(resolvedName)) {
    return null
  }

  return {
    name: resolvedName,
    code: resolvedCode || getAirlineCodeFromName(resolvedName) || resolvedName.slice(0, 2).toUpperCase(),
  }
}

function getRouteStops(segments?: FlightSegment[]): RouteStop[] {
  const routeStops: RouteStop[] = []
  const seen = new Set<string>()

  for (const segment of segments?.slice(0, -1) || []) {
    const code = (segment.destination || '').trim().toUpperCase()
    const name = (segment.destination_name || segment.destination || '').trim()
    const key = code || name.toLowerCase()

    if (!key || seen.has(key)) {
      continue
    }

    seen.add(key)
    routeStops.push({ code, name: name || code })
  }

  return routeStops
}

function getRouteViaBadge(stops: RouteStop[]) {
  const codes = stops.map((stop) => stop.code || stop.name).filter(Boolean)
  if (codes.length === 0) return null
  if (codes.length === 1) return codes[0]
  return `${codes[0]} +${codes.length - 1}`
}

function getRouteViaTitle(stops: RouteStop[]) {
  if (stops.length === 0) return undefined
  return stops.map((stop) => stop.code && stop.name ? `${stop.code} · ${stop.name}` : stop.code || stop.name).join(', ')
}

function getStopsLabel(stops: number, segments: FlightSegment[] | undefined, directLabel: string) {
  if (stops === 0) return directLabel

  const routeStops = getRouteStops(segments)
  const viaCodes = routeStops.map((stop) => stop.code || stop.name).filter(Boolean)

  if (viaCodes.length === 0) {
    return `${stops} stop${stops > 1 ? 's' : ''}`
  }

  return `${stops} stop${stops > 1 ? 's' : ''} · via ${viaCodes.join(', ')}`
}

function getOfferCarriers(offer: FlightOffer): OfferCarrier[] {
  const carriers: OfferCarrier[] = []
  const seen = new Set<string>()

  const addCarrier = (name: unknown, code: unknown, flightNumber?: unknown) => {
    const carrier = resolveCarrier(name, code, flightNumber)

    if (!carrier) {
      return
    }

    const key = carrier.name.toLowerCase()

    if (seen.has(key)) {
      return
    }

    seen.add(key)
    carriers.push(carrier)
  }

  addCarrier(offer.airline, offer.airline_code, offer.flight_number)

  for (const segment of offer.segments || []) {
    addCarrier(segment.airline, segment.airline_code, segment.flight_number)
  }

  addCarrier(offer.inbound?.airline, offer.inbound?.airline_code)

  for (const segment of offer.inbound?.segments || []) {
    addCarrier(segment.airline, segment.airline_code, segment.flight_number)
  }

  const fallbackCarrier = resolveCarrier(offer.airline, offer.airline_code, offer.flight_number)
  return carriers.length > 0 ? carriers : (fallbackCarrier ? [fallbackCarrier] : [])
}

function getOfferAirlineLabel(offer: FlightOffer) {
  return getOfferCarriers(offer).map((carrier) => carrier.name).join(' + ')
}

function getSegmentAirlineLabel(segment: FlightSegment, fallbackAirline: string) {
  const carrier = resolveCarrier(segment.airline, segment.airline_code, segment.flight_number)
  if (carrier?.name) return carrier.name

  const fallbackCarrier = resolveCarrier(fallbackAirline, '', undefined)
  return fallbackCarrier?.name || fallbackAirline
}

function fmtOfferPrice(amount: number, currency: string) {
  return `${currency}${Math.round(amount)}`
}

// ── Airline logo with IATA-code fallback ──────────────────────────────────────
function AirlineLogo({ code, name }: { code: string; name: string }) {
  const [failed, setFailed] = useState(false)
  const [tapped, setTapped] = useState(false)
  const inner = failed
    ? <div className="rf-airline-badge">{code.slice(0, 2)}</div>
    : (
      <div className="rf-airline-badge rf-airline-badge--img">
        <img
          src={getAirlineLogoUrl(code)}
          alt={name}
          width={28}
          height={28}
          onError={() => setFailed(true)}
        />
      </div>
    )
  return (
    <div
      className={`rf-airline-logo-wrap${tapped ? ' rf-airline-logo-wrap--tapped' : ''}`}
      onClick={() => setTapped((current) => !current)}
      onMouseLeave={() => setTapped(false)}
      title={name}
    >
      {inner}
      <span className="rf-airline-tooltip">{name}</span>
    </div>
  )
}

// ── Dual-handle range slider ──────────────────────────────────────────────────
function DualRange({
  min, max, low, high, onChange, formatLabel,
}: {
  min: number
  max: number
  low: number
  high: number
  onChange: (low: number, high: number) => void
  formatLabel: (v: number) => string
}) {
  const range = max - min || 1
  const loPct = ((low - min) / range) * 100
  const hiPct = ((high - min) / range) * 100

  return (
    <div className="rf-dual">
      <div className="rf-dual-vals">
        <span>{formatLabel(low)}</span>
        <span>{formatLabel(high)}</span>
      </div>
      <div
        className="rf-dual-track"
        style={{ '--lo': `${loPct}%`, '--hi': `${hiPct}%` } as React.CSSProperties}
      >
        <input
          type="range"
          className="rf-dual-input"
          min={min} max={max} value={low}
          onChange={e => onChange(Math.min(Number(e.target.value), high - 1), high)}
        />
        <input
          type="range"
          className="rf-dual-input"
          min={min} max={max} value={high}
          onChange={e => onChange(low, Math.max(Number(e.target.value), low + 1))}
        />
      </div>
    </div>
  )
}

// ── Icons ─────────────────────────────────────────────────────────────────────
function ChevronIcon({ open }: { open: boolean }) {
  return (
    <svg viewBox="0 0 20 20" fill="none" width="15" height="15" aria-hidden="true"
      style={{ transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}>
      <path d="M5 7.5l5 5 5-5" stroke="currentColor" strokeWidth="1.7"
        strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function ArrowIcon() {
  return (
    <svg viewBox="0 0 20 20" fill="none" width="13" height="13" aria-hidden="true">
      <path d="M4 10h12M10 4l6 6-6 6" stroke="currentColor" strokeWidth="2"
        strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

// ── Props ─────────────────────────────────────────────────────────────────────
interface Props {
  allOffers: FlightOffer[]
  currency: string
  priceMin: number
  priceMax: number
  searchId?: string
  trackingSearchId?: string | null
  isTestSearch?: boolean
}

// ── Main component ────────────────────────────────────────────────────────────
export default function ResultsPanel({
  allOffers,
  currency,
  priceMin,
  priceMax,
  searchId,
  trackingSearchId,
  isTestSearch = false,
}: Props) {
  const t = useTranslations('ResultsPanel')
  const locale = useLocale()
  const analyticsSearchId = trackingSearchId || searchId
  const resultsSourcePath = getTrackedSourcePath(searchId ? `/results/${searchId}` : '/results', isTestSearch)
  // ── Filter state ──────────────────────────────────────────────────────────
  const [sort, setSort] = useState<'price' | 'duration'>('price')
  const [stopsFilter, setStopsFilter] = useState<string[]>([])          // [] = all
  const [airlinesFilter, setAirlinesFilter] = useState<string[]>([])    // [] = all
  const [amenityFilters, setAmenityFilters] = useState<string[]>([])
  const [priceRange, setPriceRange] = useState<[number, number]>([priceMin, priceMax])
  const [depRange, setDepRange] = useState<[number, number]>([0, 1439])
  const [retRange, setRetRange] = useState<[number, number]>([0, 1439])
  const [durationRange, setDurationRange] = useState<[number, number]>([0, Infinity])
  const [airlinesOpen, setAirlinesOpen] = useState(true)
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false)
  const [visibleCount, setVisibleCount] = useState(20)
  const [isUnlocked, setIsUnlocked] = useState(false)
  const [revealedSources, setRevealedSources] = useState<Record<string, string>>({})

  // ── Sidebar stats (always based on all offers) ────────────────────────────
  const stopsStats = useMemo(() => {
    const groups: Record<string, { count: number; min: number }> = {}
    for (const key of ['0', '1', '2plus'] as const) {
      const arr = allOffers.filter(o =>
        key === '0' ? o.stops === 0 : key === '1' ? o.stops === 1 : o.stops >= 2
      )
      groups[key] = {
        count: arr.length,
        min: arr.length ? Math.min(...arr.map((offer) => getOfferKnownTotalPrice(offer))) : Infinity,
      }
    }
    return groups
  }, [allOffers])

  const airlineOptions = useMemo(() => {
    const map = new Map<string, number>()
    for (const o of allOffers) {
      const offerPrice = getOfferKnownTotalPrice(o)
      for (const carrier of getOfferCarriers(o)) {
        const cur = map.get(carrier.name) ?? Infinity
        if (offerPrice < cur) map.set(carrier.name, offerPrice)
      }
    }
    return [...map.entries()].sort((a, b) => a[1] - b[1]).map(([airline, minPrice]) => ({ airline, minPrice }))
  }, [allOffers])

  const amenityStats = useMemo(() => {
    const stats = {
      checked_included: 0,
      checked_fee_known: 0,
      seat_fee_known: 0,
    }

    for (const offer of allOffers) {
      if (hasIncludedAncillary(offer.ancillaries?.checked_bag)) {
        stats.checked_included += 1
      }
      if (hasPaidAncillary(offer.ancillaries?.checked_bag)) {
        stats.checked_fee_known += 1
      }
      if (hasPaidAncillary(offer.ancillaries?.seat_selection)) {
        stats.seat_fee_known += 1
      }
    }

    return stats
  }, [allOffers])

  const durationBounds = useMemo(() => {
    if (!allOffers.length) return { min: 0, max: 1440 }
    let min = Infinity, max = 0
    for (const o of allOffers) {
      if (o.duration_minutes < min) min = o.duration_minutes
      if (o.duration_minutes > max) max = o.duration_minutes
    }
    return { min, max }
  }, [allOffers])

  // ── Filtered + sorted offers ──────────────────────────────────────────────
  const displayOffers = useMemo(() => {
    let list = allOffers.filter(o => {
      const offerPrice = getOfferKnownTotalPrice(o)
      // Stops
      if (stopsFilter.length > 0) {
        const key = o.stops === 0 ? '0' : o.stops === 1 ? '1' : '2plus'
        if (!stopsFilter.includes(key)) return false
      }
      // Airlines
      if (airlinesFilter.length > 0) {
        const offerCarriers = new Set(getOfferCarriers(o).map((carrier) => carrier.name))
        if (!airlinesFilter.some((airline) => offerCarriers.has(airline))) return false
      }
      // Ancillaries
      if (amenityFilters.includes('checked_included') && !hasIncludedAncillary(o.ancillaries?.checked_bag)) return false
      if (amenityFilters.includes('checked_fee_known') && !hasPaidAncillary(o.ancillaries?.checked_bag)) return false
      if (amenityFilters.includes('seat_fee_known') && !hasPaidAncillary(o.ancillaries?.seat_selection)) return false
      // Price range
      if (offerPrice < priceRange[0] || offerPrice > priceRange[1]) return false
      // Departure time
      const dep = isoToMins(o.departure_time)
      if (dep < depRange[0] || dep > depRange[1]) return false
      // Return departure time for RT, outbound arrival for OW
      const arr = isoToMins(o.inbound?.departure_time ?? o.arrival_time)
      if (arr < retRange[0] || arr > retRange[1]) return false
      // Duration
      if (o.duration_minutes < durationRange[0] || o.duration_minutes > durationRange[1]) return false
      return true
    })
    if (sort === 'duration') {
      list = [...list].sort((a, b) => a.duration_minutes - b.duration_minutes)
    } else {
      list = [...list].sort((a, b) => getOfferKnownTotalPrice(a) - getOfferKnownTotalPrice(b))
    }
    return list
  }, [allOffers, stopsFilter, airlinesFilter, amenityFilters, priceRange, depRange, retRange, durationRange, sort])

  const visibleOffers = useMemo(() => displayOffers.slice(0, visibleCount), [displayOffers, visibleCount])

  const refreshUnlockState = useCallback(async () => {
    if (!searchId) {
      setIsUnlocked(false)
      return
    }

    try {
      const res = await fetch(`/api/unlock-status?searchId=${encodeURIComponent(searchId)}`, {
        cache: 'no-store',
      })
      if (!res.ok) return
      const data = await res.json() as { unlocked?: boolean }
      setIsUnlocked(Boolean(data.unlocked))
    } catch {
      // Ignore transient unlock-status failures.
    }
  }, [searchId])

  useEffect(() => {
    if (!searchId) return

    void refreshUnlockState()

    const handlePageShow = () => {
      void refreshUnlockState()
    }
    const handleVisibility = () => {
      if (document.visibilityState === 'visible') {
        void refreshUnlockState()
      }
    }

    window.addEventListener('pageshow', handlePageShow)
    document.addEventListener('visibilitychange', handleVisibility)

    return () => {
      window.removeEventListener('pageshow', handlePageShow)
      document.removeEventListener('visibilitychange', handleVisibility)
    }
  }, [refreshUnlockState, searchId])

  useEffect(() => {
    if (!searchId || !isUnlocked || visibleOffers.length === 0) return

    const pendingOffers = visibleOffers.filter((offer) => !revealedSources[offer.id])
    if (pendingOffers.length === 0) return

    let cancelled = false

    void Promise.all(pendingOffers.map(async (offer) => {
      try {
        const params = new URLSearchParams({
          from: searchId,
          view: 'source-meta',
        })
        appendProbeParam(params, isTestSearch)
        if (offer.offer_ref) {
          params.set('ref', offer.offer_ref)
        }

        const res = await fetch(`/api/offer/${encodeURIComponent(offer.id)}?${params.toString()}`, {
          cache: 'no-store',
        })
        if (!res.ok) return null

        const data = await res.json() as SourceMetaResponse
        const label = typeof data.booking_site_summary === 'string' && data.booking_site_summary.trim().length > 0
          ? data.booking_site_summary.trim()
          : typeof data.booking_site === 'string' && data.booking_site.trim().length > 0
            ? data.booking_site.trim()
            : ''

        return label ? { offerId: offer.id, label } : null
      } catch {
        return null
      }
    })).then((results) => {
      if (cancelled) return

      const nextSources: Record<string, string> = {}
      for (const result of results) {
        if (!result) continue
        nextSources[result.offerId] = result.label
      }

      if (Object.keys(nextSources).length > 0) {
        setRevealedSources((current) => ({ ...current, ...nextSources }))
      }
    })

    return () => {
      cancelled = true
    }
  }, [isUnlocked, revealedSources, searchId, visibleOffers])

  // ── Handlers ─────────────────────────────────────────────────────────────
  const toggleStop = useCallback((key: string) => {
    setStopsFilter(prev => prev.includes(key) ? prev.filter(s => s !== key) : [...prev, key])
    setVisibleCount(20)
    trackSearchSessionEvent(analyticsSearchId, 'filters_changed', { filter: 'stops', value: key }, {
      source: 'website-results-panel',
      source_path: resultsSourcePath,
      is_test_search: isTestSearch || undefined,
    })
  }, [analyticsSearchId, isTestSearch, resultsSourcePath])

  const toggleAirline = useCallback((airline: string) => {
    setAirlinesFilter(prev => prev.includes(airline) ? prev.filter(a => a !== airline) : [...prev, airline])
    setVisibleCount(20)
    trackSearchSessionEvent(analyticsSearchId, 'filters_changed', { filter: 'airline', value: airline }, {
      source: 'website-results-panel',
      source_path: resultsSourcePath,
      is_test_search: isTestSearch || undefined,
    })
  }, [analyticsSearchId, isTestSearch, resultsSourcePath])

  const toggleAmenity = useCallback((key: string) => {
    setAmenityFilters(prev => prev.includes(key) ? prev.filter((value) => value !== key) : [...prev, key])
    setVisibleCount(20)
    trackSearchSessionEvent(analyticsSearchId, 'filters_changed', { filter: 'amenity', value: key }, {
      source: 'website-results-panel',
      source_path: resultsSourcePath,
      is_test_search: isTestSearch || undefined,
    })
  }, [analyticsSearchId, isTestSearch, resultsSourcePath])

  const clearAll = useCallback(() => {
    setStopsFilter([])
    setAirlinesFilter([])
    setAmenityFilters([])
    setPriceRange([priceMin, priceMax])
    setDepRange([0, 1439])
    setRetRange([0, 1439])
    setDurationRange([0, Infinity])
    setVisibleCount(20)
    trackSearchSessionEvent(analyticsSearchId, 'filters_changed', { filter: 'clear_all' }, {
      source: 'website-results-panel',
      source_path: resultsSourcePath,
      is_test_search: isTestSearch || undefined,
    })
  }, [analyticsSearchId, isTestSearch, priceMax, priceMin, resultsSourcePath])

  const handleSortChange = useCallback((nextSort: 'price' | 'duration') => {
    setSort(nextSort)
    setVisibleCount(20)
    trackSearchSessionEvent(analyticsSearchId, 'sort_changed', { sort: nextSort }, {
      source: 'website-results-panel',
      source_path: resultsSourcePath,
      is_test_search: isTestSearch || undefined,
    })
  }, [analyticsSearchId, isTestSearch, resultsSourcePath])

  const hasActiveFilters = stopsFilter.length > 0 || airlinesFilter.length > 0 || amenityFilters.length > 0
    || priceRange[0] > priceMin || priceRange[1] < priceMax
    || depRange[0] > 0 || depRange[1] < 1439
    || retRange[0] > 0 || retRange[1] < 1439
    || durationRange[0] > durationBounds.min || durationRange[1] < durationBounds.max

  const fmt = (p: number) => `${currency}${Math.round(p)}`

  const stopsOptions = [
    { key: '0', label: t('direct') },
    { key: '1', label: t('oneStop') },
    { key: '2plus', label: t('twoPlus') },
  ]

  const amenityOptions = [
    { key: 'checked_included', label: t('checkedBagIncludedFilter'), count: amenityStats.checked_included },
    { key: 'checked_fee_known', label: t('checkedBagFeeFilter'), count: amenityStats.checked_fee_known },
    { key: 'seat_fee_known', label: t('seatFeeFilter'), count: amenityStats.seat_fee_known },
  ]

  return (
    <div className="rf-layout">
      {/* ── Mobile filter overlay backdrop ─────────────────────────────── */}
      {mobileFiltersOpen && (
        <div className="rf-filters-backdrop" onClick={() => setMobileFiltersOpen(false)} aria-hidden="true" />
      )}

      {/* ── Mobile filter toggle bar ───────────────────────────────────── */}
      <div className="rf-mobile-topbar">
        <button
          className={`rf-mobile-filter-btn${mobileFiltersOpen ? ' rf-mobile-filter-btn--active' : ''}`}
          onClick={() => setMobileFiltersOpen(o => !o)}
        >
          <svg viewBox="0 0 20 20" fill="none" width="15" height="15" aria-hidden="true">
            <path d="M3 5h14M6 10h8M9 15h2" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
          </svg>
          {t('filterTitle')}{hasActiveFilters ? ' ·' : ''}
        </button>
        <div className="rf-mobile-sort">
          <span className="rf-bar-label">{t('sort')}</span>
          <button className={`rf-chip${sort === 'price' ? ' rf-chip--on' : ''}`} onClick={() => handleSortChange('price')}>{t('sortPrice')}</button>
          <button className={`rf-chip${sort === 'duration' ? ' rf-chip--on' : ''}`} onClick={() => handleSortChange('duration')}>{t('sortDuration')}</button>
        </div>
      </div>
      {/* ── Filter sidebar ─────────────────────────────────────────────────── */}
      <aside className={`rf-filters${mobileFiltersOpen ? ' rf-filters--mobile-open' : ''}`}>
        <div className="rf-filters-header">
          <span className="rf-filters-title">{t('filterTitle')}</span>
          <div className="rf-filters-header-actions">
            {hasActiveFilters && (
              <button className="rf-filters-clear" onClick={clearAll}>{t('clearAll')}</button>
            )}
            <button className="rf-filters-close" onClick={() => setMobileFiltersOpen(false)} aria-label={t('closeFilters')}>
              <svg viewBox="0 0 20 20" fill="none" width="16" height="16" aria-hidden="true">
                <path d="M5 5l10 10M15 5l-10 10" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
              </svg>
            </button>
          </div>
        </div>

        {/* Stops */}
        <div className="rf-filter-section">
          <div className="rf-filter-heading"><span>{t('stops')}</span></div>
          {stopsOptions.map(({ key, label }) => {
            const stat = stopsStats[key]
            if (!stat || stat.count === 0) return null
            const active = stopsFilter.includes(key)
            return (
              <button key={key} className={`rf-filter-row${active ? ' rf-filter-row--on' : ''}`}
                onClick={() => toggleStop(key)}>
                <span className={`rf-filter-check${active ? ' rf-filter-check--on' : ''}`} aria-hidden="true" />
                <span className="rf-filter-label">{label}</span>
                {stat.min !== Infinity && <span className="rf-filter-price">{fmt(stat.min)}</span>}
              </button>
            )
          })}
        </div>

        {/* Price range */}
        <div className="rf-filter-section">
          <div className="rf-filter-heading"><span>{t('priceRange')}</span></div>
          <DualRange
            min={priceMin} max={priceMax}
            low={priceRange[0]} high={priceRange[1]}
            onChange={(lo, hi) => setPriceRange([lo, hi])}
            formatLabel={fmt}
          />
        </div>

        {/* Departure time */}
        <div className="rf-filter-section">
          <div className="rf-filter-heading"><span>{t('departureTime')}</span></div>
          <div className="rf-filter-sub">{t('outbound')}</div>
          <DualRange
            min={0} max={1439}
            low={depRange[0]} high={depRange[1]}
            onChange={(lo, hi) => setDepRange([lo, hi])}
            formatLabel={minsToLabel}
          />
        </div>

        {/* Return/arrival time */}
        <div className="rf-filter-section">
          <div className="rf-filter-heading"><span>{t('returnTime')}</span></div>
          <DualRange
            min={0} max={1439}
            low={retRange[0]} high={retRange[1]}
            onChange={(lo, hi) => setRetRange([lo, hi])}
            formatLabel={minsToLabel}
          />
        </div>

        {/* Flight duration */}
        <div className="rf-filter-section">
          <div className="rf-filter-heading"><span>{t('flightTime')}</span></div>
          <DualRange
            min={durationBounds.min} max={durationBounds.max}
            low={Math.max(durationBounds.min, isFinite(durationRange[0]) ? durationRange[0] : durationBounds.min)}
            high={Math.min(durationBounds.max, isFinite(durationRange[1]) ? durationRange[1] : durationBounds.max)}
            onChange={(lo, hi) => setDurationRange([lo, hi])}
            formatLabel={fmtDuration}
          />
        </div>

        {/* Airlines */}
        <div className="rf-filter-section">
          <button className="rf-filter-heading rf-filter-heading--btn"
            onClick={() => setAirlinesOpen(o => !o)}>
            <span>{t('airlines')}</span>
            <ChevronIcon open={airlinesOpen} />
          </button>
          {airlinesOpen && airlineOptions.map(({ airline, minPrice }) => {
            const active = airlinesFilter.includes(airline)
            return (
              <button key={airline} className={`rf-filter-row${active ? ' rf-filter-row--on' : ''}`}
                onClick={() => toggleAirline(airline)}>
                <span className={`rf-filter-check${active ? ' rf-filter-check--on' : ''}`} aria-hidden="true" />
                <span className="rf-filter-label">{airline}</span>
                <span className="rf-filter-price">{fmt(minPrice)}</span>
              </button>
            )
          })}
        </div>

        {/* Amenities */}
        <div className="rf-filter-section rf-filter-section--last">
          <div className="rf-filter-heading">
            <span>{t('amenities')}</span>
          </div>
          {amenityOptions.map(({ key, label, count }) => {
            if (count === 0) return null
            const active = amenityFilters.includes(key)
            return (
              <button key={key} className={`rf-filter-row${active ? ' rf-filter-row--on' : ''}`}
                onClick={() => toggleAmenity(key)}>
                <span className={`rf-filter-check${active ? ' rf-filter-check--on' : ''}`} aria-hidden="true" />
                <span className="rf-filter-label">{label}</span>
              </button>
            )
          })}
        </div>
      </aside>

      {/* ── Results card ───────────────────────────────────────────────────── */}
      <div className="rf-card-shell">
        {/* Sort bar */}
        <div className="rf-bar">
          <div className="rf-bar-meta">
            <span className="rf-bar-count">
              {displayOffers.length === 1 ? t('flightSingular', { count: 1 }) : t('flightPlural', { count: displayOffers.length })}
            </span>
            {displayOffers[0] && (
              <span className="rf-bar-from">
                {t('fromPrice', { price: fmt(getOfferKnownTotalPrice(displayOffers[0])) })}
              </span>
            )}
          </div>
          <div className="rf-bar-controls">
            <span className="rf-bar-label">{t('sort')}</span>
            <button
              className={`rf-chip${sort === 'price' ? ' rf-chip--on' : ''}`}
              onClick={() => handleSortChange('price')}
            >
              {t('sortPrice')}
            </button>
            <button
              className={`rf-chip${sort === 'duration' ? ' rf-chip--on' : ''}`}
              onClick={() => handleSortChange('duration')}
            >
              {t('sortDuration')}
            </button>
          </div>
        </div>

        {/* Flight list */}
        <div className="rf-list">
          {visibleOffers.map((offer, index) => {
            const isBestValue = sort === 'price' && index === 0
            const isExpanded = expandedId === offer.id
            const offerCarriers = getOfferCarriers(offer)
            const airlineLabel = getOfferAirlineLabel(offer)
            const outboundStops = getRouteStops(offer.segments)
            const outboundViaBadge = getRouteViaBadge(outboundStops)
            const outboundViaTitle = getRouteViaTitle(outboundStops)
            const inboundStops = getRouteStops(offer.inbound?.segments)
            const inboundViaBadge = getRouteViaBadge(inboundStops)
            const inboundViaTitle = getRouteViaTitle(inboundStops)
            const outboundStopsLabel = getStopsLabel(offer.stops, offer.segments, t('direct'))
            const inboundStopsLabel = offer.inbound
              ? getStopsLabel(offer.inbound.stops, offer.inbound.segments, t('direct'))
              : t('direct')
            const outboundOriginName = offer.origin_name || offer.origin
            const outboundDestinationName = offer.destination_name || offer.destination
            const inboundOriginName = offer.inbound?.segments?.[0]?.origin_name || offer.destination_name || offer.inbound?.origin || ''
            const inboundDestinationName = offer.inbound?.segments?.[offer.inbound.segments.length - 1]?.destination_name || offer.origin_name || offer.inbound?.destination || ''
            const fullOfferPrice = getOfferKnownTotalPrice(offer)
            const googleFlightsSavings = getGoogleFlightsSavingsAmount(fullOfferPrice, offer.google_flights_price)
            const googleFlightsSavingsLabel = googleFlightsSavings === null
              ? null
              : t('cheaperThanGoogleFlights', {
                  amount: formatGoogleFlightsSavings(googleFlightsSavings, offer.currency, locale),
                })
            const checkedBag = offer.ancillaries?.checked_bag
            const seatSelection = offer.ancillaries?.seat_selection
            const ancillaryBadges = [
              hasIncludedAncillary(checkedBag)
                ? t('checkedBagIncluded')
                : hasPaidAncillary(checkedBag)
                  ? t('checkedBagFee', { price: fmtOfferPrice(checkedBag!.price!, checkedBag!.currency || offer.currency) })
                  : null,
              hasIncludedAncillary(seatSelection)
                ? t('seatSelectionIncluded')
                : hasPaidAncillary(seatSelection)
                  ? t('seatSelectionFee', { price: fmtOfferPrice(seatSelection!.price!, seatSelection!.currency || offer.currency) })
                  : null,
            ].filter((value): value is string => Boolean(value))
            const checkedBagTotal = getOfferTotalWithAncillary(offer, checkedBag)
            const seatSelectionTotal = getOfferTotalWithAncillary(offer, seatSelection)
            const sourceLabel = revealedSources[offer.id]

            return (
              <div key={offer.id} className={`rf-card${isBestValue ? ' rf-card--best' : ''}${isExpanded ? ' rf-card--expanded' : ''}`}>
                {googleFlightsSavingsLabel && (
                  <div className="rf-card-badges">
                    <span className="rf-card-badge rf-card-badge--savings">{googleFlightsSavingsLabel}</span>
                  </div>
                )}
                <div className="rf-card-row">
                  <div className={`rf-airline${offerCarriers.length > 1 ? ' rf-airline--multi' : ''}`}>
                    <div className={`rf-airline-logos${offerCarriers.length > 1 ? ' rf-airline-logos--multi' : ''}`}>
                      {offerCarriers.map((carrier) => (
                        <AirlineLogo key={`${carrier.code}-${carrier.name}`} code={carrier.code} name={carrier.name} />
                      ))}
                    </div>
                    <div className={`rf-airline-copy${offerCarriers.length > 1 ? ' rf-airline-copy--multi' : ''}`}>
                      <div
                        className={`rf-airline-name${offerCarriers.length > 1 ? ' rf-airline-name--multi' : ''}`}
                        title={offerCarriers.length > 1 ? airlineLabel : undefined}
                      >
                        {airlineLabel}
                      </div>
                      {isUnlocked && sourceLabel && (
                        <div className="rf-source-pill">Deal from {sourceLabel}</div>
                      )}
                      {ancillaryBadges.length > 0 && (
                        <div className="rf-pill-row">
                          {ancillaryBadges.map((label) => (
                            <div key={label} className="rf-source-pill rf-source-pill--neutral">{label}</div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>

                  {offer.inbound ? (
                    <div className="rf-legs">
                      <div className="rf-route">
                        <div className="rf-endpoint">
                          <span className="rf-time">{formatFlightTime(offer.departure_time)}</span>
                          <span className="rf-city" title={outboundOriginName}>{outboundOriginName}</span>
                          <span className="rf-iata">{offer.origin}</span>
                        </div>
                        <div className="rf-path">
                          <span className="rf-duration">{fmtDuration(offer.duration_minutes)}</span>
                          <div className="rf-path-line">
                            <span className="rf-path-dot" />
                            <span className="rf-path-track">
                              {offer.stops > 0 && outboundViaBadge && (
                                <span className="rf-path-via" title={outboundViaTitle}>{outboundViaBadge}</span>
                              )}
                            </span>
                            <span className="rf-path-dot" />
                          </div>
                          <span className={`rf-stops${offer.stops === 0 ? ' rf-stops--direct' : ''}`} title={outboundViaTitle}>
                            {outboundStopsLabel}
                          </span>
                        </div>
                        <div className="rf-endpoint rf-endpoint--arr">
                          <span className="rf-time">{formatFlightTime(offer.arrival_time)}</span>
                          <span className="rf-city" title={outboundDestinationName}>{outboundDestinationName}</span>
                          <span className="rf-iata">{offer.destination}</span>
                        </div>
                      </div>

                      <div className="rf-leg-sep" aria-hidden="true">
                        <span className="rf-leg-sep-line" />
                        <span className="rf-leg-sep-label">{t('returnLeg')}</span>
                        <span className="rf-leg-sep-line" />
                      </div>

                      <div className="rf-route">
                        <div className="rf-endpoint">
                          <span className="rf-time">{formatFlightTime(offer.inbound.departure_time)}</span>
                          <span className="rf-city" title={inboundOriginName}>{inboundOriginName}</span>
                          <span className="rf-iata">{offer.inbound.origin}</span>
                        </div>
                        <div className="rf-path">
                          <span className="rf-duration">{fmtDuration(offer.inbound.duration_minutes)}</span>
                          <div className="rf-path-line">
                            <span className="rf-path-dot" />
                            <span className="rf-path-track">
                              {offer.inbound.stops > 0 && inboundViaBadge && (
                                <span className="rf-path-via" title={inboundViaTitle}>{inboundViaBadge}</span>
                              )}
                            </span>
                            <span className="rf-path-dot" />
                          </div>
                          <span className={`rf-stops${offer.inbound.stops === 0 ? ' rf-stops--direct' : ''}`} title={inboundViaTitle}>
                            {inboundStopsLabel}
                          </span>
                        </div>
                        <div className="rf-endpoint rf-endpoint--arr">
                          <span className="rf-time">{formatFlightTime(offer.inbound.arrival_time)}</span>
                          <span className="rf-city" title={inboundDestinationName}>{inboundDestinationName}</span>
                          <span className="rf-iata">{offer.inbound.destination}</span>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="rf-route">
                      <div className="rf-endpoint">
                        <span className="rf-time">{formatFlightTime(offer.departure_time)}</span>
                        <span className="rf-city" title={outboundOriginName}>{outboundOriginName}</span>
                        <span className="rf-iata">{offer.origin}</span>
                      </div>
                      <div className="rf-path">
                        <span className="rf-duration">{fmtDuration(offer.duration_minutes)}</span>
                        <div className="rf-path-line">
                          <span className="rf-path-dot" />
                          <span className="rf-path-track">
                            {offer.stops > 0 && outboundViaBadge && (
                              <span className="rf-path-via" title={outboundViaTitle}>{outboundViaBadge}</span>
                            )}
                          </span>
                          <span className="rf-path-dot" />
                        </div>
                        <span className={`rf-stops${offer.stops === 0 ? ' rf-stops--direct' : ''}`} title={outboundViaTitle}>
                          {outboundStopsLabel}
                        </span>
                      </div>
                      <div className="rf-endpoint rf-endpoint--arr">
                        <span className="rf-time">{formatFlightTime(offer.arrival_time)}</span>
                        <span className="rf-city" title={outboundDestinationName}>{outboundDestinationName}</span>
                        <span className="rf-iata">{offer.destination}</span>
                      </div>
                    </div>
                  )}

                  <div className="rf-price-wrap">
                    <span className="rf-price">{fmt(fullOfferPrice)}</span>
                    <span className="rf-price-sub">{t('perPerson')}</span>
                    {(checkedBagTotal || seatSelectionTotal) && (
                      <div className="rf-price-notes">
                        {checkedBagTotal && (
                          <span className="rf-price-note">{t('withCheckedBag', { price: fmtOfferPrice(checkedBagTotal, offer.currency) })}</span>
                        )}
                        {seatSelectionTotal && (
                          <span className="rf-price-note">{t('withSeatSelection', { price: fmtOfferPrice(seatSelectionTotal, offer.currency) })}</span>
                        )}
                      </div>
                    )}
                  </div>

                  <a
                    href={(() => {
                      const params = new URLSearchParams()
                      if (searchId) params.set('from', searchId)
                      if (offer.offer_ref) params.set('ref', offer.offer_ref)
                      appendProbeParam(params, isTestSearch)
                      const query = params.toString()
                      return `/book/${offer.id}${query ? `?${query}` : ''}`
                    })()}
                    className="rf-book-btn"
                    onClick={() => trackSearchSessionEvent(analyticsSearchId, 'offer_selected', {
                      offer_id: offer.id,
                      airline: airlineLabel,
                      currency: offer.currency,
                      price: offer.price,
                      google_flights_price: offer.google_flights_price ?? null,
                    }, {
                      source: 'website-results-panel',
                      source_path: resultsSourcePath,
                      is_test_search: isTestSearch || undefined,
                      selected_offer_id: offer.id,
                      selected_offer_airline: airlineLabel,
                      selected_offer_currency: offer.currency,
                      selected_offer_price: offer.price,
                      selected_offer_google_flights_price: offer.google_flights_price,
                    }, { keepalive: true })}
                  >
                    {t('select')}
                    <ArrowIcon />
                  </a>
                </div>

                {(offer.segments?.length || offer.inbound?.segments?.length) && (
                  <>
                    <button
                      className="rf-details-btn"
                      onClick={() => {
                        setExpandedId(isExpanded ? null : offer.id)
                        trackSearchSessionEvent(analyticsSearchId, 'details_toggled', {
                          offer_id: offer.id,
                          open: !isExpanded,
                        }, {
                          source: 'website-results-panel',
                          source_path: resultsSourcePath,
                          is_test_search: isTestSearch || undefined,
                        })
                      }}
                    >
                      {isExpanded ? t('hideDetails') : t('flightDetails')}
                      <ChevronIcon open={isExpanded} />
                    </button>

                    {isExpanded && (() => {
                      const hasReturn = !!offer.inbound?.segments?.length
                      const renderSegs = (segs: FlightSegment[], mainAirline: string) => segs.map((seg, si) => (
                        <div key={si}>
                          {si > 0 && segs[si - 1].layover_minutes > 0 && (
                            <div className="rf-layover">
                              <span className="rf-layover-icon" aria-hidden="true" />
                              <span className="rf-layover-text">
                                {t('layover', { duration: fmtDuration(segs[si - 1].layover_minutes), city: segs[si - 1].destination_name })}
                              </span>
                            </div>
                          )}
                          <div className="rf-leg">
                            <div className="rf-leg-header">
                              <span className="rf-leg-num">{t('leg', { number: si + 1 })}</span>
                              <span className="rf-leg-flight">{seg.flight_number} · {getSegmentAirlineLabel(seg, mainAirline)}</span>
                            </div>
                            <div className="rf-leg-body">
                              <div className="rf-leg-spine" />
                              <div className="rf-leg-stops">
                                <div className="rf-leg-point">
                                  <span className="rf-leg-dot rf-leg-dot--dep" />
                                  <div className="rf-leg-info">
                                    <span className="rf-leg-time">
                                      {formatFlightTime(seg.departure_time)}
                                      <span className="rf-leg-date">{formatFlightDateCompact(seg.departure_time)}</span>
                                    </span>
                                    <span className="rf-leg-airport">{seg.origin}{seg.origin_name ? ` · ${seg.origin_name}` : ''}</span>
                                  </div>
                                </div>
                                <div className="rf-leg-dur">{fmtDuration(seg.duration_minutes)}</div>
                                <div className="rf-leg-point">
                                  <span className="rf-leg-dot rf-leg-dot--arr" />
                                  <div className="rf-leg-info">
                                    <span className="rf-leg-time">
                                      {formatFlightTime(seg.arrival_time)}
                                      <span className="rf-leg-date">{formatFlightDateCompact(seg.arrival_time)}</span>
                                    </span>
                                    <span className="rf-leg-airport">{seg.destination}{seg.destination_name ? ` · ${seg.destination_name}` : ''}</span>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                      ))

                      return (
                        <div className={`rf-details${hasReturn ? ' rf-details--cols' : ''}`}>
                          {offer.segments?.length ? (
                            <div className="rf-details-col">
                              {hasReturn && <div className="rf-details-col-label rf-details-col-label--out">Outbound</div>}
                              {renderSegs(offer.segments, offer.airline)}
                            </div>
                          ) : null}
                          {hasReturn ? (
                            <div className="rf-details-col">
                              <div className="rf-details-col-label rf-details-col-label--ret">Return</div>
                              {renderSegs(offer.inbound!.segments!, offer.inbound!.airline || offer.airline)}
                            </div>
                          ) : null}
                        </div>
                      )
                    })()}
                  </>
                )}
              </div>
            )
          })}
          {displayOffers.length === 0 && (
            <div className="rf-empty">{t('noFlights')}</div>
          )}
          {displayOffers.length > visibleCount && (
            <div className="rf-load-more">
              <button
                className="rf-load-more-btn"
                onClick={() => {
                  setVisibleCount(c => c + 20)
                  trackSearchSessionEvent(analyticsSearchId, 'show_more', {
                    next_visible_count: Math.min(displayOffers.length, visibleCount + 20),
                  }, {
                    source: 'website-results-panel',
                    source_path: resultsSourcePath,
                    is_test_search: isTestSearch || undefined,
                  })
                }}
              >
                {t('showMore', { count: Math.min(20, displayOffers.length - visibleCount) })}
                <span className="rf-load-more-total">{t('remaining', { count: displayOffers.length - visibleCount })}</span>
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
