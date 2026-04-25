'use client'

import { useState, useMemo, useCallback } from 'react'
import { useTranslations } from 'next-intl'
import { getAirlineLogoUrl } from '../../airlineLogos'
import { withFee } from '../../../lib/pricing'

// ── Types ─────────────────────────────────────────────────────────────────────
interface FlightSegment {
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

interface FlightOffer {
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
  segments?: FlightSegment[]
  inbound?: InboundLeg
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtTime(iso: string) {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', hour12: false,
  })
}

function fmtDuration(mins: number) {
  return `${Math.floor(mins / 60)}h ${mins % 60}m`
}

function isoToMins(iso: string) {
  const d = new Date(iso)
  return d.getUTCHours() * 60 + d.getUTCMinutes()
}

function minsToLabel(m: number) {
  return `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`
}

// ── Airline logo with IATA-code fallback ──────────────────────────────────────
function AirlineLogo({ code, name }: { code: string; name: string }) {
  const [failed, setFailed] = useState(false)
  if (failed) {
    return <div className="rf-airline-badge">{code.slice(0, 2)}</div>
  }
  return (
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
}

// ── Main component ────────────────────────────────────────────────────────────
export default function ResultsPanel({ allOffers, currency, priceMin, priceMax, searchId }: Props) {
  const t = useTranslations('ResultsPanel')
  // ── Filter state ──────────────────────────────────────────────────────────
  const [sort, setSort] = useState<'price' | 'duration'>('price')
  const [stopsFilter, setStopsFilter] = useState<string[]>([])          // [] = all
  const [airlinesFilter, setAirlinesFilter] = useState<string[]>([])    // [] = all
  const [priceRange, setPriceRange] = useState<[number, number]>([priceMin, priceMax])
  const [depRange, setDepRange] = useState<[number, number]>([0, 1439])
  const [retRange, setRetRange] = useState<[number, number]>([0, 1439])
  const [durationRange, setDurationRange] = useState<[number, number]>([0, Infinity])
  const [airlinesOpen, setAirlinesOpen] = useState(true)
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false)
  const [visibleCount, setVisibleCount] = useState(20)

  // ── Sidebar stats (always based on all offers) ────────────────────────────
  const stopsStats = useMemo(() => {
    const groups: Record<string, { count: number; min: number }> = {}
    for (const key of ['0', '1', '2plus'] as const) {
      const arr = allOffers.filter(o =>
        key === '0' ? o.stops === 0 : key === '1' ? o.stops === 1 : o.stops >= 2
      )
      groups[key] = { count: arr.length, min: arr.length ? Math.min(...arr.map(o => o.price)) : Infinity }
    }
    return groups
  }, [allOffers])

  const airlineOptions = useMemo(() => {
    const map = new Map<string, number>()
    for (const o of allOffers) {
      const cur = map.get(o.airline) ?? Infinity
      if (o.price < cur) map.set(o.airline, o.price)
    }
    return [...map.entries()].sort((a, b) => a[1] - b[1]).map(([airline, minPrice]) => ({ airline, minPrice }))
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
      // Stops
      if (stopsFilter.length > 0) {
        const key = o.stops === 0 ? '0' : o.stops === 1 ? '1' : '2plus'
        if (!stopsFilter.includes(key)) return false
      }
      // Airlines
      if (airlinesFilter.length > 0 && !airlinesFilter.includes(o.airline)) return false
      // Price range
      if (o.price < priceRange[0] || o.price > priceRange[1]) return false
      // Departure time
      const dep = isoToMins(o.departure_time)
      if (dep < depRange[0] || dep > depRange[1]) return false
      // Return time (arrival as proxy — only meaningful for roundtrips)
      const arr = isoToMins(o.arrival_time)
      if (arr < retRange[0] || arr > retRange[1]) return false
      // Duration
      if (o.duration_minutes < durationRange[0] || o.duration_minutes > durationRange[1]) return false
      return true
    })
    if (sort === 'duration') list = [...list].sort((a, b) => a.duration_minutes - b.duration_minutes)
    return list
  }, [allOffers, stopsFilter, airlinesFilter, priceRange, depRange, retRange, sort])

  // ── Handlers ─────────────────────────────────────────────────────────────
  const toggleStop = useCallback((key: string) => {
    setStopsFilter(prev => prev.includes(key) ? prev.filter(s => s !== key) : [...prev, key])
    setVisibleCount(20)
  }, [])

  const toggleAirline = useCallback((airline: string) => {
    setAirlinesFilter(prev => prev.includes(airline) ? prev.filter(a => a !== airline) : [...prev, airline])
    setVisibleCount(20)
  }, [])

  const clearAll = useCallback(() => {
    setStopsFilter([])
    setAirlinesFilter([])
    setPriceRange([priceMin, priceMax])
    setDepRange([0, 1439])
    setRetRange([0, 1439])
    setDurationRange([0, Infinity])
    setVisibleCount(20)
  }, [priceMin, priceMax])

  const hasActiveFilters = stopsFilter.length > 0 || airlinesFilter.length > 0
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
          <button className={`rf-chip${sort === 'price' ? ' rf-chip--on' : ''}`} onClick={() => { setSort('price'); setVisibleCount(20) }}>{t('sortPrice')}</button>
          <button className={`rf-chip${sort === 'duration' ? ' rf-chip--on' : ''}`} onClick={() => { setSort('duration'); setVisibleCount(20) }}>{t('sortDuration')}</button>
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

        {/* Amenities placeholder */}
        <div className="rf-filter-section rf-filter-section--last">
          <div className="rf-filter-heading rf-filter-heading--muted">
            <span>{t('amenities')}</span>
          </div>
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
                {t('fromPrice', { price: `${displayOffers[0].currency}${displayOffers[0].price}` })}
              </span>
            )}
          </div>
          <div className="rf-bar-controls">
            <span className="rf-bar-label">{t('sort')}</span>
            <button
              className={`rf-chip${sort === 'price' ? ' rf-chip--on' : ''}`}
              onClick={() => { setSort('price'); setVisibleCount(20) }}
            >
              {t('sortPrice')}
            </button>
            <button
              className={`rf-chip${sort === 'duration' ? ' rf-chip--on' : ''}`}
              onClick={() => { setSort('duration'); setVisibleCount(20) }}
            >
              {t('sortDuration')}
            </button>
          </div>
        </div>

        {/* Flight list */}
        <div className="rf-list">
          {displayOffers.slice(0, visibleCount).map((offer, index) => {
            const isExpanded = expandedId === offer.id
            const viaCode = offer.segments?.[0]?.destination
            const stopsLabel = offer.stops === 0
              ? t('direct')
              : viaCode
                ? `${offer.stops} stop · via ${viaCode}`
                : `${offer.stops} stop${offer.stops > 1 ? 's' : ''}`

            return (
              <div key={offer.id} className={`rf-card${sort === 'price' && index === 0 ? ' rf-card--best' : ''}${isExpanded ? ' rf-card--expanded' : ''}`}>
                <div className="rf-card-row">
                  <div className="rf-airline">
                    <AirlineLogo code={offer.airline_code} name={offer.airline} />
                    <div className="rf-airline-name">{offer.airline}</div>
                  </div>

                  {offer.inbound ? (
                    <div className="rf-legs">
                      <div className="rf-route">
                        <div className="rf-endpoint">
                          <span className="rf-time">{fmtTime(offer.departure_time)}</span>
                          <span className="rf-iata">{offer.origin}</span>
                        </div>
                        <div className="rf-path">
                          <span className="rf-duration">{fmtDuration(offer.duration_minutes)}</span>
                          <div className="rf-path-line">
                            <span className="rf-path-dot" />
                            <span className="rf-path-track">
                              {offer.stops > 0 && viaCode && (
                                <span className="rf-path-via">{viaCode}</span>
                              )}
                            </span>
                            <span className="rf-path-dot" />
                          </div>
                          <span className={`rf-stops${offer.stops === 0 ? ' rf-stops--direct' : ''}`}>
                            {stopsLabel}
                          </span>
                        </div>
                        <div className="rf-endpoint rf-endpoint--arr">
                          <span className="rf-time">{fmtTime(offer.arrival_time)}</span>
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
                          <span className="rf-time">{fmtTime(offer.inbound.departure_time)}</span>
                          <span className="rf-iata">{offer.inbound.origin}</span>
                        </div>
                        <div className="rf-path">
                          <span className="rf-duration">{fmtDuration(offer.inbound.duration_minutes)}</span>
                          <div className="rf-path-line">
                            <span className="rf-path-dot" />
                            <span className="rf-path-track" />
                            <span className="rf-path-dot" />
                          </div>
                          <span className={`rf-stops${offer.inbound.stops === 0 ? ' rf-stops--direct' : ''}`}>
                            {offer.inbound.stops === 0 ? t('direct') : `${offer.inbound.stops} stop${offer.inbound.stops > 1 ? 's' : ''}`}
                          </span>
                        </div>
                        <div className="rf-endpoint rf-endpoint--arr">
                          <span className="rf-time">{fmtTime(offer.inbound.arrival_time)}</span>
                          <span className="rf-iata">{offer.inbound.destination}</span>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="rf-route">
                      <div className="rf-endpoint">
                        <span className="rf-time">{fmtTime(offer.departure_time)}</span>
                        <span className="rf-iata">{offer.origin}</span>
                      </div>
                      <div className="rf-path">
                        <span className="rf-duration">{fmtDuration(offer.duration_minutes)}</span>
                        <div className="rf-path-line">
                          <span className="rf-path-dot" />
                          <span className="rf-path-track">
                            {offer.stops > 0 && viaCode && (
                              <span className="rf-path-via">{viaCode}</span>
                            )}
                          </span>
                          <span className="rf-path-dot" />
                        </div>
                        <span className={`rf-stops${offer.stops === 0 ? ' rf-stops--direct' : ''}`}>
                          {stopsLabel}
                        </span>
                      </div>
                      <div className="rf-endpoint rf-endpoint--arr">
                        <span className="rf-time">{fmtTime(offer.arrival_time)}</span>
                        <span className="rf-iata">{offer.destination}</span>
                      </div>
                    </div>
                  )}

                  <div className="rf-price-wrap">
                    <span className="rf-price">{offer.currency}{Math.round(withFee(offer.price, offer.currency))}</span>
                    <span className="rf-price-sub">{t('perPerson')}</span>
                  </div>

                  <a href={`/book/${offer.id}${searchId ? `?from=${searchId}` : ''}`} className="rf-book-btn">
                    {t('select')}
                    <ArrowIcon />
                  </a>
                </div>

                {offer.stops > 0 && offer.segments && (
                  <>
                    <button
                      className="rf-details-btn"
                      onClick={() => setExpandedId(isExpanded ? null : offer.id)}
                    >
                      {isExpanded ? t('hideDetails') : t('flightDetails')}
                      <ChevronIcon open={isExpanded} />
                    </button>

                    {isExpanded && (
                      <div className="rf-details">
                        {offer.segments.map((seg, si) => (
                          <div key={si}>
                            {si > 0 && seg.layover_minutes == null && null}
                            {/* Layover row between segments */}
                            {si > 0 && offer.segments![si - 1].layover_minutes > 0 && (
                              <div className="rf-layover">
                                <span className="rf-layover-icon" aria-hidden="true" />
                                <span className="rf-layover-text">
                                  {t('layover', { duration: fmtDuration(offer.segments![si - 1].layover_minutes), city: offer.segments![si - 1].destination_name })}
                                </span>
                              </div>
                            )}
                            <div className="rf-leg">
                              <div className="rf-leg-header">
                                <span className="rf-leg-num">{t('leg', { number: si + 1 })}</span>
                                <span className="rf-leg-flight">{seg.flight_number} · {offer.airline}</span>
                              </div>
                              <div className="rf-leg-body">
                                <div className="rf-leg-spine" />
                                <div className="rf-leg-stops">
                                  <div className="rf-leg-point">
                                    <span className="rf-leg-dot rf-leg-dot--dep" />
                                    <div className="rf-leg-info">
                                      <span className="rf-leg-time">{fmtTime(seg.departure_time)}</span>
                                      <span className="rf-leg-airport">{seg.origin} · {seg.origin_name}</span>
                                    </div>
                                  </div>
                                  <div className="rf-leg-dur">{fmtDuration(seg.duration_minutes)}</div>
                                  <div className="rf-leg-point">
                                    <span className="rf-leg-dot rf-leg-dot--arr" />
                                    <div className="rf-leg-info">
                                      <span className="rf-leg-time">{fmtTime(seg.arrival_time)}</span>
                                      <span className="rf-leg-airport">{seg.destination} · {seg.destination_name}</span>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
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
                onClick={() => setVisibleCount(c => c + 20)}
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
