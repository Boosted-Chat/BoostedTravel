'use client'

import { useState, useRef, useCallback, useMemo, useEffect } from 'react'
import { useTranslations } from 'next-intl'
import { getAirlineLogoUrl } from '../../airlineLogos'
import { calculateFee, withFee } from '../../../lib/pricing'
import type { Offer } from './page'

interface Props {
  offer: Offer
  searchId: string | null
}

type CheckoutStep =
  | { type: 'checking' }           // checking unlock status on mount
  | { type: 'verifying-payment' }  // verifying Stripe session after redirect
  | { type: 'locked' }
  | { type: 'paying' }             // waiting for Stripe redirect
  | { type: 'share-select' }
  | { type: 'share-upload'; platform: Platform }
  | { type: 'share-verifying'; platform: Platform }
  | { type: 'share-rejected'; platform: Platform }
  | { type: 'unlocked'; via: 'payment' | 'share' | 'existing' }

interface Platform {
  id: string
  label: string
  instructions: string[]
}

const PLATFORMS: Platform[] = []  // replaced by getPlatforms(t) inside component


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
}

function fmtTime(iso: string) {
  return new Date(iso).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
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

export default function CheckoutPanel({ offer, searchId }: Props) {
  const t = useTranslations('Checkout')
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
  const showShareOption = fee < 20 // only when fee < ~$20 (ticket < ~$2000)

  // Start in 'checking' — we always verify unlock status on mount.
  const [step, setStep] = useState<CheckoutStep>({ type: 'checking' })
  const [uploadedFile, setUploadedFile] = useState<File | null>(null)
  const [previewUrl, setPreviewUrl] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const isUnlocked = step.type === 'unlocked'
  const isLoading = step.type === 'checking' || step.type === 'verifying-payment'

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
        body: JSON.stringify({ stripeSessionId: stripeSession }),
      })
        .then(r => r.json())
        .then((data: { unlocked: boolean }) => {
          if (data.unlocked) {
            setStep({ type: 'unlocked', via: 'payment' })
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
    fetch(`/api/unlock-status?searchId=${encodeURIComponent(searchId)}`)
      .then(r => r.json())
      .then((data: { unlocked: boolean }) => {
        setStep(data.unlocked
          ? { type: 'unlocked', via: 'existing' }
          : { type: 'locked' }
        )
      })
      .catch(() => setStep({ type: 'locked' }))
  }, [searchId]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Pay via Stripe ───────────────────────────────────────────────────────
  const handlePay = useCallback(async () => {
    setStep({ type: 'paying' })
    try {
      const res = await fetch('/api/checkout/create-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          offerId: offer.id,
          searchId: searchId ?? '',
          price: offer.price,
          currency: offer.currency,
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
  }, [offer, searchId])

  const handleSelectPlatform = useCallback((platform: Platform) => {
    setStep({ type: 'share-upload', platform })
    setUploadedFile(null)
    setPreviewUrl(null)
  }, [])

  const handleFileChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setUploadedFile(file)
    setPreviewUrl(URL.createObjectURL(file))
  }, [])

  const handleVerify = useCallback(() => {
    if (!uploadedFile || step.type !== 'share-upload') return
    const platform = step.platform
    setStep({ type: 'share-verifying', platform })
    // Simulate AI verification — 75% chance of success in demo
    setTimeout(() => {
      if (Math.random() > 0.25) {
        setStep({ type: 'unlocked', via: 'share' })
      } else {
        setStep({ type: 'share-rejected', platform })
      }
    }, 3200)
  }, [uploadedFile, step])

  const handleRetryShare = useCallback(() => {
    setStep({ type: 'share-select' })
    setUploadedFile(null)
    setPreviewUrl(null)
  }, [])

  return (
    <div className="ck-page">
      <div className="ck-inner">

        {/* ── Flight summary card ─────────────────────────────────────────── */}
        <div className="ck-flight-card">
          <div className="ck-flight-header">
            <AirlineLogo code={offer.airline_code} name={offer.airline} />
            <div className="ck-flight-airline">
              <span className="ck-airline-name">{offer.airline}</span>
              <span className="ck-flight-num">{offer.flight_number}</span>
            </div>
            <div className="ck-flight-price-badge">
              <span className="ck-flight-price">{offer.currency}{Math.round(withFee(offer.price, offer.currency))}</span>
              <span className="ck-flight-price-label">{t('perPerson')}</span>
            </div>
          </div>

          <div className="ck-flight-route">
            <div className="ck-endpoint">
              <span className="ck-time">{fmtTime(offer.departure_time)}</span>
              <span className="ck-iata">{offer.origin}</span>
              <span className="ck-city">{offer.origin_name}</span>
            </div>

            <div className="ck-path">
              <span className="ck-duration">{fmtDuration(offer.duration_minutes)}</span>
              <div className="ck-path-line">
                <span className="ck-path-dot" />
                <span className="ck-path-track" />
                {offer.stops === 0 && <span className="ck-direct-label">Direct</span>}
                {offer.stops > 0 && <span className="ck-stop-dot" />}
                <span className="ck-path-track" />
                <span className="ck-path-dot" />
              </div>
              {offer.stops > 0 && (
                <span className="ck-stops-label">{offer.stops} stop{offer.stops > 1 ? 's' : ''}</span>
              )}
            </div>

            <div className="ck-endpoint ck-endpoint--right">
              <span className="ck-time">{fmtTime(offer.arrival_time)}</span>
              <span className="ck-iata">{offer.destination}</span>
              <span className="ck-city">{offer.destination_name}</span>
            </div>
          </div>

          <div className="ck-flight-meta">
            <span>{fmtDate(offer.departure_time)}</span>
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
                            <span>⚠</span> {t('screenshotInvalid')}
                          </div>
                        )}
                      </div>
                    )}

                    {/* Upload step */}
                    {step.type === 'share-upload' && (
                      <div className="ck-share-upload">
                        <div className="ck-share-platform-header">
                          <span className="ck-platform-icon">{PLATFORM_ICONS[step.platform.id]}</span>
                          <span className="ck-share-platform-name">{step.platform.label}</span>
                          <button className="ck-share-back" onClick={() => setStep({ type: 'share-select' })}>
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

              {isUnlocked ? (
                  <a
                  href={offer.booking_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="ck-book-btn ck-book-btn--active"
                >
                  {t('bookOn', { airline: offer.airline })}
                  <ArrowIcon />
                </a>
              ) : (
                <>
                  <button className="ck-book-btn ck-book-btn--locked" disabled aria-disabled="true">
                    <LockIcon />
                    {t('bookOn', { airline: offer.airline })}
                  </button>
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
          <a href="https://letsfg.co" target="_blank" rel="noreferrer" className="ck-trust-link ck-trust-brand">LetsFG</a>
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
