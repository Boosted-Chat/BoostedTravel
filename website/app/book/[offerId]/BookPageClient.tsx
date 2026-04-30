'use client'

import Link from 'next/link'
import { useEffect, useState } from 'react'
import CheckoutPanel from './CheckoutPanel'
import { findOfferInBrowserCache } from '../../../lib/browser-offer-cache'
import type { Offer } from './page'

interface Props {
  initialOffer: Offer | null
  offerId: string
  searchId: string | null
  trackingSearchId: string | null
  isTestSearch: boolean
  offerRef: string | null
  backHref: string
}

export default function BookPageClient({
  initialOffer,
  offerId,
  searchId,
  trackingSearchId,
  isTestSearch,
  offerRef,
  backHref,
}: Props) {
  const [offer, setOffer] = useState<Offer | null>(initialOffer)
  const [checkedRecovery, setCheckedRecovery] = useState(Boolean(initialOffer) || !searchId)

  useEffect(() => {
    if (initialOffer) {
      setOffer(initialOffer)
      setCheckedRecovery(true)
      return
    }

    if (!searchId) {
      setCheckedRecovery(true)
      return
    }

    const recoveredOffer = findOfferInBrowserCache<Offer>(searchId, offerId)
    if (recoveredOffer) {
      setOffer(recoveredOffer)
    }
    setCheckedRecovery(true)
  }, [initialOffer, offerId, searchId])

  if (offer) {
    return (
      <CheckoutPanel
        offer={offer}
        searchId={searchId}
        trackingSearchId={trackingSearchId}
        isTestSearch={isTestSearch}
        offerRef={offerRef}
      />
    )
  }

  if (!checkedRecovery) {
    return (
      <section className="ck-shell">
        <div className="ck-card-shell">
          <p>Recovering saved offer…</p>
        </div>
      </section>
    )
  }

  return (
    <section className="ck-shell">
      <div className="ck-card-shell">
        <h2>Offer unavailable</h2>
        <p>This offer is no longer available on the server and no saved browser copy was found.</p>
        <Link href={backHref} className="ck-back-link">
          Back to results
        </Link>
      </div>
    </section>
  )
}