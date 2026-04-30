import { NextRequest, NextResponse } from 'next/server'
import { getStripe, toStripeAmount } from '../../../../lib/stripe'
import { calculateFee } from '../../../../lib/pricing'
import { getSessionUid } from '../../../../lib/session-uid'
import { getTrustedOffer } from '../../../../lib/trusted-offer'
import { appendProbeParam, isProbeModeValue } from '../../../../lib/probe-mode'

/**
 * POST /api/checkout/create-session
 *
 * Creates a Stripe Checkout Session for unlocking a search.
 * The session metadata stores the anonymous user ID (from the httpOnly cookie)
 * and the searchId so the verify endpoint can link payment → unlock.
 */
export async function POST(req: NextRequest) {
  const uid = getSessionUid(req)
  if (!uid) {
    return NextResponse.json({ error: 'No session cookie' }, { status: 400 })
  }

  let offerId: string, searchId: string, probe: string | undefined
  try {
    ;({ offerId, searchId, probe } = await req.json())
  } catch {
    return NextResponse.json({ error: 'Invalid request body' }, { status: 400 })
  }

  const isProbe = isProbeModeValue(probe)

  if (!offerId || !searchId) {
    return NextResponse.json({ error: 'Missing required fields' }, { status: 400 })
  }

  const trustedOffer = await getTrustedOffer(offerId, searchId)
  if (!trustedOffer) {
    return NextResponse.json({ error: 'Offer not found for this search' }, { status: 404 })
  }

  const fee = calculateFee(trustedOffer.price, trustedOffer.currency)
  const unitAmount = toStripeAmount(fee, trustedOffer.currency)

  // Only trust the configured site URL, or a same-host origin in local/dev.
  const configuredSiteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? 'https://letsfg.co'
  const requestOrigin = req.headers.get('origin')
  let origin = configuredSiteUrl
  if (requestOrigin) {
    try {
      if (new URL(requestOrigin).host === new URL(configuredSiteUrl).host) {
        origin = requestOrigin
      }
    } catch {
      origin = configuredSiteUrl
    }
  }

  try {
    const successUrl = new URL(`/book/${offerId}`, origin)
    successUrl.searchParams.set('from', searchId)
    successUrl.searchParams.set('stripe_session', '{CHECKOUT_SESSION_ID}')
    appendProbeParam(successUrl.searchParams, isProbe)

    const cancelUrl = new URL(`/book/${offerId}`, origin)
    cancelUrl.searchParams.set('from', searchId)
    appendProbeParam(cancelUrl.searchParams, isProbe)

    const stripe = getStripe()
    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      line_items: [
        {
          price_data: {
            currency: trustedOffer.currency.toLowerCase(),
            product_data: {
              name: 'LetsFG search unlock',
              description:
                'Unlock all offers in this search permanently on this browser. One-time — revisit any offer in the same search for free.',
            },
            unit_amount: unitAmount,
          },
          quantity: 1,
        },
      ],
      // lfg_uid is stored server-side in metadata; the verify endpoint confirms
      // the cookie matches before recording the unlock. This prevents one user
      // sharing a session ID with another to steal an unlock.
      metadata: {
        lfg_uid: uid,
        search_id: searchId,
        offer_id: offerId,
      },
      // {CHECKOUT_SESSION_ID} is replaced by Stripe with the actual session ID.
      success_url: successUrl.toString(),
      cancel_url: cancelUrl.toString(),
    })

    return NextResponse.json({ url: session.url })
  } catch (err) {
    console.error('[checkout] create-session error:', err)
    return NextResponse.json({ error: 'Stripe error' }, { status: 500 })
  }
}
