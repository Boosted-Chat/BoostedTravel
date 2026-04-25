import { NextRequest, NextResponse } from 'next/server'
import { getStripe, toStripeAmount } from '../../../../lib/stripe'
import { calculateFee } from '../../../../lib/pricing'

/**
 * POST /api/checkout/create-session
 *
 * Creates a Stripe Checkout Session for unlocking a search.
 * The session metadata stores the anonymous user ID (from the httpOnly cookie)
 * and the searchId so the verify endpoint can link payment → unlock.
 */
export async function POST(req: NextRequest) {
  const uid = req.cookies.get('lfg_uid')?.value
  if (!uid) {
    return NextResponse.json({ error: 'No session cookie' }, { status: 400 })
  }

  let offerId: string, searchId: string, price: number, currency: string
  try {
    ;({ offerId, searchId, price, currency } = await req.json())
  } catch {
    return NextResponse.json({ error: 'Invalid request body' }, { status: 400 })
  }

  if (!offerId || !searchId || typeof price !== 'number' || !currency) {
    return NextResponse.json({ error: 'Missing required fields' }, { status: 400 })
  }

  const fee = calculateFee(price, currency)
  const unitAmount = toStripeAmount(fee, currency)

  // Determine the site origin for redirect URLs
  const origin =
    req.headers.get('origin') ||
    (process.env.NEXT_PUBLIC_SITE_URL ?? 'https://letsfg.co')

  try {
    const stripe = getStripe()
    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      line_items: [
        {
          price_data: {
            currency: currency.toLowerCase(),
            product_data: {
              name: 'LetsFG search unlock',
              description:
                'Unlock all offers in this search. One-time — revisit any offer in the same search for free.',
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
      success_url: `${origin}/book/${offerId}?from=${searchId}&stripe_session={CHECKOUT_SESSION_ID}`,
      cancel_url: `${origin}/book/${offerId}?from=${searchId}`,
    })

    return NextResponse.json({ url: session.url })
  } catch (err) {
    console.error('[checkout] create-session error:', err)
    return NextResponse.json({ error: 'Stripe error' }, { status: 500 })
  }
}
