import { NextRequest, NextResponse } from 'next/server'
import { getStripe } from '../../../../lib/stripe'
import { saveUnlock } from '../../../../lib/firestore'

/**
 * POST /api/checkout/verify
 *
 * Called by the client after Stripe redirects back with ?stripe_session=...
 * Verifies with Stripe that the payment succeeded, confirms the session belongs
 * to the current user (cookie check), then records the unlock in Firestore.
 */
export async function POST(req: NextRequest) {
  const uid = req.cookies.get('lfg_uid')?.value
  if (!uid) {
    return NextResponse.json({ unlocked: false, error: 'No session' }, { status: 400 })
  }

  let stripeSessionId: string
  try {
    ;({ stripeSessionId } = await req.json())
  } catch {
    return NextResponse.json({ unlocked: false, error: 'Invalid body' }, { status: 400 })
  }

  if (!stripeSessionId || !stripeSessionId.startsWith('cs_')) {
    return NextResponse.json({ unlocked: false, error: 'Invalid session ID' }, { status: 400 })
  }

  try {
    const stripe = getStripe()
    const session = await stripe.checkout.sessions.retrieve(stripeSessionId)

    // Security: ensure this Stripe session was created for THIS user.
    // An attacker who knows someone else's stripe_session cannot use it to unlock
    // their own account because the metadata uid won't match their cookie.
    if (session.metadata?.lfg_uid !== uid) {
      return NextResponse.json({ unlocked: false, error: 'Session mismatch' }, { status: 403 })
    }

    if (session.payment_status !== 'paid') {
      return NextResponse.json({ unlocked: false })
    }

    const searchId = session.metadata?.search_id
    if (!searchId) {
      return NextResponse.json({ unlocked: false, error: 'Missing search ID' }, { status: 500 })
    }

    // Record the unlock — persists to Firestore (or local Map in dev)
    await saveUnlock(uid, searchId)

    return NextResponse.json({ unlocked: true, searchId })
  } catch (err) {
    console.error('[checkout] verify error:', err)
    return NextResponse.json({ unlocked: false, error: 'Stripe error' }, { status: 500 })
  }
}
