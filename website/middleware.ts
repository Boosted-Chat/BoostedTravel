import { NextRequest, NextResponse } from 'next/server'
import { randomUUID } from 'crypto'

/**
 * Sets a persistent, httpOnly anonymous user token cookie (`lfg_uid`) on every
 * request that doesn't already have one. This is the only identity signal we use
 * to scope search unlocks — no login required.
 *
 * The cookie is:
 *   - httpOnly  → JavaScript cannot read or tamper with it
 *   - SameSite=Strict → not sent on cross-site requests
 *   - Secure in production → only transmitted over HTTPS
 *   - 1-year max-age → survives browser restarts
 */
export function middleware(req: NextRequest) {
  const res = NextResponse.next()

  if (!req.cookies.get('lfg_uid')) {
    res.cookies.set('lfg_uid', randomUUID(), {
      httpOnly: true,
      sameSite: 'strict',
      secure: process.env.NODE_ENV === 'production',
      maxAge: 60 * 60 * 24 * 365, // 1 year
      path: '/',
    })
  }

  return res
}

export const config = {
  // Run on all paths except Next.js internals and static assets.
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
}
