import { NextRequest, NextResponse } from 'next/server'
import { checkUnlock } from '../../../lib/firestore'

/**
 * GET /api/unlock-status?searchId=...
 *
 * Returns whether the current user (identified by their httpOnly lfg_uid cookie)
 * has an active unlock for the given searchId.
 */
export async function GET(req: NextRequest) {
  const uid = req.cookies.get('lfg_uid')?.value
  if (!uid) {
    return NextResponse.json({ unlocked: false })
  }

  const searchId = req.nextUrl.searchParams.get('searchId')
  if (!searchId) {
    return NextResponse.json({ unlocked: false })
  }

  const unlocked = await checkUnlock(uid, searchId)
  return NextResponse.json({ unlocked })
}
