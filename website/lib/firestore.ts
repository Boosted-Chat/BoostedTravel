/**
 * Firestore client for the Next.js website.
 *
 * Uses firebase-admin with Application Default Credentials (ADC).
 * On Cloud Run, ADC is automatic (service account attached to the revision).
 * Locally, set GOOGLE_CLOUD_PROJECT=sms-caller and run:
 *   gcloud auth application-default login
 *
 * Database: sms-caller / default (us-central1)
 * Collection: search_results
 * TTL: 30 min — stored as `expires_at` field; documents deleted on stale read.
 */

import { getApps, initializeApp } from 'firebase-admin/app'
import { getFirestore, Firestore } from 'firebase-admin/firestore'

const COLLECTION = 'search_results'
const TTL_MS = 30 * 60 * 1000

let _db: Firestore | null = null
let _initFailed = false

function getDb(): Firestore | null {
  // Only run on Cloud Run — K_SERVICE is set automatically by the platform.
  // Never initialise locally to avoid accidental credential exposure.
  if (!process.env.K_SERVICE) return null
  if (_initFailed) return null
  if (_db) return _db
  try {
    if (getApps().length === 0) initializeApp()
    _db = getFirestore('default')
    return _db
  } catch (err) {
    console.warn('[firestore] init failed:', (err as Error).message)
    _initFailed = true
    return null
  }
}

/**
 * Persist a completed search result to Firestore.
 * Fire-and-forget — never awaited on the hot path.
 */
export async function saveSearchResult(
  searchId: string,
  result: Record<string, unknown>,
): Promise<void> {
  const db = getDb()
  if (!db) return
  try {
    await db.collection(COLLECTION).doc(searchId).set({
      result,
      expires_at: new Date(Date.now() + TTL_MS),
    })
  } catch (err) {
    console.warn('[firestore] saveSearchResult failed:', (err as Error).message)
  }
}

/**
 * Load a previously saved search result from Firestore.
 * Returns null if not found, expired, or Firestore unavailable.
 */
export async function loadSearchResult(
  searchId: string,
): Promise<Record<string, unknown> | null> {
  const db = getDb()
  if (!db) return null
  try {
    const snap = await db.collection(COLLECTION).doc(searchId).get()
    if (!snap.exists) return null
    const data = snap.data()!
    const expiresAt: Date | undefined = data.expires_at?.toDate?.()
    if (expiresAt && expiresAt < new Date()) {
      snap.ref.delete().catch(() => {})
      return null
    }
    return data.result as Record<string, unknown>
  } catch (err) {
    console.warn('[firestore] loadSearchResult failed:', (err as Error).message)
    return null
  }
}

// ── Search unlocks ────────────────────────────────────────────────────────────

const UNLOCK_COLLECTION = 'search_unlocks'
const UNLOCK_TTL_MS = 2 * 60 * 60 * 1000 // 2 hours — generous window for the user to revisit

function unlockKey(uid: string, searchId: string): string {
  return `${uid}|${searchId}`
}

/**
 * Record that a user has unlocked a search. Stored in Firestore only.
 * If Firestore is unavailable (not on Cloud Run), the write is a no-op —
 * the user will need to pay again. No in-memory fallback.
 */
export async function saveUnlock(uid: string, searchId: string): Promise<void> {
  const db = getDb()
  if (!db) return // not on Cloud Run — no-op, no bypass

  try {
    await db.collection(UNLOCK_COLLECTION).doc(unlockKey(uid, searchId)).set({
      uid,
      search_id: searchId,
      expires_at: new Date(Date.now() + UNLOCK_TTL_MS),
      created_at: new Date(),
    })
  } catch (err) {
    console.error('[firestore] saveUnlock failed:', (err as Error).message)
    // Do NOT fall back to any in-memory store — if we can't write the unlock,
    // the payment confirmation will fail on the next /api/unlock-status check.
    throw err
  }
}

/**
 * Check whether a user has a valid unlock for a given search.
 * Returns true only if Firestore confirms a valid, unexpired record.
 * Returns false on any error or if Firestore is unavailable.
 */
export async function checkUnlock(uid: string, searchId: string): Promise<boolean> {
  const db = getDb()
  if (!db) return false // not on Cloud Run — never grant access

  try {
    const snap = await db.collection(UNLOCK_COLLECTION).doc(unlockKey(uid, searchId)).get()
    if (!snap.exists) return false
    const data = snap.data()!
    const expiresAt: Date | undefined = data.expires_at?.toDate?.()
    if (!expiresAt || expiresAt < new Date()) {
      snap.ref.delete().catch(() => {})
      return false
    }
    return true
  } catch (err) {
    console.warn('[firestore] checkUnlock failed:', (err as Error).message)
    return false // fail closed — deny access on any Firestore error
  }
}

