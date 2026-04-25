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
