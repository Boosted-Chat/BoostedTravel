import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url))
const WEBSITE_ROOT = path.resolve(TEST_DIR, '..')

function readSource(relativePath: string): string {
  return fs.readFileSync(path.join(WEBSITE_ROOT, relativePath), 'utf8')
}

test('results client resets visible state when a new searchId loads', () => {
  const source = readSource('app/results/[searchId]/SearchPageClient.tsx')

  assert.match(
    source,
    /useEffect\(\(\) => \{\s*trackedResultsViewRef\.current = false\s*trackedExpiredRef\.current = false\s*scrollMilestonesRef\.current = new Set\(\)\s*setStatus\(initialStatus\)\s*setProgress\(initialProgress\)\s*setOffers\(initialOffers\)\s*setDisplayCurrency\(initialCurrency\)\s*\}, \[searchId, initialCurrency\]\)/s,
  )
})