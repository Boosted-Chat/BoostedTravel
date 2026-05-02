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

test('expired results pages do not revive stale browser-cached completed results', () => {
  const source = readSource('app/results/[searchId]/SearchPageClient.tsx')

  assert.match(
    source,
    /useEffect\(\(\) => \{[\s\S]*if \(initialStatus !== 'searching'\) return[\s\S]*readBrowserCachedResults<FlightOffer>\(searchId\)/,
  )
})