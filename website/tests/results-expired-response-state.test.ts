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

test('expired results API keeps rerun metadata but never serves stale cached offers', () => {
  const source = readSource('app/api/results/[searchId]/route.ts')

  assert.match(source, /function buildExpiredResult\(/)
  assert.match(source, /query: cachedResult\?\.query \|\| ''/)
  assert.match(source, /parsed: cachedResult\?\.parsed \|\| \{\}/)
  assert.doesNotMatch(source, /return NextResponse\.json\(cachedResult\)/)
})