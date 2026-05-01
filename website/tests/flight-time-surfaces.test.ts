import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url))
const WEBSITE_ROOT = path.resolve(TEST_DIR, '..')

function walkSourceFiles(dirPath: string): string[] {
  const entries = fs.readdirSync(dirPath, { withFileTypes: true })
  const files: string[] = []

  for (const entry of entries) {
    const fullPath = path.join(dirPath, entry.name)
    if (entry.isDirectory()) {
      files.push(...walkSourceFiles(fullPath))
      continue
    }

    if (fullPath.endsWith('.ts') || fullPath.endsWith('.tsx')) {
      files.push(fullPath)
    }
  }

  return files
}

function readSource(relativePath: string): string {
  return fs.readFileSync(path.join(WEBSITE_ROOT, relativePath), 'utf8')
}

test('website source does not bypass shared flight time formatting', () => {
  const sourceRoots = ['app', 'lib']

  for (const sourceRoot of sourceRoots) {
    for (const filePath of walkSourceFiles(path.join(WEBSITE_ROOT, sourceRoot))) {
      const relativePath = path.relative(WEBSITE_ROOT, filePath).replace(/\\/g, '/')
      if (relativePath === 'lib/flight-datetime.ts') {
        continue
      }

      const source = fs.readFileSync(filePath, 'utf8')
      assert.equal(
        source.includes('toLocaleTimeString('),
        false,
        `${relativePath} should route flight clock rendering through lib/flight-datetime.ts`,
      )
    }
  }
})

test('results and booking surfaces use formatFlightTime for displayed clocks', () => {
  const resultsPanel = readSource('app/results/[searchId]/ResultsPanel.tsx')
  assert.match(resultsPanel, /formatFlightTime\(offer\.departure_time\)/)
  assert.match(resultsPanel, /formatFlightTime\(offer\.arrival_time\)/)
  assert.match(resultsPanel, /formatFlightTime\(seg\.departure_time\)/)
  assert.match(resultsPanel, /formatFlightTime\(seg\.arrival_time\)/)

  const checkoutPanel = readSource('app/book/[offerId]/CheckoutPanel.tsx')
  assert.match(checkoutPanel, /function fmtTime\(iso: string\)\s*{\s*return formatFlightTime\(iso\)\s*}/s)

  const bookPage = readSource('app/book/[offerId]/page.tsx')
  assert.match(bookPage, /const fmtTime = formatFlightTime/)
})