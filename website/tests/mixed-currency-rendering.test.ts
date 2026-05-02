import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'
import { convertCurrencyAmount } from '../lib/display-price'

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url))
const WEBSITE_ROOT = path.resolve(TEST_DIR, '..')

function readSource(relativePath: string): string {
  return fs.readFileSync(path.join(WEBSITE_ROOT, relativePath), 'utf8')
}

test('display price conversion can normalize mixed offer currencies into PLN', () => {
  assert.equal(convertCurrencyAmount(41.99, 'GBP', 'PLN'), 204.16)
  assert.equal(convertCurrencyAmount(53, 'USD', 'PLN'), 189.99)
})

test('results panel formats visible prices in the selected currency', () => {
  const resultsPanel = readSource('app/results/[searchId]/ResultsPanel.tsx')
  const searchPageClient = readSource('app/results/[searchId]/SearchPageClient.tsx')
  const resultsPage = readSource('app/results/[searchId]/page.tsx')

  assert.match(
    resultsPanel,
    /getOfferDisplayTotalPrice\(offer, currency\)/,
  )

  assert.match(
    resultsPanel,
    /fmt\(getOfferDisplayTotalPrice\(displayOffers\[0\], currency\)\)/,
  )

  assert.match(
    searchPageClient,
    /currency=\{displayCurrency\}/,
  )

  assert.match(
    resultsPage,
    /getOfferDisplayTotalPrice\(offer, initialCurrency\)/,
  )

  assert.match(
    resultsPage,
    /priceCurrency: initialCurrency/,
  )
})