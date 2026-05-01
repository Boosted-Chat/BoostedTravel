import assert from 'node:assert/strict'
import test from 'node:test'

import { findBestLocationMatch, findBestMatch, searchAirports } from '../app/airports.ts'
import { parseNLQuery } from '../app/lib/searchParsing.ts'

const RealDate = Date

function withFixedNow<T>(isoTimestamp: string, run: () => T): T {
  const fixedNow = new RealDate(isoTimestamp)

  class MockDate extends RealDate {
    constructor(...args: any[]) {
      super(...(args.length === 0 ? [fixedNow.getTime()] : args))
    }

    static now() {
      return fixedNow.getTime()
    }

    static parse(value: string) {
      return RealDate.parse(value)
    }

    static UTC(...args: Parameters<DateConstructor['UTC']>) {
      return RealDate.UTC(...args)
    }
  }

  globalThis.Date = MockDate as DateConstructor
  try {
    return run()
  } finally {
    globalThis.Date = RealDate
  }
}

test('parseNLQuery keeps the reported website examples working', () => {
  withFixedNow('2026-05-01T12:00:00Z', () => {
    const jfkToKarachi = parseNLQuery('New york jfk to karachi on 1st june 2026')
    assert.deepEqual(
      {
        origin: jfkToKarachi.origin,
        destination: jfkToKarachi.destination,
        date: jfkToKarachi.date,
      },
      {
        origin: 'JFK',
        destination: 'KHI',
        date: '2026-06-01',
      },
    )

    const londonToBarcelona = parseNLQuery('London to Barcelona next Friday')
    assert.deepEqual(
      {
        origin: londonToBarcelona.origin,
        destination: londonToBarcelona.destination,
        date: londonToBarcelona.date,
      },
      {
        origin: 'LON',
        destination: 'BCN',
        date: '2026-05-08',
      },
    )

    const nycToTokyo = parseNLQuery('NYC to Tokyo in June, business class')
    assert.deepEqual(
      {
        origin: nycToTokyo.origin,
        destination: nycToTokyo.destination,
        date: nycToTokyo.date,
        cabin: nycToTokyo.cabin,
      },
      {
        origin: 'NYC',
        destination: 'TYO',
        date: '2026-06-01',
        cabin: 'C',
      },
    )
  })
})

test('parseNLQuery falls back to generated global coverage for long-tail names', () => {
  withFixedNow('2026-05-01T12:00:00Z', () => {
    const southamptonToEdinburgh = parseNLQuery('Southampton to Edinburgh next Friday')
    assert.deepEqual(
      {
        origin: southamptonToEdinburgh.origin,
        destination: southamptonToEdinburgh.destination,
        date: southamptonToEdinburgh.date,
      },
      {
        origin: 'SOU',
        destination: 'EDI',
        date: '2026-05-08',
      },
    )

    const ashgabatToTirana = parseNLQuery('Aşgabat to Tiranë on 1st june 2026')
    assert.deepEqual(
      {
        origin: ashgabatToTirana.origin,
        destination: ashgabatToTirana.destination,
        date: ashgabatToTirana.date,
      },
      {
        origin: 'ASB',
        destination: 'TIA',
        date: '2026-06-01',
      },
    )

    const abidjanToAalborg = parseNLQuery('Abidjan to Aalborg on 1st june 2026')
    assert.deepEqual(
      {
        origin: abidjanToAalborg.origin,
        destination: abidjanToAalborg.destination,
        date: abidjanToAalborg.date,
      },
      {
        origin: 'ABJ',
        destination: 'AAL',
        date: '2026-06-01',
      },
    )
  })
})

test('parseNLQuery handles shorthand airports, holiday weeks, and metro aliases', () => {
  withFixedNow('2026-05-01T12:00:00Z', () => {
    const triesteToArlanda = parseNLQuery('Trs to arl 17 july')
    assert.deepEqual(
      {
        origin: triesteToArlanda.origin,
        destination: triesteToArlanda.destination,
        date: triesteToArlanda.date,
      },
      {
        origin: 'TRS',
        destination: 'ARN',
        date: '2026-07-17',
      },
    )

    const bdlThanksgiving = parseNLQuery('BDL to SAN the week of thanksgiving')
    assert.deepEqual(
      {
        origin: bdlThanksgiving.origin,
        destination: bdlThanksgiving.destination,
        date: bdlThanksgiving.date,
      },
      {
        origin: 'BDL',
        destination: 'SAN',
        date: '2026-11-23',
      },
    )

    const hartfordThanksgiving = parseNLQuery('Hartford to san diego the week of thanksgiving')
    assert.deepEqual(
      {
        origin: hartfordThanksgiving.origin,
        destination: hartfordThanksgiving.destination,
        date: hartfordThanksgiving.date,
      },
      {
        origin: 'BDL',
        destination: 'SAN',
        date: '2026-11-23',
      },
    )

    const helsinkiToRome = parseNLQuery('helsinki to rome')
    assert.deepEqual(
      {
        origin: helsinkiToRome.origin,
        destination: helsinkiToRome.destination,
      },
      {
        origin: 'HEL',
        destination: 'ROM',
      },
    )
  })
})

test('findBestLocationMatch prefers explicit airports and city codes correctly', () => {
  assert.deepEqual(findBestLocationMatch('Aşgabat'), {
    code: 'ASB',
    name: 'Ashgabat Airport',
    type: 'airport',
    country: 'TM',
  })

  assert.deepEqual(findBestLocationMatch('New York JFK'), {
    code: 'JFK',
    name: 'John F Kennedy International Airport',
    type: 'airport',
    country: 'US',
  })

  assert.deepEqual(findBestLocationMatch('Tokyo'), {
    code: 'TYO',
    name: 'Tokyo',
    type: 'city',
    country: 'JP',
  })
})

test('homepage airport matching uses generated aliases and expanded airport coverage', () => {
  assert.equal(findBestMatch('Aşgabat', 'en')?.code, 'ASB')
  assert.equal(findBestMatch('Tiranë', 'en')?.code, 'TIA')
  assert.equal(findBestMatch('Aalborg', 'en')?.code, 'AAL')
  assert.equal(findBestMatch('Hartford', 'en')?.code, 'BDL')

  const airportResults = searchAirports('ashgabat', 'en', 5)
  assert.ok(airportResults.some((airport) => airport.code === 'ASB'))

  const hartfordResults = searchAirports('Hartford', 'en', 5)
  assert.ok(hartfordResults.some((airport) => airport.code === 'BDL'))
})