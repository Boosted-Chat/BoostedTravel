/**
 * Airline & OTA logo utilities for all LetsFG connectors.
 *
 * Airlines  → avs.io CDN by IATA code:  https://pics.avs.io/100/100/{IATA}.png
 * OTAs      → Google favicon service:    https://www.google.com/s2/favicons?domain={domain}&sz=64
 */

// ── Airlines: connector name → IATA code ──────────────────────────────────────
export const CONNECTOR_IATA: Record<string, string> = {
  aegean:               'A3',
  aerlingus:            'EI',
  aerolineas:           'AR',
  airarabia:            'G9',
  airasia:              'AK',
  airasiamove:          'AK',
  airasiax:             'D7',
  airbaltic:            'BT',
  airbusan:             'BX',
  aircairo:             'SM',
  aircalin:             'SB',
  aircanada:            'AC',
  airchina:             'CA',
  aireuropa:            'UX',
  airfrance:            'AF',
  airgreenland:         'GL',
  airindia:             'AI',
  airindiaexpress:      'IX',
  airmauritius:         'MK',
  airnewzealand:        'NZ',
  airniugini:           'PX',
  airnorth:             'TL',
  airpeace:             'P4',
  airserbia:            'JU',
  airseychelles:        'HM',
  airtahitinui:         'TN',
  airtransat:           'TS',
  airvanuatu:           'NF',
  akasa:                'QP',
  alaska:               'AS',
  allegiant:            'G4',
  american:             'AA',
  arajet:               'DM',
  asiana:               'OZ',
  austrian:             'OS',
  avelo:                'XP',
  avianca:              'AV',
  azerbaijanairlines:   'J2',
  azoresairlines:       'S4',
  azul:                 'AD',
  bangkokairways:       'PG',
  batikair:             'ID',
  biman:                'BG',
  breeze:               'MX',
  britishairways:       'BA',
  brusselsairlines:     'SN',
  caribbeanairlines:    'BW',
  cathay:               'CX',
  cebupacific:          '5J',
  chinaairlines:        'CI',
  chinaeastern:         'MU',
  chinasouthern:        'CZ',
  citilink:             'QG',
  condor:               'DE',
  copa:                 'CM',
  cyprusairways:        'CY',
  delta:                'DL',
  discover:             '4Y',
  easyjet:              'U2',
  egyptair:             'MS',
  elal:                 'LY',
  emirates:             'EK',
  ethiopian:            'ET',
  etihad:               'EY',
  eurowings:            'EW',
  evaair:               'BR',
  fijiairways:          'FJ',
  finnair:              'AY',
  flair:                'F8',
  flyadeal:             'F3',
  flyarystan:           'KC',
  flybondi:             'FO',
  flydubai:             'FZ',
  flynas:               'XY',
  flysafair:            'FA',
  frontier:             'F9',
  garuda:               'GA',
  gol:                  'G3',
  gulfair:              'GF',
  hainan:               'HU',
  hawaiian:             'HA',
  hkexpress:            'UO',
  iberia:               'IB',
  iberiaexpress:        'I2',
  ibomair:              '4D',
  icelandair:           'FI',
  indigo:               '6E',
  itaairways:           'JJ',
  itaairways_old:       'JJ',
  itaairways_v2:        'JJ',
  jal:                  'JL',
  jazeera:              'J9',
  jejuair:              '7C',
  jet2:                 'LS',
  jet2_backup:          'LS',
  jetblue:              'B6',
  jetsmart:             'JA',
  jetstar:              'JQ',
  jinair:               'LJ',
  kenyaairways:         'KQ',
  klm:                  'KL',
  korean:               'KE',
  kuwaitairways:        'KU',
  latam:                'LA',
  level:                'VU',
  linkairways:          'FC',
  lionair:              'JT',
  lot:                  'LO',
  luckyair:             '8L',
  lufthansa:            'LH',
  malaysia:             'MH',
  mea:                  'ME',
  nh:                   'NH',
  nineair:              'AQ',
  nokair:               'DD',
  norwegian:            'DY',
  olympicair_api:       'OA',
  omanair:              'WY',
  peach:                'MM',
  pegasus:              'PC',
  philippineairlines:   'PR',
  pia:                  'PK',
  pngair:               'CG',
  porter:               'PD',
  qantas:               'QF',
  qatar:                'QR',
  rex:                  'ZL',
  royalairmaroc:        'AT',
  royaljordanian:       'RJ',
  rwandair:             'WB',
  ryanair:              'FR',
  saa:                  'SA',
  salamair:             'OV',
  samoaairways:         'OL',
  sas:                  'SK',
  saudia:               'SV',
  scoot:                'TR',
  singapore:            'SQ',
  skyairline:           'H2',
  skyexpress:           'GQ',
  skymark:              'BC',
  smartwings:           'QS',
  solomonairlines:      'IE',
  southwest:            'WN',
  spicejet:             'SG',
  spirit:               'NK',
  spirit_clean:         'NK',
  spring:               'IJ',
  srilankan:            'UL',
  starlux:              'JX',
  suncountry:           'SY',
  sunexpress:           'XQ',
  superairjet:          'IU',
  swiss:                'LX',
  tap:                  'TP',
  thai:                 'TG',
  transavia:            'HV',
  transnusa:            '5R',
  turkish:              'TK',
  twayair:              'TW',
  united:               'UA',
  usbangla:             'BS',
  vietjet:              'VJ',
  vietnamairlines:      'VN',
  virginatlantic:       'VS',
  virginaustralia:      'VA',
  vivaaerobus:          'VB',
  volaris:              'Y4',
  volotea:              'V7',
  vueling:              'VY',
  westjet:              'WS',
  wingo:                'P5',
  wizzair:              'W6',
  zipair:               'ZG',
}

// ── OTAs: connector name → domain ─────────────────────────────────────────────
export const OTA_DOMAINS: Record<string, string> = {
  agoda:            'agoda.com',
  akbartravels:     'akbartravels.com',
  almosafer:        'almosafer.com',
  almundo:          'almundo.com',
  asaptickets:      'asaptickets.com',
  auntbetty:        'auntbetty.com',
  aviasales:        'aviasales.com',
  bookingcom:       'booking.com',
  byojet:           'byojet.com',
  cheapflights:     'cheapflights.com',
  cheapoair:        'cheapoair.com',
  cleartrip:        'cleartrip.com',
  despegar:         'despegar.com',
  edreams:          'edreams.com',
  esky:             'esky.com',
  etraveli:         'etraveli.com',
  expedia:          'expedia.com',
  flightcatchers:   'flightcatchers.com',
  hopper:           'hopper.com',
  iwantthatflight:  'iwantthatflight.com.au',
  ixigo:            'ixigo.com',
  kayak:            'kayak.com',
  kiwi:             'kiwi.com',
  lastminute:       'lastminute.com',
  makemytrip:       'makemytrip.com',
  momondo:          'momondo.com',
  musafir:          'musafir.com',
  onthebeach:       'onthebeach.co.uk',
  opodo:            'opodo.com',
  priceline:        'priceline.com',
  rehlat:           'rehlat.com',
  google_flights:   'google.com',
  serpapi_google:   'google.com',
  skiplagged:       'skiplagged.com',
  skyscanner:       'skyscanner.net',
  smartfares:       'smartfares.com',
  tiket:            'tiket.com',
  travelgenio:      'travelgenio.com',
  traveloka:        'traveloka.com',
  travelstart:      'travelstart.com',
  traveltrolley:    'traveltrolley.co.uk',
  travelup:         'travelup.com',
  travix:           'travix.com',
  tripcom:          'trip.com',
  tripsta:          'tripsta.com',
  webjet:           'webjet.com.au',
  wego:             'wego.com',
  yatra:            'yatra.com',
}

// ── IATA code → airline display name ─────────────────────────────────────────
// Used when FSW connectors return IATA codes in airlines[] instead of full names.
export const IATA_TO_NAME: Record<string, string> = {
  // Europe
  A3: 'Aegean Airlines', EI: 'Aer Lingus', AR: 'Aerolíneas Argentinas',
  G9: 'Air Arabia', AF: 'Air France', AI: 'Air India', IX: 'Air India Express',
  MK: 'Air Mauritius', NZ: 'Air New Zealand', JU: 'Air Serbia',
  HM: 'Air Seychelles', TS: 'Air Transat', AC: 'Air Canada', CA: 'Air China',
  UX: 'Air Europa', NF: 'Air Vanuatu', QP: 'Akasa Air', AS: 'Alaska Airlines',
  G4: 'Allegiant Air', AA: 'American Airlines', DM: 'Arajet', OZ: 'Asiana Airlines',
  OS: 'Austrian', XP: 'Avelo Airlines', AV: 'Avianca', J2: 'Azerbaijan Airlines',
  S4: 'Azores Airlines', AD: 'Azul', PG: 'Bangkok Airways', ID: 'Batik Air',
  BG: 'Biman Bangladesh', MX: 'Breeze Airways', BA: 'British Airways',
  SN: 'Brussels Airlines', BW: 'Caribbean Airlines', CX: 'Cathay Pacific',
  '5J': 'Cebu Pacific', CI: 'China Airlines', MU: 'China Eastern',
  CZ: 'China Southern', QG: 'Citilink', DE: 'Condor', CM: 'Copa Airlines',
  CY: 'Cyprus Airways', DL: 'Delta Air Lines', '4Y': 'Discover Airlines',
  U2: 'easyJet', MS: 'EgyptAir', LY: 'El Al', EK: 'Emirates',
  ET: 'Ethiopian Airlines', EY: 'Etihad Airways', EW: 'Eurowings',
  BR: 'EVA Air', FJ: 'Fiji Airways', AY: 'Finnair', F8: 'Flair Airlines',
  F3: 'Flyadeal', KC: 'FlyArystan', FO: 'Flybondi', FZ: 'flydubai',
  XY: 'flynas', FA: 'FlySafair', F9: 'Frontier Airlines', GA: 'Garuda Indonesia',
  G3: 'GOL', GF: 'Gulf Air', HU: 'Hainan Airlines', HA: 'Hawaiian Airlines',
  UO: 'HK Express', IB: 'Iberia', I2: 'Iberia Express', FI: 'Icelandair',
  '6E': 'IndiGo', JJ: 'LATAM Brasil', JL: 'Japan Airlines', J9: 'Jazeera Airways',
  '7C': 'Jeju Air', LS: 'Jet2', JA: 'JetSMART', JQ: 'Jetstar', LJ: 'Jin Air',
  KQ: 'Kenya Airways', KL: 'KLM', KE: 'Korean Air', KU: 'Kuwait Airways',
  LA: 'LATAM Airlines', VU: 'Level', FC: 'Link Airways', JT: 'Lion Air',
  LO: 'LOT Polish Airlines', '8L': 'Lucky Air', LH: 'Lufthansa',
  MH: 'Malaysia Airlines', ME: 'Middle East Airlines', NH: 'ANA',
  DD: 'Nok Air', DY: 'Norwegian', OA: 'Olympic Air', WY: 'Oman Air',
  MM: 'Peach Aviation', PC: 'Pegasus Airlines', PR: 'Philippine Airlines',
  PK: 'PIA', PD: 'Porter Airlines', QF: 'Qantas', QR: 'Qatar Airways',
  ZL: 'Rex Airlines', AT: 'Royal Air Maroc', RJ: 'Royal Jordanian',
  WB: 'Rwandair', FR: 'Ryanair', RK: 'Ryanair UK', SA: 'South African Airways',
  OV: 'SalamAir', SK: 'SAS', SV: 'Saudia', TR: 'Scoot',
  SQ: 'Singapore Airlines', H2: 'Sky Airline', GQ: 'Sky Express',
  BC: 'Skymark', QS: 'SmartWings', WN: 'Southwest Airlines',
  SG: 'SpiceJet', NK: 'Spirit Airlines', IJ: 'Spring Airlines',
  UL: 'SriLankan Airlines', JX: 'Starlux', SY: 'Sun Country Airlines',
  XQ: 'SunExpress', LX: 'Swiss', TP: 'TAP Air Portugal', TG: 'Thai Airways',
  HV: 'Transavia', TK: 'Turkish Airlines', TW: "T'way Air",
  UA: 'United Airlines', VJ: 'VietJet Air', VN: 'Vietnam Airlines',
  VS: 'Virgin Atlantic', VA: 'Virgin Australia', VB: 'VivaAerobus',
  Y4: 'Volaris', V7: 'Volotea', VY: 'Vueling', WS: 'WestJet',
  P5: 'Wingo', W6: 'Wizz Air', W9: 'Wizz Air UK', ZG: 'Zipair',
  // Common numeric-prefixed codes
  '2W': 'World2Fly', '4U': 'Germanwings', '5O': 'ASL Airlines France',
}

function normalizeAirlineLookupValue(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '')
}

const NAME_TO_IATA: Record<string, string> = Object.entries(IATA_TO_NAME).reduce<Record<string, string>>((map, [code, name]) => {
  map[normalizeAirlineLookupValue(name)] = code
  return map
}, {
  wizzairmalta: 'W6',
  wizzairuk: 'W9',
})

/** Look up airline display name from IATA code. Returns null if unknown. */
export function getAirlineNameFromCode(iata: string): string | null {
  return IATA_TO_NAME[iata.toUpperCase()] ?? null
}

/** Look up airline IATA code from a display name. Returns null if unknown. */
export function getAirlineCodeFromName(name: string): string | null {
  return NAME_TO_IATA[normalizeAirlineLookupValue(name)] ?? null
}

/** Return true if the string looks like an airline IATA code (2 chars: letters/digits). */
export function looksLikeIataCode(s: string): boolean {
  return /^[A-Z0-9]{2}$/i.test(s.trim())
}

// ── URL builders ──────────────────────────────────────────────────────────────

/** Return a logo URL for a given IATA code (e.g. "FR", "W6"). */
export function getLogoByIata(iata: string): string {
  return `https://pics.avs.io/100/100/${iata}.png`
}

const IATA_LOGO_OVERRIDES: Record<string, string> = {
  // avs.io does not consistently serve an easyJet logo, so use a known airline asset.
  U2: 'https://images.kiwi.com/airlines/64/U2.png',
  // Wizz Air UK should reuse the main Wizz brand mark when W9-specific art is missing.
  W9: getLogoByIata('W6'),
}

/** Return a favicon URL for a given OTA domain. */
export function getFaviconByDomain(domain: string): string {
  return `https://www.google.com/s2/favicons?domain=${domain}&sz=64`
}

/**
 * Get a logo URL from an IATA code (primary, used in results page).
 * Automatically falls back gracefully via onError in the component.
 */
export function getAirlineLogoUrl(airlineCode: string): string {
  const normalized = airlineCode.trim().toUpperCase()
  return IATA_LOGO_OVERRIDES[normalized] ?? getLogoByIata(normalized)
}

/**
 * Get a logo URL from a connector name.
 * Useful for source attribution, connector list pages, etc.
 */
export function getConnectorLogoUrl(connectorName: string): string | null {
  const iata = CONNECTOR_IATA[connectorName]
  if (iata) return getLogoByIata(iata)
  const domain = OTA_DOMAINS[connectorName]
  if (domain) return getFaviconByDomain(domain)
  return null
}
