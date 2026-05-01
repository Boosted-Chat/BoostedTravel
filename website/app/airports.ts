import { GENERATED_AIRPORT_SUPPLEMENT } from './lib/generated-airport-supplement'
import { GENERATED_LOCATIONS, type GeneratedLocationEntry } from './lib/generated-locations'

// Airport database with locale-aware names
// Format: { code, names: { locale: name }, country }

export interface Airport {
  code: string
  names: Record<string, string>
  country: string
}

// Top 200+ airports worldwide with localized names where relevant
export const AIRPORTS: Airport[] = [
  // Poland
  { code: 'GDN', names: { en: 'Gdansk', pl: 'Gdańsk', de: 'Danzig' }, country: 'PL' },
  { code: 'WAW', names: { en: 'Warsaw', pl: 'Warszawa', de: 'Warschau' }, country: 'PL' },
  { code: 'KRK', names: { en: 'Krakow', pl: 'Kraków', de: 'Krakau' }, country: 'PL' },
  { code: 'WRO', names: { en: 'Wroclaw', pl: 'Wrocław', de: 'Breslau' }, country: 'PL' },
  { code: 'POZ', names: { en: 'Poznan', pl: 'Poznań', de: 'Posen' }, country: 'PL' },
  { code: 'KTW', names: { en: 'Katowice', pl: 'Katowice' }, country: 'PL' },
  { code: 'LCJ', names: { en: 'Lodz', pl: 'Łódź' }, country: 'PL' },
  { code: 'RZE', names: { en: 'Rzeszow', pl: 'Rzeszów' }, country: 'PL' },
  { code: 'SZZ', names: { en: 'Szczecin', pl: 'Szczecin', de: 'Stettin' }, country: 'PL' },
  { code: 'BZG', names: { en: 'Bydgoszcz', pl: 'Bydgoszcz' }, country: 'PL' },

  // Spain
  { code: 'BCN', names: { en: 'Barcelona', es: 'Barcelona', fr: 'Barcelone', it: 'Barcellona' }, country: 'ES' },
  { code: 'MAD', names: { en: 'Madrid', es: 'Madrid' }, country: 'ES' },
  { code: 'PMI', names: { en: 'Palma de Mallorca', es: 'Palma de Mallorca', de: 'Palma de Mallorca' }, country: 'ES' },
  { code: 'AGP', names: { en: 'Malaga', es: 'Málaga' }, country: 'ES' },
  { code: 'ALC', names: { en: 'Alicante', es: 'Alicante' }, country: 'ES' },
  { code: 'VLC', names: { en: 'Valencia', es: 'Valencia', fr: 'Valence' }, country: 'ES' },
  { code: 'SVQ', names: { en: 'Seville', es: 'Sevilla', fr: 'Séville' }, country: 'ES' },
  { code: 'IBZ', names: { en: 'Ibiza', es: 'Ibiza' }, country: 'ES' },
  { code: 'TFS', names: { en: 'Tenerife South', es: 'Tenerife Sur' }, country: 'ES' },
  { code: 'LPA', names: { en: 'Gran Canaria', es: 'Gran Canaria' }, country: 'ES' },
  { code: 'BIO', names: { en: 'Bilbao', es: 'Bilbao' }, country: 'ES' },

  // UK
  { code: 'LHR', names: { en: 'London Heathrow' }, country: 'GB' },
  { code: 'LGW', names: { en: 'London Gatwick' }, country: 'GB' },
  { code: 'STN', names: { en: 'London Stansted' }, country: 'GB' },
  { code: 'LTN', names: { en: 'London Luton' }, country: 'GB' },
  { code: 'MAN', names: { en: 'Manchester' }, country: 'GB' },
  { code: 'EDI', names: { en: 'Edinburgh', de: 'Edinburgh' }, country: 'GB' },
  { code: 'BHX', names: { en: 'Birmingham' }, country: 'GB' },
  { code: 'GLA', names: { en: 'Glasgow' }, country: 'GB' },
  { code: 'BRS', names: { en: 'Bristol' }, country: 'GB' },
  { code: 'NCL', names: { en: 'Newcastle' }, country: 'GB' },
  { code: 'LPL', names: { en: 'Liverpool' }, country: 'GB' },
  { code: 'EMA', names: { en: 'East Midlands' }, country: 'GB' },
  { code: 'LBA', names: { en: 'Leeds Bradford' }, country: 'GB' },

  // Germany
  { code: 'FRA', names: { en: 'Frankfurt', de: 'Frankfurt' }, country: 'DE' },
  { code: 'MUC', names: { en: 'Munich', de: 'München', fr: 'Munich', it: 'Monaco' }, country: 'DE' },
  { code: 'BER', names: { en: 'Berlin', de: 'Berlin' }, country: 'DE' },
  { code: 'DUS', names: { en: 'Dusseldorf', de: 'Düsseldorf' }, country: 'DE' },
  { code: 'HAM', names: { en: 'Hamburg', de: 'Hamburg' }, country: 'DE' },
  { code: 'CGN', names: { en: 'Cologne', de: 'Köln', fr: 'Cologne' }, country: 'DE' },
  { code: 'STR', names: { en: 'Stuttgart', de: 'Stuttgart' }, country: 'DE' },
  { code: 'HAJ', names: { en: 'Hanover', de: 'Hannover' }, country: 'DE' },
  { code: 'NUE', names: { en: 'Nuremberg', de: 'Nürnberg' }, country: 'DE' },
  { code: 'LEJ', names: { en: 'Leipzig', de: 'Leipzig' }, country: 'DE' },
  { code: 'DRS', names: { en: 'Dresden', de: 'Dresden' }, country: 'DE' },
  { code: 'DTM', names: { en: 'Dortmund', de: 'Dortmund' }, country: 'DE' },

  // France
  { code: 'CDG', names: { en: 'Paris Charles de Gaulle', fr: 'Paris CDG' }, country: 'FR' },
  { code: 'ORY', names: { en: 'Paris Orly', fr: 'Paris Orly' }, country: 'FR' },
  { code: 'NCE', names: { en: 'Nice', fr: 'Nice', it: 'Nizza' }, country: 'FR' },
  { code: 'LYS', names: { en: 'Lyon', fr: 'Lyon' }, country: 'FR' },
  { code: 'MRS', names: { en: 'Marseille', fr: 'Marseille' }, country: 'FR' },
  { code: 'TLS', names: { en: 'Toulouse', fr: 'Toulouse' }, country: 'FR' },
  { code: 'BOD', names: { en: 'Bordeaux', fr: 'Bordeaux' }, country: 'FR' },
  { code: 'NTE', names: { en: 'Nantes', fr: 'Nantes' }, country: 'FR' },
  { code: 'SXB', names: { en: 'Strasbourg', fr: 'Strasbourg', de: 'Straßburg' }, country: 'FR' },

  // Italy
  { code: 'FCO', names: { en: 'Rome Fiumicino', it: 'Roma Fiumicino', fr: 'Rome' }, country: 'IT' },
  { code: 'MXP', names: { en: 'Milan Malpensa', it: 'Milano Malpensa' }, country: 'IT' },
  { code: 'LIN', names: { en: 'Milan Linate', it: 'Milano Linate' }, country: 'IT' },
  { code: 'VCE', names: { en: 'Venice', it: 'Venezia', de: 'Venedig', fr: 'Venise' }, country: 'IT' },
  { code: 'NAP', names: { en: 'Naples', it: 'Napoli', fr: 'Naples' }, country: 'IT' },
  { code: 'FLR', names: { en: 'Florence', it: 'Firenze', de: 'Florenz' }, country: 'IT' },
  { code: 'BLQ', names: { en: 'Bologna', it: 'Bologna' }, country: 'IT' },
  { code: 'PSA', names: { en: 'Pisa', it: 'Pisa' }, country: 'IT' },
  { code: 'CTA', names: { en: 'Catania', it: 'Catania' }, country: 'IT' },
  { code: 'PMO', names: { en: 'Palermo', it: 'Palermo' }, country: 'IT' },
  { code: 'BGY', names: { en: 'Milan Bergamo', it: 'Milano Bergamo' }, country: 'IT' },
  { code: 'TRN', names: { en: 'Turin', it: 'Torino', fr: 'Turin' }, country: 'IT' },

  // Netherlands
  { code: 'AMS', names: { en: 'Amsterdam', nl: 'Amsterdam', de: 'Amsterdam' }, country: 'NL' },
  { code: 'EIN', names: { en: 'Eindhoven', nl: 'Eindhoven' }, country: 'NL' },
  { code: 'RTM', names: { en: 'Rotterdam', nl: 'Rotterdam' }, country: 'NL' },

  // Portugal
  { code: 'LIS', names: { en: 'Lisbon', pt: 'Lisboa', es: 'Lisboa', fr: 'Lisbonne' }, country: 'PT' },
  { code: 'OPO', names: { en: 'Porto', pt: 'Porto' }, country: 'PT' },
  { code: 'FAO', names: { en: 'Faro', pt: 'Faro' }, country: 'PT' },
  { code: 'FNC', names: { en: 'Funchal Madeira', pt: 'Funchal' }, country: 'PT' },

  // Croatia
  { code: 'ZAG', names: { en: 'Zagreb', hr: 'Zagreb', de: 'Zagreb' }, country: 'HR' },
  { code: 'SPU', names: { en: 'Split', hr: 'Split' }, country: 'HR' },
  { code: 'DBV', names: { en: 'Dubrovnik', hr: 'Dubrovnik' }, country: 'HR' },
  { code: 'ZAD', names: { en: 'Zadar', hr: 'Zadar' }, country: 'HR' },
  { code: 'PUY', names: { en: 'Pula', hr: 'Pula' }, country: 'HR' },

  // Sweden
  { code: 'ARN', names: { en: 'Stockholm Arlanda', sv: 'Stockholm Arlanda' }, country: 'SE' },
  { code: 'GOT', names: { en: 'Gothenburg', sv: 'Göteborg', de: 'Göteborg' }, country: 'SE' },
  { code: 'MMX', names: { en: 'Malmo', sv: 'Malmö' }, country: 'SE' },

  // Albania
  { code: 'TIA', names: { en: 'Tirana', sq: 'Tiranë' }, country: 'AL' },

  // Greece
  { code: 'ATH', names: { en: 'Athens', de: 'Athen', fr: 'Athènes', it: 'Atene' }, country: 'GR' },
  { code: 'SKG', names: { en: 'Thessaloniki', de: 'Thessaloniki' }, country: 'GR' },
  { code: 'HER', names: { en: 'Heraklion Crete', de: 'Heraklion' }, country: 'GR' },
  { code: 'RHO', names: { en: 'Rhodes', de: 'Rhodos', it: 'Rodi' }, country: 'GR' },
  { code: 'CFU', names: { en: 'Corfu', de: 'Korfu', it: 'Corfù' }, country: 'GR' },
  { code: 'JTR', names: { en: 'Santorini' }, country: 'GR' },
  { code: 'JMK', names: { en: 'Mykonos' }, country: 'GR' },

  // Turkey
  { code: 'IST', names: { en: 'Istanbul' }, country: 'TR' },
  { code: 'SAW', names: { en: 'Istanbul Sabiha' }, country: 'TR' },
  { code: 'AYT', names: { en: 'Antalya' }, country: 'TR' },
  { code: 'ADB', names: { en: 'Izmir' }, country: 'TR' },
  { code: 'BJV', names: { en: 'Bodrum' }, country: 'TR' },
  { code: 'DLM', names: { en: 'Dalaman' }, country: 'TR' },

  // USA
  { code: 'JFK', names: { en: 'New York JFK', es: 'Nueva York', fr: 'New York', de: 'New York' }, country: 'US' },
  { code: 'EWR', names: { en: 'New York Newark' }, country: 'US' },
  { code: 'LGA', names: { en: 'New York LaGuardia' }, country: 'US' },
  { code: 'LAX', names: { en: 'Los Angeles' }, country: 'US' },
  { code: 'SFO', names: { en: 'San Francisco' }, country: 'US' },
  { code: 'ORD', names: { en: 'Chicago' }, country: 'US' },
  { code: 'MIA', names: { en: 'Miami' }, country: 'US' },
  { code: 'BOS', names: { en: 'Boston' }, country: 'US' },
  { code: 'ATL', names: { en: 'Atlanta' }, country: 'US' },
  { code: 'DFW', names: { en: 'Dallas' }, country: 'US' },
  { code: 'DEN', names: { en: 'Denver' }, country: 'US' },
  { code: 'SEA', names: { en: 'Seattle' }, country: 'US' },
  { code: 'LAS', names: { en: 'Las Vegas' }, country: 'US' },
  { code: 'PHX', names: { en: 'Phoenix' }, country: 'US' },
  { code: 'IAH', names: { en: 'Houston' }, country: 'US' },
  { code: 'MCO', names: { en: 'Orlando' }, country: 'US' },
  { code: 'FLL', names: { en: 'Fort Lauderdale' }, country: 'US' },
  { code: 'SAN', names: { en: 'San Diego' }, country: 'US' },
  { code: 'HNL', names: { en: 'Honolulu' }, country: 'US' },

  // Canada
  { code: 'YYZ', names: { en: 'Toronto' }, country: 'CA' },
  { code: 'YVR', names: { en: 'Vancouver' }, country: 'CA' },
  { code: 'YUL', names: { en: 'Montreal', fr: 'Montréal' }, country: 'CA' },
  { code: 'YYC', names: { en: 'Calgary' }, country: 'CA' },

  // UAE
  { code: 'DXB', names: { en: 'Dubai', de: 'Dubai', fr: 'Dubaï' }, country: 'AE' },
  { code: 'AUH', names: { en: 'Abu Dhabi' }, country: 'AE' },

  // Japan
  { code: 'NRT', names: { en: 'Tokyo Narita', de: 'Tokio', fr: 'Tokyo' }, country: 'JP' },
  { code: 'HND', names: { en: 'Tokyo Haneda' }, country: 'JP' },
  { code: 'KIX', names: { en: 'Osaka Kansai' }, country: 'JP' },

  // Thailand
  { code: 'BKK', names: { en: 'Bangkok' }, country: 'TH' },
  { code: 'HKT', names: { en: 'Phuket' }, country: 'TH' },

  // Singapore
  { code: 'SIN', names: { en: 'Singapore', de: 'Singapur', fr: 'Singapour' }, country: 'SG' },

  // Indonesia
  { code: 'DPS', names: { en: 'Bali Denpasar' }, country: 'ID' },
  { code: 'CGK', names: { en: 'Jakarta' }, country: 'ID' },

  // Australia
  { code: 'SYD', names: { en: 'Sydney' }, country: 'AU' },
  { code: 'MEL', names: { en: 'Melbourne' }, country: 'AU' },
  { code: 'BNE', names: { en: 'Brisbane' }, country: 'AU' },
  { code: 'PER', names: { en: 'Perth' }, country: 'AU' },

  // New Zealand
  { code: 'AKL', names: { en: 'Auckland' }, country: 'NZ' },

  // Austria
  { code: 'VIE', names: { en: 'Vienna', de: 'Wien', fr: 'Vienne', it: 'Vienna' }, country: 'AT' },
  { code: 'SZG', names: { en: 'Salzburg', de: 'Salzburg' }, country: 'AT' },
  { code: 'INN', names: { en: 'Innsbruck', de: 'Innsbruck' }, country: 'AT' },

  // Switzerland
  { code: 'ZRH', names: { en: 'Zurich', de: 'Zürich', fr: 'Zurich', it: 'Zurigo' }, country: 'CH' },
  { code: 'GVA', names: { en: 'Geneva', de: 'Genf', fr: 'Genève', it: 'Ginevra' }, country: 'CH' },
  { code: 'BSL', names: { en: 'Basel', de: 'Basel', fr: 'Bâle' }, country: 'CH' },

  // Belgium
  { code: 'BRU', names: { en: 'Brussels', de: 'Brüssel', fr: 'Bruxelles', nl: 'Brussel' }, country: 'BE' },
  { code: 'CRL', names: { en: 'Brussels Charleroi', fr: 'Charleroi' }, country: 'BE' },

  // Ireland
  { code: 'DUB', names: { en: 'Dublin' }, country: 'IE' },
  { code: 'SNN', names: { en: 'Shannon' }, country: 'IE' },
  { code: 'ORK', names: { en: 'Cork' }, country: 'IE' },

  // Czech Republic
  { code: 'PRG', names: { en: 'Prague', de: 'Prag', fr: 'Prague', it: 'Praga' }, country: 'CZ' },

  // Hungary
  { code: 'BUD', names: { en: 'Budapest', de: 'Budapest' }, country: 'HU' },

  // Denmark
  { code: 'CPH', names: { en: 'Copenhagen', de: 'Kopenhagen', sv: 'Köpenhamn', fr: 'Copenhague' }, country: 'DK' },

  // Norway
  { code: 'OSL', names: { en: 'Oslo' }, country: 'NO' },
  { code: 'BGO', names: { en: 'Bergen' }, country: 'NO' },

  // Finland
  { code: 'HEL', names: { en: 'Helsinki', sv: 'Helsingfors' }, country: 'FI' },

  // Russia
  { code: 'SVO', names: { en: 'Moscow Sheremetyevo', de: 'Moskau' }, country: 'RU' },
  { code: 'LED', names: { en: 'St Petersburg', de: 'Sankt Petersburg' }, country: 'RU' },

  // Romania
  { code: 'OTP', names: { en: 'Bucharest', de: 'Bukarest', fr: 'Bucarest' }, country: 'RO' },
  { code: 'CLJ', names: { en: 'Cluj-Napoca' }, country: 'RO' },

  // Bulgaria
  { code: 'SOF', names: { en: 'Sofia', de: 'Sofia' }, country: 'BG' },
  { code: 'VAR', names: { en: 'Varna' }, country: 'BG' },
  { code: 'BOJ', names: { en: 'Burgas' }, country: 'BG' },

  // Morocco
  { code: 'CMN', names: { en: 'Casablanca', fr: 'Casablanca' }, country: 'MA' },
  { code: 'RAK', names: { en: 'Marrakech', fr: 'Marrakech' }, country: 'MA' },

  // Egypt
  { code: 'CAI', names: { en: 'Cairo', de: 'Kairo', fr: 'Le Caire' }, country: 'EG' },
  { code: 'SSH', names: { en: 'Sharm El Sheikh' }, country: 'EG' },
  { code: 'HRG', names: { en: 'Hurghada' }, country: 'EG' },

  // South Africa
  { code: 'JNB', names: { en: 'Johannesburg' }, country: 'ZA' },
  { code: 'CPT', names: { en: 'Cape Town' }, country: 'ZA' },

  // Israel
  { code: 'TLV', names: { en: 'Tel Aviv' }, country: 'IL' },

  // India
  { code: 'DEL', names: { en: 'Delhi' }, country: 'IN' },
  { code: 'BOM', names: { en: 'Mumbai' }, country: 'IN' },
  { code: 'BLR', names: { en: 'Bangalore' }, country: 'IN' },
  { code: 'GOI', names: { en: 'Goa' }, country: 'IN' },

  // China
  { code: 'PEK', names: { en: 'Beijing', de: 'Peking', fr: 'Pékin' }, country: 'CN' },
  { code: 'PVG', names: { en: 'Shanghai Pudong' }, country: 'CN' },
  { code: 'HKG', names: { en: 'Hong Kong' }, country: 'HK' },

  // South Korea
  { code: 'ICN', names: { en: 'Seoul Incheon', de: 'Seoul', fr: 'Séoul' }, country: 'KR' },

  // Brazil
  { code: 'GRU', names: { en: 'Sao Paulo', pt: 'São Paulo', es: 'São Paulo' }, country: 'BR' },
  { code: 'GIG', names: { en: 'Rio de Janeiro', pt: 'Rio de Janeiro' }, country: 'BR' },

  // Argentina
  { code: 'EZE', names: { en: 'Buenos Aires' }, country: 'AR' },

  // Mexico
  { code: 'MEX', names: { en: 'Mexico City', es: 'Ciudad de México' }, country: 'MX' },
  { code: 'CUN', names: { en: 'Cancun', es: 'Cancún' }, country: 'MX' },

  // Malta
  { code: 'MLA', names: { en: 'Malta' }, country: 'MT' },

  // Cyprus
  { code: 'LCA', names: { en: 'Larnaca' }, country: 'CY' },
  { code: 'PFO', names: { en: 'Paphos' }, country: 'CY' },

  // Iceland
  { code: 'KEF', names: { en: 'Reykjavik Keflavik' }, country: 'IS' },

  // Serbia
  { code: 'BEG', names: { en: 'Belgrade', de: 'Belgrad', hr: 'Beograd' }, country: 'RS' },

  // Slovenia
  { code: 'LJU', names: { en: 'Ljubljana', de: 'Ljubljana' }, country: 'SI' },

  // Slovakia
  { code: 'BTS', names: { en: 'Bratislava', de: 'Bratislava' }, country: 'SK' },

  // Lithuania
  { code: 'VNO', names: { en: 'Vilnius', pl: 'Wilno', de: 'Wilna' }, country: 'LT' },
  { code: 'KUN', names: { en: 'Kaunas', pl: 'Kowno' }, country: 'LT' },

  // Latvia
  { code: 'RIX', names: { en: 'Riga', de: 'Riga', pl: 'Ryga' }, country: 'LV' },

  // Estonia
  { code: 'TLL', names: { en: 'Tallinn', de: 'Tallinn' }, country: 'EE' },

  // Ukraine
  { code: 'KBP', names: { en: 'Kyiv', pl: 'Kijów', de: 'Kiew' }, country: 'UA' },
  { code: 'LWO', names: { en: 'Lviv', pl: 'Lwów', de: 'Lemberg' }, country: 'UA' },

  // Montenegro
  { code: 'TGD', names: { en: 'Podgorica' }, country: 'ME' },
  { code: 'TIV', names: { en: 'Tivat' }, country: 'ME' },

  // North Macedonia
  { code: 'SKP', names: { en: 'Skopje' }, country: 'MK' },

  // Bosnia
  { code: 'SJJ', names: { en: 'Sarajevo', hr: 'Sarajevo' }, country: 'BA' },

  // Kosovo
  { code: 'PRN', names: { en: 'Pristina', sq: 'Prishtinë' }, country: 'XK' },

  // Luxembourg
  { code: 'LUX', names: { en: 'Luxembourg', de: 'Luxemburg', fr: 'Luxembourg' }, country: 'LU' },

  // Maldives
  { code: 'MLE', names: { en: 'Male Maldives' }, country: 'MV' },

  // Mauritius
  { code: 'MRU', names: { en: 'Mauritius' }, country: 'MU' },

  // Seychelles
  { code: 'SEZ', names: { en: 'Seychelles' }, country: 'SC' },
]

/**
 * Get the best name for an airport in the given locale
 */
export function getAirportName(airport: Airport, locale: string): string {
  return airport.names[locale] || airport.names.en
}

/**
 * Normalize string for matching (remove diacritics, lowercase)
 */
export function normalizeForSearch(str: string): string {
  return str
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '') // Remove diacritics
}

export interface LocationMatch {
  code: string
  name: string
  type: 'airport' | 'city'
  country: string
}

function dedupeAirports(airports: Airport[]): Airport[] {
  const byCode = new Map<string, Airport>()

  for (const airport of airports) {
    const existing = byCode.get(airport.code)
    if (!existing) {
      byCode.set(airport.code, airport)
      continue
    }

    byCode.set(airport.code, {
      ...existing,
      names: {
        ...airport.names,
        ...existing.names,
      },
    })
  }

  return Array.from(byCode.values())
}

function dedupeGeneratedLocations(entries: GeneratedLocationEntry[]): GeneratedLocationEntry[] {
  const byCode = new Map<string, GeneratedLocationEntry>()

  for (const entry of entries) {
    const existing = byCode.get(entry.code)
    if (!existing) {
      byCode.set(entry.code, entry)
      continue
    }

    byCode.set(entry.code, {
      ...existing,
      ...entry,
      type: existing.type === 'city' ? existing.type : entry.type,
      name: existing.name || entry.name,
      country: existing.country || entry.country,
      city: existing.city || entry.city,
      airports: existing.airports || entry.airports,
      aliases: Array.from(new Set([...(existing.aliases || []), ...(entry.aliases || [])])),
    })
  }

  return Array.from(byCode.values())
}

function toGeneratedAirport(entry: GeneratedLocationEntry): Airport {
  const normalizedName = normalizeForSearch(entry.name)
  const normalizedCity = entry.city ? normalizeForSearch(entry.city) : ''
  const label = entry.city && normalizedCity && !normalizedName.startsWith(normalizedCity)
    ? `${entry.city} ${entry.name}`
    : entry.name

  return {
    code: entry.code,
    names: { en: label },
    country: entry.country,
  }
}

const EXTRA_LOCATION_ALIASES: Record<string, string[]> = {
  BDL: ['hartford', 'hartford connecticut', 'connecticut'],
}

const ALL_GENERATED_LOCATIONS = dedupeGeneratedLocations([
  ...GENERATED_LOCATIONS,
  ...GENERATED_AIRPORT_SUPPLEMENT,
])

const GENERATED_LOCATION_BY_CODE = new Map(
  ALL_GENERATED_LOCATIONS.map((entry) => [entry.code, entry]),
)

const ALL_AIRPORTS = dedupeAirports([
  ...AIRPORTS,
  ...ALL_GENERATED_LOCATIONS
    .filter((entry) => entry.type === 'airport')
    .map((entry) => toGeneratedAirport(entry)),
])

function getLocationAliases(entry: GeneratedLocationEntry): string[] {
  const extraAliases = EXTRA_LOCATION_ALIASES[entry.code] || []
  return Array.from(
    new Set([...entry.aliases, ...extraAliases].map((alias) => normalizeForSearch(alias)).filter(Boolean)),
  )
}

function getAirportSearchTerms(airport: Airport): string[] {
  const localizedNames = Object.values(airport.names)
    .map((name) => normalizeForSearch(name))
    .filter(Boolean)

  const generatedEntry = GENERATED_LOCATION_BY_CODE.get(airport.code)
  const generatedAliases = generatedEntry?.type === 'airport' ? getLocationAliases(generatedEntry) : []

  return Array.from(new Set([
    airport.code.toLowerCase(),
    ...localizedNames,
    ...generatedAliases,
  ]))
}

function toLocationMatch(entry: GeneratedLocationEntry): LocationMatch {
  return {
    code: entry.code,
    name: entry.name,
    type: entry.type,
    country: entry.country,
  }
}

export function findExactLocationMatch(query: string): LocationMatch | null {
  if (!query || query.length < 2) return null

  const normalizedQuery = normalizeForSearch(query)
  const exactCode = GENERATED_LOCATION_BY_CODE.get(normalizedQuery.toUpperCase())
  if (exactCode) return toLocationMatch(exactCode)

  const exactAliasMatches = ALL_GENERATED_LOCATIONS.filter((entry) => getLocationAliases(entry).includes(normalizedQuery))
  if (exactAliasMatches.length > 0) {
    const cityMatch = exactAliasMatches.find((entry) => entry.type === 'city')
    return toLocationMatch(cityMatch || exactAliasMatches[0])
  }

  return null
}

function scoreLocationEntry(entry: GeneratedLocationEntry, normalizedQuery: string): number {
  let score = 0

  for (const alias of getLocationAliases(entry)) {
    if (alias === normalizedQuery) {
      score = Math.max(score, entry.type === 'city' ? 1000 : 900)
      continue
    }

    if (alias.startsWith(normalizedQuery)) {
      score = Math.max(score, entry.type === 'city' ? 650 : 600)
      continue
    }

    if (normalizedQuery.includes(alias) && alias.length >= 3) {
      score = Math.max(score, entry.type === 'airport' ? 575 : 550)
      continue
    }

    if (alias.includes(normalizedQuery)) {
      score = Math.max(score, entry.type === 'airport' ? 425 : 400)
    }
  }

  return score
}

export function findBestLocationMatch(query: string): LocationMatch | null {
  if (!query || query.length < 2) return null

  const exactMatch = findExactLocationMatch(query)
  if (exactMatch) return exactMatch

  const normalizedQuery = normalizeForSearch(query)

  let bestMatch: { entry: GeneratedLocationEntry; score: number } | null = null
  for (const entry of ALL_GENERATED_LOCATIONS) {
    const score = scoreLocationEntry(entry, normalizedQuery)
    if (score <= 0) continue

    if (
      !bestMatch ||
      score > bestMatch.score ||
      (score === bestMatch.score && entry.type === 'city' && bestMatch.entry.type !== 'city')
    ) {
      bestMatch = { entry, score }
    }
  }

  return bestMatch ? toLocationMatch(bestMatch.entry) : null
}

/**
 * Find airports matching a query string
 */
export function searchAirports(query: string, locale: string, limit = 10): Airport[] {
  if (!query || query.length < 2) return []
  
  const normalizedQuery = normalizeForSearch(query)
  
  // Score airports by match quality
  const scored = ALL_AIRPORTS.map(airport => {
    const name = getAirportName(airport, locale)
    const searchTerms = getAirportSearchTerms(airport)
    const codeMatch = airport.code.toLowerCase() === normalizedQuery
    const codeStartsWith = airport.code.toLowerCase().startsWith(normalizedQuery)
    const nameStartsWith = searchTerms.some(term => term.startsWith(normalizedQuery))
    const nameContains = searchTerms.some(term => term.includes(normalizedQuery))
    
    let score = 0
    if (codeMatch) score += 120
    if (codeStartsWith) score += 100
    if (nameStartsWith) score += 50
    if (nameContains) score += 10
    
    return { airport, score, name }
  })
  .filter(({ score }) => score > 0)
  .sort((a, b) => b.score - a.score)
  .slice(0, limit)
  
  return scored.map(({ airport }) => airport)
}

/**
 * Find the best single airport match for autocomplete
 */
export function findBestMatch(query: string, locale: string): Airport | null {
  if (!query || query.length < 2) return null
  
  const normalizedQuery = normalizeForSearch(query)
  
  // First try exact code match
  const codeMatch = ALL_AIRPORTS.find(a => a.code.toLowerCase() === normalizedQuery)
  if (codeMatch) return codeMatch

  for (const airport of ALL_AIRPORTS) {
    const searchTerms = getAirportSearchTerms(airport)
    if (searchTerms.some(term => term === normalizedQuery)) {
      return airport
    }
  }
  
  // Then try name prefix match
  for (const airport of ALL_AIRPORTS) {
    const searchTerms = getAirportSearchTerms(airport)
    if (searchTerms.some(term => term.startsWith(normalizedQuery))) {
      return airport
    }
  }
  
  // Then try code prefix
  for (const airport of ALL_AIRPORTS) {
    if (airport.code.toLowerCase().startsWith(normalizedQuery)) {
      return airport
    }
  }
  
  return null
}
