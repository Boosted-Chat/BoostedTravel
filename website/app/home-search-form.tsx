'use client'

import { FormEvent, useState, useRef, useEffect, KeyboardEvent } from 'react'
import { useRouter, useParams } from 'next/navigation'
import { useTranslations } from 'next-intl'
import { findBestMatch, getAirportName, normalizeForSearch, AIRPORTS, Airport } from './airports'

const DESTINATION_KEYS = [
  { key: 'barcelona', code: 'BCN', flag: '/flags/es.svg', img: '/destinations/barcelona.jpg' },
  { key: 'tokyo',     code: 'NRT', flag: '/flags/jp.svg', img: '/destinations/tokyo.jpg' },
  { key: 'newYork',   code: 'JFK', flag: '/flags/us.svg', img: '/destinations/newyork.jpg' },
  { key: 'paris',     code: 'CDG', flag: '/flags/fr.svg', img: '/destinations/paris.jpg' },
  { key: 'bali',      code: 'DPS', flag: '/flags/id.svg', img: '/destinations/bali.jpg' },
  { key: 'dubai',     code: 'DXB', flag: '/flags/ae.svg', img: '/destinations/dubai.jpg' },
] as const

// "to" keyword in various languages
const TO_KEYWORDS: Record<string, string[]> = {
  en: ['to'],
  pl: ['do'],
  de: ['nach'],
  es: ['a', 'hacia'],
  fr: ['vers', 'à'],
  it: ['a', 'verso'],
  pt: ['para', 'a'],
  nl: ['naar'],
  sv: ['till'],
  hr: ['u', 'za'],
  sq: ['në', 'drejt'],
}

// Return keywords
const RETURN_KEYWORDS: Record<string, string[]> = {
  en: ['returning', 'return', 'back'],
  pl: ['powrót', 'wracając'],
  de: ['zurück', 'rückkehr'],
  es: ['regreso', 'volviendo'],
  fr: ['retour'],
  it: ['ritorno'],
  pt: ['volta', 'retorno'],
  nl: ['terug', 'retour'],
  sv: ['tillbaka', 'retur'],
  hr: ['povratak'],
  sq: ['kthim'],
}

// Direct flight keywords
const DIRECT_KEYWORDS: Record<string, string[]> = {
  en: ['direct', 'nonstop', 'non-stop'],
  pl: ['bezpośredni', 'bezpośrednio'],
  de: ['direkt', 'nonstop'],
  es: ['directo', 'sin escalas'],
  fr: ['direct', 'sans escale'],
  it: ['diretto', 'senza scali'],
  pt: ['direto', 'sem escalas'],
  nl: ['direct', 'rechtstreeks'],
  sv: ['direkt'],
  hr: ['direktno', 'izravno'],
  sq: ['direkt'],
}

// Class keywords
const CLASS_KEYWORDS: Record<string, string[]> = {
  en: ['business', 'economy', 'first class', 'premium'],
  pl: ['biznes', 'ekonomiczna', 'pierwsza klasa'],
  de: ['business', 'economy', 'erste klasse'],
  es: ['business', 'económica', 'primera clase'],
  fr: ['affaires', 'économique', 'première classe'],
  it: ['business', 'economica', 'prima classe'],
  pt: ['executiva', 'econômica', 'primeira classe'],
  nl: ['business', 'economy', 'eerste klas'],
  sv: ['business', 'ekonomi', 'första klass'],
  hr: ['poslovna', 'ekonomska', 'prva klasa'],
  sq: ['biznes', 'ekonomike', 'klasa e parë'],
}

// Time filter keywords
const TIME_KEYWORDS: Record<string, string[]> = {
  en: ['morning', 'afternoon', 'evening', 'departing', 'leaving'],
  pl: ['rano', 'popołudniu', 'wieczorem', 'wylot'],
  de: ['morgens', 'nachmittags', 'abends', 'abflug'],
  es: ['mañana', 'tarde', 'noche', 'salida'],
  fr: ['matin', 'après-midi', 'soir', 'départ'],
  it: ['mattina', 'pomeriggio', 'sera', 'partenza'],
  pt: ['manhã', 'tarde', 'noite', 'partida'],
  nl: ['ochtend', 'middag', 'avond', 'vertrek'],
  sv: ['morgon', 'eftermiddag', 'kväll', 'avgång'],
  hr: ['ujutro', 'popodne', 'navečer', 'polazak'],
  sq: ['mëngjes', 'pasdite', 'mbrëmje', 'nisje'],
}

// Month names by locale
const MONTH_NAMES: Record<string, string[]> = {
  en: ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'],
  pl: ['stycznia', 'lutego', 'marca', 'kwietnia', 'maja', 'czerwca', 'lipca', 'sierpnia', 'września', 'października', 'listopada', 'grudnia'],
  de: ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember'],
  es: ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'],
  fr: ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre'],
  it: ['gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno', 'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre'],
  pt: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'],
  nl: ['januari', 'februari', 'maart', 'april', 'mei', 'juni', 'juli', 'augustus', 'september', 'oktober', 'november', 'december'],
  sv: ['januari', 'februari', 'mars', 'april', 'maj', 'juni', 'juli', 'augusti', 'september', 'oktober', 'november', 'december'],
  hr: ['siječnja', 'veljače', 'ožujka', 'travnja', 'svibnja', 'lipnja', 'srpnja', 'kolovoza', 'rujna', 'listopada', 'studenoga', 'prosinca'],
  sq: ['janar', 'shkurt', 'mars', 'prill', 'maj', 'qershor', 'korrik', 'gusht', 'shtator', 'tetor', 'nëntor', 'dhjetor'],
}

// Ordinal suffixes for English
function getOrdinal(n: number): string {
  const s = ['th', 'st', 'nd', 'rd']
  const v = n % 100
  return n + (s[(v - 20) % 10] || s[v] || s[0])
}

// Generate a dynamic date suggestion based on current date
function generateDateSuggestion(locale: string, isReturn: boolean = false): string {
  const now = new Date()
  // Random offset: 7-60 days for outbound, +3-14 days for return
  const baseOffset = isReturn ? 3 : 7
  const randomOffset = Math.floor(Math.random() * (isReturn ? 12 : 54)) + baseOffset
  const targetDate = new Date(now.getTime() + randomOffset * 24 * 60 * 60 * 1000)
  
  const day = targetDate.getDate()
  const month = targetDate.getMonth()
  const months = MONTH_NAMES[locale] || MONTH_NAMES.en
  const monthName = months[month]
  
  // Format varies by locale
  switch (locale) {
    case 'en':
      return `on ${monthName} ${getOrdinal(day)}`
    case 'pl':
      return `${day} ${monthName}`
    case 'de':
      return `am ${day}. ${monthName}`
    case 'es':
      return `el ${day} de ${monthName}`
    case 'fr':
      return `le ${day} ${monthName}`
    case 'it':
      return `il ${day} ${monthName}`
    case 'pt':
      return `${day} de ${monthName}`
    case 'nl':
      return `op ${day} ${monthName}`
    case 'sv':
      return `den ${day} ${monthName}`
    case 'hr':
      return `${day}. ${monthName}`
    case 'sq':
      return `më ${day} ${monthName}`
    default:
      return `on ${monthName} ${getOrdinal(day)}`
  }
}

// Generate return date suggestion
function generateReturnSuggestion(locale: string): string {
  const returnWord = (RETURN_KEYWORDS[locale] || RETURN_KEYWORDS.en)[0]
  const dateSuggestion = generateDateSuggestion(locale, true)
  return `, ${returnWord} ${dateSuggestion}`
}

// Generate direct flight suggestion
function generateDirectSuggestion(locale: string): string {
  const directWord = (DIRECT_KEYWORDS[locale] || DIRECT_KEYWORDS.en)[0]
  return `, ${directWord}`
}

// Generate class suggestion
function generateClassSuggestion(locale: string): string {
  const classes = CLASS_KEYWORDS[locale] || CLASS_KEYWORDS.en
  // Randomly pick business or economy
  const classWord = Math.random() > 0.5 ? classes[0] : classes[1]
  return `, ${classWord}`
}

// Generate time filter suggestion
function generateTimeSuggestion(locale: string): string {
  const times = TIME_KEYWORDS[locale] || TIME_KEYWORDS.en
  const timeOptions: Record<string, string> = {
    en: ['morning departure', 'afternoon flight', 'evening departure', 'departing after 2pm', 'leaving before noon'][Math.floor(Math.random() * 5)],
    pl: ['wylot rano', 'lot popołudniowy', 'wylot wieczorem', 'wylot po 14:00'][Math.floor(Math.random() * 4)],
    de: ['morgens abflug', 'nachmittags', 'abends abflug', 'abflug nach 14 Uhr'][Math.floor(Math.random() * 4)],
    es: ['salida por la mañana', 'vuelo de tarde', 'salida por la noche'][Math.floor(Math.random() * 3)],
    fr: ['départ le matin', 'vol l\'après-midi', 'départ le soir'][Math.floor(Math.random() * 3)],
    it: ['partenza di mattina', 'volo pomeridiano', 'partenza di sera'][Math.floor(Math.random() * 3)],
    pt: ['partida de manhã', 'voo à tarde', 'partida à noite'][Math.floor(Math.random() * 3)],
    nl: ['ochtend vertrek', 'middag vlucht', 'avond vertrek'][Math.floor(Math.random() * 3)],
    sv: ['avgång på morgonen', 'eftermiddagsflyg', 'kvällsavgång'][Math.floor(Math.random() * 3)],
    hr: ['polazak ujutro', 'popodnevni let', 'večernji polazak'][Math.floor(Math.random() * 3)],
    sq: ['nisje në mëngjes', 'fluturim pasdite', 'nisje në mbrëmje'][Math.floor(Math.random() * 3)],
  }
  return `, ${timeOptions[locale] || timeOptions.en}`
}

function PlaneIcon() {
  // Font Awesome 6 Free Solid — fa-plane-departure (CC BY 4.0)
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 576 512" aria-hidden="true" className="lp-sf-icon" fill="currentColor">
      <path d="M372 143.9L172.7 40.2c-8-4.1-17.3-4.8-25.7-1.7l-41.1 15c-10.3 3.7-13.8 16.4-7.1 25L200.3 206.4 100.1 242.8 40 206.2c-6.2-3.8-13.8-4.5-20.7-2.1L3 210.1c-9.4 3.4-13.4 14.5-8.3 23.1l53.6 91.8c15.6 26.7 48.1 38.4 77.1 27.8l12.9-4.7 0 0 398.4-145c29.1-10.6 44-42.7 33.5-71.8s-42.7-44-71.8-33.5L372 143.9zM32.2 448c-17.7 0-32 14.3-32 32s14.3 32 32 32l512 0c17.7 0 32-14.3 32-32s-14.3-32-32-32l-512 0z"/>
    </svg>
  )
}

function SearchIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true" className="lp-sf-icon" fill="none">
      <circle cx="11" cy="11" r="6.5" stroke="currentColor" strokeWidth="2.2" />
      <path d="M16 16l4 4" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" />
    </svg>
  )
}

interface ParsedQuery {
  origin: string | null
  originMatch: Airport | null
  toKeyword: string | null
  destination: string | null
  destMatch: Airport | null
  hasOutboundDate: boolean
  hasReturnKeyword: boolean
  hasReturnDate: boolean
  hasDirectKeyword: boolean
  hasClassKeyword: boolean
  hasTimeKeyword: boolean
  remainder: string
}

function parseQuery(query: string, locale: string): ParsedQuery {
  const toWords = TO_KEYWORDS[locale] || TO_KEYWORDS.en
  const returnWords = RETURN_KEYWORDS[locale] || RETURN_KEYWORDS.en
  const directWords = DIRECT_KEYWORDS[locale] || DIRECT_KEYWORDS.en
  const classWords = CLASS_KEYWORDS[locale] || CLASS_KEYWORDS.en
  const timeWords = TIME_KEYWORDS[locale] || TIME_KEYWORDS.en
  
  const lowerQuery = query.toLowerCase()
  const words = query.split(/\s+/)
  
  let origin: string | null = null
  let toKeyword: string | null = null
  let destination: string | null = null
  let toIndex = -1
  
  // Find the "to" keyword
  for (let i = 0; i < words.length; i++) {
    const word = words[i].toLowerCase()
    if (toWords.includes(word)) {
      toKeyword = words[i]
      toIndex = i
      break
    }
  }
  
  if (toIndex > 0) {
    origin = words.slice(0, toIndex).join(' ')
    if (toIndex < words.length - 1) {
      destination = words.slice(toIndex + 1).join(' ')
    }
  } else if (toIndex === -1 && words.length > 0) {
    origin = words.join(' ')
  }
  
  // Check for date-like patterns (multiple dates possible)
  const dateMatches = query.match(/\d{1,2}[\s./\-]|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|sty|lut|kwi|maj|cze|lip|sie|wrz|paz|lis|gru|janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre|januar|februar|märz|april|juni|juli|august|september|oktober|november|dezember|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre|gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre|januari|februari|maart|mei|augustus|januari|februari|mars|maj|juni|juli|augusti|oktober|studenoga|prosinca|siječnja|veljače|ožujka|travnja|svibnja|lipnja|srpnja|kolovoza|rujna|listopada)/gi)
  const dateCount = dateMatches ? dateMatches.length : 0
  
  // Check for return keyword
  const hasReturnKeyword = returnWords.some(w => lowerQuery.includes(w.toLowerCase()))
  
  // Check for direct keyword
  const hasDirectKeyword = directWords.some(w => lowerQuery.includes(w.toLowerCase()))
  
  // Check for class keyword
  const hasClassKeyword = classWords.some(w => lowerQuery.includes(w.toLowerCase()))
  
  // Check for time keyword
  const hasTimeKeyword = timeWords.some(w => lowerQuery.includes(w.toLowerCase())) || 
                         /\b\d{1,2}(:|h)\d{0,2}\s*(am|pm)?\b/i.test(query) ||
                         /\b(after|before|between)\s+\d/i.test(query)
  
  const originMatch = origin ? findBestMatch(origin, locale) : null
  const destMatch = destination ? findBestMatch(destination, locale) : null
  
  return {
    origin,
    originMatch,
    toKeyword,
    destination,
    destMatch,
    hasOutboundDate: dateCount >= 1,
    hasReturnKeyword,
    hasReturnDate: dateCount >= 2 || (hasReturnKeyword && dateCount >= 1),
    hasDirectKeyword,
    hasClassKeyword,
    hasTimeKeyword,
    remainder: query,
  }
}

function getSuggestion(query: string, locale: string): string {
  if (!query || query.length < 2) return ''
  
  const parsed = parseQuery(query, locale)
  const toWord = (TO_KEYWORDS[locale] || TO_KEYWORDS.en)[0]
  
  // Helper to get airport name completion
  const getNameCompletion = (input: string, airport: Airport): string => {
    const fullName = getAirportName(airport, locale)
    const normalizedInput = normalizeForSearch(input)
    const normalizedFull = normalizeForSearch(fullName)
    
    if (!normalizedFull.startsWith(normalizedInput) || normalizedInput.length >= normalizedFull.length) {
      return ''
    }
    
    let completionStart = input.length
    let matchedSoFar = 0
    for (let i = 0; i < fullName.length && matchedSoFar < input.length; i++) {
      const fullChar = normalizeForSearch(fullName[i])
      const inputChar = normalizeForSearch(input[matchedSoFar])
      if (fullChar === inputChar) {
        matchedSoFar++
        completionStart = i + 1
      }
    }
    
    return fullName.slice(completionStart)
  }
  
  // Helper to get trailing partial word from query (after last space/comma)
  const getTrailingPartial = (): string => {
    const trimmed = query.trimEnd()
    const lastSep = Math.max(trimmed.lastIndexOf(' '), trimmed.lastIndexOf(','))
    if (lastSep === -1) return trimmed.toLowerCase()
    return trimmed.slice(lastSep + 1).trim().toLowerCase()
  }
  
  // Helper to find keyword completion - checks if partial is prefix of any keyword
  const getKeywordCompletion = (partial: string, keywords: string[]): string | null => {
    if (!partial || partial.length < 2) return null
    const lowerPartial = partial.toLowerCase()
    for (const kw of keywords) {
      if (kw.toLowerCase().startsWith(lowerPartial) && kw.toLowerCase() !== lowerPartial) {
        return kw.slice(partial.length)
      }
    }
    return null
  }
  
  // Stage 1: Just typing origin (no "to" yet)
  if (!parsed.toKeyword && parsed.origin) {
    const match = findBestMatch(parsed.origin, locale)
    if (match) {
      const completion = getNameCompletion(parsed.origin, match)
      if (completion) {
        return completion + ' ' + toWord + ' ...'
      }
    }
    return ''
  }
  
  // Stage 2: Has "to" but no destination yet
  if (parsed.toKeyword && !parsed.destination) {
    return ' ...'
  }
  
  // Stage 3+: Has "to" and destination (possibly partial)
  if (parsed.toKeyword && parsed.destination) {
    const match = findBestMatch(parsed.destination, locale)
    
    // Still typing destination
    if (match) {
      const completion = getNameCompletion(parsed.destination, match)
      if (completion) {
        // Suggest rest of destination + outbound date
        return completion + ' ' + generateDateSuggestion(locale)
      }
    }
    
    // Stage 4: Need outbound date
    if (!parsed.hasOutboundDate) {
      return ' ' + generateDateSuggestion(locale)
    }
    
    // Stage 5: Have outbound date, suggest return (or complete partial return keyword)
    if (parsed.hasOutboundDate && !parsed.hasReturnKeyword && !parsed.hasReturnDate) {
      const trailing = getTrailingPartial()
      const returnKeywords = RETURN_KEYWORDS[locale] || RETURN_KEYWORDS.en
      const completion = getKeywordCompletion(trailing, returnKeywords)
      if (completion !== null) {
        // User is typing return keyword - complete it + add date
        const returnDate = generateReturnSuggestion(locale)
        // returnDate is like ", returning on May 5th" - extract just the date part
        const dateMatch = returnDate.match(/\d+/)
        if (dateMatch) {
          const months = MONTH_NAMES[locale] || MONTH_NAMES.en
          const futureDate = new Date()
          futureDate.setDate(futureDate.getDate() + 7 + Math.floor(Math.random() * 54) + 3 + Math.floor(Math.random() * 12))
          const month = months[futureDate.getMonth()]
          const day = futureDate.getDate()
          return completion + ' ' + month + ' ' + day
        }
        return completion
      }
      // Check if query ends with comma or space (ready for return keyword)
      const endsWithSep = query.endsWith(',') || query.endsWith(', ') || query.endsWith(' ')
      if (endsWithSep) {
        // Suggest full return phrase but without leading comma/space
        const returnKw = returnKeywords[0]
        const months = MONTH_NAMES[locale] || MONTH_NAMES.en
        const futureDate = new Date()
        futureDate.setDate(futureDate.getDate() + 7 + Math.floor(Math.random() * 54) + 3 + Math.floor(Math.random() * 12))
        const month = months[futureDate.getMonth()]
        const day = futureDate.getDate()
        if (query.endsWith(', ') || query.endsWith(' ')) {
          return returnKw + ' ' + month + ' ' + day
        }
        return ' ' + returnKw + ' ' + month + ' ' + day
      }
      return generateReturnSuggestion(locale)
    }
    
    // Stage 6: Have return, suggest direct (or complete partial direct keyword)
    if ((parsed.hasReturnDate || parsed.hasReturnKeyword) && !parsed.hasDirectKeyword) {
      const trailing = getTrailingPartial()
      const directKeywords = DIRECT_KEYWORDS[locale] || DIRECT_KEYWORDS.en
      const completion = getKeywordCompletion(trailing, directKeywords)
      if (completion !== null) {
        return completion
      }
      const endsWithSep = query.endsWith(',') || query.endsWith(', ') || query.endsWith(' ')
      if (endsWithSep) {
        const directKw = directKeywords[0]
        if (query.endsWith(', ') || query.endsWith(' ')) {
          return directKw
        }
        return ' ' + directKw
      }
      return generateDirectSuggestion(locale)
    }
    
    // Stage 7: Have direct, suggest class (or complete partial class keyword)
    if (parsed.hasDirectKeyword && !parsed.hasClassKeyword) {
      const trailing = getTrailingPartial()
      const classKeywords = CLASS_KEYWORDS[locale] || CLASS_KEYWORDS.en
      const completion = getKeywordCompletion(trailing, classKeywords)
      if (completion !== null) {
        return completion
      }
      const endsWithSep = query.endsWith(',') || query.endsWith(', ') || query.endsWith(' ')
      if (endsWithSep) {
        const classKw = classKeywords[Math.floor(Math.random() * classKeywords.length)]
        if (query.endsWith(', ') || query.endsWith(' ')) {
          return classKw
        }
        return ' ' + classKw
      }
      return generateClassSuggestion(locale)
    }
    
    // Stage 8: Have class, suggest time (or complete partial time keyword)
    if (parsed.hasClassKeyword && !parsed.hasTimeKeyword) {
      const trailing = getTrailingPartial()
      const timeKeywords = TIME_KEYWORDS[locale] || TIME_KEYWORDS.en
      const completion = getKeywordCompletion(trailing, timeKeywords)
      if (completion !== null) {
        return completion
      }
      const endsWithSep = query.endsWith(',') || query.endsWith(', ') || query.endsWith(' ')
      if (endsWithSep) {
        const timeKw = timeKeywords[Math.floor(Math.random() * timeKeywords.length)]
        if (query.endsWith(', ') || query.endsWith(' ')) {
          return timeKw
        }
        return ' ' + timeKw
      }
      return generateTimeSuggestion(locale)
    }
  }
  
  return ''
}

// Set to true to skip the API call and go straight to the loading UI demo
const DEMO_LOADING = false

interface HomeSearchFormProps {
  initialQuery?: string
  compact?: boolean
  autoFocus?: boolean
  probeMode?: boolean
}

export default function HomeSearchForm({
  initialQuery = '',
  compact = false,
  autoFocus = true,
  probeMode = false,
}: HomeSearchFormProps = {}) {
  const router = useRouter()
  const params = useParams()
  const locale = (params?.locale as string) || 'en'
  const td = useTranslations('destinations')
  const th = useTranslations('hero')
  const [query, setQuery] = useState(initialQuery)
  const [suggestion, setSuggestion] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const rowRef = useRef<HTMLDivElement>(null)

  const DESTINATIONS = DESTINATION_KEYS.map((d) => ({
    ...d,
    city: td(d.key),
    query: td(`${d.key}_query`),
  }))

  // Drag-to-scroll (no pointer capture so child button clicks still fire)
  useEffect(() => {
    const el = rowRef.current
    if (!el) return
    let isDown = false, startX = 0, scrollLeft = 0, totalDrag = 0
    const onMouseDown = (e: MouseEvent) => {
      isDown = true
      totalDrag = 0
      startX = e.pageX
      scrollLeft = el.scrollLeft
      el.style.cursor = 'grabbing'
    }
    const onMouseUp = () => {
      isDown = false
      el.style.cursor = ''
    }
    const onMouseMove = (e: MouseEvent) => {
      if (!isDown) return
      const dx = e.pageX - startX
      // Track peak displacement from origin (not accumulated), so small wobbles don't block clicks
      totalDrag = Math.max(totalDrag, Math.abs(dx))
      el.scrollLeft = scrollLeft - dx
    }
    const onClick = (e: MouseEvent) => {
      // Block click if user dragged more than 5px from the start position
      if (totalDrag > 5) e.stopPropagation()
    }
    el.addEventListener('mousedown', onMouseDown)
    window.addEventListener('mouseup', onMouseUp)
    window.addEventListener('mousemove', onMouseMove)
    el.addEventListener('click', onClick, true)
    return () => {
      el.removeEventListener('mousedown', onMouseDown)
      window.removeEventListener('mouseup', onMouseUp)
      window.removeEventListener('mousemove', onMouseMove)
      el.removeEventListener('click', onClick, true)
    }
  }, [])

  useEffect(() => {
    setQuery(initialQuery)
  }, [initialQuery])

  const handleSearch = (event: FormEvent) => {
    if (!query.trim()) {
      event.preventDefault()
      return
    }
    if (DEMO_LOADING) {
      event.preventDefault()
      setIsLoading(true)
      router.push(`/results/demo-loading${probeMode ? '?probe=1' : ''}`)
      return
    }
  }

  // Update suggestion when query changes
  useEffect(() => {
    const newSuggestion = getSuggestion(query, locale)
    setSuggestion(newSuggestion)
  }, [query, locale])

  // Handle Tab to accept suggestion
  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Tab' && suggestion && !e.shiftKey) {
      e.preventDefault()
      // Accept the suggestion
      setQuery(query + suggestion)
      setSuggestion('')
    }
  }

  return (
    <div className={`lp-sf-wrap${compact ? ' lp-sf-wrap--compact' : ''}`}>
      {!compact && (
        <div className="lp-sf-disclosure" aria-hidden="false">
          <span className="lp-sf-legal" tabIndex={0} role="note">
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden="true">
              <circle cx="7" cy="7" r="6" stroke="currentColor" strokeWidth="1.4" />
              <path d="M7 6v4M7 4.5v.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
            </svg>
            <span className="lp-sf-legal-tip" role="tooltip">
              By searching, you authorise AI agents to act on your behalf — they connect to airline
              websites and search for flights as you instructed. You are the one directing these
              agents. LetsFG provides the automation; you are responsible for the searches you initiate.
            </span>
          </span>
        </div>
      )}

      <form action="/results" method="get" onSubmit={handleSearch} className="lp-sf-form">
        {probeMode && <input type="hidden" name="probe" value="1" />}
        <div className="lp-sf-frame">
          <div className="lp-sf-input-wrap">
            <span className="lp-sf-leading" aria-hidden="true">
              <SearchIcon />
            </span>
            <input
              ref={inputRef}
              id="trip-query"
              name="q"
              type="text"
              className="lp-sf-input"
              placeholder={th('placeholder')}
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              onKeyDown={handleKeyDown}
              disabled={DEMO_LOADING && isLoading}
              autoFocus={autoFocus}
              autoComplete="off"
              spellCheck={false}
            />
            {suggestion && (
              <span className="lp-sf-ghost" aria-hidden="true">
                <span className="lp-sf-ghost-hidden">{query}</span>
                <span className="lp-sf-ghost-suggestion">{suggestion}</span>
              </span>
            )}
          </div>
          <button
            type="submit"
            className="lp-sf-button"
            disabled={(DEMO_LOADING && isLoading) || !query.trim()}
            aria-label={isLoading ? 'Searching flights' : 'Search flights'}
          >
            <PlaneIcon />
          </button>
        </div>
      </form>

      {!compact && (
        <div className="lp-dest-row" aria-label="Popular destinations">
          <div className="lp-dest-track" ref={rowRef}>
            {DESTINATIONS.map((dest) => (
              <button
                key={dest.code}
                type="button"
                className="lp-dest-card"
                onClick={() => {
                  setQuery(dest.query)
                  setTimeout(() => {
                    const input = inputRef.current
                    if (input) {
                      input.focus()
                      input.setSelectionRange(dest.query.length, dest.query.length)
                    }
                  }, 0)
                }}
                onMouseMove={(e) => {
                  const r = e.currentTarget.getBoundingClientRect()
                  const x = ((e.clientX - r.left) / r.width - 0.5) * 7
                  const y = ((e.clientY - r.top) / r.height - 0.5) * 5
                  e.currentTarget.style.setProperty('--mx', `${x}px`)
                  e.currentTarget.style.setProperty('--my', `${y}px`)
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.setProperty('--mx', '0px')
                  e.currentTarget.style.setProperty('--my', '0px')
                }}
              >
                <img src={dest.img} alt={dest.city} className="lp-dest-img" draggable={false} />
                <div className="lp-dest-overlay" />
                <img src={dest.flag} alt="" className="lp-dest-flag" draggable={false} />
                <span className="lp-dest-city">{dest.city}</span>
                <span className="lp-dest-code">{dest.code}</span>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}