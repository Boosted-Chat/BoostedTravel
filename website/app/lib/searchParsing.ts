// Shared NL query parser — used by /api/search and the SSR /results?q= page
// Handles: EN, DE, ES, FR, IT, NL, PL, PT, SQ (Albanian), HR (Croatian), SV (Swedish)
// Also handles: filler words, typos via accent-stripping, ordinals, DD/MM/YYYY, relative dates

import { findBestLocationMatch, findExactLocationMatch } from '../airports'

// ── City → IATA lookup ────────────────────────────────────────────────────────
// Keys are lowercase, accent-free. resolveCity() normalises input before lookup.

export const CITY_TO_IATA: Record<string, { code: string; name: string }> = {
  // ── UK & Ireland ────────────────────────────────────────────────────────────
  'london': { code: 'LON', name: 'London' },
  'heathrow': { code: 'LHR', name: 'London Heathrow' },
  'gatwick': { code: 'LGW', name: 'London Gatwick' },
  'stansted': { code: 'STN', name: 'London Stansted' },
  'luton': { code: 'LTN', name: 'London Luton' },
  'city airport': { code: 'LCY', name: 'London City' },
  'lcy': { code: 'LCY', name: 'London City' },
  'manchester': { code: 'MAN', name: 'Manchester' },
  'birmingham': { code: 'BHX', name: 'Birmingham' },
  'edinburgh': { code: 'EDI', name: 'Edinburgh' },
  'glasgow': { code: 'GLA', name: 'Glasgow' },
  'bristol': { code: 'BRS', name: 'Bristol' },
  'leeds': { code: 'LBA', name: 'Leeds Bradford' },
  'newcastle': { code: 'NCL', name: 'Newcastle' },
  'belfast': { code: 'BFS', name: 'Belfast' },
  'dublin': { code: 'DUB', name: 'Dublin' },
  'cork': { code: 'ORK', name: 'Cork' },
  // ── Western Europe ──────────────────────────────────────────────────────────
  'barcelona': { code: 'BCN', name: 'Barcelona' },
  'madrid': { code: 'MAD', name: 'Madrid' },
  'malaga': { code: 'AGP', name: 'Malaga' },
  'malága': { code: 'AGP', name: 'Malaga' },
  'seville': { code: 'SVQ', name: 'Seville' },
  'sevilla': { code: 'SVQ', name: 'Seville' },
  'valencia': { code: 'VLC', name: 'Valencia' },
  'alicante': { code: 'ALC', name: 'Alicante' },
  'bilbao': { code: 'BIO', name: 'Bilbao' },
  'palma': { code: 'PMI', name: 'Palma de Mallorca' },
  'mallorca': { code: 'PMI', name: 'Palma de Mallorca' },
  'majorca': { code: 'PMI', name: 'Palma de Mallorca' },
  'ibiza': { code: 'IBZ', name: 'Ibiza' },
  'tenerife': { code: 'TFS', name: 'Tenerife' },
  'gran canaria': { code: 'LPA', name: 'Gran Canaria' },
  'lanzarote': { code: 'ACE', name: 'Lanzarote' },
  'fuerteventura': { code: 'FUE', name: 'Fuerteventura' },
  'paris': { code: 'CDG', name: 'Paris' },
  'nice': { code: 'NCE', name: 'Nice' },
  'marseille': { code: 'MRS', name: 'Marseille' },
  'lyon': { code: 'LYS', name: 'Lyon' },
  'bordeaux': { code: 'BOD', name: 'Bordeaux' },
  'toulouse': { code: 'TLS', name: 'Toulouse' },
  'nantes': { code: 'NTE', name: 'Nantes' },
  'strasbourg': { code: 'SXB', name: 'Strasbourg' },
  'amsterdam': { code: 'AMS', name: 'Amsterdam' },
  'rotterdam': { code: 'RTM', name: 'Rotterdam' },
  'eindhoven': { code: 'EIN', name: 'Eindhoven' },
  'brussels': { code: 'BRU', name: 'Brussels' },
  'brussel': { code: 'BRU', name: 'Brussels' },
  'bruxelles': { code: 'BRU', name: 'Brussels' },
  'brüssel': { code: 'BRU', name: 'Brussels' },
  'lisbon': { code: 'LIS', name: 'Lisbon' },
  'lisbonne': { code: 'LIS', name: 'Lisbon' },
  'lissabon': { code: 'LIS', name: 'Lisbon' },
  'lisbona': { code: 'LIS', name: 'Lisbon' },
  'porto': { code: 'OPO', name: 'Porto' },
  'faro': { code: 'FAO', name: 'Faro' },
  'funchal': { code: 'FNC', name: 'Funchal (Madeira)' },
  'madeira': { code: 'FNC', name: 'Funchal (Madeira)' },
  'ponta delgada': { code: 'PDL', name: 'Ponta Delgada (Azores)' },
  'azores': { code: 'PDL', name: 'Ponta Delgada (Azores)' },
  // ── Central Europe ──────────────────────────────────────────────────────────
  'berlin': { code: 'BER', name: 'Berlin' },
  'munich': { code: 'MUC', name: 'Munich' },
  'munchen': { code: 'MUC', name: 'Munich' },
  'münchen': { code: 'MUC', name: 'Munich' },
  'frankfurt': { code: 'FRA', name: 'Frankfurt' },
  'hamburg': { code: 'HAM', name: 'Hamburg' },
  'dusseldorf': { code: 'DUS', name: 'Düsseldorf' },
  'düsseldorf': { code: 'DUS', name: 'Düsseldorf' },
  'cologne': { code: 'CGN', name: 'Cologne' },
  'koln': { code: 'CGN', name: 'Cologne' },
  'köln': { code: 'CGN', name: 'Cologne' },
  'stuttgart': { code: 'STR', name: 'Stuttgart' },
  'nuremberg': { code: 'NUE', name: 'Nuremberg' },
  'nürnberg': { code: 'NUE', name: 'Nuremberg' },
  'vienna': { code: 'VIE', name: 'Vienna' },
  'wien': { code: 'VIE', name: 'Vienna' },
  'vienne': { code: 'VIE', name: 'Vienna' },
  'zurich': { code: 'ZRH', name: 'Zurich' },
  'zürich': { code: 'ZRH', name: 'Zurich' },
  'geneva': { code: 'GVA', name: 'Geneva' },
  'geneve': { code: 'GVA', name: 'Geneva' },
  'genf': { code: 'GVA', name: 'Geneva' },
  'basel': { code: 'BSL', name: 'Basel' },
  'prague': { code: 'PRG', name: 'Prague' },
  'praha': { code: 'PRG', name: 'Prague' },
  'prag': { code: 'PRG', name: 'Prague' },
  'praga': { code: 'PRG', name: 'Prague' },
  'budapest': { code: 'BUD', name: 'Budapest' },
  'bratislava': { code: 'BTS', name: 'Bratislava' },
  'warsaw': { code: 'WAW', name: 'Warsaw' },
  'warsawa': { code: 'WAW', name: 'Warsaw' },
  'warszawa': { code: 'WAW', name: 'Warsaw' },
  'krakow': { code: 'KRK', name: 'Kraków' },
  'krakau': { code: 'KRK', name: 'Kraków' },
  'cracow': { code: 'KRK', name: 'Kraków' },
  'cracovie': { code: 'KRK', name: 'Kraków' },
  'gdansk': { code: 'GDN', name: 'Gdańsk' },
  'danzig': { code: 'GDN', name: 'Gdańsk' },
  'wroclaw': { code: 'WRO', name: 'Wrocław' },
  'breslau': { code: 'WRO', name: 'Wrocław' },
  'poznan': { code: 'POZ', name: 'Poznań' },
  'lodz': { code: 'LCJ', name: 'Łódź' },
  'katowice': { code: 'KTW', name: 'Katowice' },
  // ── Scandinavia & Baltics ────────────────────────────────────────────────────
  'stockholm': { code: 'ARN', name: 'Stockholm' },
  'arlanda': { code: 'ARN', name: 'Stockholm-Arlanda Airport' },
  'arl': { code: 'ARN', name: 'Stockholm-Arlanda Airport' },
  'goteborg': { code: 'GOT', name: 'Gothenburg' },
  'göteborg': { code: 'GOT', name: 'Gothenburg' },
  'gothenburg': { code: 'GOT', name: 'Gothenburg' },
  'malmo': { code: 'MMX', name: 'Malmö' },
  'malmö': { code: 'MMX', name: 'Malmö' },
  'oslo': { code: 'OSL', name: 'Oslo' },
  'bergen': { code: 'BGO', name: 'Bergen' },
  'trondheim': { code: 'TRD', name: 'Trondheim' },
  'copenhagen': { code: 'CPH', name: 'Copenhagen' },
  'kobenhavn': { code: 'CPH', name: 'Copenhagen' },
  'københavn': { code: 'CPH', name: 'Copenhagen' },
  'helsinki': { code: 'HEL', name: 'Helsinki' },
  'riga': { code: 'RIX', name: 'Riga' },
  'tallinn': { code: 'TLL', name: 'Tallinn' },
  'vilnius': { code: 'VNO', name: 'Vilnius' },
  // ── Southern Europe ──────────────────────────────────────────────────────────
  'rome': { code: 'FCO', name: 'Rome' },
  'roma': { code: 'FCO', name: 'Rome' },
  'milan': { code: 'MXP', name: 'Milan' },
  'milano': { code: 'MXP', name: 'Milan' },
  'naples': { code: 'NAP', name: 'Naples' },
  'napoli': { code: 'NAP', name: 'Naples' },
  'venice': { code: 'VCE', name: 'Venice' },
  'venezia': { code: 'VCE', name: 'Venice' },
  'florence': { code: 'FLR', name: 'Florence' },
  'firenze': { code: 'FLR', name: 'Florence' },
  'bologna': { code: 'BLQ', name: 'Bologna' },
  'catania': { code: 'CTA', name: 'Catania' },
  'palermo': { code: 'PMO', name: 'Palermo' },
  'bari': { code: 'BRI', name: 'Bari' },
  'athens': { code: 'ATH', name: 'Athens' },
  'athen': { code: 'ATH', name: 'Athens' },
  'athenes': { code: 'ATH', name: 'Athens' },
  'thessaloniki': { code: 'SKG', name: 'Thessaloniki' },
  'heraklion': { code: 'HER', name: 'Heraklion (Crete)' },
  'crete': { code: 'HER', name: 'Heraklion (Crete)' },
  'santorini': { code: 'JTR', name: 'Santorini' },
  'mykonos': { code: 'JMK', name: 'Mykonos' },
  'rhodes': { code: 'RHO', name: 'Rhodes' },
  'corfu': { code: 'CFU', name: 'Corfu' },
  'istanbul': { code: 'IST', name: 'Istanbul' },
  'ankara': { code: 'ESB', name: 'Ankara' },
  'antalya': { code: 'AYT', name: 'Antalya' },
  'izmir': { code: 'ADB', name: 'İzmir' },
  'bodrum': { code: 'BJV', name: 'Bodrum' },
  'belgrade': { code: 'BEG', name: 'Belgrade' },
  'beograd': { code: 'BEG', name: 'Belgrade' },
  'zagreb': { code: 'ZAG', name: 'Zagreb' },
  'split': { code: 'SPU', name: 'Split' },
  'dubrovnik': { code: 'DBV', name: 'Dubrovnik' },
  'sarajevo': { code: 'SJJ', name: 'Sarajevo' },
  'podgorica': { code: 'TGD', name: 'Podgorica' },
  'tirana': { code: 'TIA', name: 'Tirana' },
  'tirane': { code: 'TIA', name: 'Tirana' },
  'skopje': { code: 'SKP', name: 'Skopje' },
  'sofia': { code: 'SOF', name: 'Sofia' },
  'bucharest': { code: 'OTP', name: 'Bucharest' },
  'bukarest': { code: 'OTP', name: 'Bucharest' },
  'bucaresti': { code: 'OTP', name: 'Bucharest' },
  'cluj': { code: 'CLJ', name: 'Cluj-Napoca' },
  'chisinau': { code: 'KIV', name: 'Chișinău' },
  'kyiv': { code: 'KBP', name: 'Kyiv' },
  'kiev': { code: 'KBP', name: 'Kyiv' },
  'lviv': { code: 'LWO', name: 'Lviv' },
  'minsk': { code: 'MSQ', name: 'Minsk' },
  // ── Middle East ──────────────────────────────────────────────────────────────
  'dubai': { code: 'DXB', name: 'Dubai' },
  'abu dhabi': { code: 'AUH', name: 'Abu Dhabi' },
  'sharjah': { code: 'SHJ', name: 'Sharjah' },
  'doha': { code: 'DOH', name: 'Doha' },
  'kuwait': { code: 'KWI', name: 'Kuwait City' },
  'kuwait city': { code: 'KWI', name: 'Kuwait City' },
  'muscat': { code: 'MCT', name: 'Muscat' },
  'bahrain': { code: 'BAH', name: 'Bahrain' },
  'riyadh': { code: 'RUH', name: 'Riyadh' },
  'jeddah': { code: 'JED', name: 'Jeddah' },
  'dammam': { code: 'DMM', name: 'Dammam' },
  'amman': { code: 'AMM', name: 'Amman' },
  'beirut': { code: 'BEY', name: 'Beirut' },
  'tel aviv': { code: 'TLV', name: 'Tel Aviv' },
  'jerusalem': { code: 'TLV', name: 'Tel Aviv' },
  'baghdad': { code: 'BGW', name: 'Baghdad' },
  'tehran': { code: 'IKA', name: 'Tehran' },
  // ── Africa ───────────────────────────────────────────────────────────────────
  'cairo': { code: 'CAI', name: 'Cairo' },
  'kairo': { code: 'CAI', name: 'Cairo' },
  'casablanca': { code: 'CMN', name: 'Casablanca' },
  'marrakech': { code: 'RAK', name: 'Marrakech' },
  'marrakesh': { code: 'RAK', name: 'Marrakech' },
  'agadir': { code: 'AGA', name: 'Agadir' },
  'fez': { code: 'FEZ', name: 'Fez' },
  'tunis': { code: 'TUN', name: 'Tunis' },
  'algiers': { code: 'ALG', name: 'Algiers' },
  'tripoli': { code: 'TIP', name: 'Tripoli' },
  'nairobi': { code: 'NBO', name: 'Nairobi' },
  'mombasa': { code: 'MBA', name: 'Mombasa' },
  'addis ababa': { code: 'ADD', name: 'Addis Ababa' },
  'lagos': { code: 'LOS', name: 'Lagos' },
  'accra': { code: 'ACC', name: 'Accra' },
  'abuja': { code: 'ABV', name: 'Abuja' },
  'dakar': { code: 'DSS', name: 'Dakar' },
  'johannesburg': { code: 'JNB', name: 'Johannesburg' },
  'cape town': { code: 'CPT', name: 'Cape Town' },
  'durban': { code: 'DUR', name: 'Durban' },
  'dar es salaam': { code: 'DAR', name: 'Dar es Salaam' },
  'zanzibar': { code: 'ZNZ', name: 'Zanzibar' },
  'kampala': { code: 'EBB', name: 'Kampala' },
  'entebbe': { code: 'EBB', name: 'Kampala (Entebbe)' },
  'luanda': { code: 'LAD', name: 'Luanda' },
  'maputo': { code: 'MPM', name: 'Maputo' },
  'reunion': { code: 'RUN', name: 'Réunion' },
  'mauritius': { code: 'MRU', name: 'Mauritius' },
  // ── Asia ─────────────────────────────────────────────────────────────────────
  'tokyo': { code: 'TYO', name: 'Tokyo' },
  'osaka': { code: 'KIX', name: 'Osaka' },
  'nagoya': { code: 'NGO', name: 'Nagoya' },
  'sapporo': { code: 'CTS', name: 'Sapporo' },
  'fukuoka': { code: 'FUK', name: 'Fukuoka' },
  'seoul': { code: 'ICN', name: 'Seoul' },
  'busan': { code: 'PUS', name: 'Busan' },
  'beijing': { code: 'PEK', name: 'Beijing' },
  'peking': { code: 'PEK', name: 'Beijing' },
  'shanghai': { code: 'PVG', name: 'Shanghai' },
  'guangzhou': { code: 'CAN', name: 'Guangzhou' },
  'shenzhen': { code: 'SZX', name: 'Shenzhen' },
  'chengdu': { code: 'CTU', name: 'Chengdu' },
  'hong kong': { code: 'HKG', name: 'Hong Kong' },
  'macau': { code: 'MFM', name: 'Macau' },
  'taipei': { code: 'TPE', name: 'Taipei' },
  'singapore': { code: 'SIN', name: 'Singapore' },
  'bangkok': { code: 'BKK', name: 'Bangkok' },
  'phuket': { code: 'HKT', name: 'Phuket' },
  'chiang mai': { code: 'CNX', name: 'Chiang Mai' },
  'bali': { code: 'DPS', name: 'Bali' },
  'denpasar': { code: 'DPS', name: 'Bali' },
  'jakarta': { code: 'CGK', name: 'Jakarta' },
  'surabaya': { code: 'SUB', name: 'Surabaya' },
  'kuala lumpur': { code: 'KUL', name: 'Kuala Lumpur' },
  'penang': { code: 'PEN', name: 'Penang' },
  'manila': { code: 'MNL', name: 'Manila' },
  'cebu': { code: 'CEB', name: 'Cebu' },
  'ho chi minh': { code: 'SGN', name: 'Ho Chi Minh City' },
  'saigon': { code: 'SGN', name: 'Ho Chi Minh City' },
  'hanoi': { code: 'HAN', name: 'Hanoi' },
  'danang': { code: 'DAD', name: 'Da Nang' },
  'da nang': { code: 'DAD', name: 'Da Nang' },
  'phnom penh': { code: 'PNH', name: 'Phnom Penh' },
  'yangon': { code: 'RGN', name: 'Yangon' },
  'vientiane': { code: 'VTE', name: 'Vientiane' },
  'mumbai': { code: 'BOM', name: 'Mumbai' },
  'bombay': { code: 'BOM', name: 'Mumbai' },
  'delhi': { code: 'DEL', name: 'Delhi' },
  'new delhi': { code: 'DEL', name: 'Delhi' },
  'bangalore': { code: 'BLR', name: 'Bangalore' },
  'bengaluru': { code: 'BLR', name: 'Bangalore' },
  'hyderabad': { code: 'HYD', name: 'Hyderabad' },
  'chennai': { code: 'MAA', name: 'Chennai' },
  'madras': { code: 'MAA', name: 'Chennai' },
  'kolkata': { code: 'CCU', name: 'Kolkata' },
  'calcutta': { code: 'CCU', name: 'Kolkata' },
  'ahmedabad': { code: 'AMD', name: 'Ahmedabad' },
  'goa': { code: 'GOI', name: 'Goa' },
  'kochi': { code: 'COK', name: 'Kochi' },
  'colombo': { code: 'CMB', name: 'Colombo' },
  'kathmandu': { code: 'KTM', name: 'Kathmandu' },
  'dhaka': { code: 'DAC', name: 'Dhaka' },
  'karachi': { code: 'KHI', name: 'Karachi' },
  'lahore': { code: 'LHE', name: 'Lahore' },
  'islamabad': { code: 'ISB', name: 'Islamabad' },
  'tashkent': { code: 'TAS', name: 'Tashkent' },
  'almaty': { code: 'ALA', name: 'Almaty' },
  'astana': { code: 'NQZ', name: 'Astana' },
  'tbilisi': { code: 'TBS', name: 'Tbilisi' },
  'yerevan': { code: 'EVN', name: 'Yerevan' },
  'baku': { code: 'GYD', name: 'Baku' },
  // ── Americas ─────────────────────────────────────────────────────────────────
  'new york': { code: 'NYC', name: 'New York' },
  'nyc': { code: 'NYC', name: 'New York' },
  'jfk': { code: 'JFK', name: 'New York JFK' },
  'newark': { code: 'EWR', name: 'Newark' },
  'ewr': { code: 'EWR', name: 'Newark' },
  'laguardia': { code: 'LGA', name: 'New York LaGuardia' },
  'los angeles': { code: 'LAX', name: 'Los Angeles' },
  'la': { code: 'LAX', name: 'Los Angeles' },
  'san francisco': { code: 'SFO', name: 'San Francisco' },
  'sf': { code: 'SFO', name: 'San Francisco' },
  'chicago': { code: 'ORD', name: 'Chicago' },
  'miami': { code: 'MIA', name: 'Miami' },
  'dallas': { code: 'DFW', name: 'Dallas' },
  'houston': { code: 'IAH', name: 'Houston' },
  'boston': { code: 'BOS', name: 'Boston' },
  'seattle': { code: 'SEA', name: 'Seattle' },
  'washington': { code: 'WAS', name: 'Washington DC' },
  'dc': { code: 'WAS', name: 'Washington DC' },
  'atlanta': { code: 'ATL', name: 'Atlanta' },
  'las vegas': { code: 'LAS', name: 'Las Vegas' },
  'orlando': { code: 'MCO', name: 'Orlando' },
  'denver': { code: 'DEN', name: 'Denver' },
  'phoenix': { code: 'PHX', name: 'Phoenix' },
  'minneapolis': { code: 'MSP', name: 'Minneapolis' },
  'detroit': { code: 'DTW', name: 'Detroit' },
  'san diego': { code: 'SAN', name: 'San Diego' },
  'portland': { code: 'PDX', name: 'Portland' },
  'toronto': { code: 'YYZ', name: 'Toronto' },
  'vancouver': { code: 'YVR', name: 'Vancouver' },
  'montreal': { code: 'YUL', name: 'Montreal' },
  'calgary': { code: 'YYC', name: 'Calgary' },
  'edmonton': { code: 'YEG', name: 'Edmonton' },
  'ottawa': { code: 'YOW', name: 'Ottawa' },
  'mexico city': { code: 'MEX', name: 'Mexico City' },
  'cancun': { code: 'CUN', name: 'Cancun' },
  'guadalajara': { code: 'GDL', name: 'Guadalajara' },
  'havana': { code: 'HAV', name: 'Havana' },
  'la habana': { code: 'HAV', name: 'Havana' },
  'santo domingo': { code: 'SDQ', name: 'Santo Domingo' },
  'san jose': { code: 'SJO', name: 'San José (CR)' },
  'panama city': { code: 'PTY', name: 'Panama City' },
  'bogota': { code: 'BOG', name: 'Bogotá' },
  'bogotá': { code: 'BOG', name: 'Bogotá' },
  'medellin': { code: 'MDE', name: 'Medellín' },
  'medellín': { code: 'MDE', name: 'Medellín' },
  'lima': { code: 'LIM', name: 'Lima' },
  'santiago': { code: 'SCL', name: 'Santiago' },
  'buenos aires': { code: 'EZE', name: 'Buenos Aires' },
  'sao paulo': { code: 'GRU', name: 'São Paulo' },
  'são paulo': { code: 'GRU', name: 'São Paulo' },
  'rio de janeiro': { code: 'GIG', name: 'Rio de Janeiro' },
  'rio': { code: 'GIG', name: 'Rio de Janeiro' },
  'brasilia': { code: 'BSB', name: 'Brasília' },
  'brasília': { code: 'BSB', name: 'Brasília' },
  'manaus': { code: 'MAO', name: 'Manaus' },
  'quito': { code: 'UIO', name: 'Quito' },
  'guayaquil': { code: 'GYE', name: 'Guayaquil' },
  'la paz': { code: 'LPB', name: 'La Paz' },
  'montevideo': { code: 'MVD', name: 'Montevideo' },
  'asuncion': { code: 'ASU', name: 'Asunción' },
  // ── Oceania ──────────────────────────────────────────────────────────────────
  'sydney': { code: 'SYD', name: 'Sydney' },
  'melbourne': { code: 'MEL', name: 'Melbourne' },
  'brisbane': { code: 'BNE', name: 'Brisbane' },
  'perth': { code: 'PER', name: 'Perth' },
  'adelaide': { code: 'ADL', name: 'Adelaide' },
  'gold coast': { code: 'OOL', name: 'Gold Coast' },
  'cairns': { code: 'CNS', name: 'Cairns' },
  'darwin': { code: 'DRW', name: 'Darwin' },
  'auckland': { code: 'AKL', name: 'Auckland' },
  'wellington': { code: 'WLG', name: 'Wellington' },
  'christchurch': { code: 'CHC', name: 'Christchurch' },
  'queenstown': { code: 'ZQN', name: 'Queenstown' },
  'nadi': { code: 'NAN', name: 'Nadi (Fiji)' },
  'fiji': { code: 'NAN', name: 'Nadi (Fiji)' },
  'papeete': { code: 'PPT', name: 'Papeete (Tahiti)' },
  'tahiti': { code: 'PPT', name: 'Papeete (Tahiti)' },
  'noumea': { code: 'NOU', name: 'Nouméa' },
}

export interface ParsedQuery {
  origin?: string
  origin_name?: string
  destination?: string
  destination_name?: string
  date?: string
  return_date?: string
  cabin?: 'M' | 'W' | 'C' | 'F'   // M=economy, W=premium economy, C=business, F=first
  stops?: number                     // 0 = direct/nonstop only
  failed_origin_raw?: string         // raw text that didn't resolve to an airport
  failed_destination_raw?: string
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// Strip accents/diacritics for fuzzy city matching
function stripAccents(s: string): string {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

// Format a Date as YYYY-MM-DD in local time (avoids UTC-shift issues)
function toLocalDateStr(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

function getNthWeekdayOfMonth(year: number, monthIndex: number, weekday: number, occurrence: number): Date {
  const date = new Date(year, monthIndex, 1)
  const offset = (weekday - date.getDay() + 7) % 7
  date.setDate(1 + offset + (occurrence - 1) * 7)
  return date
}

function getUpcomingUsThanksgiving(baseDate: Date): Date {
  let thanksgiving = getNthWeekdayOfMonth(baseDate.getFullYear(), 10, 4, 4)
  if (thanksgiving < baseDate) {
    thanksgiving = getNthWeekdayOfMonth(baseDate.getFullYear() + 1, 10, 4, 4)
  }
  return thanksgiving
}

// Edit distance (Levenshtein) — for typo tolerance in city matching
function editDistance(a: string, b: string): number {
  if (a === b) return 0
  if (a.length === 0) return b.length
  if (b.length === 0) return a.length
  const row = Array.from({ length: b.length + 1 }, (_, i) => i)
  for (let i = 1; i <= a.length; i++) {
    let prev = i
    for (let j = 1; j <= b.length; j++) {
      const val = a[i - 1] === b[j - 1] ? row[j - 1] : Math.min(row[j - 1], row[j], prev) + 1
      row[j - 1] = prev
      prev = val
    }
    row[b.length] = prev
  }
  return row[b.length]
}

function containsLocationKey(text: string, key: string): boolean {
  const haystack = stripAccents(text.toLowerCase())
  const needle = stripAccents(key.toLowerCase())
  return new RegExp(`(?:^|[^a-z0-9])${escapeRegExp(needle)}(?:$|[^a-z0-9])`, 'i').test(haystack)
}

// Look up a city string → IATA:
// 1. Exact match (accent-aware)
// 2. Explicit 3-letter code inside a longer phrase ("new york jfk" → JFK)
// 3. Boundary-aware contained phrase match (longest key first)
// 4. Fuzzy edit-distance fallback (handles typos like "Barcelna" → Barcelona)
function resolveCity(raw: string): { code: string; name: string } | null {
  const s = raw.toLowerCase().trim()
  if (!s || s.length < 2) return null

  // Exact match
  if (CITY_TO_IATA[s]) return CITY_TO_IATA[s]

  // Accent-stripped exact
  const stripped = stripAccents(s)
  if (CITY_TO_IATA[stripped]) return CITY_TO_IATA[stripped]

  const explicitCodeTokens = stripped.match(/\b[a-z]{3}\b/g) || []
  for (let idx = explicitCodeTokens.length - 1; idx >= 0; idx -= 1) {
    const mapped = CITY_TO_IATA[explicitCodeTokens[idx]]
    if (mapped) return mapped
  }

  // Boundary-aware contained phrase: longest key first so "new york" beats "york"
  const entries = Object.entries(CITY_TO_IATA).sort((a, b) => b[0].length - a[0].length)
  for (const [k, v] of entries) {
    if (containsLocationKey(s, k)) return v
  }

  // Fuzzy: edit distance tolerance scales with word length
  // ≤4 chars: exact only (avoid "la" matching "lag" etc)
  // 5-7 chars: allow 1 edit
  // 8+ chars: allow 2 edits
  if (stripped.length >= 5) {
    const maxDist = stripped.length >= 8 ? 2 : 1
    let best: { dist: number; val: { code: string; name: string } } | null = null
    for (const [k, v] of entries) {
      // Skip very short keys to avoid false positives
      if (k.length < 4) continue
      const dist = editDistance(stripped, stripAccents(k))
      if (dist <= maxDist && (!best || dist < best.dist)) {
        best = { dist, val: v }
      }
    }
    if (best) return best.val
  }

  return null
}

function resolveLocation(raw: string): { code: string; name: string } | null {
  const exactGenerated = findExactLocationMatch(raw)
  const normalized = raw.toLowerCase().trim()
  const stripped = stripAccents(normalized)
  if (exactGenerated?.type === 'city') {
    const mapped = CITY_TO_IATA[normalized] || CITY_TO_IATA[stripped]
    return {
      code: exactGenerated.code,
      name: mapped?.name || exactGenerated.name,
    }
  }

  const resolved = resolveCity(raw)
  if (resolved) return resolved

  if (exactGenerated) {
    return { code: exactGenerated.code, name: exactGenerated.name }
  }

  const generated = findBestLocationMatch(raw)
  if (generated) return { code: generated.code, name: generated.name }
  if (/^[a-zA-Z]{3}$/.test(raw.trim())) {
    const code = raw.toUpperCase()
    return { code, name: code }
  }
  return null
}

// ── Cabin class extraction (all languages) ─────────────────────────────────────
function extractCabin(text: string): 'M' | 'W' | 'C' | 'F' | undefined {
  const t = stripAccents(text.toLowerCase())
  // Order: most specific first (first class before first, premium economy before economy)
  if (/\b(?:first\s+class|erste\s+klasse|primera\s+clase|premi[eè]re\s+classe|prima\s+classe|eerste\s+klas|pierwsza\s+klasa|primeira\s+classe|f[oö]rsta\s+klass|prva\s+klasa|klasa\s+e\s+par[eë])\b/.test(t)) return 'F'
  if (/\b(?:premium\s+economy|premium\s+[eé]conomique|premium\s+economi[ck]a|premium\s+econ[oô]mica|premium\s+econ[oô]mica)\b/.test(t)) return 'W'
  if (/\b(?:business\s+class|businessklasse|clase\s+(?:business|ejecutiva)|ejecutiva|classe\s+(?:affaires|business)|affaires|klasa\s+biznes|classe\s+executiva|executiva|businessklass|poslovna\s+klasa|zakenklasse|zakelijk|biznes|business)\b/.test(t)) return 'C'
  if (/\b(?:economy\s+class|wirtschaftsklasse|clase\s+turista|turista|classe\s+[eé]conomique|[eé]conomique|classe\s+economica|economica|economyclass|klasa\s+ekonomiczna|ekonomiklass|ekonomska\s+klasa|economy|coach|economica|economi[ck]a)\b/.test(t)) return 'M'
  return undefined
}

// ── Direct/nonstop extraction (all languages) ─────────────────────────────────
function extractDirect(text: string): boolean {
  const t = stripAccents(text.toLowerCase())
  return /\b(?:direct|nonstop|non[- ]stop|direkt(?:flug)?|ohne\s+(?:umstieg|zwischenstopp)|directo|sin\s+escalas?|vuelo\s+directo|sans?\s+escale|vol\s+direct|diretto|volo\s+diretto|senza\s+scal[ei]|rechtstreeks|zonder\s+tussenstop|bezposredni|bez\s+przesiadek|sem\s+escala[s]?|direto|direktflyg|izravno|bez\s+presjedanja|pa\s+ndalese)\b/.test(t)
}

// ── Month names across all supported languages ────────────────────────────────
// Each entry maps localised name → 0-based month index.
// Sorted longest-first so 'janvier' matches before 'jan'.
const MONTH_MAP: [string, number][] = ([
  // EN
  ['january',0],['february',1],['march',2],['april',3],['may',4],['june',5],
  ['july',6],['august',7],['september',8],['october',9],['november',10],['december',11],
  ['jan',0],['feb',1],['mar',2],['apr',3],['jun',5],['jul',6],['aug',7],
  ['sep',8],['oct',9],['nov',10],['dec',11],
  // DE
  ['januar',0],['februar',1],['märz',2],['maerz',2],['mai',4],['juni',5],
  ['juli',6],['oktober',9],['dezember',11],
  // ES / IT / PT
  ['enero',0],['febrero',1],['marzo',2],['abril',3],['mayo',4],['junio',5],
  ['julio',6],['agosto',7],['septiembre',8],['setiembre',8],['octubre',9],['noviembre',10],['diciembre',11],
  ['gen',0],['gennaio',0],['febbraio',1],['giugno',5],['luglio',6],['agosto',7],
  ['settembre',8],['ottobre',9],['novembre',10],['dicembre',11],
  ['janeiro',0],['fevereiro',1],['marco',2],['março',2],['junho',5],['julho',6],
  ['setembro',8],['outubro',9],['dezembro',11],
  // FR
  ['janvier',0],['février',1],['fevrier',1],['mars',2],['avril',3],['mai',4],['juin',5],
  ['juillet',6],['août',7],['aout',7],['septembre',8],['octobre',9],['novembre',10],['décembre',11],['decembre',11],
  // NL
  ['januari',0],['februari',1],['maart',2],['april',3],['mei',4],['juni',5],
  ['juli',6],['augustus',7],['september',8],['oktober',9],['november',10],['december',11],
  // PL
  ['styczeń',0],['styczen',0],['luty',1],['marzec',2],['kwiecień',3],['kwiecien',3],
  ['maj',4],['czerwiec',5],['lipiec',6],['sierpień',7],['sierpien',7],
  ['wrzesień',8],['wrzesien',8],['październik',9],['pazdziernik',9],
  ['listopad',10],['grudzień',11],['grudzien',11],
  // SV
  ['januari',0],['februari',1],['mars',2],['april',3],['maj',4],['juni',5],
  ['juli',6],['augusti',7],['september',8],['oktober',9],['november',10],['december',11],
  // HR/SQ
  ['siječanj',0],['sijecanj',0],['veljača',1],['veljaca',1],['oĵujak',2],['ozujak',2],
  ['travanj',3],['svibanj',4],['lipanj',5],['srpanj',6],['kolovoz',7],
  ['rujan',8],['listopad',9],['studeni',10],['prosinac',11],
  ['janar',0],['shkurt',1],['mars',2],['prill',3],['qershor',5],
  ['korrik',6],['gusht',7],['shtator',8],['tetor',9],['nëntor',10],['dhjetor',11],
] as [string, number][]).sort((a, b) => b[0].length - a[0].length)

function matchMonth(text: string): number | null {
  const t = stripAccents(text.toLowerCase())
  for (const [name, idx] of MONTH_MAP) {
    if (t.startsWith(stripAccents(name))) return idx
  }
  return null
}

// ── Weekday names across all supported languages ───────────────────────────────
// Value = 0 (Sun)–6 (Sat), matching Date.getDay()
const WEEKDAY_MAP: [string, number][] = ([
  // EN
  ['sunday',0],['monday',1],['tuesday',2],['wednesday',3],['thursday',4],['friday',5],['saturday',6],
  // DE
  ['sonntag',0],['montag',1],['dienstag',2],['mittwoch',3],['donnerstag',4],['freitag',5],['samstag',6],
  // ES
  ['domingo',0],['lunes',1],['martes',2],['miércoles',3],['miercoles',3],['jueves',4],['viernes',5],['sábado',6],['sabado',6],
  // FR
  ['dimanche',0],['lundi',1],['mardi',2],['mercredi',3],['jeudi',4],['vendredi',5],['samedi',6],
  // IT
  ['domenica',0],['lunedì',1],['lunedi',1],['martedì',2],['martedi',2],['mercoledì',3],['mercoledi',3],
  ['giovedì',4],['giovedi',4],['venerdì',5],['venerdi',5],['sabato',6],
  // NL
  ['zondag',0],['maandag',1],['dinsdag',2],['woensdag',3],['donderdag',4],['vrijdag',5],['zaterdag',6],
  // PL
  ['niedziela',0],['poniedziałek',1],['poniedzialek',1],['wtorek',2],['środa',3],['sroda',3],
  ['czwartek',4],['piątek',5],['piatek',5],['sobota',6],
  // PT
  ['domingo',0],['segunda',1],['terça',2],['terca',2],['quarta',3],['quinta',4],['sexta',5],['sábado',6],['sabado',6],
  // SV
  ['söndag',0],['sondag',0],['måndag',1],['mandag',1],['tisdag',2],['onsdag',3],['torsdag',4],['fredag',5],['lördag',6],['lordag',6],
  // HR
  ['nedjelja',0],['ponedjeljak',1],['utorak',2],['srijeda',3],['četvrtak',4],['petak',5],['subota',6],
  // SQ
  ['e diele',0],['e hënë',1],['e hene',1],['e martë',2],['e marte',2],['e mërkurë',3],['e merkure',3],
  ['e enjte',4],['e premte',5],['e shtunë',6],['e shtune',6],
] as [string, number][]).sort((a, b) => b[0].length - a[0].length)

// ── Keywords that introduce return date (all languages) ───────────────────────
// Order matters: longer strings first to avoid partial matches
const RETURN_SPLIT_RE = new RegExp(
  '\\s+(?:' + [
    // EN
    'returning on','returning','return on','return date','come back on','coming back on','coming back','back on','back',
    // DE
    'rückflug am','rückflug','zurück am','zurück','ruckreise am','ruckreise',
    // ES
    'regresando el','regresando','vuelta el','vuelta','de vuelta el','de vuelta','regreso el','regreso',
    // FR
    'retour le','retour',
    // IT
    'ritorno il','ritorno','di ritorno il','di ritorno',
    // NL
    'terug op','terug','retour op','retour',
    // PL
    'powrót','powrot','wracam',
    // PT
    'retorno em','retorno','de volta em','de volta','volta em','volta',
    // SV
    'återresa','aterresa','tillbaka',
    // HR
    'povratak','natrag',
    // SQ
    'kthim',
  ].join('|') + ')\\s+',
  'i'
)

// ── Preposition/filler words before city names (all languages) ────────────────
const ORIGIN_PREFIX_RE = /^(?:from|from the|fly|flight|book|find|cheap|cheapest|best|search|get me|show me|i want to fly|i want to go|i need to fly|i need to go|von|ab|von\s+|aus|desde|desde el|desde la|de|de\s+|depuis|depuis le|depuis la|da|da\s+|uit|van|vanaf|vanuit|z|ze|ze\s+|från|fran|iz|nga)\s+/i
const DEST_PREFIX_RE = /^(?:(?:to(?:\s+the)?|into|nach|in(?:\s+die|\s+den|\s+das)?|a|à|zu|para|til|naar|do|till|na|ne|drejt)\b|→|->|–|-)\s*/i

// ── Route connector words / arrows (split origin from destination) ─────────────
const ROUTE_SEP_RE = new RegExp(
  '\\s+(?:to(?:\\s+the)?|→|->|–|nach|nach\s+|aan|a\s+(?=\\p{L})|à\s+|au\s+|en\s+(?=\\p{L})|para\s+|til\s+|naar\s+|do\s+|till\s+|na\s+|drejt\s+|vo\s+|leti\s+|let\s+|leten\s+)(?=\\S)',
  'i'
)

// ── Date phrase modifiers ─────────────────────────────────────────────────────
// "next friday", "this saturday", "the friday after next", etc.
const REL_DATE_NEXT_RE = /\b(?:next|diese[rns]?|nächste[rns]?|nachste[rns]?|proxim[ao]|prochain[e]?|prossim[ao]|volgende|następn[ya]|nastepn[ya]|nästa|nasta|sljedeć[ia]|sljedeci[a]?)\b/i
const REL_DATE_THIS_RE = /\b(?:this|heute|hoy|aujourd'?hui|oggi|vandaag|dzisiaj|hoje|idag|danas|sot)\b/i
const REL_WEEKEND_RE = /\b(?:weekend|this weekend|wochenende|fin de semana|week-?end|fine settimana|weekeinde|vikend|helg)\b/i
const THANKSGIVING_WEEK_RE = /\b(?:(?:the\s+)?week\s+of\s+thanksgiving|thanksgiving\s+week)\b/i
const THANKSGIVING_RE = /\bthanksgiving\b/i

// ── Main parse function ───────────────────────────────────────────────────────

export function parseNLQuery(query: string): ParsedQuery {
  // Normalise: trim, collapse whitespace, strip leading/trailing punctuation
  const q = query.trim().replace(/\s+/g, ' ').replace(/^[,.:!?]+|[,.:!?]+$/g, '')
  const ql = q.toLowerCase()
  const result: ParsedQuery = {}

  const today = new Date()
  today.setHours(0, 0, 0, 0)

  // ── 1. Split at return keywords ──────────────────────────────────────────
  const returnSplitMatch = ql.match(RETURN_SPLIT_RE)
  const returnSplitIdx = returnSplitMatch ? ql.indexOf(returnSplitMatch[0]) : -1
  const outboundRaw = returnSplitIdx >= 0 ? q.slice(0, returnSplitIdx) : q
  const returnRaw = returnSplitIdx >= 0 ? q.slice(returnSplitIdx + returnSplitMatch![0].length) : null

  // ── 2. Extract cities from outbound part ─────────────────────────────────
  // Try multiple route separator patterns
  const routePatterns = [
    // "ORIGIN to DESTINATION"
    /^(.+?)\s+(?:to(?:\s+the)?|→|->|–)\s+(.+?)(?:\s+(?:on|in|for|at|around|circa|um|am|le|el|il|em|på|na)\s|\s+\d|\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|januar|février|fevrier|mars|abril|mayo|junio|julio|agosto|septembre|outubro|novembre)|$)/i,
    // "ORIGIN - DESTINATION" (dash as separator, not range)
    /^(.+?)\s+-\s+(.+?)(?:\s+\d|\s+(?:jan|feb|mar|may|jun|jul|aug|sep|oct|nov|dec)|$)/i,
    // "ORIGIN nach/para/à/naar/do/till DESTINATION"
    /^(.+?)\s+(?:nach|para|à|naar|do|till|na|drejt|leti)\s+(.+?)(?:\s+\d|\s+(?:jan|feb|mar|may)|$)/i,
  ]

  let originStr = '', destStr = ''
  for (const pat of routePatterns) {
    const m = outboundRaw.match(pat)
    if (m) {
      originStr = m[1].trim()
      destStr = m[2].trim()
      break
    }
  }

  // Strip filler prefixes
  if (originStr) {
    originStr = originStr.replace(ORIGIN_PREFIX_RE, '').trim()
  }
  if (destStr) {
    // Stop destination string at common date lead-ins that weren't caught by the regex
    destStr = destStr
      .replace(/\s+(?:(?:next|this)\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|weekend)|(?:the\s+week\s+of\s+thanksgiving|thanksgiving\s+week|thanksgiving))\b.*/i, '')
      .replace(/\s+(?:on|in|for|at|around|circa|um|am|le|el|il|em|på|na|dne|dia|den|am)\s.*/i, '')
      .replace(/\s+\d{1,2}(?:st|nd|rd|th)?\s.*/i, '')
      .replace(DEST_PREFIX_RE, '')
      .trim()
  }

  // Resolve cities
  if (originStr) {
    const r = resolveLocation(originStr)
    if (r) { result.origin = r.code; result.origin_name = r.name }
    else result.failed_origin_raw = originStr
  }

  if (destStr) {
    const r = resolveLocation(destStr)
    if (r) { result.destination = r.code; result.destination_name = r.name }
    else result.failed_destination_raw = destStr
  }

  // ── 3. Date extraction helper ────────────────────────────────────────────
  function extractDate(text: string): string | undefined {
    const t = text.trim()
    const tl = stripAccents(t.toLowerCase())

    // ISO: 2026-05-15
    const isoM = t.match(/\b(\d{4}-\d{2}-\d{2})\b/)
    if (isoM) return isoM[1]

    // DD/MM/YYYY or DD.MM.YYYY or DD-MM-YYYY (European)
    const dmyM = t.match(/\b(\d{1,2})[./-](\d{1,2})[./-](\d{4})\b/)
    if (dmyM) {
      const [, d, m, y] = dmyM
      return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`
    }

    // DD/MM or DD.MM (no year — assume current/next year)
    const dmM = t.match(/\b(\d{1,2})[./](\d{1,2})\b/)
    if (dmM) {
      const day = parseInt(dmM[1]), mon = parseInt(dmM[2]) - 1
      if (day >= 1 && day <= 31 && mon >= 0 && mon <= 11) {
        const d = new Date(today.getFullYear(), mon, day)
        if (d < today) d.setFullYear(today.getFullYear() + 1)
        return toLocalDateStr(d)
      }
    }

    // "15th May", "May 15", "15 mai", "le 15 mai", "am 15. mai", etc.
    // Build a token list and search for day+month in any order
    // First strip common lead-in prepositions
    const cleaned = tl.replace(/\b(?:on|le|am|el|il|em|dne|den|dia|på|na|the)\b/g, ' ').replace(/\s+/g,' ').trim()

    // Try: <number><ordinal?> <monthname>  or  <monthname> <number><ordinal?>
    const dayMonthRe = /(\d{1,2})(?:st|nd|rd|th|er|ème|eme|º|ª|\.)?\.?\s+([a-zäöüčšžćđéèêëàâùûîïôœæñß]+)(?:\s*,?\s*(\d{4}))?/
    const monthDayRe = /([a-zäöüčšžćđéèêëàâùûîïôœæñß]+)\s+(\d{1,2})(?:st|nd|rd|th|er|ème|eme|º|ª|\.)?(?:\s*,?\s*(\d{4}))?/

    const dm = cleaned.match(dayMonthRe)
    if (dm) {
      const day = parseInt(dm[1])
      const mIdx = matchMonth(dm[2])
      if (mIdx !== null && day >= 1 && day <= 31) {
        const hasExplicitYear = Boolean(dm[3])
        const year = hasExplicitYear ? parseInt(dm[3]) : today.getFullYear()
        const d = new Date(year, mIdx, day)
        if (!hasExplicitYear && d < today) d.setFullYear(today.getFullYear() + 1)
        return toLocalDateStr(d)
      }
    }
    const md = cleaned.match(monthDayRe)
    if (md) {
      const mIdx = matchMonth(md[1])
      const day = parseInt(md[2])
      if (mIdx !== null && day >= 1 && day <= 31) {
        const hasExplicitYear = Boolean(md[3])
        const year = hasExplicitYear ? parseInt(md[3]) : today.getFullYear()
        const d = new Date(year, mIdx, day)
        if (!hasExplicitYear && d < today) d.setFullYear(today.getFullYear() + 1)
        return toLocalDateStr(d)
      }
    }

    // Month-only: "in May", "im Mai", "en mayo", "en juin"
    // → default to 1st of that month
    const monthOnlyRe = /(?:in|im|en|em|i|na|vo|à|au)\s+([a-zäöüčšžćđéèêëàâùûîïôœæñß]+)(?:\s+(\d{4}))?/
    const moM = tl.match(monthOnlyRe)
    if (moM) {
      const mIdx = matchMonth(moM[1])
      if (mIdx !== null) {
        const hasExplicitYear = Boolean(moM[2])
        const year = hasExplicitYear ? parseInt(moM[2]) : today.getFullYear()
        const d = new Date(year, mIdx, 1)
        if (!hasExplicitYear && d < today) d.setFullYear(today.getFullYear() + 1)
        return toLocalDateStr(d)
      }
    }

    if (THANKSGIVING_WEEK_RE.test(tl)) {
      const thanksgiving = getUpcomingUsThanksgiving(today)
      const weekStart = new Date(thanksgiving)
      const mondayOffset = (thanksgiving.getDay() + 6) % 7
      weekStart.setDate(thanksgiving.getDate() - mondayOffset)
      return toLocalDateStr(weekStart)
    }

    if (THANKSGIVING_RE.test(tl)) {
      return toLocalDateStr(getUpcomingUsThanksgiving(today))
    }

    // Relative: "next friday", "nächsten montag", etc.
    if (REL_WEEKEND_RE.test(tl)) {
      // Next Saturday
      const d = new Date(today)
      const diff = (6 - today.getDay() + 7) % 7 || 7
      d.setDate(today.getDate() + diff)
      return toLocalDateStr(d)
    }

    const isNext = REL_DATE_NEXT_RE.test(tl)
    const isThis = REL_DATE_THIS_RE.test(tl)

    const stripped2 = stripAccents(tl)
    for (const [name, dayIdx] of WEEKDAY_MAP) {
      if (stripped2.includes(stripAccents(name))) {
        const d = new Date(today)
        let diff = (dayIdx - today.getDay() + 7) % 7
        if (diff === 0) diff = 7   // "this Monday" when today is Monday → next Monday
        if (isNext) diff = diff === 0 ? 7 : diff + (diff <= 0 ? 7 : 0)
        if (isThis && diff === 0) diff = 0  // today
        d.setDate(today.getDate() + diff)
        return toLocalDateStr(d)
      }
    }

    // "tomorrow" / "morgen" / "demain" / "mañana" / "domani" / "jutro" / "imorgon"
    if (/\b(?:tomorrow|morgen|demain|mañana|manana|domani|jutro|imorgon|nesër|nese|sutra)\b/i.test(t)) {
      const d = new Date(today)
      d.setDate(today.getDate() + 1)
      return toLocalDateStr(d)
    }

    // "in X days/weeks"
    const inXM = tl.match(/\bin\s+(\d+)\s+(?:days?|dag[ae]?n?|jours?|giorni?|dias?|dagar|dana|ditë|dite)\b/)
    if (inXM) {
      const d = new Date(today)
      d.setDate(today.getDate() + parseInt(inXM[1]))
      return toLocalDateStr(d)
    }
    const inXWM = tl.match(/\bin\s+(\d+)\s+(?:weeks?|wochen?|semaines?|settimane?|semanas?|veckor|tjedana|javë|jave)\b/)
    if (inXWM) {
      const d = new Date(today)
      d.setDate(today.getDate() + parseInt(inXWM[1]) * 7)
      return toLocalDateStr(d)
    }

    return undefined
  }

  // ── Implicit round-trip scanner ───────────────────────────────────────────
  // Finds up to 2 distinct date expressions in left-to-right order.
  // Used when no explicit return keyword (e.g. "May 1st, May 6th", "May 1-6", "1 May - 6 May").
  function scanTwoDates(text: string): [string, string] | null {
    const cleaned = stripAccents(text.toLowerCase())
      .replace(/\b(?:on|le|am|el|il|em|dne|den|dia|på|na|the)\b/g, ' ')
      .replace(/\s+/g, ' ')

    const hits: Array<{ pos: number; date: string }> = []

    const addHit = (pos: number, mIdx: number, day: number) => {
      if (mIdx < 0 || mIdx > 11 || day < 1 || day > 31) return
      const d = new Date(today.getFullYear(), mIdx, day)
      if (d < today) d.setFullYear(today.getFullYear() + 1)
      hits.push({ pos, date: toLocalDateStr(d) })
    }

    let m: RegExpExecArray | null

    // Same-month range: "May 1-6", "May 1–6"
    const smRange1Re = /([a-zäöüčšžćđéèêëàâùûîïôœæñß]{3,})\s+(\d{1,2})\s*[-–]\s*(\d{1,2})(?!\d)/g
    while ((m = smRange1Re.exec(cleaned)) !== null) {
      const mIdx = matchMonth(m[1])
      const d1 = parseInt(m[2]), d2 = parseInt(m[3])
      if (mIdx !== null && d1 < d2) {
        addHit(m.index, mIdx, d1)
        addHit(m.index + m[0].length - 1, mIdx, d2)
      }
    }

    // Same-month range reversed: "1-6 May", "1–6 May"
    const smRange2Re = /(\d{1,2})\s*[-–]\s*(\d{1,2})\s+([a-zäöüčšžćđéèêëàâùûîïôœæñß]{3,})/g
    while ((m = smRange2Re.exec(cleaned)) !== null) {
      const mIdx = matchMonth(m[3])
      const d1 = parseInt(m[1]), d2 = parseInt(m[2])
      if (mIdx !== null && d1 < d2) {
        addHit(m.index, mIdx, d1)
        addHit(m.index + m[0].length - 1, mIdx, d2)
      }
    }

    // "<month> <day>" e.g. "May 1st", "May 6th"
    const mdRe = /([a-zäöüčšžćđéèêëàâùûîïôœæñß]{3,})\s+(\d{1,2})(?:st|nd|rd|th|er|ème|eme|[.º])?(?!\d)/g
    while ((m = mdRe.exec(cleaned)) !== null) {
      const mIdx = matchMonth(m[1])
      if (mIdx !== null) addHit(m.index, mIdx, parseInt(m[2]))
    }

    // "<day> <month>" e.g. "1st May", "6 May"
    const dmRe = /(\d{1,2})(?:st|nd|rd|th|er|ème|eme|[.º])?\.?\s+([a-zäöüčšžćđéèêëàâùûîïôœæñß]{3,})/g
    while ((m = dmRe.exec(cleaned)) !== null) {
      const mIdx = matchMonth(m[2])
      if (mIdx !== null) addHit(m.index, mIdx, parseInt(m[1]))
    }

    // Sort by position, deduplicate (merge hits within 8 chars)
    hits.sort((a, b) => a.pos - b.pos)
    const deduped: string[] = []
    let lastPos = -20
    for (const h of hits) {
      if (h.pos >= lastPos + 8) {
        deduped.push(h.date)
        lastPos = h.pos
      }
    }

    if (deduped.length >= 2 && deduped[0] !== deduped[1] && deduped[1] >= deduped[0]) {
      return [deduped[0], deduped[1]]
    }
    return null
  }

  // ── 4. Extract outbound date ─────────────────────────────────────────────
  result.date = extractDate(outboundRaw)

  // If no date found, default to 1 week from today
  if (!result.date) {
    const d = new Date(today)
    d.setDate(today.getDate() + 7)
    result.date = toLocalDateStr(d)
  }

  // ── 5. Extract return date ───────────────────────────────────────────────
  if (returnRaw) {
    result.return_date = extractDate(returnRaw)
  } else {
    // No explicit return keyword — scan for two date expressions (implicit round-trip)
    // Handles: "May 1st, May 6th" / "May 1-6" / "1 May - 6 May" / "May 1 to May 6"
    const pair = scanTwoDates(outboundRaw)
    if (pair) result.return_date = pair[1]
  }

  // ── 6. Extract cabin class + direct filter from full query ───────────────
  const cabin = extractCabin(q)
  if (cabin) result.cabin = cabin
  if (extractDirect(q)) result.stops = 0

  return result
}
