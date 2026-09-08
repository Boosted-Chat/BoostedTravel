/**
 * LetsFG — Agent-native flight search & booking SDK for Node.js/TypeScript.
 *
 * Server-side engine covers hundreds of airlines. Free search via PFS Bearer token
 * or prepaid Developer API. Zero external JS dependencies. Uses native fetch (Node 18+).
 *
 * @example
 * ```ts
 * import { LetsFG } from 'letsfg';
 *
 * // PFS (free, Bearer token from `letsfg auth`)
 * const bt = new LetsFG({ bearerToken: process.env.LETSFG_BEARER_TOKEN });
 * const flights = await bt.search('GDN', 'BER', '2026-03-03');
 *
 * // Developer API (look-to-book search: 200 free after every booking)
 * const bt2 = new LetsFG({ apiKey: 'letsfg_...' });
 * const flights2 = await bt2.search('LHR', 'JFK', '2026-04-15');
 * ```
 */

// ── Types ────────────────────────────────────────────────────────────────

export interface FlightSegment {
  airline: string;
  airline_name: string;
  flight_no: string;
  origin: string;
  destination: string;
  origin_city: string;
  destination_city: string;
  departure: string;
  arrival: string;
  duration_seconds: number;
  cabin_class: string;
  aircraft: string;
  /** Starlink Wi-Fi on this leg. 'confirmed' = the carrier has fitted every
   *  aircraft of this type. 'likely' = installation on this type is underway
   *  but incomplete, so this airframe may not have it. Undefined means no
   *  information — NOT an absence of Wi-Fi. */
  starlink?: StarlinkSegmentVerdict;
}

export type StarlinkSegmentVerdict = 'confirmed' | 'likely';

/** Itinerary-level Starlink verdict. '*_some' means at least one leg has none;
 *  'likely_*' means the rollout on that subfleet is underway but incomplete.
 *  Only 'confirmed_*' should be shown to an end user as a fact. */
export type StarlinkOfferVerdict =
  | 'confirmed_all'
  | 'confirmed_some'
  | 'likely_all'
  | 'likely_some';

export interface FlightRoute {
  segments: FlightSegment[];
  total_duration_seconds: number;
  stopovers: number;
}

export interface FlightOffer {
  id: string;
  price: number;
  currency: string;
  price_formatted: string;
  outbound: FlightRoute;
  inbound: FlightRoute | null;
  airlines: string[];
  owner_airline: string;
  bags_price: Record<string, number>;
  /** Starlink Wi-Fi across the whole itinerary; per-leg detail is on each
   *  segment's `starlink`. Undefined when no leg has any information. */
  starlink?: StarlinkOfferVerdict;
  availability_seats: number | null;
  conditions: Record<string, string>;
  is_locked: boolean;
  fetched_at: string;
  booking_url: string;
}

export interface FlightSearchResult {
  search_id: string;
  offer_request_id: string;
  passenger_ids: string[];
  origin: string;
  destination: string;
  currency: string;
  offers: FlightOffer[];
  total_results: number;
  search_params: Record<string, unknown>;
  pricing_note: string;
}

export interface UnlockResult {
  offer_id: string;
  unlock_status: string;
  payment_charged: boolean;
  payment_amount_cents: number;
  payment_currency: string;
  payment_intent_id: string;
  confirmed_price: number | null;
  confirmed_currency: string;
  offer_expires_at: string;
  message: string;
}

export interface Passenger {
  id: string;
  given_name: string;
  family_name: string;
  born_on: string;
  gender?: string;
  title?: string;
  email?: string;
  phone_number?: string;
}

export interface BookingResult {
  booking_id: string;
  status: string;
  booking_type: string;
  offer_id: string;
  flight_price: number;
  service_fee: number;
  service_fee_percentage: number;
  total_charged: number;
  currency: string;
  order_id: string;
  booking_reference: string;
  unlock_payment_id: string;
  fee_payment_id: string;
  created_at: string;
  details: Record<string, unknown>;
}

export interface SearchOptions {
  returnDate?: string;
  adults?: number;
  children?: number;
  infants?: number;
  cabinClass?: 'M' | 'W' | 'C' | 'F';
  maxStopovers?: number;
  currency?: string;
  limit?: number;
  sort?: 'price' | 'duration';
  departureTimeFrom?: string;
  departureTimeTo?: string;
}

export interface LetsFGConfig {
  /** PFS Bearer token from `letsfg auth`. Enables free search via POST /api/search polling. */
  bearerToken?: string;
  /** Developer API key. Look-to-book search; no booking fee, no transaction fee. */
  apiKey?: string;
  baseUrl?: string;
  timeout?: number;
}

// ── Error codes ───────────────────────────────────────────────────────────

export const ErrorCode = {
  // Transient (safe to retry after short delay)
  SUPPLIER_TIMEOUT: 'SUPPLIER_TIMEOUT',
  RATE_LIMITED: 'RATE_LIMITED',
  SERVICE_UNAVAILABLE: 'SERVICE_UNAVAILABLE',
  NETWORK_ERROR: 'NETWORK_ERROR',
  // Validation (fix input, then retry)
  INVALID_IATA: 'INVALID_IATA',
  INVALID_DATE: 'INVALID_DATE',
  INVALID_PASSENGERS: 'INVALID_PASSENGERS',
  UNSUPPORTED_ROUTE: 'UNSUPPORTED_ROUTE',
  MISSING_PARAMETER: 'MISSING_PARAMETER',
  INVALID_PARAMETER: 'INVALID_PARAMETER',
  // Business (requires human decision)
  AUTH_INVALID: 'AUTH_INVALID',
  PAYMENT_REQUIRED: 'PAYMENT_REQUIRED',
  PAYMENT_DECLINED: 'PAYMENT_DECLINED',
  OFFER_EXPIRED: 'OFFER_EXPIRED',
  OFFER_NOT_UNLOCKED: 'OFFER_NOT_UNLOCKED',
  FARE_CHANGED: 'FARE_CHANGED',
  ALREADY_BOOKED: 'ALREADY_BOOKED',
  BOOKING_FAILED: 'BOOKING_FAILED',
} as const;

export type ErrorCodeType = (typeof ErrorCode)[keyof typeof ErrorCode];

export const ErrorCategory = {
  TRANSIENT: 'transient',
  VALIDATION: 'validation',
  BUSINESS: 'business',
} as const;

export type ErrorCategoryType = (typeof ErrorCategory)[keyof typeof ErrorCategory];

const CODE_TO_CATEGORY: Record<string, ErrorCategoryType> = {
  [ErrorCode.SUPPLIER_TIMEOUT]: ErrorCategory.TRANSIENT,
  [ErrorCode.RATE_LIMITED]: ErrorCategory.TRANSIENT,
  [ErrorCode.SERVICE_UNAVAILABLE]: ErrorCategory.TRANSIENT,
  [ErrorCode.NETWORK_ERROR]: ErrorCategory.TRANSIENT,
  [ErrorCode.INVALID_IATA]: ErrorCategory.VALIDATION,
  [ErrorCode.INVALID_DATE]: ErrorCategory.VALIDATION,
  [ErrorCode.INVALID_PASSENGERS]: ErrorCategory.VALIDATION,
  [ErrorCode.UNSUPPORTED_ROUTE]: ErrorCategory.VALIDATION,
  [ErrorCode.MISSING_PARAMETER]: ErrorCategory.VALIDATION,
  [ErrorCode.INVALID_PARAMETER]: ErrorCategory.VALIDATION,
  [ErrorCode.AUTH_INVALID]: ErrorCategory.BUSINESS,
  [ErrorCode.PAYMENT_REQUIRED]: ErrorCategory.BUSINESS,
  [ErrorCode.PAYMENT_DECLINED]: ErrorCategory.BUSINESS,
  [ErrorCode.OFFER_EXPIRED]: ErrorCategory.BUSINESS,
  [ErrorCode.OFFER_NOT_UNLOCKED]: ErrorCategory.BUSINESS,
  [ErrorCode.FARE_CHANGED]: ErrorCategory.BUSINESS,
  [ErrorCode.ALREADY_BOOKED]: ErrorCategory.BUSINESS,
  [ErrorCode.BOOKING_FAILED]: ErrorCategory.BUSINESS,
};

function inferErrorCode(statusCode: number, detail: string): string {
  const d = detail.toLowerCase();
  if (statusCode === 401) return ErrorCode.AUTH_INVALID;
  if (statusCode === 402) return d.includes('declined') ? ErrorCode.PAYMENT_DECLINED : ErrorCode.PAYMENT_REQUIRED;
  if (statusCode === 410) return ErrorCode.OFFER_EXPIRED;
  if (statusCode === 422) {
    if (d.includes('iata') || d.includes('airport')) return ErrorCode.INVALID_IATA;
    if (d.includes('date')) return ErrorCode.INVALID_DATE;
    if (d.includes('passenger')) return ErrorCode.INVALID_PASSENGERS;
    if (d.includes('route')) return ErrorCode.UNSUPPORTED_ROUTE;
    return ErrorCode.INVALID_PARAMETER;
  }
  if (statusCode === 429) return ErrorCode.RATE_LIMITED;
  if (statusCode === 503) return ErrorCode.SERVICE_UNAVAILABLE;
  if (statusCode === 504) return ErrorCode.SUPPLIER_TIMEOUT;
  if (statusCode === 409) return ErrorCode.ALREADY_BOOKED;
  return statusCode >= 500 ? ErrorCode.BOOKING_FAILED : ErrorCode.INVALID_PARAMETER;
}

// ── Errors ────────────────────────────────────────────────────────────────

export class LetsFGError extends Error {
  statusCode: number;
  response: Record<string, unknown>;
  errorCode: string;
  errorCategory: ErrorCategoryType;
  isRetryable: boolean;

  constructor(message: string, statusCode = 0, response: Record<string, unknown> = {}, errorCode = '') {
    super(message);
    this.name = 'LetsFGError';
    this.statusCode = statusCode;
    this.response = response;
    this.errorCode = errorCode || (response.error_code as string) || '';
    this.errorCategory = CODE_TO_CATEGORY[this.errorCode] || ErrorCategory.BUSINESS;
    this.isRetryable = this.errorCategory === ErrorCategory.TRANSIENT;
  }
}

export class AuthenticationError extends LetsFGError {
  constructor(message: string, response: Record<string, unknown> = {}) {
    super(message, 401, response, ErrorCode.AUTH_INVALID);
    this.name = 'AuthenticationError';
  }
}

export class PaymentRequiredError extends LetsFGError {
  constructor(message: string, response: Record<string, unknown> = {}) {
    const code = message.toLowerCase().includes('declined') ? ErrorCode.PAYMENT_DECLINED : ErrorCode.PAYMENT_REQUIRED;
    super(message, 402, response, code);
    this.name = 'PaymentRequiredError';
  }
}

export class OfferExpiredError extends LetsFGError {
  constructor(message: string, response: Record<string, unknown> = {}) {
    super(message, 410, response, ErrorCode.OFFER_EXPIRED);
    this.name = 'OfferExpiredError';
  }
}

export class ValidationError extends LetsFGError {
  constructor(message: string, statusCode = 422, response: Record<string, unknown> = {}, errorCode = '') {
    super(message, statusCode, response, errorCode || ErrorCode.INVALID_PARAMETER);
    this.name = 'ValidationError';
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────

function routeStr(route: FlightRoute): string {
  if (!route.segments.length) return '';
  const codes = [route.segments[0].origin, ...route.segments.map(s => s.destination)];
  return codes.join(' -> ');
}

function durationHuman(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h${m.toString().padStart(2, '0')}m`;
}

/** One-line offer summary */
export function offerSummary(offer: FlightOffer): string {
  const route = routeStr(offer.outbound);
  const dur = durationHuman(offer.outbound.total_duration_seconds);
  const airline = offer.owner_airline || offer.airlines[0] || '?';
  return `${offer.currency} ${offer.price.toFixed(2)} | ${airline} | ${route} | ${dur} | ${offer.outbound.stopovers} stop(s)`;
}

/** Get cheapest offer from search results */
export function cheapestOffer(result: FlightSearchResult): FlightOffer | null {
  if (!result.offers.length) return null;
  return result.offers.reduce((min, o) => (o.price < min.price ? o : min), result.offers[0]);
}

// ── Client ────────────────────────────────────────────────────────────────

const DEFAULT_BASE_URL = 'https://letsfg.co';
// Poll fast and poll FIRST. The old loop slept 10s before its opening poll,
// putting a hard 10s floor under every search however fast the engine answered.
const PFS_POLL_INTERVAL_MS = 2_000;
const PFS_POLL_TIMEOUT_MS = 120_000;

// A search reports `completed` BEFORE its offer set stops growing: the split
// probe merges in late, so the cheapest itinerary on the search is routinely
// one that does not exist yet when the status turns terminal. The server
// publishes `split_ticket_pending` / `gf_enrich_pending` until it lands, and
// stopping at `completed` silently discards the cheaper offer.
//
// Free on most searches — the split probe is gated server-side and never fires
// on the majority of routes, so these flags are already false on poll one.
// How long to wait for it is NOT a guess: the server stamps a result as
// settled SETTLE_MS = 90s after it first reports `completed`, and that is the
// window in which it expects the set to still be growing. A measured
// GDN->SFO run had the split land at 51s. 45s would have given up ~6s short of
// the offer this whole feature exists to find, so the ceiling tracks the
// server's own settle window.
//
// Set LETSFG_WAIT_FOR_SPLIT=0 to skip the wait and take the fast answer.
const LATE_MERGE_POLL_MS = 3_000;
const LATE_MERGE_GRACE_MS = 90_000;
const WAIT_FOR_SPLIT = (process.env.LETSFG_WAIT_FOR_SPLIT || '').trim() !== '0';
const NON_TERMINAL = ['pending', 'searching'];

export class LetsFG {
  private bearerToken: string;
  private apiKey: string;
  private baseUrl: string;
  private timeout: number;

  constructor(config: LetsFGConfig = {}) {
    this.bearerToken = config.bearerToken || process.env.LETSFG_BEARER_TOKEN || '';
    this.apiKey = config.apiKey || process.env.LETSFG_API_KEY || '';
    this.baseUrl = (config.baseUrl || process.env.LETSFG_BASE_URL || DEFAULT_BASE_URL).replace(/\/$/, '');
    this.timeout = config.timeout || 30000;
  }

  private requireAuth(): void {
    if (!this.bearerToken && !this.apiKey) {
      throw new AuthenticationError(
        'Authentication required. Set bearerToken (from `letsfg auth`) or apiKey in config, ' +
        'or set LETSFG_BEARER_TOKEN / LETSFG_API_KEY env vars.'
      );
    }
  }

  private requireApiKey(): void {
    if (!this.apiKey) {
      throw new AuthenticationError(
        'Developer API key required for this operation. Set apiKey in config or LETSFG_API_KEY env var. ' +
        'Register at letsfg.co/developers.'
      );
    }
  }

  /** True when using PFS Bearer token (free search path) */
  private get usingPFS(): boolean {
    return !!this.bearerToken;
  }

  // ── Core methods ─────────────────────────────────────────────────────

  /**
   * Search for flights — FREE.
   *
   * Uses PFS (Bearer token) or Developer API (X-API-Key) depending on config.
   * PFS: async polling (POST /api/search -> poll /api/results/<id> every 10s).
   * Developer API: synchronous call.
   *
   * @param origin - IATA code (e.g., "GDN", "LON")
   * @param destination - IATA code (e.g., "BER", "BCN")
   * @param dateFrom - Departure date "YYYY-MM-DD"
   * @param options - Optional search parameters
   */
  async search(
    origin: string,
    destination: string,
    dateFrom: string,
    options: SearchOptions = {},
  ): Promise<FlightSearchResult> {
    this.requireAuth();

    const body: Record<string, unknown> = {
      origin: origin.toUpperCase(),
      destination: destination.toUpperCase(),
      date_from: dateFrom,
      adults: options.adults ?? 1,
      children: options.children ?? 0,
      currency: options.currency ?? 'EUR',
      limit: options.limit ?? 50,
    };
    if (options.returnDate) body.return_date = options.returnDate;
    if (options.cabinClass) body.cabin_class = options.cabinClass;
    if (options.maxStopovers != null) body.max_stopovers = options.maxStopovers;
    if (options.sort) body.sort = options.sort;
    if (options.departureTimeFrom) body.departure_time_from = options.departureTimeFrom;
    if (options.departureTimeTo) body.departure_time_to = options.departureTimeTo;

    if (this.usingPFS) {
      return this.searchPFS(body);
    }
    return this.post<FlightSearchResult>('/developers/api/v1/flights/search', body);
  }

  /** PFS path: POST /api/search -> poll /api/results/<id> */
  private async searchPFS(body: Record<string, unknown>): Promise<FlightSearchResult> {
    const { search_id } = await this.postWithBearer<{ search_id: string }>('/api/search', body);

    type PollResult = FlightSearchResult & {
      status?: string;
      split_ticket_pending?: boolean;
      gf_enrich_pending?: boolean;
    };
    const poll = () => this.getNoAuth<PollResult>(`/api/results/${search_id}`);
    const inbound = (r: PollResult) => Boolean(r.split_ticket_pending || r.gf_enrich_pending);

    const deadline = Date.now() + PFS_POLL_TIMEOUT_MS;
    let terminal: PollResult | null = null;

    // Poll immediately, then on the interval, until the search stops reporting
    // itself in-progress. (Only 'completed' etc. is terminal — see PR #165.)
    while (Date.now() < deadline) {
      const result = await poll();
      if (!NON_TERMINAL.includes(result.status as string)) { terminal = result; break; }
      await new Promise(r => setTimeout(r, PFS_POLL_INTERVAL_MS));
    }
    if (!terminal) {
      throw new LetsFGError('Search timed out after 120s. Try polling /api/results/<id> directly.', 504);
    }

    // Terminal, but maybe still growing. Bounded wait: a flag that never clears
    // must not hang the caller.
    const lateDeadline = Date.now() + LATE_MERGE_GRACE_MS;
    while (WAIT_FOR_SPLIT && inbound(terminal) && Date.now() < lateDeadline) {
      await new Promise(r => setTimeout(r, LATE_MERGE_POLL_MS));
      const merged = await poll();
      if (!NON_TERMINAL.includes(merged.status as string)) terminal = merged;
    }
    return terminal as FlightSearchResult;
  }

  /**
   * Resolve a city/airport name to IATA codes.
   *
   * Developer API key only. There is no location endpoint on the PFS Bearer
   * lane — the same dead end `unlock()` documents below. This used to send
   * PFS callers to `/api/locations?q=`, a route that has never existed on
   * letsfg.co (verified 2026-08-16: 404, text/html), so they got a JSON parse
   * error off the 404 page instead of an answer. Pass an IATA code directly
   * on the PFS lane; a city code expands to every airport in that city.
   */
  async resolveLocation(query: string): Promise<Array<Record<string, unknown>>> {
    this.requireApiKey();
    const path = `/developers/api/v1/flights/locations/${encodeURIComponent(query)}`;
    const data = await this.getWithAuth<Array<Record<string, unknown>> | { locations: Array<Record<string, unknown>> }>(path);
    return Array.isArray(data) ? data : (data as { locations: Array<Record<string, unknown>> }).locations || [];
  }

  /**
   * RETIRED 2026-09-08. Throws instead of calling the server.
   *
   * There is no unlock step on any lane. Unlock existed to confirm a live price
   * before charging; booking now HOLDS the fare on the connected payment method
   * and captures only once a real airline PNR exists, so a fare that moved
   * cannot become a charge for a ticket you did not get. If it moves at
   * checkout you get a `price_change` question to accept or decline instead.
   *
   * Kept as a method, and throwing locally rather than making the request, so an
   * older caller gets one clear sentence at the line that is actually wrong —
   * not a 410 body to decode, and not a TypeError somewhere else.
   */
  async unlock(_offerId: string): Promise<UnlockResult> {
    throw new LetsFGError(
      'unlock() was retired on 2026-09-08 and the endpoint answers 410 Gone. There is no unlock ' +
        'step: call book() directly. The fare is held on the connected payment method and ' +
        'captured only against a real airline PNR. See https://letsfg.co/developers/api/docs',
      410,
    );
  }

  /**
   * Book a flight.
   *
   * PFS (Bearer token): POST /api/agent-book. Free — nothing charged beyond
   * the ticket price. Pass searchId (from search()'s result). Returns either
   * { ok, booked: true, order_id } or, when the booking genuinely could not
   * complete, { ok, booked: false, booking_url } — hand the link to the user,
   * nothing was charged.
   *
   * Developer API (X-API-Key): POST /flights/book. NO unlock step. searchId is
   * REQUIRED — an offer is bookable only inside the search that produced it.
   * The connected Revolut method is HELD, not charged; a LetsFG booking agent
   * buys the ticket and the hold is captured only against a real airline PNR.
   * Returns the 202 { ok, booking_id, state, held, charged: 0, poll_url } —
   * poll getBooking(bookingId) until `terminal`, or use bookAndWait().
   * Always provide idempotencyKey: a retry with the same key returns the
   * existing booking instead of opening a second hold on the card.
   */
  async book(
    offerId: string,
    passengers: Passenger[],
    contactEmail: string,
    contactPhone = '',
    idempotencyKey = '',
    searchId = '',
  ): Promise<BookingResult | Record<string, unknown>> {
    this.requireAuth();

    if (this.usingPFS) {
      if (!searchId) {
        throw new LetsFGError(
          'searchId is required to book via PFS — pass the search_id from search()\'s result.',
          400,
        );
      }
      const passenger = { ...passengers[0] } as Record<string, unknown>;
      if (contactPhone && !passenger.phone_number) passenger.phone_number = contactPhone;
      return this.postWithBearer<Record<string, unknown>>('/api/agent-book', {
        search_id: searchId,
        offer_id: offerId,
        passenger,
        contact_email: contactEmail,
      });
    }

    this.requireApiKey();
    if (!searchId) {
      throw new LetsFGError(
        'searchId is required to book on the Developer API — pass the search_id from search()\'s ' +
          'result. An offer can only be booked inside the search that produced it. (Before ' +
          '2026-09-08 this argument was ignored on this path.)',
        400,
      );
    }
    const pax = passengers.map((p) => ({ ...p })) as Array<Record<string, unknown>>;
    if (contactPhone && pax.length && !pax[0].phone_number) pax[0].phone_number = contactPhone;
    const body: Record<string, unknown> = {
      search_id: searchId,
      offer_id: offerId,
      passengers: pax,
      contact_email: contactEmail,
    };
    if (idempotencyKey) body.idempotency_key = idempotencyKey;
    return this.post<Record<string, unknown>>('/developers/api/v1/flights/book', body);
  }

  /**
   * Poll a Developer API flight booking.
   *
   * Poll every few seconds until `terminal` is true. The poll is ALSO how LetsFG
   * knows you are still there, which is what keeps a booking paused on a
   * question alive — so do not back off to minutes.
   *
   * States: authorised, card_issued, booking_in_progress, awaiting_settlement,
   * then completed (with `pnr` and `charged_amount`), failed (hold released,
   * nothing charged) or needs_attention (a human at LetsFG is on it — do not
   * book again).
   */
  async getBooking(bookingId: string): Promise<Record<string, unknown>> {
    this.requireApiKey();
    return this.getWithAuth<Record<string, unknown>>(
      `/developers/api/v1/flights/bookings/${encodeURIComponent(bookingId)}`,
    );
  }

  /**
   * Answer the open `question` on a booking.
   *
   * Echo the question's `round`. A stale round is refused with 409 rather than
   * guessed at, so an answer to an old question can never be applied to a new
   * one. Seat: { seats: [...] } or { skip: true }. Price change or paid extra:
   * { confirm: true } or { skip: true } — declining an extra still completes
   * the booking, without it.
   */
  async answerBooking(
    bookingId: string,
    round: number,
    answer: { seats?: Array<Record<string, unknown>>; confirm?: boolean; skip?: boolean } = {},
  ): Promise<Record<string, unknown>> {
    this.requireApiKey();
    return this.post<Record<string, unknown>>(
      `/developers/api/v1/flights/bookings/${encodeURIComponent(bookingId)}/answer`,
      { round, ...answer },
    );
  }

  /**
   * Book and poll to a terminal state. Mirrors bookHotelAndWait().
   *
   * Blocks for as long as the booking takes (4–11 minutes typically), so use
   * book() + getBooking() instead if your caller has a request timeout.
   *
   * `onQuestion` returns the answer for answerBooking(). Without it, a fare
   * increase is ACCEPTED and a paid extra is DECLINED — the conservative
   * reading of "the traveller asked for this flight".
   */
  async bookAndWait(
    offerId: string,
    passengers: Passenger[],
    contactEmail: string,
    searchId: string,
    opts: {
      contactPhone?: string;
      idempotencyKey?: string;
      pollMs?: number;
      timeoutMs?: number;
      onQuestion?: (q: Record<string, unknown>) => { seats?: Array<Record<string, unknown>>; confirm?: boolean; skip?: boolean };
    } = {},
  ): Promise<Record<string, unknown>> {
    const { contactPhone = '', idempotencyKey = '', pollMs = 5000, timeoutMs = 900000, onQuestion } = opts;
    const started = (await this.book(
      offerId, passengers, contactEmail, contactPhone, idempotencyKey, searchId,
    )) as Record<string, unknown>;
    if (!started || started.ok !== true) return started; // a refusal: nothing was charged

    const bookingId = String(started.booking_id);
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, pollMs));
      const state = await this.getBooking(bookingId);
      const question = state.question as Record<string, unknown> | null | undefined;
      if (question) {
        const answer = onQuestion
          ? onQuestion(question)
          : question.kind === 'extra'
            ? { skip: true }
            : { confirm: true };
        await this.answerBooking(bookingId, Number(question.round), answer);
        continue;
      }
      if (state.terminal) return state;
    }
    return this.getBooking(bookingId);
  }

  // ── Hotels ──────────────────────────────────────────────────────────
  //
  // A card on file is required for EVERY hotel call, search included. That is
  // deliberate: a hotel search opens a real session at the supplier and booking
  // blocks a real rate, so a caller is never allowed to reach the point of
  // commitment only to discover it cannot pay. The same card that authorises
  // flight booking authorises hotels — there is no separate hotel enrolment.
  //
  // Only free-cancellation, pay-later rates are sold. Those are the rates where
  // the guest's balance can safely be settled with the supplier after booking,
  // which is what makes 5%-now/rest-later work. The result set is smaller than
  // a metasearch's, and every row in it can actually be booked.

  /**
   * Resolve a place name to the city id that searchHotels() needs.
   *
   * Use `Id` from the first result as `cityId` and `Name` as `cityName`.
   */
  async hotelDestinations(text: string): Promise<Array<Record<string, unknown>>> {
    this.requireApiKey();
    const data = await this.post<{ results?: Array<Record<string, unknown>> }>(
      '/developers/api/v1/hotels/destinations', { text }, 60_000);
    return data.results ?? [];
  }

  /**
   * Search real, bookable hotel inventory.
   *
   * Slow by nature — the supplier streams a whole city and every rate is priced
   * — so this gets its own generous timeout rather than the client default.
   *
   * Each offer carries `price` (what the guest pays), `reservation_fee_now`
   * (the 5% taken at booking), `balance_to_supplier`, `balance_due_by` and
   * `free_cancellation_until`. There is no wholesale figure to quote by mistake.
   *
   * Keep `session_id` and the chosen offer's `combination_id_v2`: together they
   * identify the exact rate, and booking needs both.
   */
  async searchHotels(params: {
    cityId: number;
    cityName: string;
    checkIn: string;
    checkOut: string;
    adults?: number;
    children?: number;
    childAges?: number[];
    /** Two-letter code. Rates and taxes genuinely differ by nationality. */
    nationality?: string;
    limit?: number;
    withImages?: boolean;
  }): Promise<Record<string, unknown>> {
    this.requireApiKey();
    const body: Record<string, unknown> = {
      city_id: params.cityId,
      city_name: params.cityName,
      check_in: params.checkIn,
      check_out: params.checkOut,
      adults: params.adults ?? 2,
      children: params.children ?? 0,
      nationality: params.nationality ?? 'PL',
      limit: params.limit ?? 40,
      with_images: params.withImages ?? true,
    };
    if (params.childAges?.length) body.child_ages = params.childAges;
    return this.post<Record<string, unknown>>('/developers/api/v1/hotels/search', body, 240_000);
  }

  /**
   * Start a booking. Returns a job immediately — it does NOT book inline.
   *
   * A booking takes minutes: the rate is re-blocked at the supplier, every
   * price and date rail is checked, the 5% reservation fee is charged to your
   * card, and only then is the room committed. No proxy holds a connection that
   * long, so this returns at once and you poll hotelBooking() for the outcome.
   * Use bookHotelAndWait() if you would rather block.
   *
   * Because the fee is taken BEFORE the commit, a declined card costs nothing
   * to unwind: no reservation exists and nothing is charged.
   *
   * Send `expectedPrice` and `expectedBalance` back exactly as search returned
   * them — the booking is refused if the supplier has moved beyond tolerance,
   * so a guest is never charged a price they did not agree to.
   *
   * Do NOT call this again for the same rate while a job is running: that books
   * the room twice and charges two reservation fees.
   */
  async bookHotel(params: {
    sessionId: string;
    hotelCode: number;
    combinationIdV2: string;
    expectedPrice: number;
    expectedBalance: number;
    cityId: number;
    cityName: string;
    checkIn: string;
    checkOut: string;
    guests: Array<{ title: string; first_name: string; last_name: string }>;
    /** The voucher and the pay link go here. A typo loses the booking. */
    email: string;
    phone: string;
    adults?: number;
    combinationId?: number;
    hotelName?: string;
    phoneCountryCode?: string;
    specialRequests?: string[];
  }): Promise<Record<string, unknown>> {
    this.requireApiKey();
    const body: Record<string, unknown> = {
      session_id: params.sessionId,
      hotel_code: params.hotelCode,
      combination_id_v2: params.combinationIdV2,
      expected_price: params.expectedPrice,
      expected_balance: params.expectedBalance,
      city_id: params.cityId,
      city_name: params.cityName,
      check_in: params.checkIn,
      check_out: params.checkOut,
      adults: params.adults ?? 2,
      guests: params.guests,
      email: params.email,
      phone: params.phone,
      phone_country_code: params.phoneCountryCode ?? '48',
      special_requests: params.specialRequests ?? [],
    };
    if (params.combinationId != null) body.combination_id = params.combinationId;
    if (params.hotelName) body.hotel_name = params.hotelName;
    return this.post<Record<string, unknown>>('/developers/api/v1/hotels/book', body, 90_000);
  }

  /**
   * Collect the result of a booking started with bookHotel().
   *
   * `status` is 'in_progress', 'succeeded' or 'failed'. On success you get
   * `confirmation`, `reservation_fee_charged`, `pay_link`, `balance_due`,
   * `balance_due_by` and `terms` (including the full cancellation ladder).
   */
  async hotelBooking(bookingJobId: string): Promise<Record<string, unknown>> {
    this.requireApiKey();
    return this.get<Record<string, unknown>>(
      `/developers/api/v1/hotels/booking/${encodeURIComponent(bookingJobId)}`, 60_000);
  }

  /**
   * bookHotel(), then poll until the booking settles. Convenience only.
   *
   * Giving up after `maxWaitMs` does NOT cancel anything — the booking may
   * still complete. The returned object carries `booking_job_id` so you can
   * keep polling, and the confirmation is emailed to the guest regardless.
   */
  async bookHotelAndWait(
    params: Parameters<LetsFG['bookHotel']>[0] & { pollIntervalMs?: number; maxWaitMs?: number },
  ): Promise<Record<string, unknown>> {
    const pollIntervalMs = params.pollIntervalMs ?? 20_000;
    const maxWaitMs = params.maxWaitMs ?? 600_000;
    const job = await this.bookHotel(params);
    const jobId = job.booking_job_id as string | undefined;
    if (!jobId) return job;

    let waited = 0;
    let result: Record<string, unknown> = job;
    while (waited < maxWaitMs) {
      await new Promise((r) => setTimeout(r, pollIntervalMs));
      waited += pollIntervalMs;
      result = await this.hotelBooking(jobId);
      const st = result.status;
      if (st === 'succeeded' || st === 'failed') return result;
    }
    if (result.booking_job_id == null) result.booking_job_id = jobId;
    return result;
  }

  /**
   * Release a reservation at the supplier.
   *
   * Free until `balance_due_by`; after that the hotel's own cancellation ladder
   * applies and can reach 100%. The ladder ships in the booking's `terms`, so
   * you can see the cost before calling this. The 5% reservation fee is NOT
   * refunded.
   *
   * This drives a browser at the supplier and takes over a minute. If it times
   * out, do not assume it failed — re-check before retrying.
   */
  async cancelHotel(confirmation: string): Promise<Record<string, unknown>> {
    this.requireApiKey();
    return this.post<Record<string, unknown>>(
      '/developers/api/v1/hotels/cancel', { confirmation }, 300_000);
  }

  /**
   * [Developer API] Mint a one-time link for connecting a Revolut payment method.
   *
   * This replaced setupPayment() on 2026-09-08. Nothing is charged to connect, and
   * card details never touch LetsFG: the returned `connect_url` opens a hosted page
   * where the developer saves a card, Revolut Pay or Google Pay. A PERSON must open
   * it in a browser — there is no endpoint that takes card details, so do not ask a
   * user for a card number and do not try to automate this step.
   *
   * Most agents should NOT need a Developer API account at all. To authenticate for
   * search and booking, run `letsfg auth`, which creates no billing account.
   */
  async connectPayment(): Promise<Record<string, unknown>> {
    this.requireApiKey();
    return this.post<Record<string, unknown>>('/developers/api/v1/agents/connect-payment', {});
  }

  /**
   * RETIRED 2026-09-08 with Stripe. Throws instead of calling the server.
   *
   * `/agents/setup-payment` answers 410 Gone. Payment enrolment moved onto the same
   * Revolut rail as the rest of the product: call connectPayment() and open the
   * `connect_url` it returns.
   *
   * Kept as a method, and throwing locally rather than making the request, for the
   * same reason as unlock() — an older caller gets one clear sentence at the line that
   * is actually wrong, not a 410 body to decode and not a TypeError somewhere else.
   */
  async setupPayment(_token?: string): Promise<Record<string, unknown>> {
    throw new LetsFGError(
      'setupPayment() was retired on 2026-09-08 with Stripe and the endpoint answers 410 Gone. ' +
        'Call connectPayment() instead and open the connect_url it returns; nothing is charged ' +
        'to connect. See https://letsfg.co/developers/api/docs',
      410,
    );
  }

  /**
   * Get current agent profile and usage stats.
   */
  async me(): Promise<Record<string, unknown>> {
    this.requireApiKey();
    return this.get<Record<string, unknown>>('/developers/api/v1/agents/me');
  }

  // ── Static methods ───────────────────────────────────────────────────

  /**
   * [Developer API only] Create a PAID Developer API account with its own billing.
   *
   * Most agents should NOT call this — it creates a billing account you probably
   * do not want. To search and book flights, run `letsfg auth` instead.
   */
  static async register(
    agentName: string,
    email: string,
    baseUrl?: string,
    ownerName = '',
    description = '',
  ): Promise<Record<string, unknown>> {
    const url = (baseUrl || DEFAULT_BASE_URL).replace(/\/$/, '');
    const resp = await fetch(`${url}/developers/api/v1/agents/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'User-Agent': 'LetsFG-js/0.1.0' },
      body: JSON.stringify({ agent_name: agentName, email, owner_name: ownerName, description }),
    });

    const data = await resp.json();
    if (!resp.ok) {
      throw new LetsFGError(
        (data as Record<string, string>).detail || `Registration failed (${resp.status})`,
        resp.status,
        data as Record<string, unknown>,
      );
    }
    return data as Record<string, unknown>;
  }

  // ── Internal ────────────────────────────────────────────────────────

  private async postWithBearer<T>(path: string, body: Record<string, unknown>): Promise<T> {
    return this.requestWithHeaders<T>(path, 'POST', { 'Authorization': `Bearer ${this.bearerToken}` }, body);
  }

  private async getNoAuth<T>(path: string): Promise<T> {
    return this.requestWithHeaders<T>(path, 'GET', {});
  }

  private async getWithAuth<T>(path: string): Promise<T> {
    const headers: Record<string, string> = this.usingPFS
      ? { 'Authorization': `Bearer ${this.bearerToken}` }
      : { 'X-API-Key': this.apiKey };
    return this.requestWithHeaders<T>(path, 'GET', headers);
  }

  private async postWithAuth<T>(path: string, body: Record<string, unknown>): Promise<T> {
    const headers: Record<string, string> = this.usingPFS
      ? { 'Authorization': `Bearer ${this.bearerToken}` }
      : { 'X-API-Key': this.apiKey };
    return this.requestWithHeaders<T>(path, 'POST', headers, body);
  }

  private async post<T>(path: string, body: Record<string, unknown>, timeoutMs?: number): Promise<T> {
    return this.requestWithHeaders<T>(path, 'POST', { 'X-API-Key': this.apiKey }, body, timeoutMs);
  }

  private async get<T>(path: string, timeoutMs?: number): Promise<T> {
    return this.requestWithHeaders<T>(path, 'GET', { 'X-API-Key': this.apiKey }, undefined, timeoutMs);
  }

  private async requestWithHeaders<T>(
    path: string,
    method: string,
    extraHeaders: Record<string, string>,
    body?: Record<string, unknown>,
    timeoutMs?: number,
  ): Promise<T> {
    const controller = new AbortController();
    // Per-call timeout: one number cannot serve every endpoint here. A flight
    // search answers in seconds, a hotel search streams a whole city, and a
    // cancellation drives a browser at the supplier.
    const timer = setTimeout(() => controller.abort(), timeoutMs ?? this.timeout);

    try {
      const resp = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'LetsFG-js/0.1.0',
          'X-Client-Type': 'js-sdk',
          ...extraHeaders,
        },
        ...(body != null ? { body: JSON.stringify(body) } : {}),
        signal: controller.signal,
      });

      const data = await resp.json();

      if (!resp.ok) {
        const detail = (data as Record<string, string>).detail || `API error (${resp.status})`;
        const code = (data as Record<string, string>).error_code || inferErrorCode(resp.status, detail);
        if (resp.status === 401) throw new AuthenticationError(detail, data as Record<string, unknown>);
        if (resp.status === 402) throw new PaymentRequiredError(detail, data as Record<string, unknown>);
        if (resp.status === 410) throw new OfferExpiredError(detail, data as Record<string, unknown>);
        if (resp.status === 422) throw new ValidationError(detail, resp.status, data as Record<string, unknown>, code);
        throw new LetsFGError(detail, resp.status, data as Record<string, unknown>, code);
      }

      return data as T;
    } finally {
      clearTimeout(timer);
    }
  }
}

export default LetsFG;

// Backward-compat aliases (deprecated)
export const BoostedTravel = LetsFG;
export const BoostedTravelError = LetsFGError;
export type BoostedTravelConfig = LetsFGConfig;

// Re-export open-source ranking engine
export { rankOffers, deduplicateOffers, selectDiverseTop, getProfileLabel } from './ranking';
export type { RankOffer, RankingContext, RankedOffer, ScoreBreakdown } from './ranking';
export { extractOfferDetailSignals, getOfferDetailBadges, getOfferDetailPromptNotes } from './offer-details';
export type { OfferDetailSignals } from './offer-details';
export { normalizeTripPurposes, getPrimaryTripPurpose, TRIP_PURPOSES } from './trip-purpose';
export type { TripPurpose, TripPurposeOptions } from './trip-purpose';
