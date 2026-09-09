#!/usr/bin/env node
/**
 * LetsFG MCP Server — Model Context Protocol integration.
 *
 * All search runs server-side at letsfg.co — no local browsers or Python required.
 * Authenticate once: `letsfg auth` (zero-amount card setup, nothing charged) sets
 * LETSFG_BEARER_TOKEN,
 * or use a Developer API key (LETSFG_API_KEY): look-to-book search, plus hotels.
 *
 * Usage in Claude Desktop / Cursor config:
 * {
 *   "mcpServers": {
 *     "letsfg": {
 *       "command": "npx",
 *       "args": ["-y", "letsfg-mcp"],
 *       "env": {
 *         "LETSFG_BEARER_TOKEN": "eyJ..."
 *       }
 *     }
 *   }
 * }
 */

import * as readline from 'readline';

// ── Config ──────────────────────────────────────────────────────────────

const BASE_URL = (process.env.LETSFG_BASE_URL || 'https://letsfg.co').replace(/\/$/, '');
const BEARER_TOKEN = process.env.LETSFG_BEARER_TOKEN || '';
const API_KEY = process.env.LETSFG_API_KEY || '';
const VERSION = '1.3.1';

// The bare token `letsfg-mcp/1.3.0` was being challenged by the edge in front of
// letsfg.co from datacenter/VPS IPs — which is where MCP servers live — while a
// request from the SAME host with a `Mozilla/5.0 (compatible; …)` UA got a 200
// (issue #206, measured by the reporter on their own box, twice).
//
// This is the conventional form for a well-behaved automated client — the shape
// Googlebot, Bingbot and every other declared crawler uses, and the shape bot
// heuristics are tuned to accept. It is not a disguise: the product, the version
// and a contact URL are all still in the string, so we remain as identifiable in
// a log as before, and `X-Client-Type: mcp` still names us outright.
//
// Keep the `letsfg-mcp/<version>` token in it. Any server-side allowlist keyed on
// this client matches on that substring, not on the whole string.
const USER_AGENT = `Mozilla/5.0 (compatible; letsfg-mcp/${VERSION}; +https://github.com/LetsFG/LetsFG)`;

// Poll fast and poll FIRST. The old loop slept 10s before its opening poll,
// which put a hard 10s floor under every search however fast the engine
// answered — the speedup was real and no caller could ever see it.
const PFS_POLL_INTERVAL_MS = 2_000;
const PFS_POLL_TIMEOUT_MS = 120_000;

// A search reports `completed` BEFORE its offer set stops growing. The split
// probe fires two extra connector fan-outs and merges its result in late, so
// the cheapest itinerary on the whole search is routinely one that does not
// exist yet at the moment the status turns terminal. The server says so:
// `split_ticket_pending` (and `gf_enrich_pending` for the Google Flights
// enrich) stay true until that merge lands.
//
// Stopping at `completed` is therefore how you silently discard the cheapest
// offer. Keep polling while either flag is set.
//
// This costs nothing on the vast majority of searches: the split probe is
// gated server-side and never fires on most routes, so the flags are already
// false on the first poll and this loop exits immediately.
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
const lateMergeInbound = (r: Record<string, unknown>): boolean =>
  Boolean(r.split_ticket_pending) || Boolean(r.gf_enrich_pending);

// ── Request headers ─────────────────────────────────────────────────────

// Every request in this file goes through this one helper. That is the whole
// point of it existing: issue #163 was a missing User-Agent, fixed once in the
// Python SDK's auth path, and came straight back on its search path — because
// the fix patched a call site instead of a helper. This module had the same
// split (`X-Client-Type: mcp` was on the generic API client but on NEITHER the
// search POST nor its results poll), so any server-side rule keyed on that
// header applied to every tool EXCEPT the two that carry a real search.
//
// LETSFG_USER_AGENT overrides the UA. It exists because a client cannot fix an
// edge/WAF rule it is on the wrong side of, and waiting for one to be changed
// is not a workaround a user can apply themselves (#206).
function letsfgHeaders(opts: { json?: boolean; auth?: boolean } = {}): Record<string, string> {
  const headers: Record<string, string> = {
    'User-Agent': (process.env.LETSFG_USER_AGENT || '').trim() || USER_AGENT,
    'X-Client-Type': 'mcp',
  };
  if (opts.json) headers['Content-Type'] = 'application/json';
  // `auth: false` is for the enrollment call, which is how a caller GETS a
  // credential — sending a stale one there could only ever confuse it.
  if (opts.auth !== false) {
    if (BEARER_TOKEN) headers['Authorization'] = `Bearer ${BEARER_TOKEN}`;
    else if (API_KEY) headers['X-API-Key'] = API_KEY;
  }
  return headers;
}

// A challenge/error page is HTML, and every caller here wants JSON. Turning
// "<!DOCTYPE html>…" into `SyntaxError: Unexpected token '<'` destroys the only
// two facts the agent could act on: the status, and that something in front of
// the API — not the API — answered. Name that explicitly.
function describeNonJsonResponse(status: number, body: string, path: string): string {
  const looksLikeHtml = /^\s*<(?:!doctype|html)/i.test(body);
  const looksLikeChallenge = looksLikeHtml
    && /just a moment|cf-browser-verification|challenge-platform|attention required/i.test(body);

  if (looksLikeChallenge) {
    return `HTTP ${status} from ${path}: an anti-bot challenge page was returned instead of the API. `
      + `This is the network path in front of letsfg.co answering, not the search itself — a valid token cannot get past it. `
      + `It is most often seen from datacenter/VPS IPs. Set LETSFG_USER_AGENT to override the client User-Agent, `
      + `or report the host and egress IP at https://github.com/LetsFG/LetsFG/issues.`;
  }
  if (looksLikeHtml) {
    return `HTTP ${status} from ${path}: got an HTML page where JSON was expected — the request did not reach the API.`;
  }
  return `HTTP ${status} from ${path}: non-JSON response (${body.slice(0, 120)})`;
}

/** Read a response as JSON, or return an actionable error object — never throw. */
async function readJson(resp: Response, path: string): Promise<Record<string, unknown>> {
  const raw = await resp.text();
  let data: Record<string, unknown> | null = null;
  try {
    data = raw ? JSON.parse(raw) as Record<string, unknown> : {};
  } catch {
    return { error: true, status_code: resp.status, detail: describeNonJsonResponse(resp.status, raw, path) };
  }
  if (!resp.ok) {
    const detail = (data.detail as string) || (data.error as string) || `HTTP ${resp.status}`;
    return { error: true, status_code: resp.status, detail };
  }
  return data;
}

// ── Cloud Search (PFS Bearer token path) ───────────────────────────────

async function searchPFS(params: Record<string, unknown>): Promise<Record<string, unknown>> {
  const resp = await fetch(`${BASE_URL}/api/search`, {
    method: 'POST',
    headers: letsfgHeaders({ json: true }),
    body: JSON.stringify(params),
  });

  const started = await readJson(resp, '/api/search');
  if (started.error) return started;

  const { search_id } = started as unknown as { search_id: string };

  // Distinguishes "the search isn't finished yet" from "we are being turned
  // away". The old poll returned bare `null` for BOTH, so a 403 that started
  // mid-search burned the entire 120s timeout and then reported it as a
  // timeout — the one diagnosis guaranteed to send the user looking in the
  // wrong place. A poll that is being REFUSED will still be refused in 2s.
  let pollRefusal: Record<string, unknown> | null = null;

  const poll = async (): Promise<Record<string, unknown> | null> => {
    const pollResp = await fetch(`${BASE_URL}/api/results/${search_id}`, {
      // Carry the token on the poll too, not just the POST: results are the
      // agent's own, and the token is what buckets rate limiting to this agent.
      headers: letsfgHeaders(),
    });
    const data = await readJson(pollResp, `/api/results/${search_id}`);
    if (data.error) {
      // 5xx and 429 are transient — a search in flight legitimately produces
      // them, and giving up on the first one would throw away a live search.
      // 401/403 are a verdict on the caller, and re-asking cannot change it.
      const status = data.status_code as number | undefined;
      if (status === 401 || status === 403) pollRefusal = data;
      return null;
    }
    return data;
  };

  const deadline = Date.now() + PFS_POLL_TIMEOUT_MS;
  let terminal: Record<string, unknown> | null = null;

  // Phase 1 — poll immediately, then every PFS_POLL_INTERVAL_MS, until the API
  // stops reporting the search as in-progress.
  while (Date.now() < deadline) {
    const result = await poll();
    if (result && !NON_TERMINAL.includes(result.status as string)) { terminal = result; break; }
    if (pollRefusal) return pollRefusal;
    await new Promise(r => setTimeout(r, PFS_POLL_INTERVAL_MS));
  }
  if (!terminal) return { error: true, detail: 'Search timed out after 120s.' };

  // Phase 2 — terminal, but possibly still growing. Wait out the late merge,
  // bounded: a flag that never clears must not hang the agent, so the grace is
  // a ceiling and not a condition.
  const lateDeadline = Date.now() + LATE_MERGE_GRACE_MS;
  while (WAIT_FOR_SPLIT && lateMergeInbound(terminal) && Date.now() < lateDeadline) {
    await new Promise(r => setTimeout(r, LATE_MERGE_POLL_MS));
    const merged = await poll();
    if (merged && !NON_TERMINAL.includes(merged.status as string)) terminal = merged;
  }
  return terminal;
}

// ── API Client ──────────────────────────────────────────────────────────

// A booking pauses on the airline's own seat map, on a paid extra the run has just
// priced, or on a fare that moved. Seats and extras arrive from DIFFERENT collections
// and each clears as it is answered, so a booking can be waiting on BOTH - poll both.
// A price change travels on the extras channel carrying a `price_change` field.
// Mirrors the hosted MCP's _open_question (api/routers/mcp.py).
type BookingQuestion = {
  kind: 'seat' | 'extra';
  question?: string;
  round?: unknown;
  expires_at_ms?: unknown;
  seconds_remaining: number | null;
  [k: string]: unknown;
};

/** The free seats a model can actually put to a traveller, cheapest first.
 *
 * A price of null is NOT free. Seats have harvested unpriced from sellers that
 * charge 26-50 EUR for them, and a traveller picking a "free" seat and being
 * charged is the failure this refuses to enable: unpriced seats are listed
 * separately and labelled as such, never quoted at zero.
 */
function seatOptions(seatMap: unknown, limit = 40): Record<string, unknown> {
  const rowsRaw = Array.isArray(seatMap)
    ? seatMap
    : ((seatMap as Record<string, unknown>)?.seats ?? (seatMap as Record<string, unknown>)?.rows ?? []);
  const cells: Record<string, unknown>[] = [];
  for (const r of (Array.isArray(rowsRaw) ? rowsRaw : [])) {
    if (r && typeof r === 'object' && !Array.isArray(r) && (r as Record<string, unknown>).d) {
      cells.push(r as Record<string, unknown>);
    } else if (Array.isArray(r)) {
      for (const c of r) {
        if (c && typeof c === 'object' && (c as Record<string, unknown>).d) cells.push(c as Record<string, unknown>);
      }
    }
  }
  const free = cells.filter((c) => ['free', 'available', ''].includes(String(c.state ?? '').toLowerCase()));
  const priced = free
    .filter((c) => typeof c.price === 'number')
    .sort((a, b) => (a.price as number) - (b.price as number));
  const unpriced = free.filter((c) => typeof c.price !== 'number');
  const out: Record<string, unknown> = { available_count: free.length };
  if (priced.length) {
    const ccy = priced[0].ccy ?? '';
    out.cheapest_available = priced.slice(0, limit).map((c) => ({
      seat: c.d,
      price: c.price,
      currency: c.ccy ?? ccy,
    }));
  }
  if (unpriced.length) {
    out.price_not_shown = unpriced.slice(0, limit).map((c) => c.d);
    out.price_not_shown_note =
      "The seller's page did not attach a price to these seats. That does NOT mean free - tell " +
      'the traveller the price is not shown rather than quoting zero.';
  }
  return out;
}

async function openBookingQuestion(bookingRef: string): Promise<BookingQuestion | null> {
  for (const kind of ['seat', 'extra'] as const) {
    let d: Record<string, unknown>;
    try {
      const resp = await fetch(`${BASE_URL}/api/booking-payment/${kind}`, {
        method: 'POST',
        headers: letsfgHeaders({ json: true }),
        body: JSON.stringify({ intent: bookingRef }),
      });
      if (!resp.ok) continue;
      d = (await resp.json()) as Record<string, unknown>;
    } catch {
      // A question poll that fails must never turn a live booking status into an
      // error - the state we already have is still worth returning.
      continue;
    }
    if (!d?.open) continue;
    const exp = Number(d.expires_at_ms);
    const out: BookingQuestion = {
      kind,
      round: d.round,
      expires_at_ms: d.expires_at_ms,
      // SECONDS, because that is the number worth telling a person.
      seconds_remaining: Number.isFinite(exp) ? Math.round((exp - Date.now()) / 1000) : null,
      charge: d.charge,
    };
    if (kind === 'seat') {
      out.seat_map = d.map;
      out.options = seatOptions(d.map);
      out.ask_the_traveller =
        "The booking is paused at the airline's own seat map. Offer them the seats in " +
        '`options` with their prices and say how long they have. Do not pick for them unless ' +
        'they have already told you what they want.';
    } else {
      const pc = (d.price_change ?? {}) as Record<string, unknown>;
      if (Object.keys(pc).length) {
        out.question = 'price_change';
        out.price_change = pc;
        out.ask_the_traveller =
          'The fare moved before the ticket was issued. Tell them the old and new totals and ' +
          'ask whether to go ahead at the new price. Nothing extra is charged unless they say yes.';
      } else {
        out.extra = d.extra;
        out.ask_the_traveller =
          'The booking is paused at a paid extra the seller has just priced. Tell them what it ' +
          'is and what it costs, and ask whether to add it.';
      }
    }
    return out;
  }
  return null;
}

async function apiRequest(method: string, path: string, body?: Record<string, unknown>): Promise<unknown> {
  const resp = await fetch(`${BASE_URL}${path}`, {
    method,
    headers: letsfgHeaders({ json: true }),
    body: body ? JSON.stringify(body) : undefined,
  });

  // Parse defensively. This used to be a bare `await resp.json()`, which threw
  // on ANY non-JSON body and surfaced to the agent as
  // `Error: SyntaxError: Unexpected token '<', "<!DOCTYPE "...` — the real
  // status (404/502/a Cloudflare challenge page) was destroyed on the way out.
  // An agent cannot act on that; it can act on "status 404".
  return await readJson(resp, path);
}

async function resolveLocationCloud(query: string): Promise<unknown> {
  // There is NO location endpoint on the PFS Bearer lane. This used to fall
  // back to a website-side locations route for key-less callers; that route has
  // never existed on letsfg.co (verified 2026-08-16: 404, text/html), so every
  // Bearer-token user got a SyntaxError off the 404 HTML page. Same dead-end as
  // unlock_flight_offer below, which was already fixed for this reason. The
  // literal path is deliberately not written here — a source guard in
  // index.test.ts asserts it appears nowhere in this file.
  if (!API_KEY) {
    return {
      error: true,
      status_code: 400,
      detail:
        'resolve_location needs a Developer API key (LETSFG_API_KEY); there is no ' +
        'location endpoint on the PFS Bearer lane. Pass an IATA code directly ' +
        '(e.g. LON, WAW, JFK) — a city code expands to every airport in that city.',
    };
  }
  return apiRequest('GET', `/developers/api/v1/flights/locations/${encodeURIComponent(query)}`);
}

// ── Resources ───────────────────────────────────────────────────────────

const GUIDE_TEXT =
  '# LetsFG — Flight Search & Booking Guide\n' +
  '\n' +
  '## How It Works (2-Step Flow)\n' +
  '1. **search_flights** (FREE, unlimited) — Searches hundreds of airlines server-side. Returns prices, airlines, times, durations, stopovers.\n' +
  '2. **book_flight** — Books the offer. If we cannot complete it, you get a direct booking link for that exact offer instead. Nothing is charged by LetsFG either way.\n' +
  '\n' +
  '## Authentication\n' +
  'Run `letsfg auth`, or call the **authenticate** tool. It connects a card at https://letsfg.co/connect — a 0.00 Revolut setup, nothing charged. A person approves once in a browser; there is no endpoint that mints a token from card details. You pay the fare only when you book, and it is held, not taken, until the airline confirms.\n' +
  '\n' +
  '## DO NOT create a Developer API account\n' +
  'Do NOT call /developers/api/v1/agents/register or /developers/api/v1/agents/connect-payment, and do NOT run `letsfg register` or `letsfg connect-payment`. Those belong to a SEPARATE paid, prepaid-balance product for high-volume commercial integrations, and they create a billing account you almost certainly do not want. Older versions of these docs pointed there by default; that was wrong. Use the payment-token auth above.\n' +
  '\n' +
  '## Pricing\n' +
  '- Auth: FREE — zero-amount card setup, nothing charged\n' +
  '- Search: FREE, unlimited\n' +
  '- Book: the price shown on the offer. What you see is what is charged.\n' +
  '\n' +
  '## Critical Rules\n' +
  '- **Resolve locations first**: City names are ambiguous. "London" = 5+ airports. Use resolve_location to get IATA codes before searching.\n' +
  '- **Real passenger details REQUIRED**: Airlines send e-tickets to the email provided. Names must match passport/government ID exactly. NEVER use placeholder emails, agent emails, or fake names.\n' +
  '- **booking_unavailable is normal, not an error**: when book_flight answers {"booked": false, "booking_url": "..."}, the booking genuinely did not complete and retrying will not change that. Give the user the booking_url — it goes to that exact offer.\n' +
  '- **Offers expire with the search** (~15 min). If an offer is gone, search again.\n' +
  '\n' +
  '## Error Handling\n' +
  '- **transient** errors (SUPPLIER_TIMEOUT, RATE_LIMITED, SERVICE_UNAVAILABLE): Safe to retry after 1-5 seconds\n' +
  '- **validation** errors (INVALID_IATA, INVALID_DATE, MISSING_PARAMETER): Fix the input, then retry\n' +
  '- **business** errors (OFFER_EXPIRED, PAYMENT_DECLINED): Requires human decision — do not auto-retry\n' +
  '\n' +
  '## Search Tips\n' +
  '- Search is free — search multiple dates, cabin classes, airport combos liberally\n' +
  '- Search is async: POST /api/search -> poll /api/results/<id>. Poll immediately, then every 2s — do not sleep before the first poll\n' +
  '- A search reports `completed` BEFORE it stops growing. While `split_ticket_pending` or `gf_enrich_pending` is true, keep polling: the cheapest offer often lands after the status turns terminal\n' +
  '- Covers hundreds of airlines across all continents including low-cost carriers\n';

const RESOURCES = [
  {
    uri: 'letsfg://guide',
    name: 'LetsFG Flight Search & Booking Guide',
    description: 'Complete workflow guide: authenticate -> search -> book, pricing, passenger rules, error handling, and search tips. Read this before using any tools.',
    mimeType: 'text/markdown',
  },
];

// ── Tool Definitions ────────────────────────────────────────────────────

const TOOLS = [
  {
    name: 'search_flights',
    description:
      'Search hundreds of airlines for live flight prices — completely FREE, unlimited, read-only.\n\n' +
      'Returns structured offers with prices, airlines, times, durations, and stopovers. '+
      'Some offers carry `starlink` for in-flight Starlink Wi-Fi: "confirmed_all" / "confirmed_some" '+
      'mean the carrier has FULLY fitted that aircraft type, "likely_all" / "likely_some" mean the '+
      'rollout on that type is underway but incomplete. State only "confirmed_*" as fact; describe '+
      '"likely_*" as "the airline is fitting this aircraft type, not guaranteed on your flight". '+
      'Anything ending in "_some" has at least one leg WITHOUT it. An absent field means no '+
      'information, NOT an absence of Wi-Fi. '  +
      'Covers airlines across all continents including low-cost carriers.\n\n' +
      'Search is async: this tool polls for you, including waiting out the late split-ticket merge.\n\n' +
      'Some offers are SPLIT TICKETS: two separately-issued tickets through a hub, each leg booked from whatever is cheapest for it (usually two different airlines), because no one seller offers the combination as a single ticket. They carry `split_ticket: "true"`, `combo_type: "virtual_interlining"` and `self_transfer: "unprotected"`. ALWAYS tell the user when an offer is a split ticket and what unprotected means: the tickets are not linked, so if the first flight is late and the connection is missed, the second airline owes nothing — no rebooking, no refund. Never present a split ticket as though it were one through-fare.\n\n' +
      'Requires LETSFG_BEARER_TOKEN or LETSFG_API_KEY. ' +
      'See letsfg://guide resource for the full authenticate->search->book workflow.',
    inputSchema: {
      type: 'object',
      required: ['origin', 'destination', 'date_from'],
      properties: {
        origin: { type: 'string', description: "IATA code of departure (e.g., 'LON', 'JFK'). Use resolve_location if you only have a name." },
        destination: { type: 'string', description: "IATA code of arrival (e.g., 'BCN', 'LAX')" },
        date_from: { type: 'string', description: 'Departure date YYYY-MM-DD' },
        return_from: { type: 'string', description: 'Return date YYYY-MM-DD (omit for one-way)' },
        adults: { type: 'integer', description: 'Number of adults (default: 1)', default: 1 },
        children: { type: 'integer', description: 'Number of children (2-11)', default: 0 },
        cabin_class: { type: 'string', description: 'M=economy, W=premium, C=business, F=first', enum: ['M', 'W', 'C', 'F'] },
        currency: { type: 'string', description: 'Currency code (EUR, USD, GBP)', default: 'EUR' },
        max_results: { type: 'integer', description: 'Max offers to return', default: 10 },
        departure_time_from: { type: 'string', description: "Earliest departure time HH:MM (e.g., '06:00')" },
        departure_time_to: { type: 'string', description: "Latest departure time HH:MM (e.g., '14:00')" },
      },
    },
  },
  {
    name: 'resolve_location',
    description:
      'Convert a city/airport name to IATA codes. Always call before search_flights if you only have a city name. ' +
      'Read-only, safe to call multiple times.',
    inputSchema: {
      type: 'object',
      required: ['query'],
      properties: {
        query: { type: 'string', description: "City or airport name (e.g., 'London', 'Berlin')" },
      },
    },
  },
  // unlock_flight_offer was REMOVED from this list on 2026-09-08 and is NOT in the hosted
  // MCP's list either. A tool in the list is a CLAIM a model chooses from, and one called
  // "unlock" invites it to believe booking needs a step that no longer exists. Relabelling it
  // RETIRED was not enough - the entry itself was the invitation. The handler below still
  // answers the name with the replacement, so a client built against the old schema gets a
  // sentence rather than an unknown-tool error.
  {
    name: 'book_flight',
    description:
      'Book a flight from a search result.\n\n' +
      'FLOW: authenticate (once) -> search_flights -> book_flight\n' +
      'CHARGES: nothing from LetsFG. A completed booking pays the airline prices from airlines and the major booking sites.\n' +
      'RESULT: either {"booked": true, "order_id": "..."} or {"booked": false, "booking_url": "..."} — ' +
      'the second means the booking genuinely did not complete and nothing was charged. That is a normal ' +
      'outcome, NOT a transient error: do not retry, give the user the booking_url.\n' +
      'SAFETY: use REAL passenger details — names must match passport, email receives the e-ticket.',
    inputSchema: {
      type: 'object',
      required: ['search_id', 'offer_id', 'passengers', 'contact_email'],
      properties: {
        search_id: { type: 'string', description: 'search_id from search_flights' },
        offer_id: { type: 'string', description: 'Offer ID from search_flights' },
        passengers: {
          type: 'array',
          description: "Passengers with 'id' from search passenger_ids",
          items: {
            type: 'object',
            required: ['id', 'given_name', 'family_name', 'born_on', 'email'],
            properties: {
              id: { type: 'string', description: 'Passenger ID from search (pas_xxx)' },
              given_name: { type: 'string', description: 'First name (passport)' },
              family_name: { type: 'string', description: 'Last name (passport)' },
              born_on: { type: 'string', description: 'DOB YYYY-MM-DD' },
              gender: { type: 'string', description: 'm or f', default: 'm' },
              title: { type: 'string', description: 'mr, ms, mrs, miss', default: 'mr' },
              email: { type: 'string', description: 'Email' },
              phone_number: { type: 'string', description: 'Phone with country code' },
            },
          },
        },
        contact_email: { type: 'string', description: 'Booking contact email' },
        idempotency_key: { type: 'string', description: 'Unique key to prevent double-bookings on retry (e.g., UUID). Strongly recommended.' },
      },
    },
  },
  {
    name: 'get_flight_booking',
    description:
      'Poll a flight booking started by book_flight. REQUIRED to learn the outcome: on a PFS ' +
      'Bearer token book_flight returns a booking_ref and state "booking_in_progress", not a ' +
      'PNR - the booking itself takes 4-11 minutes.\n\n' +
      'Call it every 20-30 s with that booking_ref until state is terminal:\n' +
      '  completed       -> PNR issued, card charged\n' +
      '  failed          -> the hold was released, nothing was charged\n' +
      '  needs_attention -> a human is looking at it; do NOT rebook\n\n' +
      'Do not rebook while the state is still moving, and do not treat a slow poll as a ' +
      'failure - the money is HELD, not taken, until the airline confirms. Refs last one hour ' +
      'past the booking start.',
    inputSchema: {
      type: 'object',
      properties: {
        booking_ref: { type: 'string', description: 'The booking_ref book_flight returned' },
      },
      required: ['booking_ref'],
    },
  },
  {
    name: 'answer_booking_question',
    description:
      'Answer the question a paused booking is waiting on. get_flight_booking returns ' +
      '`awaiting_choice` when the booking agent has stopped mid-checkout holding a cart: at the ' +
      "airline's own seat map, at a paid extra it has just priced, or at a fare that moved.\n\n" +
      'NOTHING PROGRESSES UNTIL YOU ANSWER, and the cart expires. Put the question to the ' +
      'traveller with the options and the time left, then send their answer here.\n\n' +
      '  kind "seat"   -> seats: [{ d: "12A" }, ...] from the map you were shown, or skip: true\n' +
      '  kind "extra"  -> confirm: true to take it (a bag, or a moved fare), or confirm: false / ' +
      'skip: true to decline\n\n' +
      'Always pass the `round` from `awaiting_choice` - it says WHICH question you are ' +
      'answering. A return trip pauses twice, and an answer without it could seat the wrong leg.',
    inputSchema: {
      type: 'object',
      properties: {
        booking_ref: { type: 'string', description: 'The booking_ref book_flight returned' },
        kind: { type: 'string', enum: ['seat', 'extra'], description: "From awaiting_choice.kind" },
        round: { type: 'number', description: 'From awaiting_choice.round. Required.' },
        seats: {
          type: 'array',
          description: 'kind "seat" only: the chosen seats, e.g. [{ "d": "12A" }].',
          items: { type: 'object' },
        },
        confirm: { type: 'boolean', description: 'kind "extra" only: take it (true) or decline (false).' },
        skip: { type: 'boolean', description: 'Decline outright — no seat, no extra.' },
      },
      required: ['booking_ref', 'kind', 'round'],
    },
  },
  {
    name: 'resolve_hotel_city',
    description:
      'Convert a place name to the supplier city id that search_hotels needs. Always call this first if you ' +
      'only have a city name. Read-only and safe to repeat.\n\n' +
      'Use `Id` from the first result as city_id and `Name` as city_name.',
    inputSchema: {
      type: 'object',
      required: ['text'],
      properties: {
        text: { type: 'string', description: "Place name (e.g., 'Warsaw', 'Paris')" },
      },
    },
  },
  {
    name: 'search_hotels',
    description:
      'Search real, bookable hotel inventory. Requires a payment method on file — the SAME card that ' +
      'authorises flight booking. That applies to search too, not just booking, because a search opens a ' +
      'real session at the supplier.\n\n' +
      'Only free-cancellation, pay-later rates are returned, so everything you see can actually be booked ' +
      'on these terms. The result set is smaller than a metasearch and that is deliberate.\n\n' +
      '`price` is what the guest pays. Keep `session_id` and the chosen offer\'s `combination_id_v2` — ' +
      'together they identify that exact rate, and book_hotel needs both. Takes up to a few minutes.',
    inputSchema: {
      type: 'object',
      required: ['city_id', 'city_name', 'check_in', 'check_out'],
      properties: {
        city_id: { type: 'number', description: 'From resolve_hotel_city (`Id`)' },
        city_name: { type: 'string', description: 'From resolve_hotel_city (`Name`)' },
        check_in: { type: 'string', description: 'yyyy-MM-dd' },
        check_out: { type: 'string', description: 'yyyy-MM-dd' },
        adults: { type: 'number', description: 'Adult guests (default 2)' },
        children: { type: 'number', description: 'Child guests (default 0)' },
        child_ages: { type: 'array', items: { type: 'number' }, description: 'Age of each child; the supplier needs these to price' },
        nationality: { type: 'string', description: 'Two-letter guest nationality. Rates and taxes genuinely differ by it.' },
        limit: { type: 'number', description: 'Max hotels to return (default 40)' },
      },
    },
  },
  {
    name: 'book_hotel',
    description:
      'Book one hotel rate. Charges 5% of the price to the card on file immediately as a NON-REFUNDABLE ' +
      'reservation fee; the balance is paid directly to the supplier through the pay link we return, by ' +
      'balance_due_by (the supplier\'s own auto-cancellation date).\n\n' +
      'Returns a booking_job_id, NOT the booking — a booking takes minutes. Poll get_hotel_booking until ' +
      'status is succeeded or failed.\n\n' +
      'The fee is charged BEFORE the room is committed, so a declined card costs nothing: no reservation ' +
      'exists and nothing is charged.\n\n' +
      'Send expected_price and expected_balance back exactly as search returned them. NOT idempotent — ' +
      'calling twice for the same rate books the room twice and charges two fees.',
    inputSchema: {
      type: 'object',
      required: ['session_id', 'hotel_code', 'combination_id_v2', 'expected_price',
                 'expected_balance', 'city_id', 'city_name', 'check_in', 'check_out',
                 'guests', 'email', 'phone'],
      properties: {
        session_id: { type: 'string', description: 'From search_hotels' },
        hotel_code: { type: 'number', description: 'From the chosen hotel' },
        combination_id_v2: { type: 'string', description: 'From the chosen offer — identifies that exact rate' },
        combination_id: { type: 'number', description: 'From the chosen offer (optional)' },
        expected_price: { type: 'number', description: "The offer's `price`, verbatim" },
        expected_balance: { type: 'number', description: "The offer's `balance_to_supplier`, verbatim" },
        hotel_name: { type: 'string' },
        city_id: { type: 'number' },
        city_name: { type: 'string' },
        check_in: { type: 'string', description: 'yyyy-MM-dd' },
        check_out: { type: 'string', description: 'yyyy-MM-dd' },
        adults: { type: 'number' },
        guests: {
          type: 'array',
          description: 'One entry per guest: {title, first_name, last_name}',
          items: {
            type: 'object',
            required: ['title', 'first_name', 'last_name'],
            properties: {
              title: { type: 'string', description: 'Mr / Mrs / Ms' },
              first_name: { type: 'string' },
              last_name: { type: 'string' },
            },
          },
        },
        email: { type: 'string', description: 'The voucher and pay link go here. A typo loses the booking.' },
        phone: { type: 'string' },
        phone_country_code: { type: 'string', description: "Default '48'" },
        special_requests: { type: 'array', items: { type: 'string' } },
      },
    },
  },
  {
    name: 'get_hotel_booking',
    description:
      'Collect the result of a booking started with book_hotel. Poll every ~20s.\n\n' +
      'status is in_progress, succeeded or failed. On success you get confirmation, ' +
      'reservation_fee_charged, pay_link, balance_due, balance_due_by and the full cancellation ladder. ' +
      'Read-only and safe to repeat.',
    inputSchema: {
      type: 'object',
      required: ['booking_job_id'],
      properties: {
        booking_job_id: { type: 'string', description: 'From book_hotel' },
      },
    },
  },
  {
    name: 'cancel_hotel_booking',
    description:
      'Release a hotel reservation. Free until balance_due_by; after that the hotel\'s own ladder applies ' +
      'and can reach 100%. The ladder is in the booking terms, so check the cost first.\n\n' +
      'The 5% reservation fee is NOT refunded. Takes over a minute; if it times out do NOT assume it ' +
      'failed — re-check before retrying.',
    inputSchema: {
      type: 'object',
      required: ['confirmation'],
      properties: {
        confirmation: { type: 'string', description: 'From the completed booking' },
      },
    },
  },
  {
    name: 'authenticate',
    description:
      'Explain how to connect a card so this server can search and book. Nothing is charged to ' +
      'connect — a 0.00 Revolut setup that saves the card so a booking can be charged later.\n\n' +
      'Call with no arguments. It returns the current instructions and add_card_url ' +
      '(https://letsfg.co/connect). A PERSON must approve once in a browser — there is no endpoint ' +
      'that mints a token from card details, so do not ask the user for card numbers and do not ' +
      'try to automate this step.\n\n' +
      'Two ways in: (a) add LetsFG as a connector in an assistant that supports remote MCP servers ' +
      'and approve it, or (b) any OAuth-capable client can register itself — see ' +
      'https://letsfg.co/for-agents, section "Option B". Both land on the same card screen.\n\n' +
      'RETIRED 2026-09-02: the Stripe lanes (setup_url, setup_session_id, payment_method_id, ' +
      'card_token) and every token they issued. Passing them now fails.\n\n' +
      'This does NOT create a Developer API billing account. Do not use connect_payment for this.',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'connect_payment',
    description:
      '[Developer API only — you almost certainly want `authenticate` instead] Mints a one-time link ' +
      'for connecting a payment method to a PAID prepaid Developer API account. Nothing is charged to ' +
      'connect and card details never touch LetsFG: a PERSON opens the returned connect_url in a ' +
      'browser and saves a card, Revolut Pay or Google Pay there. Do not ask a user for card numbers ' +
      'and do not try to automate that step. Refuses to run unless LETSFG_API_KEY is set, because ' +
      'agents kept calling this and creating billing accounts they did not need. Replaced setup_payment ' +
      'on 2026-09-08, when the Stripe lane was retired.',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'get_agent_profile',
    description:
      '[Developer API only] Get agent profile, balance and usage stats. Read-only.\n\n' +
      'Requires LETSFG_API_KEY. A PFS Bearer token has no profile — it is bound to your payment ' +
      'method, carries no balance, and search and booking are free.',
    inputSchema: { type: 'object', properties: {} },
  },
  {
    name: 'load_resources',
    description:
      'Load the LetsFG workflow guide (3-step booking flow, pricing, passenger rules, error handling). ' +
      'Call this ONCE at the start of a conversation to understand how to use the flight tools correctly. ' +
      'Clients that support MCP resources get this automatically — this tool is for clients that do not.',
    inputSchema: { type: 'object', properties: {} },
  },
];

// ── Tool Handlers ───────────────────────────────────────────────────────

async function callTool(name: string, args: Record<string, unknown>): Promise<string> {
  switch (name) {
    case 'search_flights': {
      if (!BEARER_TOKEN && !API_KEY) {
        return JSON.stringify({
          error: 'Authentication required. Set LETSFG_BEARER_TOKEN (from `letsfg auth`) or LETSFG_API_KEY.',
        });
      }

      const params: Record<string, unknown> = {
        origin: args.origin,
        destination: args.destination,
        date_from: args.date_from,
        adults: args.adults ?? 1,
        children: args.children ?? 0,
        currency: args.currency ?? 'EUR',
        limit: args.max_results ?? 10,
      };
      if (args.return_from) params.return_from = args.return_from;
      if (args.cabin_class) params.cabin_class = args.cabin_class;
      if (args.departure_time_from) params.departure_time_from = args.departure_time_from;
      if (args.departure_time_to) params.departure_time_to = args.departure_time_to;

      let result: Record<string, unknown>;
      if (BEARER_TOKEN) {
        result = await searchPFS(params);
      } else {
        result = await apiRequest('POST', '/developers/api/v1/flights/search', params) as Record<string, unknown>;
      }

      if (result.error) return JSON.stringify(result, null, 2);

      // /api/search offers are FLAT: origin/destination/departure_time/stops/
      // duration_minutes sit at the top level, the carrier is the SINGULAR
      // `airline`, and round-trip legs live in `trip_breakdown`. There is no
      // `o.outbound.segments` -- reading it returned `outbound: null` with the
      // airline dropped on EVERY offer (issue #199). This shape was fixed and
      // shipped in 2026.5.70 but the fix never landed in the repo, so the
      // source still carried the bug; restored here.
      const offers = (result.offers || []) as Array<Record<string, unknown>>;
      const summary: Record<string, unknown> = {
        total_offers: offers.length,
        search_id: result.search_id,
        offers: offers.map(o => {
          const segs = (o.segments || []) as Array<Record<string, unknown>>;
          const legs = (o.trip_breakdown || []) as Array<Record<string, unknown>>;
          const ret = legs.find(l => l.leg === 'return');
          const airlines = [...new Set(
            [o.airline, ...legs.map(l => l.airline), ...segs.map(sg => sg.airline)].filter(Boolean)
          )] as string[];
          // The split fields are published BOTH ways: `split_ticket` is a
          // top-level boolean, while `self_transfer`, `split_hub` and
          // `split_connect_hours` live in the nested `conditions` object.
          // Reading only the top level silently drops `self_transfer` on every
          // ordinary self-transfer offer, which is the one field an agent must
          // never omit.
          const cond = (o.conditions || {}) as Record<string, unknown>;
          const isSplit = o.split_ticket === true || String(cond.split_ticket) === 'true';
          const selfTransfer = cond.self_transfer ?? o.self_transfer;
          return {
            offer_id: o.id ?? o.offer_ref,
            price: `${o.price} ${o.currency}`,
            airline: o.airline ?? airlines[0] ?? null,
            ...(airlines.length > 1 ? { airlines } : {}),
            // Virtual interlining: separate one-way fares stitched across airlines.
            ...(o.is_combo ? { virtual_interline: true } : {}),
            // A split ticket is TWO separately-issued tickets, each leg from whatever is
            // cheapest for it -- usually two different airlines.
            // The condition travels with the price or the agent misrepresents it.
            ...(isSplit ? {
              split_ticket: true,
              self_transfer: selfTransfer ?? 'unprotected',
              ...(cond.split_hub ? { connecting_via: cond.split_hub } : {}),
              ...(cond.split_connect_hours ? { connection_hours: cond.split_connect_hours } : {}),
              ...(o.split_saving_vs_through ? { saving_vs_through_fare: o.split_saving_vs_through } : {}),
              // `source` on a split reads "split:<sellerA>+<sellerB>" -- the two
              // sellers are the whole point, so pass it through unmangled.
              ...(o.source ? { sold_by: o.source } : {}),
              warning: 'Two separate tickets, each leg booked from whatever is cheapest (usually two different airlines). '
                     + 'The tickets are not linked: if the first flight is delayed and '
                     + 'the connection is missed, the second airline owes nothing '
                     + '(no rebooking, no refund). Tell the user this alongside the price.',
            } : (selfTransfer ? { self_transfer: selfTransfer } : {})),
            ...(o.booking_url ? { booking_url: o.booking_url } : {}),
            // Only present when there is something to say. Absent does NOT mean
            // the flight has no Wi-Fi -- see the tool description.
            ...(o.starlink ? { starlink: o.starlink } : {}),
            outbound: {
              from: o.origin ?? null,
              to: o.destination ?? null,
              departure: o.departure_time ?? null,
              arrival: o.arrival_time ?? null,
              stops: o.stops ?? Math.max(0, segs.length - 1),
              duration_minutes: o.duration_minutes ?? null,
            },
            // Round-trips carry a return leg in trip_breakdown; one-ways don't.
            ...(ret ? {
              return: {
                from: ret.origin ?? null,
                to: ret.destination ?? null,
                departure: ret.departure_time ?? null,
                arrival: ret.arrival_time ?? null,
                airline: ret.airline ?? null,
              },
            } : {}),
          };
        }),
      };
      return JSON.stringify(summary, null, 2);
    }

    case 'resolve_location': {
      const result = await resolveLocationCloud(args.query as string);
      return JSON.stringify(result, null, 2);
    }

    case 'unlock_flight_offer': {
      // RETIRED 2026-09-08. Refused HERE rather than forwarded, so the model gets one sentence
      // it can act on instead of a 410 body to interpret — and so no traffic is spent on a route
      // that cannot succeed.
      return JSON.stringify({
        error: 'retired',
        detail: 'unlock_flight_offer was retired on 2026-09-08 and the endpoint answers 410 Gone. There is no unlock step on any lane: call book_flight directly after search_flights. The fare is HELD on the connected payment method and captured only once a real airline PNR exists, which is what unlock existed to protect against; if the fare moves at checkout you are asked to accept or decline it.',
        next: 'book_flight',
      }, null, 2);
    }

    case 'book_flight': {
      // Bearer token → PFS booking. Only an explicit Developer API key routes to
      // the paid booking endpoint, so the default agent never touches it.
      if (BEARER_TOKEN) {
        const passengers = (args.passengers || []) as Array<Record<string, unknown>>;
        // /api/agent-book takes one passenger. Silently booking only the first
        // of several would hand back a confirmation for a trip nobody asked for,
        // so refuse instead of truncating.
        if (passengers.length > 1) {
          return JSON.stringify({
            error: 'multi_passenger_unsupported',
            detail:
              `Booking supports one passenger per call and ${passengers.length} were supplied. ` +
              'Nothing was booked. Book each passenger separately, or send the user to the ' +
              'booking_url from a single-passenger call.',
          }, null, 2);
        }
        const body: Record<string, unknown> = {
          search_id: args.search_id,
          offer_id: args.offer_id,
          contact_email: args.contact_email,
          passenger: passengers[0],
        };
        const result = await apiRequest('POST', '/api/agent-book', body) as Record<string, unknown>;
        return JSON.stringify(result, null, 2);
      }
      const body: Record<string, unknown> = {
        offer_id: args.offer_id,
        booking_type: 'flight',
        passengers: args.passengers,
        contact_email: args.contact_email,
      };
      if (args.idempotency_key) body.idempotency_key = args.idempotency_key;
      const result = await apiRequest('POST', '/developers/api/v1/bookings/book', body);
      return JSON.stringify(result, null, 2);
    }

    case 'get_flight_booking': {
      // book_flight on a Bearer token starts an ASYNC booking and hands back a
      // booking_ref. Without this tool the stdio server could start a booking and
      // then had no way to learn the outcome - the agent went blind on a live
      // charge. The hosted MCP has always exposed this; the stdio package did not.
      const ref = String(args.booking_ref || '').trim();
      if (!ref) {
        return JSON.stringify({
          error: 'booking_ref_required',
          detail: 'Pass the booking_ref that book_flight returned.',
        }, null, 2);
      }
      const result = await apiRequest('POST', '/api/agent-book/status', { booking_ref: ref }) as Record<string, unknown>;

      // A PAUSE IS NOT A STALL. `booking_in_progress` also covers a run that has
      // STOPPED holding a cart at the airline's seat map, at a paid extra, or at a
      // fare that moved - and the live-state message tells you to poll again in
      // 20-30 s, which is the opposite of what to do. Until 2026-09-08 this package
      // shipped exactly that: a booking that needed an answer sat unanswered until
      // the cart expired, with the traveller's money held the whole time.
      if (result && !result.error && result.state === 'booking_in_progress') {
        const question = await openBookingQuestion(ref);
        if (question) {
          result.awaiting_choice = question;
          // OVERWRITE, not append - the poll-again sentence must not survive in the
          // same payload as this instruction.
          result.message =
            `PAUSED - the booking is waiting for the traveller's answer about the ` +
            `${question.question === 'price_change' ? 'price change' : question.kind}. ` +
            `Nothing progresses until you answer.`;
          result.next_step =
            '1. Put awaiting_choice to the traveller now, with the options and the time left. ' +
            '2. Send their answer with answer_booking_question, passing awaiting_choice.round. ' +
            '3. Then carry on polling this tool.';
        }
      }
      return JSON.stringify(result, null, 2);
    }

    case 'answer_booking_question': {
      const ref = String(args.booking_ref || '').trim();
      const kind = String(args.kind || '').trim().toLowerCase();
      if (!ref || (kind !== 'seat' && kind !== 'extra')) {
        return JSON.stringify({
          error: true,
          detail: "booking_ref and kind ('seat' or 'extra') are required.",
        }, null, 2);
      }
      // The round is not defaulted. It says WHICH question is being answered, and a
      // return trip pauses twice - guessing it is how the wrong leg gets seated.
      if (typeof args.round !== 'number') {
        return JSON.stringify({
          error: true,
          detail:
            'round is required - it is the number get_flight_booking gave you in ' +
            'awaiting_choice.round, and it says WHICH question you are answering.',
        }, null, 2);
      }
      const payload: Record<string, unknown> = { intent: ref, round: args.round };
      if (args.skip === true) {
        payload.skip = true;
      } else if (kind === 'seat') {
        if (!Array.isArray(args.seats) || args.seats.length === 0) {
          return JSON.stringify({
            error: true,
            detail:
              'seats is required unless skip is true. Each entry names a designator from the ' +
              'seat_map you were shown, e.g. [{ "d": "12A" }].',
          }, null, 2);
        }
        payload.seats = args.seats;
      } else {
        payload.confirm = args.confirm === true;
      }
      const resp = await fetch(`${BASE_URL}/api/booking-payment/${kind}`, {
        method: 'POST',
        headers: letsfgHeaders({ json: true }),
        body: JSON.stringify(payload),
      });
      const data = await readJson(resp, `/api/booking-payment/${kind}`) as Record<string, unknown>;
      if (!resp.ok) {
        // The commonest refusal is a stale round or an expired cart, and both mean
        // the same thing here: stop answering, keep watching.
        return JSON.stringify({
          error: true,
          status_code: resp.status,
          detail: data?.error || data?.detail || 'refused',
          next_step:
            'Poll get_flight_booking again - the question may have expired or already been ' +
            'answered, and the booking carries on either way.',
        }, null, 2);
      }
      return JSON.stringify({
        ok: true,
        kind,
        round: args.round,
        recorded: data,
        next_step: 'Keep polling get_flight_booking every 20-30 s until the state is terminal.',
      }, null, 2);
    }

    case 'resolve_hotel_city': {
      const result = await apiRequest('POST', '/developers/api/v1/hotels/destinations',
        { text: args.text as string });
      return JSON.stringify(result, null, 2);
    }

    case 'search_hotels': {
      const body: Record<string, unknown> = {
        city_id: args.city_id,
        city_name: args.city_name,
        check_in: args.check_in,
        check_out: args.check_out,
        adults: args.adults ?? 2,
        children: args.children ?? 0,
        nationality: args.nationality ?? 'PL',
        limit: args.limit ?? 40,
        with_images: false,
      };
      if (Array.isArray(args.child_ages) && args.child_ages.length) {
        body.child_ages = args.child_ages;
      }
      const result = await apiRequest('POST', '/developers/api/v1/hotels/search', body);
      return JSON.stringify(result, null, 2);
    }

    case 'book_hotel': {
      const body: Record<string, unknown> = {
        session_id: args.session_id,
        hotel_code: args.hotel_code,
        combination_id_v2: args.combination_id_v2,
        expected_price: args.expected_price,
        expected_balance: args.expected_balance,
        city_id: args.city_id,
        city_name: args.city_name,
        check_in: args.check_in,
        check_out: args.check_out,
        adults: args.adults ?? 2,
        guests: args.guests,
        email: args.email,
        phone: args.phone,
        phone_country_code: args.phone_country_code ?? '48',
        special_requests: args.special_requests ?? [],
      };
      if (args.combination_id != null) body.combination_id = args.combination_id;
      if (args.hotel_name) body.hotel_name = args.hotel_name;
      const result = await apiRequest('POST', '/developers/api/v1/hotels/book', body);
      return JSON.stringify(result, null, 2);
    }

    case 'get_hotel_booking': {
      const result = await apiRequest('GET',
        `/developers/api/v1/hotels/booking/${encodeURIComponent(args.booking_job_id as string)}`);
      return JSON.stringify(result, null, 2);
    }

    case 'cancel_hotel_booking': {
      const result = await apiRequest('POST', '/developers/api/v1/hotels/cancel',
        { confirmation: args.confirmation as string });
      return JSON.stringify(result, null, 2);
    }

    case 'authenticate': {
      // The Stripe lanes are gone; /api/agent-access/verify answers 410 for those
      // credentials. Say so here rather than forwarding a dead call, so an agent
      // built against the old schema gets a reason instead of a bare 410.
      if (args.setup_session_id || args.payment_method_id || args.card_token) {
        return JSON.stringify({
          error: 'retired',
          detail:
            'setup_session_id / payment_method_id / card_token were part of the Stripe enrolment, ' +
            'retired 2026-09-02, and every token they issued was revoked. There is no endpoint that ' +
            'mints a token from card details.',
          add_card_url: `${BASE_URL}/connect`,
          next: 'Call authenticate with no arguments for the current instructions.',
          docs: `${BASE_URL}/for-agents`,
        }, null, 2);
      }
      // apiRequest treats the 402 as an error envelope; call directly so the
      // agent sees add_card_url and `how`, which is the point of the response.
      const resp = await fetch(`${BASE_URL}/api/agent-access/request`, {
        method: 'POST',
        headers: letsfgHeaders({ json: true, auth: false }),
        body: '{}',
      });
      // readJson, not resp.json(): a challenge page here used to throw a bare
      // SyntaxError at the agent, hiding the fact that enrollment never ran.
      return JSON.stringify(await readJson(resp, '/api/agent-access/request'), null, 2);
    }

    // 'setup_payment' is no longer in the tool list; the case stays so an agent built against
    // the old schema gets a sentence instead of an unknown-tool error or a bare 410.
    case 'setup_payment':
    case 'connect_payment': {
      if (!API_KEY) {
        return JSON.stringify({
          error: 'wrong_tool',
          detail:
            'connect_payment attaches a payment method to a PAID Developer API account and is not how ' +
            'agents authenticate. Use the `authenticate` tool instead — nothing is charged and no ' +
            'billing account is created.',
        }, null, 2);
      }
      if (name === 'setup_payment' || args.token || args.payment_method_id) {
        // Say what changed rather than silently succeeding on a different lane: a caller
        // that passed a Stripe token needs to know the token did nothing.
        const result = await apiRequest('POST', '/developers/api/v1/agents/connect-payment', {});
        return JSON.stringify({
          note:
            'setup_payment and its token / payment_method_id arguments were retired on 2026-09-08 with ' +
            'Stripe; /agents/setup-payment answers 410 Gone. Anything you passed was ignored. This is ' +
            'the replacement, connect_payment: open connect_url in a browser to save a card. Nothing ' +
            'is charged.',
          ...(result as Record<string, unknown>),
        }, null, 2);
      }
      const result = await apiRequest('POST', '/developers/api/v1/agents/connect-payment', {});
      return JSON.stringify(result, null, 2);
    }

    case 'get_agent_profile': {
      // /agents/me is a Developer API concept (balance, usage, billing). A PFS
      // Bearer token has no profile there and used to get a bare 401, which reads
      // like a broken tool rather than "not applicable".
      if (!API_KEY) {
        return JSON.stringify({
          not_applicable: true,
          detail:
            'Agent profiles exist only on the paid Developer API (balance, usage, billing) and need ' +
            'LETSFG_API_KEY. A PFS Bearer token has no profile: it is bound to your payment method, ' +
            'carries no balance, and search and booking are free. Nothing to check.',
        }, null, 2);
      }
      const result = await apiRequest('GET', '/developers/api/v1/agents/me');
      return JSON.stringify(result, null, 2);
    }

    case 'load_resources': {
      return GUIDE_TEXT;
    }

    default:
      return JSON.stringify({ error: `Unknown tool: ${name}` });
  }
}

// ── MCP Protocol (stdio) ───────────────────────────────────────────────

function send(msg: Record<string, unknown>) {
  process.stdout.write(JSON.stringify(msg) + '\n');
}

const rl = readline.createInterface({ input: process.stdin, terminal: false });

rl.on('line', async (line) => {
  let msg: Record<string, unknown>;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }

  const method = msg.method as string;
  const id = msg.id;

  switch (method) {
    case 'initialize':
      send({
        jsonrpc: '2.0',
        id,
        result: {
          protocolVersion: '2024-11-05',
          capabilities: { tools: {}, resources: {} },
          serverInfo: { name: 'letsfg', version: VERSION },
        },
      });
      break;

    case 'notifications/initialized':
      break;

    case 'resources/list':
      send({ jsonrpc: '2.0', id, result: { resources: RESOURCES } });
      break;

    case 'resources/read': {
      const rParams = msg.params as Record<string, unknown>;
      const uri = rParams.uri as string;
      if (uri === 'letsfg://guide') {
        send({ jsonrpc: '2.0', id, result: { contents: [{ uri, mimeType: 'text/markdown', text: GUIDE_TEXT }] } });
      } else {
        send({ jsonrpc: '2.0', id, error: { code: -32602, message: `Unknown resource: ${uri}` } });
      }
      break;
    }

    case 'tools/list':
      send({ jsonrpc: '2.0', id, result: { tools: TOOLS } });
      break;

    case 'tools/call': {
      const params = msg.params as Record<string, unknown>;
      const toolName = params.name as string;
      const toolArgs = (params.arguments || {}) as Record<string, unknown>;

      try {
        const text = await callTool(toolName, toolArgs);
        send({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text }] } });
      } catch (e) {
        send({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `Error: ${e}` }], isError: true } });
      }
      break;
    }

    case 'ping':
      send({ jsonrpc: '2.0', id, result: {} });
      break;

    default:
      if (id) {
        send({ jsonrpc: '2.0', id, error: { code: -32601, message: `Method not found: ${method}` } });
      }
  }
});

const authMode = BEARER_TOKEN ? 'PFS Bearer token' : API_KEY ? 'Developer API key' : 'NO AUTH (set LETSFG_BEARER_TOKEN or LETSFG_API_KEY)';
process.stderr.write(`LetsFG MCP v${VERSION} | auth: ${authMode}\n`);
