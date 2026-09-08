#!/usr/bin/env node
/**
 * LetsFG CLI — Agent-native flight search & booking from terminal.
 *
 * Usage:
 *   letsfg auth
 *   letsfg search GDN BER 2026-03-03
 *   letsfg book off_xxx --search-id srch_xxx --passenger '{"given_name":"John",...}' --email john@example.com
 *   letsfg locations Berlin
 *   letsfg me
 *
 * Developer API only (separate paid product):
 *   letsfg register --name my-agent --email agent@example.com
 */

import {
  LetsFG,
  LetsFGError,
  offerSummary,
  type FlightSearchResult,
  type SearchOptions,
  type BookingResult,
} from './index.js';
import { getBearerToken, paymentAuth, BearerTokenError } from './auth.js';

/**
 * Resolve credentials for a command: an explicit --api-key (or LETSFG_API_KEY
 * env var) always wins, since passing it is an explicit choice to use the
 * paid Developer API. Otherwise, fall back to a Bearer token from `letsfg
 * auth` (env var or ~/.letsfg/config.json) for the free PFS path. Neither
 * means the SDK's own requireAuth() will raise a clear error.
 */
function resolveCredentials(apiKeyFlag?: string): { bearerToken?: string; apiKey?: string } {
  const apiKey = apiKeyFlag || process.env.LETSFG_API_KEY;
  if (apiKey) return { apiKey };
  try {
    return { bearerToken: getBearerToken() };
  } catch {
    return {};
  }
}

// ── Arg parsing (zero-dependency) ────────────────────────────────────────

function getFlag(args: string[], flag: string, alias?: string): string | undefined {
  for (let i = 0; i < args.length; i++) {
    if (args[i] === flag || (alias && args[i] === alias)) {
      const val = args[i + 1];
      args.splice(i, 2);
      return val;
    }
    if (args[i].startsWith(`${flag}=`)) {
      const val = args[i].split('=').slice(1).join('=');
      args.splice(i, 1);
      return val;
    }
  }
  return undefined;
}

function hasFlag(args: string[], flag: string): boolean {
  const idx = args.indexOf(flag);
  if (idx >= 0) {
    args.splice(idx, 1);
    return true;
  }
  return false;
}

function getAllFlags(args: string[], flag: string, alias?: string): string[] {
  const results: string[] = [];
  let i = 0;
  while (i < args.length) {
    if (args[i] === flag || (alias && args[i] === alias)) {
      results.push(args[i + 1]);
      args.splice(i, 2);
    } else {
      i++;
    }
  }
  return results;
}

// ── Commands ─────────────────────────────────────────────────────────────

async function cmdSearch(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');
  const returnDate = getFlag(args, '--return', '-r');
  const adults = parseInt(getFlag(args, '--adults', '-a') || '1');
  const cabin = getFlag(args, '--cabin', '-c') as SearchOptions['cabinClass'];
  const stops = parseInt(getFlag(args, '--max-stops', '-s') || '2');
  const currency = getFlag(args, '--currency') || 'EUR';
  const limit = parseInt(getFlag(args, '--limit', '-l') || '20');
  const sort = (getFlag(args, '--sort') || 'price') as 'price' | 'duration';
  const departureFrom = getFlag(args, '--departure-from');
  const departureTo = getFlag(args, '--departure-to');

  const [origin, destination, date] = args;
  if (!origin || !destination || !date) {
    console.error('Usage: letsfg search <origin> <destination> <date> [options]');
    process.exit(1);
  }

  const creds = resolveCredentials(apiKey);
  const bt = new LetsFG({ ...creds, baseUrl });
  const result = await bt.search(origin, destination, date, {
    returnDate,
    adults,
    cabinClass: cabin,
    maxStopovers: stops,
    currency,
    limit,
    sort,
    departureTimeFrom: departureFrom,
    departureTimeTo: departureTo,
  });

  if (jsonOut) {
    console.log(JSON.stringify({
      search_id: result.search_id,
      passenger_ids: result.passenger_ids,
      total_results: result.total_results,
      offers: result.offers.map(o => ({
        id: o.id,
        price: o.price,
        currency: o.currency,
        airlines: o.airlines,
        owner_airline: o.owner_airline,
        route: [o.outbound.segments[0]?.origin, ...o.outbound.segments.map(s => s.destination)].join(' → '),
        duration_seconds: o.outbound.total_duration_seconds,
        stopovers: o.outbound.stopovers,
        conditions: o.conditions,
        is_locked: o.is_locked,
      })),
    }, null, 2));
    return;
  }

  if (!result.offers.length) {
    console.log(`No flights found for ${origin} → ${destination} on ${date}`);
    return;
  }

  console.log(`\n  ${result.total_results} offers  |  ${origin} → ${destination}  |  ${date}`);
  if (result.search_id) {
    console.log(`  search_id: ${result.search_id}  (needed for \`letsfg book\`, offers expire ~15 min after search)`);
  }
  console.log(`  Passenger IDs: ${JSON.stringify(result.passenger_ids)}\n`);

  result.offers.forEach((o, i) => {
    console.log(`  ${(i + 1).toString().padStart(3)}. ${offerSummary(o)}`);
    console.log(`       ID: ${o.id}`);
  });

  if (creds.bearerToken) {
    console.log(`\n  To book: letsfg book <offer_id> --search-id ${result.search_id} --passenger '{...}' --email you@example.com\n`);
  } else {
    console.log(`\n  To book: letsfg book <offer_id> --search-id ${result.search_id} --passenger '{...}' --email you@example.com\n`);
  }
}

async function cmdUnlock(_args: string[]) {
  // RETIRED 2026-09-08. Prints the replacement rather than calling a route that answers 410.
  console.error(
    '\n  letsfg unlock was retired on 2026-09-08 and the endpoint answers 410 Gone.\n' +
      '\n  There is no unlock step any more. Book directly:\n' +
      "    letsfg book <offer_id> --search-id <search_id> --passenger '{...}' --email you@example.com\n" +
      '\n  The fare is HELD on your connected payment method and captured only once a real\n' +
      '  airline PNR exists, which is what unlock existed to protect against. If the fare moves\n' +
      '  at checkout you are asked to accept or decline it.\n',
  );
  process.exit(1);
}

async function cmdBook(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');
  const searchId = getFlag(args, '--search-id');
  const email = getFlag(args, '--email', '-e') || '';
  const phone = getFlag(args, '--phone') || '';
  const passengerStrs = getAllFlags(args, '--passenger', '-p');
  const offerId = args[0];

  if (!offerId || !passengerStrs.length || !email) {
    console.error('Usage: letsfg book <offer_id> --search-id <id> --passenger \'{"given_name":"John",...}\' --email you@example.com');
    process.exit(1);
  }

  const passengers = passengerStrs.map(s => JSON.parse(s));
  const creds = resolveCredentials(apiKey);

  if (creds.bearerToken && !searchId) {
    console.error('Error: --search-id is required (from your `letsfg search` results) to book via the free PFS path.');
    process.exit(1);
  }

  const bt = new LetsFG({ ...creds, baseUrl });
  const result = await bt.book(offerId, passengers, email, phone, '', searchId);

  if (jsonOut) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if ('booked' in result) {
    // PFS path — free search; no booking fee, no transaction fee — our margin is already in the price you saw; no unlock step.
    if (result.booked) {
      console.log(`\n  ✓ Booking confirmed!`);
      console.log(`    Order ID: ${result.order_id}`);
      console.log(`    Charged: ${result.charged ?? 0} ${result.currency ?? ''}\n`);
    } else {
      console.log(`\n  Could not complete a confirmed booking. Nothing was charged.`);
      console.log(`    Booking link: ${result.booking_url ?? '(none)'}\n`);
    }
    return;
  }

  // Developer API path
  const br = result as BookingResult;
  if (br.status === 'confirmed') {
    console.log(`\n  ✓ Booking confirmed!`);
    console.log(`    PNR: ${br.booking_reference}`);
    console.log(`    Flight: ${br.currency} ${br.flight_price.toFixed(2)}`);
    console.log(`    Fee: ${br.currency} ${br.service_fee.toFixed(2)} (${br.service_fee_percentage}%)`);
    console.log(`    Total: ${br.currency} ${br.total_charged.toFixed(2)}`);
    console.log(`    Order: ${br.order_id}\n`);
  } else {
    console.error(`  ✗ Booking failed`);
    console.error(JSON.stringify(br.details, null, 2));
    process.exit(1);
  }
}

async function cmdLocations(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');
  const query = args[0];

  if (!query) {
    console.error('Usage: letsfg locations <city-or-airport-name>');
    process.exit(1);
  }

  const bt = new LetsFG({ apiKey, baseUrl });
  const result = await bt.resolveLocation(query);

  if (jsonOut) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (!result.length) {
    console.log(`No locations found for '${query}'`);
    return;
  }

  for (const loc of result) {
    const iata = (loc.iata_code as string || '???').padEnd(5);
    const name = loc.name || '';
    const type = loc.type || '';
    const city = loc.city_name || '';
    const country = loc.country || '';
    console.log(`  ${iata}  ${name} (${type}) — ${city}, ${country}`);
  }
}

async function cmdAuth(args: string[]) {
  // --card-token / --payment-method went with the Stripe lanes. There is no
  // headless card path any more: a person adds the card once at
  // letsfg.co/connect, and this command holds the refresh token afterwards.
  for (const dead of ['--card-token', '--payment-method']) {
    if (getFlag(args, dead)) {
      console.error(
        `\n  ${dead} was part of the Stripe enrolment, retired 2026-09-02.\n` +
        '  Run plain `letsfg auth` instead - it opens letsfg.co/connect to add a card.\n'
      );
      process.exit(1);
    }
  }
  await paymentAuth(!hasFlag(args, '--no-browser'));
  console.log('\n  You are all set. Run: letsfg search WAW BCN 2026-07-15\n');
}

async function cmdRegister(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const baseUrl = getFlag(args, '--base-url');
  const name = getFlag(args, '--name', '-n');
  const email = getFlag(args, '--email', '-e');
  const owner = getFlag(args, '--owner') || '';
  const desc = getFlag(args, '--desc') || '';

  if (!name || !email) {
    console.error('Usage: letsfg register --name my-agent --email agent@example.com');
    process.exit(1);
  }

  const result = await LetsFG.register(name, email, baseUrl, owner, desc);

  if (jsonOut) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  console.log(`\n  ✓ Agent registered!`);
  console.log(`    Agent ID: ${result.agent_id}`);
  console.log(`    API Key:  ${result.api_key}`);
  console.log(`\n    Save your API key:`);
  console.log(`    export LETSFG_API_KEY=${result.api_key}`);
  console.log(`\n    Next: letsfg search GDN BCN 2026-07-15\n`);
}

// Reached by `letsfg connect-payment` and by `letsfg setup-payment`, kept as an alias
// because that name is in published READMEs and in people's scripts — a "command not
// found" tells them nothing. --token was part of the Stripe enrolment, retired
// 2026-09-08; it is accepted and ignored rather than erroring on an old invocation.
async function cmdSetupPayment(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');
  if (getFlag(args, '--token', '-t')) {
    console.log('\n  Note: --token was part of the Stripe enrolment, retired 2026-09-08. Ignoring it.');
  }

  const bt = new LetsFG({ apiKey, baseUrl });
  const result = await bt.connectPayment();

  if (jsonOut) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  const url = result.connect_url as string | undefined;
  if (url) {
    console.log(`\n  Open this in a browser to connect a payment method:\n`);
    console.log(`    ${url}\n`);
    console.log(`    Nothing is charged. Run \`letsfg me\` afterwards to confirm it landed.\n`);
  } else {
    console.error(`  ✗ Could not mint a connect link: ${result.message || result.status}`);
    process.exit(1);
  }
}

async function cmdMe(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');

  const bt = new LetsFG({ apiKey, baseUrl });
  const profile = await bt.me();

  if (jsonOut) {
    console.log(JSON.stringify(profile, null, 2));
    return;
  }

  const p = profile as Record<string, unknown>;
  const u = (p.usage || {}) as Record<string, number>;
  console.log(`\n  Agent: ${p.agent_name} (${p.agent_id})`);
  console.log(`  Email: ${p.email}`);
  console.log(`  Tier:  ${p.tier}`);
  const access = p.access_granted || false;
  console.log(`  Access:  ${access ? '✓ Granted (search, book)' : '✗ Not granted'}`);
  console.log(`  Payment: ${p.payment_ready ? '✓ Ready' : '—'}`);
  console.log(`  Searches: ${u.total_searches || 0}`);
  console.log(`  Bookings: ${u.total_bookings || 0}`);
  console.log(`  Total spent: $${((u.total_spent_cents || 0) / 100).toFixed(2)}\n`);
}

// ── Main ─────────────────────────────────────────────────────────────────

const HELP = `
LetsFG — Agent-native flight search & booking.

Search hundreds of airlines via the LetsFG cloud engine.
Authenticate once with letsfg auth — it opens letsfg.co/connect so you can add
a card. Nothing is charged; you pay the fare only when you book, and it is held,
not taken, until the airline confirms.

Commands:
  auth                             Connect a card at letsfg.co/connect. Nothing charged
  search <origin> <dest> <date>    Search for flights (free), prints search_id
  locations <query>                Resolve city name to IATA codes
  book <offer_id> --search-id ...  Book a flight. No booking or transaction fee, no unlock step
  me                               Show agent profile

Developer API only (a SEPARATE paid product — most agents should not use these;
they create a billing account. Use auth above instead):
  register --name ... --email ... Create a paid Developer API account
  connect-payment                 Print a link to connect a card to that paid account
  setup-payment                   Alias of connect-payment (the Stripe lane retired 2026-09-08)
  unlock <offer_id>               RETIRED 2026-09-08 — no unlock step, book directly

Options:
  --json, -j          Output raw JSON
  --api-key, -k       Developer API key (or set LETSFG_API_KEY) — switches book/search to the paid path
  --base-url          API URL (default: https://letsfg.co)
  --no-browser        (auth only) Print the connect URL instead of opening a browser

Examples:
  letsfg auth
  letsfg search GDN BER 2026-03-03 --sort price
  letsfg book off_xxx --search-id srch_xxx -p '{"given_name":"Ada","family_name":"Lovelace","born_on":"1990-04-01","gender":"f"}' -e ada@example.com
`;

async function main() {
  const args = process.argv.slice(2);
  const command = args.shift();

  try {
    switch (command) {
      case 'auth':
        await cmdAuth(args);
        break;
      case 'search':
        await cmdSearch(args);
        break;
      case 'unlock':
        await cmdUnlock(args);
        break;
      case 'book':
        await cmdBook(args);
        break;
      case 'locations':
        await cmdLocations(args);
        break;
      case 'register':
        await cmdRegister(args);
        break;
      case 'connect-payment':
      case 'setup-payment':  // alias, kept: the old name is in published READMEs
        await cmdSetupPayment(args);
        break;
      case 'me':
        await cmdMe(args);
        break;
      case '--help':
      case '-h':
      case 'help':
      case undefined:
        console.log(HELP);
        break;
      default:
        console.error(`Unknown command: ${command}`);
        console.log(HELP);
        process.exit(1);
    }
  } catch (e) {
    if (e instanceof LetsFGError || e instanceof BearerTokenError) {
      console.error(`Error: ${e.message}`);
      process.exit(1);
    }
    throw e;
  }
}

main();
