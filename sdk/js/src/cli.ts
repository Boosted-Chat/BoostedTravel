#!/usr/bin/env node
/**
 * LetsFG CLI — Agent-native flight search & booking from terminal.
 *
 * Usage:
 *   letsfg search GDN BER 2026-03-03
 *   letsfg unlock off_xxx
 *   letsfg book off_xxx --passenger '{"id":"pas_xxx",...}' --email john@example.com
 *   letsfg register --name my-agent --email agent@example.com
 *   letsfg me
 *   letsfg locations Berlin
 */

import {
  LetsFG,
  LetsFGError,
  offerSummary,
  type FlightSearchResult,
  type SearchOptions,
} from './index.js';

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

  const [origin, destination, date] = args;
  if (!origin || !destination || !date) {
    console.error('Usage: letsfg search <origin> <destination> <date> [options]');
    process.exit(1);
  }

  const bt = new LetsFG({ apiKey, baseUrl });
  const result = await bt.search(origin, destination, date, {
    returnDate,
    adults,
    cabinClass: cabin,
    maxStopovers: stops,
    currency,
    limit,
    sort,
  });

  if (jsonOut) {
    console.log(JSON.stringify({
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
  console.log(`  Passenger IDs: ${JSON.stringify(result.passenger_ids)}\n`);

  result.offers.forEach((o, i) => {
    console.log(`  ${(i + 1).toString().padStart(3)}. ${offerSummary(o)}`);
    console.log(`       ID: ${o.id}`);
  });

  console.log(`\n  To unlock: letsfg unlock <offer_id>`);
  console.log(`  Passenger IDs needed for booking: ${JSON.stringify(result.passenger_ids)}\n`);
}

async function cmdUnlock(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');
  const offerId = args[0];

  if (!offerId) {
    console.error('Usage: letsfg unlock <offer_id>');
    process.exit(1);
  }

  const bt = new LetsFG({ apiKey, baseUrl });
  const result = await bt.unlock(offerId);

  if (jsonOut) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (result.unlock_status === 'unlocked') {
    console.log(`\n  ✓ Offer unlocked!`);
    console.log(`    Confirmed price: ${result.confirmed_currency} ${result.confirmed_price?.toFixed(2)}`);
    console.log(`    Expires at: ${result.offer_expires_at}`);
    console.log(`\n    Next: letsfg book ${offerId} --passenger '{...}' --email you@example.com\n`);
  } else {
    console.error(`  ✗ Unlock failed: ${result.message}`);
    process.exit(1);
  }
}

async function cmdBook(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');
  const email = getFlag(args, '--email', '-e') || '';
  const phone = getFlag(args, '--phone') || '';
  const passengerStrs = getAllFlags(args, '--passenger', '-p');
  const offerId = args[0];

  if (!offerId || !passengerStrs.length || !email) {
    console.error('Usage: letsfg book <offer_id> --passenger \'{"id":"pas_xxx",...}\' --email you@example.com');
    process.exit(1);
  }

  const passengers = passengerStrs.map(s => JSON.parse(s));
  const bt = new LetsFG({ apiKey, baseUrl });
  const result = await bt.book(offerId, passengers, email, phone);

  if (jsonOut) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (result.status === 'confirmed') {
    console.log(`\n  ✓ Booking confirmed!`);
    console.log(`    PNR: ${result.booking_reference}`);
    console.log(`    Flight: ${result.currency} ${result.flight_price.toFixed(2)}`);
    console.log(`    Fee: ${result.currency} ${result.service_fee.toFixed(2)} (${result.service_fee_percentage}%)`);
    console.log(`    Total: ${result.currency} ${result.total_charged.toFixed(2)}`);
    console.log(`    Order: ${result.order_id}\n`);
  } else {
    console.error(`  ✗ Booking failed`);
    console.error(JSON.stringify(result.details, null, 2));
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
  console.log(`\n    Next: Star the repo and link your GitHub:`);
  console.log(`    1. Star https://github.com/LetsFG/LetsFG`);
  console.log(`    2. letsfg star --github <your-github-username>\n`);
}

async function cmdStar(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');
  const github = getFlag(args, '--github', '-g');

  if (!github) {
    console.error('Usage: letsfg star --github <your-github-username>');
    process.exit(1);
  }

  const bt = new LetsFG({ apiKey, baseUrl });
  const result = await bt.linkGithub(github) as Record<string, unknown>;

  if (jsonOut) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  const status = result.status;
  if (status === 'verified') {
    console.log(`\n  ✓ GitHub star verified! Unlimited access granted.`);
    console.log(`    Username: ${result.github_username}`);
    console.log(`\n    You're all set — search, unlock, and book for free.\n`);
  } else if (status === 'already_verified') {
    console.log(`\n  ✓ Already verified! You have unlimited access.`);
    console.log(`    Username: ${result.github_username}\n`);
  } else if (status === 'star_required') {
    console.log(`\n  ✗ Star not found for '${github}'.`);
    console.log(`    1. Star the repo: https://github.com/LetsFG/LetsFG`);
    console.log(`    2. Run this command again.\n`);
  } else {
    console.error(`  ✗ Unexpected status: ${status}`);
    process.exit(1);
  }
}

async function cmdSetupPayment(args: string[]) {
  const jsonOut = hasFlag(args, '--json') || hasFlag(args, '-j');
  const apiKey = getFlag(args, '--api-key', '-k');
  const baseUrl = getFlag(args, '--base-url');
  const token = getFlag(args, '--token', '-t') || 'tok_visa';

  const bt = new LetsFG({ apiKey, baseUrl });
  const result = await bt.setupPayment(token);

  if (jsonOut) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (result.status === 'ready') {
    console.log(`\n  ✓ Payment ready! You can now unlock offers and book flights.\n`);
  } else {
    console.error(`  ✗ Payment setup failed: ${result.message || result.status}`);
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
  const gh = p.github_username || '';
  const starOk = p.github_star_verified || false;
  if (starOk) {
    console.log(`  GitHub:  ✓ ${gh} (star verified — unlimited access)`);
  } else if (gh) {
    console.log(`  GitHub:  ${gh} (star not verified)`);
  } else {
    console.log(`  GitHub:  Not linked — run: letsfg star --github <username>`);
  }
  console.log(`  Payment: ${p.payment_ready ? '✓ Ready' : '—'}`);
  console.log(`  Searches: ${u.total_searches || 0}`);
  console.log(`  Unlocks:  ${u.total_unlocks || 0}`);
  console.log(`  Bookings: ${u.total_bookings || 0}`);
  console.log(`  Total spent: $${((u.total_spent_cents || 0) / 100).toFixed(2)}\n`);
}

// ── Main ─────────────────────────────────────────────────────────────────

const HELP = `
LetsFG — Agent-native flight search & booking.

Search 400+ airlines at raw airline prices — $20-50 cheaper than OTAs.
100% FREE — just star our GitHub repo for unlimited access.

Commands:
  search <origin> <dest> <date>   Search for flights (FREE)
  locations <query>               Resolve city name to IATA codes
  star --github <username>        Link GitHub — star repo for free access
  unlock <offer_id>               Unlock offer (FREE with GitHub star)
  book <offer_id> --passenger ... Book flight (FREE after unlock)
  register --name ... --email ... Register new agent
  setup-payment                   Legacy payment setup
  me                              Show agent profile

Options:
  --json, -j       Output raw JSON
  --api-key, -k    API key (or set LETSFG_API_KEY)
  --base-url       API URL (default: https://api.letsfg.co)

Examples:
  letsfg register --name my-agent --email me@example.com
  letsfg star --github octocat
  letsfg search GDN BER 2026-03-03 --sort price
  letsfg unlock off_xxx
  letsfg book off_xxx -p '{"id":"pas_xxx",...}' -e john@ex.com
`;

async function main() {
  const args = process.argv.slice(2);
  const command = args.shift();

  try {
    switch (command) {
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
      case 'star':
        await cmdStar(args);
        break;
      case 'setup-payment':
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
    if (e instanceof LetsFGError) {
      console.error(`Error: ${e.message}`);
      process.exit(1);
    }
    throw e;
  }
}

main();
