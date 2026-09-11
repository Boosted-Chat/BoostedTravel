/**
 * MCP server smoke tests — exercises the server via stdio JSON-RPC protocol.
 * Spawns the actual server process and verifies protocol compliance.
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { join, dirname } from 'node:path';
import { readFileSync } from 'node:fs';

const __dir = dirname(fileURLToPath(import.meta.url));
const SERVER_PATH = join(__dir, 'index.ts');

function spawnServer(): ChildProcessWithoutNullStreams {
  return spawn('npx', ['tsx', SERVER_PATH], {
    stdio: ['pipe', 'pipe', 'pipe'],
  });
}

function sendMessage(proc: ChildProcessWithoutNullStreams, msg: Record<string, unknown>): void {
  proc.stdin.write(JSON.stringify(msg) + '\n');
}

function readNextMessage(proc: ChildProcessWithoutNullStreams, timeoutMs = 5000): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    let buf = '';
    const timer = setTimeout(() => reject(new Error('MCP response timeout')), timeoutMs);

    const onData = (chunk: Buffer) => {
      buf += chunk.toString();
      const nl = buf.indexOf('\n');
      if (nl !== -1) {
        clearTimeout(timer);
        proc.stdout.off('data', onData);
        try {
          resolve(JSON.parse(buf.slice(0, nl)));
        } catch {
          reject(new Error(`Invalid JSON: ${buf.slice(0, nl)}`));
        }
      }
    };

    proc.stdout.on('data', onData);
    proc.on('error', (err) => { clearTimeout(timer); reject(err); });
  });
}

// ── Protocol smoke tests ──────────────────────────────────────────────────

// Source-level guards. Deliberately do NOT spawn: these must still run when the
// spawn-based tests below can't (they shell out to `npx`, which is ENOENT on
// Windows, so the whole suite goes red for reasons unrelated to the code).
describe('MCP server — dead-route guards', () => {
  const src = readFileSync(SERVER_PATH, 'utf8');
  // Guards that assert a pattern is ABSENT must look at code only. This file
  // documents the bugs it fixed by quoting them, so matching raw source makes a
  // comment describing the old mistake read as the mistake itself.
  const code = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

  it('never calls /api/locations — that route does not exist on letsfg.co', () => {
    // 2026-08-16: resolve_location sent every key-less (PFS Bearer) caller to
    // `/api/locations?q=`, which 404s with the HTML error page. The agent got
    // `SyntaxError: Unexpected token '<'` instead of an answer. This is the
    // THIRD time a PFS dead-end shipped here (see unlock_flight_offer's own
    // comment), so it gets a guard rather than another comment.
    assert.ok(!src.includes('/api/locations'), '/api/locations is a 404 — use the Developer API locations path');
  });

  it('parses responses defensively, never a bare resp.json()', () => {
    // A bare `await resp.json()` destroys the real status on any HTML body
    // (404 page, 502, Cloudflare challenge) and reports a JSON syntax error.
    assert.ok(!/const\s+data\s*=\s*await\s+resp\.json\(\)\s*;/.test(code),
      'read resp.text() and JSON.parse it, so a non-JSON body still reports its status');
    assert.ok(!/await\s+\w*[Rr]esp\.json\(\)/.test(code),
      'every response body goes through readJson() — no call site parses its own');
  });

  // Walks EVERY request in the file rather than checking one function. The
  // per-function version of this test is exactly what let #163 ship twice: it
  // passed the whole time the second call site was broken, because it was only
  // ever asked about the first one. #206 was the same shape again — the search
  // POST and its results poll were the two requests that built their own
  // headers, and they are the two that carry a real search.
  it('every fetch() sends headers built by letsfgHeaders()', () => {
    const fetches = [...code.matchAll(/fetch\(([\s\S]*?)\n\s*\}\);/g)].map((m) => m[1]);
    assert.ok(fetches.length >= 4, `expected to find the request call sites, found ${fetches.length}`);
    for (const call of fetches) {
      assert.ok(/headers:\s*letsfgHeaders\(/.test(call),
        `a fetch() builds its own headers instead of calling letsfgHeaders():\n${call.slice(0, 200)}`);
    }
  });

  it('the User-Agent is overridable, and set in exactly one place', () => {
    // A client on the wrong side of an edge/WAF rule cannot fix the rule. Without
    // an override its only options are downgrade or wait (#206).
    assert.ok(src.includes('LETSFG_USER_AGENT'), 'LETSFG_USER_AGENT must override the client UA');
    assert.equal((code.match(/'User-Agent':/g) ?? []).length, 1,
      "the UA is set once, in letsfgHeaders() — a second literal is a call site the override can't reach");
  });

  it('the default User-Agent still names letsfg-mcp', () => {
    // #206 moved the default to the conventional `Mozilla/5.0 (compatible; …)`
    // form because the bare token was challenged from datacenter IPs. The point
    // of that form is that it stays IDENTIFIABLE — dropping our own name from it
    // would turn a declared client into an anonymous one, and would silently
    // break any allowlist keyed on the `letsfg-mcp/` substring.
    const ua = /const USER_AGENT = `([^`]+)`/.exec(code)?.[1];
    assert.ok(ua, 'USER_AGENT must be defined in one place');
    assert.match(ua!, /^Mozilla\/5\.0 \(compatible; /, 'use the declared-bot UA form');
    assert.ok(ua!.includes('letsfg-mcp/'), 'the UA must still name letsfg-mcp');
    assert.ok(ua!.includes('+https://'), 'declared-bot UAs carry a contact URL');
  });
});

describe('MCP server — initialize', () => {
  let proc: ChildProcessWithoutNullStreams;

  before(() => { proc = spawnServer(); });
  after(() => { proc.kill(); });

  it('responds to initialize with serverInfo', async () => {
    sendMessage(proc, { jsonrpc: '2.0', id: 1, method: 'initialize', params: {
      protocolVersion: '2024-11-05',
      capabilities: {},
      clientInfo: { name: 'test', version: '1' },
    }});

    const response = await readNextMessage(proc);
    assert.equal(response.jsonrpc, '2.0');
    assert.equal(response.id, 1);

    const result = response.result as Record<string, unknown>;
    assert.ok(result, 'result should be present');

    const serverInfo = result.serverInfo as Record<string, unknown>;
    assert.equal(serverInfo.name, 'letsfg');
    assert.ok(serverInfo.version, 'version should be set');

    const capabilities = result.capabilities as Record<string, unknown>;
    assert.ok(capabilities.tools !== undefined, 'should advertise tools capability');
  });
});

describe('MCP server — tools/list', () => {
  let proc: ChildProcessWithoutNullStreams;

  before(() => { proc = spawnServer(); });
  after(() => { proc.kill(); });

  it('returns expected tools', async () => {
    // Initialize first
    sendMessage(proc, { jsonrpc: '2.0', id: 1, method: 'initialize', params: {
      protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'test', version: '1' },
    }});
    await readNextMessage(proc);

    // List tools
    sendMessage(proc, { jsonrpc: '2.0', id: 2, method: 'tools/list', params: {} });
    const response = await readNextMessage(proc);

    const result = response.result as Record<string, unknown>;
    const tools = result.tools as Array<{ name: string }>;
    assert.ok(Array.isArray(tools), 'tools should be an array');

    const toolNames = tools.map(t => t.name);
    // get_flight_booking and answer_booking_question are how a booking this package
    // starts is followed to its PNR, and answered when it pauses at a seat map,
    // a paid extra or a price change.
    const requiredTools = ['search_flights', 'resolve_location', 'book_flight', 'get_flight_booking', 'answer_booking_question'];
    for (const name of requiredTools) {
      assert.ok(toolNames.includes(name), `missing required tool: ${name}`);
    }
    // Retired 2026-09-08 and delisted: a tool in the list is a claim a model chooses
    // from, and one called "unlock" says booking needs a step that no longer exists.
    assert.ok(!toolNames.includes('unlock_flight_offer'), 'unlock_flight_offer is retired and must not be listed');
  });

  it('each tool has name, description, and inputSchema', async () => {
    sendMessage(proc, { jsonrpc: '2.0', id: 1, method: 'initialize', params: {
      protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'test', version: '1' },
    }});
    await readNextMessage(proc);

    sendMessage(proc, { jsonrpc: '2.0', id: 2, method: 'tools/list', params: {} });
    const response = await readNextMessage(proc);

    const result = response.result as Record<string, unknown>;
    const tools = result.tools as Array<Record<string, unknown>>;

    for (const tool of tools) {
      assert.ok(typeof tool.name === 'string' && tool.name.length > 0, `tool.name missing on: ${JSON.stringify(tool)}`);
      assert.ok(typeof tool.description === 'string' && tool.description.length > 0, `tool.description missing on: ${tool.name}`);
      assert.ok(tool.inputSchema !== null && typeof tool.inputSchema === 'object', `tool.inputSchema missing on: ${tool.name}`);
    }
  });
  it('search_flights schema includes departure_time filters', async () => {
    sendMessage(proc, { jsonrpc: '2.0', id: 1, method: 'initialize', params: {
      protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'test', version: '1' },
    }});
    await readNextMessage(proc);

    sendMessage(proc, { jsonrpc: '2.0', id: 2, method: 'tools/list', params: {} });
    const response = await readNextMessage(proc);

    const result = response.result as Record<string, unknown>;
    const tools = result.tools as Array<Record<string, unknown>>;
    const searchTool = tools.find(t => t.name === 'search_flights');
    assert.ok(searchTool, 'search_flights tool should exist');

    const inputSchema = searchTool.inputSchema as Record<string, unknown>;
    const props = inputSchema.properties as Record<string, unknown>;
    assert.ok(props.departure_time_from, 'departure_time_from should exist in schema');
    assert.ok(props.departure_time_to, 'departure_time_to should exist in schema');
  });
});

describe('MCP server — retired unlock_flight_offer', () => {
  let proc: ChildProcessWithoutNullStreams;

  before(() => { proc = spawnServer(); });
  after(() => { proc.kill(); });

  it('still answers the old name, pointing at book_flight', async () => {
    // Delisted, but an agent holding an old tool list can still call it. It must
    // get the replacement, not an error it has to interpret.
    sendMessage(proc, { jsonrpc: '2.0', id: 1, method: 'initialize', params: {
      protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'test', version: '1' },
    }});
    await readNextMessage(proc);

    sendMessage(proc, { jsonrpc: '2.0', id: 2, method: 'tools/call', params: {
      name: 'unlock_flight_offer', arguments: { offer_id: 'off_1' },
    }});
    const response = await readNextMessage(proc);

    const result = response.result as { content: Array<{ text: string }>; isError?: boolean };
    assert.ok(!result.isError, 'the retired name is answered, not thrown');
    const body = JSON.parse(result.content[0].text) as Record<string, unknown>;
    assert.equal(body.error, 'retired');
    assert.equal(body.next, 'book_flight');
  });
});

describe('MCP server — resources/list', () => {
  let proc: ChildProcessWithoutNullStreams;

  before(() => { proc = spawnServer(); });
  after(() => { proc.kill(); });

  it('returns the guide resource', async () => {
    sendMessage(proc, { jsonrpc: '2.0', id: 1, method: 'initialize', params: {
      protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'test', version: '1' },
    }});
    await readNextMessage(proc);

    sendMessage(proc, { jsonrpc: '2.0', id: 2, method: 'resources/list', params: {} });
    const response = await readNextMessage(proc);

    const result = response.result as Record<string, unknown>;
    const resources = result.resources as Array<{ uri: string }>;
    assert.ok(Array.isArray(resources), 'resources should be an array');

    const uris = resources.map(r => r.uri);
    assert.ok(uris.includes('letsfg://guide'), 'guide resource must be registered');
  });
});

describe('MCP server — unknown method', () => {
  let proc: ChildProcessWithoutNullStreams;

  before(() => { proc = spawnServer(); });
  after(() => { proc.kill(); });

  it('returns method-not-found error for unknown methods', async () => {
    sendMessage(proc, { jsonrpc: '2.0', id: 42, method: 'nonexistent/method', params: {} });
    const response = await readNextMessage(proc);

    assert.equal(response.id, 42);
    const error = response.error as Record<string, unknown>;
    assert.ok(error, 'error field should be present');
    assert.equal(error.code, -32601);
  });
});
