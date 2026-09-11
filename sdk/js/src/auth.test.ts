import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// auth.ts resolves its config path from APPDATA (win32) or HOME (posix) at
// call time, so redirecting both env vars to a scratch dir before each test
// keeps this suite from ever touching a real ~/.letsfg/config.json.
let scratchDir: string;
let savedEnv: Record<string, string | undefined>;

beforeEach(() => {
  scratchDir = mkdtempSync(join(tmpdir(), 'letsfg-auth-test-'));
  savedEnv = {
    APPDATA: process.env.APPDATA,
    HOME: process.env.HOME,
    LETSFG_BEARER_TOKEN: process.env.LETSFG_BEARER_TOKEN,
  };
  process.env.APPDATA = scratchDir;
  process.env.HOME = scratchDir;
  delete process.env.LETSFG_BEARER_TOKEN;
});

afterEach(() => {
  for (const [key, val] of Object.entries(savedEnv)) {
    if (val === undefined) delete process.env[key];
    else process.env[key] = val;
  }
  rmSync(scratchDir, { recursive: true, force: true });
});

describe('getBearerToken / saveToken', () => {
  it('throws BearerTokenError when nothing is configured', async () => {
    const { getBearerToken, BearerTokenError } = await import('./auth.js');
    assert.throws(() => getBearerToken(), BearerTokenError);
  });

  it('returns LETSFG_BEARER_TOKEN env var immediately, without touching the config file', async () => {
    process.env.LETSFG_BEARER_TOKEN = 'example-env-token';
    const { getBearerToken } = await import('./auth.js');
    assert.equal(getBearerToken(), 'example-env-token');
  });

  it('round-trips a saved token through the config file', async () => {
    const { saveToken, getBearerToken } = await import('./auth.js');
    saveToken('saved-token-456', Date.now() + 90 * 24 * 3600 * 1000);
    assert.equal(getBearerToken(), 'saved-token-456');
  });

  it('treats a saved token past its expiry (minus buffer) as invalid', async () => {
    const { saveToken, getBearerToken, BearerTokenError } = await import('./auth.js');
    saveToken('expired-token', Date.now() + 1000); // expires in 1s, well inside the 1h buffer
    assert.throws(() => getBearerToken(), BearerTokenError);
  });
});

describe('the retired Stripe lanes', () => {
  it('verifyPaymentMethod refuses instead of calling a dead endpoint', async () => {
    // It used to POST /api/agent-access/verify, which now answers 410 for a
    // Stripe credential. An old caller deserves the reason, not a raw 410.
    const { verifyPaymentMethod, BearerTokenError } = await import('./auth.js');
    await assert.rejects(
      () => verifyPaymentMethod({ cardToken: 'tok_xxxx' }),
      (e: unknown) => {
        assert.ok(e instanceof BearerTokenError);
        assert.match((e as Error).message, /retired/i);
        assert.match((e as Error).message, /letsfg auth/);
        return true;
      },
    );
  });
});

describe('refreshAccessToken', () => {
  it('refreshes and stores the ROTATED refresh token', async () => {
    const { saveToken, refreshAccessToken, getBearerToken } = await import('./auth.js');
    saveToken('old-access', Date.now() - 1000, { refresh_token: 'r1', client_id: 'lfg_client_x' });

    const originalFetch = globalThis.fetch;
    let tokenBody = '';
    globalThis.fetch = (async (url: string, init?: RequestInit) => {
      if (String(url).includes('.well-known')) {
        return { ok: false, status: 404, json: async () => ({}) } as unknown as Response;
      }
      tokenBody = String(init?.body ?? '');
      return {
        status: 200,
        json: async () => ({ access_token: 'example-new-access', refresh_token: 'r2', expires_in: 3600 }),
      } as Response;
    }) as typeof fetch;

    try {
      const token = await refreshAccessToken();
      assert.equal(token, 'example-new-access');
      // spec encoding, not JSON
      assert.match(tokenBody, /grant_type=refresh_token/);
      assert.match(tokenBody, /refresh_token=r1/);
      assert.equal(getBearerToken(), 'example-new-access');
      // the rotated token must replace the used one, or the next refresh 400s
      const { refreshAccessToken: again } = await import('./auth.js');
      globalThis.fetch = (async (url: string, init?: RequestInit) => {
        if (String(url).includes('.well-known')) {
          return { ok: false, status: 404, json: async () => ({}) } as unknown as Response;
        }
        tokenBody = String(init?.body ?? '');
        return { status: 200, json: async () => ({ access_token: 'a3', expires_in: 3600 }) } as Response;
      }) as typeof fetch;
      await again();
      assert.match(tokenBody, /refresh_token=r2/, 'must send the rotated token, not the original');
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it('tells you to run letsfg auth when there is nothing to refresh with', async () => {
    const { saveToken, refreshAccessToken, BearerTokenError } = await import('./auth.js');
    saveToken('t', Date.now() + 60_000); // no refresh_token / client_id
    await assert.rejects(() => refreshAccessToken(), BearerTokenError);
  });
});
