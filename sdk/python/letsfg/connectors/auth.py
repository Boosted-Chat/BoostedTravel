"""
Card-backed authentication for LetsFG Programmatic Flight Search (PFS).

PFS needs a card connected before it can search or book. Nothing is charged to
connect: letsfg.co/connect runs a 0.00 Revolut setup that saves the card so a
booking can be charged later. You pay the fare only when you book, and it is
held, not taken, until the airline confirms.

This is a normal OAuth 2.1 + PKCE authorization-code flow. LetsFG advertises
authorization_endpoint = https://letsfg.co/connect and leaves /oauth/register
open (RFC 7591), so any client can register itself -- there is no allowlist and
no hosted-connector requirement.

Flow (one-time, `letsfg auth`):
  1. GET  /developers/api/.well-known/oauth-authorization-server   (discovery)
  2. POST /developers/api/oauth/register   with a loopback redirect_uri
  3. Open  https://letsfg.co/connect?...   a PERSON adds a card there
  4. POST /developers/api/oauth/token      code -> access + refresh token

The access token lasts about an hour; the refresh token lasts 30 days and
rotates on every use. Use ensure_bearer_token() in a long-lived process and it
refreshes silently.

There is NO endpoint that mints a token from card details. A person must
approve once in a browser -- do not ask a user for a card number.

RETIRED 2026-09-02: the Stripe enrolment lanes (setup_url, setup_session_id,
payment_method_id, card_token) and every token they issued.

NOTE: unrelated to `letsfg register` / `letsfg connect-payment`, which belong to
the separate, paid Developer API and create a billing account most agents do
not want.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

_BASE_URL = os.environ.get("LETSFG_BASE_URL", "https://letsfg.co")
_DEV_ROOT = f"{_BASE_URL}/developers/api"

# Access tokens are ~1h. Used only when the server does not say expires_in.
_FALLBACK_TTL = 55 * 60
# Refresh this long before expiry rather than racing the clock.
_REFRESH_SKEW = 5 * 60

# Must match client.py: Cloudflare blocks the urllib default UA with error 1010.
_USER_AGENT = "LetsFG-Python-SDK/1.0.3"
_CLIENT_NAME = "letsfg-python"
_SCOPE = "flights"


class BearerTokenError(Exception):
    """No valid Bearer token. Run `letsfg auth` to connect a card."""
    pass


# ── config ────────────────────────────────────────────────────────────────

def _config_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home()))
    else:
        base = Path.home()
    return base / ".letsfg" / "config.json"


def _load_config() -> dict:
    p = _config_path()
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return {}
    return {}


def _save_config(cfg: dict) -> None:
    p = _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cfg, indent=2))
    try:
        p.chmod(0o600)
    except Exception:
        pass


def save_token(
    token: str,
    expires_at: float | None = None,
    *,
    refresh_token: str | None = None,
    client_id: str | None = None,
) -> None:
    """Save a Bearer token (and its refresh material) to ~/.letsfg/config.json."""
    cfg = _load_config()
    auth = dict(cfg.get("pfs_auth") or {})
    auth["token"] = token
    auth["expires_at"] = expires_at if expires_at is not None else time.time() + _FALLBACK_TTL
    if refresh_token:
        auth["refresh_token"] = refresh_token
    if client_id:
        auth["client_id"] = client_id
    cfg["pfs_auth"] = auth
    _save_config(cfg)


def get_bearer_token() -> str:
    """
    Return a stored Bearer token, or raise. SYNCHRONOUS, so it cannot refresh --
    access tokens last about an hour, so a long-lived process should call
    ensure_bearer_token() and let it refresh silently.
    """
    env = os.environ.get("LETSFG_BEARER_TOKEN")
    if env:
        return env

    auth = _load_config().get("pfs_auth") or {}
    token = auth.get("token")
    if token and time.time() < float(auth.get("expires_at", 0)) - _REFRESH_SKEW:
        return token

    if auth.get("refresh_token"):
        raise BearerTokenError(
            "LetsFG token expired. Call ensure_bearer_token() to refresh it, "
            "or run:  letsfg auth"
        )

    raise BearerTokenError(
        "No valid LetsFG Bearer token.\n"
        "  Run:  letsfg auth        (connects a card at letsfg.co/connect - nothing is charged)\n"
        "  Or:   export LETSFG_BEARER_TOKEN=<token>"
    )


def ensure_bearer_token() -> str:
    """Return a valid token, refreshing with the stored refresh token if needed."""
    env = os.environ.get("LETSFG_BEARER_TOKEN")
    if env:
        return env

    auth = _load_config().get("pfs_auth") or {}
    token = auth.get("token")
    if token and time.time() < float(auth.get("expires_at", 0)) - _REFRESH_SKEW:
        return token
    if auth.get("refresh_token") and auth.get("client_id"):
        return refresh_access_token()
    return get_bearer_token()  # raises with the right guidance


# ── HTTP helpers ──────────────────────────────────────────────────────────

def _post_json(path: str, payload: dict, timeout: int = 30) -> tuple[int, dict]:
    """POST JSON and return (status, body). A 402 is an expected answer on the
    enrolment route, not an error, so HTTPError is unwrapped rather than raised."""
    req = Request(
        f"{_BASE_URL}{path}",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"error": raw[:400]}


def _post_form(url: str, fields: dict, timeout: int = 30) -> tuple[int, dict]:
    """POST application/x-www-form-urlencoded -- the OAuth token endpoint."""
    req = Request(
        url,
        data=urlencode(fields).encode(),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": _USER_AGENT,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"error": raw[:400]}


def _discover() -> dict:
    """RFC 8414 metadata, with the documented defaults as a fallback."""
    meta = {
        "authorization_endpoint": f"{_BASE_URL}/connect",
        "token_endpoint": f"{_DEV_ROOT}/oauth/token",
        "registration_endpoint": f"{_DEV_ROOT}/oauth/register",
    }
    try:
        req = Request(
            f"{_DEV_ROOT}/.well-known/oauth-authorization-server",
            headers={"User-Agent": _USER_AGENT},
        )
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        for k in meta:
            if isinstance(data.get(k), str):
                meta[k] = data[k]
    except (HTTPError, URLError, ValueError, OSError):
        pass  # the defaults above are the advertised values
    return meta


def _pkce() -> tuple[str, str]:
    """Return (verifier, S256 challenge)."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


_DONE_HTML = (
    b"<!doctype html><meta charset=utf-8>"
    b"<title>LetsFG connected</title>"
    b"<body style=\"font:16px system-ui;padding:3rem;text-align:center\">"
    b"<h2>Card connected.</h2>"
    b"<p>You can close this tab and go back to the terminal.</p>"
)
_FAIL_HTML = (
    b"<!doctype html><meta charset=utf-8>"
    b"<title>LetsFG</title>"
    b"<body style=\"font:16px system-ui;padding:3rem;text-align:center\">"
    b"<h2>Something went wrong.</h2>"
    b"<p>Go back to the terminal for the error.</p>"
)


class _CallbackServer:
    """Loopback listener for the OAuth redirect (RFC 8252)."""

    def __init__(self, expected_state: str):
        self._expected_state = expected_state
        self._result: dict = {}
        self._event = threading.Event()

        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                q = parse_qs(urlparse(self.path).query)
                code = (q.get("code") or [""])[0]
                state = (q.get("state") or [""])[0]
                err = (q.get("error") or [""])[0]

                ok = bool(code) and state == outer._expected_state and not err
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(_DONE_HTML if ok else _FAIL_HTML)

                if not outer._event.is_set():
                    if err:
                        outer._result = {"error": err}
                    elif not code:
                        outer._result = {"error": "no authorization code in the redirect"}
                    elif state != outer._expected_state:
                        # A mismatched state is the CSRF case: refuse the code.
                        outer._result = {"error": "state mismatch on the OAuth redirect"}
                    else:
                        outer._result = {"code": code}
                    outer._event.set()

            def log_message(self, *args):  # silence the default stderr logging
                pass

        self._httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._httpd.server_address[1]
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def wait_for_code(self, timeout: float = 600) -> str:
        if not self._event.wait(timeout):
            raise BearerTokenError(
                "Timed out waiting for the browser to come back. Run `letsfg auth` again."
            )
        if "error" in self._result:
            raise BearerTokenError(f"Authorization failed: {self._result['error']}")
        return self._result["code"]

    def close(self) -> None:
        try:
            self._httpd.shutdown()
            self._httpd.server_close()
        except Exception:
            pass


def refresh_access_token() -> str:
    """Swap the stored refresh token for a fresh access token. Rotates both."""
    auth = _load_config().get("pfs_auth") or {}
    refresh_token = auth.get("refresh_token")
    client_id = auth.get("client_id")
    if not refresh_token or not client_id:
        raise BearerTokenError("No refresh token stored. Run:  letsfg auth")

    meta = _discover()
    status, data = _post_form(
        meta["token_endpoint"],
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
        },
    )
    if status != 200 or not data.get("access_token"):
        raise BearerTokenError(
            f"Could not refresh the LetsFG token (HTTP {status}). Run:  letsfg auth"
        )

    expires_in = data.get("expires_in")
    expires_at = time.time() + (float(expires_in) if isinstance(expires_in, (int, float)) else _FALLBACK_TTL)
    save_token(
        str(data["access_token"]),
        expires_at,
        # The refresh token rotates on every use: store the new one or the next
        # refresh replays a spent token.
        refresh_token=data.get("refresh_token") or refresh_token,
        client_id=client_id,
    )
    return str(data["access_token"])


def connect_auth(open_browser: bool = True) -> str:
    """
    Interactive auth -- connects a card at letsfg.co/connect and stores a token.

    Registers this client, opens the card screen, and waits for the redirect.
    Nothing is charged. A person must approve in the browser.
    """
    meta = _discover()
    verifier, challenge = _pkce()
    state = base64.urlsafe_b64encode(secrets.token_bytes(16)).rstrip(b"=").decode()

    # Bind the loopback listener FIRST: redirect_uri has to match what we
    # register byte for byte, so the port must be known before registration.
    server = _CallbackServer(state)
    redirect_uri = f"http://127.0.0.1:{server.port}/callback"

    try:
        status, reg = _post_json_abs(
            meta["registration_endpoint"],
            {
                "client_name": _CLIENT_NAME,
                "redirect_uris": [redirect_uri],
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
                "token_endpoint_auth_method": "none",
            },
        )
        client_id = reg.get("client_id") if isinstance(reg, dict) else None
        if status not in (200, 201) or not client_id:
            raise BearerTokenError(
                f"Could not register with LetsFG (HTTP {status}). See {_BASE_URL}/for-agents"
            )

        auth_url = meta["authorization_endpoint"] + "?" + urlencode({
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "state": state,
            "scope": _SCOPE,
        })

        print("\n  LetsFG needs a card connected before it can search or book.")
        print("  Nothing is charged now - you pay the fare only when you book,")
        print("  and it is held, not taken, until the airline confirms.\n")
        print("  Open this and add a card (or pay 0.00 with Revolut Pay):\n")
        print(f"     {auth_url}\n")
        if open_browser:
            try:
                webbrowser.open(auth_url)
            except Exception:
                pass
        print("  Waiting for you to finish... ", end="", flush=True)

        code = server.wait_for_code()
        status, data = _post_form(
            meta["token_endpoint"],
            {
                "grant_type": "authorization_code",
                "code": code,
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "code_verifier": verifier,
            },
        )
        if status != 200 or not data.get("access_token"):
            detail = str(data.get("error_description") or data.get("error") or "").strip()
            raise BearerTokenError(
                f"Could not complete authentication (HTTP {status}). {detail}".strip()
            )

        expires_in = data.get("expires_in")
        expires_at = time.time() + (
            float(expires_in) if isinstance(expires_in, (int, float)) else _FALLBACK_TTL
        )
        save_token(
            str(data["access_token"]),
            expires_at,
            refresh_token=data.get("refresh_token"),
            client_id=str(client_id),
        )
        print("done. Card connected - the token refreshes itself from now on.")
        return str(data["access_token"])
    finally:
        server.close()


def _post_json_abs(url: str, payload: dict, timeout: int = 30) -> tuple[int, dict]:
    """_post_json against an absolute URL (registration_endpoint comes from
    discovery, so it is not necessarily under _BASE_URL)."""
    req = Request(
        url,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"error": raw[:400]}


# Back-compat name: `letsfg auth` used to mean the Stripe lane, now it is /connect.
payment_auth = connect_auth


def request_enrolment() -> dict:
    """
    The 402 envelope from /api/agent-access/request -- `add_card_url` and `how`.

    Still useful for an agent that wants the current instructions as JSON; it no
    longer carries a `setup_url`, because that lane is retired.
    """
    status, data = _post_json("/api/agent-access/request", {})
    if status not in (200, 402):
        raise BearerTokenError(
            f"Could not start authentication (HTTP {status}): {data.get('error', '')}"
        )
    return data


def verify_payment_method(*_args, **_kwargs) -> str:
    """Retired 2026-09-02. There is no endpoint that mints a token from card details."""
    raise BearerTokenError(
        "setup_session_id / payment_method_id / card_token were part of the Stripe\n"
        "enrolment, retired 2026-09-02, and every token they issued was revoked.\n"
        "There is no endpoint that mints a token from card details.\n"
        "  Run:  letsfg auth        (connects a card at letsfg.co/connect - nothing is charged)"
    )


def twitter_auth() -> str:
    """Deprecated alias -- Twitter/X auth was retired 2026-07-29."""
    import warnings
    warnings.warn(
        "twitter_auth() is deprecated: LetsFG retired Twitter/X auth on 2026-07-29. "
        "Use connect_auth() - nothing is charged.",
        DeprecationWarning,
        stacklevel=2,
    )
    return connect_auth()
