"""
LetsFG Connector Worker — Cloud Run service that runs ONE flight connector per request.
# v2026.04.18b — Re-enable Yatra; disable NH; add VA to proxy retry; Air China captcha fix

Called by the flight-search-worker (orchestrator) via HTTP fan-out.
Each Cloud Run instance handles exactly one connector at a time (concurrency=1).
Cloud Run auto-scales: 25 parallel requests = 25 separate instances.

Endpoint:
  POST /run    — Run one connector, return results as JSON
  GET  /health — Health check

Environment variables:
  WORKER_SECRET         — Shared secret for authenticating inbound requests
  LETSFG_MAX_BROWSERS   — Max concurrent Chromium processes (default: 1)
  CHROME_PATH           — Path to Chrome binary (set by Dockerfile)

Deploy (Cloud Run — NO --function flag):
  gcloud run deploy connector-worker \
    --source=. --project=sms-caller --region=us-central1 \
    --memory=2Gi --cpu=1 --concurrency=1 --max-instances=30 \\
    --timeout=120 --min-instances=0 --cpu-throttling --no-traffic

  Cost notes:
  - concurrency=1: each instance runs one browser connector at a time
  - max-instances=30: with 2-min cache + coalescing, ~6 users rarely need >30 parallel connectors
  - cpu-throttling: CPU allocated only during request processing
  - min-instances=0: scales to zero between searches (biggest cost saver)
"""

import asyncio
import logging
import os
import sys
import time

from flask import Flask, request, jsonify, abort

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("connector-worker")

# Log SDK version at import time for debugging
try:
    import letsfg as _lfg
    logger.info("letsfg SDK version: %s", getattr(_lfg, "__version__", "unknown"))
except Exception:
    logger.warning("Could not import letsfg SDK")

app = Flask(__name__)

WORKER_SECRET = os.environ.get("WORKER_SECRET", "")


def _verify_auth():
    """Verify inbound request has the correct shared secret."""
    if not WORKER_SECRET:
        return
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token or token != WORKER_SECRET:
        abort(401, "Unauthorized")


@app.route("/run", methods=["POST"])
def run():
    """
    Run a single flight connector and return results.

    JSON body:
      {
        "connector_id": "easyjet_direct",
        "origin": "LON",
        "destination": "IBZ",
        "date_from": "2026-04-14",
        "adults": 1,
        "currency": "EUR",
        "sibling_pairs": [["LHR", "IBZ"], ["LGW", "IBZ"]],
        "all_pairs": false
      }

    - all_pairs=false (default): Direct connector mode. Searches primary pair,
      then siblings only if primary returned results.
    - all_pairs=true: Fast connector mode. Searches all pairs sequentially
      using the same client instance (Ryanair, Wizzair, Kiwi).

    Returns: {"connector_id", "offers": [...], "total_results", "elapsed_seconds"}
    """
    _verify_auth()
    data = request.get_json(force=True)

    connector_id = data.get("connector_id")
    if not connector_id:
        return jsonify({"error": "Missing connector_id"}), 400

    try:
        result = asyncio.run(_execute(data))
        return jsonify(result)
    except Exception as exc:
        logger.exception("Connector %s failed: %s", connector_id, exc)
        # Return 200 with empty offers so orchestrator doesn't retry
        return jsonify({
            "connector_id": connector_id,
            "error": str(exc),
            "offers": [],
            "total_results": 0,
        })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "connector-worker"})


# ── Connector resolution ────────────────────────────────────────────────────

def _resolve_connector(connector_id: str):
    """Resolve connector_id to (class, timeout). Returns (None, 0) if not found."""
    # PATCHES FIRST: Local connector_patches/ for fixed/updated connectors
    import importlib
    _patches = {
        "ryanair_direct": ("connector_patches.ryanair", "RyanairConnectorClient", 20.0),
        "indigo_direct": ("letsfg.connectors.indigo", "IndiGoConnectorClient", 170.0),
        # delta_direct: disabled — Kasada rejects SwiftShader/Xvfb fingerprint on Cloud Run.
        # Works locally (20 offers, 36.8s) but Cloud Run gets 429 on offer-api-prd.delta.com
        # regardless of proxy location (EU/US) or JS-level fingerprint spoofing (canvas/WebGL/audio).
        # "delta_direct": ("letsfg.connectors.delta", "DeltaConnectorClient", 170.0),
    }
    if connector_id in _patches:
        mod_path, cls_name, timeout = _patches[connector_id]
        try:
            mod = importlib.import_module(mod_path)
            logger.info("Using patched connector: %s", connector_id)
            return getattr(mod, cls_name), timeout
        except Exception as exc:
            logger.warning("Failed to import patched connector %s: %s", connector_id, exc)

    # Main connector registry (triggers _safe_import for all — cached after first call)
    from letsfg.connectors.engine import _DIRECT_AIRLINE_connectorS

    for name, cls, timeout in _DIRECT_AIRLINE_connectorS:
        if name == connector_id:
            return cls, timeout

    # Fast connectors (not in _DIRECT_AIRLINE_connectorS)
    _fast = {
        "wizzair_direct": ("letsfg.connectors.wizzair", "WizzairConnectorClient", 15.0),
        "kiwi_connector": ("letsfg.connectors.kiwi", "KiwiConnectorClient", 25.0),
        "discover_direct": ("letsfg.connectors.discover", "DiscoverConnectorClient", 20.0),
    }
    if connector_id in _fast:
        mod_path, cls_name, timeout = _fast[connector_id]
        try:
            mod = importlib.import_module(mod_path)
            return getattr(mod, cls_name), timeout
        except Exception as exc:
            logger.warning("Failed to import fast connector %s: %s", connector_id, exc)
            return None, 0

    return None, 0


# ── Main execution logic ────────────────────────────────────────────────────

# CDP port mapping — browser connectors each hardcode their own port.
# Extracted from the SDK source. Used to pre-warm Chrome before search.
_BROWSER_CDP_PORTS: dict[str, int] = {
    "jetstar_direct": 9444, "scoot_direct": 9448,
    # easyjet_direct: patchright patch (no CDP)
    "edreams_ota": 9451, "opodo_ota": 9451,
    "skyscanner_meta": 9452,
    "etihad_direct": 9451, "smartwings_direct": 9452,
    "transavia_direct": 9453, "turkish_direct": 9453,
    # pegasus_direct: patchright patch (no CDP)
    "qatar_direct": 9454, "eurowings_direct": 9455, "westjet_direct": 9455,
    # latam_direct: patchright patch (no CDP)
    "copa_direct": 9487, "emirates_direct": 9457,
    "avianca_direct": 9458, "cebupacific_direct": 9459, "lot_direct": 9459,
    "porter_direct": 9512, "norwegian_direct": 9460,
    "jetsmart_direct": 9461, "volotea_direct": 9461,
    "singapore_direct": 9462,
    # spirit_direct: patchright patch (no CDP)
    "finnair_direct": 9465, "vietjet_direct": 9465,
    "peach_direct": 9468, "itaairways_direct": 9470,
    # american_direct: patchright patch (no CDP)
    # delta_direct: patchright patch (no CDP)
    "indigo_direct": 9473,
    "korean_direct": 9478, "traveloka_ota": 9480,
    "saudia_direct": 9481, "webjet_ota": 9482, "tiket_ota": 9483,
    "airchina_direct": 9491, "chinaeastern_direct": 9492,
    # chinasouthern_direct: patchright patch (no CDP)
    "asiana_direct": 9495,
    "airtransat_direct": 9496, "airserbia_direct": 9497,
    "aireuropa_direct": 9498, "mea_direct": 9499, "hainan_direct": 9500,
    "level_direct": 9503,
    "transnusa_direct": 9329, "superairjet_direct": 9331,
    "citilink_direct": 9335,
    "twayair_direct": 9451,
    "virginatlantic_direct": 9451,
    "lufthansa_direct": 9590, "swiss_direct": 9591,
    "austrian_direct": 9592, "brusselsairlines_direct": 9593,
}

# ── Proxy auth extension for CDP Chrome ──────────────────────────────────

# ── Local proxy relay for CDP Chrome ─────────────────────────────────────
#
# Chrome's --proxy-server flag does NOT support credentials, and the
# webRequest.onAuthRequired extension approach is unreliable with CONNECT
# tunnels.  Instead we start a tiny local relay on 127.0.0.1:8899 that
# forwards through the upstream residential proxy WITH credentials.
# Chrome points to localhost:8899 (no auth) — the relay adds auth.

_LOCAL_PROXY_PORT = 8899
_local_proxy_started = False
_local_proxy_upstream: str | None = None

_PROXY_RELAY_SCRIPT = r'''
import base64, os, select, socket, sys, threading

LISTEN = ("127.0.0.1", int(sys.argv[1]))
UPSTREAM_HOST = sys.argv[2]
UPSTREAM_PORT = int(sys.argv[3])
AUTH = base64.b64encode(f"{sys.argv[4]}:{sys.argv[5]}".encode()).decode()

# ── Bandwidth optimization: block Chrome background + analytics domains ──
# These domains burn 1-2 GB of proxy bandwidth per day and are never needed
# for flight scraping. Blocked at the TCP level before upstream connection.
_BLOCKED = frozenset({
    # Chrome background data hogs (observed: 863 MB on optimizationguide alone!)
    "optimizationguide-pa.googleapis.com",
    "edgedl.me.gvt1.com",
    "safebrowsing.googleapis.com",
    "update.googleapis.com",
    "clients2.google.com",
    "clients2.googleusercontent.com",
    "content-autofill.googleapis.com",
    "clientservices.googleapis.com",
    "sb-ssl.google.com",
    "accounts.google.com",
    # Google analytics / ads / tag managers (observed: 207 MB on GTM)
    "googletagmanager.com",
    "google-analytics.com",
    "googleadservices.com",
    "googlesyndication.com",
    "doubleclick.net",
    "pagead2.googlesyndication.com",
    # Facebook / social tracking
    "connect.facebook.net",
    "tr.snapchat.com",
    "ads.linkedin.com",
    "ads.twitter.com",
    # Analytics / heatmaps / error tracking
    "hotjar.com",
    "clarity.ms",
    "fullstory.com",
    "mixpanel.com",
    "segment.io",
    "amplitude.com",
    "heapanalytics.com",
    "sentry.io",
    # Ad networks
    "criteo.com",
    "taboola.com",
    "outbrain.com",
    "adnxs.com",
    "adsrvr.org",
    "amazon-adsystem.com",
    # Tracking pixels
    "bat.bing.com",
    "pixel.wp.com",
    # Chat / support widgets
    "intercom.io",
    "drift.com",
    "crisp.chat",
    "tawk.to",
    "livechatinc.com",
    "zendesk.com",
})

def _host_blocked(target):
    """Check if target host is in the blocklist (supports subdomains)."""
    # CONNECT: "host:port", HTTP: "http://host:port/path"
    t = target
    if "://" in t:
        t = t.split("://", 1)[1]
    h = t.split(":")[0].split("/")[0].lower()
    for d in _BLOCKED:
        if h == d or h.endswith("." + d):
            return True
    return False

def bridge(a, b):
    """Bidirectional socket relay."""
    socks = [a, b]
    try:
        while socks:
            rr, _, er = select.select(socks, [], socks, 120)
            if er:
                break
            for s in rr:
                data = s.recv(65536)
                if not data:
                    return
                dst = b if s is a else a
                dst.sendall(data)
    except Exception:
        pass
    finally:
        a.close()
        b.close()

def handle(csock):
    try:
        raw = b""
        while b"\r\n\r\n" not in raw:
            c = csock.recv(4096)
            if not c:
                csock.close()
                return
            raw += c
        first = raw.split(b"\r\n")[0].decode()
        method, target, _ = first.split(" ", 2)

        # Block bandwidth-wasting domains before opening upstream connection
        if _host_blocked(target):
            if method == "CONNECT":
                csock.sendall(b"HTTP/1.1 403 Blocked\r\n\r\n")
            csock.close()
            return

        up = socket.create_connection((UPSTREAM_HOST, UPSTREAM_PORT), timeout=15)
        if method == "CONNECT":
            host_port = target
            req = (f"CONNECT {host_port} HTTP/1.1\r\n"
                   f"Host: {host_port}\r\n"
                   f"Proxy-Authorization: Basic {AUTH}\r\n\r\n").encode()
            up.sendall(req)
            resp = b""
            while b"\r\n\r\n" not in resp:
                c = up.recv(4096)
                if not c:
                    break
                resp += c
            if b"200" in resp.split(b"\r\n")[0]:
                csock.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                bridge(csock, up)
            else:
                csock.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                csock.close()
                up.close()
        else:
            # Regular HTTP — inject auth header
            lines = raw.split(b"\r\n")
            new = [lines[0]]
            for ln in lines[1:]:
                if not ln.lower().startswith(b"proxy-authorization:"):
                    new.append(ln)
            idx = new.index(b"")
            new.insert(idx, f"Proxy-Authorization: Basic {AUTH}".encode())
            up.sendall(b"\r\n".join(new))
            while True:
                c = up.recv(65536)
                if not c:
                    break
                csock.sendall(c)
            csock.close()
            up.close()
    except Exception:
        try:
            csock.close()
        except Exception:
            pass

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(LISTEN)
srv.listen(50)
sys.stdout.write("RELAY_READY\n")
sys.stdout.flush()
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
'''


def _start_proxy_relay(proxy_url: str | None = None) -> bool:
    """Start the local proxy relay if an authenticated proxy is configured.

    Returns True if the relay is running (or was already started).
    If ``proxy_url`` is given, use it instead of env-var lookup.
    When a relay is already running but with a different upstream URL,
    kill it and restart with the new URL.
    """
    global _local_proxy_started, _local_proxy_upstream

    raw_url = proxy_url or os.environ.get("LETSFG_PROXY", "").strip()
    if not raw_url:
        raw_url = os.environ.get("RESIDENTIAL_PROXY_URL", "").strip()
    if not raw_url:
        logger.info("Proxy relay: no proxy URL available")
        return False
    if _local_proxy_started and raw_url == _local_proxy_upstream:
        return True
    from urllib.parse import urlparse, unquote
    p = urlparse(raw_url)
    if not p.username or not p.password:
        logger.info("Proxy relay: proxy URL has no auth (user=%s)", p.username)
        return False

    # URL-decode credentials — env vars often contain %2C (comma) etc.
    username = unquote(p.username)
    password = unquote(p.password)

    # If relay is running with a different upstream, kill it
    if _local_proxy_started and raw_url != _local_proxy_upstream:
        logger.info("Proxy relay: restarting with different upstream for %s", raw_url[:40])
        _local_proxy_started = False
        # Kill existing relay
        import subprocess as _sp2
        try:
            _sp2.run(["fuser", "-k", f"{_LOCAL_PROXY_PORT}/tcp"], capture_output=True, timeout=5)
        except Exception:
            pass

    import subprocess, tempfile, time

    script_path = "/tmp/_proxy_relay.py"
    with open(script_path, "w") as f:
        f.write(_PROXY_RELAY_SCRIPT)

    proc = subprocess.Popen(
        ["python3", script_path, str(_LOCAL_PROXY_PORT),
         p.hostname, str(p.port or 1000), username, password],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    # Wait for "RELAY_READY"
    for _ in range(20):
        line = proc.stdout.readline()
        if b"RELAY_READY" in line:
            _local_proxy_started = True
            _local_proxy_upstream = raw_url
            logger.info("Proxy relay listening on 127.0.0.1:%d", _LOCAL_PROXY_PORT)
            return True
        time.sleep(0.1)
    logger.error("Proxy relay failed to start")
    return False


def _kill_chrome_on_port(port: int) -> None:
    """Kill any process listening on the given CDP port (Linux container)."""
    import subprocess as _sp
    try:
        result = _sp.run(
            ["fuser", "-k", f"{port}/tcp"],
            capture_output=True, timeout=5,
        )
        logger.info("Killed Chrome on port %d (fuser rc=%d)", port, result.returncode)
    except FileNotFoundError:
        # fuser not installed — try lsof
        try:
            result = _sp.run(
                ["lsof", "-ti", f":{port}"],
                capture_output=True, text=True, timeout=5,
            )
            for pid in result.stdout.strip().split():
                _sp.run(["kill", "-9", pid], timeout=5)
                logger.info("Killed PID %s on port %d", pid, port)
        except Exception as e:
            logger.debug("Port kill fallback failed: %s", e)
    except Exception as e:
        logger.debug("fuser kill failed: %s", e)


async def _pre_warm_chrome(connector_id: str, use_proxy: bool = False) -> None:
    """Pre-launch Chrome on the connector's CDP port so it's ready for search.

    SDK connectors launch Chrome with subprocess + asyncio.sleep(2s),
    which isn't enough in containers (headed Chrome + Xvfb needs ~6s).
    By pre-launching here with a proper wait, the connector's _get_browser()
    finds Chrome already running and connects instantly.

    Args:
        use_proxy: When True, launch Chrome with --proxy-server pointing at
                   the local relay.  When False, launch direct (no proxy).
    """
    port = _BROWSER_CDP_PORTS.get(connector_id)
    if not port:
        return

    import socket
    # Check if Chrome is already on this port (reused instance)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(("127.0.0.1", port))
        s.close()
        logger.info("Chrome already running on port %d", port)
        return
    except OSError:
        pass

    import subprocess
    import importlib
    from letsfg.connectors.browser import find_chrome, proxy_chrome_args, disable_background_networking_args

    chrome = find_chrome()
    user_data_dir = f"/tmp/chrome_{port}"
    os.makedirs(user_data_dir, exist_ok=True)

    # Try to use connector-specific Chrome flags (critical for WAF-sensitive
    # connectors like Pegasus whose Akamai requires specific flag sets).
    connector_flags = None
    try:
        for suffix in ("_direct", "_meta", "_ota", "_connector"):
            if connector_id.endswith(suffix):
                mod_name = connector_id[: -len(suffix)]
                break
        else:
            mod_name = connector_id
        mod = importlib.import_module(f"letsfg.connectors.{mod_name}")
        raw_flags = getattr(mod, "_CHROME_FLAGS", None)
        if raw_flags:
            # Strip proxy args — we add proxy separately below.
            connector_flags = [f for f in raw_flags if not f.startswith("--proxy-server")]
            logger.info("Using connector-specific Chrome flags for %s (%d flags)", connector_id, len(connector_flags))
    except Exception:
        pass

    if connector_flags:
        # Always add background networking blocks even with custom flags.
        # Chrome background traffic (optimizationguide, safebrowsing, etc.)
        # burns 1+ GB of proxy bandwidth if not disabled.
        bg_flags = []
        flag_str = " ".join(connector_flags)
        if "--disable-background-networking" not in flag_str:
            bg_flags.append("--disable-background-networking")
        if "--disable-component-update" not in flag_str:
            bg_flags.append("--disable-component-update")
        if "--host-rules" not in flag_str:
            bg_flags.append(
                "--host-rules="
                "MAP optimizationguide-pa.googleapis.com 0.0.0.0,"
                "MAP edgedl.me.gvt1.com 0.0.0.0,"
                "MAP safebrowsing.googleapis.com 0.0.0.0,"
                "MAP www.googletagmanager.com 0.0.0.0,"
                "MAP update.googleapis.com 0.0.0.0,"
                "MAP clients2.google.com 0.0.0.0,"
                "MAP content-autofill.googleapis.com 0.0.0.0"
            )
        args = [
            chrome,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={user_data_dir}",
            *connector_flags,
            *bg_flags,
            "about:blank",
        ]
    else:
        args = [
            chrome,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={user_data_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            *disable_background_networking_args(),
            "about:blank",
        ]

    # Only route Chrome through proxy when explicitly requested.
    # Previously this always checked RESIDENTIAL_PROXY_URL, causing
    # ERR_TUNNEL_CONNECTION_FAILED on the "no proxy" first attempt.
    if use_proxy:
        if _local_proxy_started:
            # Relay already running (started by _run_once) — just point Chrome at it
            args.insert(-1, f"--proxy-server=http://127.0.0.1:{_LOCAL_PROXY_PORT}")
            logger.info("Pre-warm: using proxy relay for %s", connector_id)
        else:
            # Try to start relay from available proxy URLs
            connector_proxy = None
            for prefix, env_var in _CONNECTOR_PROXY_MAP.items():
                if connector_id.startswith(prefix):
                    connector_proxy = os.environ.get(env_var, "").strip()
                    if connector_proxy:
                        logger.info("Pre-warm: found connector-specific proxy %s for %s", env_var, connector_id)
                    break
            proxy_url = connector_proxy or os.environ.get("RESIDENTIAL_PROXY_URL", "").strip()
            if proxy_url and _start_proxy_relay(proxy_url):
                args.insert(-1, f"--proxy-server=http://127.0.0.1:{_LOCAL_PROXY_PORT}")
                logger.info("Pre-warm: proxy relay ready for %s (upstream=%s)", connector_id, proxy_url[:50])
            elif proxy_url:
                p_args = proxy_chrome_args()
                for a in p_args:
                    args.insert(-1, a)
                logger.info("Pre-warm: relay failed, using fallback proxy_chrome_args=%s for %s", p_args, connector_id)
            else:
                logger.info("Pre-warm: proxy requested but none available for %s", connector_id)
    else:
        logger.info("Pre-warm: direct Chrome for %s (no proxy)", connector_id)

    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info("Pre-warming Chrome on port %d (pid %d)", port, proc.pid)

    # Wait for CDP port to be ready (up to 10s)
    for _ in range(20):
        await asyncio.sleep(0.5)
        if proc.poll() is not None:
            logger.error("Chrome exited early (code %s) on port %d", proc.returncode, port)
            return
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(("127.0.0.1", port))
            s.close()
            logger.info("Chrome ready on port %d (%.1fs)", port, 0.5 * (_ + 1))
            return
        except OSError:
            pass

    logger.warning("Chrome not ready on port %d after 10s", port)

_CONNECTOR_PROXY_MAP: dict[str, str] = {
    # Maps connector name prefix → env-var name for its proxy
    "skyscanner": "SKYSCANNER_PROXY",
    "kayak": "KAYAK_PROXY",
    "momondo": "KAYAK_PROXY",
    "cheapflights": "KAYAK_PROXY",
    "edreams": "ODIGEO_PROXY",
    "opodo": "ODIGEO_PROXY",
    "tripcom": "TRIPCOM_PROXY",
    "citilink": "CITILINK_PROXY",
    "spirit": "SPIRIT_PROXY",
}

# Geo-blocked connectors: skip direct attempt, always use proxy (saves ~30s per search)
# These return "Access Denied" / empty from Cloud Run IPs but work via residential proxy.
_PROXY_ALWAYS: set[str] = {
    "citilink_direct",       # Indonesia only — non-ID IPs get 403
    "latam_direct",          # Cloud Run IPs always get "Access Denied" without proxy
    "indigo_direct",         # Akamai blocks from GCP IPs — needs residential proxy
    "flydubai_direct",       # Akamai/WAF blocks GCP IPs — works via residential
    "pegasus_direct",        # Akamai blocks from GCP IPs — needs residential proxy
    "chinasouthern_direct",  # GCP IPs geo-redirect to csair.com/us/en/ — needs EU residential proxy
    # delta_direct: disabled (Kasada rejects SwiftShader/Xvfb fingerprint)
}

# Connectors known to be blocked from GCP IPs that should use the residential proxy
_PROXY_RECOMMENDED: set[str] = {
    "ryanair_direct",
    "easyjet_direct",
    "norwegian_direct",
    "etihad_direct",
    "wego_meta",
    "aireuropa_direct",
    "turkish_direct",
    # Lufthansa Group — curl_cffi requests to lufthansa.com blocked from GCP
    "lufthansa_direct",
    "swiss_direct",
    "austrian_direct",
    "brusselsairlines_direct",
    # ITA Airways — Cloudflare WAF blocks GCP IPs
    "itaairways_direct",
    # EveryMundo curl_cffi connectors blocked from GCP
    "icelandair_direct",
    "evaair_direct",
    # EveryMundo / API connectors — Cloudflare blocks httpx TLS fingerprint
    "tap_direct",
    "flair_direct",
    # API connectors that fingerprint TLS
    "salamair_direct",
    "airbaltic_direct",
    # CDP browser connectors — WAF blocks GCP datacenter IPs
    "transavia_direct",
    "emirates_direct",
    "spirit_direct",
    "pegasus_direct",
    "citilink_direct",
    "singapore_direct",
    "scoot_direct",
    "jetstar_direct",
    "vietjet_direct",
    "copa_direct",
    "indigo_direct",
    # delta_direct: disabled (Kasada rejects SwiftShader/Xvfb fingerprint)
    "american_direct",
    "latam_direct",
    "porter_direct",
    "smartwings_direct",
    "airserbia_direct",
    "traveloka_ota",
    "tiket_ota",
    "webjet_ota",
    # Per-search Playwright browser connectors
    "sunexpress_direct",
    "gol_direct",
    "flynas_direct",
    "airasia_direct",
    "united_direct",
    # httpx connectors — Crane IBE returns truncated HTML (no prices) to GCP IPs
    "pia_direct",
    # curl_cffi / httpx connectors — GCP IPs blocked or rate-limited
    "iwantthatflight_direct",
    "skiplagged_meta",
    "klm_direct",
    "mea_direct",
    "olympicair_direct",
    "zipair_direct",
    # Direct API connectors — work locally, blocked from GCP IPs
    "spicejet_direct",
    "chinaairlines_direct",
    "kenyaairways_direct",
    # curl_cffi connectors patched via Dockerfile sed to use proxy
    "flydubai_direct",
    "flybondi_direct",
    # httpx connectors needing proxy for GCP IPs
    "arajet_direct",
    "despegar_ota",
    "saa_direct",
    "airarabia_direct",
    "flyarystan_direct",
    # CDP browser connectors — WAF blocks GCP IPs
    "cheapflights_meta",
    # US connectors — anti-bot blocks from GCP IPs
    "avelo_direct",
    "hawaiian_direct",
    "alaska_direct",
    "allegiant_direct",
    "southwest_direct",
    "suncountry_direct",
    # curl_cffi connectors patched via Dockerfile sed for proxy
    "aircalin_direct",
    # CDP browser connectors — missing from pre-warm, WAF blocks GCP
    "aireuropa_direct",
    "airtransat_direct",
    "asiana_direct",
    "chinaeastern_direct",
    "chinasouthern_direct",
    "citilink_direct",
    "hainan_direct",
    "level_direct",
    "saudia_direct",
    "superairjet_direct",
    "transnusa_direct",
    "airchina_direct",
    # PW browser connectors — proxy-aware, blocked from GCP
    "luckyair_direct",
    "usbangla_direct",
    "azul_direct",
    "breeze_direct",
    "volaris_direct",
    "airasiax_direct",
    # HYBRID connectors — curl_cffi fast path needs proxy
    "eurowings_direct",
    "twayair_direct",
    "volotea_direct",
    # nodriver connectors
    "batikair_direct",
    "nh_direct",
    # CDP browser connectors — Incapsula/WAF blocks GCP
    "bangkokairways_direct",
    # CDP + GraphQL interception — Akamai blocks GCP IPs
    "virginatlantic_direct",
}


def _get_residential_proxy_url() -> str | None:
    """Return the residential proxy URL from env vars.

    Reads ``RESIDENTIAL_PROXY_URL`` (preferred, full URL including auth).
    Falls back to legacy ``DECODO_PROXY_SERVER/USER/PASS`` for backwards compat.
    """
    # Preferred: single URL
    url = os.environ.get("RESIDENTIAL_PROXY_URL", "").strip()
    if url:
        return url
    # Legacy fallback: Decodo-style split vars
    server = os.environ.get("DECODO_PROXY_SERVER", "").strip()
    user = os.environ.get("DECODO_PROXY_USER", "").strip()
    passwd = os.environ.get("DECODO_PROXY_PASS", "").strip()
    if not server:
        return None
    if user and passwd:
        from urllib.parse import urlparse, urlunparse
        p = urlparse(server)
        netloc = f"{user}:{passwd}@{p.hostname}:{p.port or 10001}"
        return urlunparse(p._replace(netloc=netloc))
    return server


def _inject_proxy_for_connector(connector_id: str) -> str | None:
    """Set LETSFG_PROXY from the connector-specific env var, if available.

    Falls back to residential proxy for connectors in _PROXY_RECOMMENDED.
    Returns the previous LETSFG_PROXY value (or None) for restoration.
    """
    old_val = os.environ.get("LETSFG_PROXY")

    # 1. Check per-connector proxy env var
    for prefix, env_var in _CONNECTOR_PROXY_MAP.items():
        if connector_id.startswith(prefix):
            proxy_url = os.environ.get(env_var)
            if proxy_url:
                os.environ["LETSFG_PROXY"] = proxy_url
                logger.info("Injected %s → LETSFG_PROXY for %s", env_var, connector_id)
                return old_val
            break

    # 2. Fall back to residential proxy for blocked connectors
    if connector_id in _PROXY_RECOMMENDED:
        proxy_url = _get_residential_proxy_url()
        if proxy_url:
            os.environ["LETSFG_PROXY"] = proxy_url
            # Some connectors read their own env vars instead of LETSFG_PROXY
            _CONNECTOR_ENV_VARS = {
                "breeze_direct": "BREEZE_PROXY",
                "allegiant_direct": "ALLEGIANT_PROXY",
                "avelo_direct": "AVELO_PROXY",
                "southwest_direct": "SOUTHWEST_PROXY",
                "american_direct": "AMERICAN_PROXY",
                "delta_direct": "DELTA_PROXY",
                "itaairways_direct": "ITA_PROXY",
                "jetblue_direct": "JETBLUE_PROXY",
                "bangkokairways_direct": "BANGKOKAIRWAYS_PROXY",
                "citilink_direct": "CITILINK_PROXY",
            }
            extra_var = _CONNECTOR_ENV_VARS.get(connector_id)
            if extra_var:
                os.environ[extra_var] = proxy_url
                logger.info("Also set %s for %s", extra_var, connector_id)
            logger.info("Injected residential proxy → LETSFG_PROXY for %s", connector_id)
            return old_val

    # No proxy — clear LETSFG_PROXY if it was set from a previous run
    if old_val:
        del os.environ["LETSFG_PROXY"]
    return old_val


def _restore_proxy(old_val: str | None) -> None:
    """Restore LETSFG_PROXY to its previous value."""
    if old_val is None:
        os.environ.pop("LETSFG_PROXY", None)
    else:
        os.environ["LETSFG_PROXY"] = old_val
    # Clean up connector-specific env vars
    for var in ("BREEZE_PROXY", "ALLEGIANT_PROXY", "AVELO_PROXY",
                "SOUTHWEST_PROXY", "AMERICAN_PROXY", "DELTA_PROXY",
                "ITA_PROXY", "JETBLUE_PROXY", "BANGKOKAIRWAYS_PROXY",
                "CITILINK_PROXY"):
        os.environ.pop(var, None)


def _has_dedicated_proxy_env(connector_id: str) -> bool:
    """Return True if connector has a mapped dedicated proxy env var prefix."""
    return any(connector_id.startswith(prefix) for prefix in _CONNECTOR_PROXY_MAP)


def _looks_proxy_block(error_text: str) -> bool:
    """Heuristic for anti-bot/network blocks where proxy retry is worth it."""
    if not error_text:
        return False
    s = error_text.lower()
    tokens = (
        "403", "401", "429", "forbidden", "blocked", "access denied",
        "captcha", "cloudflare", "akamai", "waf", "ssl", "tls",
        "connection reset", "timeout", "timed out", "proxy", "challenge",
    )
    return any(t in s for t in tokens)


def _retry_on_empty_set() -> set[str]:
    """Connectors that should do a proxy retry even with no explicit error.

    Configure with LETSFG_PROXY_RETRY_ON_EMPTY="id1,id2,...".
    """
    # Keep this list tight to control proxy spend.
    defaults = {
        # Known to frequently return empty from GCP without explicit block errors
        "easyjet_direct", "norwegian_direct",
        "lufthansa_direct", "swiss_direct", "austrian_direct", "brusselsairlines_direct",
        "mea_direct", "chinasouthern_direct", "chinaeastern_direct", "airchina_direct",
        # Akamai blocks silently (403 page renders but connector doesn't detect)
        "citilink_direct",
        # Spirit Akamai/PerimeterX returns 403 on token endpoint from GCP IPs
        "spirit_direct",
        # Return 0 offers from GCP with no explicit error signal
        "pegasus_direct", "porter_direct",
        # delta_direct: disabled (Kasada rejects SwiftShader/Xvfb fingerprint)
        "american_direct", "latam_direct",
        # Akamai hard-blocks GCP IPs — needs residential proxy
        "virginatlantic_direct",
    }
    raw = os.environ.get("LETSFG_PROXY_RETRY_ON_EMPTY", "")
    custom = {x.strip() for x in raw.split(",") if x.strip()}
    return defaults | custom


async def _execute(params: dict) -> dict:
    """Import, instantiate, and run a single connector."""
    from datetime import date as date_cls
    from letsfg.models.flights import FlightSearchRequest

    connector_id = params["connector_id"]
    origin = params["origin"].strip().upper()
    destination = params["destination"].strip().upper()
    date_from = params["date_from"].strip()
    adults = int(params.get("adults", 1))
    currency = params.get("currency", "EUR")
    sibling_pairs = params.get("sibling_pairs") or []
    all_pairs = params.get("all_pairs", False)
    return_date = (params.get("return_date") or "").strip() or None

    t0 = time.monotonic()

    # Clear proxy env BEFORE module imports so that connectors with
    # module-level *proxy_chrome_args() in _CHROME_FLAGS don't bake in
    # stale proxy args from a previous request.
    _saved_proxy = os.environ.pop("LETSFG_PROXY", None)

    connector_cls, timeout = _resolve_connector(connector_id)

    # Restore after imports — _run_once will manage LETSFG_PROXY per attempt.
    if _saved_proxy is not None:
        os.environ["LETSFG_PROXY"] = _saved_proxy

    if connector_cls is None:
        return {
            "connector_id": connector_id,
            "error": f"Unknown or unavailable connector: {connector_id}",
            "offers": [],
            "total_results": 0,
        }

    req = FlightSearchRequest(
        origin=origin,
        destination=destination,
        date_from=date_cls.fromisoformat(date_from),
        adults=adults,
        currency=currency,
        **({"return_from": date_cls.fromisoformat(return_date)} if return_date else {}),
    )

    async def _run_once(use_proxy: bool) -> tuple[list, str]:
        """Run one connector attempt with current proxy mode.

        Returns (offers, error_text).
        """
        error_text = ""
        # Preserve and restore env/proxy per attempt.
        old_proxy = os.environ.get("LETSFG_PROXY")
        if use_proxy:
            old_proxy = _inject_proxy_for_connector(connector_id)
            injected_url = os.environ.get("LETSFG_PROXY", "")
            if injected_url and _start_proxy_relay(injected_url):
                os.environ["LETSFG_PROXY"] = f"http://127.0.0.1:{_LOCAL_PROXY_PORT}"
        else:
            os.environ.pop("LETSFG_PROXY", None)
            for var in ("BREEZE_PROXY", "ALLEGIANT_PROXY", "AVELO_PROXY",
                        "SOUTHWEST_PROXY", "AMERICAN_PROXY", "DELTA_PROXY",
                        "ITA_PROXY", "JETBLUE_PROXY"):
                os.environ.pop(var, None)

        # Proxy traffic is slower, so allow more cold-start overhead.
        cold_start_buffer = 40.0 if use_proxy else 20.0
        client = connector_cls(timeout=timeout)
        offers = []
        try:
            await _pre_warm_chrome(connector_id, use_proxy=use_proxy)
            if all_pairs:
                pairs = [(origin, destination)] + [(p[0], p[1]) for p in sibling_pairs]
                for o, d in pairs:
                    sub_req = (
                        req.model_copy(update={"origin": o, "destination": d})
                        if (o, d) != (origin, destination) else req
                    )
                    try:
                        result = await asyncio.wait_for(
                            client.search_flights(sub_req),
                            timeout=timeout + cold_start_buffer,
                        )
                        for offer in result.offers:
                            offer.source = connector_id
                            offer.source_tier = "free"
                        offers.extend(result.offers)
                        logger.info("%s %s->%s: %d offers (proxy=%s)",
                                    connector_id, o, d, len(result.offers), use_proxy)
                    except Exception as exc:
                        error_text = str(exc)
                        logger.warning("%s %s->%s failed (proxy=%s): %s",
                                       connector_id, o, d, use_proxy, exc)
            else:
                result = await asyncio.wait_for(
                    client.search_flights(req),
                    timeout=timeout + cold_start_buffer,
                )
                for offer in result.offers:
                    offer.source = connector_id
                    offer.source_tier = "free"
                offers.extend(result.offers)

                if offers and sibling_pairs:
                    wall_budget = timeout * 2.5 + cold_start_buffer
                    for sib in sibling_pairs:
                        remaining = wall_budget - (time.monotonic() - t0)
                        if remaining < 10.0:
                            logger.info("%s: wall-clock budget exhausted, skipping siblings", connector_id)
                            break
                        sub_req = req.model_copy(update={"origin": sib[0], "destination": sib[1]})
                        sib_timeout = min(timeout * 0.75 + 5.0, remaining)
                        try:
                            sub_result = await asyncio.wait_for(
                                client.search_flights(sub_req),
                                timeout=sib_timeout,
                            )
                            for offer in sub_result.offers:
                                offer.source = connector_id
                                offer.source_tier = "free"
                            offers.extend(sub_result.offers)
                            logger.info("%s sibling %s->%s: %d offers (proxy=%s)",
                                        connector_id, sib[0], sib[1], len(sub_result.offers), use_proxy)
                        except Exception as exc:
                            error_text = str(exc)
                            logger.debug("%s sibling %s->%s failed (proxy=%s): %s",
                                         connector_id, sib[0], sib[1], use_proxy, exc)
        except asyncio.TimeoutError:
            error_text = "hard timeout"
            logger.warning("%s: hard timeout (proxy=%s)", connector_id, use_proxy)
        except Exception as exc:
            error_text = str(exc)
            logger.warning("%s: run failed (proxy=%s): %s", connector_id, use_proxy, exc)
        finally:
            try:
                await client.close()
            except Exception:
                pass
            await _cleanup_browser(client)
            _restore_proxy(old_proxy)
        return offers, error_text

    # Smart proxy strategy (default): direct first, then proxy only when needed.
    # - always: preserve old behavior for proxy candidates
    # - smart: save proxy bandwidth/cost by avoiding blind proxy usage
    # - never: force direct (debug/testing)
    proxy_mode = os.environ.get("LETSFG_PROXY_MODE", "smart").strip().lower()
    is_candidate = _has_dedicated_proxy_env(connector_id) or connector_id in _PROXY_RECOMMENDED
    retry_on_empty = connector_id in _retry_on_empty_set()

    if proxy_mode == "never" or not is_candidate:
        attempt_plan = [False]
    elif proxy_mode == "always" or connector_id in _PROXY_ALWAYS:
        attempt_plan = [True]
    else:
        # smart
        attempt_plan = [False, True]

    all_offers: list = []
    last_error = ""
    for idx, use_proxy in enumerate(attempt_plan):
        all_offers, last_error = await _run_once(use_proxy)
        if all_offers:
            break
        if idx == 0 and len(attempt_plan) > 1:
            # Escalate to proxy only for likely blocked/error cases, or explicit override.
            if retry_on_empty or _looks_proxy_block(last_error):
                # Kill any lingering Chrome on this connector's CDP port so
                # the proxy retry can launch a fresh Chrome WITH proxy args.
                cdp_port = _BROWSER_CDP_PORTS.get(connector_id)
                if cdp_port:
                    _kill_chrome_on_port(cdp_port)
                    await asyncio.sleep(1.0)
                logger.info("%s: retrying with proxy (reason=%s)",
                            connector_id,
                            "retry_on_empty" if retry_on_empty else (last_error or "blocked"))
                continue
            logger.info("%s: not retrying with proxy (no block signal)", connector_id)
            break

    elapsed = time.monotonic() - t0
    logger.info("%s: %d total offers in %.1fs (proxy_mode=%s)",
                connector_id, len(all_offers), elapsed, proxy_mode)

    offers_json = [o.model_dump(mode="json") for o in all_offers]
    return {
        "connector_id": connector_id,
        "offers": offers_json,
        "total_results": len(offers_json),
        "elapsed_seconds": round(elapsed, 1),
    }


async def _cleanup_browser(client):
    """Clean up browser resources after a connector finishes."""
    try:
        from letsfg.connectors.browser import cleanup_module_browsers, cleanup_all_browsers
        mod = sys.modules.get(type(client).__module__)
        if mod:
            await cleanup_module_browsers(mod)
        await cleanup_all_browsers()
    except Exception:
        pass
