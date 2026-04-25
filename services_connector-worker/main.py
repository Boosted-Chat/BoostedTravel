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
        --memory=2Gi --cpu=1 --concurrency=1 --max-instances=80 \\
                --timeout=120 --min-instances=0 --cpu-throttling --no-traffic

  Cost notes:
  - concurrency=1: each instance runs one browser connector at a time
    - max-instances=80: better headroom for fan-out bursts without front-door throttling
        - min-instances=0: scales to zero when idle
  - cpu-throttling: CPU allocated only during request processing
        - increase min-instances only if cold-start buffering is worth the extra idle cost
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

from flask import Flask, request, jsonify, abort

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("connector-worker")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= minimum else default


def _default_snapshot_root() -> Path:
    explicit = os.environ.get("LETSFG_SNAPSHOT_CACHE_DIR", "").strip()
    if explicit:
        return Path(explicit)
    local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
    if local_app_data:
        return Path(local_app_data) / "letsfg" / "snapshots"
    return Path(tempfile.gettempdir()) / "letsfg-snapshots"


_CONNECTOR_SEARCH_SNAPSHOT_DIR = _default_snapshot_root() / "worker_connector"
_connector_search_snapshot_mem: dict[str, dict] = {}


def _snapshot_eligible(connector_id: str) -> bool:
    return (
        connector_id in _CONNECTOR_CACHE_SNAPSHOTS
        or connector_id in _BROWSER_CDP_PORTS
        or connector_id.endswith("_meta")
        or connector_id.endswith("_ota")
    )


def _build_connector_search_snapshot_key(params: dict) -> str:
    sibling_pairs = params.get("sibling_pairs") or []
    normalized_pairs = [
        [str(pair[0]).strip().upper(), str(pair[1]).strip().upper()]
        for pair in sibling_pairs
        if isinstance(pair, (list, tuple)) and len(pair) >= 2
    ]
    payload = {
        "connector_id": str(params.get("connector_id", "")).strip(),
        "origin": str(params.get("origin", "")).strip().upper(),
        "destination": str(params.get("destination", "")).strip().upper(),
        "date_from": str(params.get("date_from", "")).strip(),
        "return_date": str(params.get("return_date", "") or "").strip(),
        "adults": int(params.get("adults", 1) or 1),
        "currency": str(params.get("currency", "EUR") or "EUR").strip().upper(),
        "all_pairs": bool(params.get("all_pairs", False)),
        "sibling_pairs": normalized_pairs,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]
    return f"{payload['connector_id']}__{digest}"


def _prune_connector_search_snapshot_mem() -> None:
    max_entries = _env_int("LETSFG_CONNECTOR_CACHE_MAX_ENTRIES", 512, minimum=1)
    if len(_connector_search_snapshot_mem) <= max_entries:
        return
    excess = len(_connector_search_snapshot_mem) - max_entries
    oldest = sorted(
        _connector_search_snapshot_mem.items(),
        key=lambda item: float(item[1].get("ts", 0.0)),
    )[:excess]
    for key, _ in oldest:
        _connector_search_snapshot_mem.pop(key, None)


def _read_connector_search_snapshot_entry(key: str) -> dict | None:
    in_mem = _connector_search_snapshot_mem.get(key)
    if isinstance(in_mem, dict):
        return in_mem

    if not _env_bool("LETSFG_SNAPSHOT_CACHE_DISK_ENABLED", True):
        return None

    path = _CONNECTOR_SEARCH_SNAPSHOT_DIR / f"{key}.json"
    if not path.exists():
        return None
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(loaded, dict) or not isinstance(loaded.get("response"), dict):
        return None
    _connector_search_snapshot_mem[key] = loaded
    _prune_connector_search_snapshot_mem()
    return loaded


def _write_connector_search_snapshot_entry(key: str, response: dict) -> None:
    entry = {
        "ts": time.time(),
        "response": response,
    }
    _connector_search_snapshot_mem[key] = entry
    _prune_connector_search_snapshot_mem()

    if not _env_bool("LETSFG_SNAPSHOT_CACHE_DISK_ENABLED", True):
        return

    path = _CONNECTOR_SEARCH_SNAPSHOT_DIR / f"{key}.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(entry, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp_path, path)
    except Exception:
        pass


def _load_connector_search_snapshot_response(
    key: str, max_age_sec: int,
) -> tuple[dict, int] | None:
    entry = _read_connector_search_snapshot_entry(key)
    if not entry:
        return None
    try:
        age_sec = int(max(0, time.time() - float(entry.get("ts", 0.0))))
    except Exception:
        return None
    if age_sec > max_age_sec:
        return None
    payload = entry.get("response")
    if not isinstance(payload, dict):
        return None
    return dict(payload), age_sec

# Log SDK version at import time for debugging
try:
    import letsfg as _lfg
    logger.info("letsfg SDK version: %s", getattr(_lfg, "__version__", "unknown"))
except Exception:
    logger.warning("Could not import letsfg SDK")

# ---------------------------------------------------------------------------
# GCS Chrome Cache Snapshots — reduces proxy bandwidth by ~80% for covered airlines
# ---------------------------------------------------------------------------
# Pre-built Chrome disk cache for top bandwidth-consuming connectors.
# On cold start, we download the snapshot from GCS (2-3s, free same-region egress)
# and extract to the user-data-dir. Chrome then finds JS bundles in disk cache
# and skips the 4-5MB SPA download through proxy.

_GCS_CACHE_BUCKET = os.environ.get("GCS_CACHE_BUCKET", "letsfg-chrome-cache")
_GCS_CACHE_ENABLED = os.environ.get("GCS_CACHE_ENABLED", "1") == "1"
_CONNECTOR_USER_DATA_DIR_ENV = "LETSFG_CONNECTOR_USER_DATA_DIR"

# Connector ID → snapshot filename in GCS (without .tar.gz extension)
# Only browser connectors that load full SPAs benefit from this.
# Generated by generate_cache_snapshots.py — only includes sites that produced >100KB cache.
_CONNECTOR_CACHE_SNAPSHOTS: dict[str, str] = {
    # ── Original 17 ──
    "ryanair_direct": "ryanair",              # 1.8 MB
    "easyjet_direct": "easyjet",              # 4.0 MB
    "wizzair_direct": "wizzair",              # 569 KB
    "vueling_direct": "vueling",              # 5.1 MB
    "pegasus_direct": "pegasus",              # 136 KB
    "emirates_direct": "emirates",            # 3.3 MB
    "spirit_direct": "spirit",                # 477 KB
    "frontier_direct": "frontier",            # 5.4 MB
    "jetblue_direct": "jetblue",              # 4.5 MB
    "southwest_direct": "southwest",          # 20.1 MB
    "scoot_direct": "scoot",                  # 188 KB
    "vietjet_direct": "vietjet",              # 54.1 MB
    "airasia_direct": "airasia",              # 8.1 MB
    "copa_direct": "copa",                    # 4.6 MB
    "volaris_direct": "volaris",              # 13.8 MB
    "suncountry_direct": "suncountry",        # 939 KB
    "skyscanner_meta": "skyscanner",          # 674 KB
    # ── Batch 4 (edreams, opodo, westjet, cebupacific, jetsmart) ──
    "edreams_ota": "edreams",                 # 6.7 MB
    "opodo_ota": "opodo",                     # 9.2 MB
    "westjet_direct": "westjet",              # 9.9 MB
    "cebupacific_direct": "cebupacific",      # 450 KB
    "jetsmart_direct": "jetsmart",            # 11.5 MB
    # ── Batch 5 (finnair, traveloka, saudia, airchina) ──
    "finnair_direct": "finnair",              # 1.9 MB
    "traveloka_ota": "traveloka",             # 4.6 MB
    "saudia_direct": "saudia",                # 9.5 MB
    "airchina_direct": "airchina",            # 3.5 MB
    # ── Batch 6 (airtransat, hainan, transnusa) ──
    "airtransat_direct": "airtransat",        # 3.9 MB
    "hainan_direct": "hainan",                # 2.7 MB
    "transnusa_direct": "transnusa",          # 143 KB
    # ── Batch 7 (virginatlantic, chinasouthern) ──
    "virginatlantic_direct": "virginatlantic", # 2.5 MB
    "chinasouthern_direct": "chinasouthern",  # 1.2 MB
    # ── Batch 8 (meta-search engines + priority OTAs) — 2026-04-20 ──
    "momondo_meta": "momondo",                # 5.1 MB
    "kayak_meta": "kayak",                    # 5.6 MB
    "cheapflights_meta": "cheapflights",      # 1.9 MB
    "aviasales_meta": "aviasales",            # 4.2 MB
    "agoda_meta": "agoda",                    # 4.9 MB
    "tripcom_ota": "tripcom",                 # 5.4 MB
    "bookingcom_ota": "bookingcom",           # 3.2 MB
    "lastminute_ota": "lastminute",           # 100 KB (borderline)
    # ── Batch 9 (OTA expansion + ANA) — 2026-04-20 ──
    "nh_direct": "nh",                        # 17.3 MB
    "airasiamove_ota": "airasiamove",        # 0.8 MB
    "akbartravels_ota": "akbartravels",      # 1.6 MB
    "almosafer_ota": "almosafer",            # 5.6 MB
    "cleartrip_ota": "cleartrip",            # 4.2 MB
    "esky_ota": "esky",                      # 5.3 MB
    "etraveli_ota": "etraveli",              # 3.0 MB
    "flightcatchers_ota": "flightcatchers",  # 3.2 MB
    "musafir_ota": "musafir",                # 6.4 MB
    "travelstart_ota": "travelstart",        # 3.7 MB
    "traveltrolley_ota": "traveltrolley",    # 3.3 MB
    "travix_ota": "travix",                  # 1.9 MB
    # ── Batch 10 (priority browser connectors in headed mode) — 2026-04-20 ──
    "wego_meta": "wego",                     # 18.2 MB
    "united_direct": "united",               # 18.6 MB
    "singapore_direct": "singapore",         # 6.9 MB
    "korean_direct": "korean",               # 6.1 MB
    "qatar_direct": "qatar",                 # 4.2 MB
    "etihad_direct": "etihad",               # 3.4 MB
    "turkish_direct": "turkish",             # 4.8 MB
    "american_direct": "american",           # 4.4 MB
    "delta_direct": "delta",                 # 5.2 MB
        # ── Batch 11 (remaining viable batch-3 connectors) — 2026-04-20 ──
        "aerolineas_direct": "aerolineas",       # 7.8 MB
        "airasiax_direct": "airasiax",           # 0.8 MB
        "airnewzealand_direct": "airnewzealand", # 1.5 MB
        "alaska_direct": "alaska",               # 6.1 MB
        "avelo_direct": "avelo",                 # 4.7 MB
        "condor_direct": "condor",               # 5.4 MB
        "hawaiian_direct": "hawaiian",           # 8.6 MB
        "luckyair_direct": "luckyair",           # 3.6 MB
        "philippineairlines_direct": "philippineairlines", # 4.2 MB
        "qantas_direct": "qantas",               # 3.6 MB
        "royaljordanian_direct": "royaljordanian", # 10.6 MB
        "samoaairways_direct": "samoaairways",   # 0.9 MB
        "skyairline_direct": "skyairline",       # 7.9 MB
        "sunexpress_direct": "sunexpress",       # 0.7 MB
        "usbangla_direct": "usbangla",           # 1.6 MB
        "wingo_direct": "wingo",                 # 5.0 MB
        # ── Batch 12 (headed rescue: anti-bot/tiny recoveries) — 2026-04-20/21 ──
        "aireuropa_direct": "aireuropa",         # 1.6 MB
        "airserbia_direct": "airserbia",         # 6.4 MB
        "asiana_direct": "asiana",               # 1.8 MB
        "auntbetty_ota": "auntbetty",            # 1.9 MB
        "avianca_direct": "avianca",             # 7.1 MB
        "azul_direct": "azul",                   # 0.2 MB
        "bangkokairways_direct": "bangkokairways", # 9.8 MB
        "batikair_direct": "batikair",           # 6.9 MB
        "breeze_direct": "breeze",               # 23.0 MB
        "byojet_ota": "byojet",                  # 1.6 MB
        "chinaeastern_direct": "chinaeastern",   # 21.3 MB
        "chinaairlines_direct": "chinaairlines", # 5.9 MB
        "eurowings_direct": "eurowings",         # 4.3 MB
        "flybondi_direct": "flybondi",           # 5.0 MB
        "flydubai_direct": "flydubai",           # 3.1 MB
        "flynas_direct": "flynas",               # 13.9 MB
        "gol_direct": "gol",                     # 3.7 MB
        "indigo_direct": "indigo",               # 7.5 MB
        "jet2_direct": "jet2",                   # 5.0 MB
        "jetstar_direct": "jetstar",             # 7.4 MB
        "kuwaitairways_direct": "kuwaitairways", # 8.5 MB
        "latam_direct": "latam",                 # 8.4 MB
        "level_direct": "level",                 # 5.9 MB
        "lot_direct": "lot",                     # 3.6 MB
        "mea_direct": "mea",                     # 2.6 MB
        "norwegian_direct": "norwegian",         # 1.3 MB
        "peach_direct": "peach",                 # 3.2 MB
        "porter_direct": "porter",               # 6.0 MB
        "smartwings_direct": "smartwings",       # 4.9 MB
        "superairjet_direct": "superairjet",     # 23.0 MB
        "tiket_ota": "tiket",                    # 12.9 MB
        "transavia_direct": "transavia",         # 1.2 MB
        "twayair_direct": "twayair",             # 12.1 MB
        "volotea_direct": "volotea",             # 5.8 MB
        "webjet_ota": "webjet",                  # 6.6 MB
        "yatra_ota": "yatra",                    # 1.8 MB
        "zipair_direct": "zipair",               # 5.9 MB
}

_cache_download_lock = asyncio.Lock()
_cache_downloaded: set[str] = set()  # Track already-downloaded snapshots this instance

# Lazy-init GCS client (avoids import at startup if GCS disabled)
_gcs_client = None


def _get_gcs_client():
    """Get (or create) the GCS client singleton."""
    global _gcs_client
    if _gcs_client is None:
        from google.cloud import storage
        _gcs_client = storage.Client()
    return _gcs_client


def _download_gcs_cache_sync(connector_id: str, user_data_dir: str) -> bool:
    """Download Chrome cache snapshot from GCS and extract to user_data_dir.
    
    Returns True if snapshot was downloaded and extracted, False otherwise.
    This is a blocking sync function — call from async code with run_in_executor.
    """
    logger.info("GCS cache check for %s (enabled=%s, bucket=%s)", connector_id, _GCS_CACHE_ENABLED, _GCS_CACHE_BUCKET)
    if not _GCS_CACHE_ENABLED:
        logger.info("GCS cache disabled, skipping download for %s", connector_id)
        return False
    
    snapshot_name = _CONNECTOR_CACHE_SNAPSHOTS.get(connector_id)
    if not snapshot_name:
        logger.info("No cache snapshot mapped for %s", connector_id)
        return False
    
    # Check if already downloaded this session
    if connector_id in _cache_downloaded:
        logger.debug("Cache snapshot already downloaded for %s", connector_id)
        return True
    
    # Check if user_data_dir already has cache (warm instance)
    cache_dir = os.path.join(user_data_dir, "Default", "Cache")
    if os.path.exists(cache_dir) and os.listdir(cache_dir):
        logger.info("Chrome cache already exists at %s, skipping GCS download", cache_dir)
        _cache_downloaded.add(connector_id)
        return True
    
    import io
    import tarfile
    
    blob_name = f"{snapshot_name}.tar.gz"
    
    try:
        start = time.time()
        
        # Download from GCS using Python client
        client = _get_gcs_client()
        bucket = client.bucket(_GCS_CACHE_BUCKET)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            logger.warning("GCS cache snapshot not found: %s/%s", _GCS_CACHE_BUCKET, blob_name)
            return False
        
        # Download to memory and extract
        data = blob.download_as_bytes()
        
        # Extract to user_data_dir
        os.makedirs(user_data_dir, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tar.extractall(user_data_dir)
        
        elapsed = time.time() - start
        logger.info("Downloaded and extracted cache snapshot for %s in %.1fs (%.1f MB)", 
                   connector_id, elapsed, len(data) / 1024 / 1024)
        _cache_downloaded.add(connector_id)
        
        return True
        
    except Exception as e:
        logger.warning("GCS cache download failed for %s: %s", connector_id, e)
        return False


def _prepare_connector_user_data_dir_sync(connector_id: str) -> str | None:
    """Prepare a persistent browser profile for non-CDP snapshot consumers."""
    if connector_id not in _CONNECTOR_CACHE_SNAPSHOTS:
        return None
    if connector_id in _BROWSER_CDP_PORTS:
        return None

    profile_key = hashlib.sha256(connector_id.encode("utf-8")).hexdigest()[:12]
    user_data_dir = os.path.join(tempfile.gettempdir(), f"chrome_profile_{profile_key}")
    os.makedirs(user_data_dir, exist_ok=True)

    restored = _download_gcs_cache_sync(connector_id, user_data_dir)
    cache_dir = os.path.join(user_data_dir, "Default", "Cache")
    has_cache = os.path.exists(cache_dir) and bool(os.listdir(cache_dir))
    if restored or has_cache:
        return user_data_dir
    return None


app = Flask(__name__)

WORKER_SECRET = os.environ.get("WORKER_SECRET", "")


def _verify_auth():
    """Verify inbound request has the correct shared secret."""
    if not WORKER_SECRET:
        return
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token or not hmac.compare_digest(token, WORKER_SECRET):
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
        "cheapflights_meta": ("connector_patches.cheapflights", "CheapflightsConnectorClient", 70.0),
        "despegar_ota": ("connector_patches.despegar", "DespegarConnectorClient", 70.0),
        "kiwi_connector": ("connector_patches.kiwi", "KiwiConnectorClient", 25.0),
        "momondo_meta": ("connector_patches.momondo", "MomondoConnectorClient", 70.0),
        "ryanair_direct": ("connector_patches.ryanair", "RyanairConnectorClient", 20.0),
        "serpapi_google": ("connector_patches.serpapi_google", "SerpApiGoogleConnectorClient", 45.0),
        "skyscanner_meta": ("connector_patches.skyscanner", "SkyscannerConnectorClient", 55.0),
        "tripcom_ota": ("connector_patches.tripcom", "TripcomConnectorClient", 70.0),
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
    "edreams_ota": 9504, "opodo_ota": 9504,
    "skyscanner_meta": 9452,
    "aviasales_meta": 9465,
    "lastminute_ota": 9464,
    "travix_ota": 9466,
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
        data = request.get_json(force=True)
        connector_id = data.get("connector_id")
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

    # Download pre-built Chrome cache snapshot from GCS (if available)
    # This populates disk cache with JS bundles, reducing proxy bandwidth ~80%
    if connector_id in _CONNECTOR_CACHE_SNAPSHOTS:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _download_gcs_cache_sync, connector_id, user_data_dir)

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
    "skyscanner_meta",       # PX blocks GCP IPs — needs residential proxy with sticky session
    "wego_meta",             # Direct path burns ~68s on Turnstile; proxy path now passes Cloudflare
    "travix_ota",            # Direct path burns ~56s before proxy retry; go proxy-first under 120s timeout
    "webjet_ota",            # Matrix URL serves security verification from GCP; go proxy-first
    # delta_direct: disabled (Kasada rejects SwiftShader/Xvfb fingerprint)
}

# Connectors known to be blocked from GCP IPs that should use the residential proxy
_PROXY_RECOMMENDED: set[str] = {
    "ryanair_direct",
    "easyjet_direct",
    "norwegian_direct",
    "etihad_direct",
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
    "kayak_meta",
    "momondo_meta",
    "aviasales_meta",
    "travix_ota",
    "tripcom_ota",
    "opodo_ota",
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
    "despegar_ota",
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


def _proxy_env_var_for_connector(connector_id: str) -> str | None:
    """Return the dedicated proxy env var for a connector, if one exists."""
    for prefix, env_var in _CONNECTOR_PROXY_MAP.items():
        if connector_id.startswith(prefix):
            return env_var
    return None


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
        # Meta/OTA connectors that frequently return silent-empty on GCP IPs
        # unless retried through residential proxy.
        "cheapflights_meta", "kayak_meta", "momondo_meta", "aviasales_meta", "travix_ota",
        "tripcom_ota", "opodo_ota", "despegar_ota",
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
        limit=max(1, min(int(params.get("limit", 100) or 100), 200)),
        **({"return_from": date_cls.fromisoformat(return_date)} if return_date else {}),
    )

    snapshot_enabled = _env_bool("LETSFG_CONNECTOR_CACHE_ENABLED", True)
    snapshot_key = _build_connector_search_snapshot_key(params)
    snapshot_ttl = _env_int("LETSFG_CONNECTOR_CACHE_TTL_SEC", 1200, minimum=1)
    snapshot_empty_enabled = _env_bool("LETSFG_CONNECTOR_CACHE_EMPTY_ENABLED", True)
    snapshot_empty_ttl = _env_int("LETSFG_CONNECTOR_CACHE_EMPTY_TTL_SEC", 300, minimum=1)
    snapshot_eligible = _snapshot_eligible(connector_id)

    if (
        snapshot_enabled
        and snapshot_eligible
        and _env_bool("LETSFG_CONNECTOR_CACHE_SKIP_LIVE_BROWSER", True)
    ):
        fresh_hit = _load_connector_search_snapshot_response(
            snapshot_key,
            max_age_sec=max(snapshot_ttl, snapshot_empty_ttl),
        )
        cached_result = None
        age_sec = 0
        if fresh_hit:
            cached_result, age_sec = fresh_hit
            is_empty_snapshot = int(cached_result.get("total_results", 0) or 0) <= 0
            allowed_ttl = snapshot_empty_ttl if is_empty_snapshot else snapshot_ttl
            if is_empty_snapshot and not snapshot_empty_enabled:
                cached_result = None
            elif age_sec > allowed_ttl:
                cached_result = None

        if fresh_hit and cached_result is not None:
            response = dict(cached_result)
            response["elapsed_seconds"] = round(time.monotonic() - t0, 1)
            logger.info(
                "%s: connector snapshot hit (%d offers, age=%ds, kind=%s) — skipping live run",
                connector_id,
                int(response.get("total_results", 0) or 0),
                age_sec,
                "empty" if int(response.get("total_results", 0) or 0) <= 0 else "offers",
            )
            return response

    async def _run_once(use_proxy: bool) -> tuple[list, str]:
        """Run one connector attempt with current proxy mode.

        Returns (offers, error_text).
        """
        error_text = ""
        had_user_data_dir = _CONNECTOR_USER_DATA_DIR_ENV in os.environ
        old_user_data_dir = os.environ.get(_CONNECTOR_USER_DATA_DIR_ENV, "")
        prepared_user_data_dir = None
        if connector_id in _CONNECTOR_CACHE_SNAPSHOTS and connector_id not in _BROWSER_CDP_PORTS:
            loop = asyncio.get_running_loop()
            prepared_user_data_dir = await loop.run_in_executor(
                None,
                _prepare_connector_user_data_dir_sync,
                connector_id,
            )
        # Preserve and restore env/proxy per attempt.
        old_proxy = os.environ.get("LETSFG_PROXY")
        dedicated_proxy_var = _proxy_env_var_for_connector(connector_id)
        had_dedicated_proxy = bool(dedicated_proxy_var) and dedicated_proxy_var in os.environ
        old_dedicated_proxy = os.environ.get(dedicated_proxy_var, "") if dedicated_proxy_var else ""
        if use_proxy:
            old_proxy = _inject_proxy_for_connector(connector_id)
            injected_url = os.environ.get("LETSFG_PROXY", "")
            if dedicated_proxy_var:
                os.environ.pop(dedicated_proxy_var, None)
            if injected_url and _start_proxy_relay(injected_url):
                relay_url = f"http://127.0.0.1:{_LOCAL_PROXY_PORT}"
                os.environ["LETSFG_PROXY"] = relay_url
                if dedicated_proxy_var:
                    os.environ[dedicated_proxy_var] = relay_url
        else:
            os.environ.pop("LETSFG_PROXY", None)
            if dedicated_proxy_var:
                os.environ.pop(dedicated_proxy_var, None)
            for var in ("BREEZE_PROXY", "ALLEGIANT_PROXY", "AVELO_PROXY",
                        "SOUTHWEST_PROXY", "AMERICAN_PROXY", "DELTA_PROXY",
                        "ITA_PROXY", "JETBLUE_PROXY"):
                os.environ.pop(var, None)

        if prepared_user_data_dir:
            os.environ[_CONNECTOR_USER_DATA_DIR_ENV] = prepared_user_data_dir
            logger.info(
                "%s: snapshot-backed user data dir ready at %s",
                connector_id,
                prepared_user_data_dir,
            )
        else:
            os.environ.pop(_CONNECTOR_USER_DATA_DIR_ENV, None)

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
            if dedicated_proxy_var:
                if had_dedicated_proxy:
                    os.environ[dedicated_proxy_var] = old_dedicated_proxy
                else:
                    os.environ.pop(dedicated_proxy_var, None)
            if had_user_data_dir:
                os.environ[_CONNECTOR_USER_DATA_DIR_ENV] = old_user_data_dir
            else:
                os.environ.pop(_CONNECTOR_USER_DATA_DIR_ENV, None)
        return offers, error_text

    # Smart proxy strategy (default): direct first, then proxy only when needed.
    # - always: preserve old behavior for proxy candidates
    # - smart: save proxy bandwidth/cost by avoiding blind proxy usage
    # - never: force direct (debug/testing)
    proxy_mode = os.environ.get("LETSFG_PROXY_MODE", "smart").strip().lower()
    is_candidate = (
        _has_dedicated_proxy_env(connector_id)
        or connector_id in _PROXY_RECOMMENDED
        or connector_id in _PROXY_ALWAYS
    )
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
    response = {
        "connector_id": connector_id,
        "offers": offers_json,
        "total_results": len(offers_json),
        "elapsed_seconds": round(elapsed, 1),
    }

    if snapshot_enabled and snapshot_eligible and (
        response["total_results"] > 0 or snapshot_empty_enabled
    ):
        _write_connector_search_snapshot_entry(snapshot_key, response)

    return response


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
