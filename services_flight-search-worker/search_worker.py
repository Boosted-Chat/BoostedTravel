"""
Core search logic — two-phase flight search with fan-out orchestration.

Phase 1 (~3s): LetsFG API backend
  - Calls api.letsfg.co for GDS/NDC results (Duffel, Amadeus, Sabre, Travelport)
  - 400+ airlines including full-service carriers (BA, Lufthansa, Emirates, etc.)

Phase 2 (~60s): Fan-out to connector-worker instances
  - Fires N parallel HTTP calls to the connector-worker Cloud Run service
  - Each call runs ONE airline connector on its OWN Cloud Run instance
  - 174 connectors registered, route-filtered to ~15-40 per search
  - Cloud Run auto-scales: 25 parallel requests = 25 separate instances
  - No Chrome/Playwright needed here — the connector-worker handles that

Round-trip searches fire outbound + return directions in parallel,
then combine one-way legs via the combo engine for virtual interlining
(e.g. Ryanair outbound + Wizzair return).

Each phase calls back to the workflow engine as soon as results are ready.
"""

import asyncio
import hashlib
import logging
import os
import time
import urllib.request
from typing import Any

import httpx

logger = logging.getLogger("worker.search")

# ── Cloud Run identity token for service-to-service auth ────────────────────

_identity_token: str = ""
_identity_token_expiry: float = 0.0


def _get_identity_token(audience: str) -> str:
    """Fetch a Google-signed identity token from GCP metadata server.

    Uses the compute metadata endpoint directly (no extra deps needed).
    Caches the token and refreshes 5 minutes before expiry.
    On non-GCP environments (local dev), returns empty string.
    """
    global _identity_token, _identity_token_expiry

    if _identity_token and time.time() < _identity_token_expiry:
        return _identity_token

    try:
        url = (
            "http://metadata.google.internal/computeMetadata/v1/"
            f"instance/service-accounts/default/identity?audience={audience}"
        )
        req = urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            token = resp.read().decode("utf-8").strip()
        _identity_token = token
        # Token is valid for ~1 hour, refresh 5 min early
        _identity_token_expiry = time.time() + 3300
        logger.info("Fetched identity token for %s", audience)
        return token
    except Exception as exc:
        logger.warning("Failed to fetch identity token: %s — falling back to secret", exc)
        return ""


# ── Config ──────────────────────────────────────────────────────────────────

LETSFG_API_KEY = os.environ.get("LETSFG_API_KEY", "")
LETSFG_BASE_URL = os.environ.get("LETSFG_BASE_URL", "https://api.letsfg.co")
CALLBACK_SECRET = os.environ.get("CALLBACK_SECRET", "")
CONNECTOR_WORKER_URL = os.environ.get("CONNECTOR_WORKER_URL", "")
CONNECTOR_WORKER_SECRET = os.environ.get("CONNECTOR_WORKER_SECRET", "")
FANOUT_TIMEOUT = float(os.environ.get("FANOUT_TIMEOUT", "90"))

# ── Connector registry ──────────────────────────────────────────────────────
# Static list of all direct airline connectors: (name, timeout_seconds).
# Kept in sync with letsfg SDK engine.py _DIRECT_AIRLINE_connectorS.
# Avoids importing engine.py which would pull in Playwright/Chrome dependencies.
# Last synced: 2026-03-24 — 174 connectors.

CONNECTOR_REGISTRY: list[tuple[str, float]] = [
    # ── API / httpx connectors (fastest, 15-20s) ────────────────────────
    ("airbaltic_direct", 15.0),
    ("arajet_direct", 15.0),
    ("flyarystan_direct", 15.0),
    ("jazeera_direct", 15.0),
    ("austrian_direct", 20.0),
    ("brusselsairlines_direct", 20.0),
    ("discover_direct", 20.0),
    ("egyptair_direct", 20.0),
    ("garuda_direct", 20.0),
    ("itaairways_direct", 20.0),
    ("iwantthatflight_direct", 20.0),
    ("jal_direct", 20.0),
    ("lufthansa_direct", 20.0),
    ("olympicair_direct", 20.0),
    ("salamair_direct", 20.0),
    ("skyexpress_direct", 20.0),
    ("swiss_direct", 20.0),
    # ── Mixed API + light browser (25s) ─────────────────────────────────
    ("airarabia_direct", 25.0),
    ("airasia_direct", 25.0),
    ("aircalin_direct", 25.0),
    ("airfrance_direct", 25.0),
    ("airgreenland_direct", 25.0),
    ("airindia_direct", 25.0),
    ("airindiaexpress_direct", 25.0),
    ("airmauritius_direct", 25.0),
    ("airniugini_direct", 25.0),
    ("airpeace_direct", 25.0),
    ("airseychelles_direct", 25.0),
    ("airtahitinui_direct", 25.0),
    ("airvanuatu_direct", 25.0),
    ("akasa_direct", 25.0),
    ("allegiant_direct", 25.0),
    ("azerbaijanairlines_direct", 25.0),
    ("azoresairlines_direct", 25.0),
    ("azul_direct", 25.0),
    ("batikair_direct", 25.0),
    ("biman_direct", 25.0),
    ("britishairways_direct", 25.0),
    ("caribbeanairlines_direct", 25.0),
    ("cathay_direct", 25.0),
    ("cebupacific_direct", 25.0),
    ("condor_direct", 25.0),
    ("cyprusairways_direct", 25.0),
    ("despegar_ota", 25.0),
    ("easyjet_direct", 25.0),
    ("eurowings_direct", 25.0),
    ("evaair_direct", 25.0),
    ("fijiairways_direct", 25.0),
    ("flair_direct", 25.0),
    ("flyadeal_direct", 25.0),
    ("flybondi_direct", 25.0),
    ("flydubai_direct", 25.0),
    ("flynas_direct", 25.0),
    ("flysafair_direct", 25.0),
    ("frontier_direct", 25.0),
    ("gol_direct", 25.0),
    ("iberia_direct", 25.0),
    ("iberiaexpress_direct", 25.0),
    ("icelandair_direct", 25.0),
    ("indigo_direct", 25.0),
    ("jejuair_direct", 25.0),
    ("jet2_direct", 25.0),
    ("jetblue_direct", 25.0),
    ("jetsmart_direct", 25.0),
    ("jetstar_direct", 25.0),
    ("klm_direct", 25.0),
    ("malaysia_direct", 25.0),
    ("nokair_direct", 25.0),
    ("norwegian_direct", 25.0),
    ("omanair_direct", 25.0),
    ("peach_direct", 25.0),
    ("pegasus_direct", 25.0),
    ("pia_direct", 25.0),
    ("play_direct", 25.0),
    ("porter_direct", 25.0),
    ("qantas_direct", 25.0),
    ("rex_direct", 25.0),
    ("rwandair_direct", 25.0),
    ("sas_direct", 25.0),
    ("scoot_direct", 25.0),
    ("smartwings_direct", 25.0),
    ("solomonairlines_direct", 25.0),
    ("southwest_direct", 25.0),
    ("spicejet_direct", 25.0),
    ("spirit_direct", 25.0),
    ("spring_direct", 25.0),
    ("srilankan_direct", 25.0),
    ("starlux_direct", 25.0),
    ("sunexpress_direct", 25.0),
    ("thai_direct", 25.0),
    ("transavia_direct", 25.0),
    ("twayair_direct", 25.0),
    ("vietjet_direct", 25.0),
    ("virginatlantic_direct", 25.0),
    ("virginaustralia_direct", 25.0),
    ("vivaaerobus_direct", 25.0),
    ("volaris_direct", 25.0),
    ("volotea_direct", 25.0),
    ("vueling_direct", 25.0),
    ("zipair_direct", 25.0),
    # ── Medium timeout (30-35s) ─────────────────────────────────────────
    ("9air_direct", 30.0),
    ("airnorth_direct", 30.0),
    ("luckyair_direct", 30.0),
    ("serpapi_google", 30.0),
    ("chinaairlines_direct", 35.0),
    ("etihad_direct", 35.0),
    ("linkairways_direct", 35.0),
    ("pngair_direct", 35.0),
    # ── CDP browser connectors (45s) ────────────────────────────────────
    ("aegean_direct", 45.0),
    ("aerlingus_direct", 45.0),
    ("aerolineas_direct", 45.0),
    ("aircanada_direct", 45.0),
    ("airnewzealand_direct", 45.0),
    ("alaska_direct", 45.0),
    ("american_direct", 45.0),
    ("avelo_direct", 45.0),
    ("avianca_direct", 45.0),
    ("bangkokairways_direct", 45.0),
    ("breeze_direct", 45.0),
    ("citilink_direct", 45.0),
    ("copa_direct", 45.0),
    ("delta_direct", 45.0),
    ("ethiopian_direct", 45.0),
    ("finnair_direct", 45.0),
    ("hawaiian_direct", 45.0),
    ("kenyaairways_direct", 45.0),
    ("korean_direct", 45.0),
    ("philippineairlines_direct", 45.0),
    ("royalairmaroc_direct", 45.0),
    ("saa_direct", 45.0),
    ("samoaairways_direct", 45.0),
    ("skyairline_direct", 45.0),
    ("suncountry_direct", 45.0),
    ("superairjet_direct", 45.0),
    ("tap_direct", 45.0),
    ("transnusa_direct", 45.0),
    ("turkish_direct", 45.0),
    ("usbangla_direct", 45.0),
    ("westjet_direct", 45.0),
    ("wingo_direct", 45.0),
    # ── Slow browser / heavy WAF (50-60s) ───────────────────────────────
    ("latam_direct", 50.0),
    ("airchina_direct", 55.0),
    ("aireuropa_direct", 55.0),
    ("airserbia_direct", 55.0),
    ("airtransat_direct", 55.0),
    ("asiana_direct", 55.0),
    ("chinaeastern_direct", 55.0),
    ("chinasouthern_direct", 55.0),
    ("elal_direct", 55.0),
    ("hainan_direct", 55.0),
    ("kuwaitairways_direct", 55.0),
    ("level_direct", 55.0),
    ("lot_direct", 55.0),
    ("mea_direct", 55.0),
    ("qatar_direct", 55.0),
    ("royaljordanian_direct", 55.0),
    ("saudia_direct", 55.0),
    ("united_direct", 55.0),
    ("vietnamairlines_direct", 55.0),
    ("emirates_direct", 60.0),
    ("nh_direct", 60.0),
    ("singapore_direct", 60.0),
    # ── OTA / aggregator connectors ─────────────────────────────────────
    ("cheapflights_meta", 55.0),
    ("cleartrip_ota", 55.0),
    ("edreams_ota", 55.0),
    ("kayak_meta", 55.0),
    ("momondo_meta", 55.0),
    ("opodo_ota", 55.0),
    ("skyscanner_meta", 55.0),
    ("tiket_ota", 55.0),
    ("traveloka_ota", 55.0),
    ("tripcom_ota", 55.0),
    ("webjet_ota", 55.0),
    ("wego_meta", 55.0),
]

# Fast connectors: (connector_id, timeout, AIRLINE_COUNTRIES key or None for global)
FAST_CONNECTORS: list[tuple[str, float, str | None]] = [
    ("ryanair_direct", 20.0, "ryanair"),
    ("wizzair_direct", 15.0, "wizz"),
    ("kiwi_connector", 25.0, None),
]

# Connectors that support native round-trip search (return_date in URL/query).
# These get a single RT call instead of two one-way calls in round-trip mode.
RT_CAPABLE_CONNECTORS: set[str] = {
    "skyscanner_meta", "kayak_meta", "cheapflights_meta", "momondo_meta",
    "kiwi_connector",
}


# ── Main entry point ────────────────────────────────────────────────────────

async def run_search(params: dict) -> dict:
    """
    Two-phase search with callbacks after each phase.

    params:
      origin, destination, date_from  (required)
      callback_url, callback_meta     (required)
      return_date                     (optional — triggers round-trip)
      adults, currency, limit         (optional)
      max_stops                       (optional — filter by max stopovers, 0=direct)

    Round-trip mode (when return_date is present):
      Fires outbound + return directions in parallel, then builds
      cross-airline combos via the combo engine.  Wall-clock time
      stays ~60s instead of doubling.

    Returns merged results dict.
    """
    origin = params["origin"].strip().upper()
    destination = params["destination"].strip().upper()
    date_from = params["date_from"].strip()
    return_date = (params.get("return_date") or "").strip() or None
    callback_url = params["callback_url"]
    callback_meta = params["callback_meta"]
    adults = int(params.get("adults", 1))
    currency = params.get("currency", "EUR")
    limit = int(params.get("limit", 30))
    max_stops = params.get("max_stops")
    if max_stops is not None:
        max_stops = int(max_stops)

    t0 = time.monotonic()

    if return_date:
        return await _run_round_trip(
            origin, destination, date_from, return_date,
            callback_url, callback_meta,
            adults, currency, limit, t0, max_stops,
        )

    # ── One-way search (original flow) ──────────────────────────────────
    all_offers: list[dict] = []

    # ── Phase 1: API backend (fast) ─────────────────────────────────────
    if LETSFG_API_KEY:
        try:
            api_result = await _search_api(
                origin, destination, date_from, adults, currency, limit,
            )
            api_offers = api_result.get("offers", [])
            elapsed = time.monotonic() - t0
            logger.info("Phase 1: %d offers in %.1fs", len(api_offers), elapsed)

            if api_offers:
                all_offers.extend(api_offers)
                await _send_callback(callback_url, callback_meta, {
                    "phase": 1,
                    "origin": origin,
                    "destination": destination,
                    "currency": currency,
                    "offers": api_offers[:limit],
                    "total_results": len(api_offers),
                    "elapsed_seconds": round(elapsed, 1),
                })
        except Exception as exc:
            logger.error("Phase 1 failed: %s", exc)
    else:
        logger.info("Phase 1 skipped (no LETSFG_API_KEY)")

    # ── Phase 2: Local browser connectors (slow) ───────────────────────
    try:
        local_result = await _search_local(
            origin, destination, date_from, adults, currency, limit,
        )
        local_offers = local_result.get("offers", [])
        elapsed = time.monotonic() - t0
        logger.info("Phase 2: %d local offers in %.1fs", len(local_offers), elapsed)
        all_offers.extend(local_offers)
    except Exception as exc:
        logger.error("Phase 2 failed: %s", exc)

    # ── Merge, deduplicate, sort ────────────────────────────────────────
    merged = _deduplicate(all_offers)
    if max_stops is not None:
        merged = _filter_by_stops(merged, max_stops)
    merged.sort(key=lambda o: float(o.get("price", 999999)))
    merged = merged[:limit]

    elapsed = time.monotonic() - t0
    logger.info(
        "Search complete: %s→%s %s — %d offers in %.1fs",
        origin, destination, date_from, len(merged), elapsed,
    )

    # ── Final callback with merged results ──────────────────────────────
    result = {
        "phase": 2,
        "origin": origin,
        "destination": destination,
        "currency": currency,
        "offers": merged,
        "total_results": len(merged),
        "elapsed_seconds": round(elapsed, 1),
    }
    await _send_callback(callback_url, callback_meta, result)

    return result


# ── Round-trip orchestration ─────────────────────────────────────────────────

async def _run_round_trip(
    origin: str, destination: str,
    date_from: str, return_date: str,
    callback_url: str, callback_meta: dict,
    adults: int, currency: str, limit: int,
    t0: float, max_stops: int | None = None,
) -> dict:
    """Fire outbound + return searches in parallel, then build combos.

    Both directions run simultaneously (API + fan-out each), so wall-clock
    time stays ~60s instead of doubling.  The combo engine then pairs
    outbound legs from airline A with return legs from airline B.
    """
    logger.info(
        "Round-trip search: %s→%s %s, return %s→%s %s",
        origin, destination, date_from,
        destination, origin, return_date,
    )

    # ── Phase 1: Native round-trip API search ─────────────────────────
    api_offers: list[dict] = []
    if LETSFG_API_KEY:
        try:
            # Single native RT call — GDS/NDC providers price outbound+return
            # together, yielding significantly cheaper fares than two one-ways.
            api_rt_task = _search_api(
                origin, destination, date_from, adults, currency, limit * 2,
                return_date=return_date,
            )
            api_rt_res = await asyncio.gather(api_rt_task, return_exceptions=True)
            api_rt_res = api_rt_res[0]

            if isinstance(api_rt_res, Exception):
                logger.error("Phase 1 RT API failed: %s", api_rt_res)
            elif isinstance(api_rt_res, dict):
                offers = api_rt_res.get("offers", [])
                api_offers.extend(offers)
                logger.info("Phase 1 RT API: %d native round-trip offers", len(offers))

            elapsed = time.monotonic() - t0
            if api_offers:
                await _send_callback(callback_url, callback_meta, {
                    "phase": 1,
                    "origin": origin,
                    "destination": destination,
                    "return_date": return_date,
                    "currency": currency,
                    "offers": api_offers[:limit],
                    "total_results": len(api_offers),
                    "elapsed_seconds": round(elapsed, 1),
                })
        except Exception as exc:
            logger.error("Phase 1 round-trip failed: %s", exc)
    else:
        logger.info("Phase 1 skipped (no LETSFG_API_KEY)")

    # ── Phase 2: Fan-out — one-way pairs + aggregator round-trip ───────
    outbound_offers: list[dict] = []
    return_offers: list[dict] = []
    rt_aggregator_offers: list[dict] = []
    try:
        # One-way fan-outs for combo engine (direct airlines only, exclude RT-capable)
        out_local_task = _search_local(origin, destination, date_from, adults, currency, limit * 2)
        ret_local_task = _search_local(destination, origin, return_date, adults, currency, limit * 2)
        # Native RT fan-out for aggregators (Skyscanner, Kayak, CheapFlights, Momondo, Kiwi)
        rt_local_task = _search_local(
            origin, destination, date_from, adults, currency, limit * 2,
            return_date=return_date, only_rt_capable=True,
        )
        out_local_res, ret_local_res, rt_local_res = await asyncio.gather(
            out_local_task, ret_local_task, rt_local_task,
            return_exceptions=True,
        )

        if isinstance(out_local_res, Exception):
            logger.error("Phase 2 outbound fan-out failed: %s", out_local_res)
        elif isinstance(out_local_res, dict):
            outbound_offers = out_local_res.get("offers", [])
            logger.info("Phase 2 outbound: %d offers", len(outbound_offers))

        if isinstance(ret_local_res, Exception):
            logger.error("Phase 2 return fan-out failed: %s", ret_local_res)
        elif isinstance(ret_local_res, dict):
            return_offers = ret_local_res.get("offers", [])
            logger.info("Phase 2 return: %d offers", len(return_offers))

        if isinstance(rt_local_res, Exception):
            logger.error("Phase 2 RT aggregator fan-out failed: %s", rt_local_res)
        elif isinstance(rt_local_res, dict):
            rt_aggregator_offers = rt_local_res.get("offers", [])
            logger.info("Phase 2 RT aggregators: %d native round-trip offers",
                        len(rt_aggregator_offers))
    except Exception as exc:
        logger.error("Phase 2 round-trip failed: %s", exc)

    elapsed_p2 = time.monotonic() - t0
    logger.info("Phase 2 round-trip fan-out complete in %.1fs", elapsed_p2)

    # ── Build cross-airline combos ──────────────────────────────────────
    combos_json: list[dict] = []
    try:
        # Pre-filter legs when max_stops is specified.
        # This ensures combos are only built from legs meeting the stops criteria.
        combo_out = outbound_offers
        combo_ret = return_offers
        if max_stops is not None:
            combo_out = _filter_by_stops(outbound_offers, max_stops)
            combo_ret = _filter_by_stops(return_offers, max_stops)
            logger.info("Combo pre-filter: %d out → %d, %d ret → %d",
                        len(outbound_offers), len(combo_out),
                        len(return_offers), len(combo_ret))

        combos_json = _build_round_trip_combos(
            combo_out, combo_ret, currency,
        )
        logger.info("Combo engine: %d cross-airline offers", len(combos_json))
    except Exception as exc:
        logger.error("Combo engine failed: %s", exc)

    # ── Merge everything ────────────────────────────────────────────────
    # API RT offers + aggregator RT offers + outbound one-ways + return one-ways + combos
    all_offers = api_offers + rt_aggregator_offers + outbound_offers + return_offers + combos_json
    merged = _deduplicate(all_offers)
    if max_stops is not None:
        merged = _filter_by_stops(merged, max_stops)
    merged.sort(key=lambda o: float(o.get("price", 999999)))
    merged = merged[:limit]

    elapsed = time.monotonic() - t0
    logger.info(
        "Round-trip complete: %s⇄%s %s/%s — %d offers "
        "(%d api_rt + %d agg_rt + %d out + %d ret + %d combos) in %.1fs",
        origin, destination, date_from, return_date, len(merged),
        len(api_offers), len(rt_aggregator_offers),
        len(outbound_offers), len(return_offers),
        len(combos_json), elapsed,
    )

    result = {
        "phase": 2,
        "origin": origin,
        "destination": destination,
        "return_date": return_date,
        "currency": currency,
        "offers": merged,
        "total_results": len(merged),
        "elapsed_seconds": round(elapsed, 1),
    }
    await _send_callback(callback_url, callback_meta, result)

    return result


def _build_round_trip_combos(
    outbound_offers_json: list[dict],
    return_offers_json: list[dict],
    currency: str,
) -> list[dict]:
    """Convert JSON offers to FlightOffer models, run combo engine, return JSON.

    Uses the SDK's build_combos() to pair outbound legs from one airline
    with return legs from another (virtual interlining).

    IMPORTANT: Uses the combo_engine's own FlightOffer class to avoid
    Pydantic class-identity mismatches from double-imported modules.
    """
    from letsfg.connectors import combo_engine

    if not outbound_offers_json or not return_offers_json:
        return []

    # Use the SAME FlightOffer class that build_combos uses internally.
    # Python can load the same file under two module paths (e.g.
    # 'letsfg.models.flights' vs 'models.flights'), creating distinct
    # classes.  Pulling FlightOffer from combo_engine ensures consistency.
    FO = combo_engine.FlightOffer

    out_models = []
    for o in outbound_offers_json:
        try:
            out_models.append(FO.model_validate(o))
        except Exception:
            continue

    ret_models = []
    for o in return_offers_json:
        try:
            ret_models.append(FO.model_validate(o))
        except Exception:
            continue

    if not out_models or not ret_models:
        return []

    combos = combo_engine.build_combos(out_models, ret_models, currency)

    return [c.model_dump(mode="json") for c in combos]


# ── Phase 1: API backend ────────────────────────────────────────────────────

async def _search_api(
    origin: str, destination: str, date_from: str,
    adults: int, currency: str, limit: int,
    max_stopovers: int | None = None,
    return_date: str | None = None,
) -> dict:
    """Search via LetsFG API — Duffel, Amadeus, Sabre (400+ airlines).

    When return_date is provided, the API returns native round-trip offers
    with both outbound+inbound legs priced together (much cheaper than
    combining two one-way fares).
    """
    body: dict = {
        "origin": origin,
        "destination": destination,
        "date_from": date_from,
        "adults": adults,
        "currency": currency,
        "limit": min(limit, 200),  # API maximum is 200
    }
    if max_stopovers is not None:
        body["max_stopovers"] = max_stopovers
    if return_date:
        body["return_from"] = return_date
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{LETSFG_BASE_URL}/api/v1/flights/search",
            json=body,
            headers={
                "X-API-Key": LETSFG_API_KEY,
                "User-Agent": "letsfg-flight-worker/1.0",
            },
        )
        if resp.status_code != 200:
            logger.warning("API search %s->%s returned %d: %s",
                           origin, destination, resp.status_code, resp.text[:200])
            return {"offers": [], "total_results": 0}
        return resp.json()


# ── Phase 2: Fan-out to connector-worker instances ──────────────────────────

async def _search_local(
    origin: str, destination: str, date_from: str,
    adults: int, currency: str, limit: int,
    return_date: str | None = None,
    only_rt_capable: bool = False,
) -> dict:
    """Fan out search to individual connector-worker Cloud Run instances.

    Each connector gets its own HTTP call → its own Cloud Run instance.
    All connectors run truly in parallel (no browser semaphore).

    When return_date is set, the payload includes it so connectors that
    support native round-trip can build RT URLs (aggregators, Kiwi).

    When only_rt_capable is True, only fire connectors in RT_CAPABLE_CONNECTORS
    (used for the dedicated round-trip fan-out in _run_round_trip).
    """
    if not CONNECTOR_WORKER_URL:
        logger.error("CONNECTOR_WORKER_URL not set — skipping fan-out")
        return {"offers": [], "total_results": 0}

    from letsfg.connectors.airline_routes import (
        get_relevant_connectors, get_city_airports,
        AIRLINE_COUNTRIES, get_country,
    )

    t0 = time.monotonic()

    # ── Build sibling airport pairs ─────────────────────────────────────
    origin_airports = get_city_airports(origin)
    dest_airports = get_city_airports(destination)
    sibling_pairs: list[list[str]] = []
    seen = {(origin, destination)}
    for o in origin_airports:
        for d in dest_airports:
            if o != d and (o, d) not in seen:
                seen.add((o, d))
                sibling_pairs.append([o, d])

    if sibling_pairs:
        logger.info("City expansion: %s->%s + %d siblings: %s",
                     origin, destination, len(sibling_pairs),
                     ", ".join(f"{p[0]}->{p[1]}" for p in sibling_pairs))

    # ── Route-filter direct airline connectors ──────────────────────────
    stub_connectors = [(name, None, timeout) for name, timeout in CONNECTOR_REGISTRY]
    filtered = get_relevant_connectors(origin, destination, stub_connectors)
    skipped = len(CONNECTOR_REGISTRY) - len(filtered)
    if skipped:
        logger.info("Route filter: %s->%s — skipped %d/%d irrelevant connectors",
                     origin, destination, skipped, len(CONNECTOR_REGISTRY))

    # ── Build fan-out tasks ─────────────────────────────────────────────
    tasks: list[dict] = []
    origin_country = get_country(origin)
    dest_country = get_country(destination)

    def _base_payload(connector_id: str, all_pairs: bool) -> dict:
        payload = {
            "connector_id": connector_id,
            "origin": origin,
            "destination": destination,
            "date_from": date_from,
            "adults": adults,
            "currency": currency,
            "sibling_pairs": sibling_pairs,
            "all_pairs": all_pairs,
        }
        if return_date:
            payload["return_date"] = return_date
        return payload

    # Fast connectors (Ryanair, Wizzair, Kiwi) — search all pairs
    for fast_id, fast_timeout, countries_key in FAST_CONNECTORS:
        if only_rt_capable and fast_id not in RT_CAPABLE_CONNECTORS:
            continue
        if countries_key:
            countries = AIRLINE_COUNTRIES.get(countries_key)
            if countries and origin_country and dest_country:
                if origin_country not in countries and dest_country not in countries:
                    continue
        tasks.append(_base_payload(fast_id, all_pairs=True))

    # Direct airline connectors (route-filtered) — primary + siblings
    for name, _cls, timeout in filtered:
        if only_rt_capable and name not in RT_CAPABLE_CONNECTORS:
            continue
        tasks.append(_base_payload(name, all_pairs=False))

    logger.info("Fan-out: %s->%s — %d tasks (%d direct + %d fast)",
                origin, destination, len(tasks),
                len(filtered), len(tasks) - len(filtered))

    if not tasks:
        return {"offers": [], "total_results": 0}

    # ── Fire all tasks in parallel ──────────────────────────────────────
    headers = {"Content-Type": "application/json"}
    # Connector-worker auth: shared secret (IAM is allow-unauthenticated)
    if CONNECTOR_WORKER_SECRET:
        headers["Authorization"] = f"Bearer {CONNECTOR_WORKER_SECRET}"

    async with httpx.AsyncClient(timeout=FANOUT_TIMEOUT + 30) as client:
        coros = [
            _call_connector(client, headers, task)
            for task in tasks
        ]
        task_objs = [asyncio.ensure_future(c) for c in coros]

        remaining = FANOUT_TIMEOUT - (time.monotonic() - t0)
        done, pending = await asyncio.wait(
            task_objs, timeout=max(remaining, 10.0),
        )

        if pending:
            logger.info("Fan-out deadline: %d/%d done, cancelling %d pending",
                        len(done), len(task_objs), len(pending))
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    # ── Collect offers ──────────────────────────────────────────────────
    all_offers: list[dict] = []
    for i, task_obj in enumerate(task_objs):
        try:
            result = task_obj.result()
            offers = result.get("offers", [])
            all_offers.extend(offers)
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            logger.debug("Task %s cancelled/pending", tasks[i]["connector_id"])
        except Exception as exc:
            logger.warning("Task %s failed: %s", tasks[i]["connector_id"], exc)

    elapsed = time.monotonic() - t0
    logger.info("Fan-out complete: %s->%s — %d offers from %d connectors in %.1fs",
                origin, destination, len(all_offers), len(tasks), elapsed)

    return {
        "offers": all_offers,
        "total_results": len(all_offers),
        "elapsed_seconds": round(elapsed, 1),
    }


async def _call_connector(
    client: httpx.AsyncClient, headers: dict, task: dict,
) -> dict:
    """Make a single HTTP call to the connector-worker service."""
    connector_id = task["connector_id"]
    try:
        resp = await client.post(
            f"{CONNECTOR_WORKER_URL}/run",
            json=task,
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()
        n_offers = len(data.get("offers", []))
        if n_offers:
            logger.info("+ %s: %d offers in %.1fs",
                        connector_id, n_offers,
                        data.get("elapsed_seconds", 0))
        return data
    except httpx.HTTPStatusError as exc:
        logger.warning("- %s: HTTP %s", connector_id, exc.response.status_code)
        return {"offers": [], "total_results": 0}
    except Exception as exc:
        logger.warning("- %s: %s", connector_id, exc)
        return {"offers": [], "total_results": 0}


# ── Callback ────────────────────────────────────────────────────────────────

async def _send_callback(url: str, meta: dict, result: dict) -> None:
    """POST search results back to the workflow engine."""
    payload = {"meta": meta, "result": result}
    headers = {"Content-Type": "application/json"}
    if CALLBACK_SECRET:
        headers["Authorization"] = f"Bearer {CALLBACK_SECRET}"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            logger.info(
                "Callback sent (phase %s): HTTP %s, %d offers",
                result.get("phase"), resp.status_code,
                len(result.get("offers", [])),
            )
    except Exception as exc:
        logger.error("Callback failed (phase %s): %s", result.get("phase"), exc)


# ── Deduplication ────────────────────────────────────────────────────────────

def _filter_by_stops(offers: list[dict], max_stops: int) -> list[dict]:
    """Filter offers to those with at most max_stops stopovers per leg."""
    filtered = []
    for offer in offers:
        outbound = offer.get("outbound") or {}
        inbound = offer.get("inbound") or {}
        ob_stops = outbound.get("stopovers", 0)
        ib_stops = inbound.get("stopovers", 0) if inbound else 0
        if ob_stops <= max_stops and ib_stops <= max_stops:
            filtered.append(offer)
    return filtered


def _deduplicate(offers: list[dict]) -> list[dict]:
    """Remove exact duplicate offers (same source + airline + flight + price).

    Different sources for the same flight are KEPT — they may have different
    booking fees, baggage policies, or checkout prices. Users should see all
    booking options available.
    """
    seen: set[str] = set()
    unique: list[dict] = []
    for offer in offers:
        segments = (offer.get("outbound") or {}).get("segments") or []
        flight_no = segments[0].get("flight_no", "") if segments else ""
        airlines = tuple(sorted(offer.get("airlines") or []))
        price = offer.get("price", 0)
        source = offer.get("source", "")
        # Include source so same flight from different OTAs/airlines are kept
        key = f"{source}|{airlines}|{flight_no}|{price}"
        h = hashlib.md5(key.encode()).hexdigest()
        if h not in seen:
            seen.add(h)
            unique.append(offer)
    return unique
