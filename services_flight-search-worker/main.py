"""
LetsFG Flight Search Orchestrator — Cloud Run service for B2C DM-based flight search.

Fan-out architecture: fires N parallel HTTP calls to connector-worker instances.
Each connector runs on its own Cloud Run instance (174 connectors registered).
Route filtering ensures only ~15-40 relevant connectors fire per search.

Endpoints:
  POST /search      — Run two-phase flight search, callback with results
  POST /test-search — Direct search (returns results inline, no callback)
  GET  /health      — Health check

Two-phase search:
  Phase 1 (~3s):  LetsFG API backend — Duffel, Amadeus, Sabre (400+ airlines)
  Phase 2 (~60s): Fan-out to connector-worker — 174 connectors in parallel

Environment variables:
  WORKER_SECRET            — Shared secret for authenticating inbound requests
  CALLBACK_SECRET          — Shared secret for callbacks to workflow engine
  LETSFG_API_KEY           — API key for LetsFG backend (Phase 1). Optional.
  LETSFG_BASE_URL          — API base URL (default: https://api.letsfg.co)
  CONNECTOR_WORKER_URL     — URL of the connector-worker Cloud Run service
  CONNECTOR_WORKER_SECRET  — Shared secret for connector-worker authentication
  FANOUT_TIMEOUT           — Max seconds to wait for fan-out (default: 90)

Deploy (Cloud Run — NO --function flag):
  gcloud run deploy flight-search-worker \\
    --source=. --project=sms-caller --region=us-central1 \\
    --memory=512Mi --cpu=1 --concurrency=80 --max-instances=3 \\
    --timeout=240 --min-instances=0 --cpu-throttling --no-traffic

  Cost notes:
  - concurrency=80: pure I/O (httpx fan-out), no CPU work → 1 instance serves many requests
  - max-instances=3: with concurrency=80, handles 240 concurrent searches
  - timeout=240: fan-out is 180s max, 240s gives headroom
  - cpu-throttling: CPU allocated only during request processing
  - min-instances=0: scales to zero when idle
"""

import asyncio
import logging
import os

from flask import Flask, request, jsonify, abort

from search_worker import run_search, normalize_offer_currencies

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("worker")

app = Flask(__name__)

WORKER_SECRET = os.environ.get("WORKER_SECRET", "")


def _verify_auth():
    """Verify inbound request has the correct shared secret."""
    if not WORKER_SECRET:
        return  # No auth in local dev
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token or token != WORKER_SECRET:
        abort(401, "Unauthorized")


@app.route("/search", methods=["POST"])
def search():
    """
    Run two-phase flight search and callback with results.

    Expected JSON body:
      {
        "origin": "LON",
        "destination": "BCN",
        "date_from": "2026-04-15",
        "return_date": "2026-04-22",
        "callback_url": "https://workflow-engine-xxx.run.app/flight-search-callback",
        "callback_meta": {"companyId": "...", "leadId": "...", "instanceId": "..."},
        "adults": 1,
        "currency": "EUR"
      }

    When return_date is present, outbound + return are searched in parallel
    and cross-airline combos are built via virtual interlining.
    """
    _verify_auth()

    data = request.get_json(force=True)

    required = ["origin", "destination", "date_from", "callback_url", "callback_meta"]
    missing = [f for f in required if f not in data]
    if missing:
        return jsonify({"error": f"Missing: {', '.join(missing)}"}), 400

    try:
        result = asyncio.run(run_search(data))
        return jsonify({
            "status": "completed",
            "total_offers": result.get("total_results", 0),
        })
    except Exception as exc:
        logger.exception("Search failed: %s", exc)
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.route("/test-search", methods=["POST"])
def test_search():
    """
    Direct search endpoint — returns results inline (no callback).

    Runs both API backend (Duffel/Amadeus/Sabre — 400+ airlines) and
    fan-out to connector-worker instances in parallel, then merges.

    Body: {
      "origin": "LON", "destination": "DEL", "date_from": "2026-04-01",
      "return_date": "2026-04-07",  // optional
      "adults": 1, "currency": "GBP", "limit": 20, "max_stops": 0
    }
    """
    _verify_auth()
    data = request.get_json(force=True)

    origin = (data.get("origin") or "").strip().upper()
    destination = (data.get("destination") or "").strip().upper()
    date_from = (data.get("date_from") or "").strip()

    if not origin or not destination or not date_from:
        return jsonify({"error": "Need origin, destination, date_from"}), 400

    adults = int(data.get("adults", 1))
    currency = data.get("currency", "EUR")
    limit = int(data.get("limit", 20))
    return_date = (data.get("return_date") or "").strip() or None
    max_stops = data.get("max_stops")
    if max_stops is not None:
        max_stops = int(max_stops)
    cabin_class = (data.get("cabin_class") or "").strip().upper() or None
    if cabin_class and cabin_class not in ("M", "W", "C", "F"):
        cabin_class = None  # ignore invalid values

    try:
        result = asyncio.run(_run_test_search(
            origin, destination, date_from, return_date,
            adults, currency, limit, max_stops, cabin_class,
        ))
        return jsonify(result)
    except Exception as exc:
        logger.exception("Test search failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


async def _run_test_search(
    origin: str, destination: str, date_from: str, return_date: str | None,
    adults: int, currency: str, limit: int, max_stops: int | None,
    cabin_class: str | None = None,
) -> dict:
    """Run API + local search in parallel, merge results.

    For round-trip:
      - API gets ONE call with return_from → native round-trip offers
        (airlines price outbound+return together = much cheaper)
      - Local connectors still fire two one-way calls → combined into combos
      - Both streams are merged and sorted by price
    """
    import time as _time
    from search_worker import _search_api, _search_local, _deduplicate, _filter_by_stops, _filter_route_mismatch, _get_valid_airports

    LETSFG_API_KEY = os.environ.get("LETSFG_API_KEY", "")
    t0 = _time.monotonic()

    api_tasks = []
    api_labels = []
    local_tasks = []
    local_labels = []

    # API search (Duffel/Amadeus/Sabre) — native round-trip when return_date set
    if LETSFG_API_KEY:
        api_tasks.append(_search_api(
            origin, destination, date_from, adults, currency, limit,
            max_stopovers=max_stops, return_date=return_date,
            cabin_class=cabin_class,
        ))
        api_labels.append("api_rt" if return_date else "api_out")

    # Local connector fan-out — one-way for combo engine
    local_tasks.append(_search_local(origin, destination, date_from, adults, currency, limit,
                                     cabin_class=cabin_class))
    local_labels.append("local_out")
    if return_date:
        local_tasks.append(_search_local(destination, origin, return_date, adults, currency, limit,
                                         cabin_class=cabin_class))
        local_labels.append("local_ret")
        # Native RT fan-out — aggregators (Skyscanner, Kayak, etc.) do round-trip in one call
        local_tasks.append(_search_local(
            origin, destination, date_from, adults, currency, limit,
            return_date=return_date, only_rt_capable=True,
            cabin_class=cabin_class,
        ))
        local_labels.append("local_rt")

    # Start all tasks
    all_task_objs = [asyncio.ensure_future(t) for t in api_tasks + local_tasks]
    api_task_objs = all_task_objs[:len(api_tasks)]
    local_task_objs = all_task_objs[len(api_tasks):]

    # Wait for all tasks (API + local fan-out) to finish.
    # FANOUT_TIMEOUT is 110s on Cloud Run, Cloud Run request timeout is 120s.
    # Give 115s so the fan-out has time to complete before we hit Cloud Run's limit.
    from search_worker import FANOUT_TIMEOUT as _FT
    await asyncio.wait(all_task_objs, timeout=_FT + 5)

    # Cancel anything still pending
    for t in all_task_objs:
        if not t.done():
            t.cancel()
    await asyncio.gather(*[t for t in all_task_objs if t.cancelled()], return_exceptions=True)

    # Collect results by source
    task_labels = api_labels + local_labels
    api_offers = []       # native round-trip offers from API (already have outbound+inbound)
    local_out = []        # one-way outbound from local connectors
    local_ret = []        # one-way return from local connectors
    local_rt = []         # native round-trip from aggregators (Skyscanner, Kayak, etc.)
    one_way_offers = []   # all offers when no return_date

    for label, task_obj in zip(task_labels, all_task_objs):
        if task_obj.done() and not task_obj.cancelled():
            try:
                res = task_obj.result()
            except Exception as exc:
                logger.error("test-search %s failed: %s", label, exc)
                continue
            if isinstance(res, dict):
                offers = res.get("offers", [])
                logger.info("test-search %s: %d offers", label, len(offers))
                if label == "api_rt":
                    api_offers.extend(offers)
                elif label == "local_out":
                    local_out.extend(offers)
                elif label == "local_ret":
                    local_ret.extend(offers)
                elif label == "local_rt":
                    local_rt.extend(offers)
                else:
                    one_way_offers.extend(offers)
        else:
            logger.info("test-search %s: skipped (timeout/cancelled)", label)

    # Merge results
    if return_date:
        all_offers = list(api_offers) + list(local_rt)  # native round-trip offers

        # Build combos from local one-way legs using the SDK's combo engine
        # (virtual interlining — pairs outbound airline A with return airline B)
        if local_out and local_ret:
            from search_worker import _build_round_trip_combos, _filter_by_stops

            combo_out = local_out
            combo_ret = local_ret
            if max_stops is not None:
                combo_out = _filter_by_stops(local_out, max_stops)
                combo_ret = _filter_by_stops(local_ret, max_stops)
                logger.info("test-search pre-filtered for combos: %d out, %d ret",
                            len(combo_out), len(combo_ret))

            combos = _build_round_trip_combos(combo_out, combo_ret, currency)
            logger.info("test-search local combos: %d", len(combos))
            all_offers.extend(combos)

        logger.info("test-search round-trip: %d API native + %d agg RT + %d local combos = %d total",
                     len(api_offers), len(local_rt),
                     len(all_offers) - len(api_offers) - len(local_rt), len(all_offers))
    else:
        all_offers = one_way_offers + local_out

    merged = _deduplicate(all_offers)
    # Route validation — drop offers whose origin/destination don't match the request
    valid_origins, valid_dests = _get_valid_airports(origin, destination)
    merged = _filter_route_mismatch(merged, valid_origins, valid_dests)
    if max_stops is not None:
        merged = _filter_by_stops(merged, max_stops)

    # Normalize all prices to the requested currency before sorting.
    # Connectors return prices in their native currencies (USD, EUR, PLN, etc.)
    # — sorting raw price numbers across currencies is meaningless.
    normalize_offer_currencies(merged, currency)

    # Rank: price + 8% penalty per stop (direct flights rank higher)
    merged.sort(key=lambda o: float(o.get("price", 999999)) * (
        1 + 0.08 * ((o.get("outbound") or {}).get("stopovers", 0)
                    + (o.get("inbound") or {}).get("stopovers", 0))))
    merged = merged[:limit]

    elapsed = _time.monotonic() - t0
    return {
        "offers": merged,
        "total_results": len(merged),
        "elapsed_seconds": round(elapsed, 1),
    }


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "flight-search-worker"})
