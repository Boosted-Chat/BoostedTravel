"""
Spirit Airlines connector — CDP Chrome + in-page fetch API calls.

Spirit (IATA: NK) is a US ultra-low-cost carrier operating domestic and
Caribbean/Latin America routes. Protected by Akamai Bot Manager + PerimeterX.

Strategy (CDP Chrome + same-origin fetch):
1.  Launch real Chrome via CDP (--remote-debugging-port).
2.  Navigate to spirit.com homepage to establish Akamai/PX cookies.
3.  Call ``POST /api/prod-token/api/v1/token`` from page context
    via ``page.evaluate(fetch(…))`` to get a Navitaire session token.
4.  Call ``POST /api/prod-availability/api/availability/v3/search``
    with the bearer token to get flight availability.
5.  Parse the Navitaire response into FlightOffers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, date as date_type
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_CDP_PORT = 9463
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".spirit_chrome_data"
)

_SUB_KEY = "3b6a6994753b4efc86376552e52b8432"
_TOKEN_URL = "/api/prod-token/api/v1/token"
_SEARCH_URL = "/api/prod-availability/api/availability/v3/search"

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_homepage_warmed = False


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (real Chrome via CDP)."""
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            _pw_instance = pw
            logger.info("NK: connected to existing Chrome on port %d", _CDP_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

            chrome = find_chrome()
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            args = [
                chrome,
                f"--remote-debugging-port={_CDP_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                *proxy_chrome_args(),
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
                "--lang=en-US",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.5)

            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            logger.info(
                "NK: Chrome launched on CDP port %d (pid %d)",
                _CDP_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    """Wipe Chrome profile when session is corrupted."""
    global _browser, _context, _pw_instance, _chrome_proc, _homepage_warmed
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    _homepage_warmed = False
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("NK: deleted stale Chrome profile")
        except Exception:
            pass


# ── Helpers ──────────────────────────────────────────────────────────────

def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s[: len(fmt) + 2], fmt)
        except (ValueError, IndexError):
            continue
    return datetime(2000, 1, 1)


class SpiritConnectorClient:
    """Spirit CDP Chrome connector — Navitaire dotRezWeb availability API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        if ob_result.offers:
            segs = ob_result.offers[0].outbound.segments if ob_result.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            try:
                ancillary = await asyncio.wait_for(
                    self._fetch_ancillaries(
                        anc_origin, anc_dest,
                        req.date_from.isoformat(), req.adults, ob_result.currency,
                    ),
                    timeout=20.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("NK: ancillary fetch timed out")
            except Exception as _anc_err:
                logger.debug("NK: ancillary fetch error: %s", _anc_err)
        return ob_result


    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        global _homepage_warmed
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        try:
            # Warm Akamai/PX cookies by visiting homepage
            if not _homepage_warmed:
                logger.info("NK: warming Akamai cookies via homepage")
                try:
                    await page.goto(
                        "https://www.spirit.com/",
                        wait_until="domcontentloaded",
                        timeout=25000,
                    )
                except Exception as e:
                    logger.debug("NK: homepage nav: %s", e)
                await asyncio.sleep(4)
                _homepage_warmed = True
            else:
                try:
                    await page.goto(
                        "https://www.spirit.com/",
                        wait_until="domcontentloaded",
                        timeout=20000,
                    )
                except Exception as e:
                    logger.debug("NK: homepage nav: %s", e)
                await asyncio.sleep(3)

            # Step 1: Get Navitaire session token
            token_result = await page.evaluate(
                """async (subKey) => {
                    try {
                        const resp = await fetch('/api/prod-token/api/v1/token', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Ocp-Apim-Subscription-Key': subKey,
                                'Cache-Control': 'no-cache'
                            },
                            credentials: 'include',
                            body: JSON.stringify({"applicationName": "dotRezWeb"})
                        });
                        const text = await resp.text();
                        return {status: resp.status, body: text};
                    } catch(e) {
                        return {error: e.message};
                    }
                }""",
                _SUB_KEY,
            )

            if token_result.get("error"):
                logger.error("NK: token fetch error: %s", token_result["error"])
                return self._empty(req)

            if token_result["status"] not in (200, 201):
                logger.warning("NK: token returned %d, resetting profile", token_result["status"])
                await _reset_profile()
                return self._empty(req)

            token_data = json.loads(token_result["body"])
            bearer = (
                token_data.get("data", {}).get("token", "")
                or token_data.get("token", "")
            )
            if not bearer:
                logger.error("NK: no token in response: %s", token_result["body"][:200])
                return self._empty(req)
            logger.info("NK: got session token (%d chars)", len(bearer))

            # Step 2: Search flights
            dt = _to_datetime(req.date_from)
            date_str = dt.strftime("%Y-%m-%d")
            adults = req.adults or 1
            children = req.children or 0
            infants = req.infants or 0

            pax_types = [{"type": "ADT", "count": adults}]
            if children:
                pax_types.append({"type": "CHD", "count": children})

            search_payload = {
                "criteria": [{
                    "stations": {
                        "originStationCodes": [req.origin],
                        "destinationStationCodes": [req.destination],
                    },
                    "dates": {"beginDate": date_str, "endDate": date_str},
                    "filters": {"filter": "Default"},
                }],
                "passengers": {"types": pax_types},
                "codes": {"currency": "USD", "promotionCode": ""},
                "fareFilters": {
                    "loyalty": "MonetaryOnly",
                    "types": [],
                    "classControl": 1,
                },
                "taxesAndFees": "TaxesAndFees",
                "infantCount": infants,
                "includeWifiAvailability": True,
                "includeBundleAvailability": True,
                "originalJourneyKeys": [],
                "originalBookingRecordLocator": None,
                "birthDates": [],
            }

            # Add return leg criteria for round-trip searches
            if req.return_from:
                ret_dt = _to_datetime(req.return_from)
                ret_str = ret_dt.strftime("%Y-%m-%d")
                search_payload["criteria"].append({
                    "stations": {
                        "originStationCodes": [req.destination],
                        "destinationStationCodes": [req.origin],
                    },
                    "dates": {"beginDate": ret_str, "endDate": ret_str},
                    "filters": {"filter": "Default"},
                })

            api_result = await page.evaluate(
                """async (args) => {
                    try {
                        const resp = await fetch(args.url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'Accept-Language': 'en-US',
                                'Authorization': 'Bearer ' + args.token,
                                'Ocp-Apim-Subscription-Key': args.subKey,
                                'Cache-Control': 'no-cache'
                            },
                            credentials: 'include',
                            body: args.body
                        });
                        const text = await resp.text();
                        return {status: resp.status, body: text};
                    } catch(e) {
                        return {error: e.message};
                    }
                }""",
                {
                    "url": _SEARCH_URL,
                    "token": bearer,
                    "subKey": _SUB_KEY,
                    "body": json.dumps(search_payload),
                },
            )

            if api_result.get("error"):
                logger.error("NK: search fetch error: %s", api_result["error"])
                return self._empty(req)

            status = api_result.get("status", 0)
            body_text = api_result.get("body", "")

            if status == 403 or status == 429:
                logger.warning("NK: blocked (%d), resetting profile", status)
                await _reset_profile()
                return self._empty(req)

            if status != 200:
                logger.warning("NK: search returned %d: %s", status, body_text[:300])
                _homepage_warmed = False
                return self._empty(req)

            data = json.loads(body_text)
            offers = self._parse_response(data, req)
            offers.sort(key=lambda o: o.price)

            # Cache bundle-derived ancillary data for _fetch_ancillaries
            bundle_anc = self._parse_bundle_ancillary(data)
            if bundle_anc:
                _anc_key = f"nk_{req.origin}_{req.destination}_{req.date_from}"
                _ancillary_cache[_anc_key] = (time.time(), bundle_anc)

            elapsed = time.monotonic() - t0
            logger.info("NK %s->%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            h = hashlib.md5(f"spirit{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("NK CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Navitaire availability/v3/search response.

        Structure: data.trips[].journeysAvailable[].fares{<key>: {details: {passengerFares: [{fareAmount}]}}}
        """
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = []
        if isinstance(data, dict):
            d = data.get("data", data)
            trips = d.get("trips", []) if isinstance(d, dict) else []
        if not isinstance(trips, list):
            trips = []

        for trip_idx, trip in enumerate(trips):
            if not isinstance(trip, dict):
                continue
            journeys = trip.get("journeysAvailable", [])
            if not isinstance(journeys, list):
                continue
            for journey in journeys:
                if not isinstance(journey, dict) or not journey.get("isSelectable", True):
                    continue
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    if trip_idx == 0:
                        offers.append(offer)
                    elif trip_idx >= 1:
                        # Mark as inbound leg — will be combined below
                        offer._is_inbound = True  # type: ignore[attr-defined]
                        offers.append(offer)

        # Build RT combos from trips[0] outbound + trips[1] inbound
        if len(trips) > 1:
            outbound_offers = [o for o in offers if not getattr(o, "_is_inbound", False)]
            inbound_offers = [o for o in offers if getattr(o, "_is_inbound", False)]
            if outbound_offers and inbound_offers:
                rt_offers: list[FlightOffer] = []
                outbound_offers.sort(key=lambda o: o.price)
                inbound_offers.sort(key=lambda o: o.price)
                for ob in outbound_offers[:15]:
                    for ib in inbound_offers[:10]:
                        rt_price = round(ob.price + ib.price, 2)
                        rt_key = f"{ob.id}_{ib.id}"
                        rt_offers.append(FlightOffer(
                            id=f"nk_{hashlib.md5(rt_key.encode()).hexdigest()[:12]}",
                            price=rt_price,
                            currency="USD",
                            price_formatted=f"${rt_price:.2f}",
                            outbound=ob.outbound,
                            inbound=ib.outbound,
                            airlines=["Spirit"],
                            owner_airline="NK",
                            booking_url=booking_url,
                            is_locked=False,
                            source="spirit_direct",
                            source_tier="free",
                        ))
                return rt_offers

        return offers

    def _parse_journey(self, journey: dict, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        """Parse a single journey (one itinerary option) into a FlightOffer."""
        fares = journey.get("fares", {})
        if not isinstance(fares, dict) or not fares:
            return None

        # Find cheapest fare
        best_price = float("inf")
        for fare_val in fares.values():
            det = fare_val.get("details", {}) if isinstance(fare_val, dict) else {}
            for pf in det.get("passengerFares", []):
                amt = pf.get("fareAmount")
                if isinstance(amt, (int, float)) and 0 < amt < best_price:
                    best_price = amt
        if best_price == float("inf"):
            return None

        _nk_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        # Build segments from journey.segments
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []
        for seg in (segments_raw if isinstance(segments_raw, list) else []):
            des = seg.get("designator", {})
            ident = seg.get("identifier", {})
            carrier = ident.get("carrierCode", "NK")
            flight_num = ident.get("identifier", "")
            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Spirit Airlines" if carrier == "NK" else carrier,
                flight_no=f"{carrier}{flight_num}",
                origin=des.get("origin", req.origin),
                destination=des.get("destination", req.destination),
                departure=_parse_dt(des.get("departure", "")),
                arrival=_parse_dt(des.get("arrival", "")),
                cabin_class=_nk_cabin,
            ))

        if not segments:
            des = journey.get("designator", {})
            segments.append(FlightSegment(
                airline="NK", airline_name="Spirit Airlines", flight_no="",
                origin=des.get("origin", req.origin),
                destination=des.get("destination", req.destination),
                departure=_parse_dt(des.get("departure", "")),
                arrival=_parse_dt(des.get("arrival", "")),
                cabin_class=_nk_cabin,
            ))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        jk = journey.get("journeyKey", f"{time.monotonic()}")
        return FlightOffer(
            id=f"nk_{hashlib.md5(str(jk).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency="USD",
            price_formatted=f"${best_price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Spirit"],
            owner_airline="NK",
            booking_url=booking_url,
            is_locked=False,
            source="spirit_direct",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # Ancillary pricing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_bundle_ancillary(data: Any) -> dict | None:
        """Extract carry-on / bag pricing from the bundle availability block in the
        Navitaire availability/v3/search response (returned when
        includeBundleAvailability=True is set in the request)."""
        try:
            d = data.get("data", data) if isinstance(data, dict) else {}
            bundle_avail = d.get("bundleAvailability") or {}
            trip_bundles = bundle_avail.get("tripBundles") or bundle_avail.get("bundles") or []

            carry_on_price: float | None = None

            for tb in (trip_bundles if isinstance(trip_bundles, list) else []):
                bundles_list = tb.get("bundles") if isinstance(tb, dict) else []
                if isinstance(tb, dict) and "bundleCode" in tb:
                    bundles_list = trip_bundles

                for b in (bundles_list if isinstance(bundles_list, list) else []):
                    if not isinstance(b, dict):
                        continue
                    code = str(b.get("bundleCode", "")).upper()
                    price_data = b.get("bundlePriceData") or b.get("price") or {}
                    if isinstance(price_data, dict):
                        price = price_data.get("totalPrice") or price_data.get("amount") or 0.0
                    else:
                        price = float(price_data) if price_data else 0.0

                    # Spirit Standard (BST/BNS) = carry-on included
                    if code in ("BST", "BNS", "STANDARD") and price and carry_on_price is None:
                        carry_on_price = float(price)
                break  # Only use first trip bundle

            if carry_on_price is not None and carry_on_price > 0:
                return {
                    "carry_on_from": carry_on_price,
                    "carry_on_note": f"carry-on from +USD {carry_on_price:.0f} (Standard bundle — add at booking)",
                    "checked_from": 45.0,
                    "checked_note": "first checked bag from +USD 45 (separate add-on at booking)",
                    "seat_from": 1.0,
                    "seat_note": "seat selection from +USD 1",
                    "currency": "USD",
                }
        except Exception:
            pass
        return None

    async def _fetch_ancillaries(
        self,
        origin: str,
        dest: str,
        date_str: str,
        adults: int,
        currency: str,
    ) -> dict | None:
        cache_key = f"nk_{origin}_{dest}_{date_str}"
        now = time.time()
        if cache_key in _ancillary_cache:
            ts, cached = _ancillary_cache[cache_key]
            if now - ts < _ANCILLARY_CACHE_TTL:
                return cached

        # Spirit Saver$ fares don't include carry-on; add-on at booking ~$35–$65.
        result: dict = {
            "carry_on_from": 39.0,
            "carry_on_note": "carry-on from +USD 39 (Saver$ — no bags on base fare)",
            "checked_from": 45.0,
            "checked_note": "first checked bag from +USD 45 (add at booking, cheaper than airport)",
            "seat_from": 1.0,
            "seat_note": "seat selection from +USD 1",
            "currency": "USD",
        }
        _ancillary_cache[cache_key] = (now, result)
        return result

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        carry_on_from = ancillary.get("carry_on_from")
        carry_on_note = ancillary.get("carry_on_note")
        checked_from = ancillary.get("checked_from")
        checked_note = ancillary.get("checked_note")
        seat_from = ancillary.get("seat_from")
        seat_note = ancillary.get("seat_note")
        anc_currency = ancillary.get("currency", "USD")
        for offer in offers:
            ccy_ok = offer.currency.upper() == anc_currency.upper()
            if carry_on_note:
                offer.conditions["carry_on"] = carry_on_note
            if checked_note:
                offer.conditions["checked_bag"] = checked_note
            if seat_note:
                offer.conditions["seat"] = seat_note
            if carry_on_from is not None and (carry_on_from == 0.0 or ccy_ok):
                offer.bags_price["carry_on"] = carry_on_from
            if checked_from is not None and (checked_from == 0.0 or ccy_ok):
                offer.bags_price["checked_bag"] = checked_from
            if seat_from is not None and (seat_from == 0.0 or ccy_ok):
                offer.bags_price["seat_selection"] = seat_from

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = _to_datetime(req.date_from).strftime("%Y-%m-%d")
        is_rt = bool(req.return_from)
        base = (
            f"https://www.spirit.com/book/flights?from={req.origin}"
            f"&to={req.destination}&date={dep}&pax={req.adults or 1}"
            f"&tripType={'RT' if is_rt else 'OW'}"
        )
        if is_rt:
            ret = _to_datetime(req.return_from).strftime("%Y-%m-%d")
            base += f"&returnDate={ret}"
        return base

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"spirit{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )


    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req,
    ) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_spirit_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
