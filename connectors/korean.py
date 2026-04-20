"""
Korean Air CDP Chrome connector — fetches fare data from koreanair.com
(EveryMundo airTRFX platform).

Korean Air (IATA: KE) is South Korea's flag carrier based in Seoul.
Operates long-haul and regional routes from ICN hub to Asia, Europe,
North America, and Oceania. Default currency KRW.

Strategy:
1. Map IATA codes to city slugs used by koreanair.com/flights/
2. Launch CDP Chrome (koreanair.com WAF blocks httpx with 403)
3. Fetch route page: koreanair.com/flights/en/flights-from-{origin}-to-{dest}
4. Extract __NEXT_DATA__ JSON via JS evaluation
5. Parse apolloState.data → StandardFareModule fares → FlightOffers
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import _launched_procs, auto_block_if_proxied, find_chrome, proxy_chrome_args, stealth_popen_kwargs

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9478
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".korean_chrome_data"
)

# Module-level browser state (reused across searches)
_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None

# EveryMundo uses city slugs, NOT airport codes.
# "flights-from-incheon-to-tokyo" returns 404 but "flights-from-seoul-to-tokyo" works.
_IATA_TO_SLUG: dict[str, str] = {
    # City codes (multi-airport cities)
    "LON": "london", "NYC": "new-york", "PAR": "paris", "ROM": "rome",
    "TYO": "tokyo", "SEL": "seoul", "OSA": "osaka",
    # South Korea
    "ICN": "seoul",
    "GMP": "seoul",
    "PUS": "busan",
    "CJU": "jeju",
    # Japan
    "NRT": "tokyo",
    "HND": "tokyo",
    "KIX": "osaka",
    "NGO": "nagoya",
    "FUK": "fukuoka",
    "CTS": "sapporo",
    "OKA": "okinawa",
    # China
    "PVG": "shanghai",
    "SHA": "shanghai",
    "PEK": "beijing",
    "PKX": "beijing",
    "CAN": "guangzhou",
    "CTU": "chengdu",
    "SZX": "shenzhen",
    "WUH": "wuhan",
    "HGH": "hangzhou",
    "XIY": "xian",
    "CSX": "changsha",
    "TAO": "qingdao",
    "DLC": "dalian",
    "KMG": "kunming",
    "NKG": "nanjing",
    "SHE": "shenyang",
    "CGO": "zhengzhou",
    "YNT": "yantai",
    "WEH": "weihai",
    "MFM": "macau",
    "HKG": "hong-kong",
    # Taiwan
    "TPE": "taipei",
    # Southeast Asia
    "SIN": "singapore",
    "BKK": "bangkok",
    "SGN": "ho-chi-minh",
    "HAN": "hanoi",
    "DAD": "da-nang",
    "MNL": "manila",
    "CEB": "cebu",
    "KUL": "kuala-lumpur",
    "PNH": "phnom-penh",
    "REP": "siem-reap",
    "CGK": "jakarta",
    "DPS": "denpasar",
    # South Asia
    "DEL": "delhi",
    "BOM": "mumbai",
    "BLR": "bangalore",
    "MAA": "chennai",
    "CMB": "colombo",
    "DAC": "dhaka",
    "KTM": "kathmandu",
    # Central Asia / Mongolia
    "ULN": "ulaanbaatar",
    "TAS": "tashkent",
    "ALA": "almaty",
    # Middle East
    "DXB": "dubai",
    "IST": "istanbul",
    "TLV": "tel-aviv",
    # Oceania
    "SYD": "sydney",
    "MEL": "melbourne",
    "BNE": "brisbane",
    "AKL": "auckland",
    # North America
    "JFK": "new-york",
    "LAX": "los-angeles",
    "SFO": "san-francisco",
    "SEA": "seattle",
    "ORD": "chicago",
    "IAD": "washington-dc",
    "DFW": "dallas",
    "ATL": "atlanta",
    "BOS": "boston",
    "LAS": "las-vegas",
    "HNL": "honolulu",
    "YVR": "vancouver",
    "YYZ": "toronto",
    # Europe
    "LHR": "london",
    "CDG": "paris",
    "FRA": "frankfurt",
    "FCO": "rome",
    "MXP": "milan",
    "BCN": "barcelona",
    "MAD": "madrid",
    "AMS": "amsterdam",
    "ZRH": "zurich",
    "VIE": "vienna",
    "PRG": "prague",
    "BUD": "budapest",
    "WAW": "warsaw",
    "ZAG": "zagreb",
    "ARN": "stockholm",
    "CPH": "copenhagen",
    "HEL": "helsinki",
    # Russia
    "SVO": "moscow",
    "VVO": "vladivostok",
    "IKT": "irkutsk",
}

_BASE = "https://www.koreanair.com/flights/en"


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent CDP Chrome context for Korean Air.

    Uses headed Chrome (no --headless) because koreanair.com WAF blocks
    headless browsers. Window is placed offscreen to avoid user disruption.
    """
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

        # Try connecting to existing Chrome on the port
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("Korean: connected to existing Chrome on port %d", _DEBUG_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

            # Launch headed Chrome (WAF blocks headless)
            chrome = find_chrome()
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            args = [
                chrome,
                f"--remote-debugging-port={_DEBUG_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                *proxy_chrome_args(),
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
                "--window-position=-2400,-2400",
                "--window-size=1400,900",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.0)

            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            logger.info(
                "Korean: Chrome launched headed on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


class KoreanConnectorClient:
    """Korean Air CDP Chrome connector — koreanair.com EveryMundo fare pages."""

    def __init__(self, timeout: float = 30.0):
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
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        origin_slug = _IATA_TO_SLUG.get(req.origin)
        dest_slug = _IATA_TO_SLUG.get(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("Korean: unmapped IATA %s or %s", req.origin, req.destination)
            return self._empty(req)

        url = f"{_BASE}/flights-from-{origin_slug}-to-{dest_slug}"
        logger.info("Korean: fetching %s", url)

        try:
            context = await _get_context()
            page = await context.new_page()
            await auto_block_if_proxied(page)

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(2.0)

                # Extract __NEXT_DATA__ via JS
                nd_json = await page.evaluate("""() => {
                    const el = document.querySelector('script#__NEXT_DATA__');
                    return el ? el.textContent : null;
                }""")

                if not nd_json:
                    logger.warning("Korean: no __NEXT_DATA__ on %s", url)
                    return self._empty(req)

                fares = self._extract_fares(nd_json)
                if not fares:
                    logger.warning("Korean: no fares on %s", url)
                    return self._empty(req)

                offers = self._build_offers(fares, req)

                # RT: navigate to reverse route for inbound fares
                if req.return_from and offers and dest_slug:
                    try:
                        _rev_url = f"{_BASE}/flights-from-{dest_slug}-to-{origin_slug}"
                        await page.goto(_rev_url, wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(2.0)
                        _ib_json = await page.evaluate("""() => {
                            const el = document.querySelector('script#__NEXT_DATA__');
                            return el ? el.textContent : null;
                        }""")
                        if _ib_json:
                            _ib_fares = self._extract_fares(_ib_json)
                            _ib_best = float("inf")
                            for _f in _ib_fares:
                                _p = _f.get("totalPrice")
                                if _p:
                                    try:
                                        _pv = float(_p)
                                        if 0 < _pv < _ib_best:
                                            _ib_best = _pv
                                    except (ValueError, TypeError):
                                        pass
                            if _ib_best < float("inf"):
                                _ret = req.return_from
                                _ret_dt = datetime.combine(_ret, datetime.min.time()) if not isinstance(_ret, datetime) else _ret
                                _ke_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                                _ib_seg = FlightSegment(
                                    airline="KE",
                                    airline_name="Korean Air",
                                    flight_no="",
                                    origin=req.destination,
                                    destination=req.origin,
                                    departure=_ret_dt,
                                    arrival=_ret_dt,
                                    duration_seconds=0,
                                    cabin_class=_ke_cabin,
                                )
                                _ib_route = FlightRoute(segments=[_ib_seg], total_duration_seconds=0, stopovers=0)
                                for _i, _o in enumerate(offers):
                                    offers[_i] = FlightOffer(
                                        id=f"rt_{_o.id}",
                                        price=round(_o.price + _ib_best, 2),
                                        currency=_o.currency,
                                        price_formatted=f"{round(_o.price + _ib_best, 2):.2f} {_o.currency}",
                                        outbound=_o.outbound,
                                        inbound=_ib_route,
                                        airlines=_o.airlines,
                                        owner_airline=_o.owner_airline,
                                        booking_url=_o.booking_url,
                                        is_locked=False,
                                        source=_o.source,
                                        source_tier=_o.source_tier,
                                    )
                    except Exception:
                        pass

                _td = req.date_from.date() if isinstance(req.date_from, datetime) else req.date_from
                offers = [o for o in offers if o.outbound and o.outbound.segments and o.outbound.segments[0].departure.date() == _td]
                offers.sort(key=lambda o: o.price)

                elapsed = time.monotonic() - t0
                logger.info(
                    "Korean %s->%s returned %d offers in %.1fs (CDP)",
                    req.origin, req.destination, len(offers), elapsed,
                )

                h = hashlib.md5(
                    f"korean{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                ).hexdigest()[:12]
                return FlightSearchResponse(
                    search_id=f"fs_{h}",
                    origin=req.origin,
                    destination=req.destination,
                    currency=offers[0].currency if offers else (req.currency or "KRW"),
                    offers=offers,
                    total_results=len(offers),
                )

            finally:
                try:
                    await page.close()
                except Exception:
                    pass

        except Exception as e:
            logger.error("Korean CDP error: %s", e)
            return self._empty(req)

    @staticmethod
    def _extract_fares(nd_json: str) -> list[dict]:
        """Extract fare dicts from __NEXT_DATA__ apolloState.data StandardFareModule."""
        try:
            nd = json.loads(nd_json)
        except (json.JSONDecodeError, ValueError):
            return []

        apollo = (
            nd.get("props", {})
            .get("pageProps", {})
            .get("apolloState", {})
            .get("data", {})
        )
        if not apollo:
            return []

        for v in apollo.values():
            if isinstance(v, dict) and v.get("__typename") == "StandardFareModule":
                raw_fares = v.get("fares", [])
                if not raw_fares:
                    continue
                # Resolve Apollo refs or use inline fares
                resolved = []
                for f in raw_fares:
                    if isinstance(f, dict) and "__ref" in f:
                        ref_data = apollo.get(f["__ref"])
                        if ref_data and isinstance(ref_data, dict):
                            resolved.append(ref_data)
                    elif isinstance(f, dict):
                        resolved.append(f)
                if resolved:
                    return resolved
        return []

    def _build_offers(
        self, fares: list[dict], req: FlightSearchRequest
    ) -> list[FlightOffer]:
        target_date = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []

        for fare in fares:
            price = fare.get("totalPrice")
            if not price or price <= 0:
                continue

            currency = fare.get("currencyCode") or req.currency or "KRW"
            dep_date = fare.get("departureDate", "")
            origin_code = fare.get("originAirportCode") or req.origin
            dest_code = fare.get("destinationAirportCode") or req.destination

            dep_dt = datetime(2000, 1, 1)
            if dep_date:
                try:
                    dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
                except ValueError:
                    pass

            segment = FlightSegment(
                airline="KE",
                airline_name="Korean Air",
                flight_no="",
                origin=origin_code,
                destination=dest_code,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class=(fare.get("formattedTravelClass") or "Economy").lower(),
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=0,
                stopovers=0,
            )

            fid = hashlib.md5(
                f"ke_{origin_code}{dest_code}{dep_date}{price}".encode()
            ).hexdigest()[:12]

            # Korean Air cabin codes: Y=Economy, C=Business, F=First
            _ke_cabin = {"M": "Y", "W": "Y", "C": "C", "F": "F"}.get(req.cabin_class or "M", "Y")
            booking_url = (
                f"https://www.koreanair.com/booking/best-prices"
                f"?departureStation={origin_code}&arrivalStation={dest_code}"
                f"&departureDate={dep_date or target_date}"
                f"&adt={req.adults}&chd={req.children}&inf={req.infants}"
                f"&tripType={'RT' if req.return_from else 'OW'}&cabin={_ke_cabin}"
            )

            offers.append(FlightOffer(
                id=f"ke_{fid}",
                price=round(float(price), 2),
                currency=currency,
                price_formatted=fare.get("formattedTotalPrice") or f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Korean Air"],
                owner_airline="KE",
                booking_url=booking_url,
                is_locked=False,
                source="korean_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"korean{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "KRW",
            offers=[],
            total_results=0,
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
                    id=f"rt_kore_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
