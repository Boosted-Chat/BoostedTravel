"""
LATAM Airlines (LA/JJ) CDP Chrome connector — search URL + API interception.

LATAM is South America's largest airline group, operating from hubs in
Santiago (SCL), São Paulo (GRU), Lima (LIM), and Bogotá (BOG).
Their booking engine is a React SPA at latamairlines.com that calls a BFF
(Backend-For-Frontend) API at /bff/air-offers/v2/offers/search for availability.

Strategy (CDP Chrome + response interception):
1.  Launch REAL Chrome via CDP (LATAM uses reCAPTCHA Enterprise).
2.  Navigate to the LATAM flight-offers URL with pre-filled params.
3.  Intercept the BFF search API response (same domain, /bff/ path).
4.  Parse flight offers from the BFF response.

Discovered endpoints:
  - https://www.latamairlines.com/bff/air-offers/v2/offers/search?...

Search URL:
  https://www.latamairlines.com/us/en/flight-offers?outbound={YYYY-MM-DDT00:00:00.000Z}
    &inbound=null&origin={origin}&destination={dest}&adt={n}&chd={n}&inf={n}&trip=OW&cabin=Y&redemption=false&sort=RECOMMENDED
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, date as date_type, timedelta
from typing import Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, proxy_chrome_args, auto_block_if_proxied

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9456
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".la_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context."""
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
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("LA: connected to existing Chrome on port %d", _DEBUG_PORT)
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
                "LA: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    global _browser, _context, _pw_instance, _chrome_proc
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
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("LA: deleted stale Chrome profile")
        except Exception:
            pass


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_dt(s: str) -> datetime:
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ):
        try:
            dt = datetime.strptime(s[:len(fmt) + 6], fmt)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except (ValueError, IndexError):
            continue
    return datetime.strptime(s[:10], "%Y-%m-%d")


_SKIP = frozenset((
    "analytics", "google", "facebook", "doubleclick", "fonts.",
    "gtm.", "pixel", "amplitude", ".css", ".png", ".jpg", ".svg",
    ".gif", ".woff", ".ico", "demdex", "omtrdc",
    "newrelic", "nr-data", "medallia", "adobedtm",
    "tealium", "mparticle", "segment", "fullstory", "hotjar",
    "onetrust", "cookiebot",
))

_AVAIL_KEYS = (
    "bff/air-offers", "offers/search", "availability",
    "air-bound", "itinerar",
)


class LatamConnectorClient:
    """LATAM Airlines CDP Chrome connector — search URL + API interception."""

    def __init__(self, timeout: float = 50.0):
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

        context = await _get_context()
        page = await context.new_page()
        await auto_block_if_proxied(page)

        avail_data: dict = {}
        blocked = False

        async def _on_response(response):
            nonlocal blocked
            url_lower = response.url.lower()

            if any(s in url_lower for s in _SKIP):
                return

            status = response.status

            if status in (403, 429):
                if any(k in url_lower for k in _AVAIL_KEYS):
                    blocked = True
                    logger.warning("LA: %d on %s", status, response.url[:120])
                return

            if status != 200:
                return

            # Check for LATAM BFF search endpoint
            is_avail = (
                "bff/air-offers" in url_lower
                or "offers/search" in url_lower
                or any(k in url_lower for k in _AVAIL_KEYS)
            )
            if not is_avail:
                return

            try:
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = await response.text()
                if len(body) < 100:
                    return
                data = json.loads(body)
                if not isinstance(data, dict):
                    return

                if self._looks_like_flights(data):
                    if not avail_data:
                        avail_data.update(data)
                        logger.info(
                            "LA: captured flights (%d bytes) from %s",
                            len(body), response.url[:100],
                        )
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            url = self._build_search_url(req)
            logger.info("LA: loading %s->%s", req.origin, req.destination)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(4.0)

            # Dismiss cookie banners
            await self._dismiss_overlays(page)

            # Wait for BFF API response
            remaining = max(self.timeout - (time.monotonic() - t0), 20)
            deadline = time.monotonic() + remaining
            while not avail_data and not blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            if blocked:
                logger.warning("LA: bot protection triggered, resetting profile")
                await _reset_profile()
                return self._empty(req)

            if not avail_data:
                logger.warning("LA: no flight data captured")
                return self._empty(req)

            offers = self._parse_flights(avail_data, req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "LA %s->%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"la{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
            ).hexdigest()[:12]

            currency = self._get_currency(avail_data, req)

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("LA CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # URL
    # ------------------------------------------------------------------

    def _build_search_url(self, req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        outbound = dt.strftime("%Y-%m-%dT00%%3A00%%3A00.000Z")
        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0
        # LATAM cabin codes: Y=Economy, W=Premium Economy, C=Business
        _la_cabin = {"M": "Y", "W": "W", "C": "C", "F": "C"}.get(req.cabin_class or "M", "Y")
        return (
            f"https://www.latamairlines.com/us/en/flight-offers"
            f"?outbound={outbound}&inbound=null"
            f"&origin={req.origin}&destination={req.destination}"
            f"&adt={adults}&chd={children}&inf={infants}"
            f"&trip=OW&cabin={_la_cabin}&redemption=false&sort=RECOMMENDED"
        )

    async def _dismiss_overlays(self, page) -> None:
        for sel in (
            "#cookies-politics-button",
            "button:has-text('Accept')",
            "button:has-text('Got it')",
            "button:has-text('Continuar')",
            "[data-testid='cookie-bar-close']",
        ):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.is_visible(timeout=1000):
                    await btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Detection + Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _looks_like_flights(data: dict) -> bool:
        s = json.dumps(data)[:5000].lower()
        flight_sigs = (
            "flightoffer", "flightresult", "bounddetail", "segment",
            "departuredatetime", "departuretime", "airbound",
            "itinerar", "flightleg", "cabin", "faretype",
        )
        price_sigs = ('"price"', '"amount"', '"total"', '"fare"', '"lowestprice"')
        return any(sig in s for sig in flight_sigs) and any(sig in s for sig in price_sigs)

    def _parse_flights(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse LATAM BFF or GraphQL response."""
        offers: list[FlightOffer] = []

        inner = data
        for key in ("data", "response", "result"):
            if key in inner and isinstance(inner[key], (dict, list)):
                val = inner[key]
                inner = val if isinstance(val, dict) else {"items": val}

        currency = self._get_currency(data, req)

        # LATAM BFF format: data.content[] or data.flights[] etc.
        flight_list = None
        for key in (
            "content", "flights", "unbundledFlights", "flightOffers", "offers",
            "results", "items", "boundGroups",
            "unbundledItineraries", "itineraries",
        ):
            candidate = inner.get(key)
            if isinstance(candidate, list) and len(candidate) > 0:
                flight_list = candidate
                break

        if not flight_list:
            for v in inner.values():
                if isinstance(v, list) and len(v) >= 2:
                    if isinstance(v[0], dict) and self._get_price(v[0]):
                        flight_list = v
                        break

        if not flight_list:
            return offers

        for flight in flight_list[:50]:
            if not isinstance(flight, dict):
                continue

            price = self._get_price(flight)
            if not price or price <= 0:
                continue

            segments = self._extract_segments(flight, req)
            if not segments:
                continue

            total_dur = int(
                (segments[-1].arrival - segments[0].departure).total_seconds()
            ) if segments[-1].arrival > segments[0].departure else 0

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=max(len(segments) - 1, 0),
            )

            offer_key = f"la_{req.origin}_{req.destination}_{segments[0].departure.isoformat()}_{price}"
            offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]
            all_airlines = list({s.airline for s in segments})

            conditions, bags_price = self._extract_la_bag_info(flight, currency)

            offers.append(FlightOffer(
                id=f"la_{offer_id}",
                price=round(price, 2),
                currency=currency,
                outbound=route,
                airlines=[self._airline_name(a) for a in all_airlines],
                owner_airline="LA",
                conditions=conditions,
                bags_price=bags_price,
                booking_url=self._user_url(req),
                is_locked=False,
                source="latam_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _extract_la_bag_info(flight: dict, currency: str) -> tuple[dict, dict]:
        """Extract bag allowance from LATAM BFF flight entry."""
        conditions: dict[str, str] = {}
        bags_price: dict[str, float] = {}

        # BFF v2: summary.brands[] has fare family + price
        summary = flight.get("summary") or {}
        brands = summary.get("brands") if isinstance(summary, dict) else None
        fare_families = (
            flight.get("fareFamilies") or flight.get("cabins")
            or (brands if isinstance(brands, list) else None)
            or []
        )

        cheapest_family: dict = {}
        min_price = float("inf")
        for ff in (fare_families if isinstance(fare_families, list) else []):
            if not isinstance(ff, dict):
                continue
            p_obj = ff.get("price") or ff.get("amount") or ff.get("total") or {}
            p = (
                p_obj.get("amount") or p_obj.get("total")
                if isinstance(p_obj, dict) else p_obj
            )
            try:
                pf = float(p) if p is not None else float("inf")
            except (TypeError, ValueError):
                pf = float("inf")
            if pf < min_price:
                min_price = pf
                cheapest_family = ff
            # If price unknown, use first entry
            if not cheapest_family:
                cheapest_family = ff

        if not cheapest_family and isinstance(fare_families, list) and fare_families:
            cheapest_family = fare_families[0] if isinstance(fare_families[0], dict) else {}

        fare_name = str(
            cheapest_family.get("brandText") or cheapest_family.get("brandName")
            or cheapest_family.get("name") or cheapest_family.get("code")
            or cheapest_family.get("fareFamily") or ""
        )
        if fare_name:
            conditions["fare_family"] = fare_name

        bag_allow = (
            cheapest_family.get("baggageAllowance") or cheapest_family.get("baggage")
            or cheapest_family.get("checkedBag") or {}
        )
        if isinstance(bag_allow, dict) and bag_allow:
            qty = (
                bag_allow.get("quantity") or bag_allow.get("pieces")
                or bag_allow.get("count") or bag_allow.get("number")
            )
            if qty is not None:
                try:
                    qty_int = int(qty)
                    if qty_int == 0:
                        conditions["checked_bag"] = "no free checked bag (Light fare)"
                    else:
                        weight = bag_allow.get("weight") or bag_allow.get("maxWeight") or 23
                        conditions["checked_bag"] = f"{qty_int}x {weight}kg bag included"
                        bags_price["checked_bag"] = 0.0
                except (TypeError, ValueError):
                    pass

        # Fallback: infer from LATAM fare name
        if "checked_bag" not in conditions and fare_name:
            name_upper = fare_name.upper()
            if "LIGHT" in name_upper or "BASIC" in name_upper:
                conditions["checked_bag"] = "no free checked bag — add-on from ~USD 40"
                bags_price["checked_bag"] = 40.0
            elif any(k in name_upper for k in ("ECONOMY", "ECONOM", "PLUS", "FLEX")):
                if "PLUS" in name_upper or "FLEX" in name_upper:
                    conditions["checked_bag"] = "2x 23kg bags included"
                    bags_price["checked_bag"] = 0.0
                else:
                    conditions["checked_bag"] = "1x 23kg bag included"
                    bags_price["checked_bag"] = 0.0
            elif "PREMIUM" in name_upper or "BUSINESS" in name_upper or "TOP" in name_upper:
                conditions["checked_bag"] = "2x 32kg bags included"
                bags_price["checked_bag"] = 0.0
        # Carry-on: check API response first, then infer from fare family name
        if "carry_on" not in conditions:
            cabin_allow = (
                cheapest_family.get("cabinBagAllowance") or cheapest_family.get("cabinBag")
                or cheapest_family.get("handBagAllowance") or cheapest_family.get("handBag") or {}
            )
            if isinstance(cabin_allow, dict) and cabin_allow:
                qty = cabin_allow.get("quantity") or cabin_allow.get("pieces")
                if qty is not None:
                    try:
                        qty_int = int(qty)
                        weight = cabin_allow.get("weight") or cabin_allow.get("maxWeight") or 10
                        if qty_int == 0:
                            conditions["carry_on"] = "no free overhead carry-on — add-on from ~USD 40"
                            bags_price["carry_on"] = 40.0
                        else:
                            conditions["carry_on"] = f"{qty_int}x {weight}kg carry-on included"
                            bags_price["carry_on"] = 0.0
                    except (TypeError, ValueError):
                        pass
            if "carry_on" not in conditions:
                name_upper = fare_name.upper() if fare_name else ""
                if any(k in name_upper for k in ("LIGHT", "BASIC")):
                    conditions["carry_on"] = "no free overhead carry-on (Light/Basic) — add-on from ~USD 40"
                    bags_price["carry_on"] = 40.0
                elif any(k in name_upper for k in ("PREMIUM", "BUSINESS", "TOP")):
                    conditions["carry_on"] = "2x carry-on bags included"
                    bags_price["carry_on"] = 0.0
                else:
                    conditions["carry_on"] = "1x 10kg carry-on included"
                    bags_price["carry_on"] = 0.0

        # Seat selection
        if "seat" not in conditions:
            conditions["seat"] = "seat selection from ~USD 15 — add at checkout"
        conditions.setdefault("carry_on", "1 cabin bag (10 kg) + 1 personal item included")
        conditions.setdefault("seat", "seat selection from ~USD 15; included on Flex/Business fares")
        return conditions, bags_price

    def _extract_segments(self, flight: dict, req: FlightSearchRequest) -> list[FlightSegment]:
        segments: list[FlightSegment] = []

        seg_list = None
        for key in ("itinerary", "segments", "segmentList", "legs", "flightSegments", "flights"):
            candidate = flight.get(key)
            if isinstance(candidate, list) and len(candidate) > 0:
                seg_list = candidate
                break

        # LATAM sometimes nests: flight.legs[0].segments[]
        if not seg_list:
            legs = flight.get("legs", [])
            if isinstance(legs, list) and legs:
                combined = []
                for leg in legs:
                    if isinstance(leg, dict):
                        inner_segs = leg.get("segments", leg.get("flights", []))
                        if isinstance(inner_segs, list):
                            combined.extend(inner_segs)
                if combined:
                    seg_list = combined

        if not seg_list:
            dep = flight.get("departureDateTime") or flight.get("departure") or ""
            if dep:
                seg_list = [flight]

        if not seg_list:
            return segments

        for seg in seg_list:
            if not isinstance(seg, dict):
                continue

            dep_str = (
                seg.get("departureDateTime") or seg.get("departureTime")
                or seg.get("departure") or seg.get("departureDate") or ""
            )
            arr_str = (
                seg.get("arrivalDateTime") or seg.get("arrivalTime")
                or seg.get("arrival") or seg.get("arrivalDate") or ""
            )
            origin = (
                seg.get("departureAirportCode") or seg.get("origin")
                or seg.get("departureStation") or seg.get("from") or req.origin
            )
            dest = (
                seg.get("arrivalAirportCode") or seg.get("destination")
                or seg.get("arrivalStation") or seg.get("to") or req.destination
            )
            # BFF v2 nests carrier info in seg["flight"] sub-dict
            fl = seg.get("flight") or {}
            carrier = (
                fl.get("airlineCode") or fl.get("operatingAirlineCode")
                or seg.get("airlineCode") or seg.get("carrierCode")
                or seg.get("operatingCarrier") or seg.get("airline")
                or seg.get("marketingCarrier") or "LA"
            )
            fno = fl.get("flightNumber") or seg.get("flightNumber") or seg.get("flightNo") or ""
            fno = str(fno) if fno else ""
            dur = seg.get("duration") or seg.get("durationMinutes") or 0
            if isinstance(dur, str):
                # Parse ISO duration "PT5H30M"
                m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?", dur)
                dur = (int(m.group(1) or 0) * 3600 + int(m.group(2) or 0) * 60) if m else 0
            elif isinstance(dur, (int, float)) and dur < 1440:
                # BFF returns duration in minutes; convert to seconds
                dur = int(dur) * 60

            dep_dt = _parse_dt(dep_str) if dep_str else _to_datetime(req.date_from)
            arr_dt = _parse_dt(arr_str) if arr_str else dep_dt + timedelta(seconds=dur or 7200)

            _la_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            segments.append(FlightSegment(
                airline=carrier,
                airline_name=self._airline_name(carrier),
                flight_no=f"{carrier}{fno}" if fno and not fno.startswith(carrier) else (fno or f"{carrier}?"),
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur if isinstance(dur, int) else 0,
                cabin_class=_la_cabin,
            ))

        return segments

    @staticmethod
    def _get_price(obj: dict) -> Optional[float]:
        for key in (
            "price", "totalPrice", "lowestPrice", "amount",
            "fareAmount", "total", "bestPrice", "displayPrice",
            "adultPrice", "priceFrom",
        ):
            val = obj.get(key)
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
            if isinstance(val, dict):
                for ik in ("amount", "total", "value", "displayAmount"):
                    iv = val.get(ik)
                    if isinstance(iv, (int, float)) and iv > 0:
                        return float(iv)
        # Check nested fare families / brands
        fares = obj.get("fareFamilies") or obj.get("cabins") or []
        if isinstance(fares, list):
            for fare in fares:
                if isinstance(fare, dict):
                    p = fare.get("price") or fare.get("amount") or fare.get("total")
                    if isinstance(p, (int, float)) and p > 0:
                        return float(p)
                    if isinstance(p, dict):
                        a = p.get("amount") or p.get("total")
                        if isinstance(a, (int, float)) and a > 0:
                            return float(a)
        # BFF v2 format: summary.brands[].price.amount
        summary = obj.get("summary")
        if isinstance(summary, dict):
            brands = summary.get("brands")
            if isinstance(brands, list) and brands:
                bp = brands[0].get("price")
                if isinstance(bp, dict):
                    a = bp.get("amount")
                    if isinstance(a, (int, float)) and a > 0:
                        return float(a)
        return None

    @staticmethod
    def _get_currency(data: dict, req: FlightSearchRequest) -> str:
        for key in ("currencyCode", "currency", "originalCurrency"):
            val = data.get(key)
            if isinstance(val, str) and len(val) == 3:
                return val
        for v in data.values():
            if isinstance(v, dict):
                for key in ("currencyCode", "currency"):
                    val = v.get(key)
                    if isinstance(val, str) and len(val) == 3:
                        return val
        # BFF v2: content[0].summary.brands[0].price.currency
        content = data.get("content")
        if isinstance(content, list) and content:
            try:
                cur = content[0]["summary"]["brands"][0]["price"]["currency"]
                if isinstance(cur, str) and len(cur) == 3:
                    return cur
            except (KeyError, IndexError, TypeError):
                pass
        return "USD"

    @staticmethod
    def _airline_name(code: str) -> str:
        names = {
            "LA": "LATAM Airlines", "JJ": "LATAM Brasil",
            "4C": "LATAM Colombia", "4M": "LATAM Argentina",
            "LP": "LATAM Peru", "XL": "LATAM Ecuador",
        }
        return names.get(code, code)

    @staticmethod
    def _user_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        _la_cabin_url = {"M": "Y", "W": "W", "C": "C", "F": "C"}.get(req.cabin_class or "M", "Y")
        return (
            f"https://www.latamairlines.com/us/en/flight-offers"
            f"?outbound={dt.strftime('%Y-%m-%dT00:00:00.000Z')}&inbound=null"
            f"&origin={req.origin}&destination={req.destination}"
            f"&adt={req.adults or 1}&chd=0&inf=0"
            f"&trip=OW&cabin={_la_cabin_url}&redemption=false&sort=RECOMMENDED"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"la{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
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
                    id=f"rt_lata_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
