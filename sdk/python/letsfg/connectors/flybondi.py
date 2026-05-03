"""
Flybondi hybrid scraper -- curl_cffi SSR extraction (primary) + Playwright fallback.

Flybondi (IATA: FO) is an Argentine low-cost carrier operating domestic
and regional routes from Buenos Aires (EZE/AEP/BUE) and other Argentine cities.
Default currency ARS.

Strategy (hybrid — API first, browser fallback):
1. (Primary) Fetch SSR page via curl_cffi — extract viewer.flights.edges from
   inline <script> tag. ~1.5s, zero browser, zero RAM.
2. (Fallback) Playwright headed Chrome — navigate to results URL, extract SSR
   from JS context, or intercept GraphQL response.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL = True
except ImportError:
    HAS_CURL = False

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import stealth_args, auto_block_if_proxied, get_curl_cffi_proxies, get_default_proxy, find_chrome

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["es-AR", "es-UY", "es-PY", "en-US"]
_TIMEZONES = [
    "America/Argentina/Buenos_Aires", "America/Argentina/Cordoba",
    "America/Montevideo", "America/Asuncion",
]

_MAX_ATTEMPTS = 2


class FlybondiConnectorClient:
    """Flybondi hybrid scraper -- curl_cffi SSR extraction + Patchright fallback."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    def _build_search_url(self, req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        children = getattr(req, "children", 0) or 0
        infants = getattr(req, "infants", 0) or 0
        currency = req.currency or "ARS"
        url = (
            f"https://flybondi.com/ar/search/results"
            f"?departureDate={dep}"
            f"&adults={adults}&children={children}&infants={infants}"
            f"&currency={currency}"
            f"&fromCityCode={req.origin}&toCityCode={req.destination}"
        )
        if req.return_from:
            url += f"&returnDate={req.return_from.strftime('%Y-%m-%d')}"
        return url

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
        search_url = self._build_search_url(req)

        # ── Primary: curl_cffi SSR extraction (fast, no browser) ──
        if HAS_CURL:
            try:
                all_edges = await asyncio.to_thread(self._fetch_all_edges, search_url, req)
                if all_edges:
                    outbound_offers = self._parse_edges(all_edges, req)
                    if req.return_from and outbound_offers:
                        inbound_offers = self._parse_inbound_edges(all_edges, req)
                        if inbound_offers:
                            combos = self._build_rt_combos(outbound_offers, inbound_offers, req)
                            if combos:
                                outbound_offers = combos + outbound_offers
                    if outbound_offers:
                        elapsed = time.monotonic() - t0
                        logger.info(
                            "Flybondi API %s->%s: %d offers in %.1fs",
                            req.origin, req.destination, len(outbound_offers), elapsed,
                        )
                        return self._build_response(outbound_offers, req, elapsed)
                logger.warning("Flybondi API: no offers, falling back to Playwright")
            except Exception as e:
                logger.warning("Flybondi API error: %s — falling back to Playwright", e)

        # ── Fallback: Playwright browser ──
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(search_url, req)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning(
                    "Flybondi PW: attempt %d/%d returned no results",
                    attempt, _MAX_ATTEMPTS,
                )
            except Exception as e:
                logger.warning("Flybondi PW: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    def _fetch_all_edges(self, url: str, req: FlightSearchRequest) -> list[dict] | None:
        """Fetch SSR page via curl_cffi and return ALL flight edges (outbound + inbound)."""
        r = curl_requests.get(url, impersonate="chrome131", timeout=int(self.timeout), proxies=get_curl_cffi_proxies())
        if r.status_code != 200:
            logger.warning("Flybondi API: HTTP %d", r.status_code)
            return None
        scripts = re.findall(r'<script[^>]*>(.*?)</script>', r.text, re.DOTALL)
        for s in scripts:
            s = s.strip()
            if len(s) < 50000 or 'viewer' not in s:
                continue
            try:
                data = json.loads(s)
                edges = (
                    data.get("viewer", {})
                    .get("flights", {})
                    .get("edges", [])
                )
                if edges:
                    return edges
            except (json.JSONDecodeError, TypeError):
                continue
        for s in scripts:
            s = s.strip()
            if len(s) > 5000 and '"error"' in s:
                try:
                    data = json.loads(s)
                    err = data.get("error", {})
                    if err.get("graphqlError"):
                        logger.warning("Flybondi API: GraphQL error: %s",
                                       err.get("errorMessage", "")[:120])
                except (json.JSONDecodeError, TypeError):
                    pass
        return None

    def _parse_inbound_edges(self, edges: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse INBOUND direction flights from edges for RT combos."""
        currency = req.currency or "ARS"
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        for edge in edges:
            node = edge.get("node", {})
            if not node:
                continue
            direction = node.get("direction", "OUTBOUND")
            if direction != "INBOUND":
                continue
            offer = self._parse_flight_node(node, currency, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _build_rt_combos(
        self,
        outbound: list[FlightOffer],
        inbound: list[FlightOffer],
        req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Combine outbound × inbound into RT offers."""
        combos: list[FlightOffer] = []
        for ob in outbound[:15]:
            for ib in inbound[:10]:
                price = round(ob.price + ib.price, 2)
                combo_key = f"fo_rt_{ob.id}_{ib.id}"
                combos.append(FlightOffer(
                    id=f"fo_{hashlib.md5(combo_key.encode()).hexdigest()[:12]}",
                    price=price,
                    currency=ob.currency,
                    price_formatted=f"{price:,.2f} {ob.currency}",
                    outbound=ob.outbound,
                    inbound=ib.outbound,
                    airlines=list(set(ob.airlines + ib.airlines)),
                    owner_airline="FO",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="flybondi_direct",
                    source_tier="free",
                ))
        combos.sort(key=lambda o: o.price)
        return combos[:50]

    async def _attempt_search(
        self, url: str, req: FlightSearchRequest
    ) -> Optional[list[FlightOffer]]:
        """Use Patchright (undetected Chrome) to bypass Cloudflare Turnstile,
        then intercept ANY JSON response from flybondi.com that contains flight edges."""
        from patchright.async_api import async_playwright as patchright_playwright
        from .browser import get_default_proxy, find_chrome

        proxy = get_default_proxy()
        viewport = random.choice(_VIEWPORTS)
        launch_kwargs: dict = {
            "headless": False,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-first-run",
                "--no-default-browser-check",
                f"--window-size={viewport['width']},{viewport['height']}",
            ],
            "proxy": proxy,
        }
        try:
            chrome_path = find_chrome()
            if chrome_path:
                launch_kwargs["executable_path"] = chrome_path
        except Exception:
            pass

        pw = await patchright_playwright().start()
        browser = await pw.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            viewport=viewport,
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
        )

        try:
            page = await context.new_page()
            await auto_block_if_proxied(page)

            captured_flights: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    if response.status != 200:
                        return
                    rurl = response.url
                    # Intercept any JSON from flybondi.com that might contain flight data
                    if "flybondi.com" not in rurl:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    data = await response.json()
                    if not isinstance(data, dict):
                        return
                    # SSR-embedded path: data.viewer.flights.edges
                    edges = (
                        data.get("data", {})
                        .get("viewer", {})
                        .get("flights", {})
                        .get("edges", [])
                    ) or (
                        data.get("viewer", {})
                        .get("flights", {})
                        .get("edges", [])
                    )
                    if not edges:
                        # Try alternate top-level or Relay paths
                        for top_val in data.values():
                            if isinstance(top_val, dict):
                                for v in top_val.values():
                                    if isinstance(v, dict):
                                        candidate = v.get("flights", {}).get("edges", [])
                                        if candidate:
                                            edges = candidate
                                            break
                                if edges:
                                    break
                    if not edges:
                        # Try REST-style: look for a list of flights under any key
                        for v in data.values():
                            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                                if any(k in v[0] for k in ("departureDateTime", "departure", "origin", "flightNumber")):
                                    captured_flights["raw_list"] = v
                                    api_event.set()
                                    logger.info("Flybondi: captured %d flights (REST list) from %s", len(v), rurl[:80])
                                    return
                    if edges:
                        captured_flights["edges"] = edges
                        api_event.set()
                        logger.info("Flybondi: captured %d flight edges from %s", len(edges), rurl[:80])
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("Flybondi: loading with Patchright %s", url[:120])
            await page.goto(url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))

            await self._dismiss_cookies(page)

            # Wait up to 30s for Cloudflare Turnstile to pass and flight data to arrive
            try:
                await asyncio.wait_for(api_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

            # Also try SSR extraction after the full page load
            if not captured_flights:
                edges = await self._extract_ssr_edges(page)
                if edges:
                    logger.info("Flybondi: extracted %d flights from SSR (Patchright)", len(edges))
                    return self._parse_edges(edges, req)

            if "edges" in captured_flights:
                return self._parse_edges(captured_flights["edges"], req)

            logger.warning("Flybondi: no flight data found via SSR or GraphQL")
            return None
        finally:
            await context.close()
            await browser.close()
            await pw.stop()

    async def _extract_ssr_edges(self, page) -> Optional[list[dict]]:
        """Extract flight edges from SSR data embedded in a script tag."""
        ssr_data = await page.evaluate(r"""() => {
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                const t = s.textContent || '';
                if (t.length > 50000 && t.includes('viewer')) {
                    try {
                        const data = JSON.parse(t);
                        if (data && data.viewer && data.viewer.flights) {
                            return data.viewer.flights.edges || null;
                        }
                    } catch(e) {}
                }
            }
            return null;
        }""")
        if ssr_data and isinstance(ssr_data, list) and len(ssr_data) > 0:
            return ssr_data
        return None

    async def _dismiss_cookies(self, page) -> None:
        try:
            btn = page.locator("button:has-text('Aceptar')")
            if await btn.count() > 0:
                await btn.first.click(timeout=3000)
                await asyncio.sleep(0.5)
        except Exception:
            pass

    def _parse_edges(self, edges: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        currency = req.currency or "ARS"
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for edge in edges:
            node = edge.get("node", {})
            if not node:
                continue
            # Only include outbound flights
            direction = node.get("direction", "OUTBOUND")
            if direction != "OUTBOUND":
                continue

            offer = self._parse_flight_node(node, currency, req, booking_url)
            if offer:
                offers.append(offer)

        return offers

    def _parse_flight_node(
        self, node: dict, currency: str, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        price = self._extract_best_price(node)
        if price is None or price <= 0:
            return None

        # Use node currency if available
        currency = node.get("currency", currency) or currency

        # Build segments from legs
        legs_raw = node.get("legs", [])
        _fo_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
        segments: list[FlightSegment] = []
        for leg in legs_raw:
            segments.append(FlightSegment(
                airline="FO",
                airline_name="Flybondi",
                flight_no=str(leg.get("flightNo", node.get("flightNo", ""))),
                origin=leg.get("origin", node.get("origin", req.origin)),
                destination=leg.get("destination", node.get("destination", req.destination)),
                departure=self._parse_dt(leg.get("departureDate", "")),
                arrival=self._parse_dt(leg.get("arrivalDate", "")),
                cabin_class=_fo_cabin,
            ))

        if not segments:
            # Fallback: build segment from top-level node fields
            segments.append(FlightSegment(
                airline="FO",
                airline_name="Flybondi",
                flight_no=str(node.get("segmentFlightNo", node.get("flightNo", ""))),
                origin=node.get("origin", req.origin),
                destination=node.get("destination", req.destination),
                departure=self._parse_dt(node.get("departureDate", "")),
                arrival=self._parse_dt(node.get("arrivalDate", "")),
                cabin_class=_fo_cabin,
            ))

        # Total duration from node or compute from segments
        dur_min = node.get("flightTimeMinutes", 0)
        total_dur = dur_min * 60 if dur_min else 0
        if not total_dur and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=node.get("stops", max(len(segments) - 1, 0)),
        )

        flight_key = f"FO{node.get('flightNo', '')}_{node.get('departureDate', '')}_{node.get('origin', '')}"
        conditions, bags_price = self._extract_fo_bag_info(node, currency)
        return FlightOffer(
            id=f"fo_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:,.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Flybondi"],
            owner_airline="FO",
            conditions=conditions,
            bags_price=bags_price,
            booking_url=booking_url,
            is_locked=False,
            source="flybondi_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_fo_bag_info(node: dict, currency: str) -> tuple[dict, dict]:
        """Extract bag allowance from Flybondi GraphQL flight node fares."""
        conditions: dict[str, str] = {}
        bags_price: dict[str, float] = {}

        fares = node.get("fares", [])
        if not fares:
            return conditions, bags_price

        # Find STANDARD fare (same logic as _extract_best_price)
        cheapest_fare: dict = {}
        best_price = float("inf")
        for fare in fares:
            if not isinstance(fare, dict):
                continue
            fare_type = fare.get("type", "")
            prices = fare.get("prices", {})
            after_tax = prices.get("afterTax")
            if after_tax is not None and fare_type == "STANDARD":
                try:
                    v = float(after_tax)
                    if 0 < v < best_price:
                        best_price = v
                        cheapest_fare = fare
                except (TypeError, ValueError):
                    pass
        if not cheapest_fare:
            for fare in fares:
                if not isinstance(fare, dict):
                    continue
                prices = fare.get("prices", {})
                after_tax = prices.get("afterTax")
                if after_tax is not None:
                    try:
                        v = float(after_tax)
                        if 0 < v < best_price:
                            best_price = v
                            cheapest_fare = fare
                    except (TypeError, ValueError):
                        pass

        if not cheapest_fare:
            return conditions, bags_price

        fare_name = str(cheapest_fare.get("type") or cheapest_fare.get("name") or "")
        if fare_name:
            conditions["fare_family"] = fare_name

        # Try structured bag allowance from fare
        bag_allow = (
            cheapest_fare.get("baggageAllowance") or cheapest_fare.get("baggage")
            or cheapest_fare.get("checkedBaggage") or {}
        )
        if isinstance(bag_allow, dict) and bag_allow:
            qty = (
                bag_allow.get("quantity") or bag_allow.get("pieces") or bag_allow.get("count")
            )
            if qty is not None:
                try:
                    qty_int = int(qty)
                    if qty_int == 0:
                        conditions["checked_bag"] = "no free checked bag"
                    else:
                        weight = bag_allow.get("weight") or bag_allow.get("maxWeight") or 23
                        conditions["checked_bag"] = f"{qty_int}x {weight}kg bag included"
                        bags_price["checked_bag"] = 0.0
                except (TypeError, ValueError):
                    pass

        # Flybondi STANDARD base fare has no free checked bag (ultra-LCC)
        if "checked_bag" not in conditions:
            conditions["checked_bag"] = "no free checked bag (ultra-LCC fare)"

        # Carry-on: Flybondi is ultra-LCC — overhead bag always a paid add-on
        if "carry_on" not in conditions:
            conditions["carry_on"] = "overhead carry-on is a paid add-on (from ~USD 10)"

        # Seat selection
        if "seat" not in conditions:
            conditions["seat"] = "seat selection from ~USD 5 — add at checkout"

        return conditions, bags_price

    @staticmethod
    def _extract_best_price(node: dict) -> Optional[float]:
        """Extract cheapest STANDARD fare price (afterTax) from fares list."""
        fares = node.get("fares", [])
        best = float("inf")
        for fare in fares:
            # Prefer STANDARD fares over CLUB (member-only) fares
            fare_type = fare.get("type", "")
            prices = fare.get("prices", {})
            after_tax = prices.get("afterTax")
            if after_tax is not None and fare_type == "STANDARD":
                try:
                    val = float(after_tax)
                    if 0 < val < best:
                        best = val
                except (TypeError, ValueError):
                    pass
        # If no STANDARD fare found, try any fare
        if best == float("inf"):
            for fare in fares:
                prices = fare.get("prices", {})
                after_tax = prices.get("afterTax")
                if after_tax is not None:
                    try:
                        val = float(after_tax)
                        if 0 < val < best:
                            best = val
                    except (TypeError, ValueError):
                        pass
        return best if best < float("inf") else None

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Flybondi %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(
            f"flybondi{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "ARS"),
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
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
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        children = getattr(req, "children", 0) or 0
        infants = getattr(req, "infants", 0) or 0
        return (
            f"https://flybondi.com/ar/search/results"
            f"?departureDate={dep}"
            f"&adults={adults}&children={children}&infants={infants}"
            f"&currency={req.currency or 'ARS'}"
            f"&fromCityCode={req.origin}&toCityCode={req.destination}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"flybondi{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "ARS",
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
                    id=f"rt_flyb_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
