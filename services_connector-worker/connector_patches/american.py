"""
American Airlines patchright connector -- Cloud Run patch.

Replaces CDP Chrome with Patchright headed browser to bypass Akamai.
Form fill + ng-state extraction logic preserved from SDK.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
from collections import defaultdict
from datetime import datetime, date
from typing import Any, Optional

from ..models.flights import (
    AirlineSummary,
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .airline_routes import get_city_airports

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 2
_RESULTS_WAIT = 30


async def _launch_browser():
    """Launch a Patchright browser using the system Chrome binary."""
    from patchright.async_api import async_playwright
    from .browser import find_chrome, inject_stealth_js, auto_block_if_proxied

    proxy = None
    _BYPASS = ".google.com,.googletagmanager.com,.gstatic.com,.googleapis.com,.google-analytics.com,.googlesyndication.com,.doubleclick.net"
    letsfg_proxy = os.environ.get("LETSFG_PROXY", "").strip()
    if letsfg_proxy:
        import socket as _sock
        try:
            _s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
            _s.connect(("127.0.0.1", 8899))
            _s.close()
            proxy = {"server": "http://127.0.0.1:8899", "bypass": _BYPASS}
            logger.info("American: using proxy relay on port 8899")
        except OSError:
            from urllib.parse import urlparse
            p = urlparse(letsfg_proxy)
            proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}", "bypass": _BYPASS}
            if p.username:
                proxy["username"] = p.username
                proxy["password"] = p.password or ""
            logger.info("American: using direct proxy %s:%s", p.hostname, p.port)
    else:
        logger.info("American: no proxy, direct connection")

    try:
        chrome_path = find_chrome()
        logger.info("American: using system Chrome at %s", chrome_path)
    except RuntimeError:
        chrome_path = None
        logger.info("American: system Chrome not found, using bundled Chromium")

    pw = await async_playwright().start()
    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--no-default-browser-check",
        "--window-size=1366,768",
    ]
    launch_kwargs = dict(
        headless=False,
        args=launch_args,
        proxy=proxy,
    )
    if chrome_path:
        launch_kwargs["executable_path"] = chrome_path
    browser = await pw.chromium.launch(**launch_kwargs)
    context = await browser.new_context(
        viewport={"width": 1366, "height": 768},
        locale="en-US",
        timezone_id="America/New_York",
        color_scheme="light",
    )
    page = await context.new_page()

    # Stealth JS and resource blocking BEFORE navigation
    await inject_stealth_js(page)
    await auto_block_if_proxied(page)

    return pw, browser, context, page


class AmericanConnectorClient:
    """American Airlines -- Patchright + form fill + ng-state extraction."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob = await self._search_ow(req)
        if req.return_from and ob.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib = await self._search_ow(ib_req)
            if ib.total_results > 0:
                ob.offers = self._combine_rt(ob.offers, ib.offers, req)
                ob.total_results = len(ob.offers)
        return ob

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        origins = get_city_airports(req.origin)
        if len(origins) > 1:
            req = FlightSearchRequest(origin=origins[0], destination=req.destination, date_from=req.date_from, return_from=req.return_from, adults=req.adults, children=req.children, infants=req.infants, cabin_class=req.cabin_class, currency=req.currency, max_stopovers=req.max_stopovers)
        t0 = time.monotonic()
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._attempt_search(req, t0)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning("American: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)
        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[FlightSearchResponse]:
        pw = browser = context = page = None
        try:
            pw, browser, context, page = await _launch_browser()

            # Block OneTrust cookie consent overlay
            async def _abort_route(route):
                await route.abort()
            for pattern in ("**/*onetrust*", "**/*cookielaw*", "**/*optanon*"):
                try:
                    await page.route(pattern, _abort_route)
                except Exception:
                    pass
            try:
                await page.add_init_script("""
                    (function() {
                        const kill = () => {
                            const sdk = document.getElementById('onetrust-consent-sdk');
                            if (sdk) sdk.remove();
                            document.querySelectorAll('.onetrust-pc-dark-filter, .ot-fade-in').forEach(e => e.remove());
                            if (document.body) document.body.style.overflow = 'auto';
                        };
                        kill();
                        const obs = new MutationObserver(kill);
                        if (document.body) { obs.observe(document.body, { childList: true, subtree: true }); }
                        else { document.addEventListener('DOMContentLoaded', () => { kill(); obs.observe(document.body, { childList: true, subtree: true }); }); }
                    })();
                """)
            except Exception:
                pass

            logger.info("American: searching %s->%s on %s", req.origin, req.destination, req.date_from)

            url = "https://www.aa.com/booking/search/find-flights"
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            # Log page state for diagnostics
            page_info = await page.evaluate("""() => {
                return {
                    url: location.href,
                    title: document.title,
                    bodyLen: (document.body?.innerText || '').length,
                    snippet: (document.body?.innerText || '').slice(0, 300).replace(/\\s+/g, ' ')
                };
            }""")
            logger.info("American: page loaded — url=%s title=%s bodyLen=%d",
                        page_info.get("url", "?")[:80], page_info.get("title", "?")[:60],
                        page_info.get("bodyLen", 0))
            if page_info.get("bodyLen", 0) < 200:
                logger.info("American: page snippet: %s", page_info.get("snippet", "")[:200])

            # Remove OneTrust overlay if it loaded before init_script
            await page.evaluate("""() => {
                document.querySelectorAll('[id*="onetrust"], .onetrust-pc-dark-filter, .ot-fade-in').forEach(e => e.remove());
                if (document.body) document.body.style.overflow = 'auto';
            }""")

            # Wait for form to load (check broader selectors)
            form_found = False
            for _w in range(25):
                has_form = await page.evaluate("""() => {
                    return !!(document.querySelector('[aria-label="Departure airport"]')
                        || document.querySelector('#departureAirport')
                        || document.querySelector('input[name*="origin"]')
                        || document.querySelector('[class*="airport"]')
                        || document.querySelector('[class*="booking-form"]'));
                }""")
                if has_form:
                    form_found = True
                    break
                await asyncio.sleep(1)
            await asyncio.sleep(0.5)

            if not form_found:
                logger.warning("American: booking form not found after 25s")
                return None

            # Human-like mouse moves
            for _ in range(2):
                await page.mouse.move(random.randint(200, 800), random.randint(100, 400), steps=random.randint(5, 8))
                await asyncio.sleep(random.uniform(0.2, 0.4))

            # Select one way
            await self._select_one_way(page)
            await asyncio.sleep(0.3)

            # Fill origin
            ok = await self._fill_airport(page, "Departure airport", req.origin)
            if not ok:
                logger.warning("American: origin fill failed")
                return None
            await asyncio.sleep(0.4)

            # Fill destination
            ok = await self._fill_airport(page, "Arrival airport", req.destination)
            if not ok:
                logger.warning("American: dest fill failed")
                return None
            await asyncio.sleep(0.4)

            # Fill date -- mm/dd/yyyy format
            date_str = req.date_from.strftime("%m/%d/%Y")
            date_input = page.get_by_role("textbox", name="Departure date")
            if await date_input.count() == 0:
                date_input = page.locator('#departDate, [aria-label*="Depart"], input[name*="departDate"]').first
            await date_input.click(timeout=5000)
            await asyncio.sleep(0.3)
            await page.keyboard.press("Control+a")
            await asyncio.sleep(0.1)
            await date_input.press_sequentially(date_str, delay=80)
            await asyncio.sleep(0.3)

            # Close any calendar popup
            try:
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.2)
            except Exception:
                pass

            # Click search
            search_btn = page.locator('#flightSearchSubmit, button[type="submit"][id*="earch"], input[type="submit"]').first
            try:
                await search_btn.click(timeout=5000)
            except Exception:
                # Fallback: the AA search page uses a plain "Search" button
                await page.get_by_role("button", name="Search").first.click(timeout=5000)

            # Wait for results page
            try:
                await page.wait_for_url("**/choose-flights/**", timeout=_RESULTS_WAIT * 1000)
            except Exception:
                logger.warning("American: did not navigate to results page")
                # Try to extract anyway

            await asyncio.sleep(1.5)

            # Extract ng-state
            ng_text = await page.evaluate("""() => {
                const el = document.getElementById('ng-state');
                return el ? el.textContent : null;
            }""")

            if not ng_text:
                logger.warning("American: ng-state not found")
                return None

            try:
                ng_data = json.loads(ng_text)
            except Exception:
                logger.warning("American: ng-state parse failed")
                return None

            elapsed = time.monotonic() - t0
            offers = self._parse_ng_state(ng_data, req)
            return self._build_response(offers, req, elapsed)

        except asyncio.TimeoutError:
            logger.warning("American: search timed out")
            return None
        except Exception as e:
            logger.warning("American: search error: %s", e)
            return None
        finally:
            try:
                if page: await page.close()
            except Exception: pass
            try:
                if context: await context.close()
            except Exception: pass
            try:
                if browser: await browser.close()
            except Exception: pass
            try:
                if pw: await pw.stop()
            except Exception: pass

    async def _select_one_way(self, page) -> None:
        try:
            ow_radio = page.locator('#booking-type-1, label:has-text("One way"), [aria-label="One way"]').first
            if await ow_radio.count() > 0 and await ow_radio.is_visible():
                await ow_radio.click(timeout=3000)
                await asyncio.sleep(0.3)
                return
            await page.evaluate("""() => {
                const labels = document.querySelectorAll('label');
                for (const lb of labels) {
                    if (lb.textContent.toLowerCase().includes('one way')) { lb.click(); return; }
                }
            }""")
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug("American: one way selection error: %s", e)

    async def _fill_airport(self, page, label: str, iata: str) -> bool:
        try:
            # Try aria-label combobox first
            combo = page.get_by_role("combobox", name=label).first
            if await combo.count() > 0:
                await combo.click(timeout=5000)
                await asyncio.sleep(0.4)
                await combo.fill("")
                await asyncio.sleep(0.2)
                await combo.press_sequentially(iata, delay=120)
                await asyncio.sleep(0.6)
                option = page.locator("[role='option'], .airport-item, .suggestion-item").filter(has_text=iata).first
                await option.click(timeout=5000)
                await asyncio.sleep(0.3)
                return True
        except Exception:
            pass

        try:
            # Fallback: generic input
            inp = page.locator(f'input[aria-label*="{label.split()[0]}"]').first
            await inp.click(timeout=5000)
            await asyncio.sleep(0.4)
            await inp.fill("")
            await inp.press_sequentially(iata, delay=120)
            await asyncio.sleep(0.6)
            opt = page.locator("[role='option']").filter(has_text=iata).first
            await opt.click(timeout=5000)
            await asyncio.sleep(0.3)
            return True
        except Exception as e:
            logger.debug("American: airport fill '%s' error: %s", label, e)
            return False

    # -- Parsing (from SDK) ---

    def _parse_ng_state(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        search_data = None
        for key, val in data.items():
            if isinstance(val, dict):
                if "itineraryResult" in val or "SearchData" in str(key):
                    search_data = val
                    break
                if "SearchData" in val:
                    search_data = val.get("SearchData")
                    break
        if not search_data:
            # Deep search
            search_data = self._find_nested(data, "itineraryResult")
        if not search_data:
            logger.warning("American: no SearchData in ng-state")
            return offers

        itinerary = search_data.get("itineraryResult") or search_data
        slices_raw = itinerary.get("slices") or []
        for sl in slices_raw:
            offer = self._parse_slice(sl, req)
            if offer:
                offers.append(offer)
        return offers

    def _find_nested(self, obj: Any, key: str) -> Any:
        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            for v in obj.values():
                r = self._find_nested(v, key)
                if r is not None:
                    return r
        elif isinstance(obj, list):
            for item in obj:
                r = self._find_nested(item, key)
                if r is not None:
                    return r
        return None

    def _parse_slice(self, sl: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        if not isinstance(sl, dict):
            return None
        price = None
        cheapest_price = sl.get("cheapestPrice") or sl.get("lowestPrice") or {}
        if isinstance(cheapest_price, dict):
            price = cheapest_price.get("perPassengerDisplayTotal", {}).get("amount")
            if price is None:
                price = cheapest_price.get("amount") or cheapest_price.get("totalAmount")
        if price is None:
            price = sl.get("price") or sl.get("totalPrice") or sl.get("displayPrice")
            if isinstance(price, dict):
                price = price.get("amount") or price.get("value")
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        currency = "USD"
        if isinstance(cheapest_price, dict):
            currency = cheapest_price.get("perPassengerDisplayTotal", {}).get("currency") or cheapest_price.get("currency") or "USD"

        segments: list[FlightSegment] = []
        seg_raw = sl.get("segments") or sl.get("legs") or sl.get("flights") or []
        for seg in seg_raw:
            if not isinstance(seg, dict):
                continue
            legs = seg.get("legs") or [seg]
            for leg in legs:
                dep = leg.get("departureDateTime") or leg.get("departure") or ""
                arr = leg.get("arrivalDateTime") or leg.get("arrival") or ""
                carrier_code = (leg.get("flight", {}).get("carrierCode") or leg.get("operatingCarrier", {}).get("code") or leg.get("carrier") or "AA")
                flight_num = str(leg.get("flight", {}).get("flightNumber") or leg.get("flightNumber") or "")
                segments.append(FlightSegment(
                    airline=carrier_code,
                    airline_name=_carrier_name(carrier_code),
                    flight_no=f"{carrier_code}{flight_num}" if flight_num and not flight_num.startswith(carrier_code) else flight_num,
                    origin=leg.get("origin", {}).get("code") or leg.get("departureAirport") or req.origin,
                    destination=leg.get("destination", {}).get("code") or leg.get("arrivalAirport") or req.destination,
                    departure=_parse_dt(dep),
                    arrival=_parse_dt(arr),
                ))
        if not segments:
            return None

        total_dur = 0
        dur_mins = sl.get("durationInMinutes") or sl.get("totalDuration")
        if dur_mins:
            try:
                total_dur = int(dur_mins) * 60
            except (TypeError, ValueError):
                pass
        if total_dur == 0 and segments[0].departure and segments[-1].arrival:
            try:
                td0 = datetime.fromisoformat(segments[0].departure)
                td1 = datetime.fromisoformat(segments[-1].arrival)
                total_dur = int((td1 - td0).total_seconds())
            except Exception:
                pass

        route = FlightRoute(segments=segments, total_duration_seconds=max(total_dur, 0), stopovers=max(len(segments) - 1, 0))
        airlines = list(dict.fromkeys(s.airline for s in segments if s.airline))
        slice_id = sl.get("id") or sl.get("sliceId") or hashlib.md5(json.dumps(sl, default=str).encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"aa-{hashlib.md5(str(slice_id).encode()).hexdigest()[:14]}",
            price=round(price, 2), currency=currency,
            price_formatted=f"${price:.2f}" if currency == "USD" else f"{price:.2f} {currency}",
            outbound=route, airlines=airlines, owner_airline="AA",
            source="american_direct", source_tier="protocol", is_locked=True,
            booking_url=f"https://www.aa.com/booking/choose-flights?locale=en_US&sliceIndex=0&from={req.origin}&to={req.destination}&departDate={req.date_from.strftime('%Y/%m/%d')}&tripType=OneWay&passengers=1",
        )

    def _build_response(self, offers, req, elapsed):
        offers.sort(key=lambda o: o.price)
        by_airline: dict[str, list[FlightOffer]] = defaultdict(list)
        for o in offers:
            by_airline[o.owner_airline or "AA"].append(o)
        airlines_summary = [
            AirlineSummary(airline_code=c, airline_name=_carrier_name(c), cheapest_price=min(al, key=lambda o: o.price).price, currency=min(al, key=lambda o: o.price).currency, offer_count=len(al), cheapest_offer_id=min(al, key=lambda o: o.price).id, sample_route=f"{req.origin}->{req.destination}")
            for c, al in by_airline.items()
        ]
        logger.info("American: %d offers for %s->%s on %s (%.1fs)", len(offers), req.origin, req.destination, req.date_from, elapsed)
        return FlightSearchResponse(
            search_id=hashlib.md5(f"aa-{req.origin}-{req.destination}-{req.date_from}-{time.time()}".encode()).hexdigest()[:12],
            origin=req.origin, destination=req.destination, currency=offers[0].currency if offers else "USD",
            offers=offers[:req.limit], total_results=len(offers), airlines_summary=airlines_summary,
            search_params={"source": "american_direct", "method": "patchright_form_fill_ng_state", "elapsed": round(elapsed, 2)},
            source_tiers={"protocol": "American Airlines direct (aa.com)"},
        )

    @staticmethod
    def _combine_rt(ob, ib, req):
        combos = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(id=f"rt_aa_{cid}", price=price, currency=o.currency, outbound=o.outbound, inbound=i.outbound, airlines=list(dict.fromkeys(o.airlines + i.airlines)), owner_airline=o.owner_airline, booking_url=o.booking_url, is_locked=False, source=o.source, source_tier=o.source_tier))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req):
        return FlightSearchResponse(origin=req.origin, destination=req.destination, currency="USD", offers=[], total_results=0, search_params={"source": "american_direct", "error": "no_results"}, source_tiers={"protocol": "American Airlines direct (aa.com)"})


def _parse_dt(s: str) -> str:
    if not s:
        return ""
    try:
        return datetime.fromisoformat(s).isoformat()
    except (ValueError, TypeError):
        return s

def _carrier_name(code: str) -> str:
    return {"AA": "American Airlines", "DL": "Delta Air Lines", "UA": "United Airlines", "BA": "British Airways", "AS": "Alaska Airlines", "B6": "JetBlue Airways", "OH": "PSA Airlines", "MQ": "Envoy Air", "YX": "Republic Airways", "9E": "Endeavor Air", "OO": "SkyWest Airlines", "QF": "Qantas", "JL": "Japan Airlines", "CX": "Cathay Pacific", "IB": "Iberia", "QR": "Qatar Airways"}.get(code, code)