"""
Aviasales connector — Russia/CIS's largest flight meta-search (CDP Chrome + GraphQL interception).

Covers Aviasales, JetRadar (int'l brand), OneTwoTrip, KupiBiliet.
Aviasales uses a GraphQL API at ariadne.aviasales.com/api/gql which requires
browser-level session cookies (Cloudflare protected).

Strategy (CDP Chrome + API response interception):
1. Launch real Chrome via --remote-debugging-port.
2. Connect via Playwright CDP.
3. Navigate to aviasales.com → fill search form → submit.
4. Intercept GraphQL responses from ariadne.aviasales.com/api/gql.
5. Parse ticket data → FlightOffers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import find_chrome, stealth_popen_kwargs, _launched_procs, bandwidth_saving_args, disable_background_networking_args, apply_cdp_url_blocking

logger = logging.getLogger(__name__)

_BASE = "https://www.aviasales.com"
_CDP_PORT = 9465
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".aviasales_chrome_data"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_context = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


async def _get_context():
    global _context
    browser = await _get_browser()
    if _context:
        try:
            if _context.pages:
                return _context
        except Exception:
            pass
    contexts = browser.contexts
    if contexts:
        _context = contexts[0]
    else:
        _context = await browser.new_context(viewport={"width": 1366, "height": 768})
    return _context


async def _get_browser():
    global _pw_instance, _browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
            _pw_instance = pw
            logger.info("Aviasales: connected to existing Chrome on port %d", _CDP_PORT)
            return _browser
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
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            *bandwidth_saving_args(),
            *disable_background_networking_args(),
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
        logger.info("Aviasales: Chrome launched on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


async def _dismiss_cookies(page) -> None:
    for label in ["Accept", "Accept all", "OK", "Got it", "I agree", "Agree"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue


class AviasalesConnectorClient:
    """Aviasales — Russia/CIS flight meta-search, CDP Chrome + GraphQL interception."""

    def __init__(self, timeout: float = 55.0):
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
                    timeout=45.0,
                )
                if ancillary:
                    self._apply_ancillaries(ob_result.offers, ancillary)
            except (asyncio.TimeoutError, TimeoutError):
                logger.debug("Ancillary fetch timed out for %s\u2192%s", anc_origin, anc_dest)
            except Exception as _anc_err:
                logger.debug("Ancillary fetch error for %s\u2192%s: %s", anc_origin, anc_dest, _anc_err)
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str
    ) -> dict | None:
        """Aviasales is a meta-search; underlying airline bag pricing not available."""
        return {
            "checked_bag": "baggage depends on airline and fare – may be included or add-on from ~20 €; check at checkout",
            "bags_note": "personal item free on most airlines; cabin bag varies by airline and fare",
            "seat_note": "seat selection varies by airline; skip at checkout for free random seat",
        }
    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        bags_note = ancillary.get("bags_note")
        checked_note = ancillary.get("checked_bag") or bags_note
        seat_note = ancillary.get("seat_note")
        bags_from = ancillary.get("bags_from")
        checked_from = ancillary.get("checked_bag_price")
        anc_currency = ancillary.get("currency", "USD")
        for offer in offers:
            if bags_note:
                offer.conditions["carry_on"] = bags_note
            if checked_note:
                offer.conditions.setdefault("checked_bag", checked_note)
            if seat_note:
                offer.conditions["seat"] = seat_note
            if bags_from == 0.0:
                offer.bags_price["carry_on"] = 0.0
            if checked_from == 0.0:
                offer.bags_price["checked_bag"] = 0.0

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")
        date_ddmm = req.date_from.strftime("%d%m")

        context = await _get_context()
        page = await context.new_page()
        await apply_cdp_url_blocking(page)

        # Capture ALL JSON responses — Aviasales uses multiple APIs (GQL, search API, polling)
        captured: list[dict] = []

        async def _on_response(response):
            url = response.url
            if response.status == 200:
                ct = response.headers.get("content-type", "")
                if "json" in ct or "graphql" in url or "gql" in url:
                    try:
                        data = await response.json()
                        if isinstance(data, dict):
                            captured.append(data)
                        elif isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict):
                                    captured.append(item)
                    except Exception:
                        pass
                # Also capture text responses that might be JSONP or newline-delimited JSON
                elif any(kw in url for kw in ("search", "ticket", "result", "poll", "chunk")):
                    try:
                        text = await response.text()
                        if text and text.strip().startswith(("{", "[")):
                            data = json.loads(text)
                            if isinstance(data, dict):
                                captured.append(data)
                            elif isinstance(data, list):
                                for item in data:
                                    if isinstance(item, dict):
                                        captured.append(item)
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            # Use deep-link format: /search/{ORIGIN}{DDMM}{DEST}{adults}
            # e.g. /search/LON1506BCN1  (LON, June 15, BCN, 1 adult)
            adults = req.adults or 1
            deep_url = f"{_BASE}/search/{req.origin}{date_ddmm}{req.destination}{adults}"
            logger.info("Aviasales: navigating to deep-link %s", deep_url)
            await page.goto(deep_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            await asyncio.sleep(3.0)  # Aviasales needs more time to init search

            await _dismiss_cookies(page)

            # Wait for search results to load — Aviasales uses async polling
            # Results typically appear after 5-15 seconds of background XHR polling
            remaining = max(self.timeout - (time.monotonic() - t0), 25)
            deadline = time.monotonic() + remaining
            all_offers: list[FlightOffer] = []
            last_captured_count = 0

            while time.monotonic() < deadline and not all_offers:
                # Parse all captured responses looking for ticket/flight data
                if len(captured) > last_captured_count:
                    # Re-parse all chunks (later chunks may have more flight_legs)
                    all_offers.clear()
                    last_captured_count = len(captured)
                    for chunk_data in captured:
                        offers = self._parse_gql(chunk_data, req, date_str)
                        all_offers.extend(offers)

                # Also try DOM extraction for server-rendered results
                if not all_offers:
                    try:
                        dom_offers = await self._extract_from_dom(page, req, date_str)
                        if dom_offers:
                            all_offers.extend(dom_offers)
                    except Exception:
                        pass

                if not all_offers:
                    # Check if there's a "no results", error, or "Oops" indicator
                    try:
                        body_text = await page.evaluate("() => document.body?.textContent?.substring(0, 2000) || ''")
                        if "Oops" in body_text and "search failed" in body_text:
                            logger.info("Aviasales: search failed to launch — retrying not possible")
                            break
                        no_results = await page.locator('[class*="no-result"], [class*="empty"], [data-test-id="no-tickets"]').count()
                        if no_results > 0:
                            logger.info("Aviasales: 'no results' indicator found, stopping")
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(2.5)

            # Deduplicate by offer id
            seen = set()
            unique: list[FlightOffer] = []
            for o in all_offers:
                if o.id not in seen:
                    seen.add(o.id)
                    unique.append(o)

            unique.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("Aviasales %s→%s: %d offers in %.1fs (CDP Chrome)", req.origin, req.destination, len(unique), elapsed)

            sh = hashlib.md5(f"aviasales{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
                currency=unique[0].currency if unique else "EUR",
                offers=unique, total_results=len(unique),
            )
        except Exception as e:
            logger.error("Aviasales CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _extract_from_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Extract flight results from aviasales DOM when API interception misses data."""
        try:
            data = await page.evaluate("""() => {
                const results = [];
                
                // Aviasales uses multiple possible card selectors depending on version
                const selectors = [
                    '[data-test-id="ticket"]',
                    '[class*="ticket-desktop"]',
                    '[class*="ResultCard"]',
                    '[class*="result-card"]',
                    '[class*="TicketDesktop"]',
                    '[class*="ticket_ticket"]',
                    '[data-test="ticket"]',
                    'article[class*="ticket"]',
                    'div[class*="product-list"] > div',
                ];
                
                let cards = [];
                for (const sel of selectors) {
                    cards = document.querySelectorAll(sel);
                    if (cards.length > 0) break;
                }
                
                for (const card of cards) {
                    // Price extraction — multiple possible selectors
                    let priceText = '';
                    for (const psel of [
                        '[data-test-id="price"]', '[class*="price"]', '[class*="Price"]',
                        '[data-test="price"]', '[class*="buy-btn"]', '[class*="amount"]',
                    ]) {
                        const el = card.querySelector(psel);
                        if (el && el.textContent.match(/\\d/)) {
                            priceText = el.textContent;
                            break;
                        }
                    }
                    
                    // Airline name
                    let airline = '';
                    for (const asel of [
                        '[data-test-id="airline"]', '[class*="airline"]', '[class*="carrier"]',
                        'img[alt]', '[class*="Airline"]',
                    ]) {
                        const el = card.querySelector(asel);
                        if (el) {
                            airline = (el.alt || el.title || el.textContent || '').trim();
                            if (airline) break;
                        }
                    }
                    
                    if (priceText) {
                        // Clean price: remove currency symbols, spaces, and handle commas
                        const cleaned = priceText.replace(/[^0-9.,]/g, '').replace(/\\s/g, '');
                        // Handle European format: 1.234,56 vs US: 1,234.56
                        let price = 0;
                        if (cleaned.includes(',') && cleaned.indexOf(',') > cleaned.lastIndexOf('.')) {
                            price = parseFloat(cleaned.replace('.', '').replace(',', '.'));
                        } else {
                            price = parseFloat(cleaned.replace(/,/g, ''));
                        }
                        
                        if (price > 0) {
                            results.push({
                                price: price,
                                airline: airline || '',
                            });
                        }
                    }
                }
                return results;
            }""")

            offers: list[FlightOffer] = []
            if not data or not isinstance(data, list):
                return offers

            for item in data[:30]:
                price = float(item.get("price", 0))
                if price <= 0:
                    continue

                airline = item.get("airline", "Aviasales")
                segments = [FlightSegment(
                    airline=airline, flight_no="",
                    origin=req.origin, destination=req.destination,
                    departure=datetime(2000, 1, 1), arrival=datetime(2000, 1, 1),
                    duration_seconds=0,
                )]
                route = FlightRoute(segments=segments, total_duration_seconds=0, stopovers=0)
                oid = hashlib.md5(f"avsls_dom_{req.origin}{req.destination}{date_str}{price}".encode()).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"avsls_{oid}", price=round(price, 2), currency="EUR",
                    price_formatted=f"{price:.2f} EUR",
                    outbound=route, inbound=None,
                    airlines=[airline] if airline else ["Aviasales"],
                    owner_airline=airline or "Aviasales",
                    booking_url=f"https://www.aviasales.com/search/{req.origin}{req.destination}{req.date_from.strftime('%d%m')}1",
                    is_locked=False, source="aviasales_meta", source_tier="free",
                ))
            return offers
        except Exception:
            return []

    def _parse_gql(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse Aviasales v3.2 search results chunk.

        The API returns chunks with structure:
          {tickets: [...], flight_legs: [...], airlines: {...}, ...}
        Ticket segments reference flight_legs by index.
        """
        offers: list[FlightOffer] = []

        # v3.2 chunk format: tickets + flight_legs at top level
        tickets = data.get("tickets")
        flight_legs = data.get("flight_legs") or []
        airlines_map = data.get("airlines") or {}

        if isinstance(tickets, list) and tickets:
            offers.extend(self._parse_v3_tickets(tickets, flight_legs, airlines_map, req, date_str))
            return offers

        # Fallback: GraphQL/legacy wrapper
        inner = data.get("data") or data
        if isinstance(inner, dict):
            for key in ("tickets", "flights", "results", "proposals", "searchResults"):
                val = inner.get(key)
                if isinstance(val, list) and val:
                    fl = inner.get("flight_legs") or flight_legs
                    am = inner.get("airlines") or airlines_map
                    offers.extend(self._parse_v3_tickets(val, fl, am, req, date_str))
                    break

        return offers

    def _parse_v3_tickets(self, tickets: list, flight_legs: list, airlines_map: dict,
                          req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        """Parse tickets from v3.2 response where segments reference flight_legs by index."""
        offers: list[FlightOffer] = []
        for ticket in tickets[:50]:
            if not isinstance(ticket, dict):
                continue
            try:
                # Price from first proposal
                proposals = ticket.get("proposals") or []
                if not proposals:
                    continue
                proposal = proposals[0]
                price_obj = proposal.get("price") or proposal.get("price_per_person") or {}
                price = float(price_obj.get("value") or 0)
                if price <= 0:
                    continue
                currency = price_obj.get("currency_code") or "USD"

                # Resolve flight legs from segment indices
                seg_groups = ticket.get("segments") or []
                if not seg_groups:
                    continue

                segments: list[FlightSegment] = []
                for seg_group in seg_groups:
                    flight_indices = seg_group.get("flights") or []
                    for idx in flight_indices:
                        if not isinstance(idx, int) or idx < 0 or idx >= len(flight_legs):
                            continue
                        leg = flight_legs[idx]
                        carrier_info = leg.get("operating_carrier_designator") or {}
                        carrier = carrier_info.get("carrier") or carrier_info.get("airline_id") or ""
                        flight_num = str(carrier_info.get("number") or "")
                        dep_airport = leg.get("origin") or req.origin
                        arr_airport = leg.get("destination") or req.destination
                        dep_str = leg.get("local_departure_date_time") or ""
                        arr_str = leg.get("local_arrival_date_time") or ""
                        dep_ts = leg.get("departure_unix_timestamp") or 0
                        arr_ts = leg.get("arrival_unix_timestamp") or 0

                        dep_dt = _parse_dt(dep_str)
                        arr_dt = _parse_dt(arr_str)
                        dur = (arr_ts - dep_ts) if (arr_ts and dep_ts) else 0

                        # Resolve airline name
                        airline_name = carrier
                        airline_info = airlines_map.get(carrier)
                        if isinstance(airline_info, dict):
                            name_val = airline_info.get("name") or carrier
                            # name can be nested: {"en": {"default": "EasyJet"}}
                            if isinstance(name_val, dict):
                                for lang in ("en", "ru"):
                                    lv = name_val.get(lang)
                                    if isinstance(lv, dict):
                                        airline_name = lv.get("default") or lv.get("short") or carrier
                                        break
                                    elif isinstance(lv, str):
                                        airline_name = lv
                                        break
                            elif isinstance(name_val, str):
                                airline_name = name_val

                        segments.append(FlightSegment(
                            airline=airline_name, flight_no=f"{carrier}{flight_num}",
                            origin=dep_airport, destination=arr_airport,
                            departure=dep_dt, arrival=arr_dt,
                            duration_seconds=max(dur, 0),
                        ))

                if not segments:
                    continue

                total_dur = sum(s.duration_seconds for s in segments)
                stopovers = max(0, len(segments) - 1)
                route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=stopovers)
                oid = hashlib.md5(f"avsls_{ticket.get('id','')}{price}".encode()).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"avsls_{oid}", price=round(price, 2), currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route, inbound=None,
                    airlines=list({s.airline for s in segments}),
                    owner_airline=segments[0].airline,
                    booking_url=f"https://www.aviasales.com/search/{req.origin}{req.date_from.strftime('%d%m')}{req.destination}{req.adults or 1}",
                    is_locked=False, source="aviasales_meta", source_tier="free",
                ))
            except Exception as e:
                logger.debug("Aviasales parse ticket error: %s", e)

        return offers

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
                    id=f"rt_avsls_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"aviasales{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="EUR", offers=[], total_results=0,
        )
