"""
Spirit Airlines connector — Patchright headed Chrome + in-page fetch.

Spirit (IATA: NK) is a US ultra-low-cost carrier operating domestic and
Caribbean/Latin America routes. Protected by Akamai Bot Manager + PerimeterX.

Strategy (Patchright headed Chrome + in-page fetch):
1.  Launch Chrome via Patchright (anti-detection Playwright fork) in HEADED
    mode using Xvfb virtual display (DISPLAY=:99 on Cloud Run).
2.  Navigate to spirit.com homepage to establish PX/Akamai cookies.
3.  Wait for _pxhd cookie + human activity for PX evaluation.
4.  Call token + search APIs via page.evaluate(fetch()) from homepage.
5.  Parse the Navitaire response into FlightOffers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
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

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Human Behavior Simulation — Defeat PerimeterX behavioral analysis
# ═══════════════════════════════════════════════════════════════════════════

async def human_delay(min_ms: int = 100, max_ms: int = 400) -> None:
    """Random delay to simulate human reaction time."""
    delay = random.randint(min_ms, max_ms) / 1000.0
    await asyncio.sleep(delay)


async def human_type(page, selector: str, text: str, clear: bool = True) -> None:
    """Type text with human-like timing (variable delays between keystrokes)."""
    elem = page.locator(selector).first
    await elem.click()
    await human_delay(50, 150)
    
    if clear:
        await elem.fill("")
        await human_delay(30, 80)
    
    for char in text:
        await elem.type(char, delay=0)
        # Variable delay: faster for common keys, slower for others
        delay = random.randint(40, 120) if char.isalnum() else random.randint(80, 200)
        await asyncio.sleep(delay / 1000.0)


def _bezier_point(t: float, p0: float, p1: float, p2: float, p3: float) -> float:
    """Calculate point on cubic bezier curve at parameter t."""
    return (1-t)**3 * p0 + 3*(1-t)**2 * t * p1 + 3*(1-t) * t**2 * p2 + t**3 * p3


async def bezier_mouse_move(page, x: int, y: int, steps: int = 25) -> None:
    """Move mouse along a natural bezier curve path (not straight line).
    
    Real humans don't move mice in perfectly straight lines — they curve.
    """
    try:
        # Get current mouse position (approximate from viewport center if unknown)
        current_x = random.randint(300, 600)
        current_y = random.randint(200, 400)
        
        # Generate random control points for natural curve
        cx1 = current_x + random.randint(-50, 100)
        cy1 = current_y + random.randint(-80, 80)
        cx2 = x + random.randint(-100, 50)
        cy2 = y + random.randint(-80, 80)
        
        # Move along bezier curve
        for i in range(steps + 1):
            t = i / steps
            # Add slight jitter to simulate human hand tremor
            jitter_x = random.randint(-2, 2) if i % 3 == 0 else 0
            jitter_y = random.randint(-2, 2) if i % 3 == 0 else 0
            
            px = int(_bezier_point(t, current_x, cx1, cx2, x)) + jitter_x
            py = int(_bezier_point(t, current_y, cy1, cy2, y)) + jitter_y
            
            await page.mouse.move(px, py)
            # Variable speed: faster in middle, slower at start/end
            delay = random.randint(8, 20) if 0.2 < t < 0.8 else random.randint(15, 35)
            await asyncio.sleep(delay / 1000.0)
    except Exception:
        # Fallback to simple move
        await page.mouse.move(x, y)


async def human_scroll(page, direction: str = "down", amount: int = None) -> None:
    """Scroll with human-like behavior (variable speed, pauses)."""
    if amount is None:
        amount = random.randint(100, 300)
    
    if direction == "down":
        delta = amount
    else:
        delta = -amount
    
    # Scroll in chunks with variable timing (like real mouse wheel)
    chunks = random.randint(3, 6)
    chunk_size = delta // chunks
    
    for _ in range(chunks):
        await page.mouse.wheel(0, chunk_size)
        await asyncio.sleep(random.randint(30, 80) / 1000.0)
    
    # Pause after scrolling (human looks at content)
    await asyncio.sleep(random.randint(200, 500) / 1000.0)


async def random_viewport_activity(page, duration_ms: int = 2000) -> None:
    """Simulate a human looking around the page — random movements, pauses, scrolls.
    
    PerimeterX analyzes mouse behavior patterns. Real users:
    - Don't go straight to form fields
    - Move mouse around while reading
    - Have irregular timing
    """
    end_time = time.monotonic() + duration_ms / 1000.0
    
    actions = ["move", "move", "move", "pause", "scroll"]  # Weighted toward movement
    
    while time.monotonic() < end_time:
        action = random.choice(actions)
        
        if action == "move":
            # Random position in viewport
            x = random.randint(100, 1100)
            y = random.randint(100, 600)
            await bezier_mouse_move(page, x, y, steps=random.randint(10, 20))
        elif action == "pause":
            await asyncio.sleep(random.randint(300, 800) / 1000.0)
        elif action == "scroll":
            await human_scroll(page, random.choice(["down", "up"]), random.randint(50, 150))
        
        await asyncio.sleep(random.randint(100, 300) / 1000.0)


async def human_click(page, selector: str, move_first: bool = True) -> None:
    """Click element with human-like behavior — move to it, pause, then click."""
    try:
        elem = page.locator(selector).first
        box = await elem.bounding_box()
        if box and move_first:
            # Don't click exact center — add offset
            x = box["x"] + box["width"] / 2 + random.randint(-5, 5)
            y = box["y"] + box["height"] / 2 + random.randint(-3, 3)
            await bezier_mouse_move(page, int(x), int(y))
            await human_delay(50, 150)
        
        await elem.click()
        await human_delay(100, 300)
    except Exception:
        # Fallback to direct click
        await page.locator(selector).first.click(force=True)


# ═══════════════════════════════════════════════════════════════════════════

_SUB_KEY = "3b6a6994753b4efc86376552e52b8432"
_TOKEN_URL = "/api/prod-token/api/v1/token"
_SEARCH_URL = "/api/prod-availability/api/availability/v3/search"

# Patchright handles anti-detection at the binary level — no manual
# inject_stealth_js, no CDP subprocess, no port management needed.


async def _launch_browser():
    """Launch a fresh Patchright browser in headed mode (Xvfb on Cloud Run).

    Patchright is an anti-detection fork of Playwright that patches
    automation indicators at the protocol level. Combined with Xvfb
    headed mode, it produces a browser indistinguishable from real Chrome.

    Returns (pw, browser, context, page).
    """
    from patchright.async_api import async_playwright

    # Determine proxy from env (main.py sets/clears LETSFG_PROXY per attempt).
    proxy = None
    letsfg_proxy = os.environ.get("LETSFG_PROXY", "").strip()
    if letsfg_proxy:
        import socket as _sock
        # Check if auth-handling relay is running on 8899
        try:
            _s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
            _s.connect(("127.0.0.1", 8899))
            _s.close()
            proxy = {"server": "http://127.0.0.1:8899"}
            logger.info("NK: using proxy relay on port 8899")
        except OSError:
            # Parse LETSFG_PROXY for patchright format
            from urllib.parse import urlparse
            p = urlparse(letsfg_proxy)
            proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
            if p.username:
                proxy["username"] = p.username
                proxy["password"] = p.password or ""
            logger.info("NK: using direct proxy %s:%s", p.hostname, p.port)
    else:
        logger.info("NK: no proxy, direct connection")

    # Set timezone to US Eastern (matches US residential proxy geo).
    os.environ["TZ"] = "America/New_York"
    try:
        time.tzset()
    except AttributeError:
        pass

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=False,  # HEADED mode — uses Xvfb (DISPLAY=:99) on Cloud Run
        args=[
            "--disable-blink-features=AutomationControlled",
            "--lang=en-US",
            "--window-size=1366,768",
        ],
        proxy=proxy,
    )
    context = await browser.new_context(
        viewport={"width": 1366, "height": 768},
        locale="en-US",
        timezone_id="America/New_York",
        color_scheme="light",
    )
    page = await context.new_page()
    return pw, browser, context, page


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
        """Search via Patchright headed Chrome + in-page fetch.

        Patchright handles anti-detection at the binary level. Combined with
        headed mode (Xvfb), this produces a browser indistinguishable from
        real Chrome. No manual inject_stealth_js needed.
        """
        t0 = time.monotonic()
        pw = browser = context = page = None

        try:
            pw, browser, context, page = await _launch_browser()
            logger.info("NK: browser launched (headed + patchright)")

            dt = _to_datetime(req.date_from)
            adults = req.adults or 1
            children = req.children or 0
            infants = req.infants or 0

            # ── Phase 1: Load homepage for PX/Akamai cookies ──
            logger.info("NK: loading homepage %s->%s", req.origin, req.destination)
            try:
                await page.goto("https://www.spirit.com/", wait_until="domcontentloaded", timeout=25000)
            except Exception as e:
                logger.debug("NK: homepage nav: %s", e)

            # Wait for PX Human Detection (_pxhd) cookie with human activity.
            # PX needs behavioral signals (mouse, scroll) during evaluation.
            _real_title = False
            _has_pxhd = False
            for _wait_i in range(15):
                await asyncio.sleep(1)
                # Human-like activity during each wait iteration
                try:
                    await page.mouse.move(
                        random.randint(200, 900), random.randint(100, 500))
                    if _wait_i % 3 == 1:
                        await page.mouse.wheel(0, random.randint(50, 200))
                except Exception:
                    pass
                try:
                    _state = await page.evaluate("""() => ({
                        pxhd: document.cookie.includes('_pxhd'),
                        title: document.title,
                        url: location.href
                    })""")
                    _has_pxhd = _state.get("pxhd", False)
                    _cur_title = _state.get("title", "")
                    _real_title = "spirit" in _cur_title.lower() and "denied" not in _cur_title.lower()
                    if _wait_i % 4 == 3:
                        logger.info("NK: wait %ds: title=%r pxhd=%s",
                                    _wait_i + 1, _cur_title[:60], _has_pxhd)
                    if _real_title and _has_pxhd and (_wait_i + 1) >= 5:
                        logger.info("NK: ready after %ds (pxhd=%s, title=%r)",
                                    _wait_i + 1, _has_pxhd, _cur_title[:60])
                        break
                except Exception:
                    pass
            else:
                logger.info("NK: wait done 15s (pxhd=%s, real_title=%s)", _has_pxhd, _real_title)

            page_title = await page.title()
            logger.info("NK: homepage title=%r pxhd=%s", page_title, _has_pxhd)

            # Extra human activity burst before API calls
            try:
                await random_viewport_activity(page, duration_ms=2000)
            except Exception:
                await human_delay(1000, 2000)

            # Log cookie state
            try:
                px_state = await page.evaluate("""() => {
                    const c = document.cookie;
                    return {
                        pxhd: c.includes('_pxhd'),
                        pxvid: c.includes('_pxvid'),
                        abck: c.includes('_abck'),
                        bm: c.includes('bm_s'),
                        n_cookies: c.split(';').length,
                        preview: c.substring(0, 400)
                    };
                }""")
                logger.info("NK: cookies: %s", px_state)
            except Exception:
                pass

            # ── Phase 2: Get Navitaire session token ──
            logger.info("NK: calling token API")
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

            token_status = token_result.get("status", 0)
            token_body = token_result.get("body", "")
            logger.info("NK: token API: status=%d len=%d body=%s", token_status, len(token_body), token_body[:400])

            if token_status not in (200, 201):
                logger.warning("NK: token returned %d: %s", token_status, token_body[:200])
                return self._empty(req)

            token_data = json.loads(token_body)
            bearer_token = (
                token_data.get("data", {}).get("token", "")
                or token_data.get("token", "")
            )
            if not bearer_token:
                logger.error("NK: no token in response: %s", token_body[:200])
                return self._empty(req)
            logger.info("NK: got session token (%d chars)", len(bearer_token))

            # ── Phase 3: Search flights ──
            date_str = dt.strftime("%Y-%m-%d")
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

            logger.info("NK: calling search API")
            search_args = json.dumps({
                "url": _SEARCH_URL,
                "token": bearer_token,
                "subKey": _SUB_KEY,
                "body": json.dumps(search_payload),
            })
            search_result = await page.evaluate(
                """async (argsJson) => {
                    const args = JSON.parse(argsJson);
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
                search_args,
            )

            if search_result.get("error"):
                logger.error("NK: search fetch error: %s", search_result["error"])
                return self._empty(req)

            search_status = search_result.get("status", 0)
            search_body = search_result.get("body", "")
            logger.info("NK: search API: status=%d len=%d", search_status, len(search_body))

            if search_status == 403 or search_status == 429:
                logger.warning("NK: blocked (%d): %s", search_status, search_body[:300])
                return self._empty(req)

            if search_status != 200:
                logger.warning("NK: search returned %d: %s", search_status, search_body[:300])
                return self._empty(req)

            data = json.loads(search_body)
            offers = self._parse_response(data, req)
            offers.sort(key=lambda o: o.price)

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
            logger.error("NK error: %s", e)
            return self._empty(req)
        finally:
            # Clean up browser resources (each search gets a fresh browser)
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass
            try:
                if pw:
                    await pw.stop()
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

        for trip in trips:
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
                    offers.append(offer)
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

        # Build segments from journey.segments
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []
        for seg in (segments_raw if isinstance(segments_raw, list) else []):
            des = seg.get("designator", {})
            ident = seg.get("identifier", {})
            _nk_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
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
            _nk_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
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
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = _to_datetime(req.date_from).strftime("%Y-%m-%d")
        return (
            f"https://www.spirit.com/book/flights?from={req.origin}"
            f"&to={req.destination}&date={dep}&pax={req.adults or 1}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"spirit{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
