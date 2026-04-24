"""
Wego connector — patchright CDP browser + RSC parsing.

Wego is a major metasearch engine popular in Middle East, South Asia,
and SE Asia. Aggregates results from 700+ airlines and OTAs.

Strategy (rewritten Jul 2026 — RSC parsing model):
1. Each search launches fresh patchright browser with residential proxy.
2. Navigate to Wego search results URL.
3. Handle Cloudflare Turnstile challenges automatically.
4. Parse React Server Components (RSC) streaming data from page HTML.
5. Extract flight segments and itineraries from RSC chunks.
6. Close browser + cleanup.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import shutil
import sys
import tempfile
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
from .browser import (
    inject_stealth_js,
    get_default_proxy,
    acquire_browser_slot,
    release_browser_slot,
    block_all_heavy_resources,
    auto_block_if_proxied,
)

logger = logging.getLogger(__name__)
_CONNECTOR_USER_DATA_DIR_ENV = "LETSFG_CONNECTOR_USER_DATA_DIR"

# ── IATA → Wego city slug mapping ──
# Wego URLs use format: /flights/{city}-{IATA}/{city}-{IATA}/{date}
_WEGO_SLUGS: dict[str, str] = {
    "LON": "london", "LHR": "london", "LGW": "london", "STN": "london",
    "LTN": "london", "LCY": "london", "SEN": "london",
    "BCN": "barcelona", "MAD": "madrid", "AGP": "malaga", "ALC": "alicante",
    "PMI": "palma-de-mallorca", "IBZ": "ibiza", "VLC": "valencia",
    "NYC": "new-york", "JFK": "new-york", "EWR": "new-york", "LGA": "new-york",
    "PAR": "paris", "CDG": "paris", "ORY": "paris",
    "BER": "berlin", "TXL": "berlin", "FRA": "frankfurt", "MUC": "munich",
    "ROM": "rome", "FCO": "rome", "MIL": "milan", "MXP": "milan", "LIN": "milan", "VCE": "venice",
    "IST": "istanbul", "SAW": "istanbul", "AYT": "antalya",
    "DXB": "dubai", "AUH": "abu-dhabi", "DOH": "doha",
    "SIN": "singapore", "BKK": "bangkok", "KUL": "kuala-lumpur",
    "DEL": "delhi", "BOM": "mumbai", "BLR": "bangalore",
    "HKG": "hong-kong", "TYO": "tokyo", "NRT": "tokyo", "HND": "tokyo",
    "SEL": "seoul", "ICN": "seoul", "GMP": "seoul",
    "BJS": "beijing", "PEK": "beijing", "PKX": "beijing",
    "SHA": "shanghai", "PVG": "shanghai",
    "SYD": "sydney", "MEL": "melbourne", "AKL": "auckland",
    "LIS": "lisbon", "OPO": "porto", "ATH": "athens",
    "AMS": "amsterdam", "BRU": "brussels", "DUB": "dublin",
    "ZRH": "zurich", "GVA": "geneva", "VIE": "vienna",
    "OSL": "oslo", "STO": "stockholm", "ARN": "stockholm", "BMA": "stockholm", "NYO": "stockholm",
    "CPH": "copenhagen", "HEL": "helsinki",
    "WAW": "warsaw", "PRG": "prague", "BUD": "budapest",
    "MOW": "moscow", "SVO": "moscow", "DME": "moscow", "VKO": "moscow",
    "CAI": "cairo", "JNB": "johannesburg", "NBO": "nairobi",
    "CHI": "chicago", "LAX": "los-angeles", "SFO": "san-francisco", "ORD": "chicago", "MDW": "chicago",
    "MIA": "miami", "DFW": "dallas", "ATL": "atlanta",
    "WAS": "washington", "IAD": "washington", "DCA": "washington",
    "YYZ": "toronto", "YVR": "vancouver", "MEX": "mexico-city",
    "SAO": "sao-paulo", "GRU": "sao-paulo", "CGH": "sao-paulo",
    "BUE": "buenos-aires", "EZE": "buenos-aires", "AEP": "buenos-aires",
    "BOG": "bogota", "SCL": "santiago", "LIM": "lima",
}

# Airport IATA → City IATA for multi-airport cities.
# Wego URLs must use the city code, not individual airport codes.
_AIRPORT_TO_CITY: dict[str, str] = {
    "LHR": "LON", "LGW": "LON", "STN": "LON", "LTN": "LON", "LCY": "LON", "SEN": "LON",
    "JFK": "NYC", "EWR": "NYC", "LGA": "NYC",
    "CDG": "PAR", "ORY": "PAR",
    "NRT": "TYO", "HND": "TYO",
    "FCO": "ROM", "CIA": "ROM",
    "MXP": "MIL", "LIN": "MIL",
    "TXL": "BER", "SXF": "BER",
    "SAW": "IST",
    "PVG": "SHA",
    "PKX": "BJS", "PEK": "BJS",
    "ICN": "SEL", "GMP": "SEL",
    "ARN": "STO", "BMA": "STO", "NYO": "STO",
    "SVO": "MOW", "DME": "MOW", "VKO": "MOW",
    "EZE": "BUE", "AEP": "BUE",
    "GRU": "SAO", "CGH": "SAO", "VCP": "SAO",
    "ORD": "CHI", "MDW": "CHI",
    "IAD": "WAS", "DCA": "WAS", "BWI": "WAS",
}


# ── Bezier curve helpers for human-like movements ──
def _bezier_curve(p0: tuple, p1: tuple, p2: tuple, p3: tuple, steps: int = 30) -> list:
    """Generate points along cubic bezier curve."""
    pts = []
    for i in range(steps + 1):
        t = i / steps
        s = 1 - t
        x = s**3 * p0[0] + 3*s**2*t * p1[0] + 3*s*t**2 * p2[0] + t**3 * p3[0]
        y = s**3 * p0[1] + 3*s**2*t * p1[1] + 3*s*t**2 * p2[1] + t**3 * p3[1]
        pts.append((x, y))
    return pts


async def _human_mouse_move(page, start_x: float, start_y: float, end_x: float, end_y: float):
    """Move mouse from start to end using bezier curve with micro-variations."""
    dx = end_x - start_x
    dy = end_y - start_y
    ctrl1 = (start_x + dx * random.uniform(0.2, 0.4), start_y + dy * random.uniform(-0.3, 0.3))
    ctrl2 = (start_x + dx * random.uniform(0.6, 0.8), end_y + random.uniform(-15, 15))
    pts = _bezier_curve((start_x, start_y), ctrl1, ctrl2, (end_x, end_y), steps=random.randint(25, 40))
    for px, py in pts:
        px += random.uniform(-1.5, 1.5)
        py += random.uniform(-1.5, 1.5)
        await page.mouse.move(px, py)
        await asyncio.sleep(random.uniform(0.004, 0.012))


async def _cf_token_present(page) -> bool:
    """Check if Cloudflare Turnstile has been solved (token set)."""
    try:
        token = await page.evaluate("""() => {
            const el = document.querySelector('[name="cf-turnstile-response"]');
            return el ? el.value : '';
        }""")
        return bool(token)
    except Exception:
        return False


async def _simulate_human_idle(page):
    """Simulate idle human behaviour — small mouse jitter + scroll."""
    try:
        vw = 1366
        vh = 800
        x = random.randint(200, vw - 200)
        y = random.randint(200, vh - 200)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.3, 0.8))
        # Small scroll
        await page.evaluate(f"window.scrollBy(0, {random.randint(-60, 60)})")
    except Exception:
        pass


async def _solve_cf_turnstile(page) -> bool:
    """Actively attempt to solve Cloudflare Turnstile challenge.

    Strategies in order of reliability:
    0. Check if already solved (patchright auto‑handled).
    1. Bring page to front — patchright needs focus for auto‑solve.
    2. Call turnstile JS API to force execution / re‑render.
    3. Click the checkbox inside the CF iframe (standard Turnstile).
    4. Click the iframe element itself (managed / invisible mode).
    5. Click any turnstile widget container div.
    6. Click any visible verify / confirm button.
    """
    # ── Quick check: already solved? ──
    if await _cf_token_present(page):
        logger.info("WEGO: Turnstile already solved (token present)")
        return True

    # ── Ensure page focus (critical for patchright auto-solve) ──
    try:
        await page.bring_to_front()
    except Exception:
        pass

    # ── Simulate a small mouse movement (triggers CF behaviour check) ──
    await _simulate_human_idle(page)

    # ── Strategy 1: Call Turnstile JS API ──
    try:
        api_result = await page.evaluate("""() => {
            // turnstile global (CF injects this)
            if (typeof turnstile !== 'undefined') {
                try { turnstile.execute(); return 'executed'; } catch(_) {}
                try { turnstile.reset();   return 'reset';    } catch(_) {}
            }
            if (window.turnstile) {
                try { window.turnstile.execute(); return 'win_executed'; } catch(_) {}
                try { window.turnstile.reset();   return 'win_reset';    } catch(_) {}
            }
            return null;
        }""")
        if api_result:
            logger.info("WEGO: Turnstile JS API called: %s", api_result)
            await asyncio.sleep(3)
            if await _cf_token_present(page):
                return True
    except Exception as e:
        logger.debug("WEGO: Turnstile JS API attempt: %s", e)

    solved = False

    # ── Strategy 2: iframe checkbox ──
    try:
        cf_frame = page.frame_locator('iframe[src*="challenges.cloudflare.com"]')
        checkbox = cf_frame.locator('input[type="checkbox"]')
        if await checkbox.count() > 0:
            logger.info("WEGO: clicking Turnstile checkbox in iframe")
            try:
                await checkbox.hover(timeout=2000)
                await asyncio.sleep(random.uniform(0.25, 0.6))
            except Exception:
                pass
            box = await checkbox.bounding_box()
            if box:
                cx = box["x"] + box["width"] / 2
                cy = box["y"] + box["height"] / 2
                await _human_mouse_move(page, random.randint(50, 200), random.randint(50, 200), cx, cy)
                await asyncio.sleep(random.uniform(0.15, 0.35))
                await page.mouse.down()
                await asyncio.sleep(random.uniform(0.08, 0.18))
                await page.mouse.up()
                await asyncio.sleep(3)
                if await _cf_token_present(page):
                    return True
            try:
                await checkbox.click(force=True, timeout=2000)
                await asyncio.sleep(2)
                if await _cf_token_present(page):
                    return True
            except Exception:
                pass
                solved = True
    except Exception as e:
        logger.debug("WEGO: Turnstile iframe checkbox attempt: %s", e)

    # ── Strategy 3: click the iframe element itself ──
    if not solved:
        try:
            iframe_el = page.locator('iframe[src*="challenges.cloudflare.com"]')
            if await iframe_el.count() > 0:
                logger.info("WEGO: clicking Turnstile iframe element")
                try:
                    await iframe_el.first.hover(timeout=2000)
                    await asyncio.sleep(random.uniform(0.25, 0.5))
                except Exception:
                    pass
                box = await iframe_el.bounding_box()
                if box and box["width"] > 0 and box["height"] > 0:
                    cx = box["x"] + box["width"] / 2
                    cy = box["y"] + box["height"] / 2
                    await _human_mouse_move(page, random.randint(50, 200), random.randint(50, 200), cx, cy)
                    await asyncio.sleep(random.uniform(0.1, 0.3))
                    await page.mouse.down()
                    await asyncio.sleep(random.uniform(0.08, 0.18))
                    await page.mouse.up()
                    await asyncio.sleep(3)
                    if await _cf_token_present(page):
                        return True
                try:
                    await iframe_el.first.click(force=True, timeout=2000)
                    await asyncio.sleep(2)
                    if await _cf_token_present(page):
                        return True
                except Exception:
                    pass
                    solved = True
        except Exception as e:
            logger.debug("WEGO: Turnstile iframe click attempt: %s", e)

    # ── Strategy 4: click turnstile widget container ──
    if not solved:
        try:
            for selector in [
                'div.cf-turnstile',
                '[class*="cf-turnstile"]',
                '[id*="turnstile"]',
                '[class*="turnstile"]',
            ]:
                widget = page.locator(selector)
                if await widget.count() > 0:
                    logger.info("WEGO: clicking Turnstile widget (%s)", selector)
                    box = await widget.first.bounding_box()
                    if box and box["width"] > 0:
                        cx = box["x"] + box["width"] / 2
                        cy = box["y"] + box["height"] / 2
                        await _human_mouse_move(page, random.randint(50, 200), random.randint(50, 200), cx, cy)
                        await asyncio.sleep(random.uniform(0.1, 0.3))
                        await page.mouse.down()
                        await asyncio.sleep(random.uniform(0.08, 0.18))
                        await page.mouse.up()
                        await asyncio.sleep(3)
                        if await _cf_token_present(page):
                            return True
                        try:
                            await widget.first.click(force=True, timeout=2000)
                            await asyncio.sleep(2)
                            if await _cf_token_present(page):
                                return True
                        except Exception:
                            pass
                        solved = True
                        break
        except Exception as e:
            logger.debug("WEGO: Turnstile widget click attempt: %s", e)

    # ── Strategy 5: click any verify/confirm button ──
    if not solved:
        try:
            for btn_text in ["Verify", "verify", "Confirm", "I am human", "I'm not a robot"]:
                btn = page.locator(f'button:has-text("{btn_text}"), a:has-text("{btn_text}")')
                if await btn.count() > 0:
                    logger.info("WEGO: clicking '%s' button", btn_text)
                    await btn.first.click()
                    await asyncio.sleep(3)
                    if await _cf_token_present(page):
                        return True
                    solved = True
                    break
        except Exception as e:
            logger.debug("WEGO: verify button click attempt: %s", e)

    return solved or await _cf_token_present(page)


def _wego_slug(iata: str) -> str:
    """Convert IATA code to Wego URL slug: bare city IATA code.

    Wego URLs use format: /flights/LON/BCN/2026-06-15
    Map airport → city first (LHR → LON, JFK → NYC).
    """
    code = iata.upper()
    city_code = _AIRPORT_TO_CITY.get(code, code)
    return city_code


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
            return datetime.strptime(s[:len(fmt) + 2], fmt)
        except (ValueError, IndexError):
            continue
    return datetime(2000, 1, 1)


def _build_search_urls(
    req: FlightSearchRequest,
    dt: datetime,
    adults: int,
    children: int,
    infants: int,
    cabin: str,
) -> list[str]:
    """Build candidate Wego search URLs.

    Wego has used multiple result URL families over time. Try the
    search-oriented routes first and fall back to the current SEO-like path.
    """
    date_str = dt.strftime("%Y-%m-%d")
    origin_code = _wego_slug(req.origin)
    dest_code = _wego_slug(req.destination)
    query = (
        f"adults={adults}&children={children}&infants={infants}"
        f"&cabin={cabin}&sort=price"
    )

    candidates = [
        f"https://www.wego.com/flights/search/{origin_code}/{dest_code}/{date_str}?{query}",
        f"https://www.wego.com/en/flights/searches/{origin_code}-{dest_code}/{date_str}?{query}",
        f"https://www.wego.com/flights/{origin_code}/{dest_code}/{date_str}?{query}",
    ]

    unique_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            unique_candidates.append(candidate)
    return unique_candidates


def _looks_like_seo_page(
    page_url: str,
    title: str,
    canonical_url: Optional[str],
) -> bool:
    text = " ".join(part for part in (page_url, canonical_url or "", title) if part).lower()
    return (
        "cheapest-flights-from" in text
        or title.lower().startswith("cheap flights from")
    )


def _is_wego_result_response(url: str) -> bool:
    url = url.lower()
    return (
        ("srv.wego.com" in url and any(k in url for k in ("search", "result", "results", "fare", "fares")))
        or ("wego.com/api" in url and "flight" in url)
        or ("wego.com" in url and "graphql" in url)
    )


class WegoConnectorClient:
    """Wego — ME/Asia metasearch, CDP Chrome + API interception."""

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
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        dt = _to_datetime(req.date_from)

        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0

        cabin_map = {"M": "economy", "W": "premium_economy",
                     "C": "business", "F": "first"}
        cabin = cabin_map.get(req.cabin_class, "economy") if req.cabin_class else "economy"

        search_urls = _build_search_urls(req, dt, adults, children, infants, cabin)

        for attempt, search_url in enumerate(search_urls):
            try:
                prewarm_url = search_urls[1] if attempt == 2 and len(search_urls) > 1 else None
                offers = await self._do_search(search_url, req, dt, attempt, prewarm_url=prewarm_url)
                if offers:
                    offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "WEGO %s→%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    h = hashlib.md5(
                        f"wego{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{h}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=offers[0].currency if offers else "USD",
                        offers=offers,
                        total_results=len(offers),
                    )
                if offers == []:
                    logger.info(
                        "WEGO: candidate %d returned no offers, trying next route",
                        attempt + 1,
                    )
            except Exception as e:
                logger.warning("WEGO attempt %d failed: %s", attempt, e)

        return self._empty(req)

    async def _do_search(
        self,
        search_url: str,
        req: FlightSearchRequest,
        dt: datetime,
        attempt: int = 0,
        prewarm_url: Optional[str] = None,
    ) -> list[FlightOffer] | None:
        snapshot_user_data_dir = ""
        if os.environ.get("LETSFG_WEGO_USE_SNAPSHOT_PROFILE", "1").strip().lower() in {"1", "true", "yes", "on"}:
            snapshot_user_data_dir = os.environ.get(_CONNECTOR_USER_DATA_DIR_ENV, "").strip()

        if snapshot_user_data_dir:
            try:
                offers = await self._do_search_once(
                    search_url,
                    req,
                    dt,
                    attempt=attempt,
                    prewarm_url=prewarm_url,
                    snapshot_user_data_dir=snapshot_user_data_dir,
                    launch_mode="snapshot-profile",
                )
                if offers:
                    return offers
                logger.warning(
                    "WEGO: snapshot-backed profile returned no offers for %s, falling back to fresh launch",
                    search_url,
                )
            except Exception as exc:
                logger.warning(
                    "WEGO: snapshot-backed profile failed for %s: %s; falling back to fresh launch",
                    search_url,
                    exc,
                )

        return await self._do_search_once(
            search_url,
            req,
            dt,
            attempt=attempt,
            prewarm_url=prewarm_url,
            snapshot_user_data_dir=None,
            launch_mode="fresh",
        )

    async def _do_search_once(
        self,
        search_url: str,
        req: FlightSearchRequest,
        dt: datetime,
        attempt: int = 0,
        prewarm_url: Optional[str] = None,
        snapshot_user_data_dir: Optional[str] = None,
        launch_mode: str = "fresh",
    ) -> list[FlightOffer] | None:
        """Search using patchright with DOM text parsing."""
        from patchright.async_api import async_playwright

        browser = None
        context = None
        pw_instance = None

        try:
            await acquire_browser_slot()
            
            pw_instance = await async_playwright().start()
            
            # Build proxy config with session ID for different IP on retry
            launch_kwargs = {
                "headless": False,
                "args": ["--window-position=-2400,-2400", "--window-size=1366,800"],
            }
            proxy = get_default_proxy()
            if proxy:
                launch_kwargs["proxy"] = proxy

            if snapshot_user_data_dir:
                persistent_kwargs = {
                    "user_data_dir": snapshot_user_data_dir,
                    "headless": False,
                    "viewport": {"width": 1366, "height": 800},
                    "locale": "en-US",
                    "args": ["--window-position=-2400,-2400", "--window-size=1366,800"],
                }
                if proxy:
                    persistent_kwargs["proxy"] = proxy
                logger.info(
                    "WEGO: launch_mode=%s user_data_dir=%s",
                    launch_mode,
                    snapshot_user_data_dir,
                )
                context = await pw_instance.chromium.launch_persistent_context(**persistent_kwargs)
            else:
                logger.info("WEGO: launch_mode=%s", launch_mode)
                browser = await pw_instance.chromium.launch(**launch_kwargs)
                context = await browser.new_context(
                    viewport={"width": 1366, "height": 800},
                    locale="en-US",
                )
            page = context.pages[0] if context.pages else await context.new_page()
            await auto_block_if_proxied(page)

            # ── API response interception ──
            # Capture Wego's flight data API responses as they stream in.
            intercepted_data: list[tuple[str, Any]] = []
            intercepted_other_urls: list[str] = []
            xhr_request_urls: list[str] = []
            xhr_response_urls: list[str] = []

            def _record_limited(items: list[str], value: str, limit: int = 12) -> None:
                if len(items) < limit and value not in items:
                    items.append(value)

            async def _on_request(request):
                try:
                    resource_type = request.resource_type
                    url = request.url
                    if resource_type in {"fetch", "xhr"} and "wego" in url.lower():
                        _record_limited(xhr_request_urls, f"{request.method} {url}")
                except Exception:
                    pass

            async def _on_response(response):
                url = response.url
                url_lower = url.lower()
                try:
                    resource_type = response.request.resource_type
                    ct = response.headers.get("content-type", "")
                    if resource_type in {"fetch", "xhr"} and "wego" in url_lower:
                        ct_short = ct.split(";", 1)[0]
                        _record_limited(
                            xhr_response_urls,
                            f"{response.status} {resource_type} {ct_short} {url}",
                        )
                    if "json" in ct:
                        if _is_wego_result_response(url_lower):
                            body = await response.json()
                            intercepted_data.append((url, body))
                            logger.debug("WEGO: intercepted API response from %s", url)
                        elif "wego" in url_lower and len(intercepted_other_urls) < 8:
                            intercepted_other_urls.append(url)
                except Exception:
                    pass

            page.on("request", _on_request)
            page.on("response", _on_response)

            if prewarm_url:
                logger.info("WEGO: prewarming session via %s", prewarm_url)
                try:
                    await page.goto(prewarm_url, wait_until="domcontentloaded", timeout=25000)
                    await asyncio.sleep(4)
                except Exception as prewarm_err:
                    logger.info("WEGO: prewarm navigation failed: %s", prewarm_err)
                intercepted_data.clear()
                intercepted_other_urls.clear()
                xhr_request_urls.clear()
                xhr_response_urls.clear()

            logger.info("WEGO: navigating to %s", search_url)
            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=25000)
            except Exception as nav_err:
                err_str = str(nav_err)
                if "ERR_TUNNEL" in err_str:
                    logger.warning("WEGO: proxy tunnel failed, retrying with different session")
                    raise
                if "ERR_HTTP_RESPONSE_CODE_FAILURE" in err_str:
                    logger.warning("WEGO: goto returned HTTP failure, continuing with loaded page")
                    try:
                        title = await page.title()
                        body_text = (await page.text_content("body") or "").strip().replace("\n", " ")
                        logger.info(
                            "WEGO: failed goto page url=%s title=%s body=%s",
                            page.url,
                            title[:160],
                            body_text[:320],
                        )
                    except Exception:
                        pass
                else:
                    raise

            # ── Handle Cloudflare Turnstile ──
            # Patchright auto-solves most challenges but needs page focus.
            # We poll via title AND token, simulate human idle behaviour,
            # and actively solve on escalation.
            try:
                await page.bring_to_front()
            except Exception:
                pass

            cf_passed = False
            for cf_wait in range(30):  # up to 30 s
                # Check 1: title no longer shows challenge page
                try:
                    title = (await page.title()).lower()
                    body_text = (await page.text_content("body") or "").lower()
                except Exception:
                    await asyncio.sleep(1)
                    continue
                is_cf = any(t in title for t in (
                    "just a moment", "checking", "challenge",
                    "attention required", "please wait",
                ))
                if not is_cf:
                    is_cf = any(marker in body_text for marker in (
                        "performing security verification",
                        "website uses a security service",
                        "verify you are not a bot",
                    ))
                if not is_cf:
                    if cf_wait > 0:
                        logger.info("WEGO: Cloudflare passed after ~%ds", cf_wait)
                    cf_passed = True
                    break

                # Check 2: token present (patchright may have solved silently)
                if await _cf_token_present(page):
                    logger.info("WEGO: Cloudflare token detected at ~%ds", cf_wait)
                    cf_passed = True
                    break

                if cf_wait == 0:
                    logger.info("WEGO: Cloudflare challenge detected, waiting...")

                # Simulate human idle every 2s (mouse jitter, tiny scroll)
                if cf_wait % 2 == 0:
                    await _simulate_human_idle(page)

                # Every 4s try to actively solve Turnstile
                if cf_wait > 2 and cf_wait % 4 == 0:
                    await _solve_cf_turnstile(page)

                await asyncio.sleep(1)

            if not cf_passed:
                # Reload fallback — different proxy session on next attempt
                logger.warning("WEGO: Cloudflare still blocking after 30s, reloading...")
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=15000)
                except Exception:
                    pass
                try:
                    await page.bring_to_front()
                except Exception:
                    pass
                for cf_retry in range(12):
                    try:
                        title = (await page.title()).lower()
                        body_text = (await page.text_content("body") or "").lower()
                    except Exception:
                        await asyncio.sleep(1)
                        continue
                    still_blocked = any(t in title for t in (
                        "just a moment", "challenge", "checking",
                        "attention required",
                    ))
                    if not still_blocked:
                        still_blocked = any(marker in body_text for marker in (
                            "performing security verification",
                            "website uses a security service",
                            "verify you are not a bot",
                        ))
                    if not still_blocked:
                        logger.info("WEGO: Cloudflare passed after reload + %ds", cf_retry)
                        cf_passed = True
                        break
                    if await _cf_token_present(page):
                        cf_passed = True
                        break
                    if cf_retry % 2 == 0:
                        await _simulate_human_idle(page)
                    if cf_retry % 4 == 0:
                        await _solve_cf_turnstile(page)
                    await asyncio.sleep(1)

            if not cf_passed:
                logger.warning("WEGO: Cloudflare challenge NOT resolved — results may be empty")

            # Wait for page to fully render and for any late result calls to arrive.
            logger.info("WEGO: waiting for flight results")
            await asyncio.sleep(3)

            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            await asyncio.sleep(2)

            for _ in range(2):
                await page.evaluate("window.scrollBy(0, 400)")
                await asyncio.sleep(0.8)

            deadline = time.monotonic() + 24
            last_count = len(intercepted_data)
            stable_ticks = 0
            while time.monotonic() < deadline:
                await asyncio.sleep(3)
                current_count = len(intercepted_data)
                if current_count > last_count:
                    last_count = current_count
                    stable_ticks = 0
                else:
                    stable_ticks += 1
                    if stable_ticks >= 3 and intercepted_data:
                        break

            landing_title = ""
            landing_canonical = None
            looks_like_seo = False
            try:
                landing_title = await page.title()
                canonical = page.locator('link[rel="canonical"]')
                if await canonical.count() > 0:
                    landing_canonical = await canonical.first.get_attribute("href")
                looks_like_seo = _looks_like_seo_page(page.url, landing_title, landing_canonical)
                logger.info(
                    "WEGO: candidate %d landed url=%s title=%s canonical=%s seo=%s",
                    attempt + 1,
                    page.url,
                    landing_title[:160],
                    (landing_canonical or "")[:200],
                    looks_like_seo,
                )
            except Exception:
                pass

            # Get page HTML and try multiple extraction methods
            html = await page.content()

            # Method 0: Use intercepted API data (most reliable)
            if intercepted_data:
                logger.info("WEGO: %d API responses intercepted, parsing...", len(intercepted_data))
                seen: set[str] = set()
                api_offers: list[FlightOffer] = []
                for idx, item in enumerate(intercepted_data, start=1):
                    try:
                        source_url, data = item
                        parsed_offers: list[FlightOffer] = []
                        if isinstance(data, dict):
                            parsed_offers.extend(self._parse_response(data, req, dt, seen))
                        elif isinstance(data, list):
                            # Some Wego endpoints return bare arrays
                            for item in data:
                                if isinstance(item, dict):
                                    parsed_offers.extend(self._parse_response(item, req, dt, seen))
                        if parsed_offers:
                            api_offers.extend(parsed_offers)
                        elif idx <= 8:
                            logger.info(
                                "WEGO: intercepted response %d yielded no offers url=%s shape=%s",
                                idx,
                                source_url[:180],
                                self._summarize_response_shape(data),
                            )
                    except Exception as e:
                        logger.debug("WEGO: intercepted data parse error: %s", e)
                if api_offers:
                    return api_offers

            in_page_api_data = await self._fetch_search_api_in_page(page, req, dt)
            if in_page_api_data:
                logger.info("WEGO: %d in-page API responses fetched, parsing...", len(in_page_api_data))
                seen: set[str] = set()
                api_offers: list[FlightOffer] = []
                for idx, item in enumerate(in_page_api_data, start=1):
                    try:
                        source_url, data = item
                        parsed_offers: list[FlightOffer] = []
                        if isinstance(data, dict):
                            parsed_offers.extend(self._parse_response(data, req, dt, seen))
                        elif isinstance(data, list):
                            for row in data:
                                if isinstance(row, dict):
                                    parsed_offers.extend(self._parse_response(row, req, dt, seen))
                        if parsed_offers:
                            api_offers.extend(parsed_offers)
                        else:
                            logger.info(
                                "WEGO: in-page API response %d yielded no offers url=%s shape=%s",
                                idx,
                                source_url[:180],
                                self._summarize_response_shape(data),
                            )
                            if idx <= 3:
                                self._log_wego_result_sample(source_url, data)
                    except Exception as e:
                        logger.debug("WEGO: in-page API parse error: %s", e)
                if api_offers:
                    return api_offers
            elif intercepted_other_urls:
                logger.info(
                    "WEGO: no result payloads intercepted; saw other JSON urls=%s",
                    " | ".join(intercepted_other_urls[:6]),
                )
            if xhr_request_urls:
                logger.info(
                    "WEGO: xhr/fetch requests seen=%s",
                    " | ".join(xhr_request_urls[:10]),
                )
            if xhr_response_urls:
                logger.info(
                    "WEGO: xhr/fetch responses seen=%s",
                    " | ".join(xhr_response_urls[:10]),
                )

            # Method 1: Try DOM text parsing (most reliable for Wego)
            offers = await self._parse_dom_text(page, req, dt)
            if offers:
                return offers
            
            # Method 2: Fall back to RSC parsing
            offers = self._parse_rsc_data(html, req, dt)
            if offers:
                return offers
            
            # Method 3: Legacy DOM extraction
            offers = await self._extract_from_dom(page, req, dt)
            if not offers and looks_like_seo:
                logger.info(
                    "WEGO: candidate %d produced no offers on SEO-like page, trying next route",
                    attempt + 1,
                )
                return None
            return offers

        finally:
            try:
                if context:
                    await context.close()
            except Exception:
                pass
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass
            try:
                if pw_instance:
                    await pw_instance.stop()
            except Exception:
                pass
            release_browser_slot()

    # ------------------------------------------------------------------
    # DOM Text Parsing (primary method for Wego)
    # ------------------------------------------------------------------

    async def _parse_dom_text(
        self, page, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Parse flight schedules from visible page text.
        
        Wego renders flight schedules as visible text with this pattern:
        - Times: LHR 06:00, ZRH 12:30 (departure/arrival pairs)
        - Duration: 12h 5m
        - Stops: 1 Stop, ZRH · 3h 50m
        - Airlines: Swiss, Finnair, Emirates
        - Fare Guide prices: US$ 334, US$ 340
        """
        try:
            # Get visible text from main element
            text = await page.evaluate("""() => {
                const main = document.querySelector('main');
                return main ? main.innerText : document.body.innerText;
            }""")
            
            if not text or len(text) < 500:
                logger.debug("WEGO: insufficient page text")
                return []
            
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            
            # Extract fare guide prices (US$ XXX)
            prices_found = []
            for line in lines:
                m = re.search(r'US\$\s*([\d,]+)', line)
                if m:
                    try:
                        price = float(m.group(1).replace(',', ''))
                        if 50 < price < 10000:
                            prices_found.append(price)
                    except ValueError:
                        pass
            
            # Remove obvious low prices (usually partial fares)
            if prices_found:
                min_price = min(prices_found)
                prices_found = [p for p in prices_found if p >= min_price]
            
            logger.info("WEGO: found %d fare guide prices", len(prices_found))
            
            # Extract flight schedules
            # Pattern: airport HH:MM lines followed by duration and airline
            schedules = []
            i = 0
            while i < len(lines) - 5:
                line = lines[i]
                
                # Look for departure pattern: LHR 06:00
                dep_match = re.match(r'^([A-Z]{3})\s+(\d{1,2}:\d{2})$', line)
                if dep_match:
                    dep_airport = dep_match.group(1)
                    dep_time = dep_match.group(2)
                    
                    # Look ahead for more info (arrival, duration, airline)
                    schedule = {
                        'dep_airport': dep_airport,
                        'dep_time': dep_time,
                        'arr_airport': req.destination,
                        'arr_time': None,
                        'duration': None,
                        'stops': 0,
                        'airlines': [],
                    }
                    
                    # Parse next few lines
                    for j in range(1, min(12, len(lines) - i)):
                        next_line = lines[i + j]
                        
                        # Arrival airport/time: DXB 21:05
                        arr_match = re.match(r'^([A-Z]{3})\s+(\d{1,2}:\d{2})$', next_line)
                        if arr_match:
                            schedule['arr_airport'] = arr_match.group(1)
                            schedule['arr_time'] = arr_match.group(2)
                            continue
                        
                        # Duration: 12h 5m or 12h 05m
                        dur_match = re.match(r'^(\d+)h\s*(\d+)m$', next_line)
                        if dur_match:
                            hours = int(dur_match.group(1))
                            mins = int(dur_match.group(2))
                            schedule['duration'] = hours * 60 + mins
                            continue
                        
                        # Stops: 1 Stop, 2 Stops, Direct
                        if 'Stop' in next_line:
                            stop_match = re.search(r'(\d+)\s*Stop', next_line)
                            if stop_match:
                                schedule['stops'] = int(stop_match.group(1))
                            continue
                        if next_line.lower() == 'direct':
                            schedule['stops'] = 0
                            continue
                        
                        # Airline names (common carriers)
                        airline_names = [
                            'Emirates', 'Etihad', 'Qatar', 'Swiss', 'Lufthansa',
                            'British Airways', 'KLM', 'Air France', 'Finnair',
                            'Turkish', 'Ryanair', 'EasyJet', 'Wizz', 'Vueling',
                            'Norwegian', 'SAS', 'Aeroflot', 'Saudia', 'Gulf Air',
                            'Kuwait Airways', 'Oman Air', 'Flydubai', 'Air India',
                            'Singapore Airlines', 'Cathay', 'Thai', 'Malaysia',
                        ]
                        for airline in airline_names:
                            if airline.lower() in next_line.lower():
                                if airline not in schedule['airlines']:
                                    schedule['airlines'].append(airline)
                    
                    if schedule['duration'] or schedule['arr_time']:
                        schedules.append(schedule)
                
                i += 1
            
            logger.info("WEGO: found %d flight schedules", len(schedules))
            
            # Build offers from schedules + prices
            offers: list[FlightOffer] = []
            seen: set[str] = set()
            
            # Get unique price list
            unique_prices = sorted(set(prices_found))[:20]
            
            for i, schedule in enumerate(schedules[:len(unique_prices)]):
                # Assign price from fare guide (lower prices to shorter durations)
                price_idx = min(i, len(unique_prices) - 1)
                if price_idx >= len(unique_prices):
                    continue
                price_f = unique_prices[price_idx]
                
                airline = schedule['airlines'][0] if schedule['airlines'] else 'Unknown'
                
                # Deduplicate
                dedup = f"{schedule['dep_airport']}_{schedule['arr_airport']}_{schedule['dep_time']}_{price_f}"
                if dedup in seen:
                    continue
                seen.add(dedup)
                
                # Parse times
                dep_time_str = f"{dt:%Y-%m-%d} {schedule['dep_time']}"
                try:
                    departure = datetime.strptime(dep_time_str, "%Y-%m-%d %H:%M")
                except ValueError:
                    departure = dt
                
                duration_s = (schedule['duration'] or 0) * 60
                arrival = departure
                if duration_s > 0:
                    from datetime import timedelta
                    arrival = departure + timedelta(seconds=duration_s)
                
                _wego_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                seg = FlightSegment(
                    airline=airline,
                    airline_name=airline,
                    flight_no="",
                    origin=schedule['dep_airport'],
                    destination=schedule['arr_airport'],
                    departure=departure,
                    arrival=arrival,
                    duration_seconds=duration_s,
                    cabin_class=_wego_cabin,
                )
                
                route = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=duration_s,
                    stopovers=schedule['stops'],
                )
                
                fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"wego_{fid}",
                    price=price_f,
                    currency="USD",
                    price_formatted=f"${price_f:.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=schedule['airlines'] or [airline],
                    owner_airline=airline,
                    booking_url=(
                        f"https://www.wego.com/flights/{req.origin}/{req.destination}"
                        f"/{dt:%Y-%m-%d}?adults={req.adults or 1}"
                    ),
                    is_locked=False,
                    source="wego_meta",
                    source_tier="free",
                ))
            
            return offers
            
        except Exception as e:
            logger.debug("WEGO: DOM text parsing failed: %s", e)
            return []

    # ------------------------------------------------------------------
    # RSC Parsing (React Server Components)
    # ------------------------------------------------------------------

    def _parse_rsc_data(
        self, html: str, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Parse React Server Components streaming data from page HTML.
        
        Wego uses Next.js App Router with RSC streaming. Flight data is
        embedded in <script>self.__next_f.push([1,"..."])</script> tags.
        """
        # Extract all RSC chunks
        pattern = r'<script>self\.__next_f\.push\(\[1,"([^"]+)"\]\)</script>'
        chunks = re.findall(pattern, html, re.DOTALL)
        
        if not chunks:
            logger.warning("WEGO: no RSC chunks found in HTML")
            snippet = re.sub(r"\s+", " ", html[:500])
            logger.info("WEGO: HTML prefix=%s", snippet)
            return []
        
        # Concatenate and unescape
        all_data = ''
        for chunk in chunks:
            unescaped = chunk.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
            all_data += unescaped
        
        logger.debug("WEGO: parsed %d RSC chunks, %d bytes total", len(chunks), len(all_data))
        
        offers: list[FlightOffer] = []
        seen: set[str] = set()
        
        # Extract flight segments
        # Pattern: "arrivalTime":"HH:MM","departureTime":"HH:MM","flightNumber":"NNN",...
        segment_pattern = (
            r'"arrivalTime":"(\d{2}:\d{2})",'
            r'"departureTime":"(\d{2}:\d{2})",'
            r'"flightNumber":"([^"]+)",'
            r'"designatorCode":"([^"]+)",'
            r'"airlineCode":"([A-Z0-9]{2})"'
        )
        segments_found = []
        for m in re.finditer(segment_pattern, all_data):
            segments_found.append({
                'arrival': m.group(1),
                'departure': m.group(2),
                'flight_number': m.group(3),
                'designator': m.group(4),
                'airline': m.group(5),
            })
        
        # Extract itinerary data with duration and airports
        itin_pattern = (
            r'"durationTimeMinutes":(\d+).*?'
            r'"departureAirportCode":"([A-Z]{3})".*?'
            r'"arrivalAirportCode":"([A-Z]{3})".*?'
            r'"stopoversCount":(\d+)'
        )
        itineraries = []
        for m in re.finditer(itin_pattern, all_data[:500000]):  # Limit for performance
            itineraries.append({
                'duration_min': int(m.group(1)),
                'origin': m.group(2),
                'destination': m.group(3),
                'stops': int(m.group(4)),
            })
        
        # Extract price data
        # Format: "priceUsd":333.70,"price":333.7,"outboundAirlineCodes":["VF","W9"]
        price_pattern = (
            r'"priceUsd":(\d+(?:\.\d+)?),?"price":(\d+(?:\.\d+)?).*?'
            r'"outboundAirlineCodes":\["([^"]+)"'
        )
        prices = []
        for m in re.finditer(price_pattern, all_data):
            prices.append({
                'price_usd': float(m.group(1)),
                'price': float(m.group(2)),
                'airline': m.group(3),
            })
        
        logger.info("WEGO: found %d segments, %d itineraries, %d prices",
                   len(segments_found), len(itineraries), len(prices))
        
        # Build offers from price data
        for i, price_data in enumerate(prices):
            price_f = round(price_data['price_usd'], 2)
            if price_f <= 0:
                continue
            
            airline = price_data.get('airline', 'Unknown')
            
            # Deduplicate
            dedup = f"{req.origin}_{req.destination}_{dt:%Y%m%d}_{price_f}_{airline}"
            if dedup in seen:
                continue
            seen.add(dedup)
            
            # Find matching itinerary
            matching_itin = None
            for itin in itineraries:
                # Match by similar airports (city codes may differ from airport codes)
                if (itin['origin'][:2] == req.origin[:2] or itin['origin'] in req.origin
                    or req.origin in itin['origin']):
                    if (itin['destination'][:2] == req.destination[:2] 
                        or itin['destination'] in req.destination
                        or req.destination in itin['destination']):
                        matching_itin = itin
                        break
            
            # Build segment
            duration_s = 0
            stops = 0
            if matching_itin:
                duration_s = matching_itin['duration_min'] * 60
                stops = matching_itin['stops']
            
            _wego_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
            seg = FlightSegment(
                airline=airline,
                airline_name=airline,
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                departure=dt,
                arrival=dt,
                duration_seconds=duration_s,
                cabin_class=_wego_cabin,
            )
            
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=duration_s,
                stopovers=stops,
            )
            
            fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"wego_{fid}",
                price=price_f,
                currency="USD",
                price_formatted=f"${price_f:.0f}",
                outbound=route,
                inbound=None,
                airlines=[airline],
                owner_airline=airline,
                booking_url=(
                    f"https://www.wego.com/flights/{req.origin}/{req.destination}"
                    f"/{dt:%Y-%m-%d}?adults={req.adults or 1}"
                ),
                is_locked=False,
                source="wego_meta",
                source_tier="free",
            ))
        
        # If no price data found, try to extract from segments + visible prices
        if not offers and segments_found:
            logger.info("WEGO: no price objects, building from segments")
            # Look for dollar amounts in the data
            dollar_pattern = r'\$(\d+(?:,\d{3})*(?:\.\d{2})?)'
            dollar_amounts = re.findall(dollar_pattern, all_data)
            dollar_values = [float(d.replace(',', '')) for d in dollar_amounts]
            dollar_values = sorted(set(v for v in dollar_values if 50 < v < 5000))
            
            # Dedupe segments
            seen_segs = set()
            unique_segs = []
            for s in segments_found:
                key = (s['designator'], s['departure'])
                if key not in seen_segs:
                    seen_segs.add(key)
                    unique_segs.append(s)
            
            # Match segments to prices heuristically
            for i, seg_data in enumerate(unique_segs[:len(dollar_values)]):
                if i >= len(dollar_values):
                    break
                    
                price_f = dollar_values[i] if i < len(dollar_values) else 0
                if price_f <= 0:
                    continue
                
                airline = seg_data['airline']
                dedup = f"{req.origin}_{req.destination}_{seg_data['designator']}_{price_f}"
                if dedup in seen:
                    continue
                seen.add(dedup)
                
                # Parse times
                dep_time = datetime.strptime(f"{dt:%Y-%m-%d} {seg_data['departure']}", "%Y-%m-%d %H:%M")
                arr_time = datetime.strptime(f"{dt:%Y-%m-%d} {seg_data['arrival']}", "%Y-%m-%d %H:%M")
                if arr_time < dep_time:
                    arr_time = arr_time.replace(day=arr_time.day + 1)
                
                duration_s = int((arr_time - dep_time).total_seconds())
                
                _wego_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                seg = FlightSegment(
                    airline=airline,
                    airline_name=airline,
                    flight_no=seg_data['designator'],
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_time,
                    arrival=arr_time,
                    duration_seconds=duration_s,
                    cabin_class=_wego_cabin,
                )
                
                route = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=duration_s,
                    stopovers=0,
                )
                
                fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"wego_{fid}",
                    price=price_f,
                    currency="USD",
                    price_formatted=f"${price_f:.0f}",
                    outbound=route,
                    inbound=None,
                    airlines=[airline],
                    owner_airline=airline,
                    booking_url=(
                        f"https://www.wego.com/flights/{req.origin}/{req.destination}"
                        f"/{dt:%Y-%m-%d}?adults={req.adults or 1}"
                    ),
                    is_locked=False,
                    source="wego_meta",
                    source_tier="free",
                ))
        
        return offers

    async def _fetch_search_api_in_page(
        self, page, req: FlightSearchRequest, dt: datetime,
    ) -> list[tuple[str, Any]]:
        """Try Wego search APIs from the page context using the live browser session."""
        cabin_map = {
            "M": "economy",
            "W": "premium_economy",
            "C": "business",
            "F": "first",
        }
        cabin = cabin_map.get(req.cabin_class, "economy") if req.cabin_class else "economy"
        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0
        date_str = dt.strftime("%Y-%m-%d")
        departure_city_code = _AIRPORT_TO_CITY.get(req.origin.upper(), req.origin.upper())
        arrival_city_code = _AIRPORT_TO_CITY.get(req.destination.upper(), req.destination.upper())

        attempts = [
            (
                "https://srv.wego.com/v3/metasearch/flights/searches",
                {
                    "search": {
                        "cabin": cabin,
                        "adultsCount": adults,
                        "childrenCount": children,
                        "infantsCount": infants,
                        "locale": "en",
                        "siteCode": "US",
                        "currencyCode": "USD",
                        "deviceType": "DESKTOP",
                        "appType": "WEB_APP",
                        "legs": [
                            {
                                "departureAirportCode": req.origin,
                                "arrivalAirportCode": req.destination,
                                "departureCityCode": departure_city_code,
                                "arrivalCityCode": arrival_city_code,
                                "outboundDate": date_str,
                            }
                        ],
                    },
                },
            ),
        ]

        results: list[tuple[str, Any]] = []
        for url, payload in attempts:
            try:
                response = await page.evaluate(
                    """async ({ url, payload }) => {
                        try {
                            const resp = await fetch(url, {
                                method: 'POST',
                                credentials: 'include',
                                headers: {
                                    'accept': 'application/json',
                                    'content-type': 'application/json'
                                },
                                body: JSON.stringify(payload),
                            });
                            const text = await resp.text();
                            return {
                                ok: resp.ok,
                                status: resp.status,
                                contentType: resp.headers.get('content-type') || '',
                                text,
                            };
                        } catch (error) {
                            return { error: String(error) };
                        }
                    }""",
                    {"url": url, "payload": payload},
                )
            except Exception as e:
                logger.info("WEGO: in-page API fetch failed url=%s err=%s", url, e)
                continue

            if not isinstance(response, dict):
                continue

            if response.get("error"):
                logger.info("WEGO: in-page API error url=%s err=%s", url, response.get("error"))
                continue

            status = response.get("status")
            content_type = str(response.get("contentType") or "")
            body_text = str(response.get("text") or "")
            logger.info(
                "WEGO: in-page API status=%s ct=%s url=%s",
                status,
                content_type.split(";", 1)[0],
                url,
            )
            if status != 200 or "json" not in content_type.lower() or not body_text:
                if body_text:
                    logger.info("WEGO: in-page API body preview url=%s body=%s", url, body_text[:180])
                continue

            try:
                data = json.loads(body_text)
            except Exception as e:
                logger.info("WEGO: in-page API JSON decode failed url=%s err=%s", url, e)
                continue

            results.append((url, data))

            search_id = None
            if isinstance(data, dict):
                search_id = (
                    data.get("searchId")
                    or data.get("search_id")
                    or data.get("id")
                )
                if not search_id:
                    search_node = data.get("search")
                    if isinstance(search_node, dict):
                        search_id = (
                            search_node.get("id")
                            or search_node.get("searchId")
                            or search_node.get("search_id")
                        )
                    logger.info(
                        "WEGO: in-page search object preview=%s",
                        json.dumps(search_node)[:220],
                    )

            has_results = False
            if isinstance(data, dict):
                has_results = bool(data.get("fares") or data.get("trips") or data.get("results"))
            if not search_id or has_results:
                continue

            results_urls = [
                f"https://srv.wego.com/v3/metasearch/flights/searches/{search_id}",
                f"https://srv.wego.com/v3/metasearch/flights/searches/{search_id}/results",
                f"https://srv.wego.com/v3/metasearch/flights/searches/{search_id}/fares",
                f"https://srv.wego.com/v3/metasearch/flights/results/{search_id}",
            ]
            for poll_index in range(4):
                await asyncio.sleep(2)
                found_results = False
                for results_url in results_urls:
                    try:
                        poll_response = await page.evaluate(
                            """async (url) => {
                                try {
                                    const resp = await fetch(url, {
                                        method: 'GET',
                                        credentials: 'include',
                                        headers: { 'accept': 'application/json' },
                                    });
                                    const text = await resp.text();
                                    return {
                                        status: resp.status,
                                        contentType: resp.headers.get('content-type') || '',
                                        text,
                                    };
                                } catch (error) {
                                    return { error: String(error) };
                                }
                            }""",
                            results_url,
                        )
                    except Exception as e:
                        logger.info("WEGO: in-page results poll failed url=%s err=%s", results_url, e)
                        continue

                    if not isinstance(poll_response, dict):
                        continue
                    if poll_response.get("error"):
                        logger.info(
                            "WEGO: in-page results poll error url=%s err=%s",
                            results_url,
                            poll_response.get("error"),
                        )
                        continue

                    poll_status = poll_response.get("status")
                    poll_ct = str(poll_response.get("contentType") or "")
                    poll_text = str(poll_response.get("text") or "")
                    logger.info(
                        "WEGO: in-page results poll %d status=%s ct=%s url=%s",
                        poll_index + 1,
                        poll_status,
                        poll_ct.split(";", 1)[0],
                        results_url,
                    )
                    if poll_status != 200 or "json" not in poll_ct.lower() or not poll_text:
                        continue
                    try:
                        poll_data = json.loads(poll_text)
                    except Exception as e:
                        logger.info("WEGO: in-page results poll JSON decode failed url=%s err=%s", results_url, e)
                        continue

                    results.append((results_url, poll_data))
                    if isinstance(poll_data, dict) and (poll_data.get("fares") or poll_data.get("trips") or poll_data.get("results")):
                        found_results = True
                        break
                if found_results:
                    break

        return results

    # ------------------------------------------------------------------
    # Legacy Response parsing (kept for backwards compatibility)
    # ------------------------------------------------------------------

    def _parse_response(
        self, data: dict, req: FlightSearchRequest, dt: datetime, seen: set,
    ) -> list[FlightOffer]:
        """Parse Wego metasearch API response data."""
        offers: list[FlightOffer] = []
        graph_fares: list[dict] = []

        fare_keys = {
            "amount", "avgPrice", "avgPriceUsd", "price", "priceUsd",
            "totalAmount",
        }
        route_keys = {
            "airlineCode", "airlineCodes", "arrivalAirportCode",
            "arrivalCityCode", "departureAirportCode", "departureCityCode",
            "destination", "legs", "origin", "outboundAirlineCodes",
            "segments", "slices",
        }
        fares: list[dict] = []

        gql = data.get("data") if isinstance(data.get("data"), dict) else {}
        flight_search = gql.get("flightSearch") if isinstance(gql.get("flightSearch"), dict) else {}
        flights = gql.get("flights") if isinstance(gql.get("flights"), dict) else {}

        def collect_graph_fares(root: Any) -> None:
            if not isinstance(root, dict):
                return

            fare_nodes = root.get("fares")
            trip_nodes = root.get("trips")
            leg_nodes = root.get("legs")
            if not (
                isinstance(fare_nodes, list)
                and isinstance(trip_nodes, list)
                and isinstance(leg_nodes, list)
            ):
                return

            trip_map = {
                str(item.get("id")): item
                for item in trip_nodes
                if isinstance(item, dict) and item.get("id")
            }
            leg_map = {
                str(item.get("id")): item
                for item in leg_nodes
                if isinstance(item, dict) and item.get("id")
            }

            for fare in fare_nodes:
                if not isinstance(fare, dict):
                    continue

                raw_trip_ids = fare.get("tripIds") or fare.get("tripId") or fare.get("flightIds") or fare.get("flightId")
                if isinstance(raw_trip_ids, list):
                    trip_ids = [str(value) for value in raw_trip_ids if value]
                elif raw_trip_ids:
                    trip_ids = [str(raw_trip_ids)]
                else:
                    trip_ids = []

                resolved_legs: list[dict] = []
                for trip_id in trip_ids:
                    trip = trip_map.get(trip_id)
                    if not isinstance(trip, dict):
                        continue
                    for leg_id in trip.get("legIds") or []:
                        leg = leg_map.get(str(leg_id))
                        if isinstance(leg, dict):
                            resolved_legs.append(leg)

                if not resolved_legs:
                    continue

                graph_fare = dict(fare)
                graph_fare["legs"] = resolved_legs
                graph_fare["tripIds"] = trip_ids
                graph_fares.append(graph_fare)

        def collect_fares(container: Any) -> None:
            if not container:
                return
            if isinstance(container, list):
                for item in container:
                    collect_fares(item)
                return
            if not isinstance(container, dict):
                return

            keys = set(container.keys())
            if (fare_keys & keys) and (route_keys & keys):
                fares.append(container)
                return

            for value in container.values():
                if isinstance(value, (list, dict)):
                    collect_fares(value)

        seen_graph_roots: set[int] = set()
        for root in (
            data,
            data.get("search"),
            gql,
            gql.get("search"),
            gql.get("results"),
            flight_search,
            flight_search.get("search"),
            flights,
            flights.get("search"),
        ):
            if isinstance(root, dict) and id(root) not in seen_graph_roots:
                seen_graph_roots.add(id(root))
                collect_graph_fares(root)

        for container in (
            data.get("fares"),
            data.get("trips"),
            data.get("results"),
            data.get("itineraries"),
            data.get("flights"),
            data.get("faresByAirlines"),
            data.get("faresByMonth"),
            data.get("faresByDay"),
        ):
            collect_fares(container)
        for container in (
            gql.get("faresByAirlines"),
            gql.get("faresByMonth"),
            gql.get("faresByDay"),
            flight_search.get("fares"),
            flight_search.get("results"),
            flight_search.get("faresByAirlines"),
            flight_search.get("faresByMonth"),
            flight_search.get("faresByDay"),
            flights.get("results"),
            flights.get("faresByAirlines"),
            flights.get("faresByMonth"),
            flights.get("faresByDay"),
        ):
            collect_fares(container)

        # Lookup tables (Wego often sends airlines/airports separately)
        airlines_map = {}
        for payload in (
            data.get("airlines"),
            gql.get("airlines"),
            flight_search.get("airlines"),
            flights.get("airlines"),
        ):
            if isinstance(payload, list):
                for a in payload:
                    if isinstance(a, dict):
                        code = a.get("code") or a.get("iata") or ""
                        airlines_map[code] = a.get("name") or code
            elif isinstance(payload, dict):
                for code, name in payload.items():
                    if isinstance(name, dict):
                        airlines_map[str(code)] = name.get("name") or str(code)
                    else:
                        airlines_map[str(code)] = str(name)

        date_key = dt.strftime("%Y-%m-%d")
        for fare in graph_fares + fares:
            try:
                fare_date = fare.get("departureDate") or fare.get("date") or fare.get("departure_date")
                if fare_date and str(fare_date)[:10] != date_key:
                    continue
                offer = self._parse_fare(fare, req, dt, seen, airlines_map)
                if offer:
                    offers.append(offer)
            except Exception as e:
                logger.debug("WEGO: parse fare error: %s", e)

        return offers

    @staticmethod
    def _summarize_response_shape(payload: Any) -> str:
        """Return a compact payload shape summary for live parser diagnostics."""
        def describe(value: Any) -> str:
            if isinstance(value, list):
                if value and isinstance(value[0], dict):
                    first_keys = ",".join(list(value[0].keys())[:6])
                    return f"list[{len(value)}]:{first_keys}"
                return f"list[{len(value)}]"
            if isinstance(value, dict):
                return f"dict:{','.join(list(value.keys())[:8])}"
            return type(value).__name__

        if isinstance(payload, list):
            return describe(payload)
        if not isinstance(payload, dict):
            return type(payload).__name__

        parts = [f"top={','.join(list(payload.keys())[:10])}"]
        for key in (
            "data", "results", "fares", "trips", "itineraries", "flights",
            "faresByAirlines", "faresByMonth", "faresByDay",
        ):
            if key in payload:
                parts.append(f"{key}={describe(payload.get(key))}")

        data_node = payload.get("data")
        if isinstance(data_node, dict):
            for key in ("flightSearch", "flights", "search", "results"):
                if key in data_node:
                    parts.append(f"data.{key}={describe(data_node.get(key))}")

        return "; ".join(parts)

    @staticmethod
    def _log_wego_result_sample(source_url: str, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        def _preview(value: Any) -> str:
            try:
                return json.dumps(value)[:400]
            except Exception:
                return str(value)[:400]

        fares = payload.get("fares")
        trips = payload.get("trips")
        legs = payload.get("legs")
        if isinstance(payload.get("data"), dict):
            fares = fares or payload["data"].get("fares")
            trips = trips or payload["data"].get("trips")
            legs = legs or payload["data"].get("legs")

        if isinstance(fares, list) and fares:
            logger.info("WEGO: sample fare url=%s data=%s", source_url[:180], _preview(fares[0]))
        if isinstance(trips, list) and trips:
            logger.info("WEGO: sample trip url=%s data=%s", source_url[:180], _preview(trips[0]))
        if isinstance(legs, list) and legs:
            logger.info("WEGO: sample leg url=%s data=%s", source_url[:180], _preview(legs[0]))

    def _parse_fare(
        self, fare: dict, req: FlightSearchRequest, dt: datetime,
        seen: set, airlines_map: dict,
    ) -> FlightOffer | None:
        # Price
        price_obj = fare.get("price") or fare
        if isinstance(price_obj, dict):
            price = (
                price_obj.get("amountWithFraction") or price_obj.get("totalAmountWithFraction")
                or price_obj.get("amount") or price_obj.get("totalAmount")
                or price_obj.get("priceUsd") or price_obj.get("avgPriceUsd")
                or price_obj.get("price") or price_obj.get("avgPrice") or 0
            )
            currency = (
                price_obj.get("currencyCode") or price_obj.get("currency")
                or fare.get("currency") or "USD"
            )
        else:
            try:
                price = float(price_obj)
            except (ValueError, TypeError):
                return None
            currency = "USD"

        try:
            price_f = round(float(price), 2)
        except (ValueError, TypeError):
            return None
        if price_f <= 0:
            return None

        # Segments / legs
        legs = fare.get("legs") or fare.get("segments") or fare.get("slices") or []
        if not legs:
            # Flat fare structure
            legs = [fare]

        segments: list[FlightSegment] = []
        for leg in legs:
            seg_items = leg.get("segments") or [leg]
            for sd in seg_items:
                airline_code = (
                    sd.get("airlineCode") or sd.get("operatingCarrier")
                    or sd.get("marketingCarrier") or sd.get("operatingAirlineCode")
                    or sd.get("airline") or ""
                )
                if not airline_code:
                    raw_codes = sd.get("outboundAirlineCodes") or sd.get("airlineCodes") or []
                    if isinstance(raw_codes, list) and raw_codes:
                        airline_code = str(raw_codes[0])
                    elif isinstance(raw_codes, str):
                        airline_code = raw_codes.split(",")[0].strip()
                airline_name = (
                    sd.get("airlineName") or airlines_map.get(airline_code, "")
                    or airline_code
                )
                fno = sd.get("flightNumber") or sd.get("flightNo") or sd.get("designatorCode") or ""
                if airline_code and fno and not fno.startswith(airline_code):
                    fno = f"{airline_code}{fno}"

                dep_time = (
                    sd.get("departureTime") or sd.get("departure")
                    or sd.get("departureDateTime") or ""
                )
                arr_time = (
                    sd.get("arrivalTime") or sd.get("arrival")
                    or sd.get("arrivalDateTime") or ""
                )
                dep_apt = (
                    sd.get("departureAirportCode") or sd.get("departureCode")
                    or sd.get("departureCityCode") or sd.get("origin") or req.origin
                )
                arr_apt = (
                    sd.get("arrivalAirportCode") or sd.get("arrivalCode")
                    or sd.get("arrivalCityCode") or sd.get("destination") or req.destination
                )
                dur = (
                    sd.get("durationMinutes") or sd.get("durationTimeMinutes")
                    or sd.get("flightDuration") or sd.get("tripDuration")
                    or sd.get("duration") or 0
                )
                try:
                    dur_s = int(float(dur)) * 60 if dur else 0
                except (TypeError, ValueError):
                    dur_s = 0

                _wego_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                segments.append(FlightSegment(
                    airline=airline_code or airline_name,
                    airline_name=airline_name,
                    flight_no=fno,
                    origin=dep_apt,
                    destination=arr_apt,
                    departure=_parse_dt(dep_time) if dep_time else dt,
                    arrival=_parse_dt(arr_time) if arr_time else dt,
                    duration_seconds=dur_s,
                    cabin_class=_wego_cabin,
                ))

        if not segments:
            return None

        total_dur = sum(s.duration_seconds for s in segments)
        if not total_dur and segments[0].departure != segments[-1].arrival:
            diff = (segments[-1].arrival - segments[0].departure).total_seconds()
            if 0 < diff < 86400 * 3:
                total_dur = int(diff)

        fno_key = "_".join(s.flight_no for s in segments)
        dedup = f"{req.origin}_{req.destination}_{dt:%Y%m%d}_{price_f}_{fno_key}"
        if dedup in seen:
            return None
        seen.add(dedup)

        airlines_set = list(dict.fromkeys(s.airline for s in segments if s.airline))
        names_set = list(dict.fromkeys(
            s.airline_name for s in segments if s.airline_name
        ))

        stops_override = fare.get("stopsCount") or fare.get("stopoversCount")
        try:
            stopovers = max(0, int(stops_override)) if stops_override not in (None, "") else max(0, len(segments) - 1)
        except (TypeError, ValueError):
            stopovers = max(0, len(segments) - 1)

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=stopovers,
        )

        fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"wego_{fid}",
            price=price_f,
            currency=currency,
            price_formatted=f"{price_f:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=names_set or airlines_set,
            owner_airline=airlines_set[0] if airlines_set else "",
            booking_url=(
                fare.get("handoffUrl") or fare.get("bookingUrl") or (
                    f"https://www.wego.com/flights/{req.origin}/{req.destination}"
                    f"/{dt:%Y-%m-%d}?adults={req.adults or 1}"
                )
            ),
            is_locked=False,
            source="wego_meta",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # DOM fallback
    # ------------------------------------------------------------------

    async def _extract_from_dom(
        self, page, req: FlightSearchRequest, dt: datetime,
    ) -> list[FlightOffer]:
        """Fallback: scrape visible fare cards from the Wego results page."""
        try:
            data = await page.evaluate("""() => {
                const cards = document.querySelectorAll(
                    '[class*="FareCard"], [class*="fare-card"], '
                  + '[class*="ResultCard"], [class*="result-card"], '
                  + '[data-testid*="fare"], [data-testid*="result"]'
                );
                const out = [];
                cards.forEach(c => {
                    const p = c.querySelector(
                        '[class*="price"], [class*="Price"], [data-testid*="price"]'
                    );
                    const a = c.querySelector(
                        '[class*="airline"], [class*="Airline"], [data-testid*="airline"]'
                    );
                    const d = c.querySelector(
                        '[class*="duration"], [class*="Duration"]'
                    );
                    const stops = c.querySelector(
                        '[class*="stop"], [class*="Stop"]'
                    );
                    if (p) out.push({
                        price: p.textContent.trim(),
                        airline: a ? a.textContent.trim() : '',
                        duration: d ? d.textContent.trim() : '',
                        stops: stops ? stops.textContent.trim() : '',
                    });
                });
                return out;
            }""")

            offers: list[FlightOffer] = []
            seen: set[str] = set()
            for item in data or []:
                nums = re.findall(r"[\d]+", item.get("price", "").replace(",", ""))
                if not nums:
                    continue
                try:
                    price_f = round(float(nums[-1]), 2)
                except (ValueError, IndexError):
                    continue
                if price_f <= 0:
                    continue

                airline = item.get("airline") or "Unknown"
                dedup = f"{req.origin}_{req.destination}_{price_f}_{airline}"
                if dedup in seen:
                    continue
                seen.add(dedup)

                seg = FlightSegment(
                    airline=airline, flight_no="",
                    origin=req.origin, destination=req.destination,
                    departure=dt, arrival=dt, duration_seconds=0,
                )
                route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
                fid = hashlib.md5(dedup.encode()).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=f"wego_{fid}",
                    price=price_f,
                    currency="USD",
                    price_formatted=f"{price_f:.2f} USD",
                    outbound=route,
                    inbound=None,
                    airlines=[airline],
                    owner_airline="",
                    booking_url=(
                        f"https://www.wego.com/flights/{req.origin}"
                        f"/{req.destination}/{dt:%Y-%m-%d}"
                    ),
                    is_locked=False,
                    source="wego_meta",
                    source_tier="free",
                ))
            return offers
        except Exception as e:
            logger.debug("WEGO: DOM extraction failed: %s", e)
            return []

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
                    id=f"rt_wego_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"wego{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
