"""
9 Air browser-based scraper — Playwright form fill + response interception.

9 Air (IATA: AQ) is a Chinese LCC headquartered in Guangzhou, Guangdong.
Hub: CAN (Guangzhou Baiyun). Domestic network: ~30 Chinese cities.
Website: www.9air.com (Chinese, Next.js SPA).

NOT available in GDS — must be scraped directly.
Direct API calls fail with anti-spider verification ("检验失败").
Must use browser-based approach: fill form → click search → intercept.

Strategy (browser-based, validated Mar 2026):
  1. Launch Playwright headed browser
  2. Navigate to https://www.9air.com/zh-CN (homepage with search form)
  3. Fill the search form (departure, arrival, date)
  4. Click search → page navigates to results
  5. Intercept /shop/api/shopping/b2c/searchflight response
  6. Parse flight results → FlightOffers

Search API endpoint (captured via interception):
  POST /shop/api/shopping/b2c/searchflight?language=zh_CN&currency=CNY

City dictionary (static, works directly):
  GET /frontendfile/cityDict.js

Discovered via Playwright probes, Mar 2026.
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

from boostedtravel.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from boostedtravel.connectors.browser import stealth_args

logger = logging.getLogger(__name__)

# ── Module-level browser state (cleaned up by engine.py) ───────────────────
_browser = None
_pw_instance = None

_BASE_URL = "https://www.9air.com"
_SEARCH_PAGE = f"{_BASE_URL}/zh-CN"

_VIEWPORTS = [
    {"width": 1280, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
]


async def _get_browser():
    """Get or create the shared Playwright browser."""
    global _browser, _pw_instance
    if _browser and _browser.is_connected():
        return _browser

    from playwright.async_api import async_playwright
    _pw_instance = await async_playwright().start()
    try:
        _browser = await _pw_instance.chromium.launch(
            headless=True,
            channel="chrome",
            args=[*stealth_args(), "--lang=zh-CN"],
        )
    except Exception:
        _browser = await _pw_instance.chromium.launch(
            headless=True,
            args=[*stealth_args(), "--lang=zh-CN"],
        )
    logger.info("9 Air: browser launched")
    return _browser


class NineAirConnectorClient:
    """9 Air scraper — browser form fill + API response interception."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass  # Cleaned up by engine.py via module globals

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            browser = await _get_browser()
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
                viewport=random.choice(_VIEWPORTS),
            )

            try:
                page = await context.new_page()
                offers = await self._search_with_interception(page, req, t0)

                elapsed = time.monotonic() - t0
                if offers:
                    offers.sort(key=lambda o: o.price)
                logger.info(
                    "9 Air %s→%s returned %d offers in %.1fs",
                    req.origin, req.destination, len(offers), elapsed,
                )

                search_hash = hashlib.md5(
                    f"9air{req.origin}{req.destination}{req.date_from}".encode()
                ).hexdigest()[:12]
                return FlightSearchResponse(
                    search_id=f"fs_{search_hash}",
                    origin=req.origin,
                    destination=req.destination,
                    currency=req.currency or "CNY",
                    offers=offers,
                    total_results=len(offers),
                )
            finally:
                await context.close()

        except Exception as exc:
            logger.error("9 Air error: %s", exc)
            return self._empty(req)

    async def _search_with_interception(
        self, page, req: FlightSearchRequest, t0: float,
    ) -> list[FlightOffer]:
        """Navigate to search page, fill form, intercept API response."""

        remaining = lambda: max(self.timeout - (time.monotonic() - t0), 5)

        captured_data: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                url = response.url.lower()
                if response.status == 200 and any(k in url for k in (
                    "searchflight", "calendarshopping", "availability",
                    "flightsearch", "search/flights", "shopping/b2c",
                )):
                    ct = response.headers.get("content-type", "")
                    if "json" in ct or "javascript" in ct:
                        data = await response.json()
                        if data and isinstance(data, dict):
                            # Look for successful flight data
                            if data.get("status") not in ("500",) and "data" in data:
                                captured_data["json"] = data
                                api_event.set()
            except Exception:
                pass

        page.on("response", on_response)

        # Step 1: Load homepage
        logger.info("9 Air: loading homepage for %s→%s", req.origin, req.destination)
        try:
            await page.goto(
                _SEARCH_PAGE,
                wait_until="domcontentloaded",
                timeout=int(min(remaining(), 15) * 1000),
            )
        except Exception as exc:
            logger.warning("9 Air: failed to load page: %s", exc)
            return []

        await asyncio.sleep(3.0)

        # Dismiss cookie/privacy banners
        await self._dismiss_popups(page)

        # Step 2: Select one-way
        try:
            await page.click("text=单程", timeout=3000)
            await asyncio.sleep(0.5)
        except Exception:
            pass

        # Step 3: Fill departure city
        ok = await self._fill_city(page, "departure", req.origin)
        if not ok:
            logger.warning("9 Air: could not fill departure city %s", req.origin)
            return []

        # Step 4: Fill arrival city
        ok = await self._fill_city(page, "arrival", req.destination)
        if not ok:
            logger.warning("9 Air: could not fill arrival city %s", req.destination)
            return []

        # Step 5: Select date
        await self._select_date(page, req.date_from)

        # Step 6: Click search
        await self._click_search(page)

        # Step 7: Wait for API response
        try:
            await asyncio.wait_for(api_event.wait(), timeout=remaining())
        except asyncio.TimeoutError:
            logger.warning("9 Air: timed out waiting for search results")
            # Try DOM extraction as fallback
            return await self._extract_from_dom(page, req)

        data = captured_data.get("json", {})
        if not data:
            return []

        return self._parse_response(data, req)

    async def _fill_city(self, page, field_type: str, code: str) -> bool:
        """Fill a departure or arrival city field."""

        # Strategy 1: .cityInput:visible (9air-specific class, no placeholders)
        try:
            idx = 0 if field_type == "departure" else 1
            city_inputs = page.locator(".cityInput:visible")
            if await city_inputs.count() > idx:
                inp = city_inputs.nth(idx)
                await inp.click(timeout=3000)
                await asyncio.sleep(0.5)
                await inp.fill(code)
                await asyncio.sleep(2.0)
                if await self._click_city_suggestion(page):
                    return True
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
                return True
        except Exception:
            pass

        # Strategy 2: match by placeholder text
        placeholders = {
            "departure": ["出发城市", "出发地", "出发", "departure", "from"],
            "arrival": ["到达城市", "目的地", "到达", "arrival", "to"],
        }
        for ph in placeholders.get(field_type, []):
            try:
                inp = page.locator(f"input[placeholder*='{ph}']").first
                if await inp.count() > 0:
                    await inp.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    await inp.fill(code)
                    await asyncio.sleep(2.0)
                    if await self._click_city_suggestion(page):
                        return True
                    await page.keyboard.press("Enter")
                    await asyncio.sleep(0.5)
                    return True
            except Exception:
                continue

        # Strategy 3: positional — departure=first text input, arrival=second
        try:
            idx = 0 if field_type == "departure" else 1
            search_section = page.locator(".flight-searchbar:visible, .plane-search:visible, form.flight-way:visible").first
            if await search_section.count() > 0:
                inputs = search_section.locator("input[type='text'], input:not([type])")
            else:
                inputs = page.locator("input[type='text'], input:not([type])")
            if await inputs.count() > idx:
                inp = inputs.nth(idx)
                await inp.click(timeout=3000)
                await asyncio.sleep(0.5)
                await inp.fill(code)
                await asyncio.sleep(2.0)
                if await self._click_city_suggestion(page):
                    return True
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
                return True
        except Exception:
            pass

        return False

    async def _click_city_suggestion(self, page) -> bool:
        """Click the first visible city suggestion in a dropdown."""
        for sel in (".panel-search-body:visible li",
                    ".city-item", "[class*=city-option]",
                    "[class*=city-content-list]:visible li",
                    "[class*=dropdown] li", "[class*=suggest] li",
                    ".ant-select-dropdown-menu-item",
                    "[class*=option]", "[class*=list] li",
                    "[role=option]", "[role=listbox] li"):
            try:
                item = page.locator(sel).first
                if await item.count() > 0 and await item.is_visible():
                    await item.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return True
            except Exception:
                continue
        return False

    async def _select_date(self, page, target_date) -> None:
        """Try to select the departure date."""
        try:
            # Click date input
            for sel in ("input[placeholder*='日期']", "input[placeholder*='date']",
                        "[class*=date] input", "[class*=calendar] input"):
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        await asyncio.sleep(1.0)
                        break
                except Exception:
                    continue

            # Navigate calendar to target month
            target_str = target_date.strftime("%Y-%m-%d")
            day = target_date.day

            # Try clicking "next month" buttons to get to the right month
            for _ in range(6):
                try:
                    next_btn = page.locator("[class*=next-month], [class*=arrow-right], button:has-text('>')").first
                    if await next_btn.count() > 0:
                        # Check if we're past the target month
                        calendar_text = await page.locator("[class*=calendar]").first.text_content()
                        if calendar_text and str(target_date.year) in calendar_text:
                            month_names = ["一月","二月","三月","四月","五月","六月",
                                          "七月","八月","九月","十月","十一月","十二月"]
                            target_month_zh = month_names[target_date.month - 1]
                            if target_month_zh in calendar_text or f"{target_date.month}月" in calendar_text:
                                break
                        await next_btn.click(timeout=2000)
                        await asyncio.sleep(0.3)
                except Exception:
                    break

            # Click the target day
            await page.click(
                f"td:not(.disabled):not([class*=disabled]) >> text=/^{day}$/",
                timeout=3000,
            )
            await asyncio.sleep(0.5)
        except Exception as exc:
            logger.debug("9 Air: date selection failed: %s", exc)

    async def _click_search(self, page) -> None:
        """Click the search button."""
        for sel in ("button:has-text('搜索')", "button:has-text('查询')",
                    "button:has-text('Search')", "button[type='submit']",
                    "[class*=search-btn]", "[class*=search] button"):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(timeout=3000)
                    await asyncio.sleep(1.0)
                    return
            except Exception:
                continue

    async def _dismiss_popups(self, page) -> None:
        """Dismiss cookie banners and popups."""
        for sel in ("text=接受", "text=同意", "text=Accept", "text=OK",
                    "[class*=cookie] button", "[class*=modal] [class*=close]"):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(timeout=1500)
                    await asyncio.sleep(0.3)
            except Exception:
                continue

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: extract flight info from DOM if API interception failed."""
        try:
            cards = await page.evaluate("""() => {
                const results = [];
                const cards = document.querySelectorAll(
                    '[class*=flight-card], [class*=flight-item], [class*=result-item], ' +
                    '[class*=flight-row], [class*=itinerary]'
                );
                for (const card of [...cards].slice(0, 20)) {
                    const text = card.textContent || '';
                    const priceMatch = text.match(/[¥￥](\\d+)/);
                    const timeMatch = text.match(/(\\d{2}:\\d{2})/g);
                    const flightMatch = text.match(/([A-Z0-9]{2}\\d{3,4})/);
                    if (priceMatch) {
                        results.push({
                            price: parseInt(priceMatch[1]),
                            times: timeMatch || [],
                            flight_no: flightMatch ? flightMatch[0] : '',
                            text: text.slice(0, 200),
                        });
                    }
                }
                return results;
            }""")

            if not cards:
                return []

            offers = []
            for card in cards:
                price = card.get("price", 0)
                if not price or price <= 0:
                    continue

                flight_no = card.get("flight_no", "AQ")
                times = card.get("times", [])

                dep_dt = datetime(
                    req.date_from.year, req.date_from.month, req.date_from.day,
                    int(times[0].split(":")[0]) if times else 0,
                    int(times[0].split(":")[1]) if times else 0,
                ) if times else datetime(req.date_from.year, req.date_from.month, req.date_from.day)

                arr_dt = datetime(
                    req.date_from.year, req.date_from.month, req.date_from.day,
                    int(times[1].split(":")[0]) if len(times) > 1 else 0,
                    int(times[1].split(":")[1]) if len(times) > 1 else 0,
                ) if len(times) > 1 else dep_dt

                dur = max(int((arr_dt - dep_dt).total_seconds()), 0) if arr_dt > dep_dt else 0

                fid = hashlib.md5(
                    f"aq_{req.origin}{req.destination}{flight_no}{price}".encode()
                ).hexdigest()[:12]

                segment = FlightSegment(
                    airline="AQ",
                    airline_name="9 Air",
                    flight_no=flight_no,
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=dur,
                    cabin_class="economy",
                )

                offers.append(FlightOffer(
                    id=f"aq_{fid}",
                    price=float(price),
                    currency="CNY",
                    price_formatted=f"{price} CNY",
                    outbound=FlightRoute(
                        segments=[segment],
                        total_duration_seconds=dur,
                        stopovers=0,
                    ),
                    inbound=None,
                    airlines=["9 Air"],
                    owner_airline="AQ",
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source="9air_direct",
                    source_tier="free",
                ))

            return offers
        except Exception as exc:
            logger.debug("9 Air: DOM extraction failed: %s", exc)
            return []

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the searchflight API response."""
        offers: list[FlightOffer] = []
        booking_url = self._booking_url(req)

        # 9 Air API response format varies — try common patterns
        flights = data.get("data", [])
        if isinstance(flights, dict):
            flights = flights.get("flightList", []) or flights.get("flights", []) or flights.get("results", [])

        if not isinstance(flights, list):
            return []

        for flight in flights:
            if not isinstance(flight, dict):
                continue

            price = (
                flight.get("price") or flight.get("minPrice") or
                flight.get("lowestPrice") or flight.get("salePrice") or 0
            )
            try:
                price = float(price)
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue

            flight_no = (
                flight.get("flightNo") or flight.get("flightNumber") or
                flight.get("no") or "AQ"
            )
            dep_time = (
                flight.get("depTime") or flight.get("departureTime") or
                flight.get("deptTime") or ""
            )
            arr_time = (
                flight.get("arrTime") or flight.get("arrivalTime") or
                flight.get("destTime") or ""
            )
            dep_airport = flight.get("depAirportCode") or flight.get("depCode") or req.origin
            arr_airport = flight.get("arrAirportCode") or flight.get("arrCode") or req.destination

            dep_dt = self._parse_time(dep_time, req.date_from)
            arr_dt = self._parse_time(arr_time, req.date_from)
            dur = max(int((arr_dt - dep_dt).total_seconds()), 0) if arr_dt > dep_dt else 0

            fid = hashlib.md5(
                f"aq_{req.origin}{req.destination}{flight_no}{price}".encode()
            ).hexdigest()[:12]

            segment = FlightSegment(
                airline="AQ",
                airline_name="9 Air",
                flight_no=flight_no,
                origin=dep_airport,
                destination=arr_airport,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur,
                cabin_class="economy",
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=dur,
                stopovers=0,
            )

            offers.append(FlightOffer(
                id=f"aq_{fid}",
                price=round(price, 2),
                currency="CNY",
                price_formatted=f"{price:.0f} CNY",
                outbound=route,
                inbound=None,
                airlines=["9 Air"],
                owner_airline="AQ",
                booking_url=booking_url,
                is_locked=False,
                source="9air_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _parse_time(raw: str, fallback_date) -> datetime:
        """Parse time string into datetime."""
        if not raw:
            return datetime(fallback_date.year, fallback_date.month, fallback_date.day)

        # Try full datetime formats
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M",
                    "%Y-%m-%dT%H:%M"):
            try:
                return datetime.strptime(raw[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue

        # Try time-only format "HH:MM" — combine with search date
        m = re.match(r"(\d{2}):(\d{2})", raw)
        if m:
            return datetime(
                fallback_date.year, fallback_date.month, fallback_date.day,
                int(m.group(1)), int(m.group(2)),
            )

        return datetime(fallback_date.year, fallback_date.month, fallback_date.day)

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.9air.com/zh-CN/booking/search"
            f"?depCity={req.origin}&arrCity={req.destination}&goDate={dep}"
            f"&adtCount=1&chdCount=0&infCount=0"
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"9air{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CNY",
            offers=[],
            total_results=0,
        )
