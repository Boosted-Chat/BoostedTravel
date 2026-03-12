"""
Southwest Airlines Playwright scraper -- navigates to southwest.com and searches flights.

Southwest (IATA: WN) is the largest US low-cost carrier.
Their search API is at /api/air-booking/ endpoints, protected by heavy bot detection.

Strategy:
1. Navigate to southwest.com homepage
2. Dismiss cookie consent banner ("Accept all cookies")
3. Fill search form (origin, destination, date, one-way)
4. Intercept /api/air-booking/ API responses
5. Parse results -> FlightOffers
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# -- Anti-fingerprint pools --
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-US", "en-GB", "en-CA", "en-AU"]
_TIMEZONES = [
    "America/Chicago", "America/New_York", "America/Denver",
    "America/Los_Angeles", "America/Phoenix",
]

# -- Shared browser singleton --
_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Shared headed Chromium (launched once, reused across searches)."""
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright

        _pw_instance = await async_playwright().start()
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
        logger.info("Southwest: Playwright browser launched (headed Chrome)")
        return _browser


class SouthwestConnectorClient:
    """Southwest Playwright scraper -- homepage form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            try:
                cdp = await context.new_cdp_session(page)
                await cdp.send("Network.setCacheDisabled", {"cacheDisabled": True})
            except Exception:
                pass

            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and (
                        "/api/air-booking/" in url
                        or "air-booking/page/air/booking/shopping" in url
                        or "/api/mobile-air-booking/" in url
                        or "shopping/flight" in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, (dict, list)):
                                captured_data["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("Southwest: loading booking page for %s->%s", req.origin, req.destination)
            await page.goto(
                "https://www.southwest.com/air/booking/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            # Force-remove any remaining overlays
            try:
                await page.evaluate("""() => {
                    document.querySelectorAll(
                        '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                        '[class*="onetrust"], [id*="onetrust"]'
                    ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                    document.body.style.overflow = 'auto';
                }""")
            except Exception:
                pass
            await asyncio.sleep(0.5)

            # Set one-way
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, "Depart", req.origin, is_origin=True)
            if not ok:
                logger.warning("Southwest: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, "Arrive", req.destination, is_origin=False)
            if not ok:
                logger.warning("Southwest: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("Southwest: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)

            # If still round-trip, fill the return date so the search works
            await self._fill_return_date_if_needed(page, req)
            await asyncio.sleep(0.3)

            await self._click_search(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Southwest: timed out waiting for API response")
                offers = await self._extract_from_dom(page, req)
                if offers:
                    return self._build_response(offers, req, time.monotonic() - t0)
                return self._empty(req)

            data = captured_data.get("json", {})
            if not data:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_response(data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Southwest Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # -- Cookie dismissal --

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept all cookies", "Accept All Cookies", "Accept all",
            "Accept", "I agree", "OK", "Got it",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="Cookie"], [id*="Cookie"], [class*="onetrust"], [id*="onetrust"], ' +
                    '[class*="overlay"], [id*="overlay"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # -- Flights tab --

    async def _select_flights_tab(self, page) -> None:
        for label in ["Flights", "FLIGHTS", "Flight"]:
            try:
                tab = page.get_by_role("tab", name=re.compile(rf"^{label}$", re.IGNORECASE))
                if await tab.count() > 0:
                    await tab.first.click(timeout=3000)
                    return
            except Exception:
                continue
        try:
            tab = page.locator("[data-link='flight']").first
            if await tab.count() > 0:
                await tab.click(timeout=2000)
        except Exception:
            pass

    # -- Trip type --

    async def _set_one_way(self, page) -> None:
        """Select One-way trip type via the custom combobox."""
        try:
            # Use get_by_role — CSS [aria-label=...] doesn't match this element
            combo = page.get_by_role("combobox", name=re.compile(r"trip type", re.IGNORECASE))
            if await combo.count() == 0:
                logger.debug("Southwest: trip type combobox not found")
                return

            # Click the combobox to open the dropdown
            await combo.first.click(timeout=5000)
            await asyncio.sleep(0.8)

            # Try to click "One-way" option directly
            ow = page.get_by_role("option", name=re.compile(r"one.?way", re.IGNORECASE))
            if await ow.count() > 0:
                await ow.first.click(timeout=3000)
                await asyncio.sleep(0.5)
            else:
                # Keyboard fallback: ArrowUp + Enter
                await page.keyboard.press("ArrowUp")
                await asyncio.sleep(0.2)
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)

            val = await combo.first.inner_text()
            if "one" in val.lower():
                logger.info("Southwest: selected One-way trip type")
            else:
                logger.warning("Southwest: trip type is '%s' (expected One-way)", val)
        except Exception as e:
            logger.debug("Southwest: trip type error: %s", e)

    # -- Airport fields --

    async def _fill_airport_field(self, page, label: str, iata: str, is_origin: bool) -> bool:
        """Fill airport combobox: type IATA code, pick from role=option suggestions."""
        try:
            # Southwest uses combobox with Depart*/Arrive* labels
            name_pattern = re.compile(rf"{re.escape(label)}", re.IGNORECASE)
            field = page.get_by_role("combobox", name=name_pattern)
            if await field.count() == 0:
                field = page.get_by_role("textbox", name=name_pattern)
            if await field.count() == 0:
                return False

            await field.first.click(timeout=5000)
            await asyncio.sleep(0.3)
            await field.first.fill("")
            await asyncio.sleep(0.2)
            await field.first.fill(iata)
            await asyncio.sleep(2.5)

            # Southwest shows <li role="option"> suggestions like "Los Angeles, CA - LAX"
            opt = page.get_by_role("option").filter(
                has_text=re.compile(rf"\b{re.escape(iata)}\b", re.IGNORECASE)
            ).first
            if await opt.count() > 0:
                await opt.click(timeout=3000)
                logger.info("Southwest: selected %s for %s", iata, label)
                return True

            # Fallback: click first option
            first_opt = page.get_by_role("option").first
            if await first_opt.count() > 0:
                await first_opt.click(timeout=3000)
                return True

            await page.keyboard.press("Enter")
            return True
        except Exception as e:
            logger.debug("Southwest: %s field error: %s", label, e)
            return False

    # -- Date picker --

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill departure date by typing MM/DD directly into the masked input."""
        target = req.date_from
        try:
            date_field = page.get_by_role(
                "textbox", name=re.compile(r"Depart date", re.IGNORECASE)
            )
            if await date_field.count() == 0:
                # Fallback: find by id
                date_field = page.locator("#departureDate")
            if await date_field.count() == 0:
                logger.warning("Southwest: departure date field not found")
                return False

            # Triple-click to select all existing text, then type MM/DD
            await date_field.first.click(click_count=3, timeout=3000)
            await asyncio.sleep(0.3)
            date_str = target.strftime("%m/%d")  # e.g. "04/15"
            # Type digits only — the masked input auto-inserts the slash
            await page.keyboard.type(date_str.replace("/", ""))
            await asyncio.sleep(0.5)

            # Verify value was set
            val = await date_field.first.input_value()
            logger.info("Southwest: departure date set to %s", val)

            # Click elsewhere to close any calendar popup
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
            return True
        except Exception as e:
            logger.warning("Southwest: date error: %s", e)
            return False

    async def _fill_return_date_if_needed(self, page, req: FlightSearchRequest) -> None:
        """If round-trip mode is still active, fill return date so the form is valid."""
        try:
            ret_field = page.get_by_role(
                "textbox", name=re.compile(r"Return date", re.IGNORECASE)
            )
            if await ret_field.count() == 0:
                return  # One-way mode — no return date field
            from datetime import timedelta
            ret_date = req.date_from + timedelta(days=7)
            await ret_field.first.click(click_count=3, timeout=3000)
            await asyncio.sleep(0.3)
            await page.keyboard.type(ret_date.strftime("%m%d"))
            await asyncio.sleep(0.5)
            val = await ret_field.first.input_value()
            logger.info("Southwest: return date set to %s (round-trip fallback)", val)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug("Southwest: return date fill error: %s", e)

    # -- Search button --

    async def _click_search(self, page) -> None:
        # Prefer the form's submit button — avoid nav links also matching "SEARCH"
        try:
            submit = page.locator("button[type='submit']")
            if await submit.count() > 0:
                await submit.first.click(timeout=5000)
                logger.info("Southwest: clicked submit button")
                return
        except Exception:
            pass
        # Fallback: look for a button with search-related text
        for label in ["Search flights", "Search Flights", "Find flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    logger.info("Southwest: clicked search button '%s'", label)
                    return
            except Exception:
                continue
        # Last resort
        try:
            await page.locator("#form-mixin--submit-button").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # -- DOM fallback --

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.appModel) return window.appModel;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.airProducts || d.flightShoppingPage)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    # -- Response parsing --

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        currency = "USD"
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Southwest API: flightShoppingPage > outboundPage > cards
        shopping = data.get("flightShoppingPage") or data.get("data", {}).get("searchResults") or data
        air_products = (
            shopping.get("outboundPage", {}).get("cards")
            or shopping.get("airProducts")
            or shopping.get("outboundFlights")
            or shopping.get("flights")
            or data.get("outbound")
            or []
        )
        if not isinstance(air_products, list):
            air_products = []

        # Southwest nests flights under airProducts[].details[]
        for product in air_products:
            details = product.get("details") or []
            for detail in details:
                offer = self._parse_single_flight(detail, currency, req, booking_url)
                if offer:
                    offers.append(offer)
        return offers

    def _parse_single_flight(self, detail: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        """Parse a single flight from Southwest's details[] structure."""
        # Extract cheapest fare from fareProducts.ADULT.<fareClass>.fare.totalFare.value
        fare_products = detail.get("fareProducts", {}).get("ADULT", {})
        best_price = float("inf")
        for fare_class, fare_data in fare_products.items():
            if not isinstance(fare_data, dict):
                continue
            fare = fare_data.get("fare", {})
            total_fare = fare.get("totalFare", {})
            if isinstance(total_fare, dict):
                try:
                    val = float(total_fare.get("value", 0))
                    if 0 < val < best_price:
                        best_price = val
                        currency = total_fare.get("currencyCode", currency)
                except (TypeError, ValueError):
                    pass

        if best_price == float("inf") or best_price <= 0:
            return None

        # Parse segments from productId string:
        # "WGA|VBNVN2D,V,LAX,LAS,2026-04-15T06:35-07:00,2026-04-15T07:45-07:00,WN,WN,2855,7S7"
        segments: list[FlightSegment] = []
        # Pick the cheapest fare's productId for segment info
        cheapest_product_id = ""
        for fare_data in fare_products.values():
            if isinstance(fare_data, dict) and fare_data.get("fare", {}).get("totalFare", {}).get("value"):
                try:
                    if float(fare_data["fare"]["totalFare"]["value"]) == best_price:
                        cheapest_product_id = fare_data.get("productId", "")
                        break
                except (TypeError, ValueError):
                    continue
            if not cheapest_product_id and isinstance(fare_data, dict):
                cheapest_product_id = fare_data.get("productId", "")

        if cheapest_product_id:
            segments = self._parse_product_id_segments(cheapest_product_id)

        if not segments:
            # Fallback: use route-level origin/destination
            segments = [FlightSegment(
                airline="WN", airline_name="Southwest Airlines", flight_no="",
                origin=req.origin, destination=req.destination,
                departure=datetime(2000, 1, 1), arrival=datetime(2000, 1, 1),
                cabin_class="M",
            )]

        total_dur = detail.get("totalDuration", 0)
        if isinstance(total_dur, str):
            try:
                total_dur = int(total_dur)
            except ValueError:
                total_dur = 0
        total_dur_seconds = total_dur * 60  # totalDuration is in minutes

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur_seconds, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = cheapest_product_id or f"{req.origin}{req.destination}{best_price}_{id(detail)}"
        return FlightOffer(
            id=f"wn_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=currency,
            price_formatted=f"${best_price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Southwest"],
            owner_airline="WN",
            booking_url=booking_url,
            is_locked=False,
            source="southwest_direct",
            source_tier="free",
        )

    def _parse_product_id_segments(self, product_id: str) -> list[FlightSegment]:
        """Parse segment info from Southwest productId string.
        
        Format: "WGA|code,class,ORIGIN,DEST,depTimeISO,arrTimeISO,carrier,opCarrier,flightNum,aircraft"
        Multi-segment: "WGA|...|WGA|code,class,ORIGIN2,DEST2,..."
        """
        segments: list[FlightSegment] = []
        # Remove fare class prefix (e.g. "WGA|")
        parts = product_id.split("|")
        for i in range(1, len(parts)):
            fields = parts[i].split(",")
            if len(fields) >= 9:
                origin = fields[2]
                dest = fields[3]
                dep_str = fields[4]
                arr_str = fields[5]
                flight_no = fields[8]
                segments.append(FlightSegment(
                    airline="WN",
                    airline_name="Southwest Airlines",
                    flight_no=f"WN{flight_no}",
                    origin=origin,
                    destination=dest,
                    departure=self._parse_dt(dep_str),
                    arrival=self._parse_dt(arr_str),
                    cabin_class="M",
                ))
        return segments

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Southwest %s->%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"southwest{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="USD", offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.southwest.com/air/booking/select.html"
            f"?originationAirportCode={req.origin}"
            f"&destinationAirportCode={req.destination}"
            f"&departureDate={dep}&tripType=oneway&adultPassengersCount={req.adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"southwest{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="USD", offers=[], total_results=0,
        )
