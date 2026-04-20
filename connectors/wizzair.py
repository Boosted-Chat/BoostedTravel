"""
Wizzair scraper — curl_cffi direct API (no browser, no KPSDK).

Strategy:
1. Fetch API version from /buildnumber (plain text, unprotected).
2. POST to /Api/search/timetableV2 — returns prices + departure times.
   This endpoint is NOT behind KPSDK (unlike /Api/search/search).
3. Parse response into FlightOffer objects.

Result: ~1-3s per search, works in Cloud Run containers, zero browser overhead.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import logging
import re
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
from .airline_routes import get_city_airports
from .browser import auto_block_if_proxied

logger = logging.getLogger(__name__)

_IMPERSONATE = "chrome131"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)
_FALLBACK_VERSION = "28.3.0"
_MAX_ATTEMPTS = 2


def _api_headers() -> dict[str, str]:
    return {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://wizzair.com",
        "Referer": "https://wizzair.com/",
    }


def _get_curl_proxy() -> dict | None:
    """Return curl_cffi proxy dict from LETSFG_PROXY, or None."""
    import os
    url = os.environ.get("LETSFG_PROXY", "").strip()
    if not url:
        return None
    return {"http": url, "https": url}


def _get_version_sync() -> str:
    """Fetch API version from /buildnumber (sync, run in executor)."""
    from curl_cffi import requests as cffi_requests

    try:
        proxies = _get_curl_proxy()
        sess = cffi_requests.Session(impersonate=_IMPERSONATE, proxies=proxies)
        r = sess.get(
            "https://wizzair.com/buildnumber",
            headers={"User-Agent": _UA},
            timeout=10,
        )
        m = re.search(r"(\d+\.\d+\.\d+)", r.text)
        return m.group(1) if m else _FALLBACK_VERSION
    except Exception as exc:
        logger.warning("Wizzair: buildnumber fetch failed: %s", exc)
        return _FALLBACK_VERSION


def _search_timetable_sync(
    version: str,
    origin: str,
    destination: str,
    date_from: str,
    date_to: str | None,
    adults: int,
    children: int,
    infants: int,
) -> dict | None:
    """POST to timetableV2 (sync, run in executor). Returns parsed JSON."""
    from curl_cffi import requests as cffi_requests

    proxies = _get_curl_proxy()
    sess = cffi_requests.Session(impersonate=_IMPERSONATE, proxies=proxies)
    base = f"https://be.wizzair.com/{version}/Api"

    flight_list = [
        {
            "departureStation": origin,
            "arrivalStation": destination,
            "from": date_from,
            "to": date_from,  # single-day query
        }
    ]
    if date_to:
        flight_list.append(
            {
                "departureStation": destination,
                "arrivalStation": origin,
                "from": date_to,
                "to": date_to,
            }
        )

    body = {
        "flightList": flight_list,
        "adultCount": adults,
        "childCount": children,
        "infantCount": infants,
        "wdc": True,
        "priceType": "regular",
    }

    r = sess.post(
        f"{base}/search/timetableV2",
        json=body,
        headers=_api_headers(),
        timeout=15,
    )
    if r.status_code == 200:
        return r.json()
    logger.warning("Wizzair timetableV2: %d %s", r.status_code, r.text[:200])
    return None


class WizzairConnectorClient:
    """Wizzair search via curl_cffi + timetableV2 (no browser needed)."""

    def __init__(self, timeout: float = 25.0):
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
        # Wizzair requires station codes, not city codes. Expand and merge.
        origins = get_city_airports(req.origin)
        destinations = get_city_airports(req.destination)

        if len(origins) > 1 or len(destinations) > 1:
            all_offers: list[FlightOffer] = []
            for o in origins:
                for d in destinations:
                    if o == d:
                        continue
                    sub_req = FlightSearchRequest(
                        origin=o,
                        destination=d,
                        date_from=req.date_from,
                        return_from=req.return_from,
                        adults=req.adults,
                        children=req.children,
                        infants=req.infants,
                        cabin_class=req.cabin_class,
                        currency=req.currency,
                        max_stopovers=req.max_stopovers,
                    )
                    try:
                        resp = await self._search_single(sub_req)
                        all_offers.extend(resp.offers)
                    except Exception:
                        pass
            all_offers.sort(key=lambda o: o.price)
            search_hash_id = hashlib.md5(
                f"wizzair{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{search_hash_id}",
                origin=req.origin,
                destination=req.destination,
                currency=req.currency,
                offers=all_offers,
                total_results=len(all_offers),
            )
        return await self._search_single(req)

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search a single origin→destination pair (station-level codes)."""
        t0 = time.monotonic()
        loop = asyncio.get_running_loop()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                version = await loop.run_in_executor(None, _get_version_sync)
                logger.info(
                    "Wizzair: v%s, searching %s→%s on %s",
                    version, req.origin, req.destination, req.date_from,
                )

                date_from = req.date_from.strftime("%Y-%m-%d")
                date_to = (
                    req.return_from.strftime("%Y-%m-%d") if req.return_from else None
                )

                data = await loop.run_in_executor(
                    None,
                    functools.partial(
                        _search_timetable_sync,
                        version,
                        req.origin,
                        req.destination,
                        date_from,
                        date_to,
                        req.adults,
                        req.children,
                        req.infants,
                    ),
                )
                if data is not None:
                    _w6_cabin = {"M": "economy", "W": "premium_economy", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                    outbound = self._parse_timetable(
                        data.get("outboundFlights") or [], req.date_from, _w6_cabin
                    )
                    inbound = self._parse_timetable(
                        data.get("returnFlights") or [],
                        req.return_from if req.return_from else req.date_from,
                        _w6_cabin,
                    )
                    offers = self._build_offers(req, outbound, inbound)
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "Wizzair %s→%s returned %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    search_hash_id = hashlib.md5(
                        f"wizzair{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{search_hash_id}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
                logger.warning("Wizzair: attempt %d/%d empty", attempt, _MAX_ATTEMPTS)
            except Exception as e:
                logger.warning("Wizzair: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    # ------------------------------------------------------------------
    # Parsing (timetableV2 response)
    # ------------------------------------------------------------------

    def _parse_timetable(
        self, flights: list[dict], target_date: datetime | object, cabin_class: str = "economy"
    ) -> list[dict]:
        """Parse timetableV2 flight entries into intermediate format.

        Each entry represents one day and contains:
          - departureStation, arrivalStation
          - price.amount, price.currencyCode
          - departureDates: [{date: ..., isCheapestOfTheDay: bool}, ...]
        We create one parsed record per departure time slot.
        """
        results: list[dict] = []
        target_ymd = (
            target_date.strftime("%Y-%m-%d")
            if hasattr(target_date, "strftime")
            else str(target_date)[:10]
        )

        for flight in flights:
            price_obj = flight.get("price") or {}
            amount = price_obj.get("amount", 0)
            currency = price_obj.get("currencyCode", "EUR")
            if not amount or amount <= 0:
                continue

            dep_station = flight.get("departureStation", "")
            arr_station = flight.get("arrivalStation", "")
            dep_dates = flight.get("departureDates") or []

            # Filter departure times to the target date
            for slot in dep_dates:
                slot_dt_str = slot.get("date", "")
                if not slot_dt_str.startswith(target_ymd):
                    continue

                dep_dt = self._parse_dt(slot_dt_str)
                key = f"W6_{dep_station}{arr_station}_{slot_dt_str}"

                route = FlightRoute(
                    segments=[
                        FlightSegment(
                            airline="W6",
                            airline_name="Wizz Air",
                            flight_no="W6",
                            origin=dep_station,
                            destination=arr_station,
                            departure=dep_dt,
                            arrival=dep_dt,  # timetableV2 has no arrival time
                            cabin_class=cabin_class,
                        )
                    ],
                    total_duration_seconds=0,
                    stopovers=0,
                )
                results.append(
                    {
                        "price": float(amount),
                        "currency": currency,
                        "key": key,
                        "route": route,
                        "cheapest": slot.get("isCheapestOfTheDay", False),
                    }
                )

        return results

    def _build_offers(
        self,
        req: FlightSearchRequest,
        outbound_parsed: list[dict],
        return_parsed: list[dict],
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        if req.return_from and return_parsed:
            outbound_parsed.sort(key=lambda x: x["price"])
            return_parsed.sort(key=lambda x: x["price"])

            for ob in outbound_parsed[:15]:
                for rt in return_parsed[:10]:
                    total = ob["price"] + rt["price"]
                    offer = FlightOffer(
                        id=f"w6_{hashlib.md5((ob['key'] + rt['key']).encode()).hexdigest()[:12]}",
                        price=round(total, 2),
                        currency=ob.get("currency", req.currency),
                        price_formatted=f"{total:.2f} {ob.get('currency', req.currency)}",
                        outbound=ob["route"],
                        inbound=rt["route"],
                        airlines=["Wizz Air"],
                        owner_airline="W6",
                        booking_url=self._build_booking_url(req),
                        is_locked=False,
                        source="wizzair_api",
                        source_tier="free",
                    )
                    offers.append(offer)
        else:
            for ob in outbound_parsed:
                offer = FlightOffer(
                    id=f"w6_{hashlib.md5(ob['key'].encode()).hexdigest()[:12]}",
                    price=round(ob["price"], 2),
                    currency=ob.get("currency", req.currency),
                    price_formatted=f"{ob['price']:.2f} {ob.get('currency', req.currency)}",
                    outbound=ob["route"],
                    inbound=None,
                    airlines=["Wizz Air"],
                    owner_airline="W6",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="wizzair_api",
                    source_tier="free",
                )
                offers.append(offer)

        offers.sort(key=lambda o: o.price)
        return offers

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_out = req.date_from.isoformat()
        date_in = req.return_from.isoformat() if req.return_from else ""
        return (
            f"https://wizzair.com/en-gb#/booking/select-flight/"
            f"{req.origin}/{req.destination}/{date_out}/{date_in}/"
            f"{req.adults}/{req.children}/{req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"wizzair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
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
                    id=f"rt_wizz_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]


# ── Bookable connector (checkout automation) ─────────────────────────────

class WizzairBookableConnector:
    """
    Drive Wizzair checkout up to (not including) payment submission.

    Flow: Homepage (Kasada init) → Flight selection → BASIC fare →
          Passengers → Skip extras (bags, insurance, priority, seats) →
          STOP at payment page.

    Uses Playwright with Kasada bypass. Never submits payment.
    """

    AIRLINE_NAME = "Wizz Air"
    SOURCE_TAG = "wizzair_direct"

    async def start_checkout(
        self,
        offer: dict,
        passengers: list[dict],
        checkout_token: str,
        api_key: str,
        *,
        base_url: str | None = None,
    ):
        from .booking_base import (
            CheckoutProgress,
            dismiss_overlays,
            safe_click,
            safe_fill,
            take_screenshot_b64,
            verify_checkout_token,
        )
        import random
        import time

        t0 = time.monotonic()
        booking_url = offer.get("booking_url", "")
        offer_id = offer.get("id", "")

        # Verify checkout token with backend
        try:
            verification = verify_checkout_token(offer_id, checkout_token, api_key, base_url)
            if not verification.get("valid"):
                return CheckoutProgress(
                    status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                    offer_id=offer_id, booking_url=booking_url,
                    message="Checkout token invalid or expired. Call unlock() first.",
                )
        except Exception as e:
            return CheckoutProgress(
                status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                offer_id=offer_id, booking_url=booking_url,
                message=f"Token verification failed: {e}",
            )

        if not booking_url:
            return CheckoutProgress(
                status="failed", airline=self.AIRLINE_NAME, source=self.SOURCE_TAG,
                offer_id=offer_id, message="No booking URL available for this offer.",
            )

        from playwright.async_api import async_playwright

        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1440,900",
            ],
        )
        _browser_pid = None
        try:
            _browser_pid = browser._impl_obj._browser_process.pid
        except Exception:
            pass
        context = await browser.new_context(
            viewport={"width": random.choice([1366, 1440, 1920]),
                       "height": random.choice([768, 900, 1080])},
            locale=random.choice(["en-GB", "en-US", "en-IE"]),
            timezone_id=random.choice(["Europe/Warsaw", "Europe/London", "Europe/Budapest"]),
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await auto_block_if_proxied(page)
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()
                await auto_block_if_proxied(page)

            step = "started"
            pax = passengers[0] if passengers else {}

            # Step 1: Load Wizzair homepage first (Kasada initialization)
            logger.info("Wizzair checkout: loading homepage for Kasada init")
            await page.goto("https://wizzair.com/en-gb", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
            await dismiss_overlays(page)

            # Navigate to booking URL
            logger.info("Wizzair checkout: navigating to %s", booking_url)
            await page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            await dismiss_overlays(page)
            step = "page_loaded"

            # Step 2: Select flights — wait for flight cards to appear
            try:
                await page.wait_for_selector(
                    "[data-test*='flight'], [class*='flight-select'], [class*='flight-row']",
                    timeout=20000,
                )
            except Exception:
                logger.warning("Wizzair checkout: flight cards not visible")

            await dismiss_overlays(page)

            # Select outbound flight
            outbound = offer.get("outbound", {})
            segments = outbound.get("segments", []) if isinstance(outbound, dict) else []
            if segments:
                dep = segments[0].get("departure", "")
                if dep and len(dep) >= 16:
                    dep_time = dep[11:16]
                    try:
                        card = page.locator(f"text='{dep_time}'").first
                        if await card.is_visible(timeout=3000):
                            await card.click()
                    except Exception:
                        pass

            # Fallback: click first flight
            for sel in [
                "[data-test*='flight']:first-child",
                "[class*='flight-select']:first-child",
                "[class*='flight-row']:first-child",
            ]:
                await safe_click(page, sel, timeout=3000, desc="first flight")

            await page.wait_for_timeout(2000)
            step = "flights_selected"

            # Step 3: Select BASIC fare
            for sel in [
                "[data-test*='basic'] button",
                "button:has-text('BASIC')",
                "[class*='fare-selector'] button:first-child",
                "button:has-text('Select'):first-child",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="select BASIC fare"):
                    break

            await page.wait_for_timeout(1500)
            step = "fare_selected"

            # Dismiss login modal if it appears
            for sel in [
                "button:has-text('Continue as guest')",
                "button:has-text('No, thanks')",
                "button:has-text('Not now')",
                "[data-test*='login-modal'] button:has-text('Later')",
                "[class*='modal'] button:has-text('Continue')",
            ]:
                if await safe_click(page, sel, timeout=4000, desc="skip login"):
                    break
            await page.wait_for_timeout(1000)
            await dismiss_overlays(page)
            step = "login_bypassed"

            # Step 4: Fill passenger details
            try:
                await page.wait_for_selector(
                    "input[data-test*='first-name'], input[name*='firstName'], [class*='passenger-form']",
                    timeout=15000,
                )
            except Exception:
                pass

            # First name
            for sel in [
                "input[data-test*='first-name']",
                "input[name*='firstName']",
                "input[placeholder*='First name' i]",
            ]:
                if await safe_fill(page, sel, pax.get("given_name", "Test")):
                    break

            # Last name
            for sel in [
                "input[data-test*='last-name']",
                "input[name*='lastName']",
                "input[placeholder*='Last name' i]",
            ]:
                if await safe_fill(page, sel, pax.get("family_name", "Traveler")):
                    break

            # Gender
            gender = pax.get("gender", "m")
            gender_text = "Male" if gender == "m" else "Female"
            for sel in [
                f"label:has-text('{gender_text}')",
                f"[data-test*='gender-{gender}']",
            ]:
                await safe_click(page, sel, timeout=3000, desc=f"gender {gender_text}")

            # Date of birth
            dob = pax.get("born_on", "1990-06-15")
            dob_parts = dob.split("-")
            if len(dob_parts) == 3:
                for sel in [
                    "input[data-test*='dob-year']",
                    "input[name*='birthYear']",
                ]:
                    await safe_fill(page, sel, dob_parts[0])
                for sel in [
                    "input[data-test*='dob-month']",
                    "input[name*='birthMonth']",
                ]:
                    await safe_fill(page, sel, dob_parts[1])
                for sel in [
                    "input[data-test*='dob-day']",
                    "input[name*='birthDay']",
                ]:
                    await safe_fill(page, sel, dob_parts[2])

            # Email + phone
            for sel in [
                "input[data-test*='email']",
                "input[name*='email']",
                "input[type='email']",
            ]:
                if await safe_fill(page, sel, pax.get("email", "test@example.com")):
                    break
            for sel in [
                "input[data-test*='phone']",
                "input[name*='phone']",
                "input[type='tel']",
            ]:
                if await safe_fill(page, sel, pax.get("phone_number", "+441234567890")):
                    break

            step = "passengers_filled"

            # Click continue/next
            for sel in [
                "button:has-text('Continue')",
                "button:has-text('Next')",
                "[data-test*='continue'] button",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="continue after passengers"):
                    break
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            # Step 5: Skip extras (bags, insurance, priority, etc.)
            for _ in range(5):
                await dismiss_overlays(page)
                for sel in [
                    "button:has-text('No, thanks')",
                    "button:has-text('Continue')",
                    "button:has-text('Skip')",
                    "button:has-text('I don\\'t need')",
                    "button:has-text('Next')",
                    "[data-test*='cabin-bag-no']",
                    "[data-test*='skip']",
                ]:
                    await safe_click(page, sel, timeout=2000, desc="skip extras")
                await page.wait_for_timeout(1500)

            step = "extras_skipped"

            # Step 6: Skip seat selection
            for sel in [
                "button:has-text('Skip seat selection')",
                "button:has-text('No, thanks')",
                "button:has-text('Continue without')",
                "button:has-text('Skip')",
                "[data-test*='skip-seat']",
            ]:
                if await safe_click(page, sel, timeout=4000, desc="skip seats"):
                    break
            await page.wait_for_timeout(1500)
            # Confirm skip dialog
            for sel in ["button:has-text('OK')", "button:has-text('Yes')", "button:has-text('Continue')"]:
                await safe_click(page, sel, timeout=3000, desc="confirm skip seats")

            step = "seats_skipped"
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            # Step 7: Payment page reached — STOP HERE
            step = "payment_page_reached"
            screenshot = await take_screenshot_b64(page)

            # Try to read displayed price
            page_price = offer.get("price", 0.0)
            try:
                for sel in [
                    "[data-test*='total-price']",
                    "[class*='total'] [class*='price']",
                    "[class*='summary-price']",
                ]:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=2000):
                        text = await el.text_content()
                        if text:
                            import re
                            nums = re.findall(r"[\d,.]+", text)
                            if nums:
                                page_price = float(nums[-1].replace(",", ""))
                        break
            except Exception:
                pass

            elapsed = time.monotonic() - t0
            return CheckoutProgress(
                status="payment_page_reached",
                step=step,
                step_index=8,
                airline=self.AIRLINE_NAME,
                source=self.SOURCE_TAG,
                offer_id=offer_id,
                total_price=page_price,
                currency=offer.get("currency", "EUR"),
                booking_url=booking_url,
                screenshot_b64=screenshot,
                message=(
                    f"Wizz Air checkout complete — reached payment page in {elapsed:.0f}s. "
                    f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                    f"Payment NOT submitted (safe mode). "
                    f"Complete manually at: {booking_url}"
                ),
                can_complete_manually=True,
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            logger.error("Wizzair checkout error: %s", e, exc_info=True)
            screenshot = ""
            try:
                screenshot = await take_screenshot_b64(page)
            except Exception:
                pass
            return CheckoutProgress(
                status="error",
                step=step,
                airline=self.AIRLINE_NAME,
                source=self.SOURCE_TAG,
                offer_id=offer_id,
                booking_url=booking_url,
                screenshot_b64=screenshot,
                message=f"Checkout error at step '{step}': {e}",
                elapsed_seconds=time.monotonic() - t0,
            )
        finally:
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass
            try:
                await pw.stop()
            except Exception:
                pass
            if _browser_pid:
                try:
                    import subprocess
                    subprocess.run(
                        ["taskkill", "/F", "/T", "/PID", str(_browser_pid)],
                        capture_output=True, timeout=5,
                    )
                except Exception:
                    pass


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
                    id=f"rt_wizz_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
