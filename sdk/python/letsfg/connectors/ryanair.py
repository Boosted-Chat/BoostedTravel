"""
Ryanair direct API scraper — queries Ryanair's public REST API.

Ryanair exposes a rich internal REST API used by their SPA frontend.
These endpoints are publicly accessible without authentication.
This is the DEFINITIVE source for Ryanair pricing — no middleman markup.

Uses curl_cffi to bypass Cloudflare TLS fingerprint detection.
Implements the Ryanair AIP (api/protocols/ryanair.json).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
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
from .airline_routes import get_city_airports
from .browser import get_curl_cffi_proxies

logger = logging.getLogger(__name__)

RYANAIR_API = "https://www.ryanair.com/api"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


class RyanairConnectorClient:
    """Direct scraper for Ryanair's public API — zero auth, real-time prices."""

    def __init__(self, timeout: float = 20.0):
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
        """
        Search Ryanair availability API.

        GET /booking/v4/en-gb/availability
        Returns real-time fares directly from Ryanair — no middleman.

        Ryanair requires station codes (STN, LTN, LGW), not city codes
        (LON). When a city code is passed, we fan out to all constituent
        airports and merge results.
        """
        origins = get_city_airports(req.origin)
        destinations = get_city_airports(req.destination)

        # If city expansion produced multiple airports, fan out
        if len(origins) > 1 or len(destinations) > 1:
            import asyncio as _aio
            tasks = []
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
                    tasks.append(self._search_single(sub_req))
            results = await _aio.gather(*tasks, return_exceptions=True)
            all_offers = []
            for r in results:
                if isinstance(r, FlightSearchResponse):
                    all_offers.extend(r.offers)
            all_offers.sort(key=lambda o: o.price)
            search_hash = hashlib.md5(
                f"ryanair{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=req.currency,
                offers=all_offers,
                total_results=len(all_offers),
            )
        return await self._search_single(req)

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search a single origin→destination pair (airport-level codes)."""
        from curl_cffi.requests import AsyncSession

        params: dict[str, Any] = {
            "ADT": req.adults,
            "CHD": req.children,
            "INF": req.infants,
            "DateOut": req.date_from.isoformat(),
            "Origin": req.origin,
            "Destination": req.destination,
            "RoundTrip": "true" if req.return_from else "false",
            "ToUs": "AGREED",
            "IncludeConnectingFlights": "true",
            "FlexDaysBeforeOut": 0,
            "FlexDaysOut": 0,
            "FlexDaysBeforeIn": 0,
            "FlexDaysIn": 0,
            "promoCode": "",
        }

        if req.return_from:
            params["DateIn"] = req.return_from.isoformat()
        else:
            params["DateIn"] = ""

        t0 = time.monotonic()

        try:
            # Use curl_cffi with Chrome impersonation to bypass Cloudflare
            async with AsyncSession(
                impersonate="chrome131",
                timeout=self.timeout,
                proxies=get_curl_cffi_proxies(),
            ) as sess:
                resp = await sess.get(
                    f"{RYANAIR_API}/booking/v4/en-gb/availability",
                    params=params,
                    headers=_HEADERS,
                )
        except asyncio.TimeoutError:
            logger.warning("Ryanair API timed out (%s→%s)", req.origin, req.destination)
            return self._empty(req)
        except Exception as e:
            logger.error("Ryanair API error (%s→%s): %s", req.origin, req.destination, e)
            return self._empty(req)

        elapsed = time.monotonic() - t0

        if resp.status_code != 200:
            logger.warning(
                "Ryanair API %s→%s returned %d: %s",
                req.origin, req.destination,
                resp.status_code,
                resp.text[:300],
            )
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("Ryanair returned non-JSON response: %s", resp.text[:200])
            return self._empty(req)

        currency = data.get("currency", req.currency)
        trips = data.get("trips", [])

        # Parse outbound and return trip data
        outbound_flights = []
        return_flights = []

        for trip in trips:
            for date_entry in trip.get("dates", []):
                for flight in date_entry.get("flights", []):
                    if not flight.get("regularFare"):
                        continue  # Skip sold-out flights
                    parsed = self._parse_flight(flight, currency, trip)
                    if parsed:
                        # Determine if outbound or return based on trip index
                        if trip == trips[0]:
                            outbound_flights.append(parsed)
                        else:
                            return_flights.append(parsed)

        # Build offers: if round trip, combine outbound × return (cheapest combos)
        offers = []
        if req.return_from and return_flights:
            # Combine best outbound with best return
            outbound_flights.sort(key=lambda x: x["price"])
            return_flights.sort(key=lambda x: x["price"])

            # Take top outbound and return (limit combos to avoid explosion)
            for ob in outbound_flights[:15]:
                for rt in return_flights[:10]:
                    total = ob["price"] + rt["price"]
                    offer = FlightOffer(
                        id=f"ry_{hashlib.md5((ob['key'] + rt['key']).encode()).hexdigest()[:12]}",
                        price=round(total, 2),
                        currency=currency,
                        price_formatted=f"{total:.2f} {currency}",
                        outbound=ob["route"],
                        inbound=rt["route"],
                        airlines=["Ryanair"],
                        owner_airline="FR",
                        booking_url=self._build_booking_url(req),
                        is_locked=False,
                        source="ryanair_direct",
                        source_tier="free",
                    )
                    offers.append(offer)
        else:
            # One-way: each outbound is an offer
            for ob in outbound_flights:
                offer = FlightOffer(
                    id=f"ry_{hashlib.md5(ob['key'].encode()).hexdigest()[:12]}",
                    price=round(ob["price"], 2),
                    currency=currency,
                    price_formatted=f"{ob['price']:.2f} {currency}",
                    outbound=ob["route"],
                    inbound=None,
                    airlines=["Ryanair"],
                    owner_airline="FR",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="ryanair_direct",
                    source_tier="free",
                )
                offers.append(offer)

        # Sort by price
        offers.sort(key=lambda o: o.price)

        logger.info(
            "Ryanair direct %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"ryanair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=offers,
            total_results=len(offers),
        )

    def _parse_flight(
        self, flight: dict, currency: str, trip: dict
    ) -> Optional[dict]:
        """Parse a single Ryanair flight entry."""
        regular_fare = flight.get("regularFare")
        if not regular_fare:
            return None

        # Get total adult fare
        fares = regular_fare.get("fares", [])
        total_price = 0.0
        for fare in fares:
            total_price += float(fare.get("amount", 0)) * int(fare.get("count", 1))

        if total_price <= 0:
            return None

        # Parse segments
        segments_data = flight.get("segments", [])
        time_arr = flight.get("timeUTC", flight.get("time", []))

        segments = []
        for seg in segments_data:
            seg_time = seg.get("time", [])
            dep_str = seg_time[0] if len(seg_time) > 0 else ""
            arr_str = seg_time[1] if len(seg_time) > 1 else ""

            dep_dt = self._parse_dt(dep_str)
            arr_dt = self._parse_dt(arr_str)

            flight_no = seg.get("flightNumber", "").replace(" ", "")

            segments.append(FlightSegment(
                airline="FR",
                airline_name="Ryanair",
                flight_no=flight_no,
                origin=seg.get("origin", trip.get("origin", "")),
                destination=seg.get("destination", trip.get("destination", "")),
                departure=dep_dt,
                arrival=arr_dt,
                cabin_class="M",
            ))

        if not segments:
            # Fallback: build from trip-level time
            dep_str = time_arr[0] if len(time_arr) > 0 else ""
            arr_str = time_arr[1] if len(time_arr) > 1 else ""

            segments.append(FlightSegment(
                airline="FR",
                airline_name="Ryanair",
                flight_no=flight.get("flightNumber", ""),
                origin=trip.get("origin", ""),
                destination=trip.get("destination", ""),
                departure=self._parse_dt(dep_str),
                arrival=self._parse_dt(arr_str),
                cabin_class="M",
            ))

        total_dur = 0
        if len(segments) >= 1:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = flight.get("flightKey", f"{segments[0].flight_no}_{segments[0].departure.isoformat()}")

        return {
            "price": total_price,
            "key": flight_key,
            "route": route,
            "seats_left": flight.get("faresLeft", -1),
        }

    def _parse_dt(self, s: str) -> datetime:
        """Parse Ryanair datetime string."""
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
        """Build a direct Ryanair booking URL."""
        date_out = req.date_from.isoformat()
        date_in = req.return_from.isoformat() if req.return_from else ""
        is_return = "true" if req.return_from else "false"
        return (
            f"https://www.ryanair.com/gb/en/trip/flights/select"
            f"?adults={req.adults}&teens=0&children={req.children}"
            f"&infants={req.infants}&dateOut={date_out}&dateIn={date_in}"
            f"&isConnectedFlight=false&discount=0&isReturn={is_return}"
            f"&promoCode=&originIata={req.origin}&destinationIata={req.destination}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"ryanair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


# ── Bookable connector (checkout automation) ─────────────────────────────

class RyanairBookableConnector:
    """
    Drive Ryanair checkout up to (not including) payment submission.

    Flow: Navigate booking URL → Select flights → Pick cheapest fare →
          Skip login → Fill passengers → Skip extras & seats →
          STOP at payment page.

    Uses Playwright. Never submits payment. Safe for testing.
    """
    from connectors.booking_base import BookableConnector, CheckoutProgress

    AIRLINE_NAME = "Ryanair"
    SOURCE_TAG = "ryanair_direct"

    async def start_checkout(
        self,
        offer: dict,
        passengers: list[dict],
        checkout_token: str,
        api_key: str,
        *,
        base_url: str | None = None,
    ):
        from connectors.booking_base import (
            BookableConnector,
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
            locale="en-GB",
            timezone_id="Europe/London",
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

            # Step 1: Navigate to booking page
            await page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)
            step = "page_loaded"

            # Step 2: Select flights
            try:
                await page.wait_for_selector(
                    "[data-ref*='flight-card'], flight-card, [class*='flight-card']",
                    timeout=15000,
                )
            except Exception:
                pass
            await dismiss_overlays(page)

            # Select outbound by departure time if available
            outbound = offer.get("outbound", {})
            segments = outbound.get("segments", []) if isinstance(outbound, dict) else []
            if segments:
                dep = segments[0].get("departure", "")
                if dep and len(dep) >= 16:
                    dep_time = dep[11:16]  # "HH:MM"
                    try:
                        card = page.locator(f"text='{dep_time}'").first
                        if await card.is_visible(timeout=3000):
                            await card.click()
                    except Exception:
                        pass

            # Fallback: click first flight card
            for sel in ["flight-card:first-child", "[class*='flight-card']:first-child"]:
                await safe_click(page, sel, timeout=3000, desc="first flight")

            await page.wait_for_timeout(1500)
            step = "flights_selected"

            # Step 3: Select cheapest fare
            for sel in [
                "[data-ref*='fare-card--regular'] button",
                "fare-card:first-child button",
                "button:has-text('Regular')",
                "button:has-text('Value')",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="select fare"):
                    await page.wait_for_timeout(1000)
                    # Dismiss upsell
                    for upsell in [
                        "button:has-text('No, thanks')",
                        "button:has-text('Continue with Regular')",
                    ]:
                        await safe_click(page, upsell, timeout=3000, desc="decline upsell")
                    break

            step = "fare_selected"
            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)

            # Step 4: Skip login
            for sel in [
                "button:has-text('Log in later')",
                "button:has-text('Continue as guest')",
                "[data-ref='login-gate__skip']",
                "button:has-text('Not now')",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="skip login"):
                    break
            await page.wait_for_timeout(1500)
            await dismiss_overlays(page)
            step = "login_bypassed"

            # Step 5: Fill passenger details
            try:
                await page.wait_for_selector(
                    "input[name*='name'], pax-passenger, [class*='passenger-form']",
                    timeout=15000,
                )
            except Exception:
                pass

            # Title
            title_text = "Mr" if pax.get("gender", "m") == "m" else "Ms"
            for sel in [
                "button[data-ref='title-toggle']",
                "[class*='dropdown'] button:has-text('Title')",
            ]:
                if await safe_click(page, sel, timeout=5000, desc="title dropdown"):
                    await page.wait_for_timeout(500)
                    await safe_click(page, f"button:has-text('{title_text}')", timeout=3000)
                    break

            # First name
            for sel in [
                "input[name*='name'][name*='first']",
                "input[data-ref*='first-name']",
                "input[placeholder*='First name' i]",
            ]:
                if await safe_fill(page, sel, pax.get("given_name", "Test")):
                    break

            # Last name
            for sel in [
                "input[name*='name'][name*='last']",
                "input[data-ref*='last-name']",
                "input[placeholder*='Last name' i]",
            ]:
                if await safe_fill(page, sel, pax.get("family_name", "Traveler")):
                    break

            step = "passengers_filled"

            # Click continue
            await safe_click(page, "button:has-text('Continue'), button[data-ref*='continue']",
                             desc="continue after passengers")
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            # Step 6: Skip extras (bags)
            for _ in range(4):
                await dismiss_overlays(page)
                for sel in [
                    "button:has-text('Continue without')",
                    "button:has-text('No thanks')",
                    "button:has-text('OK, got it')",
                    "button:has-text('Continue')",
                    "button:has-text('Not interested')",
                    "button:has-text('Skip')",
                ]:
                    await safe_click(page, sel, timeout=2000, desc="skip extras")
                await page.wait_for_timeout(1000)

            step = "extras_skipped"

            # Step 7: Skip seats
            for sel in [
                "button:has-text('No thanks')",
                "button:has-text('Not now')",
                "button:has-text('Continue without')",
                "button:has-text('OK, pick seats later')",
                "[data-ref*='seats-action__button--later']",
            ]:
                if await safe_click(page, sel, timeout=4000, desc="skip seats"):
                    break
            await page.wait_for_timeout(1000)
            for sel in ["button:has-text('OK')", "button:has-text('Continue')"]:
                await safe_click(page, sel, timeout=3000, desc="confirm skip seats")

            step = "seats_skipped"
            await page.wait_for_timeout(2000)
            await dismiss_overlays(page)

            # Step 8: Payment page reached — STOP HERE
            step = "payment_page_reached"
            screenshot = await take_screenshot_b64(page)

            # Try to extract the displayed price
            page_price = offer.get("price", 0.0)
            try:
                for sel in [
                    "[class*='total'] [class*='price']",
                    "[data-ref*='total']",
                    "[class*='summary'] [class*='amount']",
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
                    f"Ryanair checkout complete — reached payment page in {elapsed:.0f}s. "
                    f"Price: {page_price} {offer.get('currency', 'EUR')}. "
                    f"Payment NOT submitted (safe mode). "
                    f"Complete manually at: {booking_url}"
                ),
                can_complete_manually=True,
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            logger.error("Ryanair checkout error: %s", e, exc_info=True)
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
                    id=f"rt_ryan_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
